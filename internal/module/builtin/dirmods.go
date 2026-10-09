package builtin

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/happydave/hoardmq/ext"
	extauth "github.com/happydave/hoardmq/ext/auth"
	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/jose"
	"github.com/happydave/hoardmq/internal/ldap"
)

func init() {
	ext.Register("auth.ldap", func() ext.Module { return &ldapAuth{} })
	ext.Register("auth.db", func() ext.Module { return &dbAuth{} })
}

// ldapConfig is auth.ldap's configuration.
type ldapConfig struct {
	URL           string        `json:"url"`
	StartTLS      bool          `json:"startTLS"`
	CAFile        string        `json:"caFile"`
	BindDN        string        `json:"bindDn"`
	Search        *ldapSearchCf `json:"search"`
	OnUnavailable string        `json:"onUnavailable"`
	MaxConns      *int          `json:"maxConnections"`
	TimeoutMs     *int          `json:"timeoutMs"`
}

type ldapSearchCf struct {
	BaseDN           string `json:"baseDn"`
	Filter           string `json:"filter"`
	Scope            string `json:"scope"`
	BindDN           string `json:"bindDn"`
	BindPasswordFile string `json:"bindPasswordFile"`
}

// ldapProblems is every way the configuration is refused; it reads the
// password and CA files it names.
func ldapProblems(c ldapConfig) []string {
	var p []string
	e, err := ldap.ParseURL(c.URL)
	switch {
	case c.URL == "":
		p = append(p, "url is required")
	case err != nil:
		p = append(p, err.Error())
	case c.StartTLS && e.LDAPS:
		p = append(p, "startTLS is for ldap:// only; ldaps:// is TLS already")
	default:
		e.StartTLS = c.StartTLS
		if !e.Encrypted() && !e.Loopback() {
			p = append(p, "a password would cross the network in clear: use ldaps://, or startTLS, unless the host is loopback")
		}
	}
	switch {
	case c.BindDN != "" && c.Search != nil:
		p = append(p, "set one of bindDn and search, not both")
	case c.BindDN == "" && c.Search == nil:
		p = append(p, "one of bindDn and search is required")
	case c.BindDN != "":
		if _, err := ldap.ParseDNTemplate(c.BindDN); err != nil {
			p = append(p, err.Error())
		}
	default:
		s := c.Search
		if s.BaseDN == "" {
			p = append(p, "search.baseDn is required")
		}
		if _, err := ldap.ParseTemplate(s.Filter); err != nil {
			p = append(p, "search."+err.Error())
		}
		if s.Scope != "" && s.Scope != "sub" && s.Scope != "one" {
			p = append(p, `search.scope must be "sub" or "one"`)
		}
		switch {
		case (s.BindDN == "") != (s.BindPasswordFile == ""):
			p = append(p, "search.bindDn and search.bindPasswordFile go together")
		case s.BindPasswordFile != "":
			if b, err := os.ReadFile(s.BindPasswordFile); err != nil {
				p = append(p, "search.bindPasswordFile: "+err.Error())
			} else if strings.TrimSpace(string(b)) == "" {
				p = append(p, "search.bindPasswordFile is empty")
			}
		}
	}
	if c.OnUnavailable != "" && c.OnUnavailable != "deny" && c.OnUnavailable != "pass" {
		p = append(p, `onUnavailable must be "deny" or "pass"`)
	}
	p = inRange(p, "maxConnections", c.MaxConns, 1, 256)
	p = inRange(p, "timeoutMs", c.TimeoutMs, 100, 30000)
	if c.CAFile != "" {
		if _, err := jose.CertPool(c.CAFile); err != nil {
			p = append(p, "caFile: "+err.Error())
		}
	}
	return p
}

// ldapAuth is auth.ldap: a directory bind, direct or after a search.
type ldapAuth struct {
	toggle
	inner   *auth.LDAPAuthenticator
	metrics ext.MetricsProvider
}

func (*ldapAuth) ID() ext.ID                   { return "auth.ldap" }
func (*ldapAuth) Capabilities() ext.Capability { return ext.CapAuthentication }
func (*ldapAuth) Start(context.Context) error  { return nil }

func (l *ldapAuth) Stop(context.Context) error {
	if l.inner != nil {
		l.inner.Pool.Close()
	}
	return nil
}

// Provision reads the files the configuration names and dials nothing.
func (l *ldapAuth) Provision(ctx *ext.Context, raw json.RawMessage) error {
	var c ldapConfig
	if err := decode(raw, &c); err != nil {
		return err
	}
	if p := ldapProblems(c); len(p) > 0 {
		return errors.New(strings.Join(p, "; "))
	}
	e, _ := ldap.ParseURL(c.URL)
	e.StartTLS = c.StartTLS
	if c.CAFile != "" {
		roots, err := jose.CertPool(c.CAFile)
		if err != nil {
			return err
		}
		e.Config = tlsRoots(roots)
	}
	a := &auth.LDAPAuthenticator{PassOnUnavailable: c.OnUnavailable == "pass",
		Timeout: time.Duration(intOr(c.TimeoutMs, 2000)) * time.Millisecond,
		Count:   l.count}
	a.Pool = &ldap.Pool{Endpoint: e, Max: intOr(c.MaxConns, 16), OnDial: func(ok bool) {
		outcome := "opened"
		if !ok {
			outcome = "failed"
		}
		l.count("hoardmq_auth_ldap_connections_total", map[string]string{"outcome": outcome})
	}}
	if c.BindDN != "" {
		a.BindDN, _ = ldap.ParseDNTemplate(c.BindDN)
	} else {
		s := c.Search
		f, _ := ldap.ParseTemplate(s.Filter)
		scope := ldap.ScopeSub
		if s.Scope == "one" {
			scope = ldap.ScopeOne
		}
		ls := &auth.LDAPSearch{Base: s.BaseDN, Scope: scope, Filter: f, BindDN: s.BindDN}
		if s.BindPasswordFile != "" {
			b, err := os.ReadFile(s.BindPasswordFile)
			if err != nil {
				return err
			}
			ls.BindPassword = []byte(strings.TrimSpace(string(b)))
		}
		a.Search = ls
	}
	l.inner = a
	l.enabled.Store(true)
	ctx.Logger.Info("ldap authenticator configured", "url", c.URL, "startTLS", c.StartTLS, "search", c.Search != nil,
		"onUnavailable", fmt.Sprint(a.PassOnUnavailable))
	return nil
}

func (l *ldapAuth) UseMetrics(m ext.MetricsProvider) { l.metrics = m }

func (l *ldapAuth) count(name string, labels map[string]string) {
	if l.metrics != nil {
		l.metrics.Add(name, labels, 1)
	}
}

func (l *ldapAuth) Authenticate(ctx context.Context, req *extauth.Request) extauth.Result {
	return l.inner.Authenticate(ctx, req)
}

// dbAuth is auth.db: client credentials in the replicated records store.
type dbAuth struct {
	toggle
	inner *auth.DBAuthenticator
}

func (*dbAuth) ID() ext.ID                   { return "auth.db" }
func (*dbAuth) Capabilities() ext.Capability { return ext.CapAuthentication }
func (*dbAuth) Start(context.Context) error  { return nil }
func (*dbAuth) Stop(context.Context) error   { return nil }

// Provision takes no configuration: the credentials are the cluster's,
// administered through the admin API.
func (d *dbAuth) Provision(ctx *ext.Context, raw json.RawMessage) error {
	var c struct{}
	if err := decode(raw, &c); err != nil {
		return err
	}
	svc := ctx.Services
	d.inner = &auth.DBAuthenticator{Cache: ctx.Passwords, Lookup: func(username string) (json.RawMessage, bool, bool) {
		if svc == nil {
			return nil, false, false
		}
		store := svc.Records()
		if store == nil {
			return nil, false, false
		}
		rec, ok := store.Get(auth.CredentialsCollection, username)
		return rec.Value, ok, true
	}}
	d.enabled.Store(true)
	return nil
}

func (d *dbAuth) Authenticate(ctx context.Context, req *extauth.Request) extauth.Result {
	return d.inner.Authenticate(ctx, req)
}

func tlsRoots(roots *x509.CertPool) *tls.Config {
	return &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
}
