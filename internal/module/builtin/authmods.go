package builtin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/jose"
	"github.com/happydave/hoardmq/internal/module"
)

func init() {
	module.Register("auth.jwt", func() module.Module { return &jwtAuth{} })
	module.Register("auth.x509", func() module.Module { return &x509Auth{} })
	module.Register("auth.scram", func() module.Module { return &scramAuth{} })
}

// jwtConfig is auth.jwt's configuration.
type jwtConfig struct {
	Issuer                  string               `json:"issuer"`
	Audiences               []string             `json:"audiences"`
	JWKSURI                 string               `json:"jwksUri"`
	Introspection           *introspectionConfig `json:"introspection"`
	From                    string               `json:"from"`
	UsernameClaim           string               `json:"usernameClaim"`
	ClientIDClaim           string               `json:"clientIdClaim"`
	ClockSkewSeconds        *int                 `json:"clockSkewSeconds"`
	MaxTokenLifetimeSeconds *int                 `json:"maxTokenLifetimeSeconds"`
	FetchWaitMs             *int                 `json:"fetchWaitMs"`
	CAFile                  string               `json:"caFile"`
}

type introspectionConfig struct {
	Endpoint         string `json:"endpoint"`
	ClientID         string `json:"clientId"`
	ClientSecretFile string `json:"clientSecretFile"`
	TimeoutMs        *int   `json:"timeoutMs"`
	// OpaqueTokens introspects a value that is not a JWS too; with
	// OpaqueTokenPrefix, only one that starts with it.
	OpaqueTokens      bool   `json:"opaqueTokens"`
	OpaqueTokenPrefix string `json:"opaqueTokenPrefix"`
}

// intOr is v's value, or def when v is absent.
func intOr(v *int, def int) int {
	if v == nil {
		return def
	}
	return *v
}

// inRange appends a problem when v is present and outside [lo, hi].
func inRange(problems []string, name string, v *int, lo, hi int) []string {
	if v != nil && (*v < lo || *v > hi) {
		return append(problems, fmt.Sprintf("%s must be %d to %d", name, lo, hi))
	}
	return problems
}

// jwtProblems is every way the configuration is refused. It reads the secret
// and CA files, and a file it cannot read is a problem.
func jwtProblems(c jwtConfig) []string {
	var p []string
	if c.Issuer == "" {
		p = append(p, "issuer is required")
	}
	if len(c.Audiences) == 0 {
		p = append(p, "audiences needs at least one audience")
	}
	for i, a := range c.Audiences {
		if a == "" {
			p = append(p, fmt.Sprintf("audiences[%d] is empty", i))
		}
	}
	switch {
	case c.JWKSURI != "" && c.Introspection != nil:
		p = append(p, "set one of jwksUri and introspection, not both")
	case c.JWKSURI == "" && c.Introspection == nil:
		p = append(p, "one of jwksUri and introspection is required")
	case c.JWKSURI != "":
		if msg := jose.EndpointOK(c.JWKSURI); msg != "" {
			p = append(p, "jwksUri is "+msg+" (https unless the host is loopback)")
		}
	default:
		in := c.Introspection
		if msg := jose.EndpointOK(in.Endpoint); msg != "" {
			p = append(p, "introspection.endpoint is "+msg+" (https unless the host is loopback)")
		}
		if in.ClientID == "" {
			p = append(p, "introspection.clientId is required")
		}
		if in.ClientSecretFile == "" {
			p = append(p, "introspection.clientSecretFile is required")
		} else if b, err := os.ReadFile(in.ClientSecretFile); err != nil {
			p = append(p, "introspection.clientSecretFile: "+err.Error())
		} else if strings.TrimSpace(string(b)) == "" {
			p = append(p, "introspection.clientSecretFile is empty")
		}
		p = inRange(p, "introspection.timeoutMs", in.TimeoutMs, 100, 10000)
		if in.OpaqueTokenPrefix != "" && !in.OpaqueTokens {
			p = append(p, "introspection.opaqueTokenPrefix needs opaqueTokens: without it no opaque value is introspected")
		}
	}
	if c.From != "" && c.From != "password" && c.From != "username" {
		p = append(p, `from must be "password" or "username"`)
	}
	p = inRange(p, "clockSkewSeconds", c.ClockSkewSeconds, 0, 300)
	p = inRange(p, "maxTokenLifetimeSeconds", c.MaxTokenLifetimeSeconds, 60, 31536000)
	p = inRange(p, "fetchWaitMs", c.FetchWaitMs, 0, 10000)
	if c.CAFile != "" {
		if _, err := jose.CertPool(c.CAFile); err != nil {
			p = append(p, "caFile: "+err.Error())
		}
	}
	return p
}

// jwtAuth is auth.jwt: access tokens in the password (or username) field.
type jwtAuth struct {
	toggle
	inner   *auth.JWTAuthenticator
	metrics module.MetricsProvider
}

func (*jwtAuth) ID() module.ID                   { return "auth.jwt" }
func (*jwtAuth) Capabilities() module.Capability { return module.CapAuthentication }
func (*jwtAuth) Stop(context.Context) error      { return nil }

func (j *jwtAuth) Provision(ctx *module.Context, raw json.RawMessage) error {
	var c jwtConfig
	if err := decode(raw, &c); err != nil {
		return err
	}
	if p := jwtProblems(c); len(p) > 0 {
		return errors.New(strings.Join(p, "; "))
	}
	client, err := jose.HTTPClient(c.CAFile)
	if err != nil {
		return err
	}
	username := c.UsernameClaim
	if username == "" {
		username = "sub"
	}
	a := &auth.JWTAuthenticator{Issuer: c.Issuer, Audiences: c.Audiences, FromUsername: c.From == "username",
		UsernameClaim: username, ClientIDClaim: c.ClientIDClaim,
		Skew:        time.Duration(intOr(c.ClockSkewSeconds, 60)) * time.Second,
		MaxLifetime: time.Duration(intOr(c.MaxTokenLifetimeSeconds, 86400)) * time.Second,
		FetchWait:   time.Duration(intOr(c.FetchWaitMs, 2000)) * time.Millisecond, Now: time.Now}
	if c.JWKSURI != "" {
		a.Keys = jose.NewKeySource(c.JWKSURI, client, ctx.Logger)
		a.Keys.StaleWhileRefresh = true
		a.Keys.OnFetch = func(err error) {
			outcome := "ok"
			if err != nil {
				outcome = "failed"
			}
			j.count("hoardmq_auth_jwt_key_fetches_total", map[string]string{"outcome": outcome})
		}
	} else {
		secret, err := os.ReadFile(c.Introspection.ClientSecretFile)
		if err != nil {
			return err
		}
		transport := client.Transport.(*http.Transport).Clone()
		transport.MaxConnsPerHost = auth.MaxIntrospectionConns
		a.Introspection = &auth.Introspector{Endpoint: c.Introspection.Endpoint, ClientID: c.Introspection.ClientID,
			Secret: strings.TrimSpace(string(secret)), Timeout: time.Duration(intOr(c.Introspection.TimeoutMs, 2000)) * time.Millisecond,
			Client: &http.Client{Transport: transport},
			OnResult: func(outcome string) {
				j.count("hoardmq_auth_jwt_introspections_total", map[string]string{"outcome": outcome})
			}}
		a.OpaqueTokens, a.OpaquePrefix = c.Introspection.OpaqueTokens, c.Introspection.OpaqueTokenPrefix
	}
	j.inner = a
	j.enabled.Store(true)
	ctx.Logger.Info("jwt authenticator configured", "issuer", c.Issuer, "introspection", c.Introspection != nil)
	return nil
}

// Start prefetches the key set in the background and returns at once.
func (j *jwtAuth) Start(context.Context) error {
	if j.inner.Keys != nil {
		j.inner.Keys.Prefetch()
	}
	return nil
}

func (j *jwtAuth) UseMetrics(m module.MetricsProvider) { j.metrics = m }

func (j *jwtAuth) count(name string, labels map[string]string) {
	if j.metrics != nil {
		j.metrics.Add(name, labels, 1)
	}
}

func (j *jwtAuth) Authenticate(ctx context.Context, req *auth.Request) auth.Result {
	return j.inner.Authenticate(ctx, req)
}

// x509Config is auth.x509's configuration.
type x509Config struct {
	OCSP        *bool    `json:"ocsp"`
	CRL         *bool    `json:"crl"`
	OCSPURL     string   `json:"ocspUrl"`
	CRLURLs     []string `json:"crlUrls"`
	FailOpen    bool     `json:"failOpen"`
	FetchWaitMs *int     `json:"fetchWaitMs"`
	TimeoutMs   *int     `json:"timeoutMs"`
}

func boolOr(v *bool, def bool) bool {
	if v == nil {
		return def
	}
	return *v
}

// revocationURLOK is the rule for a revocation endpoint: absolute http or
// https. Both answers are signed, and plain HTTP is their convention.
func revocationURLOK(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && u.IsAbs() && u.Host != "" && (u.Scheme == "http" || u.Scheme == "https")
}

func x509Problems(c x509Config) []string {
	var p []string
	if !boolOr(c.OCSP, true) && !boolOr(c.CRL, true) {
		p = append(p, "at least one of ocsp and crl must be on")
	}
	if c.OCSPURL != "" && !revocationURLOK(c.OCSPURL) {
		p = append(p, "ocspUrl must be an absolute http or https URL")
	}
	for i, u := range c.CRLURLs {
		if !revocationURLOK(u) {
			p = append(p, fmt.Sprintf("crlUrls[%d] must be an absolute http or https URL", i))
		}
	}
	p = inRange(p, "fetchWaitMs", c.FetchWaitMs, 0, 10000)
	p = inRange(p, "timeoutMs", c.TimeoutMs, 100, 30000)
	return p
}

// x509Auth is auth.x509: revocation of verified client certificates.
type x509Auth struct {
	toggle
	inner   *auth.Revocation
	metrics module.MetricsProvider
}

func (*x509Auth) ID() module.ID { return "auth.x509" }
func (*x509Auth) Capabilities() module.Capability {
	return module.CapAuthentication | module.CapCertificate
}
func (*x509Auth) Start(context.Context) error { return nil }
func (*x509Auth) Stop(context.Context) error  { return nil }
func (*x509Auth) ChecksCertificates()         {}

func (x *x509Auth) Provision(ctx *module.Context, raw json.RawMessage) error {
	var c x509Config
	if err := decode(raw, &c); err != nil {
		return err
	}
	if p := x509Problems(c); len(p) > 0 {
		return errors.New(strings.Join(p, "; "))
	}
	x.inner = &auth.Revocation{OCSP: boolOr(c.OCSP, true), CRL: boolOr(c.CRL, true), OCSPURL: c.OCSPURL, CRLURLs: c.CRLURLs,
		FailOpen: c.FailOpen, FetchWait: time.Duration(intOr(c.FetchWaitMs, 2000)) * time.Millisecond,
		Timeout: time.Duration(intOr(c.TimeoutMs, 5000)) * time.Millisecond, Client: &http.Client{}, Now: time.Now,
		Count: func(name string, labels map[string]string) {
			if x.metrics != nil {
				x.metrics.Add(name, labels, 1)
			}
		}}
	x.enabled.Store(true)
	ctx.Logger.Info("certificate revocation configured", "ocsp", x.inner.OCSP, "crl", x.inner.CRL, "failOpen", c.FailOpen)
	return nil
}

func (x *x509Auth) UseMetrics(m module.MetricsProvider) { x.metrics = m }

func (x *x509Auth) Authenticate(ctx context.Context, req *auth.Request) auth.Result {
	return x.inner.Authenticate(ctx, req)
}

// scramAuth is auth.scram: SCRAM-SHA-256 over a users file.
type scramAuth struct {
	toggle
	users *auth.FileAuthenticator
	srv   *auth.SCRAMServer
}

func (*scramAuth) ID() module.ID                   { return "auth.scram" }
func (*scramAuth) Capabilities() module.Capability { return module.CapMechanism }
func (*scramAuth) Start(context.Context) error     { return nil }
func (*scramAuth) Stop(context.Context) error      { return nil }
func (s *scramAuth) Reload() error                 { return s.users.Reload() }

func (s *scramAuth) Provision(ctx *module.Context, raw json.RawMessage) error {
	var c pathConfig
	if err := decode(raw, &c); err != nil {
		return err
	}
	if c.Path == "" {
		return errors.New("path is required")
	}
	users, err := auth.LoadUsersFile(c.Path)
	if err != nil {
		return err
	}
	s.users, s.srv = users, auth.NewSCRAMServer(users.SCRAM)
	s.enabled.Store(true)
	ctx.Logger.Info("scram users loaded", "path", c.Path)
	return nil
}

func (s *scramAuth) Methods() []string { return s.srv.Methods() }
func (s *scramAuth) Begin(ctx context.Context, method string, req *auth.Request) auth.Exchange {
	return s.srv.Begin(ctx, method, req)
}
