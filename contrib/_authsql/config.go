package authsql

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/happydave/hoardmq/ext/auth"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type config struct {
	ConnString     string `json:"connString"`
	PasswordFile   string `json:"passwordFile"`
	Query          string `json:"query"`
	OnUnavailable  string `json:"onUnavailable"`
	TimeoutMs      *int   `json:"timeoutMs"`
	MaxConnections *int   `json:"maxConnections"`
}

// settings is a validated configuration.
type settings struct {
	pool              *pgxpool.Config
	query             string   // the query with $1, $2… in place of the placeholders
	params            []string // the placeholder each parameter stands for, in order
	timeout           time.Duration
	passOnUnavailable bool
}

func decodeStrict(raw json.RawMessage, v any) error {
	if len(bytes.TrimSpace(raw)) == 0 {
		raw = json.RawMessage("{}")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

func intOr(p *int, def int) int {
	if p == nil {
		return def
	}
	return *p
}

var (
	placeholder = regexp.MustCompile(`\$\{[^}]*\}`)
	numbered    = regexp.MustCompile(`\$[0-9]`)
	// keywordPassword finds password= in a keyword/value connection string.
	keywordPassword = regexp.MustCompile(`(^|\s)password\s*=`)
)

// rewrite replaces the query's placeholders by positional parameters, in
// order of appearance, and returns which value each stands for.
func rewrite(q string) (string, []string, error) {
	if numbered.MatchString(placeholder.ReplaceAllString(q, "")) {
		return "", nil, fmt.Errorf("query: write values as ${username}, ${clientid} or ${cn}, not $1")
	}
	var params []string
	var bad string
	out := placeholder.ReplaceAllStringFunc(q, func(p string) string {
		name := strings.TrimSuffix(strings.TrimPrefix(p, "${"), "}")
		switch name {
		case "username", "clientid", "cn":
		default:
			if bad == "" {
				bad = p
			}
		}
		params = append(params, name)
		return "$" + strconv.Itoa(len(params))
	})
	switch {
	case bad != "":
		return "", nil, fmt.Errorf("query: unknown placeholder %s; the placeholders are ${username}, ${clientid} and ${cn}", bad)
	case len(params) == 0:
		return "", nil, fmt.Errorf("query: has no placeholder, so it would find the same row for every client")
	}
	return out, params, nil
}

// args are the values the query's parameters stand for. The password is
// never one of them.
func (s *settings) args(req *auth.Request) []any {
	out := make([]any, len(s.params))
	for i, p := range s.params {
		switch p {
		case "username":
			out[i] = req.Username
		case "clientid":
			out[i] = req.ClientID
		case "cn":
			out[i] = req.CertCN
		}
	}
	return out
}

// hasPassword reports whether a connection string carries a password, in
// the URL form or the keyword form.
func hasPassword(s string) bool {
	if strings.HasPrefix(s, "postgres://") || strings.HasPrefix(s, "postgresql://") {
		u, err := url.Parse(s)
		if err != nil {
			return strings.Contains(s, "password=")
		}
		if _, set := u.User.Password(); set {
			return true
		}
		return u.Query().Has("password")
	}
	return keywordPassword.MatchString(s)
}

// local reports whether a database host is this machine: a Unix socket or
// a loopback name or address.
func local(host string, port uint16) bool {
	if network, _ := pgconn.NetworkAddress(host, port); network == "unix" {
		return true
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// verifies reports whether a TLS configuration encrypts and checks the
// server: pgx's verify-full verifies normally, and verify-ca (and require
// with sslrootcert) skips the name but checks the chain itself.
func verifies(c *tls.Config) bool {
	return c != nil && (!c.InsecureSkipVerify || c.VerifyPeerCertificate != nil)
}

// secure reports whether every connection attempt the parsed configuration
// may make, the first and each fallback, is to this machine or over TLS
// that verifies the server.
func secure(c *pgconn.Config) bool {
	if !local(c.Host, c.Port) && !verifies(c.TLSConfig) {
		return false
	}
	for _, f := range c.Fallbacks {
		if !local(f.Host, f.Port) && !verifies(f.TLSConfig) {
			return false
		}
	}
	return true
}

func (c config) validate(readFile func(string) ([]byte, error)) (*settings, []string) {
	var problems []string
	add := func(format string, a ...any) { problems = append(problems, fmt.Sprintf(format, a...)) }
	s := &settings{}
	switch {
	case c.ConnString == "":
		add("connString is required")
	case hasPassword(c.ConnString):
		add("connString holds a password; put it in passwordFile")
	default:
		pc, err := pgxpool.ParseConfig(c.ConnString)
		switch {
		case err != nil:
			add("connString: %v", err)
		case !secure(&pc.ConnConfig.Config):
			add("connString: a host that is not this machine needs sslmode verify-full or verify-ca (or require with sslrootcert): the database password would cross the network in clear or to an unverified server")
		default:
			s.pool = pc
		}
	}
	if c.PasswordFile != "" {
		data, err := readFile(c.PasswordFile)
		switch {
		case err != nil:
			add("passwordFile %s is unreadable", c.PasswordFile)
		case s.pool != nil:
			s.pool.ConnConfig.Password = strings.TrimSuffix(strings.TrimSuffix(string(data), "\n"), "\r")
		}
	}
	if c.Query == "" {
		add("query is required")
	} else if q, params, err := rewrite(c.Query); err != nil {
		add("%v", err)
	} else {
		s.query, s.params = q, params
	}
	switch c.OnUnavailable {
	case "", "deny":
	case "pass":
		s.passOnUnavailable = true
	default:
		add(`onUnavailable must be "deny" or "pass"`)
	}
	if v := intOr(c.TimeoutMs, 2000); v < 100 || v > 30000 {
		add("timeoutMs must be 100 to 30000")
	} else {
		s.timeout = timeoutOf(v)
	}
	if v := intOr(c.MaxConnections, 8); v < 1 || v > 64 {
		add("maxConnections must be 1 to 64")
	} else if s.pool != nil {
		s.pool.MaxConns = int32(v)
		s.pool.MinConns = 0
	}
	if len(problems) > 0 {
		return nil, problems
	}
	return s, nil
}
