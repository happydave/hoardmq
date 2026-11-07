package sinkpostgres

// The connection rules are auth.sql's, copied: a module cannot import
// another module's internals.

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"net"
	"net/url"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// keywordPassword finds password= in a keyword/value connection string.
var keywordPassword = regexp.MustCompile(`(^|\s)password\s*=`)

func decodeStrict(raw json.RawMessage, v any) error {
	if len(bytes.TrimSpace(raw)) == 0 {
		raw = json.RawMessage("{}")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
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
