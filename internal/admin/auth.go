// Package admin is the admin REST API: authentication, authorization, the
// route table that both dispatches and generates the OpenAPI document, and
// the handlers over a Backend the broker implements.
package admin

import (
	"crypto/sha256"
	"crypto/subtle"
	"crypto/x509"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/happydave/hoardmq/internal/config"
)

// Scope names what a route lets a principal do. The schema's adminScope enum
// repeats the list and the shorthands; a test keeps them equal.
type Scope string

// Scopes.
const (
	ClientsRead   Scope = "clients:read"
	ClientsWrite  Scope = "clients:write"
	RetainedRead  Scope = "retained:read"
	RetainedWrite Scope = "retained:write"
	ClusterRead   Scope = "cluster:read"
	ClusterDrain  Scope = "cluster:drain"
	ConfigRead    Scope = "config:read"
	ConfigWrite   Scope = "config:write"
	TracesRead    Scope = "traces:read"
	TracesWrite   Scope = "traces:write"
	ModulesRead   Scope = "modules:read"
	ModulesWrite  Scope = "modules:write"
	AuditRead     Scope = "audit:read"
	APIKeysRead   Scope = "apikeys:read"
	APIKeysWrite  Scope = "apikeys:write"
	// CredentialsRead and CredentialsWrite read and change the in-broker
	// database of client credentials.
	CredentialsRead  Scope = "credentials:read"
	CredentialsWrite Scope = "credentials:write"
	// BansRead and BansWrite read and change the ban list; LimitsRead and
	// LimitsWrite the administered limits.
	BansRead    Scope = "bans:read"
	BansWrite   Scope = "bans:write"
	LimitsRead  Scope = "limits:read"
	LimitsWrite Scope = "limits:write"
)

// AllScopes lists every scope a route may declare.
var AllScopes = []Scope{ClientsRead, ClientsWrite, RetainedRead, RetainedWrite, ClusterRead, ClusterDrain,
	ConfigRead, ConfigWrite, TracesRead, TracesWrite, ModulesRead, ModulesWrite, AuditRead, APIKeysRead, APIKeysWrite,
	CredentialsRead, CredentialsWrite, BansRead, BansWrite, LimitsRead, LimitsWrite}

// Shorthands a configured identity may hold: every :read scope, and every
// scope.
const (
	ShorthandRead  = "read"
	ShorthandAdmin = "admin"
)

// Expand turns configured scope names into the set a principal holds.
func Expand(names []string) (map[Scope]bool, error) {
	out := map[Scope]bool{}
	for _, n := range names {
		switch n {
		case ShorthandAdmin:
			for _, s := range AllScopes {
				out[s] = true
			}
		case ShorthandRead:
			for _, s := range AllScopes {
				if strings.HasSuffix(string(s), ":read") {
					out[s] = true
				}
			}
		default:
			known := false
			for _, s := range AllScopes {
				if string(s) == n {
					out[s], known = true, true
				}
			}
			if !known {
				return nil, fmt.Errorf("unknown scope %q", n)
			}
		}
	}
	return out, nil
}

// Principal is an authenticated identity and what it may do.
type Principal struct {
	// Name is how audit records name it: token:<name>, cert:<common name>,
	// apikey:<id> or oidc:<username>.
	Name string
	// Method is the credential kind that authenticated it.
	Method string
	Scopes map[Scope]bool
	// Roles are the roles its scopes came from, beside any it holds directly.
	Roles []string
	// Groups are an identity provider's groups, for an OIDC identity.
	Groups []string
	// Expires is when the credential stops authenticating; zero for none.
	Expires time.Time
}

// ScopeList is the principal's scopes, sorted.
func (p Principal) ScopeList() []string {
	out := make([]string, 0, len(p.Scopes))
	for s := range p.Scopes {
		out = append(out, string(s))
	}
	sort.Strings(out)
	return out
}

// Allowed is the authorization decision: a route declaring no scope needs
// only an authenticated principal; any other needs its scope in the set.
func Allowed(p Principal, need Scope) bool {
	if need == "" {
		return p.Name != ""
	}
	return p.Scopes[need]
}

// Verdict is an authenticator's answer about one request.
type Verdict int

// Verdicts. NotMine lets the next authenticator try.
const (
	NotMine Verdict = iota
	Authenticated
	Refused
)

// Authenticator maps a request's credential to a principal. A refusal
// carries a short reason for the audit record, never the credential.
type Authenticator interface {
	Authenticate(r *http.Request) (Principal, Verdict, string)
}

// Authenticate runs the chain in order: the first authenticator that
// authenticates or refuses decides; a request none claims is refused.
func Authenticate(chain []Authenticator, r *http.Request) (Principal, bool, string) {
	for _, a := range chain {
		p, v, reason := a.Authenticate(r)
		switch v {
		case Authenticated:
			return p, true, ""
		case Refused:
			return Principal{}, false, reason
		}
	}
	return Principal{}, false, ""
}

// Roles are the configured roles by name, each expanded to its scopes.
type Roles map[string]map[Scope]bool

// NewRoles expands the configured roles.
func NewRoles(defs []config.AdminRole) (Roles, error) {
	out := Roles{}
	for _, d := range defs {
		scopes, err := Expand(d.Scopes)
		if err != nil {
			return nil, fmt.Errorf("admin role %q: %w", d.Name, err)
		}
		out[d.Name] = scopes
	}
	return out, nil
}

// Grant is the scope set of an identity holding scopes directly and the
// named roles: the union, each expanded once.
func (r Roles) Grant(scopes, roles []string) (map[Scope]bool, error) {
	out, err := Expand(scopes)
	if err != nil {
		return nil, err
	}
	for _, name := range roles {
		rs, ok := r[name]
		if !ok {
			return nil, fmt.Errorf("no role named %q", name)
		}
		for s := range rs {
			out[s] = true
		}
	}
	return out, nil
}

// tokenEntry is one configured bearer identity: the token's digest, never
// the token.
type tokenEntry struct {
	digest    [sha256.Size]byte
	principal Principal
}

// matchToken decides which configured identity a presented bearer value is.
// It compares the value's digest with every entry's in constant time and
// visits all of them whatever matches.
func matchToken(presented []byte, entries []tokenEntry) (Principal, bool) {
	d := sha256.Sum256(presented)
	found := -1
	for i := range entries {
		if subtle.ConstantTimeCompare(d[:], entries[i].digest[:]) == 1 && found < 0 {
			found = i
		}
	}
	if found < 0 {
		return Principal{}, false
	}
	return entries[found].principal, true
}

// bearer extracts the value of an Authorization: Bearer header.
func bearer(r *http.Request) ([]byte, bool) {
	h := r.Header.Get("Authorization")
	scheme, value, ok := strings.Cut(h, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return nil, false
	}
	value = strings.TrimSpace(value)
	return []byte(value), value != ""
}

type tokenAuthenticator struct{ entries []tokenEntry }

func (a tokenAuthenticator) Authenticate(r *http.Request) (Principal, Verdict, string) {
	value, ok := bearer(r)
	if !ok {
		return Principal{}, NotMine, ""
	}
	if p, ok := matchToken(value, a.entries); ok {
		return p, Authenticated, ""
	}
	return Principal{}, NotMine, ""
}

// matchCertificate decides which configured identity a client certificate
// is. Only a chain the TLS layer verified counts: a certificate that was
// merely presented is not an identity.
func matchCertificate(verified [][]*x509.Certificate, byCN map[string]Principal) (Principal, bool) {
	if len(verified) == 0 || len(verified[0]) == 0 {
		return Principal{}, false
	}
	p, ok := byCN[verified[0][0].Subject.CommonName]
	return p, ok
}

type certificateAuthenticator struct{ byCN map[string]Principal }

func (a certificateAuthenticator) Authenticate(r *http.Request) (Principal, Verdict, string) {
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		return Principal{}, NotMine, ""
	}
	if p, ok := matchCertificate(r.TLS.VerifiedChains, a.byCN); ok {
		return p, Authenticated, ""
	}
	// A certificate the operator configured no identity for: the client
	// chose to present it, so a bearer token does not stand in for it.
	return Principal{}, Refused, "certificate: no identity"
}

// Authenticators builds the configured part of the chain: certificates
// first, then OIDC ID tokens, then bearer tokens, whose files are read here.
// Nothing contacts the identity provider. The server adds the API-key
// authenticator after the certificates.
func Authenticators(cfg config.Admin, roles Roles, log *slog.Logger) ([]Authenticator, error) {
	var chain []Authenticator
	if len(cfg.Certificates) > 0 {
		byCN := map[string]Principal{}
		for _, c := range cfg.Certificates {
			scopes, err := roles.Grant(c.Scopes, c.Roles)
			if err != nil {
				return nil, fmt.Errorf("admin certificate %q: %w", c.CommonName, err)
			}
			byCN[c.CommonName] = Principal{Name: "cert:" + c.CommonName, Method: "certificate", Scopes: scopes, Roles: c.Roles}
		}
		chain = append(chain, certificateAuthenticator{byCN: byCN})
	}
	if cfg.OIDC != nil {
		a, err := newOIDCAuthenticator(*cfg.OIDC, roles, log)
		if err != nil {
			return nil, err
		}
		chain = append(chain, a)
	}
	if len(cfg.Tokens) > 0 {
		var entries []tokenEntry
		for _, t := range cfg.Tokens {
			value, err := config.ReadAdminToken(t.TokenFile)
			if err != nil {
				return nil, fmt.Errorf("admin token %q: %w", t.Name, err)
			}
			if len(value) < config.MinAdminTokenBytes {
				return nil, fmt.Errorf("admin token %q: shorter than %d bytes", t.Name, config.MinAdminTokenBytes)
			}
			scopes, err := roles.Grant(t.Scopes, t.Roles)
			if err != nil {
				return nil, fmt.Errorf("admin token %q: %w", t.Name, err)
			}
			entries = append(entries, tokenEntry{digest: sha256.Sum256(value), principal: Principal{Name: "token:" + t.Name, Method: "token", Scopes: scopes, Roles: t.Roles}})
		}
		chain = append(chain, tokenAuthenticator{entries: entries})
	}
	return chain, nil
}
