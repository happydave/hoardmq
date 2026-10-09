// Package auth holds what an authentication or authorization module is asked
// and answers: the connection's request, the decision with its attributes, the
// identity an authorizer sees, and the enhanced-authentication mechanism.
// Chains are ordered; the first member that decides wins; a chain whose
// members all pass leaves the decision to the broker, whose default is deny.
package auth

import (
	"context"
	"crypto/x509"
	"time"
)

// Decision is an authenticator's or authorizer's answer.
type Decision int

// Decisions.
const (
	Pass Decision = iota
	Allow
	Deny
)

func (d Decision) String() string {
	switch d {
	case Allow:
		return "allow"
	case Deny:
		return "deny"
	}
	return "pass"
}

// Request is what a connection presents for authentication.
type Request struct {
	Namespace   string
	ClientID    string
	Username    string
	HasUsername bool
	Password    []byte
	HasPassword bool
	PeerAddr    string
	// CertCN is the subject common name of a verified TLS client certificate,
	// or "" when none was presented.
	CertCN string
	// CertChain is the verified chain, leaf first, or nil.
	CertChain []*x509.Certificate
	Listener  string
}

// HasCredentials reports whether the request carries anything to check.
func (r *Request) HasCredentials() bool { return r.HasUsername || r.HasPassword || r.CertCN != "" }

// Result is an authenticator's answer with optional attributes that attach to
// the connection's identity.
type Result struct {
	Decision   Decision
	Attributes map[string]string
	// Username, set on an allow, is the identity's username for
	// authorization, replacing the one the client sent.
	Username string
	// Expires is when the decision lapses and must be made again; zero is
	// never, before the broker's own bound.
	Expires time.Time
	// Reason says why a deny refused, for the audit log; it never holds a
	// credential.
	Reason string
}

// Authenticator decides whether a connection may proceed.
type Authenticator interface {
	Authenticate(ctx context.Context, req *Request) Result
}

// Identity is the authenticated principal an authorizer sees.
type Identity struct {
	Namespace  string
	ClientID   string
	Username   string
	CertCN     string
	Attributes map[string]string
}

// Action is what an authorizer is asked about.
type Action int

// Actions.
const (
	Publish Action = iota
	Subscribe
	Unsubscribe
)

func (a Action) String() string {
	switch a {
	case Publish:
		return "publish"
	case Subscribe:
		return "subscribe"
	}
	return "unsubscribe"
}

// Authorizer decides whether an identity may act on a topic or filter.
type Authorizer interface {
	Authorize(ctx context.Context, action Action, topic string, id *Identity) Decision
}

// CertificateAuthenticator is an authenticator that decides on the client
// certificate. The connection runs every one before any other authenticator
// and before an enhanced-authentication mechanism, so a certificate it refuses
// is refused whatever else the client presents.
type CertificateAuthenticator interface {
	Authenticator
	ChecksCertificates()
}
