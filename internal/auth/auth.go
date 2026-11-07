// Package auth defines the authentication and authorization chains and their
// file-backed members. Both chains are ordered; the first member that decides
// wins; a chain whose members all pass leaves the decision to the caller,
// whose default is deny.
package auth

import (
	"context"

	extauth "github.com/happydave/hoardmq/ext/auth"
)

// The request, decision and interfaces are the extension API's.
type (
	Decision                 = extauth.Decision
	Request                  = extauth.Request
	Result                   = extauth.Result
	Authenticator            = extauth.Authenticator
	Identity                 = extauth.Identity
	Action                   = extauth.Action
	Authorizer               = extauth.Authorizer
	CertificateAuthenticator = extauth.CertificateAuthenticator
)

// Decisions and actions.
const (
	Pass        = extauth.Pass
	Allow       = extauth.Allow
	Deny        = extauth.Deny
	Publish     = extauth.Publish
	Subscribe   = extauth.Subscribe
	Unsubscribe = extauth.Unsubscribe
)

// Admitter is a chain member, or a mechanism, that names the module it runs
// for, so the broker can record which module admitted a connection.
type Admitter interface {
	AdmitterID() string
}

// AdmitterOf names the module behind a, or "" when it names none.
func AdmitterOf(a any) string {
	if n, ok := a.(Admitter); ok {
		return n.AdmitterID()
	}
	return ""
}

// AuthenticatorChain runs authenticators in order.
type AuthenticatorChain []Authenticator

// Authenticate returns the first allow or deny, merging attributes from
// passing members along the way; all-pass returns Pass.
func (c AuthenticatorChain) Authenticate(ctx context.Context, req *Request) Result {
	r, _ := c.Admit(ctx, req)
	return r
}

// Admit is Authenticate, also naming the module of the member that allowed,
// or "" when none did.
func (c AuthenticatorChain) Admit(ctx context.Context, req *Request) (Result, string) {
	merged := Result{Decision: Pass}
	for _, a := range c {
		r := a.Authenticate(ctx, req)
		for k, v := range r.Attributes {
			if merged.Attributes == nil {
				merged.Attributes = map[string]string{}
			}
			merged.Attributes[k] = v
		}
		if r.Decision != Pass {
			merged.Decision, merged.Username, merged.Expires, merged.Reason = r.Decision, r.Username, r.Expires, r.Reason
			if r.Decision == Allow {
				return merged, AdmitterOf(a)
			}
			return merged, ""
		}
	}
	return merged, ""
}

// AuthorizerChain runs authorizers in order and returns the first decision.
type AuthorizerChain []Authorizer

// Authorize returns the first allow or deny, else Pass. Each member gets its
// own copy of the identity, so none can change AdmittedBy for the next one or
// for the connection.
func (c AuthorizerChain) Authorize(ctx context.Context, action Action, topic string, id *Identity) Decision {
	for _, a := range c {
		mine := id
		if id != nil {
			cp := *id
			mine = &cp
		}
		if d := a.Authorize(ctx, action, topic, mine); d != Pass {
			return d
		}
	}
	return Pass
}
