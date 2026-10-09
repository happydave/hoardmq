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

// AuthenticatorChain runs authenticators in order.
type AuthenticatorChain []Authenticator

// Authenticate returns the first allow or deny, merging attributes from
// passing members along the way; all-pass returns Pass.
func (c AuthenticatorChain) Authenticate(ctx context.Context, req *Request) Result {
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
			return merged
		}
	}
	return merged
}

// AuthorizerChain runs authorizers in order and returns the first decision.
type AuthorizerChain []Authorizer

// Authorize returns the first allow or deny, else Pass.
func (c AuthorizerChain) Authorize(ctx context.Context, action Action, topic string, id *Identity) Decision {
	for _, a := range c {
		if d := a.Authorize(ctx, action, topic, id); d != Pass {
			return d
		}
	}
	return Pass
}
