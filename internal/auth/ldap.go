package auth

import (
	"context"
	"fmt"
	"time"

	"github.com/happydave/hoardmq/internal/ldap"
)

// LDAPSearch is the search-then-bind mode: bind as the service account (or
// anonymously when BindDN is empty), find the user's entry under Base, bind
// as its DN.
type LDAPSearch struct {
	Base         string
	Scope        ldap.Scope
	Filter       *ldap.Filter
	BindDN       string
	BindPassword []byte
}

// LDAPAuthenticator binds to a directory as the client: directly by a DN
// template, or after a search.
type LDAPAuthenticator struct {
	Pool   *ldap.Pool
	BindDN ldap.DNTemplate
	Search *LDAPSearch
	// PassOnUnavailable hands an unavailable directory's clients to the next
	// authenticator; otherwise they are refused.
	PassOnUnavailable bool
	Timeout           time.Duration
	Count             func(name string, labels map[string]string)
}

// ldapClaims is the claim rule: a non-empty username and a non-empty
// password. An empty password would be an unauthenticated bind, which a
// directory may answer with success (RFC 4513 § 5.1.2), so it is never sent.
func ldapClaims(req *Request) bool {
	return req.HasUsername && req.Username != "" && req.HasPassword && len(req.Password) > 0
}

// ldapMaySend is the decision to put a password on the wire to e: only
// encrypted, or to this host.
func ldapMaySend(e ldap.Endpoint) bool { return e.Encrypted() || e.Loopback() }

// ldapStages is what the directory said at each step of one authentication.
// A step that failed — a refused service bind, a transport, TLS or protocol
// failure, the deadline — leaves the steps after it unset.
type ldapStages struct {
	searchMode bool
	searched   bool
	searchCode int
	entries    int
	userBound  bool
	bindCode   int
}

// bindAfterSearch is the decision to send the client's password after a
// search: the search ended normally and found exactly one entry.
func bindAfterSearch(st ldapStages) bool {
	return st.searched && (st.searchCode == ldap.Success || st.searchCode == ldap.SizeLimitExceeded) && st.entries == 1
}

// ldapOutcome is the decision on one authentication's stages.
func ldapOutcome(st ldapStages, passOnUnavailable bool) (Result, string) {
	unavailable := func() (Result, string) {
		if passOnUnavailable {
			return Result{Decision: Pass, Reason: "ldap: directory unavailable"}, "unavailable"
		}
		return Result{Decision: Deny, Reason: "ldap: directory unavailable"}, "unavailable"
	}
	switch {
	case st.searchMode && (!st.searched || st.searchCode != ldap.Success && st.searchCode != ldap.SizeLimitExceeded):
		return unavailable()
	case st.searchMode && st.entries == 0:
		return Result{Decision: Pass}, "unknown"
	case st.searchMode && st.entries > 1:
		return Result{Decision: Deny, Reason: "ldap: ambiguous user"}, "refused"
	case !st.userBound:
		return unavailable()
	}
	switch st.bindCode {
	case ldap.Success:
		return Result{Decision: Allow}, "allowed"
	case ldap.InvalidCredentials:
		return Result{Decision: Deny, Reason: "ldap: invalid credentials"}, "refused"
	case ldap.Busy, ldap.Unavailable:
		return unavailable()
	}
	return Result{Decision: Deny, Reason: fmt.Sprintf("ldap: bind refused (%d)", st.bindCode)}, "refused"
}

// Authenticate implements Authenticator.
func (a *LDAPAuthenticator) Authenticate(ctx context.Context, req *Request) Result {
	if !ldapClaims(req) {
		return Result{Decision: Pass}
	}
	ctx, cancel := context.WithTimeout(ctx, a.Timeout)
	defer cancel()
	st := a.run(ctx, req.Username, req.Password)
	res, outcome := ldapOutcome(st, a.PassOnUnavailable)
	a.count("hoardmq_auth_ldap_decisions_total", map[string]string{"outcome": outcome})
	if outcome == "unavailable" {
		policy := "deny"
		if a.PassOnUnavailable {
			policy = "pass"
		}
		a.count("hoardmq_auth_ldap_unavailable_total", map[string]string{"policy": policy})
	}
	return res
}

func (a *LDAPAuthenticator) count(name string, labels map[string]string) {
	if a.Count != nil {
		a.Count(name, labels)
	}
}

// run performs the binds and the search on one pooled connection. Every
// password it sends passes ldapMaySend first.
func (a *LDAPAuthenticator) run(ctx context.Context, username string, password []byte) ldapStages {
	if !ldapMaySend(a.Pool.Endpoint) {
		return ldapStages{}
	}
	var st ldapStages
	_ = a.Pool.Do(ctx, func(c *ldap.Conn) error {
		st = ldapStages{searchMode: a.Search != nil}
		var dn string
		if s := a.Search; s != nil {
			code, err := c.Bind(ctx, s.BindDN, s.BindPassword)
			if err != nil {
				return err
			}
			if code != ldap.Success {
				return nil
			}
			dns, code, err := c.Search(ctx, s.Base, s.Scope, 2, s.Filter.Encode([]byte(username)))
			if err != nil {
				return err
			}
			st.searched, st.searchCode, st.entries = true, code, len(dns)
			if !bindAfterSearch(st) {
				return nil
			}
			dn = dns[0]
		} else {
			dn = a.BindDN.Fill(username)
		}
		code, err := c.Bind(ctx, dn, password)
		if err != nil {
			return err
		}
		st.userBound, st.bindCode = true, code
		return nil
	})
	return st
}
