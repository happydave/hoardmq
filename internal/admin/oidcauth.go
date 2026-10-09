package admin

import (
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"sort"
	"time"

	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/jose"
	"github.com/happydave/hoardmq/internal/oidc"
)

// idpMapping is how an identity provider's claims become a principal.
type idpMapping struct {
	usernameClaim string
	groupsClaim   string
	// byGroup is the roles each group grants.
	byGroup map[string][]string
	roles   Roles
}

// oidcIdentity is the decision on a verified token's claims: the username
// claim names the identity, the groups claim its groups, and the roles every
// mapping of those groups grants; an identity granted no role is refused.
func oidcIdentity(c jose.Claims, m idpMapping) (Principal, string) {
	user, _ := c.String(m.usernameClaim)
	if user == "" {
		return Principal{}, "oidc: no " + m.usernameClaim + " claim"
	}
	groups, ok := c.Strings(m.groupsClaim)
	if !ok {
		return Principal{}, "oidc: the " + m.groupsClaim + " claim is not a list of names"
	}
	var roles []string
	for _, g := range groups {
		for _, r := range m.byGroup[g] {
			if !slices.Contains(roles, r) {
				roles = append(roles, r)
			}
		}
	}
	sort.Strings(roles)
	scopes, err := m.roles.Grant(nil, roles)
	if err != nil {
		return Principal{}, "oidc: " + err.Error()
	}
	if len(scopes) == 0 {
		return Principal{}, "oidc: no role mapped"
	}
	p := Principal{Name: "oidc:" + user, Method: "oidc", Scopes: scopes, Roles: roles, Groups: groups}
	if exp, ok := c.Time("exp"); ok {
		p.Expires = time.Unix(exp, 0).UTC()
	}
	return p, ""
}

// decideOIDC is the whole decision on a presented ID token, from the key set
// already in hand: verification, then the identity.
func decideOIDC(token string, keys jose.KeySet, policy jose.Policy, m idpMapping, now time.Time) (Principal, string) {
	claims, err := jose.Verify(token, keys, policy, now)
	if err != nil {
		return Principal{}, "oidc: " + err.Error()
	}
	return oidcIdentity(claims, m)
}

// oidcAuthenticator claims a bearer value that is a compact JWS whose
// unverified issuer is the configured one; any other value is not its. It
// fetches the issuer's keys (the source's rule) and calls decideOIDC.
type oidcAuthenticator struct {
	policy  jose.Policy
	mapping idpMapping
	keys    *jose.KeySource
	now     func() time.Time
}

func newOIDCAuthenticator(cfg config.AdminOIDC, roles Roles, log *slog.Logger) (*oidcAuthenticator, error) {
	client, err := jose.HTTPClient(cfg.CAFile)
	if err != nil {
		return nil, err
	}
	byGroup := map[string][]string{}
	for _, m := range cfg.RoleMappings {
		for _, r := range m.Roles {
			if _, ok := roles[r]; !ok {
				return nil, errors.New("admin oidc: no role named " + r)
			}
		}
		byGroup[m.Group] = append(byGroup[m.Group], m.Roles...)
	}
	return &oidcAuthenticator{
		policy: jose.Policy{Issuer: cfg.Issuer, Audiences: cfg.ClientIDs, Skew: time.Duration(cfg.Skew()) * time.Second,
			MaxLifetime: time.Duration(cfg.MaxLifetime()) * time.Second},
		mapping: idpMapping{usernameClaim: cfg.Username(), groupsClaim: cfg.Groups(), byGroup: byGroup, roles: roles},
		keys:    oidc.NewKeySource(cfg.Issuer, client, log),
		now:     time.Now,
	}, nil
}

func (a *oidcAuthenticator) Authenticate(r *http.Request) (Principal, Verdict, string) {
	value, ok := bearer(r)
	if !ok {
		return Principal{}, NotMine, ""
	}
	token := string(value)
	parsed, err := jose.Parse(token)
	if err != nil {
		return Principal{}, NotMine, ""
	}
	if iss, _ := parsed.Claims.String("iss"); iss != a.policy.Issuer {
		return Principal{}, NotMine, ""
	}
	keys, err := a.keys.Keys(r.Context(), parsed.Header.Kid)
	if err != nil {
		return Principal{}, Refused, "oidc: " + err.Error()
	}
	p, reason := decideOIDC(token, keys, a.policy, a.mapping, a.now())
	if reason != "" {
		return Principal{}, Refused, reason
	}
	return p, Authenticated, ""
}
