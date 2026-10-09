// Package oidc is the OpenID Connect layer over package jose: provider
// discovery, the admin API's key source located by discovery, and the client
// half of the authorization-code flow the CLI logs in with. It uses only the
// standard library.
package oidc

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/happydave/hoardmq/internal/jose"
)

// maxDocumentBytes bounds a token endpoint's answer.
const maxDocumentBytes = 1 << 20

// Metadata is the part of a discovery document this package reads.
type Metadata struct {
	Issuer                string `json:"issuer"`
	JWKSURI               string `json:"jwks_uri"`
	AuthorizationEndpoint string `json:"authorization_endpoint"`
	TokenEndpoint         string `json:"token_endpoint"`
}

// Discover reads the issuer's discovery document. Its issuer must equal the
// one asked for exactly (OpenID Connect Discovery § 4.3), and every endpoint
// it names must pass jose.EndpointOK.
func Discover(ctx context.Context, c *http.Client, issuer string) (Metadata, error) {
	data, err := jose.Fetch(ctx, c, strings.TrimSuffix(issuer, "/")+"/.well-known/openid-configuration")
	if err != nil {
		return Metadata{}, err
	}
	var m Metadata
	if err := json.Unmarshal(data, &m); err != nil {
		return Metadata{}, fmt.Errorf("discovery document: %w", err)
	}
	if m.Issuer != issuer {
		return Metadata{}, fmt.Errorf("discovery document names issuer %q, not %q", m.Issuer, issuer)
	}
	for name, u := range map[string]string{"jwks_uri": m.JWKSURI, "authorization_endpoint": m.AuthorizationEndpoint, "token_endpoint": m.TokenEndpoint} {
		if u == "" && name != "jwks_uri" {
			continue
		}
		if msg := jose.EndpointOK(u); msg != "" {
			return Metadata{}, fmt.Errorf("discovery document: %s %q is %s", name, u, msg)
		}
	}
	return m, nil
}

// NewKeySource returns a key source for the issuer's ID tokens: the JWKS its
// discovery document names, discovered afresh at each fetch. It contacts
// nothing.
func NewKeySource(issuer string, c *http.Client, log *slog.Logger) *jose.KeySource {
	return jose.NewLocatedKeySource(issuer, func(ctx context.Context) (string, error) {
		m, err := Discover(ctx, c, issuer)
		if err != nil {
			return "", err
		}
		return m.JWKSURI, nil
	}, c, log)
}
