package oidc

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/happydave/hoardmq/internal/jose"
)

// Random returns n bytes from crypto/rand as unpadded base64url: a state, a
// nonce or a PKCE verifier.
func Random(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}

// Challenge is the PKCE S256 challenge of a verifier (RFC 7636 § 4.2).
func Challenge(verifier string) string {
	sum := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

// Request is one authorization-code login's parameters.
type Request struct {
	ClientID    string
	RedirectURI string
	Scopes      []string
	State       string
	Nonce       string
	Verifier    string
}

// AuthURL is the authorization endpoint URL the user's browser opens.
func AuthURL(m Metadata, r Request) string {
	q := url.Values{
		"response_type":         {"code"},
		"client_id":             {r.ClientID},
		"redirect_uri":          {r.RedirectURI},
		"scope":                 {strings.Join(r.Scopes, " ")},
		"state":                 {r.State},
		"nonce":                 {r.Nonce},
		"code_challenge":        {Challenge(r.Verifier)},
		"code_challenge_method": {"S256"},
	}
	sep := "?"
	if strings.Contains(m.AuthorizationEndpoint, "?") {
		sep = "&"
	}
	return m.AuthorizationEndpoint + sep + q.Encode()
}

// Callback is the decision on the redirect the browser brings back: the
// provider's error, a missing code, or a state other than the one sent
// (compared in constant time) fails the login. It returns the code.
func Callback(q url.Values, state string) (string, error) {
	if e := q.Get("error"); e != "" {
		msg := "the provider refused the login: " + e
		if d := q.Get("error_description"); d != "" {
			msg += ": " + d
		}
		return "", errors.New(msg)
	}
	if subtle.ConstantTimeCompare([]byte(q.Get("state")), []byte(state)) != 1 {
		return "", errors.New("the callback's state is not the one this login sent")
	}
	code := q.Get("code")
	if code == "" {
		return "", errors.New("the callback carries no code")
	}
	return code, nil
}

// Exchange trades a code for the ID token at the token endpoint, as a public
// client proving the PKCE verifier (no client secret).
func Exchange(ctx context.Context, c *http.Client, m Metadata, r Request, code string) (string, error) {
	form := url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {code},
		"redirect_uri":  {r.RedirectURI},
		"client_id":     {r.ClientID},
		"code_verifier": {r.Verifier},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, m.TokenEndpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxDocumentBytes))
	if err != nil {
		return "", err
	}
	var body struct {
		IDToken     string `json:"id_token"`
		Error       string `json:"error"`
		Description string `json:"error_description"`
	}
	_ = json.Unmarshal(data, &body)
	if resp.StatusCode != http.StatusOK {
		if body.Error != "" {
			return "", fmt.Errorf("token endpoint: %s: %s %s", resp.Status, body.Error, body.Description)
		}
		return "", fmt.Errorf("token endpoint: %s", resp.Status)
	}
	if body.IDToken == "" {
		return "", errors.New("token endpoint: no id_token in the answer")
	}
	return body.IDToken, nil
}

// CheckIDToken is the CLI's decision on the token the endpoint returned: its
// issuer, its audience and the nonce this login sent. The signature is the
// admin API's to check; a token read straight from the token endpoint over
// TLS may skip it (OpenID Connect Core § 3.1.3.7, step 6).
func CheckIDToken(token, issuer, clientID, nonce string) (jose.Claims, error) {
	t, err := jose.Parse(token)
	if err != nil {
		return nil, err
	}
	if iss, _ := t.Claims.String("iss"); iss != issuer {
		return nil, errors.New("the ID token's issuer is not " + issuer)
	}
	if aud, ok := t.Claims.Strings("aud"); !ok || !slices.Contains(aud, clientID) {
		return nil, errors.New("the ID token is not for client " + clientID)
	}
	if n, _ := t.Claims.String("nonce"); subtle.ConstantTimeCompare([]byte(n), []byte(nonce)) != 1 {
		return nil, errors.New("the ID token's nonce is not the one this login sent")
	}
	return t.Claims, nil
}
