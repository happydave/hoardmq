package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/happydave/hoardmq/internal/jose"
)

// JWTAuthenticator authenticates a client by an access token in its password
// (or username) field: verified against a JWKS, or introspected (RFC 7662).
// It claims a compact JWS whose unverified issuer is the configured one, and,
// only when OpaqueTokens is set, a value that is not a JWS; any other value
// passes, and so is never sent to an introspection endpoint.
type JWTAuthenticator struct {
	Issuer    string
	Audiences []string
	// FromUsername reads the token from the username field.
	FromUsername  bool
	UsernameClaim string
	// ClientIDClaim, when set, must equal the client identifier.
	ClientIDClaim string
	Skew          time.Duration
	MaxLifetime   time.Duration
	// FetchWait bounds how long a CONNECT waits for the key set.
	FetchWait time.Duration
	// Exactly one of Keys and Introspection is set.
	Keys          *jose.KeySource
	Introspection *Introspector
	// OpaqueTokens claims a value that is not a JWS for introspection; with
	// OpaquePrefix set, only one that starts with it.
	OpaqueTokens bool
	OpaquePrefix string
	Now          func() time.Time
}

// Authenticate implements Authenticator.
func (a *JWTAuthenticator) Authenticate(ctx context.Context, req *Request) Result {
	parsed, token, ok := a.claim(req)
	if !ok {
		return Result{Decision: Pass}
	}
	now := a.Now()
	if a.Keys != nil {
		wctx, cancel := context.WithTimeout(ctx, a.FetchWait)
		keys, err := a.Keys.Keys(wctx, parsed.Header.Kid)
		cancel()
		if err != nil {
			return Result{Decision: Deny, Reason: "jwt: provider unavailable"}
		}
		return a.decideJWS(token, keys, req, now)
	}
	answer, err := a.Introspection.Introspect(ctx, token)
	if err != nil {
		return Result{Decision: Deny, Reason: "jwt: provider unavailable"}
	}
	return a.decideIntrospection(answer, req, now)
}

// claim is the claim rule: claimJWT's, and in introspection mode with
// OpaqueTokens a value that is not a JWS, with the prefix when one is set. A
// JWS naming another issuer is never claimed.
func (a *JWTAuthenticator) claim(req *Request) (jose.Parsed, string, bool) {
	if parsed, token, ok := claimJWT(req, a.FromUsername, a.Issuer); ok {
		return parsed, token, true
	}
	if a.Introspection == nil || !a.OpaqueTokens {
		return jose.Parsed{}, "", false
	}
	token, present := fieldValue(req, a.FromUsername)
	if !present || token == "" || !strings.HasPrefix(token, a.OpaquePrefix) {
		return jose.Parsed{}, "", false
	}
	if _, err := jose.Parse(token); err == nil {
		return jose.Parsed{}, "", false // a JWS for another issuer
	}
	return jose.Parsed{}, token, true
}

// fieldValue is the configured field's value, and whether the client sent it.
func fieldValue(req *Request, fromUsername bool) (string, bool) {
	switch {
	case fromUsername && req.HasUsername:
		return req.Username, true
	case !fromUsername && req.HasPassword:
		return string(req.Password), true
	}
	return "", false
}

// claimJWT is the claim rule for a JWS: the configured field holds a compact
// JWS whose unverified issuer is the configured one.
func claimJWT(req *Request, fromUsername bool, issuer string) (jose.Parsed, string, bool) {
	token, present := fieldValue(req, fromUsername)
	if !present {
		return jose.Parsed{}, "", false
	}
	parsed, err := jose.Parse(token)
	if err != nil {
		return jose.Parsed{}, "", false
	}
	if iss, _ := parsed.Claims.String("iss"); iss != issuer {
		return jose.Parsed{}, "", false
	}
	return parsed, token, true
}

func (a *JWTAuthenticator) policy() jose.Policy {
	return jose.Policy{Issuer: a.Issuer, Audiences: a.Audiences, Skew: a.Skew, MaxLifetime: a.MaxLifetime,
		AcceptAccessTokens: true, IgnoreAZP: true}
}

// decideJWS is the decision on a token and the key set in hand: the
// verification, then the identity.
func (a *JWTAuthenticator) decideJWS(token string, keys jose.KeySet, req *Request, now time.Time) Result {
	claims, err := jose.Verify(token, keys, a.policy(), now)
	if err != nil {
		return Result{Decision: Deny, Reason: "jwt: " + err.Error()}
	}
	return a.identity(claims, req)
}

// decideIntrospection is the decision on an introspection answer: active,
// unexpired, for one of the audiences and from the issuer when it says so,
// then the identity.
func (a *JWTAuthenticator) decideIntrospection(c jose.Claims, req *Request, now time.Time) Result {
	if active, _ := c["active"].(bool); !active {
		return Result{Decision: Deny, Reason: "jwt: token inactive"}
	}
	if _, present := c["exp"]; present {
		exp, ok := c.Time("exp")
		if !ok || now.Unix() >= exp+int64(a.Skew/time.Second) {
			return Result{Decision: Deny, Reason: "jwt: token expired"}
		}
	}
	if _, present := c["aud"]; present {
		aud, ok := c.Strings("aud")
		if !ok || !slices.ContainsFunc(aud, func(x string) bool { return slices.Contains(a.Audiences, x) }) {
			return Result{Decision: Deny, Reason: "jwt: audience mismatch"}
		}
	}
	if _, present := c["iss"]; present {
		if iss, _ := c.String("iss"); iss != a.Issuer {
			return Result{Decision: Deny, Reason: "jwt: issuer mismatch"}
		}
	}
	return a.identity(c, req)
}

// identity maps verified claims to the client: the username claim names it,
// and must equal a username the client sent beside a password token; the
// client-identifier claim, when configured, must equal the client
// identifier. The decision lapses at exp plus the skew.
func (a *JWTAuthenticator) identity(c jose.Claims, req *Request) Result {
	user, _ := c.String(a.UsernameClaim)
	if user == "" {
		return Result{Decision: Deny, Reason: "jwt: no " + a.UsernameClaim + " claim"}
	}
	if !a.FromUsername && req.HasUsername && req.Username != user {
		return Result{Decision: Deny, Reason: "jwt: the username is not the token's " + a.UsernameClaim}
	}
	if a.ClientIDClaim != "" {
		if cid, _ := c.String(a.ClientIDClaim); cid == "" || cid != req.ClientID {
			return Result{Decision: Deny, Reason: "jwt: the client identifier is not the token's " + a.ClientIDClaim}
		}
	}
	r := Result{Decision: Allow, Username: user}
	if exp, ok := c.Time("exp"); ok {
		r.Expires = time.Unix(exp, 0).Add(a.Skew)
	}
	return r
}

// Introspector asks an RFC 7662 endpoint about a token, authenticating with
// HTTP Basic.
type Introspector struct {
	Endpoint string
	ClientID string
	Secret   string
	Timeout  time.Duration
	Client   *http.Client
	// OnResult, when set, is told each introspection's outcome: active,
	// inactive or failed.
	OnResult func(outcome string)
}

// maxIntrospectionBytes bounds an introspection answer.
const maxIntrospectionBytes = 64 << 10

// MaxIntrospectionConns bounds the connections open to the endpoint.
const MaxIntrospectionConns = 64

// Introspect posts the token and returns the answer's members.
func (in *Introspector) Introspect(ctx context.Context, token string) (jose.Claims, error) {
	c, err := in.introspect(ctx, token)
	if in.OnResult != nil {
		switch {
		case err != nil:
			in.OnResult("failed")
		case c["active"] == true:
			in.OnResult("active")
		default:
			in.OnResult("inactive")
		}
	}
	return c, err
}

func (in *Introspector) introspect(ctx context.Context, token string) (jose.Claims, error) {
	ctx, cancel := context.WithTimeout(ctx, in.Timeout)
	defer cancel()
	form := url.Values{"token": {token}, "token_type_hint": {"access_token"}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, in.Endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	// RFC 6749 § 2.3.1: the client identifier and secret are form-encoded
	// before they are joined.
	req.SetBasicAuth(url.QueryEscape(in.ClientID), url.QueryEscape(in.Secret))
	resp, err := in.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("introspection: %s", resp.Status)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxIntrospectionBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxIntrospectionBytes {
		return nil, errors.New("introspection: answer larger than 64 KiB")
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var c jose.Claims
	if err := dec.Decode(&c); err != nil || c == nil {
		return nil, errors.New("introspection: answer is not a JSON object")
	}
	if _, ok := c["active"].(bool); !ok {
		return nil, errors.New("introspection: answer has no active member")
	}
	return c, nil
}
