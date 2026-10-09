package auth

import (
	"context"
	"crypto/rsa"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/jose"
	"github.com/happydave/hoardmq/internal/oidc/oidctest"
)

// jwtNow is the fixed time the JWT cases are decided at.
var jwtNow = time.Unix(1_800_000_000, 0)

func jwtFixture(t *testing.T) (*oidctest.Provider, *JWTAuthenticator) {
	t.Helper()
	p := oidctest.New(t, "hoardmq")
	p.Now = func() time.Time { return jwtNow }
	c, err := jose.HTTPClient("")
	if err != nil {
		t.Fatal(err)
	}
	keys := jose.NewKeySource(p.URL+"/keys", c, nil)
	keys.Now = func() time.Time { return jwtNow }
	return p, &JWTAuthenticator{Issuer: p.URL, Audiences: []string{"hoardmq"}, UsernameClaim: "sub",
		Skew: 60 * time.Second, MaxLifetime: 86400 * time.Second, FetchWait: 2 * time.Second, Keys: keys,
		Now: func() time.Time { return jwtNow }}
}

// deviceClaims are an access token for dev-1: an hour to live.
func deviceClaims(p *oidctest.Provider) map[string]any {
	return map[string]any{"iss": p.URL, "sub": "dev-1", "aud": "hoardmq", "iat": jwtNow.Unix(),
		"exp": jwtNow.Add(time.Hour).Unix(), "client_id": "fleet", "scope": "mqtt"}
}

func accessToken(t *testing.T, p *oidctest.Provider, change func(map[string]any)) string {
	t.Helper()
	c := deviceClaims(p)
	if change != nil {
		change(c)
	}
	return p.SignWith(t, "k1-RS256", map[string]any{"typ": "at+jwt"}, c)
}

func tokenRequest(user string, token string) *Request {
	r := &Request{ClientID: "c-1", HasPassword: true, Password: []byte(token)}
	if user != "" {
		r.HasUsername, r.Username = true, user
	}
	return r
}

// The JWKS mode, one clause at a time from a token that passes.
func TestJWTAuthenticatorJWKS(t *testing.T) {
	p, a := jwtFixture(t)
	ctx := context.Background()
	sec := func(d time.Duration) int64 { return jwtNow.Add(d).Unix() }
	good := accessToken(t, p, nil)
	rsPub := p.Public("k1-RS256").(*rsa.PublicKey)
	cases := []struct {
		name string
		req  *Request
		want Decision
		why  string // a substring of the reason, for a deny
		user string // the Result's username, for an allow
	}{
		{"an access token beside its username", tokenRequest("dev-1", good), Allow, "", "dev-1"},
		{"an access token without a username", tokenRequest("", good), Allow, "", "dev-1"},
		{"another username", tokenRequest("dev-2", good), Deny, "username", ""},
		{"expired 30 s ago, inside the skew", tokenRequest("dev-1", accessToken(t, p, func(c map[string]any) { c["exp"] = sec(-30 * time.Second); c["iat"] = sec(-time.Hour) })), Allow, "", "dev-1"},
		{"expired 90 s ago", tokenRequest("dev-1", accessToken(t, p, func(c map[string]any) { c["exp"] = sec(-90 * time.Second); c["iat"] = sec(-time.Hour) })), Deny, "token expired", ""},
		{"another audience", tokenRequest("dev-1", accessToken(t, p, func(c map[string]any) { c["aud"] = "other" })), Deny, "audience", ""},
		{"an audience array holding ours", tokenRequest("dev-1", accessToken(t, p, func(c map[string]any) { c["aud"] = []string{"other", "hoardmq"} })), Allow, "", "dev-1"},
		{"a foreign azp", tokenRequest("dev-1", accessToken(t, p, func(c map[string]any) { c["azp"] = "someone-else" })), Allow, "", "dev-1"},
		{"another issuer", tokenRequest("dev-1", accessToken(t, p, func(c map[string]any) { c["iss"] = "https://other.example" })), Pass, "", ""},
		{"the issuer with a trailing slash", tokenRequest("dev-1", accessToken(t, p, func(c map[string]any) { c["iss"] = p.URL + "/" })), Pass, "", ""},
		{"a password", tokenRequest("dev-1", "hunter2"), Pass, "", ""},
		{"alg none", tokenRequest("dev-1", noneJWT(t, deviceClaims(p))), Deny, "algorithm none", ""},
		{"HS256 keyed with the RSA modulus", tokenRequest("dev-1", oidctest.SignHMAC(t, map[string]any{"alg": "HS256", "kid": "k1-RS256"}, deviceClaims(p), rsPub.N.Bytes())), Deny, "HS256", ""},
		{"no sub", tokenRequest("", accessToken(t, p, func(c map[string]any) { delete(c, "sub") })), Deny, "no sub", ""},
	}
	for _, c := range cases {
		r := a.Authenticate(ctx, c.req)
		if r.Decision != c.want || c.want == Deny && !strings.Contains(r.Reason, c.why) || c.want == Allow && r.Username != c.user {
			t.Errorf("%s: %+v, want %v %q %q", c.name, r, c.want, c.why, c.user)
		}
	}
	if r := a.Authenticate(ctx, tokenRequest("dev-1", good)); !r.Expires.Equal(time.Unix(sec(time.Hour), 0).Add(time.Minute)) {
		t.Errorf("the decision's expiry %v, want exp plus the skew", r.Expires)
	}
	if strings.Contains(a.Authenticate(ctx, tokenRequest("dev-2", good)).Reason, good) {
		t.Error("a reason carries the token")
	}
}

func noneJWT(t *testing.T, claims map[string]any) string {
	t.Helper()
	return oidctest.SignHMAC(t, map[string]any{"alg": "none", "kid": "k1-RS256"}, claims, nil)
}

// The client-identifier claim binds a token to its client.
func TestJWTClientIDClaim(t *testing.T) {
	p, a := jwtFixture(t)
	a.ClientIDClaim = "cid"
	with := accessToken(t, p, func(c map[string]any) { c["cid"] = "c-1" })
	without := accessToken(t, p, nil)
	ctx := context.Background()
	if r := a.Authenticate(ctx, tokenRequest("dev-1", with)); r.Decision != Allow {
		t.Errorf("cid c-1 from c-1: %+v", r)
	}
	other := tokenRequest("dev-1", with)
	other.ClientID = "c-2"
	if r := a.Authenticate(ctx, other); r.Decision != Deny {
		t.Errorf("cid c-1 from c-2: %+v", r)
	}
	if r := a.Authenticate(ctx, tokenRequest("dev-1", without)); r.Decision != Deny {
		t.Errorf("no cid: %+v", r)
	}
}

// A token in the username field.
func TestJWTFromUsername(t *testing.T) {
	p, a := jwtFixture(t)
	a.FromUsername = true
	tok := accessToken(t, p, nil)
	ctx := context.Background()
	if r := a.Authenticate(ctx, &Request{HasUsername: true, Username: tok}); r.Decision != Allow || r.Username != "dev-1" {
		t.Errorf("the token as the username: %+v", r)
	}
	if r := a.Authenticate(ctx, tokenRequest("dev-1", tok)); r.Decision != Pass {
		t.Errorf("the token as the password: %+v", r)
	}
}

// A provider that has never answered refuses its tokens within the wait.
func TestJWTProviderDown(t *testing.T) {
	p, a := jwtFixture(t)
	a.FetchWait = 200 * time.Millisecond
	p.Lock()
	p.Down = true
	p.Unlock()
	start := time.Now()
	r := a.Authenticate(context.Background(), tokenRequest("dev-1", accessToken(t, p, nil)))
	if r.Decision != Deny || r.Reason != "jwt: provider unavailable" {
		t.Fatalf("%+v", r)
	}
	if el := time.Since(start); el > 500*time.Millisecond {
		t.Fatalf("decided after %v", el)
	}
	// A slow provider: the CONNECT waits FetchWait, not the fetch.
	q, b := jwtFixture(t)
	b.FetchWait = 200 * time.Millisecond
	q.Lock()
	q.Delay = time.Second
	q.Unlock()
	start = time.Now()
	if r := b.Authenticate(context.Background(), tokenRequest("dev-1", accessToken(t, q, nil))); r.Decision != Deny || r.Reason != "jwt: provider unavailable" {
		t.Fatalf("a slow provider: %+v", r)
	}
	if el := time.Since(start); el > 500*time.Millisecond {
		t.Fatalf("a slow provider: decided after %v", el)
	}
}

func introspectionFixture(t *testing.T) (*oidctest.Provider, *JWTAuthenticator) {
	t.Helper()
	p, a := jwtFixture(t)
	p.Lock()
	p.IntrospectClientID, p.IntrospectSecret = "hoardmq-broker", "s3cret: with/odd&chars"
	p.Unlock()
	a.Keys = nil
	a.Introspection = &Introspector{Endpoint: p.URL + "/introspect", ClientID: "hoardmq-broker", Secret: "s3cret: with/odd&chars",
		Timeout: 2 * time.Second, Client: &http.Client{}}
	return p, a
}

func TestJWTIntrospection(t *testing.T) {
	p, a := introspectionFixture(t)
	ctx := context.Background()
	tok := accessToken(t, p, nil)
	if r := a.Authenticate(ctx, tokenRequest("", tok)); r.Decision != Allow || r.Username != "dev-1" {
		t.Fatalf("an active token: %+v", r)
	}
	for _, c := range []struct {
		name   string
		answer map[string]any
		why    string
	}{
		{"an answer expired 90 s ago", map[string]any{"exp": jwtNow.Add(-90 * time.Second).Unix()}, "token expired"},
		{"an answer for another audience", map[string]any{"aud": "other"}, "audience"},
		{"an answer from another issuer", map[string]any{"iss": "https://other.example"}, "issuer"},
		{"an answer without sub", map[string]any{"sub": nil}, "no sub"},
	} {
		p.Lock()
		p.Answer = c.answer
		p.Unlock()
		if r := a.Authenticate(ctx, tokenRequest("", tok)); r.Decision != Deny || !strings.Contains(r.Reason, c.why) {
			t.Errorf("%s: %+v", c.name, r)
		}
	}
	p.Lock()
	p.Answer = map[string]any{"exp": jwtNow.Add(-30 * time.Second).Unix()}
	p.Unlock()
	if r := a.Authenticate(ctx, tokenRequest("", tok)); r.Decision != Allow {
		t.Errorf("an answer expired 30 s ago, inside the skew: %+v", r)
	}
	p.Lock()
	p.Answer = nil
	p.Unlock()
	p.Revoke(tok)
	if r := a.Authenticate(ctx, tokenRequest("", tok)); r.Decision != Deny || r.Reason != "jwt: token inactive" {
		t.Errorf("a revoked token: %+v", r)
	}
	before := p.Count("introspect")
	if r := a.Authenticate(ctx, tokenRequest("dev-1", "hunter2")); r.Decision != Pass {
		t.Errorf("a password: %+v", r)
	}
	if p.Count("introspect") != before {
		t.Error("a password was sent to the introspection endpoint")
	}
	a.Introspection.Secret = "wrong"
	if r := a.Authenticate(ctx, tokenRequest("", accessToken(t, p, nil))); r.Decision != Deny || r.Reason != "jwt: provider unavailable" {
		t.Errorf("a wrong client secret: %+v", r)
	}
}

func TestJWTIntrospectionTimeout(t *testing.T) {
	p, a := introspectionFixture(t)
	a.Introspection.Timeout = 300 * time.Millisecond
	p.Lock()
	p.Delay = 3 * time.Second
	p.Unlock()
	start := time.Now()
	if r := a.Authenticate(context.Background(), tokenRequest("", accessToken(t, p, nil))); r.Decision != Deny {
		t.Fatalf("%+v", r)
	}
	if el := time.Since(start); el > 800*time.Millisecond {
		t.Fatalf("decided after %v", el)
	}
}

// The fixture's own contract: client authentication, and inactive for what it
// did not sign.
func TestIntrospectionFixture(t *testing.T) {
	p, _ := introspectionFixture(t)
	post := func(id, secret, token string) (int, string) {
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, p.URL+"/introspect", strings.NewReader(url.Values{"token": {token}}.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.SetBasicAuth(url.QueryEscape(id), url.QueryEscape(secret))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	if code, _ := post("hoardmq-broker", "wrong", accessToken(t, p, nil)); code != 401 {
		t.Errorf("a wrong secret: %d", code)
	}
	if code, body := post("hoardmq-broker", "s3cret: with/odd&chars", "not-signed-here"); code != 200 || !strings.Contains(body, `"active":false`) {
		t.Errorf("a token it did not sign: %d %s", code, body)
	}
}

// The per-CONNECT cost of a JWKS-mode token with the key set held, at the
// machine's GOMAXPROCS in parallel.
func BenchmarkJWTConnectDecision(b *testing.B) {
	p := oidctest.New(b, "hoardmq")
	c, _ := jose.HTTPClient("")
	keys := jose.NewKeySource(p.URL+"/keys", c, nil)
	a := &JWTAuthenticator{Issuer: p.URL, Audiences: []string{"hoardmq"}, UsernameClaim: "sub", Skew: time.Minute,
		MaxLifetime: 24 * time.Hour, FetchWait: 2 * time.Second, Keys: keys, Now: time.Now}
	now := time.Now()
	tok := p.SignWith(b, "k1-RS256", map[string]any{"typ": "at+jwt"}, map[string]any{"iss": p.URL, "sub": "dev-1", "aud": "hoardmq", "iat": now.Unix(), "exp": now.Add(time.Hour).Unix()})
	req := tokenRequest("dev-1", tok)
	if r := a.Authenticate(context.Background(), req); r.Decision != Allow {
		b.Fatal(r)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if r := a.Authenticate(context.Background(), req); r.Decision != Allow {
				b.Error(r)
				return
			}
		}
	})
}

// Opaque tokens are introspected only when the operator opts in, and then
// only those with the prefix when one is set; a JWS for another issuer is
// never claimed.
func TestJWTOpaqueTokens(t *testing.T) {
	p, a := introspectionFixture(t)
	ctx := context.Background()
	p.Opaque("opq_good", map[string]any{"iss": a.Issuer, "sub": "dev-9", "aud": a.Audiences[0]})
	p.Opaque("opq_revoked", map[string]any{"iss": a.Issuer, "sub": "dev-9"})
	p.Revoke("opq_revoked")
	asked := func() int { return p.Count("introspect") }

	before := asked()
	if r := a.Authenticate(ctx, tokenRequest("", "opq_good")); r.Decision != Pass || asked() != before {
		t.Fatalf("option off: %+v, %d requests", r, asked()-before)
	}

	a.OpaqueTokens = true
	before = asked()
	if r := a.Authenticate(ctx, tokenRequest("dev-1", "")); r.Decision != Pass || asked() != before {
		t.Fatalf("an empty password: %+v, %d requests", r, asked()-before)
	}
	if r := a.Authenticate(ctx, tokenRequest("", "opq_good")); r.Decision != Allow || r.Username != "dev-9" {
		t.Fatalf("an active opaque token: %+v", r)
	}
	if r := a.Authenticate(ctx, tokenRequest("", "opq_revoked")); r.Decision != Deny || r.Reason != "jwt: token inactive" {
		t.Fatalf("an inactive opaque token: %+v", r)
	}
	other := oidctest.SignHMAC(t, map[string]any{"alg": "HS256"}, map[string]any{"iss": "https://other.example"}, []byte("k"))
	before = asked()
	if r := a.Authenticate(ctx, tokenRequest("", other)); r.Decision != Pass || asked() != before {
		t.Fatalf("a JWS for another issuer: %+v, %d requests", r, asked()-before)
	}

	a.OpaquePrefix = "opq_"
	before = asked()
	if r := a.Authenticate(ctx, tokenRequest("dev-1", "hunter2")); r.Decision != Pass || asked() != before {
		t.Fatalf("a password without the prefix: %+v, %d requests", r, asked()-before)
	}
	if r := a.Authenticate(ctx, tokenRequest("", "opq_good")); r.Decision != Allow {
		t.Fatalf("a token with the prefix: %+v", r)
	}
}
