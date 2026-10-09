package oidc_test

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/happydave/hoardmq/internal/oidc"
	"github.com/happydave/hoardmq/internal/oidc/oidctest"
)

func TestCallback(t *testing.T) {
	cases := []struct {
		name  string
		query string
		want  string // the code, or a substring of the refusal
		ok    bool
	}{
		{"the state sent", "code=c1&state=s-123", "c1", true},
		{"another state", "code=c1&state=s-124", "state", false},
		{"no state", "code=c1", "state", false},
		{"a provider error", "error=access_denied&error_description=no&state=s-123", "access_denied", false},
		{"no code", "state=s-123", "no code", false},
	}
	for _, c := range cases {
		q, _ := url.ParseQuery(c.query)
		code, err := oidc.Callback(q, "s-123")
		if c.ok != (err == nil) || c.ok && code != c.want || !c.ok && !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %q %v", c.name, code, err)
		}
	}
}

func TestCheckIDToken(t *testing.T) {
	p := oidctest.New(t, "cli")
	claims := p.Claims()
	claims["nonce"] = "n-1"
	tok := p.Mint(t, claims)
	if _, err := oidc.CheckIDToken(tok, p.URL, "cli", "n-1"); err != nil {
		t.Fatalf("the token this login asked for: %v", err)
	}
	for name, args := range map[string][3]string{
		"another nonce":  {p.URL, "cli", "n-2"},
		"another client": {p.URL, "web", "n-1"},
		"another issuer": {p.URL + "/x", "cli", "n-1"},
	} {
		if _, err := oidc.CheckIDToken(tok, args[0], args[1], args[2]); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	delete(claims, "nonce")
	if _, err := oidc.CheckIDToken(p.Mint(t, claims), p.URL, "cli", "n-1"); err == nil {
		t.Error("a token without a nonce: accepted")
	}
}

// The whole code flow against the fixture, driven by hand: the request
// carries PKCE and the nonce, and the exchange returns the ID token.
func TestCodeFlow(t *testing.T) {
	p := oidctest.New(t, "cli")
	c := mustClient(t)
	m, err := oidc.Discover(t.Context(), c, p.URL)
	if err != nil {
		t.Fatal(err)
	}
	r := oidc.Request{ClientID: "cli", RedirectURI: "http://127.0.0.1:1/callback", Scopes: []string{"openid", "groups"},
		State: oidc.Random(16), Nonce: oidc.Random(16), Verifier: oidc.Random(32)}
	u, _ := url.Parse(oidc.AuthURL(m, r))
	q := u.Query()
	if q.Get("code_challenge") != oidc.Challenge(r.Verifier) || q.Get("code_challenge_method") != "S256" || q.Get("nonce") != r.Nonce || q.Get("scope") != "openid groups" {
		t.Fatalf("authorization URL %s", u)
	}
	noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := noRedirect.Get(u.String())
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	back, _ := url.Parse(resp.Header.Get("Location"))
	code, err := oidc.Callback(back.Query(), r.State)
	if err != nil {
		t.Fatal(err)
	}
	tok, err := oidc.Exchange(t.Context(), c, m, r, code)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := oidc.CheckIDToken(tok, p.URL, "cli", r.Nonce); err != nil {
		t.Fatal(err)
	}
}

// The fixture's own contract: a code is exchanged once, for its redirect URI,
// with the verifier whose challenge it was issued under.
func TestFixtureTokenEndpoint(t *testing.T) {
	p := oidctest.New(t, "cli")
	c := mustClient(t)
	m, err := oidc.Discover(t.Context(), c, p.URL)
	if err != nil {
		t.Fatal(err)
	}
	noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	code := func(r oidc.Request) string {
		resp, err := noRedirect.Get(oidc.AuthURL(m, r))
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		back, _ := url.Parse(resp.Header.Get("Location"))
		return back.Query().Get("code")
	}
	r := oidc.Request{ClientID: "cli", RedirectURI: "http://127.0.0.1:1/callback", Scopes: []string{"openid"}, State: "s", Nonce: "n", Verifier: oidc.Random(32)}
	wrong := r
	wrong.Verifier = oidc.Random(32)
	if _, err := oidc.Exchange(t.Context(), c, m, wrong, code(r)); err == nil {
		t.Error("another verifier: exchanged")
	}
	other := r
	other.RedirectURI = "http://127.0.0.1:2/callback"
	if _, err := oidc.Exchange(t.Context(), c, m, other, code(r)); err == nil {
		t.Error("another redirect URI: exchanged")
	}
	k := code(r)
	if _, err := oidc.Exchange(t.Context(), c, m, r, k); err != nil {
		t.Fatalf("the right verifier: %v", err)
	}
	if _, err := oidc.Exchange(t.Context(), c, m, r, k); err == nil {
		t.Error("a used code: exchanged")
	}
	resp, err := noRedirect.Get(strings.Replace(oidc.AuthURL(m, r), "code_challenge_method=S256", "code_challenge_method=plain", 1))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("a plain challenge: %d", resp.StatusCode)
	}
}
