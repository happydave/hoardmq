package jose_test

import (
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/jose"
	"github.com/happydave/hoardmq/internal/oidc/oidctest"
)

// keysOf reads the fixture's published key set, as the key source would.
func keysOf(t *testing.T, p *oidctest.Provider) jose.KeySet {
	t.Helper()
	src := jose.NewKeySource(p.URL+"/keys", mustClient(t), nil)
	ks, err := src.Keys(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	return ks
}

func mustClient(t *testing.T) *http.Client {
	t.Helper()
	c, err := jose.HTTPClient("")
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// The verification decision, one clause at a time from a token that passes:
// skew 60 s, lifetime cap 86400 s, now fixed.
func TestVerify(t *testing.T) {
	p := oidctest.New(t, "cli")
	now := time.Unix(1_800_000_000, 0)
	p.Now = func() time.Time { return now }
	es256 := p.AddKey(t, "ES256")
	es384 := p.AddKey(t, "ES384")
	ed := p.AddKey(t, "EdDSA")
	small := p.AddKey(t, "RS1024")
	rs := "k1-RS256"
	// Without the keys' own alg members, so the type and curve clauses are
	// what refuses a mismatched header (TestKeyMembersBind holds alg).
	keys := jose.KeySet{}
	for id, k := range keysOf(t, p) {
		k.Alg = ""
		keys[id] = k
	}
	policy := jose.Policy{Issuer: p.URL, Audiences: []string{"cli", "web"}, Skew: 60 * time.Second, MaxLifetime: 86400 * time.Second}

	base := func() map[string]any { return p.Claims() }
	with := func(change func(c map[string]any)) map[string]any {
		c := base()
		change(c)
		return c
	}
	sec := func(d time.Duration) int64 { return now.Add(d).Unix() }
	tamper := func(tok string) string {
		parts := strings.Split(tok, ".")
		c := map[string]any{}
		_ = json.Unmarshal(mustSeg(t, parts[1]), &c)
		c["sub"] = "00u-mallory"
		b, _ := json.Marshal(c)
		return parts[0] + "." + b64(b) + "." + parts[2]
	}
	rsPub := p.Public(rs).(*rsa.PublicKey)

	cases := []struct {
		name  string
		token string
		want  string // "" passes; otherwise a substring of the refusal
	}{
		{"a valid RS256 token", p.SignWith(t, rs, nil, base()), ""},
		{"a valid ES256 token", p.SignWith(t, es256, nil, base()), ""},
		{"a valid ES384 token", p.SignWith(t, es384, nil, base()), ""},
		{"a valid EdDSA token", p.SignWith(t, ed, nil, base()), ""},
		{"a changed payload", tamper(p.SignWith(t, rs, nil, base())), "bad signature"},
		{"alg none", noneToken(t, base()), "algorithm none not allowed"},
		{"HS256 keyed with the RSA modulus", oidctest.SignHMAC(t, map[string]any{"alg": "HS256", "kid": rs}, base(), rsPub.N.Bytes()), "algorithm HS256 not allowed"},
		{"an RS256 header over an EC key", p.SignWith(t, es256, map[string]any{"alg": "RS256"}, base()), "for RS256"},
		{"an ES256 header over a P-384 key", p.SignWith(t, es384, map[string]any{"alg": "ES256"}, base()), "wrong curve"},
		{"an ES256 header over an RSA key", p.SignWith(t, rs, map[string]any{"alg": "ES256"}, base()), "an RSA key for ES256"},
		{"an RS256 header over an Ed25519 key", p.SignWith(t, ed, map[string]any{"alg": "RS256"}, base()), "an Ed25519 key for RS256"},
		{"an unknown kid", p.SignWith(t, rs, map[string]any{"kid": "k99"}, base()), "unknown signing key"},
		{"no kid", p.SignWith(t, rs, map[string]any{"kid": nil}, base()), "no key identifier"},
		{"a 1024-bit RSA key", p.SignWith(t, small, nil, base()), "under 2048 bits"},
		{"a critical header", p.SignWith(t, rs, map[string]any{"crit": []string{"exp"}}, base()), "critical"},
		{"an access token", p.SignWith(t, rs, map[string]any{"typ": "at+jwt"}, base()), "access token"},
		{"no typ", p.SignWith(t, rs, map[string]any{"typ": nil}, base()), ""},
		{"an issuer with a trailing slash", p.SignWith(t, rs, nil, with(func(c map[string]any) { c["iss"] = p.URL + "/" })), "issuer mismatch"},
		{"another client's token", p.SignWith(t, rs, nil, with(func(c map[string]any) { c["aud"] = "other" })), "audience mismatch"},
		{"an audience array holding ours", p.SignWith(t, rs, nil, with(func(c map[string]any) { c["aud"] = []string{"other", "web"} })), ""},
		{"an audience array without ours", p.SignWith(t, rs, nil, with(func(c map[string]any) { c["aud"] = []string{"other", "third"} })), "audience mismatch"},
		{"azp another client", p.SignWith(t, rs, nil, with(func(c map[string]any) { c["azp"] = "other" })), "authorized party"},
		{"azp ours", p.SignWith(t, rs, nil, with(func(c map[string]any) { c["azp"] = "web" })), ""},
		{"expired 30 s ago, inside the skew", p.SignWith(t, rs, nil, with(func(c map[string]any) { c["exp"] = sec(-30 * time.Second); c["iat"] = sec(-time.Hour) })), ""},
		{"expired 90 s ago", p.SignWith(t, rs, nil, with(func(c map[string]any) { c["exp"] = sec(-90 * time.Second); c["iat"] = sec(-time.Hour) })), "token expired"},
		{"no exp", p.SignWith(t, rs, nil, with(func(c map[string]any) { delete(c, "exp") })), "no expiry"},
		{"no iat", p.SignWith(t, rs, nil, with(func(c map[string]any) { delete(c, "iat") })), "no issue time"},
		{"issued 30 s ahead", p.SignWith(t, rs, nil, with(func(c map[string]any) { c["iat"] = sec(30 * time.Second) })), ""},
		{"issued 90 s ahead", p.SignWith(t, rs, nil, with(func(c map[string]any) { c["iat"] = sec(90 * time.Second) })), "issued in the future"},
		{"nbf 30 s ahead", p.SignWith(t, rs, nil, with(func(c map[string]any) { c["nbf"] = sec(30 * time.Second) })), ""},
		{"nbf 90 s ahead", p.SignWith(t, rs, nil, with(func(c map[string]any) { c["nbf"] = sec(90 * time.Second) })), "not yet valid"},
		{"a lifetime of 86400 s", p.SignWith(t, rs, nil, with(func(c map[string]any) { c["iat"] = sec(-time.Hour); c["exp"] = sec(-time.Hour + 86400*time.Second) })), ""},
		{"a lifetime of 86401 s", p.SignWith(t, rs, nil, with(func(c map[string]any) { c["iat"] = sec(-time.Hour); c["exp"] = sec(-time.Hour + 86401*time.Second) })), "lifetime longer"},
		{"a fractional exp", p.SignWith(t, rs, nil, with(func(c map[string]any) { c["exp"] = float64(sec(time.Hour)) + 0.5 })), ""},
		{"not a JWS", "opaque-token-0123456789", "not a compact JWS"},
		{"padded segments", strings.Replace(p.SignWith(t, rs, nil, base()), ".", "=.", 1), "not a compact JWS"},
		{"a token over 16 KiB", p.SignWith(t, rs, nil, with(func(c map[string]any) { c["pad"] = strings.Repeat("x", 17<<10) })), "16 KiB"},
	}
	for _, c := range cases {
		_, err := jose.Verify(c.token, keys, policy, now)
		switch {
		case c.want == "" && err != nil:
			t.Errorf("%s: refused: %v", c.name, err)
		case c.want != "" && err == nil:
			t.Errorf("%s: accepted", c.name)
		case c.want != "" && !strings.Contains(err.Error(), c.want):
			t.Errorf("%s: refused for %q, want %q", c.name, err, c.want)
		}
	}
}

// A key's own alg and use members bind it.
func TestKeyMembersBind(t *testing.T) {
	p := oidctest.New(t, "cli")
	keys := keysOf(t, p)
	k := keys["k1-RS256"]
	now := time.Now()
	policy := jose.Policy{Issuer: p.URL, Audiences: []string{"cli"}, Skew: time.Minute, MaxLifetime: 24 * time.Hour}
	tok := p.Mint(t, p.Claims())
	if _, err := jose.Verify(tok, keys, policy, now); err != nil {
		t.Fatalf("valid: %v", err)
	}
	for name, change := range map[string]func(*jose.Key){
		"alg RS384":  func(k *jose.Key) { k.Alg = "RS384" },
		"use enc":    func(k *jose.Key) { k.Use = "enc" },
		"no members": func(k *jose.Key) { k.Alg, k.Use = "", "" },
	} {
		kk := k
		change(&kk)
		_, err := jose.Verify(tok, jose.KeySet{kk.ID: kk}, policy, now)
		if (name == "no members") != (err == nil) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestParseKeySet(t *testing.T) {
	ks, err := jose.ParseKeySet([]byte(`{"keys":[{"kty":"oct","kid":"s","k":"AAAA"},{"kty":"RSA","n":"AQAB","e":"AQAB"},{"kty":"OKP","crv":"X25519","kid":"x","x":"AAAA"}]}`))
	if err != nil || len(ks) != 0 {
		t.Fatalf("unsupported and kid-less keys: %v %v", ks, err)
	}
	if _, err := jose.ParseKeySet([]byte(`{"keys":[{"kty":"EC","kid":"e","crv":"P-256","x":"AAAA","y":"AAAA"}]}`)); err == nil {
		t.Fatal("an EC key of the wrong size parsed")
	}
	if _, err := jose.ParseKeySet([]byte(`{}`)); err == nil {
		t.Fatal("a document without keys parsed")
	}
}

// The ID-token policy refuses an access token and a foreign azp; the
// access-token policy accepts both.
func TestPolicySwitches(t *testing.T) {
	p := oidctest.New(t, "cli")
	keys := keysOf(t, p)
	now := time.Now()
	idTokens := jose.Policy{Issuer: p.URL, Audiences: []string{"cli"}, Skew: time.Minute, MaxLifetime: 24 * time.Hour}
	accessTokens := idTokens
	accessTokens.AcceptAccessTokens, accessTokens.IgnoreAZP = true, true
	at := p.SignWith(t, "k1-RS256", map[string]any{"typ": "at+jwt"}, p.Claims())
	azp := p.Claims()
	azp["azp"] = "someone-else"
	foreign := p.SignWith(t, "k1-RS256", nil, azp)
	for _, c := range []struct {
		name, token string
		policy      jose.Policy
		want        string
	}{
		{"an access token, ID-token policy", at, idTokens, "access token"},
		{"an access token, access-token policy", at, accessTokens, ""},
		{"a foreign azp, ID-token policy", foreign, idTokens, "authorized party"},
		{"a foreign azp, access-token policy", foreign, accessTokens, ""},
	} {
		_, err := jose.Verify(c.token, keys, c.policy, now)
		if (c.want == "") != (err == nil) || err != nil && !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want %q", c.name, err, c.want)
		}
	}
}
