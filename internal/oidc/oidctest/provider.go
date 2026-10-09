// Package oidctest is an in-process OpenID Connect provider for tests: it
// serves discovery, a JWKS, an authorization endpoint that consents at once
// as its user, and a token endpoint that checks PKCE. Its keys can be added,
// made current and withdrawn, and it mints tokens with any header and claims.
package oidctest

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"hash"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/oidc"
)

// User is who the provider logs in.
type User struct {
	Sub    string
	Email  string
	Groups []string
}

type key struct {
	kid     string
	alg     string
	private crypto.Signer
	// secret is an HS256 key, for tokens a verifier must refuse.
	secret []byte
}

type grant struct {
	clientID, redirectURI, challenge, nonce string
	used                                    bool
}

// Provider is the fixture. Its exported fields may be changed between
// requests under Lock.
type Provider struct {
	// URL is the issuer.
	URL      string
	ClientID string
	srv      *httptest.Server

	mu       sync.Mutex
	User     User
	Lifetime time.Duration
	Now      func() time.Time
	// Nonce, when set, replaces the nonce the token endpoint puts in the ID
	// token; DiscoveryIssuer, when set, is the issuer discovery names; Down
	// answers 503 to every request.
	Nonce           string
	DiscoveryIssuer string
	Down            bool
	// Delay holds each JWKS and introspection answer back.
	Delay time.Duration
	// IntrospectClientID and IntrospectSecret are the credentials the
	// introspection endpoint requires; Answer, when set, overrides members of
	// every active answer (a nil value removes one).
	IntrospectClientID string
	IntrospectSecret   string
	Answer             map[string]any
	minted             map[string]map[string]any
	revoked            map[string]bool
	keys               []*key
	current            string
	withdrawn          map[string]bool
	grants             map[string]*grant
	counts             map[string]int
}

// New starts a provider serving plain HTTP on loopback, with one RS256 key
// and the user alice in the group hoardmq-ops; it stops with the test.
func New(t testing.TB, clientID string) *Provider {
	t.Helper()
	p := &Provider{ClientID: clientID, User: User{Sub: "00u-alice", Email: "alice@example.com", Groups: []string{"everyone", "hoardmq-ops"}},
		Lifetime: time.Hour, Now: time.Now, withdrawn: map[string]bool{}, grants: map[string]*grant{}, counts: map[string]int{}}
	p.srv = httptest.NewServer(http.HandlerFunc(p.serve))
	t.Cleanup(p.srv.Close)
	p.URL = p.srv.URL
	p.current = p.AddKey(t, "RS256")
	return p
}

// Lock and Unlock guard the exported fields.
func (p *Provider) Lock()   { p.mu.Lock() }
func (p *Provider) Unlock() { p.mu.Unlock() }

// Count is how many requests an endpoint answered: "discovery", "jwks",
// "authorize" or "token".
func (p *Provider) Count(endpoint string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.counts[endpoint]
}

// AddKey creates and publishes a key for alg (RS256, RS1024 — an RS256 key
// of 1024 bits —, ES256, ES384, EdDSA or HS256) and returns its key
// identifier. It does not make the key current.
func (p *Provider) AddKey(t testing.TB, alg string) string {
	t.Helper()
	k := &key{alg: alg}
	var err error
	switch alg {
	case "RS256":
		k.private, err = rsa.GenerateKey(rand.Reader, 2048)
	case "RS1024":
		k.alg = "RS256"
		k.private, err = rsa.GenerateKey(rand.Reader, 1024)
	case "ES256":
		k.private, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	case "ES384":
		k.private, err = ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	case "EdDSA":
		_, k.private, err = ed25519.GenerateKey(rand.Reader)
	case "HS256":
		k.secret = make([]byte, 32)
		_, err = rand.Read(k.secret)
	default:
		t.Fatalf("oidctest: no key type for %s", alg)
	}
	if err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	k.kid = fmt.Sprintf("k%d-%s", len(p.keys)+1, alg)
	p.keys = append(p.keys, k)
	return k.kid
}

// Use makes kid the key the token endpoint signs with.
func (p *Provider) Use(kid string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.current = kid
}

// Withdraw stops publishing kid.
func (p *Provider) Withdraw(kid string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.withdrawn[kid] = true
}

// Public returns kid's public key, for tests that build a confused token.
func (p *Provider) Public(kid string) crypto.PublicKey {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.find(kid).private.Public()
}

func (p *Provider) find(kid string) *key {
	for _, k := range p.keys {
		if k.kid == kid {
			return k
		}
	}
	return nil
}

// Claims are the claims the token endpoint issues for the user at now, with
// no nonce.
func (p *Provider) Claims() map[string]any {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.claimsLocked()
}

func (p *Provider) claimsLocked() map[string]any {
	now := p.Now()
	groups := make([]any, 0, len(p.User.Groups))
	for _, g := range p.User.Groups {
		groups = append(groups, g)
	}
	return map[string]any{"iss": p.URL, "sub": p.User.Sub, "aud": p.ClientID, "iat": now.Unix(),
		"exp": now.Add(p.Lifetime).Unix(), "email": p.User.Email, "groups": groups}
}

// Mint signs claims with the current key and the header it implies.
func (p *Provider) Mint(t testing.TB, claims map[string]any) string {
	t.Helper()
	p.mu.Lock()
	kid := p.current
	p.mu.Unlock()
	return p.SignWith(t, kid, nil, claims)
}

// SignWith signs claims with kid's key. header members override the implied
// alg, kid and typ; a nil value removes a member.
func (p *Provider) SignWith(t testing.TB, kid string, header map[string]any, claims map[string]any) string {
	t.Helper()
	p.mu.Lock()
	k := p.find(kid)
	p.mu.Unlock()
	if k == nil {
		t.Fatalf("oidctest: no key %s", kid)
	}
	h := map[string]any{"alg": k.alg, "kid": k.kid, "typ": "JWT"}
	for name, v := range header {
		if v == nil {
			delete(h, name)
		} else {
			h[name] = v
		}
	}
	alg, _ := h["alg"].(string)
	input := enc(t, h) + "." + enc(t, claims)
	token := input + "." + base64.RawURLEncoding.EncodeToString(sign(t, k, alg, []byte(input)))
	p.mu.Lock()
	if p.minted == nil {
		p.minted = map[string]map[string]any{}
	}
	p.minted[token] = claims
	p.mu.Unlock()
	return token
}

// Opaque registers a reference token the introspection endpoint answers for
// with claims, as it answers for a token it signed.
func (p *Provider) Opaque(token string, claims map[string]any) {
	p.mu.Lock()
	if p.minted == nil {
		p.minted = map[string]map[string]any{}
	}
	p.minted[token] = claims
	p.mu.Unlock()
}

// Revoke makes the introspection endpoint answer the token inactive.
func (p *Provider) Revoke(token string) {
	p.mu.Lock()
	if p.revoked == nil {
		p.revoked = map[string]bool{}
	}
	p.revoked[token] = true
	p.mu.Unlock()
}

// introspect is RFC 7662's endpoint: HTTP Basic client authentication, then
// active for a token this provider signed and has not revoked.
func (p *Provider) introspect(w http.ResponseWriter, r *http.Request) {
	id, secret, ok := r.BasicAuth()
	p.mu.Lock()
	wantID, wantSecret := p.IntrospectClientID, p.IntrospectSecret
	p.mu.Unlock()
	cid, _ := url.QueryUnescape(id)
	sec, _ := url.QueryUnescape(secret)
	if r.Method != http.MethodPost || !ok || cid != wantID || sec != wantSecret || wantID == "" {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "invalid_client"})
		return
	}
	token := r.PostFormValue("token")
	p.mu.Lock()
	defer p.mu.Unlock()
	claims, minted := p.minted[token]
	if !minted || p.revoked[token] {
		writeJSON(w, http.StatusOK, map[string]any{"active": false})
		return
	}
	answer := map[string]any{"active": true, "client_id": p.ClientID, "token_type": "Bearer"}
	for _, m := range []string{"iss", "sub", "aud", "exp", "iat", "scope"} {
		if v, ok := claims[m]; ok {
			answer[m] = v
		}
	}
	for m, v := range p.Answer {
		if v == nil {
			delete(answer, m)
		} else {
			answer[m] = v
		}
	}
	writeJSON(w, http.StatusOK, answer)
}

// SignHMAC signs claims with HS256 under secret, whatever key it names.
func SignHMAC(t testing.TB, header map[string]any, claims map[string]any, secret []byte) string {
	t.Helper()
	input := enc(t, header) + "." + enc(t, claims)
	m := hmac.New(sha256.New, secret)
	m.Write([]byte(input))
	return input + "." + base64.RawURLEncoding.EncodeToString(m.Sum(nil))
}

func enc(t testing.TB, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

func sign(t testing.TB, k *key, alg string, input []byte) []byte {
	t.Helper()
	if k.secret != nil {
		m := hmac.New(sha256.New, k.secret)
		m.Write(input)
		return m.Sum(nil)
	}
	var h crypto.Hash
	var w hash.Hash
	switch alg {
	case "RS384", "ES384":
		h, w = crypto.SHA384, sha512.New384()
	case "RS512", "ES512":
		h, w = crypto.SHA512, sha512.New()
	default:
		h, w = crypto.SHA256, sha256.New()
	}
	switch priv := k.private.(type) {
	case *rsa.PrivateKey:
		w.Write(input)
		sig, err := rsa.SignPKCS1v15(rand.Reader, priv, h, w.Sum(nil))
		if err != nil {
			t.Fatal(err)
		}
		return sig
	case *ecdsa.PrivateKey:
		w.Write(input)
		r, s, err := ecdsa.Sign(rand.Reader, priv, w.Sum(nil))
		if err != nil {
			t.Fatal(err)
		}
		size := (priv.Curve.Params().BitSize + 7) / 8
		out := make([]byte, 2*size)
		r.FillBytes(out[:size])
		s.FillBytes(out[size:])
		return out
	case ed25519.PrivateKey:
		return ed25519.Sign(priv, input)
	}
	t.Fatalf("oidctest: cannot sign with %T", k.private)
	return nil
}

func (p *Provider) serve(w http.ResponseWriter, r *http.Request) {
	p.mu.Lock()
	down := p.Down
	p.mu.Unlock()
	if down {
		http.Error(w, "down", http.StatusServiceUnavailable)
		return
	}
	switch r.URL.Path {
	case "/.well-known/openid-configuration":
		p.count("discovery")
		p.mu.Lock()
		iss := p.URL
		if p.DiscoveryIssuer != "" {
			iss = p.DiscoveryIssuer
		}
		p.mu.Unlock()
		writeJSON(w, http.StatusOK, map[string]any{"issuer": iss, "jwks_uri": p.URL + "/keys",
			"authorization_endpoint": p.URL + "/authorize", "token_endpoint": p.URL + "/token"})
	case "/keys":
		p.count("jwks")
		p.delay()
		writeJSON(w, http.StatusOK, map[string]any{"keys": p.jwks()})
	case "/authorize":
		p.count("authorize")
		p.authorize(w, r)
	case "/token":
		p.count("token")
		p.token(w, r)
	case "/introspect":
		p.count("introspect")
		p.delay()
		p.introspect(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (p *Provider) delay() {
	p.mu.Lock()
	d := p.Delay
	p.mu.Unlock()
	time.Sleep(d)
}

func (p *Provider) count(name string) {
	p.mu.Lock()
	p.counts[name]++
	p.mu.Unlock()
}

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

func (p *Provider) jwks() []any {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := []any{}
	for _, k := range p.keys {
		if p.withdrawn[k.kid] || k.secret != nil {
			continue
		}
		switch pub := k.private.Public().(type) {
		case *rsa.PublicKey:
			out = append(out, map[string]any{"kty": "RSA", "kid": k.kid, "alg": k.alg, "use": "sig", "n": b64(pub.N.Bytes()), "e": b64(big.NewInt(int64(pub.E)).Bytes())})
		case *ecdsa.PublicKey:
			size := (pub.Curve.Params().BitSize + 7) / 8
			point, err := pub.Bytes()
			if err != nil {
				continue
			}
			x, y := point[1:1+size], point[1+size:]
			out = append(out, map[string]any{"kty": "EC", "kid": k.kid, "alg": k.alg, "use": "sig", "crv": pub.Curve.Params().Name, "x": b64(x), "y": b64(y)})
		case ed25519.PublicKey:
			out = append(out, map[string]any{"kty": "OKP", "kid": k.kid, "alg": "EdDSA", "use": "sig", "crv": "Ed25519", "x": b64(pub)})
		}
	}
	return out
}

// authorize consents at once: it records the grant and redirects to the
// redirect URI with a one-time code and the state.
func (p *Provider) authorize(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	redirect := q.Get("redirect_uri")
	if q.Get("response_type") != "code" || q.Get("client_id") != p.ClientID || redirect == "" ||
		q.Get("code_challenge") == "" || q.Get("code_challenge_method") != "S256" {
		http.Error(w, "bad authorization request", http.StatusBadRequest)
		return
	}
	code := oidc.Random(16)
	p.mu.Lock()
	p.grants[code] = &grant{clientID: q.Get("client_id"), redirectURI: redirect, challenge: q.Get("code_challenge"), nonce: q.Get("nonce")}
	p.mu.Unlock()
	u, err := url.Parse(redirect)
	if err != nil {
		http.Error(w, "bad redirect", http.StatusBadRequest)
		return
	}
	back := u.Query()
	back.Set("code", code)
	back.Set("state", q.Get("state"))
	u.RawQuery = back.Encode()
	http.Redirect(w, r, u.String(), http.StatusFound)
}

// token exchanges a code once, for the client and redirect URI it was issued
// to, with the verifier whose challenge it was issued under.
func (p *Provider) token(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil || r.PostForm.Get("grant_type") != "authorization_code" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "unsupported_grant_type"})
		return
	}
	f := r.PostForm
	p.mu.Lock()
	g := p.grants[f.Get("code")]
	ok := g != nil && !g.used && g.clientID == f.Get("client_id") && g.redirectURI == f.Get("redirect_uri") &&
		oidc.Challenge(f.Get("code_verifier")) == g.challenge
	if g != nil {
		g.used = true
	}
	var claims map[string]any
	kid := p.current
	if ok {
		claims = p.claimsLocked()
		claims["nonce"] = g.nonce
		if p.Nonce != "" {
			claims["nonce"] = p.Nonce
		}
	}
	p.mu.Unlock()
	if !ok {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_grant"})
		return
	}
	idToken := p.SignWith(testingT{}, kid, nil, claims)
	writeJSON(w, http.StatusOK, map[string]any{"id_token": idToken, "token_type": "Bearer", "expires_in": int(p.Lifetime / time.Second)})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// testingT lets the token endpoint sign with the helpers that take a
// testing.TB; a signing failure there panics the handler, which the client
// sees as a dropped connection.
type testingT struct{ testing.TB }

func (testingT) Helper()                   {}
func (testingT) Fatal(args ...any)         { panic(fmt.Sprint(args...)) }
func (testingT) Fatalf(f string, a ...any) { panic(fmt.Sprintf(f, a...)) }
