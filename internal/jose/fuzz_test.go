package jose_test

import (
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/fuzzseed"
	"github.com/happydave/hoardmq/internal/jose"
	"github.com/happydave/hoardmq/internal/oidc/oidctest"
)

// fuzzTokens are the fixture's tokens: valid under each key, tampered, and
// unsigned.
func fuzzTokens(f *testing.F, p *oidctest.Provider) []string {
	f.Helper()
	es := p.AddKey(f, "ES256")
	ed := p.AddKey(f, "EdDSA")
	good := p.SignWith(f, "k1-RS256", nil, p.Claims())
	return []string{
		good,
		p.SignWith(f, es, nil, p.Claims()),
		p.SignWith(f, ed, nil, p.Claims()),
		good[:len(good)-4] + "AAAA",
		"eyJhbGciOiJub25lIn0.e30.",
		"a.b.c",
		"",
	}
}

// FuzzParse: no token panics the parser.
func FuzzParse(f *testing.F) {
	p := oidctest.New(f, "cli")
	for _, tok := range fuzzTokens(f, p) {
		fuzzseed.Add(f, tok)
	}
	f.Fuzz(func(t *testing.T, token string) {
		_, _ = jose.Parse(token)
	})
}

// FuzzParseKeySet: no key set document panics the parser.
func FuzzParseKeySet(f *testing.F) {
	for _, doc := range []string{
		`{"keys":[{"kty":"oct","kid":"s","k":"AAAA"},{"kty":"RSA","n":"AQAB","e":"AQAB"},{"kty":"OKP","crv":"X25519","kid":"x","x":"AAAA"}]}`,
		`{"keys":[{"kty":"EC","kid":"e","crv":"P-256","x":"AAAA","y":"AAAA"}]}`,
		`{"keys":[{"kty":"OKP","crv":"Ed25519","kid":"d","x":"11qYAYKxCrfVS_7TyWQHOg7hcvPapiMlrwIaaPcHURo"}]}`,
		`{}`,
		`[]`,
	} {
		fuzzseed.Add(f, []byte(doc))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = jose.ParseKeySet(data)
	})
}

// FuzzVerify: no token panics verification against the fixture's keys, and
// one token gets one answer.
func FuzzVerify(f *testing.F) {
	p := oidctest.New(f, "cli")
	now := time.Unix(1_800_000_000, 0)
	p.Now = func() time.Time { return now }
	tokens := fuzzTokens(f, p)
	src := jose.NewKeySource(p.URL+"/keys", fuzzClient(f), nil)
	keys, err := src.Keys(f.Context(), "")
	if err != nil {
		f.Fatal(err)
	}
	for _, tok := range tokens {
		fuzzseed.Add(f, tok)
	}
	policy := jose.Policy{Issuer: p.URL, Audiences: []string{"cli"}, Skew: 60 * time.Second, MaxLifetime: 86400 * time.Second}
	f.Fuzz(func(t *testing.T, token string) {
		c1, err1 := jose.Verify(token, keys, policy, now)
		c2, err2 := jose.Verify(token, keys, policy, now)
		if (err1 == nil) != (err2 == nil) || !reflect.DeepEqual(c1, c2) {
			t.Fatalf("two answers for one token: %v %v / %v %v", c1, err1, c2, err2)
		}
	})
}

func fuzzClient(f *testing.F) *http.Client {
	f.Helper()
	c, err := jose.HTTPClient("")
	if err != nil {
		f.Fatal(err)
	}
	return c
}
