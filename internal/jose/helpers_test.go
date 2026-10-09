package jose_test

import (
	"encoding/base64"
	"encoding/json"
	"testing"
)

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

func mustSeg(t *testing.T, s string) []byte {
	t.Helper()
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// noneToken is an unsigned token: alg none and an empty signature.
func noneToken(t *testing.T, claims map[string]any) string {
	t.Helper()
	h, _ := json.Marshal(map[string]any{"alg": "none", "kid": "k1-RS256"})
	c, _ := json.Marshal(claims)
	return b64(h) + "." + b64(c) + "."
}
