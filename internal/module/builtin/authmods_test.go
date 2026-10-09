package builtin

import (
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/happydave/hoardmq/internal/module"
)

func provision(t *testing.T, m module.Module, doc string) error {
	t.Helper()
	return m.Provision(&module.Context{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, json.RawMessage(doc))
}

// Each configuration rule refuses a value, and a value one step inside it
// passes.
func TestAuthModuleConfiguration(t *testing.T) {
	dir := t.TempDir()
	secret := filepath.Join(dir, "secret")
	empty := filepath.Join(dir, "empty")
	_ = os.WriteFile(secret, []byte("s3cret\n"), 0o600)
	_ = os.WriteFile(empty, []byte(" \n"), 0o600)
	jwks := `"issuer":"https://idp.example","audiences":["hoardmq"],"jwksUri":"https://idp.example/keys"`
	intro := func(extra string) string {
		return `"issuer":"https://idp.example","audiences":["hoardmq"],"introspection":{"endpoint":"https://idp.example/introspect","clientId":"b","clientSecretFile":"` + secret + `"` + extra + `}`
	}
	for _, c := range []struct {
		name, doc, want string // want "" passes
	}{
		{"jwks", `{` + jwks + `}`, ""},
		{"introspection", `{` + intro("") + `}`, ""},
		{"opaque tokens", `{` + intro(`,"opaqueTokens":true,"opaqueTokenPrefix":"opq_"`) + `}`, ""},
		{"a prefix without opaque tokens", `{` + intro(`,"opaqueTokenPrefix":"opq_"`) + `}`, "opaqueTokenPrefix needs opaqueTokens"},
		{"no issuer", `{"audiences":["hoardmq"],"jwksUri":"https://idp.example/keys"}`, "issuer is required"},
		{"no audience", `{"issuer":"i","audiences":[],"jwksUri":"https://idp.example/keys"}`, "at least one audience"},
		{"an empty audience", `{"issuer":"i","audiences":[""],"jwksUri":"https://idp.example/keys"}`, "audiences[0] is empty"},
		{"both modes", `{` + jwks + `,"introspection":{"endpoint":"https://x/i","clientId":"b","clientSecretFile":"` + secret + `"}}`, "not both"},
		{"neither mode", `{"issuer":"i","audiences":["a"]}`, "one of jwksUri and introspection is required"},
		{"http jwksUri off loopback", `{"issuer":"i","audiences":["a"],"jwksUri":"http://idp.example/keys"}`, "jwksUri is not https"},
		{"http jwksUri on loopback", `{"issuer":"i","audiences":["a"],"jwksUri":"http://127.0.0.1:1/keys"}`, ""},
		{"http endpoint off loopback", `{"issuer":"i","audiences":["a"],"introspection":{"endpoint":"http://idp.example/i","clientId":"b","clientSecretFile":"` + secret + `"}}`, "endpoint is not https"},
		{"no clientId", `{"issuer":"i","audiences":["a"],"introspection":{"endpoint":"https://x/i","clientSecretFile":"` + secret + `"}}`, "clientId is required"},
		{"no secret file", `{"issuer":"i","audiences":["a"],"introspection":{"endpoint":"https://x/i","clientId":"b"}}`, "clientSecretFile is required"},
		{"an absent secret file", `{"issuer":"i","audiences":["a"],"introspection":{"endpoint":"https://x/i","clientId":"b","clientSecretFile":"` + filepath.Join(dir, "absent") + `"}}`, "no such file"},
		{"an empty secret file", `{"issuer":"i","audiences":["a"],"introspection":{"endpoint":"https://x/i","clientId":"b","clientSecretFile":"` + empty + `"}}`, "clientSecretFile is empty"},
		{"timeoutMs 99", `{` + intro(`,"timeoutMs":99`) + `}`, "timeoutMs must be 100 to 10000"},
		{"timeoutMs 100", `{` + intro(`,"timeoutMs":100`) + `}`, ""},
		{"from token", `{` + jwks + `,"from":"token"}`, `from must be`},
		{"from username", `{` + jwks + `,"from":"username"}`, ""},
		{"skew 301", `{` + jwks + `,"clockSkewSeconds":301}`, "clockSkewSeconds must be 0 to 300"},
		{"skew 300", `{` + jwks + `,"clockSkewSeconds":300}`, ""},
		{"lifetime 59", `{` + jwks + `,"maxTokenLifetimeSeconds":59}`, "maxTokenLifetimeSeconds must be 60"},
		{"lifetime 60", `{` + jwks + `,"maxTokenLifetimeSeconds":60}`, ""},
		{"wait 10001", `{` + jwks + `,"fetchWaitMs":10001}`, "fetchWaitMs must be 0 to 10000"},
		{"wait 0", `{` + jwks + `,"fetchWaitMs":0}`, ""},
		{"a CA file with no certificate", `{` + jwks + `,"caFile":"` + secret + `"}`, "no certificate"},
		{"a CA file with no certificate beside a bad skew", `{` + jwks + `,"clockSkewSeconds":301,"caFile":"` + secret + `"}`, "caFile: "},
		{"an unknown member", `{` + jwks + `,"audience":"x"}`, "unknown field"},
	} {
		err := provision(t, &jwtAuth{}, c.doc)
		switch {
		case c.want == "" && err != nil:
			t.Errorf("auth.jwt %s: %v", c.name, err)
		case c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)):
			t.Errorf("auth.jwt %s: %v, want %q", c.name, err, c.want)
		}
	}
	for _, c := range []struct {
		name, doc, want string
	}{
		{"defaults", `{}`, ""},
		{"both off", `{"ocsp":false,"crl":false}`, "at least one of ocsp and crl"},
		{"crl only", `{"ocsp":false}`, ""},
		{"an ftp ocspUrl", `{"ocspUrl":"ftp://x/ocsp"}`, "ocspUrl must be"},
		{"an http ocspUrl", `{"ocspUrl":"http://ocsp.example"}`, ""},
		{"a relative crlUrl", `{"crlUrls":["/crl"]}`, "crlUrls[0] must be"},
		{"timeoutMs 99", `{"timeoutMs":99}`, "timeoutMs must be 100 to 30000"},
		{"timeoutMs 30000", `{"timeoutMs":30000}`, ""},
		{"wait 10001", `{"fetchWaitMs":10001}`, "fetchWaitMs must be 0 to 10000"},
		{"an unknown member", `{"failClosed":true}`, "unknown field"},
	} {
		err := provision(t, &x509Auth{}, c.doc)
		switch {
		case c.want == "" && err != nil:
			t.Errorf("auth.x509 %s: %v", c.name, err)
		case c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)):
			t.Errorf("auth.x509 %s: %v, want %q", c.name, err, c.want)
		}
	}
	if err := provision(t, &scramAuth{}, `{}`); err == nil || !strings.Contains(err.Error(), "path is required") {
		t.Errorf("auth.scram without a path: %v", err)
	}
}

// The opaque-token options reach the authenticator.
func TestJWTOpaqueOptionsProvisioned(t *testing.T) {
	secret := filepath.Join(t.TempDir(), "secret")
	_ = os.WriteFile(secret, []byte("s3cret\n"), 0o600)
	j := &jwtAuth{}
	doc := `{"issuer":"https://idp.example","audiences":["hoardmq"],"introspection":{"endpoint":"https://idp.example/introspect","clientId":"b","clientSecretFile":"` +
		secret + `","opaqueTokens":true,"opaqueTokenPrefix":"opq_"}}`
	if err := provision(t, j, doc); err != nil {
		t.Fatal(err)
	}
	if !j.inner.OpaqueTokens || j.inner.OpaquePrefix != "opq_" {
		t.Fatalf("provisioned opaqueTokens %v, prefix %q", j.inner.OpaqueTokens, j.inner.OpaquePrefix)
	}
}
