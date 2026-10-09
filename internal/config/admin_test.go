package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// adminDoc builds a document whose admin block is the given JSON, with token
// files under dir named by their content length.
func adminDoc(admin string) string {
	return `{"version":1,"node":{"dataDir":"d"},"listeners":[{"name":"t","type":"tcp","address":":1883"}],"observability":{"address":"127.0.0.1:9090"},"admin":` + admin + `}`
}

// oidcJSON is an admin block with a valid OIDC member, each given member
// replacing the valid one.
func oidcJSON(members string) string {
	base := map[string]string{
		"issuer":       `"https://idp.example"`,
		"clientIds":    `["cli"]`,
		"roleMappings": `[{"group":"g","roles":["ops"]}]`,
	}
	name, value, _ := strings.Cut(members, ":")
	name = strings.Trim(name, `"`)
	base[name] = value
	var parts []string
	for _, k := range []string{"issuer", "clientIds", "roleMappings"} {
		parts = append(parts, `"`+k+`":`+base[k])
		delete(base, k)
	}
	for k, v := range base {
		parts = append(parts, `"`+k+`":`+v)
	}
	return `{"address":"127.0.0.1:9091","roles":[{"name":"ops","scopes":["admin"]}],"oidc":{` + strings.Join(parts, ",") + `}}`
}

func tokenFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestAdminRules(t *testing.T) {
	dir := t.TempDir()
	tok16 := tokenFile(t, dir, "sixteen", "0123456789abcdef\n")
	tok15 := tokenFile(t, dir, "fifteen", "0123456789abcde\n")
	tokDup := tokenFile(t, dir, "dup", "  0123456789abcdef  ")
	other := tokenFile(t, dir, "other", "fedcba9876543210")
	tlsJSON := `"tls":{"certFile":"c.pem","keyFile":"k.pem"}`
	cases := []struct {
		name, admin, pointer, contains string
	}{
		{"no identity", `{"address":"127.0.0.1:9091"}`, "/admin/address", "at least one token, certificate or oidc"},
		{"remote without tls", `{"address":"0.0.0.0:9091","tokens":[{"name":"a","tokenFile":"` + tok16 + `","scopes":["read"]}]}`, "/admin/tls", "not loopback"},
		{"empty host without tls", `{"address":":9091","tokens":[{"name":"a","tokenFile":"` + tok16 + `","scopes":["read"]}]}`, "/admin/tls", "not loopback"},
		{"certificates without client CA", `{"address":"127.0.0.1:9091",` + tlsJSON + `,"certificates":[{"commonName":"ops","scopes":["read"]}]}`, "/admin/certificates", "clientCAFile"},
		{"collides with observability", `{"address":"127.0.0.1:9090","tokens":[{"name":"a","tokenFile":"` + tok16 + `","scopes":["read"]}]}`, "/admin/address", "/observability/address"},
		{"collides with a listener on all hosts", `{"address":"127.0.0.1:1883","tokens":[{"name":"a","tokenFile":"` + tok16 + `","scopes":["read"]}]}`, "/admin/address", "/listeners/0/address"},
		{"short token", `{"address":"127.0.0.1:9091","tokens":[{"name":"a","tokenFile":"` + tok15 + `","scopes":["read"]}]}`, "/admin/tokens/0/tokenFile", "at least 16 bytes"},
		{"missing token file", `{"address":"127.0.0.1:9091","tokens":[{"name":"a","tokenFile":"` + filepath.Join(dir, "absent") + `","scopes":["read"]}]}`, "/admin/tokens/0/tokenFile", "no such file"},
		{"duplicate name", `{"address":"127.0.0.1:9091","tokens":[{"name":"a","tokenFile":"` + tok16 + `","scopes":["read"]},{"name":"a","tokenFile":"` + other + `","scopes":["read"]}]}`, "/admin/tokens/1/name", "duplicate"},
		{"duplicate value", `{"address":"127.0.0.1:9091","tokens":[{"name":"a","tokenFile":"` + tok16 + `","scopes":["read"]},{"name":"b","tokenFile":"` + tokDup + `","scopes":["read"]}]}`, "/admin/tokens/1/tokenFile", "same token"},
		{"no scopes", `{"address":"127.0.0.1:9091","tokens":[{"name":"a","tokenFile":"` + tok16 + `","scopes":[]}]}`, "/admin/tokens/0", "at least one scope or role"},
		{"no scopes and no roles", `{"address":"127.0.0.1:9091","tokens":[{"name":"a","tokenFile":"` + tok16 + `"}]}`, "/admin/tokens/0", "at least one scope or role"},
		{"certificate without scopes", `{"address":"127.0.0.1:9091",` + `"tls":{"certFile":"c.pem","keyFile":"k.pem","clientCAFile":"ca.pem"},"certificates":[{"commonName":"ops","scopes":[]}]}`, "/admin/certificates/0", "at least one scope or role"},
		{"duplicate role", `{"roles":[{"name":"v","scopes":["read"]},{"name":"v","scopes":["admin"]}]}`, "/admin/roles/1/name", "duplicate role"},
		{"role without scopes", `{"roles":[{"name":"v","scopes":[]}]}`, "/admin/roles/0/scopes", "at least one scope"},
		{"token names an undefined role", `{"address":"127.0.0.1:9091","roles":[{"name":"viewer","scopes":["read"]}],"tokens":[{"name":"a","tokenFile":"` + tok16 + `","roles":["viewer","viewr"]}]}`, "/admin/tokens/0/roles/1", "no role named \"viewr\""},
		{"certificate names an undefined role", `{"address":"127.0.0.1:9091","tls":{"certFile":"c.pem","keyFile":"k.pem","clientCAFile":"ca.pem"},"certificates":[{"commonName":"ops","roles":["ops"]}]}`, "/admin/certificates/0/roles/0", "no role named"},
		{"mapping names an undefined role", oidcJSON(`"roleMappings":[{"group":"g","roles":["nope"]}]`), "/admin/oidc/roleMappings/0/roles/0", "no role named"},
		{"mapping without roles", oidcJSON(`"roleMappings":[{"group":"g","roles":[]}]`), "/admin/oidc/roleMappings/0/roles", "at least one role"},
		{"no mappings", oidcJSON(`"roleMappings":[]`), "/admin/oidc/roleMappings", "at least one mapping"},
		{"no client ids", oidcJSON(`"clientIds":[]`), "/admin/oidc/clientIds", "at least one client identifier"},
		{"issuer over http off loopback", oidcJSON(`"issuer":"http://idp.example"`), "/admin/oidc/issuer", "https unless the host is loopback"},
		{"issuer with a query", oidcJSON(`"issuer":"https://idp.example/?x=1"`), "/admin/oidc/issuer", "query or a fragment"},
		{"issuer with a fragment", oidcJSON(`"issuer":"https://idp.example/#f"`), "/admin/oidc/issuer", "query or a fragment"},
		{"issuer not absolute", oidcJSON(`"issuer":"idp.example/oauth2"`), "/admin/oidc/issuer", "absolute URL"},
		{"issuer of another scheme", oidcJSON(`"issuer":"ftp://idp.example"`), "/admin/oidc/issuer", "must be https"},
		{"unreadable CA file", oidcJSON(`"caFile":"` + filepath.Join(dir, "absent.pem") + `"`), "/admin/oidc/caFile", "no such file"},
		{"CA file without a certificate", oidcJSON(`"caFile":"` + tok16 + `"`), "/admin/oidc/caFile", "no certificate"},
		{"skew over five minutes", oidcJSON(`"clockSkewSeconds":301`), "/admin/oidc/clockSkewSeconds", "at most 300"},
		{"lifetime under a minute", oidcJSON(`"maxTokenLifetimeSeconds":59`), "/admin/oidc/maxTokenLifetimeSeconds", "at least 60"},
		{"unknown scope", `{"address":"127.0.0.1:9091","tokens":[{"name":"a","tokenFile":"` + tok16 + `","scopes":["root"]}]}`, "/admin/tokens/0/scopes/0", "one of"},
		{"bad address", `{"address":"nope","tokens":[{"name":"a","tokenFile":"` + tok16 + `","scopes":["read"]}]}`, "/admin/address", "host:port"},
		{"body cap low", `{"maxBodyBytes":10}`, "/admin/maxBodyBytes", "at least 1024"},
		{"unknown member", `{"token":"x"}`, "/admin/token", "unknown member"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, problems := Parse([]byte(adminDoc(c.admin)), nil)
			for _, p := range problems {
				if p.Pointer == c.pointer && strings.Contains(p.Message, c.contains) {
					return
				}
			}
			t.Errorf("no problem at %s containing %q; got:\n%s", c.pointer, c.contains, lines(problems))
		})
	}
	// One step inside each rule passes: a 16-byte token on loopback without
	// TLS, a certificate identity with a client CA, a port no one else binds,
	// and the same port on another host.
	for _, ok := range []string{
		`{"address":"127.0.0.1:9091","tokens":[{"name":"a","tokenFile":"` + tok16 + `","scopes":["read"]},{"name":"b","tokenFile":"` + other + `","scopes":["admin","cluster:drain"]}]}`,
		`{"address":"0.0.0.0:9443","tls":{"certFile":"c.pem","keyFile":"k.pem","clientCAFile":"ca.pem"},"certificates":[{"commonName":"ops","scopes":["read"]}]}`,
		`{"address":"127.0.0.2:9090","tokens":[{"name":"a","tokenFile":"` + tok16 + `","scopes":["read"]}]}`,
		`{}`,
		`{"address":"127.0.0.1:9091","roles":[{"name":"viewer","scopes":["clients:read"]}],"tokens":[{"name":"a","tokenFile":"` + tok16 + `","roles":["viewer"]}]}`,
		`{"address":"127.0.0.1:9091","roles":[{"name":"ops","scopes":["admin"]}],"oidc":{"issuer":"http://127.0.0.1:8080","clientIds":["cli"],"roleMappings":[{"group":"g","roles":["ops"]}],"clockSkewSeconds":0,"maxTokenLifetimeSeconds":60}}`,
		`{"address":"127.0.0.1:9091","roles":[{"name":"ops","scopes":["admin"]}],"oidc":{"issuer":"https://idp.example/oauth2/default","clientIds":["cli"],"roleMappings":[{"group":"g","roles":["ops"]}]}}`,
		`{"address":"127.0.0.1:9091","roles":[{"name":"ops","scopes":["admin"]}],"oidc":{"issuer":"http://localhost:8080/","clientIds":["cli"],"roleMappings":[{"group":"g","roles":["ops"]}],"clockSkewSeconds":300}}`,
	} {
		doc, problems := Parse([]byte(adminDoc(ok)), nil)
		if len(problems) > 0 {
			t.Errorf("%s: %s", ok, lines(problems))
		}
		if ok == `{}` && (doc.Admin.Address != "" || doc.Admin.MaxBodyBytes != 1<<20) {
			t.Errorf("admin defaults %+v", doc.Admin)
		}
	}
}

func TestAddressesCollide(t *testing.T) {
	cases := []struct {
		a, b string
		want bool
	}{
		{"127.0.0.1:9091", "127.0.0.1:9091", true},
		{"127.0.0.1:9091", ":9091", true},
		{"127.0.0.1:9091", "0.0.0.0:9091", true},
		{"[::]:9091", "127.0.0.1:9091", true},
		{"127.0.0.1:9091", "127.0.0.2:9091", false},
		{"127.0.0.1:9091", "127.0.0.1:9092", false},
		{"127.0.0.1:0", "127.0.0.1:0", false},
	}
	for _, c := range cases {
		if got := addressesCollide(c.a, c.b); got != c.want {
			t.Errorf("%s vs %s: %v", c.a, c.b, got)
		}
	}
}

// The admin CLI's environment is not a configuration override: a shell that
// exports it can still run serve.
func TestAdminClientEnvironmentIsNotAnOverride(t *testing.T) {
	_, problems := Parse([]byte(`{"version":1,"node":{"dataDir":"d"}}`), []string{EnvAdminToken + "=x", EnvAdminAddress + "=127.0.0.1:9091"})
	if len(problems) > 0 {
		t.Fatal(lines(problems))
	}
	_, problems = Parse([]byte(`{"version":1,"node":{"dataDir":"d"}}`), []string{EnvPrefix + "ADMIN_NOPE=x"})
	if len(problems) != 1 || !strings.Contains(problems[0].Message, "unknown environment override") {
		t.Fatalf("an unknown HOARDMQ_ADMIN_ name must still be refused: %s", lines(problems))
	}
}
