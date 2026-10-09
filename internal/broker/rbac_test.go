package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/admin"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/oidc/oidctest"
)

// lockedBuffer is a log destination the test reads while the node writes.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

const adminViewer = "broker-test-viewer-token-0123456"

// rbacNode starts a one-node broker on dataDir with roles, a viewer token,
// the ops token and, when idp is set, OIDC; its log goes to logs.
func rbacNode(t *testing.T, dataDir string, idp *oidctest.Provider, logs io.Writer) (*Server, error) {
	t.Helper()
	dir := t.TempDir()
	write := func(name, value string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	doc := config.Default()
	doc.Node.DataDir = dataDir
	doc.Auth.Anonymous = true
	doc.Cluster.Raft.TickMs = 10
	doc.Cluster.Readiness.StableMs = 0
	doc.Listeners = []config.Listener{{Name: "tcp", Type: "tcp", Address: "127.0.0.1:0"}}
	doc.Admin.Address = "127.0.0.1:0"
	doc.Admin.Roles = []config.AdminRole{{Name: "viewer", Scopes: []string{"clients:read"}}, {Name: "operator", Scopes: []string{"read", "clients:write"}}}
	doc.Admin.Tokens = []config.AdminToken{
		{Name: "ops", TokenFile: write("ops", adminOps), Scopes: []string{"admin"}},
		{Name: "viewer", TokenFile: write("viewer", adminViewer), Roles: []string{"viewer"}},
	}
	if idp != nil {
		doc.Admin.OIDC = &config.AdminOIDC{Issuer: idp.URL, ClientIDs: []string{idp.ClientID},
			RoleMappings: []config.AdminRoleMapping{{Group: "hoardmq-ops", Roles: []string{"operator"}}}}
	}
	if p := doc.Validate(); len(p) != 0 {
		t.Fatalf("config: %v", p)
	}
	srv, err := New(doc, slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start(); err != nil {
		return nil, err
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	return srv, nil
}

func call(t *testing.T, srv *Server, method, path, token, body string) (int, []byte) {
	t.Helper()
	return api(t, &fixture{t: t, srv: srv}, method, path, token, body)
}

// The workitem's three criteria on a live node: a role lacking a scope is
// refused and the refusal is in the node's audit log; an ID token from the
// configured provider is an identity with its mapped roles; an API key is
// scoped, revocable, never in the audit log or the server's log, and kept
// across a restart.
func TestRolesOIDCAndAPIKeysOnANode(t *testing.T) {
	idp := oidctest.New(t, "hoardmq-cli")
	data := filepath.Join(t.TempDir(), "data")
	logs := &lockedBuffer{}
	srv, err := rbacNode(t, data, idp, logs)
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{t: t, srv: srv}
	if n := idp.Count("discovery") + idp.Count("jwks"); n != 0 {
		t.Fatalf("starting the node contacted the provider %d times", n)
	}
	if _, err := os.Stat(filepath.Join(data, "admin")); !os.IsNotExist(err) {
		t.Fatalf("the key directory exists before any key: %v", err)
	}

	// AC 1.
	if code, _ := call(t, srv, "GET", "/api/v1/clients", adminViewer, ""); code != 200 {
		t.Fatalf("viewer lists clients: %d", code)
	}
	if code, _ := call(t, srv, "POST", "/api/v1/clients/c9/disconnect", adminViewer, ""); code != 403 {
		t.Fatalf("viewer disconnects: %d", code)
	}
	if !auditHas(t, f, "token:viewer", "client.disconnect", "c9", "denied") {
		t.Fatal("the viewer's refusal is not in the node's audit log")
	}

	// AC 2.
	code, data2 := call(t, srv, "GET", "/api/v1/whoami", idp.Mint(t, idp.Claims()), "")
	who := decode[admin.WhoAmI](t, data2)
	if code != 200 || who.Name != "oidc:00u-alice" || strings.Join(who.Roles, ",") != "operator" {
		t.Fatalf("whoami with an ID token: %d %s", code, data2)
	}

	// AC 3.
	code, data2 = call(t, srv, "POST", "/api/v1/apikeys", adminOps, `{"name":"k1","scopes":["clients:read"]}`)
	k1 := decode[admin.CreatedAPIKey](t, data2)
	if code != 201 {
		t.Fatalf("create k1: %d %s", code, data2)
	}
	_, data2 = call(t, srv, "POST", "/api/v1/apikeys", adminOps, `{"name":"k2","scopes":["clients:read"]}`)
	k2 := decode[admin.CreatedAPIKey](t, data2)
	// A node takes API keys once it has renewed its lease since it started.
	adminWait(t, "k1 to list clients", func() bool {
		code, _ := call(t, srv, "GET", "/api/v1/clients", k1.Key, "")
		return code == 200
	})
	if code, _ := call(t, srv, "DELETE", "/api/v1/clients/c9/session", k1.Key, ""); code != 403 {
		t.Fatalf("k1 invalidates: %d", code)
	}
	if code, _ := call(t, srv, "DELETE", "/api/v1/apikeys/"+k1.ID, adminOps, ""); code != 200 {
		t.Fatalf("revoke k1: %d", code)
	}
	if code, _ := call(t, srv, "GET", "/api/v1/clients", k1.Key, ""); code != 401 {
		t.Fatalf("revoked k1: %d", code)
	}
	if !auditHas(t, f, "token:ops", "apikey.create", k1.ID, "ok") || !auditHas(t, f, "token:ops", "apikey.revoke", k1.ID, "ok") ||
		!auditHas(t, f, "apikey:"+k1.ID, "session.invalidate", "c9", "denied") {
		t.Fatal("the key's creation, revocation or refusal is not in the audit log")
	}
	var auditText strings.Builder
	files, _ := filepath.Glob(filepath.Join(data, "audit", "*"))
	for _, p := range files {
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		auditText.Write(b)
	}
	if !strings.Contains(auditText.String(), k1.ID) {
		t.Fatal("the audit files hold no record of the key, so their silence proves nothing")
	}
	for name, text := range map[string]string{"audit log": auditText.String(), "server log": logs.String()} {
		for _, key := range []string{k1.Key, k2.Key} {
			if strings.Contains(text, key) {
				t.Errorf("the %s holds a key", name)
			}
		}
	}

	// A restart on the same data directory keeps both keys' state.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	again, err := rbacNode(t, data, nil, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	adminWait(t, "k2 to list clients after a restart", func() bool {
		code, _ := call(t, again, "GET", "/api/v1/clients", k2.Key, "")
		return code == 200
	})
	if code, _ := call(t, again, "GET", "/api/v1/clients", k1.Key, ""); code != 401 {
		t.Fatalf("revoked k1 after a restart: %d", code)
	}
}

// A key file that does not parse refuses the start, naming the file.
func TestAdminKeyFileGarbageRefusesStart(t *testing.T) {
	data := filepath.Join(t.TempDir(), "data")
	if err := os.MkdirAll(filepath.Join(data, "admin"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(data, "admin", "apikeys.json"), []byte(`{"version":1,"keys":[{"id":"x"`), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := rbacNode(t, data, nil, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "apikeys.json") {
		t.Fatalf("start with a garbage key file: %v", err)
	}
}

// The provider unreachable: an ID token is refused and the node's other
// identities keep working.
func TestOIDCProviderDownOnANode(t *testing.T) {
	idp := oidctest.New(t, "hoardmq-cli")
	idp.Lock()
	idp.Down = true
	idp.Unlock()
	srv, err := rbacNode(t, filepath.Join(t.TempDir(), "data"), idp, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if code, _ := call(t, srv, "GET", "/api/v1/whoami", idp.Mint(t, idp.Claims()), ""); code != 401 {
		t.Fatalf("an ID token with the provider down: %d", code)
	}
	if code, _ := call(t, srv, "GET", "/api/v1/clients", adminOps, ""); code != 200 {
		t.Fatalf("a token with the provider down: %d", code)
	}
	var evs []admin.AuditEvent
	_, raw := call(t, srv, "GET", "/api/v1/audit?limit=100", adminOps, "")
	_ = json.Unmarshal(raw, &evs)
	found := false
	for _, e := range evs {
		if e.Action == "admin.auth.failure" && e.Detail["reason"] == "oidc: provider unavailable" {
			found = true
		}
	}
	if !found {
		t.Fatalf("no refusal naming the provider in %s", raw)
	}
}
