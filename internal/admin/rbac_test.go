package admin

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/jose"
	"github.com/happydave/hoardmq/internal/oidc/oidctest"
)

// testClock is a settable time for the server's API-key decisions.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *testClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *testClock) Add(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

const (
	keymasterToken = "key-0123456789abcdef0123456789ab"
	viewerToken    = "vie-0123456789abcdef0123456789ab"
)

// rbacFixture is a server with roles, a token per role, an optional OIDC
// provider, and a records store holding the API keys; keys is a directory
// for a key file to import.
type rbacFixture struct {
	*fixture
	keys  string
	clock *testClock
	idp   *oidctest.Provider
}

func newRBACFixture(t *testing.T, withOIDC bool) *rbacFixture {
	t.Helper()
	dir := t.TempDir()
	write := func(name, value string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(value+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	cfg := config.Admin{
		Roles: []config.AdminRole{
			{Name: "viewer", Scopes: []string{"clients:read"}},
			{Name: "keymaster", Scopes: []string{"apikeys:read", "apikeys:write"}},
			{Name: "operator", Scopes: []string{"read", "clients:write", "cluster:drain"}},
		},
		Tokens: []config.AdminToken{
			{Name: "ops", TokenFile: write("ops", opsToken), Scopes: []string{"admin"}},
			{Name: "keymaster", TokenFile: write("key", keymasterToken), Scopes: []string{"clients:read"}, Roles: []string{"keymaster"}},
			{Name: "viewer", TokenFile: write("vie", viewerToken), Roles: []string{"viewer"}},
		},
	}
	f := &rbacFixture{clock: &testClock{now: time.Unix(1_800_000_000, 0)}}
	if withOIDC {
		f.idp = oidctest.New(t, "hoardmq-cli")
		cfg.OIDC = &config.AdminOIDC{Issuer: f.idp.URL, ClientIDs: []string{"hoardmq-cli"}, RoleMappings: []config.AdminRoleMapping{{Group: "hoardmq-ops", Roles: []string{"operator"}}}}
	}
	b := &fakeBackend{errFor: map[string]error{}}
	var logs bytes.Buffer
	srv, err := New(cfg, b, slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	if err != nil {
		t.Fatal(err)
	}
	srv.now = f.clock.Now
	f.keys = filepath.Join(t.TempDir(), "admin")
	b.records = localRecords()
	f.fixture = &fixture{srv: srv, b: b, log: &logs}
	return f
}

// A certificate identity holds its roles' scopes.
func TestCertificateByRole(t *testing.T) {
	cfg := config.Admin{Roles: []config.AdminRole{{Name: "viewer", Scopes: []string{"clients:read"}}},
		Certificates: []config.AdminCertificate{{CommonName: "bob", Roles: []string{"viewer"}}}}
	b := &fakeBackend{errFor: map[string]error{}}
	srv, err := New(cfg, b, nil)
	if err != nil {
		t.Fatal(err)
	}
	for path, want := range map[string]int{"/api/v1/clients": 200, "/api/v1/modules": 403} {
		req := httptest.NewRequest("GET", path, nil)
		req.TLS = certState("bob")
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, req)
		if rec.Code != want {
			t.Errorf("%s: %d, want %d", path, rec.Code, want)
		}
	}
}

// Roles grant the union of their scopes and an identity's own; the preStop
// token of the chart holds exactly its two scopes.
func TestRolesGrant(t *testing.T) {
	roles, err := NewRoles([]config.AdminRole{{Name: "viewer", Scopes: []string{"clients:read"}}, {Name: "keys", Scopes: []string{"apikeys:write"}}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := roles.Grant([]string{"cluster:read"}, []string{"viewer"})
	if err != nil || len(got) != 2 || !got[ClusterRead] || !got[ClientsRead] {
		t.Fatalf("cluster:read and viewer: %v %v", got, err)
	}
	prestop, err := roles.Grant([]string{"cluster:drain", "cluster:read"}, nil)
	if err != nil || strings.Join(Principal{Scopes: prestop}.ScopeList(), ",") != "cluster:drain,cluster:read" {
		t.Fatalf("the preStop token's scopes: %v %v", prestop, err)
	}
	if _, err := roles.Grant(nil, []string{"nope"}); err == nil {
		t.Fatal("an undefined role granted")
	}
	if _, err := NewRoles([]config.AdminRole{{Name: "bad", Scopes: []string{"root"}}}); err == nil {
		t.Fatal("a role with an unknown scope")
	}
	read, _ := Expand([]string{"read"})
	if !read[APIKeysRead] || read[APIKeysWrite] {
		t.Fatalf("read: %v", read)
	}
}

// Workitem AC 1: on every route that declares a scope, an identity whose
// only role holds every scope but that one is refused with 403 naming it,
// and the refusal is audited; one whose role holds exactly that scope is
// not refused with 403.
func TestRoleLackingAScopeIsRefused(t *testing.T) {
	var defs []config.AdminRole
	var tokens []config.AdminToken
	dir := t.TempDir()
	i := 0
	for _, s := range AllScopes {
		var but []string
		for _, o := range AllScopes {
			if o != s {
				but = append(but, string(o))
			}
		}
		for _, grant := range [][]string{but, {string(s)}} {
			i++
			name := "r" + string(rune('a'+i/26)) + string(rune('a'+i%26))
			defs = append(defs, config.AdminRole{Name: name, Scopes: grant})
			file := filepath.Join(dir, name)
			if err := os.WriteFile(file, []byte(tokenFor(name)), 0o600); err != nil {
				t.Fatal(err)
			}
			tokens = append(tokens, config.AdminToken{Name: name, TokenFile: file, Roles: []string{name}})
		}
	}
	b := &fakeBackend{errFor: map[string]error{}, records: localRecords()}
	srv, err := New(config.Admin{Roles: defs, Tokens: tokens}, b, nil)
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{srv: srv, b: b, log: &bytes.Buffer{}}
	byScope := map[Scope][2]string{}
	for j, s := range AllScopes {
		byScope[s] = [2]string{defs[2*j].Name, defs[2*j+1].Name}
	}
	for _, rt := range srv.Routes() {
		if rt.Scope == "" {
			continue
		}
		path := strings.NewReplacer("{clientId}", "c1", "{moduleId}", "metrics.internal", "{keyId}", "000000000000").Replace(rt.Path)
		if rt.Path == "/subscriptions" || rt.Path == "/retained" && rt.Method == "DELETE" {
			path += "?topic=t"
		}
		body := ""
		if rt.Method == "POST" && rt.Path == "/apikeys" {
			body = `{"name":"k","scopes":["` + string(rt.Scope) + `"]}`
		}
		names := byScope[rt.Scope]
		lacking, holding := names[0], names[1]
		rec := f.do(rt.Method, Prefix+path, tokenFor(lacking), body)
		if rec.Code != 403 || !strings.Contains(rec.Body.String(), string(rt.Scope)) {
			t.Errorf("%s %s with every scope but %s: %d %s", rt.Method, rt.Path, rt.Scope, rec.Code, rec.Body)
			continue
		}
		e, _ := f.b.lastAudit(t)
		wantAction := rt.Action
		if wantAction == "" {
			wantAction = "admin.denied"
		}
		if e.Actor != "token:"+lacking || e.Action != wantAction || e.Outcome != "denied" {
			t.Errorf("%s %s: denial audit %+v", rt.Method, rt.Path, e)
		}
		if rec := f.do(rt.Method, Prefix+path, tokenFor(holding), body); rec.Code == 403 {
			t.Errorf("%s %s with exactly %s: 403 %s", rt.Method, rt.Path, rt.Scope, rec.Body)
		}
	}
}

// tokenFor is a 32-byte token derived from a name.
func tokenFor(name string) string {
	return (name + "-token-0123456789abcdef0123456789")[:32]
}

func createKey(t *testing.T, f *rbacFixture, token, body string) (CreatedAPIKey, int) {
	t.Helper()
	rec := f.do("POST", "/api/v1/apikeys", token, body)
	var k CreatedAPIKey
	if rec.Code == 201 {
		if err := json.Unmarshal(rec.Body.Bytes(), &k); err != nil {
			t.Fatal(err)
		}
	}
	return k, rec.Code
}

// Workitem AC 3: a key is scoped, revocable and never logged.
func TestAPIKeyLifecycle(t *testing.T) {
	f := newRBACFixture(t, false)
	k, code := createKey(t, f, opsToken, `{"name":"ci deploys","scopes":["clients:read"]}`)
	if code != 201 || k.Key == "" || k.ID == "" || k.CreatedBy != "token:ops" || strings.Join(k.Scopes, ",") != "clients:read" {
		t.Fatalf("create: %d %+v", code, k)
	}
	created, _ := f.b.lastAudit(t)
	if created.Action != "apikey.create" || created.Target != k.ID || created.Actor != "token:ops" || created.Outcome != "ok" || created.Detail["scopes"] != "clients:read" {
		t.Fatalf("the creation's audit %+v", created)
	}
	if rec := f.do("GET", "/api/v1/clients", k.Key, ""); rec.Code != 200 {
		t.Fatalf("the key reads clients: %d", rec.Code)
	}
	rec := f.do("POST", "/api/v1/clients/c1/disconnect", k.Key, "")
	if e, _ := f.b.lastAudit(t); rec.Code != 403 || e.Actor != "apikey:"+k.ID || e.Outcome != "denied" || e.Detail["credential"] != "apikey" {
		t.Fatalf("the key's mutation: %d %+v", rec.Code, e)
	}
	// One character off, and the identifier with another key's secret.
	f.clock.Add(time.Second)
	other, _ := createKey(t, f, opsToken, `{"name":"other","scopes":["clients:read"]}`)
	last := k.Key[len(k.Key)-1]
	flipped := "A"
	if last == 'A' {
		flipped = "B"
	}
	for name, v := range map[string]string{
		"one character off":         k.Key[:len(k.Key)-1] + flipped,
		"another key's secret":      k.Key[:len(apiKeyPrefix)+apiKeyIDLen+1] + other.Key[len(apiKeyPrefix)+apiKeyIDLen+1:],
		"an identifier nobody made": apiKeyPrefix + "ffffffffffff" + k.Key[len(apiKeyPrefix)+apiKeyIDLen:],
	} {
		if rec := f.do("GET", "/api/v1/clients", v, ""); rec.Code != 401 {
			t.Errorf("%s: %d", name, rec.Code)
		}
	}
	// Revoked: refused, audited with the reason; the list shows it.
	rec = f.do("DELETE", "/api/v1/apikeys/"+k.ID, opsToken, "")
	if rec.Code != 200 {
		t.Fatalf("revoke: %d %s", rec.Code, rec.Body)
	}
	if e, _ := f.b.lastAudit(t); e.Action != "apikey.revoke" || e.Target != k.ID || e.Outcome != "ok" {
		t.Fatalf("the revocation's audit %+v", e)
	}
	if rec := f.do("GET", "/api/v1/clients", k.Key, ""); rec.Code != 401 {
		t.Fatalf("a revoked key: %d", rec.Code)
	}
	if e, lossy := f.b.lastAudit(t); e.Action != "admin.auth.failure" || e.Detail["reason"] != "apikey: revoked" || !lossy {
		t.Fatalf("the revoked key's refusal %+v", e)
	}
	rec = f.do("GET", "/api/v1/apikeys", opsToken, "")
	var list []APIKey
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil || len(list) != 2 {
		t.Fatalf("list %s", rec.Body)
	}
	if list[0].ID != k.ID || list[0].RevokedAt == nil || list[0].RevokedBy != "token:ops" || list[1].RevokedAt != nil {
		t.Fatalf("list %+v", list)
	}
	if rec := f.do("DELETE", "/api/v1/apikeys/"+k.ID, opsToken, ""); rec.Code != 200 {
		t.Fatalf("revoking again: %d", rec.Code)
	}
	if rec := f.do("DELETE", "/api/v1/apikeys/000000000000", opsToken, ""); rec.Code != 404 {
		t.Fatalf("an unknown key: %d", rec.Code)
	}
	// Never logged: the creation's record exists and names the key (above),
	// the collection holds its digest; its value is in none of them, nor in
	// the list or the document.
	stored, ok := f.b.records.Get(APIKeysCollection, k.ID)
	if !ok || !strings.Contains(string(stored.Value), keyDigest(k.Key)) {
		t.Fatal("the collection does not hold the key's digest")
	}
	file := string(stored.Value)
	audits, _ := json.Marshal(f.b.audits)
	doc := f.do("GET", "/api/v1/openapi.json", opsToken, "")
	if strings.Contains(rec.Body.String(), keyDigest(k.Key)) {
		t.Error("the list holds a key's digest")
	}
	for name, text := range map[string]string{"audit": string(audits), "log": f.log.String(), "list": rec.Body.String(), "record": file, "document": doc.Body.String()} {
		for _, key := range []string{k.Key, other.Key} {
			if strings.Contains(text, key) {
				t.Errorf("the %s holds a key", name)
			}
		}
	}
	// The revocation was confirmed at the index it was written at.
	if len(f.b.confirmed) == 0 || f.b.confirmed[0] == 0 {
		t.Fatalf("revocations confirmed at %v", f.b.confirmed)
	}
}

func TestAPIKeyExpiry(t *testing.T) {
	f := newRBACFixture(t, false)
	if _, code := createKey(t, f, opsToken, `{"name":"short","scopes":["clients:read"],"expiresInSeconds":59}`); code != 400 {
		t.Fatalf("59 s: %d", code)
	}
	k, code := createKey(t, f, opsToken, `{"name":"short","scopes":["clients:read"],"expiresInSeconds":60}`)
	if code != 201 || k.ExpiresAt == nil || !k.ExpiresAt.Equal(f.clock.Now().Add(time.Minute)) {
		t.Fatalf("create: %d %+v", code, k)
	}
	f.clock.Add(59 * time.Second)
	if rec := f.do("GET", "/api/v1/whoami", k.Key, ""); rec.Code != 200 || !strings.Contains(rec.Body.String(), `"expires"`) {
		t.Fatalf("at +59 s: %d %s", rec.Code, rec.Body)
	}
	f.clock.Add(2 * time.Second)
	if rec := f.do("GET", "/api/v1/clients", k.Key, ""); rec.Code != 401 {
		t.Fatalf("at +61 s: %d", rec.Code)
	}
	if e, _ := f.b.lastAudit(t); e.Detail["reason"] != "apikey: expired" {
		t.Fatalf("the refusal %+v", e)
	}
}

// A key holds no scope its creator does not, and a key cannot make keys.
func TestAPIKeyGrantIsBounded(t *testing.T) {
	f := newRBACFixture(t, false)
	if _, code := createKey(t, f, keymasterToken, `{"name":"ok","scopes":["clients:read"]}`); code != 201 {
		t.Fatalf("a scope the creator holds: %d", code)
	}
	for _, body := range []string{`{"name":"x","scopes":["config:write"]}`, `{"name":"x","scopes":["admin"]}`, `{"name":"x","roles":["operator"]}`} {
		rec := f.do("POST", "/api/v1/apikeys", keymasterToken, body)
		if rec.Code != 403 || !strings.Contains(rec.Body.String(), "cannot grant") {
			t.Errorf("%s: %d %s", body, rec.Code, rec.Body)
		}
		if e, _ := f.b.lastAudit(t); e.Action != "apikey.create" || e.Outcome != "denied" || e.Actor != "token:keymaster" {
			t.Errorf("%s: audit %+v", body, e)
		}
	}
	minter, code := createKey(t, f, opsToken, `{"name":"minter","scopes":["apikeys:write","clients:read"]}`)
	if code != 201 {
		t.Fatal(code)
	}
	if rec := f.do("POST", "/api/v1/apikeys", minter.Key, `{"name":"child","scopes":["clients:read"]}`); rec.Code != 403 || !strings.Contains(rec.Body.String(), "an API key cannot create") {
		t.Fatalf("a key making a key: %d %s", rec.Code, rec.Body)
	}
	for _, body := range []string{`{"scopes":["clients:read"]}`, `{"name":"x"}`, `{"name":"x","scopes":["root"]}`, `{"name":"x","roles":["nope"]}`, `{"name":"` + strings.Repeat("n", 65) + `","scopes":["clients:read"]}`, `{"name":"x","scopes":["clients:read"],"extra":1}`} {
		if _, code := createKey(t, f, opsToken, body); code != 400 {
			t.Errorf("%s: %d", body, code)
		}
	}
	// A role's scopes are stored at creation.
	k, _ := createKey(t, f, opsToken, `{"name":"op","roles":["operator"]}`)
	if !strings.Contains(strings.Join(k.Scopes, ","), "clients:write") || strings.Join(k.Roles, ",") != "operator" {
		t.Fatalf("a role's key: %+v", k)
	}
}

func TestMayGrant(t *testing.T) {
	holder := Principal{Name: "token:k", Method: "token", Scopes: map[Scope]bool{ClientsRead: true, APIKeysWrite: true}}
	if msg := mayGrant(holder, map[Scope]bool{ClientsRead: true}); msg != "" {
		t.Fatalf("a held scope: %s", msg)
	}
	if msg := mayGrant(holder, map[Scope]bool{ClientsRead: true, ConfigWrite: true}); !strings.Contains(msg, "config:write") {
		t.Fatalf("a scope not held: %q", msg)
	}
	key := holder
	key.Method = "apikey"
	if msg := mayGrant(key, map[Scope]bool{ClientsRead: true}); msg == "" {
		t.Fatal("an API key made a key")
	}
}

func TestMatchAPIKey(t *testing.T) {
	id, key := newAPIKey()
	now := time.Unix(1_800_000_000, 0)
	later := now.Add(time.Hour)
	keys := map[string]storedKey{id: {APIKey: APIKey{ID: id, Scopes: []string{"clients:read"}, ExpiresAt: &later}, Digest: keyDigest(key)}}
	if p, v, _ := matchAPIKey(key, keys, now); v != Authenticated || p.Name != "apikey:"+id || !p.Scopes[ClientsRead] {
		t.Fatalf("the key: %v %+v", v, p)
	}
	if _, v, _ := matchAPIKey(key, keys, later); v != Refused {
		t.Fatal("at its expiry")
	}
	if _, v, _ := matchAPIKey("opaque-token-0123456789abcdef", keys, now); v != NotMine {
		t.Fatal("a value not shaped as a key")
	}
	revoked := keys[id]
	revoked.RevokedAt = &now
	if _, v, reason := matchAPIKey(key, map[string]storedKey{id: revoked}, now); v != Refused || reason != "apikey: revoked" {
		t.Fatalf("revoked: %v %q", v, reason)
	}
	if _, v, reason := matchAPIKey(key[:len(key)-1]+"x", keys, now); v != Refused || reason != "apikey: wrong secret" {
		t.Fatalf("another secret: %v %q", v, reason)
	}
}

// TestAPIKeyImportFromAFile: a key file a node-local build wrote is put into
// the collection, revocations included, without overwriting a record of the
// same identifier; a file that does not parse is refused, and a missing one
// imports nothing.
func TestAPIKeyImportFromAFile(t *testing.T) {
	f := newRBACFixture(t, false)
	live, liveKey := newAPIKey()
	gone, goneKey := newAPIKey()
	taken, takenKey := newAPIKey()
	when := f.clock.Now().UTC()
	doc := keyFileDoc{Version: 1, Keys: []storedKey{
		{APIKey: APIKey{ID: live, Name: "live", Scopes: []string{"clients:read"}, CreatedBy: "token:ops", CreatedAt: when}, Digest: keyDigest(liveKey)},
		{APIKey: APIKey{ID: gone, Name: "gone", Scopes: []string{"clients:read"}, CreatedBy: "token:ops", CreatedAt: when, RevokedAt: &when, RevokedBy: "token:ops"}, Digest: keyDigest(goneKey)},
		{APIKey: APIKey{ID: taken, Name: "file", Scopes: []string{"clients:read"}, CreatedBy: "token:ops", CreatedAt: when}, Digest: keyDigest(takenKey)},
	}}
	// The cluster already holds a revoked record for taken: the file's live
	// copy must not replace it.
	held, _ := json.Marshal(storedKey{APIKey: APIKey{ID: taken, Name: "cluster", Scopes: []string{"clients:read"}, CreatedAt: when, RevokedAt: &when}, Digest: keyDigest(takenKey)})
	if _, err := f.b.records.Put(context.Background(), APIKeysCollection, taken, held, nil, "test", when); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(doc)
	if err := os.MkdirAll(f.keys, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.keys, apiKeyFile), data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := f.srv.ReadKeyFile(f.keys); err != nil {
		t.Fatal(err)
	}
	imported, kept, err := f.srv.ImportKeys(context.Background())
	if err != nil || imported != 2 || kept != 1 {
		t.Fatalf("import: %d imported, %d kept, %v", imported, kept, err)
	}
	if rec := f.do("GET", "/api/v1/clients", liveKey, ""); rec.Code != 200 {
		t.Fatalf("the imported live key: %d", rec.Code)
	}
	for name, key := range map[string]string{"the imported revoked key": goneKey, "the key the cluster revoked": takenKey} {
		if rec := f.do("GET", "/api/v1/clients", key, ""); rec.Code != 401 {
			t.Errorf("%s: %d", name, rec.Code)
		}
	}
	if k, _, _ := getKey(f.b.records, taken); k.Name != "cluster" {
		t.Fatalf("the record of %s was replaced: %+v", taken, k)
	}
	// Importing again changes nothing.
	if imported, kept, err := f.srv.ImportKeys(context.Background()); err != nil || imported != 0 || kept != 3 {
		t.Fatalf("a second import: %d imported, %d kept, %v", imported, kept, err)
	}
	bad := t.TempDir()
	if err := os.WriteFile(filepath.Join(bad, apiKeyFile), []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := f.srv.ReadKeyFile(bad); err == nil || !strings.Contains(err.Error(), apiKeyFile) {
		t.Fatalf("a garbage key file: %v", err)
	}
	empty := filepath.Join(t.TempDir(), "admin")
	if err := f.srv.ReadKeyFile(empty); err != nil {
		t.Fatal(err)
	}
	if imported, kept, err := f.srv.ImportKeys(context.Background()); err != nil || imported != 0 || kept != 0 {
		t.Fatalf("an empty directory: %d imported, %d kept, %v", imported, kept, err)
	}
}

// TestAPIKeyRefusedOnANodeNotCaughtUp: a node that is not ready refuses a
// valid key, says why, and still serves every other credential.
func TestAPIKeyRefusedOnANodeNotCaughtUp(t *testing.T) {
	f := newRBACFixture(t, false)
	k, code := createKey(t, f, opsToken, `{"name":"k","scopes":["clients:read"]}`)
	if code != 201 {
		t.Fatal(code)
	}
	f.b.mu.Lock()
	f.b.notReady = "no renewal with the leader since this node started"
	f.b.mu.Unlock()
	if rec := f.do("GET", "/api/v1/clients", k.Key, ""); rec.Code != 401 {
		t.Fatalf("a valid key on a node not caught up: %d", rec.Code)
	}
	if e, _ := f.b.lastAudit(t); e.Action != "admin.auth.failure" || e.Detail["reason"] != "apikey: this node is not caught up with the cluster" {
		t.Fatalf("the refusal %+v", e)
	}
	if rec := f.do("GET", "/api/v1/clients", opsToken, ""); rec.Code != 200 {
		t.Fatalf("a token on a node not caught up: %d", rec.Code)
	}
	f.b.mu.Lock()
	f.b.notReady = ""
	f.b.mu.Unlock()
	if rec := f.do("GET", "/api/v1/clients", k.Key, ""); rec.Code != 200 {
		t.Fatalf("the key once caught up: %d", rec.Code)
	}
}

// TestAPIKeyRevocationNotConfirmed: a revocation a live node has not
// confirmed answers 503 naming it; the key is revoked in the collection, and
// revoking again once every node confirms answers 200.
func TestAPIKeyRevocationNotConfirmed(t *testing.T) {
	f := newRBACFixture(t, false)
	k, _ := createKey(t, f, opsToken, `{"name":"k","scopes":["clients:read"]}`)
	f.b.mu.Lock()
	f.b.unconfirmed = []string{"n3"}
	f.b.mu.Unlock()
	rec := f.do("DELETE", "/api/v1/apikeys/"+k.ID, opsToken, "")
	var p Problem
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil || rec.Code != 503 || !strings.Contains(p.Detail, "n3") {
		t.Fatalf("an unconfirmed revocation: %d %s", rec.Code, rec.Body)
	}
	if stored, _, _ := getKey(f.b.records, k.ID); stored.RevokedAt == nil {
		t.Fatal("the revocation was not written")
	}
	written := f.b.confirmed[len(f.b.confirmed)-1]
	f.b.mu.Lock()
	f.b.unconfirmed = nil
	f.b.mu.Unlock()
	if rec := f.do("DELETE", "/api/v1/apikeys/"+k.ID, opsToken, ""); rec.Code != 200 {
		t.Fatalf("revoking again once confirmed: %d %s", rec.Code, rec.Body)
	}
	if again := f.b.confirmed[len(f.b.confirmed)-1]; again != written {
		t.Fatalf("the second confirmation asked for %d, the revocation is at %d", again, written)
	}
}

// Without an open store the key routes answer 409 and no key authenticates.
func TestAPIKeysWithoutAStore(t *testing.T) {
	f := newFixture(t, 0)
	for _, c := range [][2]string{{"GET", "/api/v1/apikeys"}, {"POST", "/api/v1/apikeys"}, {"DELETE", "/api/v1/apikeys/000000000000"}} {
		body := ""
		if c[0] == "POST" {
			body = `{"name":"x","scopes":["clients:read"]}`
		}
		if rec := f.do(c[0], c[1], opsToken, body); rec.Code != 409 {
			t.Errorf("%s %s: %d", c[0], c[1], rec.Code)
		}
	}
}

// Workitem AC 2 at the API: an ID token from the configured provider is an
// identity with the roles its groups map to.
func TestOIDCAuthentication(t *testing.T) {
	f := newRBACFixture(t, true)
	if n := f.idp.Count("discovery") + f.idp.Count("jwks"); n != 0 {
		t.Fatalf("New contacted the provider %d times", n)
	}
	tok := f.idp.Mint(t, f.idp.Claims())
	rec := f.do("GET", "/api/v1/whoami", tok, "")
	var who WhoAmI
	if rec.Code != 200 || json.Unmarshal(rec.Body.Bytes(), &who) != nil {
		t.Fatalf("whoami: %d %s", rec.Code, rec.Body)
	}
	if who.Name != "oidc:00u-alice" || who.Method != "oidc" || strings.Join(who.Roles, ",") != "operator" || who.Expires == nil ||
		!strings.Contains(strings.Join(who.Scopes, ","), "clients:write") || strings.Contains(strings.Join(who.Scopes, ","), "config:write") {
		t.Fatalf("whoami %+v", who)
	}
	if rec := f.do("POST", "/api/v1/clients/c1/disconnect", tok, ""); rec.Code != 200 {
		t.Fatalf("an operator's disconnect: %d", rec.Code)
	}
	if e, _ := f.b.lastAudit(t); e.Actor != "oidc:00u-alice" || e.Detail["credential"] != "oidc" {
		t.Fatalf("audit %+v", e)
	}
	if rec := f.do("POST", "/api/v1/config/validate", tok, `{}`); rec.Code != 403 {
		t.Fatalf("a scope the role lacks: %d", rec.Code)
	}
	// Groups no mapping names: refused, with the reason audited.
	f.idp.Lock()
	f.idp.User.Groups = []string{"everyone"}
	f.idp.Unlock()
	unmapped := f.idp.Mint(t, f.idp.Claims())
	if rec := f.do("GET", "/api/v1/whoami", unmapped, ""); rec.Code != 401 {
		t.Fatalf("an unmapped identity: %d", rec.Code)
	}
	if e, _ := f.b.lastAudit(t); e.Detail["reason"] != "oidc: no role mapped" {
		t.Fatalf("the unmapped identity's refusal %+v", e)
	}
	// An expired token of a mapped identity: refused, the reason audited,
	// the token nowhere.
	f.idp.Lock()
	f.idp.User.Groups = []string{"hoardmq-ops"}
	f.idp.Unlock()
	c := f.idp.Claims()
	c["exp"] = time.Now().Add(-10 * time.Minute).Unix()
	c["iat"] = time.Now().Add(-time.Hour).Unix()
	expired := f.idp.Mint(t, c)
	if rec := f.do("GET", "/api/v1/clients", expired, ""); rec.Code != 401 {
		t.Fatalf("an expired token: %d", rec.Code)
	}
	if e, _ := f.b.lastAudit(t); e.Detail["reason"] != "oidc: token expired" {
		t.Fatalf("the expired token's refusal %+v", e)
	}
	// A token from another issuer is not the OIDC authenticator's: plain 401.
	other := oidctest.New(t, "hoardmq-cli")
	if rec := f.do("GET", "/api/v1/clients", other.Mint(t, other.Claims()), ""); rec.Code != 401 {
		t.Fatalf("another issuer's token: %d", rec.Code)
	}
	if e, _ := f.b.lastAudit(t); e.Detail["reason"] != "" {
		t.Fatalf("another issuer's token was decided: %+v", e)
	}
	audits, _ := json.Marshal(f.b.audits)
	for _, secret := range []string{tok, unmapped, expired} {
		if strings.Contains(string(audits), secret) || strings.Contains(f.log.String(), secret) {
			t.Fatal("an ID token reached the audit or the log")
		}
	}
	// The configured tokens are unaffected by the provider going down.
	f.idp.Lock()
	f.idp.Down = true
	f.idp.Unlock()
	if rec := f.do("GET", "/api/v1/clients", opsToken, ""); rec.Code != 200 {
		t.Fatalf("a token with the provider down: %d", rec.Code)
	}
}

// The identity decision over verified claims.
func TestOIDCIdentity(t *testing.T) {
	roles, _ := NewRoles([]config.AdminRole{{Name: "operator", Scopes: []string{"clients:write"}}, {Name: "viewer", Scopes: []string{"clients:read"}}})
	m := idpMapping{usernameClaim: "sub", groupsClaim: "groups", roles: roles,
		byGroup: map[string][]string{"hoardmq-ops": {"operator"}, "eng": {"viewer", "operator"}}}
	claims := func(groups any) jose.Claims {
		c := jose.Claims{"sub": "alice", "exp": json.Number("1800000000")}
		if groups != nil {
			c["groups"] = groups
		}
		return c
	}
	cases := []struct {
		name   string
		c      jose.Claims
		roles  string
		reason string
	}{
		{"two groups", claims([]any{"everyone", "hoardmq-ops"}), "operator", ""},
		{"overlapping mappings", claims([]any{"eng", "hoardmq-ops"}), "operator,viewer", ""},
		{"one group as a string", claims("hoardmq-ops"), "operator", ""},
		{"unmapped groups", claims([]any{"everyone"}), "", "oidc: no role mapped"},
		{"no groups claim", claims(nil), "", "oidc: no role mapped"},
		{"groups not names", claims([]any{1, 2}), "", "not a list of names"},
		{"no username", jose.Claims{"groups": []any{"hoardmq-ops"}}, "", "oidc: no sub claim"},
	}
	for _, c := range cases {
		p, reason := oidcIdentity(c.c, m)
		if !strings.Contains(reason, c.reason) || c.reason == "" && reason != "" || strings.Join(p.Roles, ",") != c.roles {
			t.Errorf("%s: %+v %q", c.name, p, reason)
		}
	}
	p, _ := oidcIdentity(claims([]any{"hoardmq-ops"}), m)
	if p.Name != "oidc:alice" || !p.Scopes[ClientsWrite] || p.Scopes[ClientsRead] || p.Expires.Unix() != 1800000000 {
		t.Fatalf("the principal %+v", p)
	}
}

// Every refusal the chain decides carries its reason to the audit record,
// and no credential.
func TestRefusalReasons(t *testing.T) {
	f := newRBACFixture(t, true)
	k, _ := createKey(t, f, opsToken, `{"name":"k","scopes":["clients:read"]}`)
	_ = f.do("DELETE", "/api/v1/apikeys/"+k.ID, opsToken, "")
	c := f.idp.Claims()
	c["aud"] = "someone-else"
	cases := map[string]string{
		k.Key:                 "apikey: revoked",
		f.idp.Mint(t, c):      "oidc: audience mismatch",
		"not-a-known-token-0": "",
	}
	for cred, reason := range cases {
		before := f.b.auditCount()
		if rec := f.do("GET", "/api/v1/clients", cred, ""); rec.Code != 401 {
			t.Fatalf("%q: %d", reason, rec.Code)
		}
		if f.b.auditCount() != before+1 {
			t.Fatalf("%q: not audited", reason)
		}
		e, _ := f.b.lastAudit(t)
		if e.Detail["reason"] != reason {
			t.Errorf("reason %q, want %q", e.Detail["reason"], reason)
		}
		raw, _ := json.Marshal(e)
		if strings.Contains(string(raw), cred) {
			t.Errorf("%q: the credential is in the audit record", reason)
		}
	}
	// A provider that has never answered: refused, with that reason.
	down := newRBACFixture(t, true)
	down.idp.Lock()
	down.idp.Down = true
	down.idp.Unlock()
	if rec := down.do("GET", "/api/v1/clients", down.idp.Mint(t, down.idp.Claims()), ""); rec.Code != 401 {
		t.Fatalf("the provider down: %d", rec.Code)
	}
	if e, _ := down.b.lastAudit(t); e.Detail["reason"] != "oidc: provider unavailable" {
		t.Errorf("the provider down: %+v", e)
	}
	// A certificate with no identity carries its reason too.
	req := httptest.NewRequest("GET", "/api/v1/clients", nil)
	f2 := newFixture(t, 0)
	req.TLS = certState("mallory")
	f2.srv.ServeHTTP(httptest.NewRecorder(), req)
	if e, _ := f2.b.lastAudit(t); e.Detail["reason"] != "certificate: no identity" {
		t.Errorf("certificate refusal %+v", e)
	}
}

// certState is a TLS state whose verified chain is a certificate for cn.
func certState(cn string) *tls.ConnectionState {
	leaf := &x509.Certificate{Subject: pkix.Name{CommonName: cn}}
	return &tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf}, VerifiedChains: [][]*x509.Certificate{{leaf}}}
}
