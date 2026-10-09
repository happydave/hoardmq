package admin

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/schema"
)

// fakeBackend answers from canned data and records what it was asked to do
// and what it was asked to audit.
type fakeBackend struct {
	mu           sync.Mutex
	audits       []AuditEvent
	lossy        []bool
	disconnected []string
	drains       int
	toggled      []string
	errFor       map[string]error
	records      RecordStore
	configured   config.Limits
	reload       ConfigReload
	reloads      int
	// forwards are the requests Forward was asked to send, and forwardTo the
	// answer it gives (or forwardErr).
	forwards   []ForwardRequest
	forwardTo  []string
	forwardAns ForwardAnswer
	forwardErr error
	// notReady, when set, is why KeysReady says no; unconfirmed is what
	// ConfirmApplied answers, and confirmed the indexes it was asked.
	notReady    string
	unconfirmed []string
	confirmed   []uint64
}

func (f *fakeBackend) Forward(_ context.Context, node string, req ForwardRequest) (ForwardAnswer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.forwards = append(f.forwards, req)
	f.forwardTo = append(f.forwardTo, node)
	return f.forwardAns, f.forwardErr
}

// KeysReady is ready unless notReady says why not.
func (f *fakeBackend) KeysReady() (bool, string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.notReady == "", f.notReady
}

// ConfirmApplied records the index asked and answers unconfirmed.
func (f *fakeBackend) ConfirmApplied(_ context.Context, index uint64) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.confirmed = append(f.confirmed, index)
	return f.unconfirmed, nil
}

func (f *fakeBackend) forwardCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.forwards)
}

func (f *fakeBackend) Records() RecordStore { return f.records }

// Limits reads the administered record from the store, as the broker does.
func (f *fakeBackend) Limits() LimitsView {
	v := LimitsView{Namespace: config.DefaultLimitsKey, Configured: RatesOf(f.configured), InForce: RatesOf(f.configured)}
	if f.records != nil {
		if rec, ok := f.records.Get(config.LimitsCollection, config.DefaultLimitsKey); ok {
			_ = json.Unmarshal(rec.Value, &v.Administered)
			v.InForce, v.Version = RatesOf(f.configured.With(v.Administered)), rec.Version
		}
	}
	return v
}

func (f *fakeBackend) Overload() OverloadView {
	return OverloadView{NodeLevel: 2, ClusterLevel: 3, Indicators: map[string]int64{"queued_messages": 7}, Peers: map[string]uint8{"n2": 3}}
}

func (f *fakeBackend) fail(id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.errFor[id]
}

func (f *fakeBackend) Node() string { return "n1" }
func (f *fakeBackend) ScanClients(_ context.Context, search, after, owner string, limit int) ([]ClientListItem, bool, error) {
	all := []string{"a1", "a2", "b1"}
	var out []ClientListItem
	for _, id := range all {
		if id > after && strings.Contains(id, search) && (owner == "" || owner == "n1") {
			out = append(out, ClientListItem{ClientID: id, Node: "n1"})
		}
	}
	if len(out) > limit {
		return out[:limit], true, nil
	}
	return out, false, nil
}
func (f *fakeBackend) Client(id string) (ClientDetail, error) {
	if err := f.fail(id); err != nil {
		return ClientDetail{}, err
	}
	return ClientDetail{Session: ClientSummary{ClientID: id}}, nil
}
func (f *fakeBackend) Subscriptions(id string) ([]Subscription, error) {
	return []Subscription{{Filter: "a/#", QoS: 1}}, f.fail(id)
}
func (f *fakeBackend) Disconnect(id string) error {
	if err := f.fail(id); err != nil {
		return err
	}
	f.mu.Lock()
	f.disconnected = append(f.disconnected, id)
	f.mu.Unlock()
	return nil
}
func (f *fakeBackend) Invalidate(_ context.Context, id string) (Invalidated, error) {
	return Invalidated{ClientID: id, Durable: true}, f.fail(id)
}
func (f *fakeBackend) Matchers(string) []Matcher { return nil }
func (f *fakeBackend) Retained(string, string, int) ([]RetainedMessage, bool) {
	return []RetainedMessage{{Topic: "r/1", Payload: []byte("x"), PayloadBytes: 1}}, false
}
func (f *fakeBackend) DeleteRetained(context.Context, string) (Durability, error) {
	return Durability{Durable: true}, nil
}
func (f *fakeBackend) Cluster() ClusterState { return ClusterState{Node: "n1"} }
func (f *fakeBackend) Shards() ShardMap      { return ShardMap{} }
func (f *fakeBackend) Lease(id string) (Lease, error) {
	return Lease{ClientID: id, Owner: "n1"}, f.fail(id)
}
func (f *fakeBackend) DrainStatus() DrainStatus { return DrainStatus{} }
func (f *fakeBackend) Drain() DrainStatus {
	f.mu.Lock()
	f.drains++
	f.mu.Unlock()
	return DrainStatus{Draining: true}
}
func (f *fakeBackend) RunningConfig() ([]byte, error) {
	return []byte(`{"version":1,"cluster":{"auth":{"secretFile":"/s"}},"modules":{"auth.ldap":{"bindPassword":"hunter2","url":"ldap://x"}}}`), nil
}
func (f *fakeBackend) ReloadConfig() ConfigReload {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reloads++
	return f.reload
}
func (f *fakeBackend) ValidateConfig(doc []byte) []ConfigProblem {
	if !json.Valid(doc) {
		return []ConfigProblem{{Pointer: "/", Message: "invalid JSON"}}
	}
	return nil
}
func (f *fakeBackend) StartTrace(id string, limit int) (TraceStarted, error) {
	return TraceStarted{ClientID: id, Limit: limit}, f.fail(id)
}
func (f *fakeBackend) StopTrace(id string) ([]TraceEvent, error) { return []TraceEvent{}, f.fail(id) }
func (f *fakeBackend) Trace(id string) ([]TraceEvent, error)     { return []TraceEvent{}, f.fail(id) }
func (f *fakeBackend) Modules() []Module {
	return []Module{{ID: "metrics.internal", Family: "metrics", Enabled: true, Toggleable: true}}
}
func (f *fakeBackend) SetModuleEnabled(id string, on bool) error {
	if err := f.fail(id); err != nil {
		return err
	}
	f.mu.Lock()
	f.toggled = append(f.toggled, id)
	f.mu.Unlock()
	return nil
}
func (f *fakeBackend) AuditTail(int) ([]AuditEvent, error) { return []AuditEvent{}, nil }
func (f *fakeBackend) Audit(e AuditEvent, lossy bool) {
	f.mu.Lock()
	f.audits = append(f.audits, e)
	f.lossy = append(f.lossy, lossy)
	f.mu.Unlock()
}

func (f *fakeBackend) lastAudit(t *testing.T) (AuditEvent, bool) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.audits) == 0 {
		t.Fatal("no audit record")
	}
	return f.audits[len(f.audits)-1], f.lossy[len(f.lossy)-1]
}

func (f *fakeBackend) auditCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.audits)
}

// Token values of 32 bytes, and one that differs from ops only in its last
// character.
const (
	opsToken    = "ops-0123456789abcdef0123456789ab"
	readToken   = "read-123456789abcdef0123456789ab"
	clientsRead = "cli-0123456789abcdef0123456789ab"
	drainToken  = "drn-0123456789abcdef0123456789ab"
	nearOps     = "ops-0123456789abcdef0123456789ac"
)

type fixture struct {
	srv *Server
	b   *fakeBackend
	log *bytes.Buffer
}

func newFixture(t *testing.T, maxBody int64) *fixture {
	t.Helper()
	dir := t.TempDir()
	write := func(name, value string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(value+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	cfg := config.Admin{MaxBodyBytes: maxBody,
		Tokens: []config.AdminToken{
			{Name: "ops", TokenFile: write("ops", opsToken), Scopes: []string{"admin"}},
			{Name: "reader", TokenFile: write("read", readToken), Scopes: []string{"read"}},
			{Name: "clients", TokenFile: write("cli", clientsRead), Scopes: []string{"clients:read"}},
			{Name: "prestop", TokenFile: write("drn", drainToken), Scopes: []string{"cluster:drain"}},
		},
		Certificates: []config.AdminCertificate{{CommonName: "alice", Scopes: []string{"read"}}},
	}
	b := &fakeBackend{errFor: map[string]error{}}
	var logs bytes.Buffer
	srv, err := New(cfg, b, slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{srv: srv, b: b, log: &logs}
}

// do sends one request through the whole server: authentication, routing,
// authorization, the handler and the audit.
func (f *fixture) do(method, path, token string, body string) *httptest.ResponseRecorder {
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, r)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	f.srv.ServeHTTP(rec, req)
	return rec
}

func TestTokenAuthentication(t *testing.T) {
	f := newFixture(t, 0)
	if rec := f.do("GET", "/api/v1/clients", opsToken, ""); rec.Code != 200 {
		t.Fatalf("a configured token: %d %s", rec.Code, rec.Body)
	}
	var first string
	for _, c := range []struct{ name, path, header string }{
		{"no header", "/api/v1/clients", ""},
		{"another scheme", "/api/v1/clients", "Basic b3BzOnB3"},
		{"a configured token under another scheme", "/api/v1/clients", "Token " + opsToken},
		{"unknown token", "/api/v1/clients", "Bearer nobody-0123456789abcdef012345"},
		{"one character off", "/api/v1/clients", "Bearer " + nearOps},
		{"empty bearer", "/api/v1/clients", "Bearer "},
		{"unknown path", "/api/v1/nope", ""},
		{"outside the API", "/metrics", ""},
	} {
		req := httptest.NewRequest("GET", c.path, nil)
		if c.header != "" {
			req.Header.Set("Authorization", c.header)
		}
		rec := httptest.NewRecorder()
		before := f.b.auditCount()
		f.srv.ServeHTTP(rec, req)
		if rec.Code != 401 || !strings.HasPrefix(rec.Header().Get("WWW-Authenticate"), "Bearer") {
			t.Errorf("%s: %d %q", c.name, rec.Code, rec.Header().Get("WWW-Authenticate"))
		}
		if first == "" {
			first = rec.Body.String()
		} else if rec.Body.String() != first {
			t.Errorf("%s: a different body for a refusal: %q vs %q", c.name, rec.Body, first)
		}
		if f.b.auditCount() != before+1 {
			t.Errorf("%s: the refusal was not audited", c.name)
		} else if e, lossy := f.b.lastAudit(t); e.Action != "admin.auth.failure" || e.Outcome != "unauthenticated" || !lossy {
			t.Errorf("%s: audit %+v lossy=%v", c.name, e, lossy)
		}
	}
	if strings.Contains(first, "clients") || strings.Contains(first, "nope") {
		t.Errorf("the refusal names the resource: %q", first)
	}
}

// The token's value reaches no log, audit record or response: asserted after
// the audit record of the same request is shown to exist and name the
// identity.
func TestTokenNeverEchoed(t *testing.T) {
	f := newFixture(t, 0)
	rec := f.do("POST", "/api/v1/clients/c1/disconnect", opsToken, "")
	if rec.Code != 200 {
		t.Fatalf("disconnect %d %s", rec.Code, rec.Body)
	}
	e, _ := f.b.lastAudit(t)
	if e.Actor != "token:ops" || e.Action != "client.disconnect" || e.Target != "c1" || e.Detail["request"] != rec.Header().Get("X-Request-Id") || e.Detail["request"] == "" {
		t.Fatalf("audit %+v", e)
	}
	refused := f.do("GET", "/api/v1/clients", nearOps, "")
	all, _ := json.Marshal(f.b.audits)
	for name, text := range map[string]string{"audit": string(all), "log": f.log.String(), "response": rec.Body.String() + refused.Body.String()} {
		for _, secret := range []string{opsToken, nearOps} {
			if strings.Contains(text, secret) {
				t.Errorf("the %s holds a token value", name)
			}
		}
	}
}

func TestCertificateAuthentication(t *testing.T) {
	f := newFixture(t, 0)
	leaf := &x509.Certificate{Subject: pkix.Name{CommonName: "alice"}}
	stranger := &x509.Certificate{Subject: pkix.Name{CommonName: "mallory"}}
	send := func(peer []*x509.Certificate, verified [][]*x509.Certificate, token string) int {
		req := httptest.NewRequest("GET", "/api/v1/clients", nil)
		req.TLS = &tls.ConnectionState{PeerCertificates: peer, VerifiedChains: verified}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		rec := httptest.NewRecorder()
		f.srv.ServeHTTP(rec, req)
		return rec.Code
	}
	if code := send([]*x509.Certificate{leaf}, [][]*x509.Certificate{{leaf}}, ""); code != 200 {
		t.Fatalf("a verified, configured certificate: %d", code)
	}
	if f.b.auditCount() != 0 {
		t.Fatal("a read was audited")
	}
	if code := send([]*x509.Certificate{stranger}, [][]*x509.Certificate{{stranger}}, ""); code != 401 {
		t.Fatalf("a verified certificate with no identity: %d", code)
	}
	// Presented and not verified: not an identity, and a token does not
	// stand in for the certificate the client chose to present.
	if code := send([]*x509.Certificate{leaf}, nil, ""); code != 401 {
		t.Fatalf("an unverified certificate: %d", code)
	}
	if code := send([]*x509.Certificate{stranger}, [][]*x509.Certificate{{stranger}}, opsToken); code != 401 {
		t.Fatalf("an unmapped certificate with a token: %d", code)
	}
	rec := f.do("POST", "/api/v1/clients/c1/disconnect", "", "")
	if rec.Code != 401 {
		t.Fatalf("no credential: %d", rec.Code)
	}
	req := httptest.NewRequest("POST", "/api/v1/modules/metrics.internal/disable", nil)
	req.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf}, VerifiedChains: [][]*x509.Certificate{{leaf}}}
	rec = httptest.NewRecorder()
	f.srv.ServeHTTP(rec, req)
	if e, _ := f.b.lastAudit(t); rec.Code != 403 || e.Actor != "cert:alice" || e.Outcome != "denied" {
		t.Fatalf("a read-scoped certificate's mutation: %d %+v", rec.Code, e)
	}
}

func TestAuthorization(t *testing.T) {
	f := newFixture(t, 0)
	if rec := f.do("GET", "/api/v1/clients", readToken, ""); rec.Code != 200 {
		t.Fatalf("read token, list: %d", rec.Code)
	}
	rec := f.do("POST", "/api/v1/clients/c1/disconnect", readToken, "")
	if rec.Code != 403 || !strings.Contains(rec.Body.String(), "clients:write") {
		t.Fatalf("read token, disconnect: %d %s", rec.Code, rec.Body)
	}
	e, lossy := f.b.lastAudit(t)
	if e.Actor != "token:reader" || e.Action != "client.disconnect" || e.Outcome != "denied" || e.Target != "c1" || lossy {
		t.Fatalf("denied audit %+v lossy=%v", e, lossy)
	}
	if len(f.b.disconnected) != 0 {
		t.Fatal("a denied disconnect reached the backend")
	}
	// read includes config:read, not config:write: a reload is refused before
	// the backend, and audited.
	rec = f.do("POST", "/api/v1/config/reload", readToken, "")
	if rec.Code != 403 || !strings.Contains(rec.Body.String(), "config:write") {
		t.Fatalf("read token, reload: %d %s", rec.Code, rec.Body)
	}
	if e, _ := f.b.lastAudit(t); e.Actor != "token:reader" || e.Action != "config.reload" || e.Outcome != "denied" {
		t.Fatalf("denied reload audit %+v", e)
	}
	f.b.mu.Lock()
	reloads := f.b.reloads
	f.b.mu.Unlock()
	if reloads != 0 {
		t.Fatal("a denied reload reached the backend")
	}
	// clients:read is not read: the modules group is refused, and audited.
	if rec := f.do("GET", "/api/v1/modules", clientsRead, ""); rec.Code != 403 {
		t.Fatalf("clients:read, modules: %d", rec.Code)
	}
	if e, _ := f.b.lastAudit(t); e.Action != "admin.denied" || e.Outcome != "denied" || e.Actor != "token:clients" {
		t.Fatalf("denied read audit %+v", e)
	}
	// The drain scope alone starts a drain and reads nothing.
	if rec := f.do("GET", "/api/v1/cluster/drain", drainToken, ""); rec.Code != 403 {
		t.Fatalf("drain token, status: %d", rec.Code)
	}
	if rec := f.do("POST", "/api/v1/cluster/drain", drainToken, ""); rec.Code != 202 || f.b.drains != 1 {
		t.Fatalf("drain token, start: %d drains=%d", rec.Code, f.b.drains)
	}
	if e, _ := f.b.lastAudit(t); e.Action != "cluster.drain" || e.Outcome != "ok" || e.Target != "n1" {
		t.Fatalf("drain audit %+v", e)
	}
	// Any authenticated principal reads the document; the drain token too.
	if rec := f.do("GET", "/api/v1/openapi.json", drainToken, ""); rec.Code != 200 {
		t.Fatalf("openapi: %d", rec.Code)
	}
}

// The authorization predicate, driven with the arguments it exists to refuse.
func TestAllowed(t *testing.T) {
	read, _ := Expand([]string{"read"})
	cases := []struct {
		p    Principal
		need Scope
		want bool
	}{
		{Principal{Name: "token:r", Scopes: read}, ClientsRead, true},
		{Principal{Name: "token:r", Scopes: read}, ClientsWrite, false},
		{Principal{Name: "token:r", Scopes: read}, ClusterDrain, false},
		{Principal{Name: "token:none", Scopes: map[Scope]bool{}}, ClientsRead, false},
		{Principal{}, "", false},
		{Principal{Name: "token:none"}, "", true},
		{Principal{Scopes: map[Scope]bool{ClientsRead: true}}, ClientsRead, true},
	}
	for i, c := range cases {
		if got := Allowed(c.p, c.need); got != c.want {
			t.Errorf("case %d: Allowed(%v, %q) = %v", i, c.p.ScopeList(), c.need, got)
		}
	}
}

func TestExpand(t *testing.T) {
	read, err := Expand([]string{"read"})
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range AllScopes {
		if read[s] != strings.HasSuffix(string(s), ":read") {
			t.Errorf("read and %s: %v", s, read[s])
		}
	}
	all, _ := Expand([]string{"admin"})
	if len(all) != len(AllScopes) {
		t.Errorf("admin expands to %d of %d", len(all), len(AllScopes))
	}
	one, _ := Expand([]string{"cluster:drain"})
	if len(one) != 1 || !one[ClusterDrain] {
		t.Errorf("cluster:drain expands to %v", one)
	}
	if _, err := Expand([]string{"root"}); err == nil {
		t.Error("an unknown scope expanded")
	}
}

// The token predicate, driven with the values it exists to refuse.
func TestMatchToken(t *testing.T) {
	entries := []tokenEntry{
		{digest: sha256.Sum256([]byte(opsToken)), principal: Principal{Name: "token:ops"}},
		{digest: sha256.Sum256([]byte(readToken)), principal: Principal{Name: "token:reader"}},
	}
	for _, c := range []struct {
		value string
		want  string
	}{
		{opsToken, "token:ops"},
		{readToken, "token:reader"},
		{nearOps, ""},
		{opsToken[:len(opsToken)-1], ""},
		{opsToken + "x", ""},
		{"", ""},
	} {
		p, ok := matchToken([]byte(c.value), entries)
		if ok != (c.want != "") || p.Name != c.want {
			t.Errorf("%q: %q %v", c.value, p.Name, ok)
		}
	}
	if _, ok := matchToken([]byte(opsToken), nil); ok {
		t.Error("no entries matched a token")
	}
}

// The certificate predicate reads verified chains only.
func TestMatchCertificate(t *testing.T) {
	alice := &x509.Certificate{Subject: pkix.Name{CommonName: "alice"}}
	byCN := map[string]Principal{"alice": {Name: "cert:alice"}}
	if p, ok := matchCertificate([][]*x509.Certificate{{alice}}, byCN); !ok || p.Name != "cert:alice" {
		t.Fatalf("verified alice: %v %v", p, ok)
	}
	if _, ok := matchCertificate(nil, byCN); ok {
		t.Fatal("no verified chain matched")
	}
	if _, ok := matchCertificate([][]*x509.Certificate{{{Subject: pkix.Name{CommonName: "bob"}}}}, byCN); ok {
		t.Fatal("an unmapped common name matched")
	}
}

// Every route declares a scope from the closed set (the document, any
// principal, excepted), and exactly the routes that are not GETs are audited.
func TestEveryRouteDeclaresAScope(t *testing.T) {
	f := newFixture(t, 0)
	known := map[Scope]bool{}
	for _, s := range AllScopes {
		known[s] = true
	}
	for _, rt := range f.srv.Routes() {
		if rt.Path == "/openapi.json" || rt.Path == "/whoami" {
			if rt.Scope != "" {
				t.Errorf("%s needs %q", rt.Path, rt.Scope)
			}
			continue
		}
		if !known[rt.Scope] {
			t.Errorf("%s %s: scope %q", rt.Method, rt.Path, rt.Scope)
		}
		if (rt.Method != "GET") != (rt.Action != "") {
			t.Errorf("%s %s: action %q", rt.Method, rt.Path, rt.Action)
		}
		if rt.Action != "" && rt.target == nil {
			t.Errorf("%s %s: audited with no target", rt.Method, rt.Path)
		}
	}
}

// The schema's scope enum is the scopes and the two shorthands.
func TestScopesMatchSchema(t *testing.T) {
	var doc struct {
		Defs map[string]struct {
			Enum []string `json:"enum"`
		} `json:"$defs"`
	}
	if err := json.Unmarshal(schema.Bytes, &doc); err != nil {
		t.Fatal(err)
	}
	got := doc.Defs["adminScope"].Enum
	want := []string{ShorthandAdmin, ShorthandRead}
	for _, s := range AllScopes {
		want = append(want, string(s))
	}
	sort.Strings(got)
	sort.Strings(want)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("schema %v, code %v", got, want)
	}
}

// Every audited route writes its action, target, outcome and request
// identity, and the outcome follows the status.
func TestAuditedRoutes(t *testing.T) {
	f := newFixture(t, 0)
	f.b.errFor["gone"] = NotFound("client gone is not held here")
	f.b.errFor["far"] = HeldElsewhere("far", "n3")
	cases := []struct {
		method, path, body string
		status             int
		action, target     string
		outcome            string
	}{
		{"POST", "/api/v1/clients/c1/disconnect", "", 200, "client.disconnect", "c1", "ok"},
		{"POST", "/api/v1/clients/gone/disconnect", "", 404, "client.disconnect", "gone", "not-found"},
		{"DELETE", "/api/v1/clients/far/session", "", 409, "session.invalidate", "far", "conflict"},
		{"DELETE", "/api/v1/clients/a%2Fb/session", "", 200, "session.invalidate", "a/b", "ok"},
		{"DELETE", "/api/v1/retained?topic=r/1", "", 200, "retained.delete", "r/1", "ok"},
		{"DELETE", "/api/v1/retained?topic=r/%23", "", 400, "retained.delete", "r/#", "bad-request"},
		{"POST", "/api/v1/cluster/drain", "", 202, "cluster.drain", "n1", "ok"},
		{"POST", "/api/v1/config/validate", `{"version":1}`, 200, "config.validate", "n1", "ok"},
		{"POST", "/api/v1/config/reload", "", 200, "config.reload", "n1", "ok"},
		{"POST", "/api/v1/traces/c1", `{"limit":20}`, 200, "trace.start", "c1", "ok"},
		{"POST", "/api/v1/traces/c1", `{"limt":20}`, 400, "trace.start", "c1", "bad-request"},
		{"DELETE", "/api/v1/traces/c1", "", 200, "trace.stop", "c1", "ok"},
		{"POST", "/api/v1/modules/metrics.internal/enable", "", 200, "module.enable", "metrics.internal", "ok"},
		{"POST", "/api/v1/modules/metrics.internal/disable", "", 200, "module.disable", "metrics.internal", "ok"},
	}
	for _, c := range cases {
		rec := f.do(c.method, c.path, opsToken, c.body)
		if rec.Code != c.status {
			t.Errorf("%s %s: %d %s", c.method, c.path, rec.Code, rec.Body)
			continue
		}
		e, lossy := f.b.lastAudit(t)
		if e.Actor != "token:ops" || e.Action != c.action || e.Target != c.target || e.Outcome != c.outcome || lossy ||
			e.Detail["request"] != rec.Header().Get("X-Request-Id") || e.Detail["method"] != c.method {
			t.Errorf("%s %s: audit %+v", c.method, c.path, e)
		}
	}
	// A reload's record says whether it was applied, and what changed.
	for _, c := range []struct {
		res    ConfigReload
		result string
	}{
		{ConfigReload{Applied: true, Changed: []string{"/log/level"}}, "applied"},
		{ConfigReload{RestartRequired: true, Problems: []ConfigProblem{{Pointer: "/node/dataDir"}}}, "refused"},
		{ConfigReload{Problems: []ConfigProblem{{Pointer: "/log/level"}}}, "invalid"},
	} {
		f.b.mu.Lock()
		f.b.reload = c.res
		f.b.mu.Unlock()
		if rec := f.do("POST", "/api/v1/config/reload", opsToken, ""); rec.Code != 200 {
			t.Fatalf("reload: %d %s", rec.Code, rec.Body)
		}
		if e, _ := f.b.lastAudit(t); e.Detail["result"] != c.result {
			t.Errorf("a %s reload was audited with result %q", c.result, e.Detail["result"])
		}
	}
	rec := f.do("DELETE", "/api/v1/clients/far/session", opsToken, "")
	var p Problem
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil || p.Owner != "n3" || p.Status != 409 || rec.Header().Get("Content-Type") != "application/problem+json" {
		t.Fatalf("409 body %q: %+v %v", rec.Body, p, err)
	}
	// Reads are not audited.
	before := f.b.auditCount()
	for _, path := range []string{"/api/v1/clients", "/api/v1/cluster", "/api/v1/modules", "/api/v1/config", "/api/v1/audit"} {
		if rec := f.do("GET", path, opsToken, ""); rec.Code != 200 {
			t.Errorf("GET %s: %d", path, rec.Code)
		}
	}
	if f.b.auditCount() != before {
		t.Errorf("reads wrote %d audit records", f.b.auditCount()-before)
	}
}

func TestBodyCapAndMalformedInput(t *testing.T) {
	f := newFixture(t, 1024)
	big := `{"version":1,"pad":"` + strings.Repeat("x", 2048) + `"}`
	rec := f.do("POST", "/api/v1/config/validate", opsToken, big)
	if rec.Code != 413 {
		t.Fatalf("2 KiB against a 1 KiB cap: %d %s", rec.Code, rec.Body)
	}
	if e, _ := f.b.lastAudit(t); e.Outcome != "too-large" {
		t.Fatalf("audit %+v", e)
	}
	small := `{"version":1,"pad":"` + strings.Repeat("x", 870) + `"}`
	if len(small) > 1024 || len(small) < 880 {
		t.Fatalf("fixture size %d", len(small))
	}
	if rec := f.do("POST", "/api/v1/config/validate", opsToken, small); rec.Code != 200 {
		t.Fatalf("%d bytes against a 1 KiB cap: %d", len(small), rec.Code)
	}
	if rec := f.do("POST", "/api/v1/config/validate", opsToken, `{"version":`); rec.Code != 400 {
		t.Fatalf("malformed JSON: %d", rec.Code)
	}
	for _, q := range []string{"limit=0", "limit=1001", "limit=x"} {
		if rec := f.do("GET", "/api/v1/clients?"+q, opsToken, ""); rec.Code != 400 {
			t.Errorf("%s: %d", q, rec.Code)
		}
	}
	for _, q := range []string{"", "?topic=", "?topic=a/%2B"} {
		if rec := f.do("GET", "/api/v1/subscriptions"+q, opsToken, ""); rec.Code != 400 {
			t.Errorf("subscriptions%s: %d", q, rec.Code)
		}
	}
}

func TestClientPaging(t *testing.T) {
	f := newFixture(t, 0)
	var pg ClientPage
	rec := f.do("GET", "/api/v1/clients?search=a&limit=1", opsToken, "")
	if err := json.Unmarshal(rec.Body.Bytes(), &pg); err != nil || len(pg.Items) != 1 || pg.Items[0].ClientID != "a1" || pg.Next != "a1" {
		t.Fatalf("first page %s", rec.Body)
	}
	pg = ClientPage{}
	rec = f.do("GET", "/api/v1/clients?search=a&limit=1&after=a1", opsToken, "")
	if err := json.Unmarshal(rec.Body.Bytes(), &pg); err != nil || len(pg.Items) != 1 || pg.Items[0].ClientID != "a2" || pg.Next != "" {
		t.Fatalf("last page %s", rec.Body)
	}
}

func TestRedact(t *testing.T) {
	f := newFixture(t, 0)
	rec := f.do("GET", "/api/v1/config", opsToken, "")
	body := rec.Body.String()
	for _, present := range []string{`"secretFile"`, `"bindPassword"`, `"url"`, `ldap://x`, `"version"`} {
		if !strings.Contains(body, present) {
			t.Fatalf("%s is missing from the view: %s", present, body)
		}
	}
	for _, secret := range []string{"hunter2", `"/s"`} {
		if strings.Contains(body, secret) {
			t.Errorf("the view holds %s", secret)
		}
	}
	if strings.Count(body, Redacted) != 2 {
		t.Errorf("redacted %d members: %s", strings.Count(body, Redacted), body)
	}
	out, err := Redact([]byte(`{"admin":{"tokens":[{"name":"ops","tokenFile":"/t","scopes":["admin"]}],"tls":{"certFile":"c","keyFile":"k"}},"limits":{"maxKeepAlive":60}}`))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"name":"ops"`, `"scopes":["admin"]`, `"certFile":"c"`, `"maxKeepAlive":60`, `"tokenFile":"[redacted]"`, `"keyFile":"[redacted]"`} {
		if !strings.Contains(string(out), want) {
			t.Errorf("%s missing from %s", want, out)
		}
	}
}
