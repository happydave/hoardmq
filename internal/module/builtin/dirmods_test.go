package builtin

import (
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

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/cluster/records"
	"github.com/happydave/hoardmq/internal/ldap/ldaptest"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/module"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/session"
)

// Each auth.ldap rule refuses a value, and a value one step inside it passes.
func TestLDAPModuleConfiguration(t *testing.T) {
	dir := t.TempDir()
	pw := filepath.Join(dir, "pw")
	empty := filepath.Join(dir, "empty")
	_ = os.WriteFile(pw, []byte("svc\n"), 0o600)
	_ = os.WriteFile(empty, []byte(" \n"), 0o600)
	direct := `"url":"ldap://127.0.0.1:1","bindDn":"uid=${username},dc=x"`
	search := func(extra string) string {
		return `"url":"ldaps://dir.example","search":{"baseDn":"dc=x","filter":"(uid=${username})"` + extra + `}`
	}
	for _, c := range []struct {
		name, doc, want string
	}{
		{"direct on loopback", `{` + direct + `}`, ""},
		{"search over ldaps", `{` + search("") + `}`, ""},
		{"search with a service account", `{` + search(`,"bindDn":"cn=svc,dc=x","bindPasswordFile":"`+pw+`"`) + `}`, ""},
		{"no url", `{"bindDn":"uid=${username},dc=x"}`, "url is required"},
		{"an http url", `{"url":"http://dir.example","bindDn":"uid=${username},dc=x"}`, "scheme must be ldap or ldaps"},
		{"ldap off loopback", `{"url":"ldap://dir.example","bindDn":"uid=${username},dc=x"}`, "in clear"},
		{"ldap off loopback with startTLS", `{"url":"ldap://dir.example","startTLS":true,"bindDn":"uid=${username},dc=x"}`, ""},
		{"startTLS on ldaps", `{"url":"ldaps://dir.example","startTLS":true,"bindDn":"uid=${username},dc=x"}`, "startTLS is for ldap://"},
		{"both modes", `{` + direct + `,"search":{"baseDn":"dc=x","filter":"(uid=${username})"}}`, "not both"},
		{"neither mode", `{"url":"ldap://127.0.0.1"}`, "one of bindDn and search is required"},
		{"a bindDn without the placeholder", `{"url":"ldap://127.0.0.1","bindDn":"uid=x,dc=x"}`, "exactly once"},
		{"no baseDn", `{"url":"ldaps://d","search":{"filter":"(uid=${username})"}}`, "baseDn is required"},
		{"a substring filter", `{"url":"ldaps://d","search":{"baseDn":"dc=x","filter":"(&(uid=${username})(cn=a*))"}}`, "equality and presence"},
		{"a filter without the placeholder", `{"url":"ldaps://d","search":{"baseDn":"dc=x","filter":"(uid=x)"}}`, "exactly once"},
		{"scope base", `{` + search(`,"scope":"base"`) + `}`, "scope must be"},
		{"scope one", `{` + search(`,"scope":"one"`) + `}`, ""},
		{"a bindDn without its password", `{` + search(`,"bindDn":"cn=svc,dc=x"`) + `}`, "go together"},
		{"a password file without its bindDn", `{` + search(`,"bindPasswordFile":"`+pw+`"`) + `}`, "go together"},
		{"an empty password file", `{` + search(`,"bindDn":"cn=svc,dc=x","bindPasswordFile":"`+empty+`"`) + `}`, "bindPasswordFile is empty"},
		{"an absent password file", `{` + search(`,"bindDn":"cn=svc,dc=x","bindPasswordFile":"`+filepath.Join(dir, "absent")+`"`) + `}`, "no such file"},
		{"onUnavailable allow", `{` + direct + `,"onUnavailable":"allow"}`, `onUnavailable must be`},
		{"onUnavailable pass", `{` + direct + `,"onUnavailable":"pass"}`, ""},
		{"maxConnections 257", `{` + direct + `,"maxConnections":257}`, "maxConnections must be 1 to 256"},
		{"maxConnections 256", `{` + direct + `,"maxConnections":256}`, ""},
		{"maxConnections 0", `{` + direct + `,"maxConnections":0}`, "maxConnections must be 1 to 256"},
		{"timeoutMs 99", `{` + direct + `,"timeoutMs":99}`, "timeoutMs must be 100 to 30000"},
		{"timeoutMs 100", `{` + direct + `,"timeoutMs":100}`, ""},
		{"timeoutMs 30001", `{` + direct + `,"timeoutMs":30001}`, "timeoutMs must be 100 to 30000"},
		{"a CA file with no certificate", `{` + direct + `,"caFile":"` + pw + `"}`, "no certificate"},
		{"a CA file with no certificate beside a bad timeout", `{` + direct + `,"timeoutMs":1,"caFile":"` + pw + `"}`, "caFile: "},
		{"an unknown member", `{` + direct + `,"filter":"x"}`, "unknown field"},
	} {
		err := provision(t, &ldapAuth{}, c.doc)
		switch {
		case c.want == "" && err != nil:
			t.Errorf("auth.ldap %s: %v", c.name, err)
		case c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)):
			t.Errorf("auth.ldap %s: %v, want %q", c.name, err, c.want)
		}
	}
	if err := provision(t, &dbAuth{}, `{"collection":"x"}`); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Errorf("auth.db with a member: %v", err)
	}
	if err := provision(t, &dbAuth{}, `{}`); err != nil {
		t.Errorf("auth.db: %v", err)
	}
}

type countingMetrics struct {
	mu sync.Mutex
	m  map[string]float64
}

func (c *countingMetrics) Add(name string, labels map[string]string, d float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m == nil {
		c.m = map[string]float64{}
	}
	c.m[name+labels["outcome"]+labels["policy"]] += d
}
func (c *countingMetrics) Set(string, map[string]string, float64) {}
func (c *countingMetrics) Snapshot() []module.Metric              { return nil }
func (c *countingMetrics) get(k string) float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.m[k]
}

// The module's configuration reaches the authenticator, and it contacts the
// directory only when a client authenticates.
func TestLDAPModuleWiring(t *testing.T) {
	dir := t.TempDir()
	pw := filepath.Join(dir, "pw")
	_ = os.WriteFile(pw, []byte("svc-secret\n"), 0o600)
	s := ldaptest.New(t, "ou=people,dc=x",
		ldaptest.Entry{DN: "uid=alice,ou=people,dc=x", Password: "wonderland", Attrs: map[string][]string{"objectClass": {"person"}, "uid": {"alice"}}},
		ldaptest.Entry{DN: "cn=svc,dc=x", Password: "svc-secret"})
	m := &ldapAuth{}
	doc := `{"url":"` + s.URL + `","search":{"baseDn":"ou=people,dc=x","filter":"(&(objectClass=person)(uid=${username}))","bindDn":"cn=svc,dc=x","bindPasswordFile":"` + pw + `"},"onUnavailable":"pass","timeoutMs":300,"maxConnections":1}`
	if err := provision(t, m, doc); err != nil {
		t.Fatal(err)
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := s.Connections(); n != 0 {
		t.Fatalf("%d connections after provision and start", n)
	}
	metrics := &countingMetrics{}
	m.UseMetrics(metrics)
	req := &auth.Request{Username: "alice", HasUsername: true, Password: []byte("wonderland"), HasPassword: true}
	if r := m.Authenticate(context.Background(), req); r.Decision != auth.Allow {
		t.Fatalf("alice: %+v", r)
	}
	if log := s.BindLog(); len(log) != 2 || log[0] != "cn=svc,dc=x" {
		t.Fatalf("binds %q: the service account from its file first", log)
	}
	if m.inner.Pool.Max != 1 || m.inner.Timeout != 300*time.Millisecond || m.inner.Search.Scope != 2 {
		t.Fatalf("pool %d, timeout %v, scope %d", m.inner.Pool.Max, m.inner.Timeout, m.inner.Search.Scope)
	}
	s.SetBindDelay(2 * time.Second)
	if r := m.Authenticate(context.Background(), req); r.Decision != auth.Pass {
		t.Fatalf("a slow directory under pass: %+v", r)
	}
	if metrics.get("hoardmq_auth_ldap_decisions_totalallowed") != 1 || metrics.get("hoardmq_auth_ldap_unavailable_totalpass") != 1 ||
		metrics.get("hoardmq_auth_ldap_connections_totalopened") < 1 {
		t.Fatalf("metrics %v", metrics.m)
	}
	s.SetBindDelay(0)
	if r := m.Authenticate(context.Background(), req); r.Decision != auth.Allow || s.OpenConnections() == 0 {
		t.Fatalf("alice again: %+v, %d open", r, s.OpenConnections())
	}
	_ = m.Stop(context.Background())
	deadline := time.Now().Add(2 * time.Second)
	for s.OpenConnections() != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := s.OpenConnections(); n != 0 {
		t.Fatalf("%d connections open after Stop", n)
	}
}

// fakeServices hands auth.db a records store.
type fakeServices struct{ store module.RecordStore }

func (fakeServices) Publish(*message.Message)                          {}
func (fakeServices) Disconnect(string, string, packet.ReasonCode) bool { return false }
func (fakeServices) Sessions() []session.Record                        { return nil }
func (fakeServices) Retained(string, string) []*message.Message        { return nil }
func (fakeServices) Ready() bool                                       { return true }
func (fakeServices) Drain()                                            {}
func (fakeServices) DrainStatus() module.DrainStatus                   { return module.DrainStatus{} }
func (fakeServices) Serving() (bool, string)                           { return true, "" }
func (f fakeServices) Records() module.RecordStore                     { return f.store }

func TestDBModuleReadsTheRecords(t *testing.T) {
	comp := records.New(0)
	phc, err := auth.HashSCRAM([]byte("s3cret"), auth.MinSCRAMIterations)
	if err != nil {
		t.Fatal(err)
	}
	v, _ := json.Marshal(auth.StoredCredential{Password: phc})
	op, _ := json.Marshal(records.Op{Collection: auth.CredentialsCollection, Key: "alice", Value: v})
	if _, err := comp.Apply(1, op); err != nil {
		t.Fatal(err)
	}
	m := &dbAuth{}
	ctx := &module.Context{Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Services: fakeServices{store: &records.Store{Component: comp}}}
	if err := m.Provision(ctx, json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	req := func(pw string) *auth.Request {
		return &auth.Request{Username: "alice", HasUsername: true, Password: []byte(pw), HasPassword: true}
	}
	if r := m.Authenticate(context.Background(), req("s3cret")); r.Decision != auth.Allow {
		t.Fatalf("alice: %+v", r)
	}
	if r := m.Authenticate(context.Background(), req("nope")); r.Decision != auth.Deny {
		t.Fatalf("alice, wrong: %+v", r)
	}
	closed := &dbAuth{}
	_ = closed.Provision(&module.Context{Logger: ctx.Logger, Services: fakeServices{}}, json.RawMessage(`{}`))
	if r := closed.Authenticate(context.Background(), req("s3cret")); r.Decision != auth.Pass {
		t.Fatalf("no store yet: %+v", r)
	}
	check := &dbAuth{}
	_ = check.Provision(&module.Context{Logger: ctx.Logger}, json.RawMessage(`{}`))
	if r := check.Authenticate(context.Background(), req("s3cret")); r.Decision != auth.Pass {
		t.Fatalf("no services (check): %+v", r)
	}
}

// auth.file and auth.db verify through the node's one cache: a password
// verified by one is answered from the cache by the other when both hold the
// same stored credential.
func TestPasswordModulesShareTheNodeCache(t *testing.T) {
	phc, err := auth.HashSCRAM([]byte("s3cret"), auth.MinSCRAMIterations)
	if err != nil {
		t.Fatal(err)
	}
	comp := records.New(0)
	v, _ := json.Marshal(auth.StoredCredential{Password: phc})
	op, _ := json.Marshal(records.Op{Collection: auth.CredentialsCollection, Key: "alice", Value: v})
	if _, err := comp.Apply(1, op); err != nil {
		t.Fatal(err)
	}
	users := filepath.Join(t.TempDir(), "users.json")
	if err := os.WriteFile(users, []byte(`{"users":[{"username":"alice","password":"`+phc+`"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cache := auth.NewVerifyCache(time.Minute, 10)
	ctx := &module.Context{Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Services: fakeServices{store: &records.Store{Component: comp}}, Passwords: cache}
	file, db := &fileAuth{}, &dbAuth{}
	if err := file.Provision(ctx, json.RawMessage(`{"path":"`+users+`"}`)); err != nil {
		t.Fatal(err)
	}
	if err := db.Provision(ctx, json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	req := &auth.Request{Username: "alice", HasUsername: true, Password: []byte("s3cret"), HasPassword: true}
	if r := file.Authenticate(context.Background(), req); r.Decision != auth.Allow {
		t.Fatalf("auth.file: %+v", r)
	}
	if r := db.Authenticate(context.Background(), req); r.Decision != auth.Allow {
		t.Fatalf("auth.db: %+v", r)
	}
	if s := cache.Stats(); s.Derivations != 1 || s.Hits != 1 {
		t.Fatalf("cache %+v: want the second module answered from the first's verification", s)
	}
}
