package authsql

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/ext"
	"github.com/happydave/hoardmq/ext/auth"
)

// isolate keeps pgx from reading the environment's or the user's
// connection defaults.
func isolate(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	for _, k := range []string{"PGHOST", "PGPORT", "PGUSER", "PGPASSWORD", "PGDATABASE", "PGSSLMODE", "PGSSLROOTCERT", "PGSERVICE"} {
		t.Setenv(k, "")
	}
	t.Setenv("PGPASSFILE", filepath.Join(dir, "none"))
	t.Setenv("PGSERVICEFILE", filepath.Join(dir, "none"))
}

// writeCA writes a self-signed CA certificate, for sslrootcert.
func writeCA(t *testing.T, dir string) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(p, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func validate(t *testing.T, doc string) error {
	t.Helper()
	m := &module{readFile: os.ReadFile, open: openPool}
	if err := m.Provision(&ext.Context{Logger: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))}, json.RawMessage(doc)); err != nil {
		return err
	}
	return m.Validate()
}

// TestConfiguration: good documents validate and connect to nothing; each
// bad one is refused naming its key.
func TestConfiguration(t *testing.T) {
	isolate(t)
	dir := t.TempDir()
	ca := writeCA(t, dir)
	pw := filepath.Join(dir, "db.pw")
	if err := os.WriteFile(pw, []byte("db-secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	q := `"query":"SELECT hash FROM users WHERE name = ${username}"`
	for _, good := range []string{
		// Nothing listens on these; Validate must not try.
		`{"connString":"postgres://hoardmq@127.0.0.1:1/mqtt?sslmode=disable",` + q + `}`,
		`{"connString":"postgres://hoardmq@localhost:1/mqtt?sslmode=prefer",` + q + `}`,
		`{"connString":"host=/var/run/postgresql user=hoardmq dbname=mqtt sslmode=disable",` + q + `}`,
		`{"connString":"postgres://hoardmq@db.example.com/mqtt?sslmode=verify-full&sslrootcert=` + ca + `","passwordFile":"` + pw + `",` + q + `}`,
		`{"connString":"postgres://hoardmq@db.example.com/mqtt?sslmode=verify-ca&sslrootcert=` + ca + `",` + q + `}`,
		`{"connString":"postgres://hoardmq@db.example.com/mqtt?sslmode=require&sslrootcert=` + ca + `",` + q + `}`,
		`{"connString":"postgres://hoardmq@127.0.0.1/mqtt",` + q + `,"onUnavailable":"pass","timeoutMs":100,"maxConnections":64}`,
	} {
		if err := validate(t, good); err != nil {
			t.Errorf("%s: %v", good, err)
		}
	}
	for _, c := range []struct{ doc, want string }{
		{`{` + q + `}`, "connString is required"},
		{`{"connString":"postgres://hoardmq:hunter2@127.0.0.1/mqtt",` + q + `}`, "connString holds a password"},
		{`{"connString":"postgres://hoardmq@127.0.0.1/mqtt?password=hunter2",` + q + `}`, "connString holds a password"},
		{`{"connString":"host=127.0.0.1 user=hoardmq password=hunter2",` + q + `}`, "connString holds a password"},
		{`{"connString":"postgres://hoardmq@db.example.com/mqtt?sslmode=disable",` + q + `}`, "verify-full"},
		{`{"connString":"postgres://hoardmq@db.example.com/mqtt?sslmode=allow",` + q + `}`, "verify-full"},
		{`{"connString":"postgres://hoardmq@db.example.com/mqtt?sslmode=prefer",` + q + `}`, "verify-full"},
		{`{"connString":"postgres://hoardmq@db.example.com/mqtt?sslmode=require",` + q + `}`, "verify-full"},
		{`{"connString":"postgres://hoardmq@db.example.com/mqtt",` + q + `}`, "verify-full"},
		{`{"connString":"postgres://hoardmq@127.0.0.1,db.example.com/mqtt?sslmode=prefer",` + q + `}`, "verify-full"},
		{`{"connString":"postgres://hoardmq@127.0.0.1/mqtt","passwordFile":"` + filepath.Join(dir, "absent") + `",` + q + `}`, "passwordFile"},
		{`{"connString":"postgres://hoardmq@127.0.0.1/mqtt"}`, "query is required"},
		{`{"connString":"postgres://hoardmq@127.0.0.1/mqtt","query":"SELECT hash FROM users"}`, "no placeholder"},
		{`{"connString":"postgres://hoardmq@127.0.0.1/mqtt","query":"SELECT hash FROM users WHERE p = ${password}"}`, "${password}"},
		{`{"connString":"postgres://hoardmq@127.0.0.1/mqtt","query":"SELECT hash FROM users WHERE name = ${username} OR id = $1"}`, "not $1"},
		{`{"connString":"postgres://hoardmq@127.0.0.1/mqtt",` + q + `,"timeoutMs":99}`, "timeoutMs"},
		{`{"connString":"postgres://hoardmq@127.0.0.1/mqtt",` + q + `,"timeoutMs":30001}`, "timeoutMs"},
		{`{"connString":"postgres://hoardmq@127.0.0.1/mqtt",` + q + `,"maxConnections":0}`, "maxConnections"},
		{`{"connString":"postgres://hoardmq@127.0.0.1/mqtt",` + q + `,"maxConnections":65}`, "maxConnections"},
		{`{"connString":"postgres://hoardmq@127.0.0.1/mqtt",` + q + `,"onUnavailable":"allow"}`, "onUnavailable"},
		{`{"connString":"postgres://hoardmq@127.0.0.1/mqtt",` + q + `,"retries":2}`, "retries"},
	} {
		err := validate(t, c.doc)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want %q", c.doc, err, c.want)
		}
		if err != nil && strings.Contains(err.Error(), "hunter2") {
			t.Errorf("%s: the refusal quotes the password: %v", c.doc, err)
		}
	}
}

// TestPasswordFile: the file's password, one trailing newline dropped, is
// the connection's.
func TestPasswordFile(t *testing.T) {
	isolate(t)
	pw := filepath.Join(t.TempDir(), "db.pw")
	if err := os.WriteFile(pw, []byte("db-secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	set, problems := config{ConnString: "postgres://hoardmq@127.0.0.1/mqtt", PasswordFile: pw, Query: "SELECT h FROM u WHERE n = ${username}"}.validate(os.ReadFile)
	if len(problems) > 0 || set.pool.ConnConfig.Password != "db-secret" || set.pool.MaxConns != 8 || set.timeout != 2*time.Second {
		t.Fatalf("%v %+v", problems, set)
	}
}

// TestRewrite: placeholders become positional parameters in order, and the
// arguments are the client's values, never its password.
func TestRewrite(t *testing.T) {
	q, params, err := rewrite("SELECT h FROM u WHERE n = ${username} OR cn = ${cn} AND c = ${clientid} OR n = ${username}")
	if err != nil || q != "SELECT h FROM u WHERE n = $1 OR cn = $2 AND c = $3 OR n = $4" {
		t.Fatalf("%q %v", q, err)
	}
	s := &settings{query: q, params: params}
	req := &auth.Request{Username: "alice", ClientID: "dev-1", CertCN: "gw", Password: []byte("pa55word"), HasPassword: true}
	if got := fmt.Sprint(s.args(req)); got != "[alice gw dev-1 alice]" {
		t.Fatalf("args %s", got)
	}
	for _, a := range s.args(req) {
		if a == "pa55word" {
			t.Fatal("the password is an argument")
		}
	}
}

// fakeLookup answers with fixed rows, or an error, or blocks until its
// context ends or five seconds pass; it records the arguments it was given.
type fakeLookup struct {
	mu    sync.Mutex
	cols  []string
	rows  [][]*string
	err   error
	block bool
	args  [][]any
}

func (f *fakeLookup) Query(ctx context.Context, _ string, args []any) ([]string, [][]*string, error) {
	f.mu.Lock()
	f.args = append(f.args, args)
	f.mu.Unlock()
	if f.block {
		// Bounded, so a lookup the module never cuts fails the test's
		// timing assertion rather than hanging the package.
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-time.After(5 * time.Second):
			return nil, nil, errors.New("the fake's bound passed")
		}
	}
	return f.cols, f.rows, f.err
}

func (f *fakeLookup) Close() {}

// fakeVerifier checks a password against "hash:<password>", errs on
// "bad", and records what it was given.
type fakeVerifier struct {
	mu   sync.Mutex
	seen []string
}

func (v *fakeVerifier) Verify(stored string, password []byte) (bool, error) {
	v.mu.Lock()
	v.seen = append(v.seen, stored+"|"+string(password))
	v.mu.Unlock()
	if stored == "bad" {
		return false, errors.New("not a PHC string")
	}
	return stored == "hash:"+string(password), nil
}

type fakeMetrics struct {
	mu sync.Mutex
	v  map[string]float64
}

func (m *fakeMetrics) Add(name string, labels map[string]string, d float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.v == nil {
		m.v = map[string]float64{}
	}
	parts := []string{}
	for k, v := range labels {
		parts = append(parts, k+"="+v)
	}
	sort.Strings(parts)
	m.v[name+"{"+strings.Join(parts, ",")+"}"] += d
}

func (m *fakeMetrics) Set(string, map[string]string, float64) {}
func (m *fakeMetrics) Snapshot() []ext.Metric                 { return nil }

func (m *fakeMetrics) keys() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for k := range m.v {
		out = append(out, k)
	}
	sort.Strings(out)
	return strings.Join(out, "\n")
}

type logBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *logBuf) Write(p []byte) (int, error) { l.mu.Lock(); defer l.mu.Unlock(); return l.b.Write(p) }
func (l *logBuf) String() string              { l.mu.Lock(); defer l.mu.Unlock(); return l.b.String() }

// started returns auth.sql provisioned, validated and started over f.
func started(t *testing.T, f *fakeLookup, extra string) (*module, *fakeVerifier, *fakeMetrics, *logBuf) {
	t.Helper()
	isolate(t)
	v, log, mx := &fakeVerifier{}, &logBuf{}, &fakeMetrics{}
	m := &module{readFile: os.ReadFile, open: func(*settings) (lookup, error) { return f, nil }}
	doc := `{"connString":"postgres://hoardmq@127.0.0.1/mqtt","query":"SELECT h, tenant FROM u WHERE n = ${username}","timeoutMs":100` + extra + `}`
	if err := m.Provision(&ext.Context{Logger: slog.New(slog.NewTextHandler(log, nil)), Passwords: v}, json.RawMessage(doc)); err != nil {
		t.Fatal(err)
	}
	if err := m.Validate(); err != nil {
		t.Fatal(err)
	}
	m.UseMetrics(mx)
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return m, v, mx, log
}

func str(s string) *string { return &s }

func request(user, password string) *auth.Request {
	return &auth.Request{Namespace: "default", ClientID: "c1", Username: user, HasUsername: true, Password: []byte(password), HasPassword: true}
}

// TestDecisions: each answer decides as tabled, counted by outcome.
func TestDecisions(t *testing.T) {
	cols := []string{"h", "tenant"}
	for _, c := range []struct {
		name    string
		rows    [][]*string
		err     error
		pass    bool
		want    auth.Decision
		reason  string
		attrs   string
		outcome string
	}{
		{"match", [][]*string{{str("hash:pw"), str("blue")}}, nil, false, auth.Allow, "", "map[tenant:blue]", "allowed"},
		{"match, NULL attribute", [][]*string{{str("hash:pw"), nil}}, nil, false, auth.Allow, "", "map[]", "allowed"},
		{"wrong password", [][]*string{{str("hash:other"), str("blue")}}, nil, false, auth.Deny, "sql: wrong password", "map[]", "refused"},
		{"no row", nil, nil, false, auth.Pass, "", "map[]", "unknown"},
		{"two rows", [][]*string{{str("hash:pw"), nil}, {str("hash:pw"), nil}}, nil, false, auth.Deny, "sql: ambiguous user", "map[]", "refused"},
		{"NULL hash", [][]*string{{nil, str("blue")}}, nil, false, auth.Deny, "sql: no password hash", "map[]", "refused"},
		{"empty hash", [][]*string{{str(""), str("blue")}}, nil, false, auth.Deny, "sql: no password hash", "map[]", "refused"},
		{"unreadable hash", [][]*string{{str("bad"), nil}}, nil, false, auth.Deny, "sql: stored credential unreadable", "map[]", "refused"},
		{"database error", nil, errors.New("connection refused"), false, auth.Deny, "sql: database unavailable", "map[]", "unavailable"},
		{"database error, pass", nil, errors.New("connection refused"), true, auth.Pass, "sql: database unavailable", "map[]", "unavailable"},
	} {
		f := &fakeLookup{cols: cols, rows: c.rows, err: c.err}
		extra := ""
		if c.pass {
			extra = `,"onUnavailable":"pass"`
		}
		m, v, mx, _ := started(t, f, extra)
		r := m.Authenticate(context.Background(), request("alice", "pw"))
		if r.Decision != c.want || r.Reason != c.reason || fmt.Sprint(r.Attributes) != c.attrs || !r.Expires.IsZero() {
			t.Errorf("%s: %+v, want %v %q %s", c.name, r, c.want, c.reason, c.attrs)
		}
		if got := mx.keys(); !strings.Contains(got, "hoardmq_auth_sql_decisions_total{outcome="+c.outcome+"}") {
			t.Errorf("%s: metrics %s, want outcome %s", c.name, got, c.outcome)
		}
		if c.name == "match" && (len(v.seen) != 1 || v.seen[0] != "hash:pw|pw") {
			t.Errorf("the verifier saw %v, want the stored hash and the client's password", v.seen)
		}
		if strings.HasSuffix(c.name, "hash") && c.name != "unreadable hash" && len(v.seen) != 0 {
			t.Errorf("%s: the verifier was called", c.name)
		}
		if c.outcome == "unavailable" {
			policy := "deny"
			if c.pass {
				policy = "pass"
			}
			if !strings.Contains(mx.keys(), "hoardmq_auth_sql_unavailable_total{policy="+policy+"}") {
				t.Errorf("%s: metrics %s", c.name, mx.keys())
			}
		}
	}
}

// TestTooManyAttributes: a row past the bound is refused.
func TestTooManyAttributes(t *testing.T) {
	cols, row := []string{"h"}, []*string{str("hash:pw")}
	for i := range MaxAttributes + 1 {
		cols, row = append(cols, fmt.Sprintf("a%d", i)), append(row, str("x"))
	}
	if r, _ := decide(cols, [][]*string{row}, func(string) (bool, error) { return true, nil }); r.Decision != auth.Deny {
		t.Fatalf("%d attributes: %v", MaxAttributes+1, r.Decision)
	}
	if r, _ := decide(cols[:MaxAttributes+1], [][]*string{row[:MaxAttributes+1]}, func(string) (bool, error) { return true, nil }); r.Decision != auth.Allow {
		t.Fatalf("%d attributes: %v", MaxAttributes, r.Decision)
	}
}

// TestClaims: a CONNECT without a username or password passes with no
// query.
func TestClaims(t *testing.T) {
	f := &fakeLookup{cols: []string{"h"}, rows: [][]*string{{str("hash:pw")}}}
	m, _, _, _ := started(t, f, "")
	for _, req := range []*auth.Request{
		{ClientID: "c"},
		{ClientID: "c", Username: "alice", HasUsername: true},
		{ClientID: "c", Username: "alice", HasUsername: true, HasPassword: true},
		{ClientID: "c", Password: []byte("pw"), HasPassword: true},
	} {
		if r := m.Authenticate(context.Background(), req); r.Decision != auth.Pass {
			t.Errorf("%+v: %v", req, r.Decision)
		}
	}
	if len(f.args) != 0 {
		t.Fatalf("queried %d times", len(f.args))
	}
	m.Authenticate(context.Background(), request("alice", "pw"))
	if len(f.args) != 1 || fmt.Sprint(f.args[0]) != "[alice]" {
		t.Fatalf("args %v", f.args)
	}
}

// TestTimeout: a lookup that does not answer is cut at timeoutMs and is
// unavailable.
func TestTimeout(t *testing.T) {
	m, _, _, _ := started(t, &fakeLookup{block: true}, "")
	start := time.Now()
	r := m.Authenticate(context.Background(), request("alice", "pw"))
	if el := time.Since(start); r.Decision != auth.Deny || r.Reason != "sql: database unavailable" || el < 100*time.Millisecond || el > 2*time.Second {
		t.Fatalf("%+v after %v", r, el)
	}
}

// TestStopped: after Stop a CONNECT is unavailable.
func TestStopped(t *testing.T) {
	m, _, _, _ := started(t, &fakeLookup{cols: []string{"h"}, rows: [][]*string{{str("hash:pw")}}}, "")
	if err := m.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if r := m.Authenticate(context.Background(), request("alice", "pw")); r.Reason != "sql: database unavailable" {
		t.Fatalf("%+v", r)
	}
}

// TestNoCredentialLogged: over every decision, neither the client's
// password nor the database's appears in a log line or a metric label.
func TestNoCredentialLogged(t *testing.T) {
	isolate(t)
	pw := filepath.Join(t.TempDir(), "db.pw")
	if err := os.WriteFile(pw, []byte("db-secret-value\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	f := &fakeLookup{cols: []string{"h"}}
	v, log, mx := &fakeVerifier{}, &logBuf{}, &fakeMetrics{}
	m := &module{readFile: os.ReadFile, open: func(*settings) (lookup, error) { return f, nil }}
	doc := `{"connString":"postgres://hoardmq@127.0.0.1/mqtt","passwordFile":"` + pw + `","query":"SELECT h FROM u WHERE n = ${username}"}`
	if err := m.Provision(&ext.Context{Logger: slog.New(slog.NewTextHandler(log, nil)), Passwords: v}, json.RawMessage(doc)); err != nil {
		t.Fatal(err)
	}
	if err := m.Validate(); err != nil {
		t.Fatal(err)
	}
	m.UseMetrics(mx)
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, step := range []func(){
		func() { f.rows = [][]*string{{str("hash:client-secret-value")}} },
		func() { f.rows = [][]*string{{str("hash:other")}} },
		func() { f.rows = nil },
		func() {
			f.err = errors.New(`failed to connect to host=127.0.0.1 user=hoardmq database=mqtt: server error`)
		},
	} {
		step()
		m.Authenticate(context.Background(), request("alice", "client-secret-value"))
	}
	// Presence first: the secrets reached the module, and the Warn was written
	// with the database's error.
	if m.set.pool.ConnConfig.Password != "db-secret-value" || len(v.seen) == 0 || !strings.Contains(v.seen[0], "client-secret-value") {
		t.Fatalf("the secrets did not reach the module: %q %v", m.set.pool.ConnConfig.Password, v.seen)
	}
	if !strings.Contains(log.String(), "auth.sql database unavailable") || !strings.Contains(log.String(), "server error") {
		t.Fatalf("no Warn with the error:\n%s", log.String())
	}
	out := log.String() + mx.keys()
	for _, secret := range []string{"client-secret-value", "db-secret-value"} {
		if strings.Contains(out, secret) {
			t.Errorf("%q in a log line or a metric label:\n%s", secret, out)
		}
	}
	for _, a := range f.args {
		for _, x := range a {
			if x == "client-secret-value" {
				t.Error("the client's password was a query argument")
			}
		}
	}
}
