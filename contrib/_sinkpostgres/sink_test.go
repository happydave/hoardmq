package sinkpostgres

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
	"github.com/happydave/hoardmq/ext/mqtt"
	"github.com/jackc/pgx/v5/pgconn"
)

func isolate(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	for _, k := range []string{"PGHOST", "PGPORT", "PGUSER", "PGPASSWORD", "PGDATABASE", "PGSSLMODE", "PGSSLROOTCERT", "PGSERVICE"} {
		t.Setenv(k, "")
	}
	t.Setenv("PGPASSFILE", filepath.Join(dir, "none"))
	t.Setenv("PGSERVICEFILE", filepath.Join(dir, "none"))
}

func writeCA(t *testing.T, dir string) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
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

func validateDoc(doc string) error {
	s := &sink{readFile: os.ReadFile, open: openPool}
	if err := s.Provision(&ext.Context{Logger: slog.New(slog.DiscardHandler), Namespace: "default"}, json.RawMessage(doc)); err != nil {
		return err
	}
	return s.Validate()
}

// TestConfiguration: good documents validate and connect to nothing; each
// bad one is refused naming its key.
func TestConfiguration(t *testing.T) {
	isolate(t)
	dir := t.TempDir()
	ca := writeCA(t, dir)
	rt := `"routes":[{"filter":"s/#","table":"readings"}]`
	local := `"connString":"postgres://hoardmq@127.0.0.1:1/mqtt?sslmode=disable"`
	for _, good := range []string{
		`{` + local + `,` + rt + `}`,
		`{"connString":"postgres://hoardmq@db.example.com/mqtt?sslmode=verify-full&sslrootcert=` + ca + `","routes":[{"filter":"a","table":"telemetry.readings","namespace":"t"}]}`,
		`{` + local + `,` + rt + `,"batch":{"maxRows":10000,"lingerMs":0},"buffer":{"maxRecords":1,"maxBytes":1048576,"whenFull":"drop","waitMs":0},"flushTimeoutMs":0,"timeoutMs":100}`,
	} {
		if err := validateDoc(good); err != nil {
			t.Errorf("%s: %v", good, err)
		}
	}
	for _, c := range []struct{ doc, want string }{
		{`{` + rt + `}`, "connString is required"},
		{`{"connString":"postgres://hoardmq:pw@127.0.0.1/mqtt",` + rt + `}`, "connString holds a password"},
		{`{"connString":"postgres://hoardmq@db.example.com/mqtt?sslmode=require",` + rt + `}`, "verify-full"},
		{`{"connString":"postgres://hoardmq@db.example.com/mqtt",` + rt + `}`, "verify-full"},
		{`{` + local + `}`, "routes: at least one route"},
		{`{` + local + `,"routes":[{"filter":"a","table":"a;b"}]}`, "routes/0/table"},
		{`{` + local + `,"routes":[{"filter":"a","table":"a.b.c"}]}`, "routes/0/table"},
		{`{` + local + `,"routes":[{"filter":"a","table":"` + strings.Repeat("t", 64) + `"}]}`, "routes/0/table"},
		{`{` + local + `,"routes":[{"filter":"$share/g/a","table":"t"}]}`, "routes/0/filter: a shared filter"},
		{`{` + local + `,` + rt + `,"batch":{"maxRows":0}}`, "batch.maxRows"},
		{`{` + local + `,` + rt + `,"batch":{"maxRows":10001}}`, "batch.maxRows"},
		{`{` + local + `,` + rt + `,"batch":{"lingerMs":1001}}`, "batch.lingerMs"},
		{`{` + local + `,` + rt + `,"buffer":{"maxRecords":0}}`, "buffer.maxRecords"},
		{`{` + local + `,` + rt + `,"buffer":{"maxBytes":1048575}}`, "buffer.maxBytes"},
		{`{` + local + `,` + rt + `,"buffer":{"whenFull":"block"}}`, "buffer.whenFull"},
		{`{` + local + `,` + rt + `,"buffer":{"waitMs":10001}}`, "buffer.waitMs"},
		{`{` + local + `,` + rt + `,"flushTimeoutMs":60001}`, "flushTimeoutMs"},
		{`{` + local + `,` + rt + `,"timeoutMs":99}`, "timeoutMs"},
		{`{` + local + `,` + rt + `,"retries":3}`, "retries"},
	} {
		err := validateDoc(c.doc)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%.90s: %v, want %q", c.doc, err, c.want)
		}
		if err != nil && strings.Contains(err.Error(), ":pw@") {
			t.Errorf("the refusal quotes the password: %v", err)
		}
	}
}

// fakeWriter records batches; it can hold every write, or fail them.
type fakeWriter struct {
	mu      sync.Mutex
	batches []fakeBatch
	hold    chan struct{} // when set, a write waits for it or its context
	fail    []error       // errors returned by the next writes, in order
	failFor map[string]error
}

type fakeBatch struct {
	table string
	rows  [][]any
}

func (f *fakeWriter) Write(ctx context.Context, r route, rows [][]any) error {
	f.mu.Lock()
	hold := f.hold
	var err error
	if len(f.fail) > 0 {
		err, f.fail = f.fail[0], f.fail[1:]
	} else if e, ok := f.failFor[r.table]; ok {
		err = e
	}
	f.mu.Unlock()
	if hold != nil {
		select {
		case <-hold:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err != nil {
		return err
	}
	f.mu.Lock()
	f.batches = append(f.batches, fakeBatch{table: r.table, rows: rows})
	f.mu.Unlock()
	return nil
}

func (f *fakeWriter) Close() {}

func (f *fakeWriter) written() []fakeBatch {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fakeBatch(nil), f.batches...)
}

type fakeMetrics struct {
	mu         sync.Mutex
	v          map[string]float64
	collectors []func(func(string, map[string]string, float64))
}

func key(name string, labels map[string]string) string {
	parts := []string{}
	for k, v := range labels {
		parts = append(parts, k+"="+v)
	}
	sort.Strings(parts)
	return name + "{" + strings.Join(parts, ",") + "}"
}

func (m *fakeMetrics) Add(name string, labels map[string]string, d float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.v == nil {
		m.v = map[string]float64{}
	}
	m.v[key(name, labels)] += d
}
func (m *fakeMetrics) Set(name string, labels map[string]string, v float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.v == nil {
		m.v = map[string]float64{}
	}
	m.v[key(name, labels)] = v
}
func (m *fakeMetrics) Snapshot() []ext.Metric { return nil }
func (m *fakeMetrics) AddCollector(f func(func(string, map[string]string, float64))) {
	m.mu.Lock()
	m.collectors = append(m.collectors, f)
	m.mu.Unlock()
}
func (m *fakeMetrics) get(k string) float64 {
	m.mu.Lock()
	cs := append([]func(func(string, map[string]string, float64)){}, m.collectors...)
	m.mu.Unlock()
	for _, c := range cs {
		c(m.Set)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.v[k]
}

type logBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *logBuf) Write(p []byte) (int, error) { l.mu.Lock(); defer l.mu.Unlock(); return l.b.Write(p) }
func (l *logBuf) String() string              { l.mu.Lock(); defer l.mu.Unlock(); return l.b.String() }

// started is sink.postgres provisioned, validated and started over w.
func started(t *testing.T, w *fakeWriter, extra string) (*sink, *fakeMetrics, *logBuf) {
	t.Helper()
	isolate(t)
	log, mx := &logBuf{}, &fakeMetrics{}
	s := &sink{readFile: os.ReadFile, open: func(*settings) (writer, error) { return w, nil }}
	doc := `{"connString":"postgres://hoardmq@127.0.0.1/mqtt","routes":[{"filter":"s/#","table":"readings"},{"filter":"s/a/#","table":"alerts"},{"filter":"s/#","table":"readings"}]` + extra + `}`
	if err := s.Provision(&ext.Context{Logger: slog.New(slog.NewTextHandler(log, nil)), Namespace: "default"}, json.RawMessage(doc)); err != nil {
		t.Fatal(err)
	}
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
	s.UseMetrics(mx)
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Stop(context.Background()) })
	return s, mx, log
}

func msg(topic string, qos byte, payload string) *mqtt.Message {
	return &mqtt.Message{Namespace: "default", Topic: topic, QoS: qos, Payload: []byte(payload),
		Publisher: mqtt.Publisher{ClientID: "dev-1", Username: "secret-user"}, Arrived: time.Unix(1_800_000_000, 0)}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("%s: not within 10s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestMapping: each column as tabled, one row per distinct table, a module's
// publish with a NULL client identifier, an empty payload not NULL.
func TestMapping(t *testing.T) {
	w := &fakeWriter{}
	s, mx, _ := started(t, w, `,"batch":{"lingerMs":0}`)
	s.OnPublishAccepted(context.Background(), ext.SourceClient, msg("s/a/1", 1, "21.5"))
	mod := msg("s/x", 0, "")
	mod.Payload, mod.Publisher = nil, mqtt.Publisher{}
	s.OnPublishAccepted(context.Background(), ext.SourceModule, mod)
	will := msg("s/w", 1, "bye")
	will.Retain = true
	s.OnPublishAccepted(context.Background(), ext.SourceWill, will)
	s.OnPublishAccepted(context.Background(), ext.SourceClient, msg("other", 1, "x"))
	elsewhere := msg("s/a/1", 1, "x")
	elsewhere.Namespace = "tenant"
	s.OnPublishAccepted(context.Background(), ext.SourceClient, elsewhere)
	eventually(t, "four rows", func() bool {
		return mx.get("hoardmq_sink_postgres_written_total{table=readings}") == 3 && mx.get("hoardmq_sink_postgres_written_total{table=alerts}") == 1
	})
	var rows []string
	for _, b := range w.written() {
		for _, r := range b.rows {
			rows = append(rows, fmt.Sprintf("%s %q %v", b.table, r[1], r))
		}
	}
	sort.Strings(rows)
	at := time.Unix(1_800_000_000, 0)
	want := []string{
		fmt.Sprintf("alerts %q %v", []byte("21.5"), []any{"s/a/1", []byte("21.5"), int16(1), false, "dev-1", "default", "client", at}),
		fmt.Sprintf("readings %q %v", []byte(""), []any{"s/x", []byte{}, int16(0), false, nil, "default", "module", at}),
		fmt.Sprintf("readings %q %v", []byte("21.5"), []any{"s/a/1", []byte("21.5"), int16(1), false, "dev-1", "default", "client", at}),
		fmt.Sprintf("readings %q %v", []byte("bye"), []any{"s/w", []byte("bye"), int16(1), true, "dev-1", "default", "will", at}),
	}
	sort.Strings(want)
	if strings.Join(rows, "\n") != strings.Join(want, "\n") {
		t.Fatalf("rows\n%s\nwant\n%s", strings.Join(rows, "\n"), strings.Join(want, "\n"))
	}
	for _, b := range w.written() {
		for _, r := range b.rows {
			for _, v := range r {
				if v == "secret-user" {
					t.Fatal("the username was written")
				}
			}
		}
	}
}

// TestPolicy: a full buffer refuses a client's QoS 1 under refuse, drops
// under drop, drops QoS 0 always, and holds an admitted QoS 1 for waitMs
// before overflow.
func TestPolicy(t *testing.T) {
	hold := make(chan struct{})
	defer close(hold)
	w := &fakeWriter{hold: hold}
	s, mx, log := started(t, w, `,"batch":{"lingerMs":0,"maxRows":1},"buffer":{"maxRecords":3,"waitMs":200},"flushTimeoutMs":0`)
	for i := range 3 {
		m := msg(fmt.Sprintf("s/%d", i), 1, "x")
		if out := s.AdmitPublish(context.Background(), ext.ConnectionInfo{}, m); out != ext.OK {
			t.Fatalf("publish %d with room: %v", i, out)
		}
		s.OnPublishAccepted(context.Background(), ext.SourceClient, m)
	}
	if r, _ := s.q.size(); r != 3 {
		t.Fatalf("buffered %d, want 3", r)
	}
	m := msg("s/4", 1, "x")
	if out := s.AdmitPublish(context.Background(), ext.ConnectionInfo{}, m); out.Action != ext.Drop || out.Reason != mqtt.QuotaExceeded {
		t.Fatalf("a full buffer under refuse: %v, want Drop 0x97", out)
	}
	if got := mx.get("hoardmq_sink_postgres_refused_total{table=readings}"); got != 1 {
		t.Fatalf("refused %v", got)
	}
	if out := s.AdmitPublish(context.Background(), ext.ConnectionInfo{}, msg("s/5", 0, "x")); out != ext.OK {
		t.Fatalf("QoS 0 refused: %v", out)
	}
	s.OnPublishAccepted(context.Background(), ext.SourceClient, msg("s/5", 0, "x"))
	if got := mx.get("hoardmq_sink_postgres_dropped_total{reason=buffer_full,table=readings}"); got != 1 {
		t.Fatalf("QoS 0 buffer_full %v", got)
	}
	start := time.Now()
	s.OnPublishAccepted(context.Background(), ext.SourceClient, msg("s/6", 1, "x"))
	if el := time.Since(start); el < 200*time.Millisecond || el > 1200*time.Millisecond {
		t.Fatalf("an admitted publish meeting a full buffer returned after %v, want the 200 ms wait", el)
	}
	if got := mx.get("hoardmq_sink_postgres_dropped_total{reason=overflow,table=readings}"); got != 1 {
		t.Fatalf("overflow %v", got)
	}
	if got := mx.get("hoardmq_sink_postgres_buffered_records{}"); got != 3 {
		t.Fatalf("buffered gauge %v", got)
	}
	if !strings.Contains(log.String(), "reason=buffer_full records=1") {
		t.Fatalf("no Warn for the drop:\n%s", log.String())
	}

	dw := &fakeWriter{hold: hold}
	d, dmx, _ := started(t, dw, `,"batch":{"lingerMs":0},"buffer":{"maxRecords":1,"whenFull":"drop"},"flushTimeoutMs":0`)
	d.OnPublishAccepted(context.Background(), ext.SourceClient, msg("s/1", 1, "x"))
	eventually(t, "the first row taken", func() bool { r, _ := d.q.size(); return r == 1 })
	if out := d.AdmitPublish(context.Background(), ext.ConnectionInfo{}, msg("s/2", 1, "x")); out != ext.OK {
		t.Fatalf("drop policy refused: %v", out)
	}
	start = time.Now()
	d.OnPublishAccepted(context.Background(), ext.SourceClient, msg("s/2", 1, "x"))
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("the drop policy waited")
	}
	if got := dmx.get("hoardmq_sink_postgres_dropped_total{reason=buffer_full,table=readings}"); got != 1 {
		t.Fatalf("drop buffer_full %v", got)
	}

	bw := &fakeWriter{hold: hold}
	b, _, _ := started(t, bw, `,"buffer":{"maxBytes":1048576},"flushTimeoutMs":0`)
	big := msg("s/big", 1, strings.Repeat("x", 1<<20))
	b.OnPublishAccepted(context.Background(), ext.SourceClient, big)
	if out := b.AdmitPublish(context.Background(), ext.ConnectionInfo{}, msg("s/2", 1, "x")); out.Action != ext.Drop {
		t.Fatalf("maxBytes reached: %v, want a refusal", out)
	}
}

// TestWriter: batches of at most maxRows in order per table; a transient
// failure is retried and nothing dropped; a permanent one is counted
// rejected and the next batch written.
func TestWriter(t *testing.T) {
	w := &fakeWriter{fail: []error{errors.New("dial tcp: connection refused"), &pgconn.PgError{Code: "57P01"}}}
	s, mx, _ := started(t, w, `,"batch":{"maxRows":2,"lingerMs":50}`)
	for i := range 5 {
		s.OnPublishAccepted(context.Background(), ext.SourceClient, msg("s/x", 1, strconv(i)))
	}
	eventually(t, "five written", func() bool { return mx.get("hoardmq_sink_postgres_written_total{table=readings}") == 5 })
	// Written rows give their room back.
	eventually(t, "the buffer empty", func() bool { return mx.get("hoardmq_sink_postgres_buffered_records{}") == 0 })
	var got []string
	for _, b := range w.written() {
		if len(b.rows) > 2 {
			t.Fatalf("a batch of %d rows, want at most 2", len(b.rows))
		}
		for _, r := range b.rows {
			got = append(got, string(r[1].([]byte)))
		}
	}
	if strings.Join(got, ",") != "0,1,2,3,4" {
		t.Fatalf("order %v", got)
	}
	if mx.get("hoardmq_sink_postgres_retries_total{}") != 2 {
		t.Fatalf("retries %v", mx.get("hoardmq_sink_postgres_retries_total{}"))
	}

	pw := &fakeWriter{failFor: map[string]error{"alerts": &pgconn.PgError{Code: "42P01", Message: "relation does not exist"}}}
	p, pmx, plog := started(t, pw, `,"batch":{"lingerMs":0}`)
	p.OnPublishAccepted(context.Background(), ext.SourceClient, msg("s/a/1", 1, "x"))
	eventually(t, "rejected and written", func() bool {
		return pmx.get("hoardmq_sink_postgres_dropped_total{reason=rejected,table=alerts}") == 1 && pmx.get("hoardmq_sink_postgres_written_total{table=readings}") == 1
	})
	if !strings.Contains(plog.String(), "reason=rejected") {
		t.Fatalf("no Warn for the rejection:\n%s", plog.String())
	}
}

func strconv(i int) string { return fmt.Sprint(i) }

// TestStop: Stop writes what the buffer holds; with the writer held it
// returns after flushTimeoutMs and counts every row left shutdown, the batch
// in flight included.
func TestStop(t *testing.T) {
	w := &fakeWriter{}
	s, mx, _ := started(t, w, `,"batch":{"lingerMs":1000}`)
	for i := range 4 {
		s.OnPublishAccepted(context.Background(), ext.SourceClient, msg("s/x", 1, strconv(i)))
	}
	if err := s.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := mx.get("hoardmq_sink_postgres_written_total{table=readings}"); got != 4 {
		t.Fatalf("written %v at Stop, want 4", got)
	}
	s.OnPublishAccepted(context.Background(), ext.SourceClient, msg("s/x", 1, "late"))
	if got := mx.get("hoardmq_sink_postgres_dropped_total{reason=shutdown,table=readings}"); got != 1 {
		t.Fatalf("after Stop: shutdown %v, want 1", got)
	}

	hold := make(chan struct{})
	defer close(hold)
	hw := &fakeWriter{hold: hold}
	h, hmx, _ := started(t, hw, `,"batch":{"lingerMs":0,"maxRows":2},"flushTimeoutMs":200`)
	for i := range 5 {
		h.OnPublishAccepted(context.Background(), ext.SourceClient, msg("s/x", 1, strconv(i)))
	}
	start := time.Now()
	if err := h.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if el := time.Since(start); el < 200*time.Millisecond || el > 2*time.Second {
		t.Fatalf("Stop took %v, want the 200 ms flush", el)
	}
	if got := hmx.get("hoardmq_sink_postgres_dropped_total{reason=shutdown,table=readings}"); got != 5 {
		t.Fatalf("shutdown %v, want 5", got)
	}
	if r, b := h.q.size(); r != 0 || b != 0 {
		t.Fatalf("buffer after Stop: %d rows, %d bytes", r, b)
	}
}

// TestTransient: a server error in the retried classes, or one with no
// SQLSTATE, is retried; any other is permanent.
func TestTransient(t *testing.T) {
	for _, c := range []struct {
		err  error
		want bool
	}{
		{errors.New("dial tcp 127.0.0.1:7066: connect: connection refused"), true},
		{context.DeadlineExceeded, true},
		{&pgconn.PgError{Code: "08006"}, true},
		{&pgconn.PgError{Code: "40001"}, true},
		{&pgconn.PgError{Code: "53300"}, true},
		{&pgconn.PgError{Code: "57P01"}, true},
		{&pgconn.PgError{Code: "42P01"}, false},
		{&pgconn.PgError{Code: "23505"}, false},
		{&pgconn.PgError{Code: "22001"}, false},
		{fmt.Errorf("copy: %w", &pgconn.PgError{Code: "42501"}), false},
	} {
		if got := transient(c.err); got != c.want {
			t.Errorf("%v: transient %v, want %v", c.err, got, c.want)
		}
	}
}

// TestNoCredentialLogged: the database password reaches the module and no
// log line or metric label.
func TestNoCredentialLogged(t *testing.T) {
	isolate(t)
	pw := filepath.Join(t.TempDir(), "db.pw")
	if err := os.WriteFile(pw, []byte("db-secret-value\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	log, mx := &logBuf{}, &fakeMetrics{}
	w := &fakeWriter{failFor: map[string]error{"readings": errors.New("failed to connect to `user=hoardmq database=mqtt`: server error")}}
	s := &sink{readFile: os.ReadFile, open: func(*settings) (writer, error) { return w, nil }}
	doc := `{"connString":"postgres://hoardmq@127.0.0.1/mqtt","passwordFile":"` + pw + `","routes":[{"filter":"s/#","table":"readings"}],"batch":{"lingerMs":0},"flushTimeoutMs":100}`
	if err := s.Provision(&ext.Context{Logger: slog.New(slog.NewTextHandler(log, nil)), Namespace: "default"}, json.RawMessage(doc)); err != nil {
		t.Fatal(err)
	}
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
	s.UseMetrics(mx)
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	s.OnPublishAccepted(context.Background(), ext.SourceClient, msg("s/1", 1, "x"))
	eventually(t, "a retry", func() bool { return mx.get("hoardmq_sink_postgres_retries_total{}") > 0 })
	_ = s.Stop(context.Background())
	if s.set.pool.ConnConfig.Password != "db-secret-value" || !strings.Contains(log.String(), "server error") {
		t.Fatalf("presence: password %q, log:\n%s", s.set.pool.ConnConfig.Password, log.String())
	}
	mx.mu.Lock()
	var keys []string
	for k := range mx.v {
		keys = append(keys, k)
	}
	mx.mu.Unlock()
	if out := log.String() + strings.Join(keys, "\n"); strings.Contains(out, "db-secret-value") {
		t.Fatalf("the database password in a log line or label:\n%s", out)
	}
}
