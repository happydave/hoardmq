package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/ext"
)

// fakeMetrics counts by name and sorted labels, and runs its collectors on
// read.
type fakeMetrics struct {
	mu         sync.Mutex
	v          map[string]float64
	collectors []collector
}

type collector = func(func(string, map[string]string, float64))

func seriesKey(name string, labels map[string]string) string {
	parts := make([]string, 0, len(labels))
	for k, v := range labels {
		parts = append(parts, k+"="+v)
	}
	sort.Strings(parts)
	return name + "{" + strings.Join(parts, ",") + "}"
}

func (m *fakeMetrics) Add(name string, labels map[string]string, delta float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.v == nil {
		m.v = map[string]float64{}
	}
	m.v[seriesKey(name, labels)] += delta
}

func (m *fakeMetrics) Set(name string, labels map[string]string, value float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.v == nil {
		m.v = map[string]float64{}
	}
	m.v[seriesKey(name, labels)] = value
}

func (m *fakeMetrics) Snapshot() []ext.Metric { return nil }

func (m *fakeMetrics) AddCollector(f collector) {
	m.mu.Lock()
	m.collectors = append(m.collectors, f)
	m.mu.Unlock()
}

// get runs the collectors and returns the series named by key.
func (m *fakeMetrics) get(key string) float64 {
	m.mu.Lock()
	cs := append([]collector(nil), m.collectors...)
	m.mu.Unlock()
	for _, c := range cs {
		c(m.Set)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.v[key]
}

// keys lists every series key, labels included.
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

// logBuf is a log a test reads back.
type logBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *logBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *logBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// clock is a time a test moves.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func moduleContext(log *logBuf, ttl time.Duration) *ext.Context {
	return &ext.Context{
		Logger: slog.New(slog.NewTextHandler(log, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Node:   "n1", Namespace: "default", DecisionTTL: ttl,
	}
}

// newAuth provisions and validates auth.http from doc.
func newAuth(t *testing.T, doc string, ttl time.Duration) (*authHTTP, *fakeMetrics, *logBuf, *clock) {
	t.Helper()
	a := &authHTTP{readFile: readFileOS}
	c := &clock{t: time.Unix(1_800_000_000, 0)}
	a.now = c.now
	log := &logBuf{}
	if err := a.Provision(moduleContext(log, ttl), json.RawMessage(doc)); err != nil {
		t.Fatal(err)
	}
	if err := a.Validate(); err != nil {
		t.Fatal(err)
	}
	m := &fakeMetrics{}
	a.UseMetrics(m)
	return a, m, log, c
}

// newEvents provisions, validates and starts events.http from doc.
func newEvents(t *testing.T, doc string) (*eventsHTTP, *fakeMetrics, *logBuf) {
	t.Helper()
	e := &eventsHTTP{readFile: readFileOS, now: func() time.Time { return time.UnixMilli(1_800_000_000_123) }}
	log := &logBuf{}
	if err := e.Provision(moduleContext(log, 0), json.RawMessage(doc)); err != nil {
		t.Fatal(err)
	}
	if err := e.Validate(); err != nil {
		t.Fatal(err)
	}
	m := &fakeMetrics{}
	e.UseMetrics(m)
	if err := e.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Stop(context.Background()) })
	return e, m, log
}

// eventually polls cond until it holds or ten seconds pass.
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
