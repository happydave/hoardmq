package sinkkafka

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/happydave/hoardmq/ext"
	"github.com/twmb/franz-go/pkg/kgo"
)

// fakeProducer is a producer with a bounded buffer that delivers nothing
// until told to. It fails a produce into a full buffer with ErrMaxBuffered,
// as franz-go's TryProduce does.
type fakeProducer struct {
	mu       sync.Mutex
	max      int
	buffered []pending
	closed   bool
	flushed  bool
	attempts int
}

type pending struct {
	rec     *kgo.Record
	promise func(*kgo.Record, error)
}

func (f *fakeProducer) TryProduce(_ context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
	f.mu.Lock()
	f.attempts++
	if len(f.buffered) >= f.max {
		f.mu.Unlock()
		promise(r, kgo.ErrMaxBuffered)
		return
	}
	f.buffered = append(f.buffered, pending{r, promise})
	f.mu.Unlock()
}

func (f *fakeProducer) BufferedProduceRecords() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return int64(len(f.buffered))
}

func (f *fakeProducer) BufferedProduceBytes() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	var n int64
	for _, p := range f.buffered {
		n += int64(len(p.rec.Key) + len(p.rec.Value))
	}
	return n
}

// finish completes the oldest n buffered records with err.
func (f *fakeProducer) finish(n int, err error) {
	f.mu.Lock()
	done := f.buffered[:n]
	f.buffered = append([]pending(nil), f.buffered[n:]...)
	f.mu.Unlock()
	for _, p := range done {
		p.promise(p.rec, err)
	}
}

func (f *fakeProducer) Flush(ctx context.Context) error {
	f.mu.Lock()
	f.flushed = true
	empty := len(f.buffered) == 0
	f.mu.Unlock()
	if empty {
		return nil
	}
	<-ctx.Done()
	return ctx.Err()
}

func (f *fakeProducer) UnsafeAbortBufferedRecords() {
	f.mu.Lock()
	n := len(f.buffered)
	f.mu.Unlock()
	f.finish(n, kgo.ErrAborting)
}

func (f *fakeProducer) Close() {
	f.mu.Lock()
	f.closed = true
	f.mu.Unlock()
}

// fakeMetrics counts by name and sorted labels, and runs its collectors on
// snapshot.
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

// total sums every series whose name is name.
func (m *fakeMetrics) total(name string) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	var n float64
	for k, v := range m.v {
		if strings.HasPrefix(k, name+"{") {
			n += v
		}
	}
	return n
}

// started provisions, validates and starts a sink with a fake producer of
// capacity max, from the configuration doc.
func started(t *testing.T, doc string, max int) (*sink, *fakeProducer, *fakeMetrics) {
	t.Helper()
	fp := &fakeProducer{max: max}
	s := &sink{newProducer: func(*settings) (producer, error) { return fp, nil }}
	ctx := &ext.Context{Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Node: "n1", Namespace: "default"}
	if err := s.Provision(ctx, json.RawMessage(doc)); err != nil {
		t.Fatal(err)
	}
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
	m := &fakeMetrics{}
	s.UseMetrics(m)
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Stop(context.Background()) })
	return s, fp, m
}

var errFake = errors.New("fake: the broker refused the record")

// state reads what the fake has seen.
func (f *fakeProducer) state() (attempts int, flushed bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attempts, f.flushed
}
