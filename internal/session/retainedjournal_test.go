package session

import (
	"io"
	"log/slog"
	"testing"

	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
	"github.com/happydave/hoardmq/internal/store/faultdisk"
)

func retained(v string) *message.Message {
	return &message.Message{Namespace: "default", Topic: "r", Payload: []byte(v), Retain: true}
}

// TestInjectRetainedReportsAFailedAppend: the shard leader's install of a
// retained write whose record failed to append is not durable, though no sync
// is left for it to fail.
func TestInjectRetainedReportsAFailedAppend(t *testing.T) {
	metrics := &seriesMetrics{}
	m, d, _ := journaledManager(t, func(o *Options) { o.Metrics = metrics })
	d.FailNext(1)
	if m.InjectRetained(retained("a")) {
		t.Fatal("a retained write whose record failed to append reported durable")
	}
	if n := failedAppends(metrics); n != 1 {
		t.Fatalf("the failed append counted %v, want 1", n)
	}
	if !m.InjectRetained(retained("b")) {
		t.Fatal("a retained write with no fault reported not durable")
	}
}

// TestInjectRetainedReportsAFailedSync: an append that did not roll, whose
// sync fails, is not durable either.
func TestInjectRetainedReportsAFailedSync(t *testing.T) {
	d := faultdisk.New()
	st, err := store.Open(t.TempDir(), store.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Sync: d.Sync})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	metrics := &seriesMetrics{}
	m, _ := newManager(t, func(o *Options) {
		o.Journal = st
		o.Metrics = metrics
	})
	d.FailNext(1)
	if m.InjectRetained(retained("a")) {
		t.Fatal("a retained write whose sync failed reported durable")
	}
	snap := metrics.snapshot()
	if snap[seriesKey("hoardmq_journal_failures_total", opSync)] != 1 || snap[seriesKey("hoardmq_journal_failures_total", opAppend)] != 0 {
		t.Fatalf("precondition: want one failed sync and no failed append, have %v", snap)
	}
	if !m.InjectRetained(retained("b")) {
		t.Fatal("a retained write with no fault reported not durable")
	}
}

// TestDeleteRetainedReportsAFailedAppend: the admin API's delete, stored on
// this node, reports a record that failed to append, which no durability
// wait would notice.
func TestDeleteRetainedReportsAFailedAppend(t *testing.T) {
	m, d, _ := journaledManager(t, nil)
	d.FailNext(1)
	if _, appended := m.DeleteRetained("default", "r"); appended {
		t.Fatal("a delete whose record failed to append reported it appended")
	}
	if _, appended := m.DeleteRetained("default", "r"); !appended {
		t.Fatal("a delete with no fault reported its record not appended")
	}
}
