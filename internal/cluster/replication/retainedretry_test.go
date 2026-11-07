package replication

import (
	"bytes"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
)

// logBuffer is a node's debug log, kept for a test to read.
type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *logBuffer) count(substr string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return strings.Count(b.buf.String(), substr)
}

// withNodeLog is withCountersAnd with node's debug log kept in the returned
// buffer.
func withNodeLog(t *testing.T, node string) (*harness, map[string]*counters, *logBuffer) {
	t.Helper()
	logs := &logBuffer{}
	h, sinks := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, func(o *Options) {
		quickAck(o)
		if o.Node == node {
			o.Logger = slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})).With("node", node)
		}
	})
	return h, sinks, logs
}

// noteEnded is the line a keep note's end logs.
const noteEnded = `msg="retained keep note ended"`

// TestOldLeaderRetryKeepsItsNoteWhole: the promotion of
// TestOldLeaderKeepsItsOwnCopyUntilReleased, with the order forced. n2's
// rebalance is held before it promotes its replica copy, so n1's first
// hand-over is not acknowledged and n1 keeps its copy with the hand-over owed.
// The retry is answered held, and n1 keeps the copy for n2's release: the
// same keep, noted throughout and counted once.
func TestOldLeaderRetryKeepsItsNoteWhole(t *testing.T) {
	h, sinks, logs := withNodeLog(t, "n1")
	allAlive(h)
	h.applyMap(moveMap(h, 1, sh("n1", "n2"), sh("n2", "n3")))
	waitSettled(h, 1)
	tp := topicIn(2, 0)
	if err := writeRetained(h, "n1", tp, "kept"); err != nil {
		t.Fatal(err)
	}
	h.waitFor("n2 to hold the copy", 5*time.Second, func() bool { return replicaHolds(h, "n2", tp) == "kept" })
	dropAll(h, "n2", "n3", 15)
	v2 := moveMap(h, 2, sh("n2", "n3"), sh("n2", "n3"))

	n1, n2 := h.nodes["n1"], h.nodes["n2"]
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	n2.rep.fanOutGate = func() { <-release }
	stop := sampleOwn(t, h, "n1", tp)

	for _, n := range []string{"n2", "n1"} {
		if _, err := h.nodes[n].table.Apply(0, shardmap.Encode(v2)); err != nil {
			t.Fatal(err)
		}
	}
	h.waitFor("n1 to reconcile v2", 5*time.Second, func() bool { return n1.rep.Reconciled() == 2 })
	if first, ok := n1.rep.keptNote(0); !ok || !first.HandOver || !first.Own {
		t.Fatalf("precondition: n1's note after v2 is %+v (noted %v), want its own copy kept with the hand-over owed", first, ok)
	}
	unblock()
	if _, err := h.nodes["n3"].table.Apply(0, shardmap.Encode(v2)); err != nil {
		t.Fatal(err)
	}
	waitSettled(h, 2)
	h.waitFor("n1's retried hand-over to be acknowledged", 5*time.Second, func() bool {
		n, ok := n1.rep.keptNote(0)
		return ok && !n.HandOver
	})
	time.Sleep(200 * time.Millisecond)
	missing := stop()
	last, _ := n1.rep.keptNote(0)
	if last.Releaser != "n2" || last.Version != 2 || !last.Own || last.HandOver {
		t.Errorf("n1's note after the retry is %+v, want n2's release awaited at v2 for its own copy, nothing owed", last)
	}
	if got := keptMovedRetained(sinks, "n1"); got != 1 {
		t.Errorf("kept{retained-moved} on n1 = %v, want 1: one keep of the shard", got)
	}
	if got := logs.count(noteEnded); got != 0 {
		t.Errorf("n1 ended its keep note %d times across the retry, want 0", got)
	}
	if missing != 0 {
		t.Errorf("n1's own copy was missing in %d samples", missing)
	}
}

// TestOldFollowerRetryKeepsItsNoteWhole: under v2 n3, which held nothing,
// leads and n1 follows; n2 no longer holds the shard. n2's first hand-over of
// its replica copy to n3 is lost, so it keeps the copy with the hand-over
// owed; its retry is acknowledged and it keeps the copy for n3's release,
// which no frame from n3 brings within the test: the same keep, noted
// throughout and counted once.
func TestOldFollowerRetryKeepsItsNoteWhole(t *testing.T) {
	h, sinks, logs := withNodeLog(t, "n2")
	allAlive(h)
	h.applyMap(moveMap(h, 1, sh("n1", "n2"), sh("n2", "n3")))
	waitSettled(h, 1)
	tp := topicIn(2, 0)
	if err := writeRetained(h, "n1", tp, "kept"); err != nil {
		t.Fatal(err)
	}
	h.waitFor("n2 to hold the copy", 5*time.Second, func() bool { return replicaHolds(h, "n2", tp) == "kept" })
	n2 := h.nodes["n2"]
	dropAll(h, "n2", "n3", 16)
	dropAll(h, "n3", "n2", 17)
	stop := sampleReplica(t, h, "n2", tp)
	h.applyMap(moveMap(h, 2, sh("n3", "n1"), sh("n2", "n3")))
	waitSettled(h, 2)
	if first, ok := n2.rep.keptNote(0); !ok || !first.HandOver || first.Own {
		t.Fatalf("precondition: n2's note after v2 is %+v (noted %v), want its replica copy kept with the hand-over owed", first, ok)
	}
	n2.tr.SetFault("n3", nil)
	h.waitFor("n2's retried hand-over to be acknowledged", 5*time.Second, func() bool {
		n, ok := n2.rep.keptNote(0)
		return ok && !n.HandOver
	})
	time.Sleep(200 * time.Millisecond)
	missing := stop()
	last, _ := n2.rep.keptNote(0)
	if last.Releaser != "n3" || last.Version != 2 || last.Own || last.HandOver {
		t.Errorf("n2's note after the retry is %+v, want n3's release awaited at v2 for a follower's copy, nothing owed", last)
	}
	if got := keptMovedRetained(sinks, "n2"); got != 1 {
		t.Errorf("kept{retained-moved} on n2 = %v, want 1: one keep of the shard", got)
	}
	if got := logs.count(noteEnded); got != 0 {
		t.Errorf("n2 ended its keep note %d times across the retry, want 0", got)
	}
	if missing != 0 {
		t.Errorf("n2's replica copy was missing in %d samples", missing)
	}
}

// sampleOwn reads node's own copy of topic until the returned stop, which
// reports in how many samples it was missing.
func sampleOwn(t *testing.T, h *harness, node, topic string) func() int64 {
	return sample(t, func() bool { return ownHolds(h, node, topic) == "kept" })
}

func sampleReplica(t *testing.T, h *harness, node, topic string) func() int64 {
	return sample(t, func() bool { return replicaHolds(h, node, topic) == "kept" })
}

func sample(t *testing.T, held func() bool) func() int64 {
	t.Helper()
	var quit atomic.Bool
	var missing atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		for !quit.Load() {
			if !held() {
				missing.Add(1)
			}
			time.Sleep(200 * time.Microsecond)
		}
	}()
	stop := sync.OnceValue(func() int64 {
		quit.Store(true)
		<-done
		return missing.Load()
	})
	t.Cleanup(func() { stop() })
	return stop
}
