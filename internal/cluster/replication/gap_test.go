package replication

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

// gapScenario drops exactly one replication frame between owner n1 and
// follower n2 with the link up, then writes again, and waits for the stream
// to settle. It returns the follower's copy, how many gaps the follower
// reported, and how many resyncs of n2 the owner completed.
func gapScenario(t *testing.T, h *harness, sinks map[string]*counters) (copy *store.SessionState, gaps, resyncs float64) {
	t.Helper()
	h.applyMap(shardmap.Compute(1, h.order, 2, 2))
	waitSettled(h, 1)
	id := clientIn(2, 0) // leader n1, follower n2
	h.writeSession("n1", id, 1)
	n1, n2 := h.nodes["n1"], h.nodes["n2"]
	if err := n1.barrier(context.Background()); err != nil {
		t.Fatal(err)
	}
	key := store.Key{Namespace: "default", ClientID: id, Epoch: 1}
	entry := func(seq uint64) store.Entry {
		return store.Entry{Seq: seq, Message: &message.Message{Namespace: "default", Topic: "a/b", Payload: []byte("x")}, QoS: 1}
	}
	drained := func() bool {
		p := n1.rep.outboundFor("n2")
		p.mu.Lock()
		defer p.mu.Unlock()
		return len(p.queue) == 0
	}
	// A resync sends what the owner's session manager holds; the harness's
	// manager is a fake fed by hand, so it is given the state the owner will hold
	// by the time the gap is reported.
	n1.sessions.mu.Lock()
	n1.sessions.sessions = []*store.SessionState{{
		SessionMeta:   store.SessionMeta{Namespace: "default", ClientID: id, Epoch: 1, Version: 5, ExpiryInterval: 300},
		Subscriptions: []store.SubscriptionRecord{{Filter: "a/b", QoS: 1}},
		Entries:       []store.Entry{entry(1), entry(2), entry(3)},
	}}
	n1.sessions.mu.Unlock()
	// One frame dropped on the wire, link up.
	n1.tr.SetFault("n2", &transport.Fault{Drop: 1, Classes: []transport.Class{transport.ReplicationAppend}, Rand: rand.New(rand.NewSource(3))}) //nolint:gosec // seeded
	if err := n1.own.Enqueue(key, entry(2)); err != nil {
		t.Fatal(err)
	}
	if err := n1.own.Sync(); err != nil {
		t.Fatal(err)
	}
	h.waitFor("the dropped frame to be sent", 2*time.Second, drained)
	n1.tr.SetFault("n2", nil)
	// The next frame arrives past the gap.
	if err := n1.own.Enqueue(key, entry(3)); err != nil {
		t.Fatal(err)
	}
	if err := n1.own.Sync(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// The barrier passes once the stream is whole again — at once on the
	// defective build, after the resync on the sound one.
	if err := n1.rep.Barrier(ctx, []store.Scope{{Namespace: "default", ClientID: id, Epoch: 1}}); err != nil {
		t.Fatalf("the barrier did not pass after the stream settled: %v", err)
	}
	h.waitFor("the follower to apply what it was sent", 2*time.Second, func() bool {
		ss := n2.rep.ReplicaState().Find("default", id)
		return ss != nil && len(ss.Entries) >= 2
	})
	return n2.rep.ReplicaState().Find("default", id),
		sinks["n2"].get("hoardmq_replication_gaps_total", map[string]string{"peer": "n1"}),
		sinks["n1"].get("hoardmq_replication_resyncs_total", map[string]string{"peer": "n2"})
}

func seqsOf(ss *store.SessionState) []uint64 {
	var out []uint64
	if ss != nil {
		for _, e := range ss.Entries {
			out = append(out, e.Seq)
		}
	}
	return out
}

// TestLostReplicationFrameIsDetectedAndRepaired: a frame lost with the link
// up is reported by the follower as a gap; the owner treats the report as a
// hole and resyncs; the follower's copy ends whole. (That a barrier fails
// while the hole is open is broken()'s contract, tested with send failures.)
func TestLostReplicationFrameIsDetectedAndRepaired(t *testing.T) {
	h, sinks := withCounters(t, []string{"n1", "n2"}, 2, 2)
	copy, gaps, resyncs := gapScenario(t, h, sinks)
	if gaps != 1 {
		t.Fatalf("the follower reported %v gaps, want 1", gaps)
	}
	if resyncs != 1 {
		t.Fatalf("the owner completed %v resyncs of n2, want 1 — the gap report should have started it", resyncs)
	}
	if got := seqsOf(copy); len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("after the resync the follower holds %v, want [1 2 3]", got)
	}
}

// TestDefectIgnoreReplicationGaps is the control: with the check off, no gap
// is reported, nothing resyncs, and the follower's copy lacks the frame.
func TestDefectIgnoreReplicationGaps(t *testing.T) {
	h, sinks := withCountersAnd(t, []string{"n1", "n2"}, 2, 2, func(o *Options) { o.IgnoreGaps = true })
	copy, gaps, resyncs := gapScenario(t, h, sinks)
	if gaps != 0 || resyncs != 0 {
		t.Fatalf("defective build: %v gaps reported, %v resyncs; want none", gaps, resyncs)
	}
	if got := seqsOf(copy); len(got) != 2 || got[0] != 1 || got[1] != 3 {
		t.Fatalf("defective build: the follower should hold [1 3], holds %v", got)
	}
}

// TestFirstFrameAboveOneIsAGap: a stream starts at 1, so a follower whose
// first frame from a peer is above it reports the gap — the owner's first
// push to it was lost.
func TestFirstFrameAboveOneIsAGap(t *testing.T) {
	h, sinks := withCounters(t, []string{"n1", "n2"}, 2, 2)
	f := h.nodes["n2"].rep.follower
	w := newWorker(f, "n1")
	defer w.close()
	w.process([]frame{{Kind: kindResyncMark, Seq: 3}})
	if got := sinks["n2"].get("hoardmq_replication_gaps_total", map[string]string{"peer": "n1"}); got != 1 {
		t.Fatalf("a first frame at sequence 3 reported %v gaps, want 1", got)
	}
	w2 := newWorker(f, "n3")
	defer w2.close()
	w2.process([]frame{{Kind: kindResyncMark, Seq: 1}})
	if got := sinks["n2"].get("hoardmq_replication_gaps_total", map[string]string{"peer": "n3"}); got != 0 {
		t.Fatalf("a first frame at sequence 1 reported %v gaps, want 0", got)
	}
}
