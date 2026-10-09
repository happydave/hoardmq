package replication

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

// withCountersAnd is withCounters with a further option mutation per node.
func withCountersAnd(t *testing.T, names []string, shards, replicas int, mutate func(*Options)) (*harness, map[string]*counters) {
	t.Helper()
	sinks := map[string]*counters{}
	var mu sync.Mutex
	next := 0
	h := newHarnessWith(t, names, shards, replicas, func(o *Options) {
		mu.Lock()
		c := newCounters()
		sinks[names[next]] = c
		next++
		mu.Unlock()
		o.Metrics = c
		mutate(o)
	})
	return h, sinks
}

// retainedHeldBy reports whether a node's replica holds a retained message.
func retainedHeldBy(h *harness, node, ns, topic string) bool {
	for _, m := range h.nodes[node].rep.ReplicaState().Retained {
		if m.Namespace == ns && m.Topic == topic {
			return true
		}
	}
	return false
}

// replicateRetained publishes a retained message from a node that does not
// lead its shard: it is forwarded to the leader, stored there, and replicated
// to the shard's followers, which the wait covers.
func replicateRetained(t *testing.T, h *harness, from, follower, topic string) {
	t.Helper()
	m := &message.Message{Namespace: "default", Topic: topic, Payload: []byte("kept"), Retain: true}
	if h.nodes[from].rep.StoreRetained(m) {
		t.Fatalf("%s stored the retained message locally; it should not lead the shard", from)
	}
	if err := h.nodes[from].barrier(context.Background()); err != nil {
		t.Fatal(err)
	}
	h.waitFor(follower+" to hold the retained copy", 5*time.Second, func() bool {
		return retainedHeldBy(h, follower, "default", topic)
	})
}

// TestRetainedReplicaDropRespectsTheVersionFloor: a retained copy a follower
// holds is dropped only on a map at or past what the node last acted on;
// below that floor it is kept and counted, as a session copy is.
func TestRetainedReplicaDropRespectsTheVersionFloor(t *testing.T) {
	h, sinks := withCounters(t, []string{"n1", "n2"}, 2, 2)
	m1 := shardmap.Compute(1, h.order, 2, 2)
	h.applyMap(m1)
	waitSettled(h, 1)
	topic := topicIn(2, 0) // led by n1; n2 follows
	replicateRetained(t, h, "n2", "n2", topic)
	// n1 alone: n2 holds nothing under the new map.
	gone := shardmap.Compute(2, []string{"n1"}, 1, 2)
	r := h.nodes["n2"].rep
	kept := func() float64 {
		return sinks["n2"].get("hoardmq_replicas_kept_total", map[string]string{"reason": "map-behind"})
	}

	// The node last acted on version 10 in a previous incarnation.
	r.recMu.Lock()
	r.persisted, r.persistedKnown = 10, true
	r.recMu.Unlock()
	r.rebalance(m1, gone)
	if !retainedHeldBy(h, "n2", "default", topic) {
		t.Fatal("a retained copy was dropped on a map below the version floor")
	}
	if got := kept(); got != 1 {
		t.Fatalf("kept copies counted %v, want 1", got)
	}

	// At the floor the copy is judged as a moved shard's: kept until
	// the shard's leader under the new map releases it, not dropped at once.
	r.recMu.Lock()
	r.persisted = 2
	r.recMu.Unlock()
	r.rebalance(m1, gone)
	if !retainedHeldBy(h, "n2", "default", topic) {
		t.Fatal("a retained copy was dropped at the version floor before its shard's leader released it")
	}
	if note, ok := r.keptNote(0); !ok || note.Releaser != "n1" || note.Version != 2 {
		t.Fatalf("kept note %+v (%v), want released by n1 at map 2", note, ok)
	}
}

// TestUnreadableReconciledFileKeepsEveryCopy: a node whose reconciled-version
// file cannot be read has no floor, so the reconcile that first runs in this
// incarnation drops nothing; it writes a version of its own, and from then on
// drops as before.
func TestUnreadableReconciledFileKeepsEveryCopy(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, reconciledFile), []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	h, sinks := withCountersAnd(t, []string{"n1", "n2"}, 2, 2, func(o *Options) {
		if o.Node == "n2" {
			o.ReplicaDir = dir
		}
	})
	r := h.nodes["n2"].rep
	floorKnown := func() bool {
		r.recMu.Lock()
		defer r.recMu.Unlock()
		return r.persistedKnown
	}
	if floorKnown() {
		t.Fatal("an unreadable reconciled file read as known")
	}
	// A copy from a previous incarnation, under a map this node no longer
	// holds the shard in.
	topic := topicIn(2, 0)
	if _, err := r.follower.apply(store.RetainRecord(&message.Message{Namespace: "default", Topic: topic, Payload: []byte("kept"), Retain: true})); err != nil {
		t.Fatal(err)
	}
	gone := func(v uint64) shardmap.Map { return shardmap.Compute(v, []string{"n1"}, 1, 2) }
	h.applyMap(gone(1))
	h.waitFor("the first reconcile to write a version", 5*time.Second, floorKnown)
	if !retainedHeldBy(h, "n2", "default", topic) {
		t.Fatal("the first reconcile of an incarnation with no floor dropped a copy")
	}
	if got := sinks["n2"].get("hoardmq_replicas_kept_total", map[string]string{"reason": "map-behind"}); got != 1 {
		t.Fatalf("kept copies counted %v, want 1", got)
	}
	h.applyMap(gone(2))
	h.waitFor("the copy to be dropped once the floor is known", 5*time.Second, func() bool {
		return !retainedHeldBy(h, "n2", "default", topic)
	})
}
