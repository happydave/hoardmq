package replication

import (
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
)

// TestTombstoneAcrossALeaderChange runs a clear's tombstone through a leader
// change on four nodes, with an old holder the leaders treat differently.
//
// Under v1 n1 leads shard 0 and n3 follows; n3 holds "old" and is declared
// before v2 moves the shard's follower to n2, so n1's move passes n3 over and
// n3 keeps its copy. n1 takes a client's clear. Past the hold, n1 keeps the
// tombstone for n3, and n2, its follower, keeps it too: a follower drops one
// only on its leader's word.
//
// n1 is declared and v3 makes n2 the leader with n4. n1 comes back, is
// released, and is declared again. n2 never knew n3 as a holder, so once its
// own hold has passed it drops the tombstone. Then n3's hand-on, held by
// BeforeRetainedHandOver until now, reaches n2: the older copy installs into
// the emptied topic and is counted below-dropped.
func TestTombstoneAcrossALeaderChange(t *testing.T) {
	hold := time.Second
	release := make(chan struct{})
	var once sync.Once
	open := func() { once.Do(func() { close(release) }) }
	h, sinks := withCountersAnd(t, []string{"n1", "n2", "n3", "n4"}, 2, 2, func(o *Options) {
		quickAck(o)
		o.RetainedTombstoneHold = hold
		if o.Node == "n3" {
			o.BeforeRetainedHandOver = func(string) { <-release }
		}
	})
	t.Cleanup(open)
	allAlive(h)
	all := []string{"n1", "n2", "n3", "n4"}
	h.applyMap(shardmap.Map{Version: 1, Basis: all, Replicas: 2, Shards: []shardmap.Shard{sh("n1", "n3"), sh("n2", "n4")}})
	waitSettled(h, 1)
	tp := topicIn(2, 0)
	if err := writeRetained(h, "n1", tp, "old"); err != nil {
		t.Fatalf("precondition: the write under v1: %v", err)
	}
	h.waitFor("n3 to hold the copy", 5*time.Second, func() bool { return replicaHolds(h, "n3", tp) == "old" })

	declare(h, "n3")
	h.applyMap(shardmap.Map{Version: 2, Basis: []string{"n1", "n2", "n4"}, Replicas: 2, Shards: []shardmap.Shard{sh("n1", "n2"), sh("n2", "n4")}})
	waitSettled(h, 2)
	h.waitFor("n1's move to complete with n3 passed over", 5*time.Second, func() bool {
		r := h.nodes["n1"].rep
		r.rmoves.mu.Lock()
		defer r.rmoves.mu.Unlock()
		_, passed := r.rmoves.passed[0]["n3"]
		return passed && len(r.rmoves.pending) == 0
	})
	if replicaHolds(h, "n3", tp) != "old" || h.nodes["n3"].rep.KeptRetained() != 1 {
		t.Fatalf("precondition: n3 holds %q and keeps %d shards, want its copy kept", replicaHolds(h, "n3", tp), h.nodes["n3"].rep.KeptRetained())
	}
	if err := writeRetained(h, "n1", tp, ""); err != nil {
		t.Fatalf("precondition: the clear under v2: %v", err)
	}
	h.waitFor("n2 to hold the tombstone", 5*time.Second, func() bool { return replicaTombstone(h, "n2", tp) })
	time.Sleep(2 * hold)
	if !ownTombstone(h, "n1", tp) {
		t.Fatal("n1 dropped the tombstone with n3, an old holder it passed over, not yet released")
	}
	if !replicaTombstone(h, "n2", tp) {
		t.Fatal("n2 dropped the tombstone on its own reading")
	}

	for _, p := range []string{"n2", "n3", "n4"} {
		dropAll(h, "n1", p, 31)
	}
	declare(h, "n1")
	h.applyMap(shardmap.Map{Version: 3, Basis: []string{"n2", "n4"}, Replicas: 2, Shards: []shardmap.Shard{sh("n2", "n4"), sh("n2", "n4")}})
	waitSettled(h, 3)
	h.waitFor("n4 to hold the tombstone from n2's push", 5*time.Second, func() bool { return replicaTombstone(h, "n4", tp) })
	if !ownTombstone(h, "n2", tp) {
		t.Fatal("precondition: n2, promoted, holds no tombstone")
	}

	for _, p := range []string{"n2", "n3", "n4"} {
		h.nodes["n1"].tr.SetFault(p, nil)
	}
	revive(h, "n1")
	h.waitFor("n2 to release n1", 5*time.Second, func() bool {
		r := h.nodes["n2"].rep
		r.rmoves.mu.Lock()
		defer r.rmoves.mu.Unlock()
		return len(r.rmoves.passed) == 0 && len(r.rmoves.pending) == 0
	})
	h.waitFor("n2 to drop the tombstone and name it to n4", 5*time.Second, func() bool {
		return !ownTombstone(h, "n2", tp) && !replicaTombstone(h, "n4", tp)
	})

	declare(h, "n1")
	revive(h, "n3")
	open()
	below := func() float64 {
		return sinks["n2"].get("hoardmq_retained_merges_total", map[string]string{"path": "handover", "outcome": "below-dropped"})
	}
	h.waitFor("n3's hand-on to reach n2", 5*time.Second, func() bool { return h.nodes["n3"].rep.KeptRetained() == 0 })
	if got := ownHolds(h, "n2", tp); got != "old" {
		t.Fatalf("n2 holds %q after n3's hand-on into the emptied topic, want the older copy installed", got)
	}
	if got := below(); got < 1 {
		t.Fatalf("n3's older copy installed below the dropped tombstone uncounted: below-dropped %v", got)
	}
}
