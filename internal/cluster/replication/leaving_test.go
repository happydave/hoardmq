package replication

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/lease"
	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/store"
)

func TestPlacementBasis(t *testing.T) {
	for _, c := range []struct {
		name           string
		alive, leaving []string
		replicas       int
		want           []string
	}{
		{"three pods, two copies, one leaving", []string{"n1", "n2", "n3"}, []string{"n3"}, 2, []string{"n1", "n2"}},
		{"three pods, three copies: the floor keeps it", []string{"n1", "n2", "n3"}, []string{"n3"}, 3, []string{"n1", "n2", "n3"}},
		{"two pods, two copies: the floor keeps it", []string{"n1", "n2"}, []string{"n2"}, 2, []string{"n1", "n2"}},
		{"four pods, two copies, two leaving", []string{"n1", "n2", "n3", "n4"}, []string{"n2", "n4"}, 2, []string{"n1", "n3"}},
		{"three pods, two copies, two leaving: the floor keeps both", []string{"n1", "n2", "n3"}, []string{"n2", "n3"}, 2, []string{"n1", "n2", "n3"}},
		{"the leader leaving is left out", []string{"n1", "n2", "n3"}, []string{"n1"}, 2, []string{"n2", "n3"}},
		{"nobody leaving", []string{"n3", "n1", "n2"}, nil, 2, []string{"n1", "n2", "n3"}},
		{"one copy: a lone stayer is enough", []string{"n1", "n2"}, []string{"n2"}, 1, []string{"n1"}},
	} {
		if got := placementBasis(c.alive, c.leaving, c.replicas); !slices.Equal(got, c.want) {
			t.Errorf("%s: placementBasis = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestPlacementOf(t *testing.T) {
	without := shardmap.Map{Version: 2, Basis: []string{"n1", "n2"}}
	with := shardmap.Map{Version: 1, Basis: []string{"n1", "n2", "n3"}}
	all := []string{"n1", "n2", "n3"}
	for _, c := range []struct {
		name                    string
		leaving                 []string
		replicas                int
		cur                     shardmap.Map
		reconciled              uint64
		willPlace, left, marked bool
	}{
		{"marked, the map without it reconciled: left", []string{"n3"}, 2, without, 2, false, true, true},
		{"marked, three copies: the floor keeps it", []string{"n3"}, 3, with, 1, true, false, true},
		{"not marked: placed, not marked", nil, 2, with, 1, true, false, false},
		{"the map without it, not yet reconciled: not left", []string{"n3"}, 2, without, 1, false, false, true},
		{"another node marked: this one placed and unmarked", []string{"n2"}, 2, with, 1, true, false, false},
	} {
		willPlace, left, marked := placementOf("n3", all, c.leaving, c.replicas, c.cur, c.reconciled)
		if willPlace != c.willPlace || left != c.left || marked != c.marked {
			t.Errorf("%s: placementOf = %v, %v, %v; want %v, %v, %v", c.name, willPlace, left, marked, c.willPlace, c.left, c.marked)
		}
	}
}

// TestPlacementReadsTheMarkOnce: the hand-off's mark applies between two
// readings of the lease table, as it can on a follower just after its
// hand-off returns. Placement reads this node's mark once, so willPlace and
// marked agree: the node is not reported both marked and kept by the copy
// count, which a drain would count as placed.
func TestPlacementReadsTheMarkOnce(t *testing.T) {
	h := newHarness(t, []string{"n1", "n2", "n3"}, 4, 2)
	now := time.Now()
	n3 := h.nodes["n3"]
	for _, o := range h.order {
		n3.leases.nodes[o] = lease.NodeInfo{Alive: now}
	}
	reads := 0
	n3.leases.onLeaving = func(node string) {
		if node != "n3" {
			return
		}
		reads++
		if reads == 2 {
			n3.leases.mu.Lock()
			info := n3.leases.nodes["n3"]
			info.LeavingAt = 9
			n3.leases.nodes["n3"] = info
			n3.leases.mu.Unlock()
		}
	}
	willPlace, _, marked := n3.rep.Placement()
	if willPlace && marked {
		t.Fatal("Placement read n3 as kept in the basis and marked leaving: two readings of the mark")
	}
	if !willPlace || marked || reads != 1 {
		t.Fatalf("Placement = willPlace %v, marked %v after %d readings of n3's mark; want true, false, 1", willPlace, marked, reads)
	}
}

func TestSpanTargets(t *testing.T) {
	order := []string{"n1", "n2", "n3"}
	v1, v2 := unfedMaps(order)
	shardOf := func(ns, name string) int { return shardmap.ShardOf(ns, name, 2) }
	id := clientIn(2, 0)
	keys := []store.Scope{{Namespace: "default", ClientID: id}}
	targets, _ := barrierTargets(v2, shardOf, "n1", keys)
	upAll := func(string) bool { return true }
	if got := spanTargets(v1, v2, shardOf, "n1", keys, targets, upAll); !slices.Equal(got, []string{"n2"}) {
		t.Fatalf("spanTargets over a target move = %v, want [n2]", got)
	}
	if got := spanTargets(v1, v2, shardOf, "n1", keys, targets, func(p string) bool { return p != "n2" }); len(got) != 0 {
		t.Fatalf("spanTargets with the old target down = %v, want none", got)
	}
	if got := spanTargets(shardmap.Map{}, v2, shardOf, "n1", keys, targets, upAll); len(got) != 0 {
		t.Fatalf("spanTargets from no map = %v, want none", got)
	}
	if got := spanTargets(v2, v2, shardOf, "n1", keys, targets, upAll); len(got) != 0 {
		t.Fatalf("spanTargets over the same map = %v, want none", got)
	}
}

// movedFixture: n2 holds a copy of X, owned by n1 and written at epoch 1 under
// v1, where n2 is X's target; under v2 n3 is, and n2 still holds the shard.
// lose drops every replication frame n1 sends n3 from the map change on, so
// n1's whole write to n3 is never acknowledged.
func movedFixture(t *testing.T, lose bool, mutate func(*Options)) (h *harness, sinks map[string]*counters, id string) {
	t.Helper()
	h, sinks = withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, func(o *Options) {
		quickAck(o)
		if mutate != nil {
			mutate(o)
		}
	})
	v1, v2 := unfedMaps(h.order)
	h.applyMap(v1)
	waitSettled(h, 1)
	id = clientIn(2, 0)
	h.writeSession("n1", id, 1)
	feedSession(h.nodes["n1"], id, queued(1, "queued"))
	alive(h, "n1")
	if err := h.nodes["n1"].barrier(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !holdsCopy(h, "n2", id) {
		t.Fatal("precondition: n2 does not hold the copy under v1")
	}
	if lose {
		dropAll(h, "n1", "n3", 1)
	}
	h.applyMap(v2)
	waitSettled(h, 2)
	return h, sinks, id
}

func keptMovedCount(sinks map[string]*counters, node string) float64 {
	return sinks[node].get("hoardmq_replicas_kept_total", map[string]string{"reason": "moved-target"})
}

// TestMovedCopyIsKeptUntilTheOwnerConfirms: n1's whole write to n3 is lost,
// so n1 confirms nothing and n2 keeps the copy. A confirmation from another
// node, or below the lease's token, keeps it; the owner's at the token drops
// it.
func TestMovedCopyIsKeptUntilTheOwnerConfirms(t *testing.T) {
	h, sinks, id := movedFixture(t, true, nil)
	time.Sleep(300 * time.Millisecond) // past n1's own barrier, which fails at the ack timeout
	if !holdsCopy(h, "n2", id) || keptMovedCount(sinks, "n2") < 1 {
		t.Fatalf("after the map change: n2 holds %v, kept{moved-target} %v; want the copy kept", holdsCopy(h, "n2", id), keptMovedCount(sinks, "n2"))
	}
	n2 := h.nodes["n2"].rep
	n2.NoteWritten("default", id, "n3", 1)
	h.setLease("default", id, "n1", 2)
	n2.NoteWritten("default", id, "n1", 1)
	time.Sleep(200 * time.Millisecond)
	if !holdsCopy(h, "n2", id) {
		t.Fatal("a confirmation from a node that is not the owner, or below the lease's token, dropped the copy")
	}
	n2.NoteWritten("default", id, "n1", 2)
	h.waitFor("n2 to drop the copy on the owner's confirmation", 3*time.Second, func() bool { return !holdsCopy(h, "n2", id) })
	if got := dropped(sinks, "n2", "not-a-target-confirmed"); got != 1 {
		t.Fatalf("dropped{not-a-target-confirmed} = %v, want 1", got)
	}
	if n := n2.KeptMoved(); n != 0 {
		t.Fatalf("%d copies still recorded as kept for a moved target", n)
	}
}

// TestMovedCopyOutlivesTheBound: no confirmation ever comes, and the copy
// stays past the interval at which a copy from another grant is checked. An
// owner that died before it was declared would otherwise leave no copy at all.
func TestMovedCopyOutlivesTheBound(t *testing.T) {
	h, sinks, id := movedFixture(t, true, func(o *Options) { o.UnwrittenCheck = 300 * time.Millisecond })
	time.Sleep(time.Second)
	if !holdsCopy(h, "n2", id) {
		t.Fatalf("the copy went with no confirmation (dropped{not-a-target-held} %v)", dropped(sinks, "n2", "not-a-target-held"))
	}
	if n := h.nodes["n2"].rep.KeptMoved(); n != 1 {
		t.Fatalf("KeptMoved = %d, want 1", n)
	}
	if got := unwrittenChecks(sinks, "n2", "not-held"); got < 1 {
		t.Fatalf("checks{not-held} = %v, want at least 1: the copy was kept without asking the owner's targets", got)
	}
}

// TestBarrierWaitsWhereTheRecordsWentAcrossAMapChange: a record of X is on
// its way to n2, X's target under v1, when n1 applies v2 (n3 the target); n1's
// rebalance has not yet sent X whole to n3. A barrier over X must not pass
// before n2 holds the record: n3 holds nothing of X yet.
func TestBarrierWaitsWhereTheRecordsWentAcrossAMapChange(t *testing.T) {
	h, _ := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, quickAck)
	v1, v2 := unfedMaps(h.order)
	h.applyMap(v1)
	waitSettled(h, 1)
	id := clientIn(2, 0)
	alive(h, "n1")
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	h.nodes["n1"].rep.fanOutGate = func() { <-release }
	h.nodes["n1"].tr.SetFault("n2", &transport.Fault{Delay: 60 * time.Millisecond, Classes: []transport.Class{transport.ReplicationAppend}})
	h.writeSession("n1", id, 1)
	if _, err := h.nodes["n1"].table.Apply(0, shardmap.Encode(v2)); err != nil {
		t.Fatal(err)
	}
	err := h.nodes["n1"].rep.Barrier(context.Background(), []store.Scope{{Namespace: "default", ClientID: id}})
	n2Held := holdsCopy(h, "n2", id)
	unblock()
	if err != nil {
		t.Fatalf("barrier: %v", err)
	}
	if !n2Held {
		t.Fatal("the barrier passed before n2, where X's records went, held them")
	}
}

// TestDriverLeavesOutALeavingMember: a member the lease table marks leaving
// is left out of the next map, and back in once its mark clears.
func TestDriverLeavesOutALeavingMember(t *testing.T) {
	h := newHarnessWith(t, []string{"n1", "n2", "n3"}, 4, 2, func(o *Options) { o.Interval = 50 * time.Millisecond })
	now := time.Now()
	for _, n := range h.nodes {
		for _, o := range h.order {
			n.leases.nodes[o] = lease.NodeInfo{Alive: now}
		}
	}
	h.nodes["n1"].core.role = "leader"
	for _, n := range h.order {
		h.nodes[n].rep.Start()
	}
	basisIs := func(want ...string) func() bool {
		return func() bool {
			for _, n := range h.nodes {
				if !slices.Equal(n.table.Current().Basis, want) {
					return false
				}
			}
			return true
		}
	}
	h.waitFor("a map over every node", 5*time.Second, basisIs("n1", "n2", "n3"))
	setLeaving := func(node string, at uint64) {
		for _, n := range h.nodes {
			n.leases.mu.Lock()
			info := n.leases.nodes[node]
			info.LeavingAt = at
			n.leases.nodes[node] = info
			n.leases.mu.Unlock()
		}
	}
	setLeaving("n3", 9)
	h.waitFor("a map without the leaving node", 5*time.Second, basisIs("n1", "n2"))
	if willPlace, left, marked := h.nodes["n3"].rep.Placement(); willPlace || !left || !marked {
		t.Fatalf("n3's Placement = %v, %v, %v; want false, true, true", willPlace, left, marked)
	}
	setLeaving("n3", 0)
	h.waitFor("the node back in the map", 5*time.Second, basisIs("n1", "n2", "n3"))
}

// TestKeptMovedCountsOnlyCopiesStillHeld: a kept copy that another path
// deleted is not waited for, though its note lasts until its recheck.
func TestKeptMovedCountsOnlyCopiesStillHeld(t *testing.T) {
	h, _, id := movedFixture(t, true, nil)
	n2 := h.nodes["n2"].rep
	if n := n2.KeptMoved(); n != 1 {
		t.Fatalf("KeptMoved = %d with the copy kept, want 1", n)
	}
	if !n2.follower.dropCopy("default", id, "test") {
		t.Fatal("precondition: the copy was not dropped")
	}
	if n := n2.KeptMoved(); n != 0 {
		t.Fatalf("KeptMoved = %d after the copy went, want 0", n)
	}
}
