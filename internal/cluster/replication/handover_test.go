package replication

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/lease"
	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

func TestShouldDropCopy(t *testing.T) {
	for _, c := range []struct {
		past, alive, acked bool
		want               dropVerdict
	}{
		{true, true, true, dropCopy},
		{false, true, true, keepMapBehind},
		{true, false, true, keepOwnerNotAlive},
		{true, true, false, keepHandoverUnacknowledged},
		{false, false, false, keepMapBehind},
	} {
		if got := shouldDropCopy(c.past, c.alive, c.acked); got != c.want {
			t.Errorf("shouldDropCopy(%v, %v, %v) = %v, want %v", c.past, c.alive, c.acked, got, c.want)
		}
	}
}

// handOverMaps: under v1 shard 0 is led by n1 with n2 following; under v2 n3
// follows instead, so n2 must hand its copy to n3 and let go.
func handOverMaps(order []string) (v1, v2 shardmap.Map) {
	v1 = shardmap.Compute(1, order, 2, 2)
	v2 = shardmap.Map{Version: 2, Basis: order, Replicas: 2, Shards: []shardmap.Shard{{Leader: "n1", Followers: []string{"n3"}}, {Leader: "n2", Followers: []string{"n3"}}}}
	return v1, v2
}

// dropAll makes every replication frame from a node to a peer vanish on the
// wire, link up.
func dropAll(h *harness, from, to string, seed int64) {
	h.nodes[from].tr.SetFault(to, &transport.Fault{Drop: 1, Classes: []transport.Class{transport.ReplicationAppend}, Rand: rand.New(rand.NewSource(seed))}) //nolint:gosec // seeded
}

func kept(sinks map[string]*counters, node string) float64 {
	return sinks[node].get("hoardmq_replicas_kept_total", map[string]string{"reason": "handover-unacknowledged"})
}

// sessionHandOver runs the map change with the given faults and reports
// whether n2 still holds the copy, whether n3 holds it, and n2's kept count.
// With serving, n1's session manager holds the session, so n1 sends it whole
// to n3 at the map change and confirms it to n2.
func sessionHandOver(t *testing.T, h *harness, sinks map[string]*counters, serving bool, fault func()) (n2Holds, n3Holds bool, n2Kept float64) {
	t.Helper()
	v1, v2 := handOverMaps(h.order)
	h.applyMap(v1)
	waitSettled(h, 1)
	id := clientIn(2, 0)
	h.writeSession("n1", id, 1)
	if serving {
		feedSession(h.nodes["n1"], id, queued(1, "queued"))
	}
	if err := h.nodes["n1"].barrier(context.Background()); err != nil {
		t.Fatal(err)
	}
	if h.nodes["n2"].rep.ReplicaState().Find("default", id) == nil {
		t.Fatal("n2 does not hold the copy under v1")
	}
	// The owner is alive by every node's lease table: a copy whose owner is
	// gone is kept for that reason, and this test is about the hand-over.
	for _, n := range h.nodes {
		n.leases.mu.Lock()
		n.leases.nodes["n1"] = lease.NodeInfo{Alive: time.Now()}
		n.leases.mu.Unlock()
	}
	fault()
	h.applyMap(v2)
	waitSettled(h, 2)
	// n3 applies what it was sent on its own worker; give it a moment.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && h.nodes["n3"].rep.ReplicaState().Find("default", id) == nil {
		time.Sleep(10 * time.Millisecond)
	}
	return h.nodes["n2"].rep.ReplicaState().Find("default", id) != nil,
		h.nodes["n3"].rep.ReplicaState().Find("default", id) != nil,
		kept(sinks, "n2")
}

func quickAck(o *Options) { o.AckTimeout = 300 * time.Millisecond }

// TestHandOverAcknowledgedThenDropped: the sound path — n3 acknowledges the
// owner's whole write, the owner confirms, and n2 lets go without handing its
// copy over.
func TestHandOverAcknowledgedThenDropped(t *testing.T) {
	h, sinks := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, quickAck)
	_, n3Holds, n2Kept := sessionHandOver(t, h, sinks, true, func() {})
	id := clientIn(2, 0)
	h.waitFor("n2 to drop its copy on the owner's confirmation", 3*time.Second, func() bool { return !holdsCopy(h, "n2", id) })
	n2Holds := holdsCopy(h, "n2", id)
	if n2Holds || !n3Holds || n2Kept != 0 {
		t.Fatalf("after the owner confirmed: n2 holds %v, n3 holds %v, kept %v; want false, true, 0", n2Holds, n3Holds, n2Kept)
	}
	if got := dropped(sinks, "n2", "not-a-target-confirmed"); got != 1 {
		t.Fatalf("dropped{not-a-target-confirmed} = %v, want 1", got)
	}
}

// TestHandOverUnacknowledgedKeepsTheCopy: every frame to n3 is lost on the
// wire, so the owner's whole write is never acknowledged and nothing is
// confirmed; n2 keeps its copy and counts it.
func TestHandOverUnacknowledgedKeepsTheCopy(t *testing.T) {
	h, sinks := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, quickAck)
	n2Holds, n3Holds, _ := sessionHandOver(t, h, sinks, true, func() { dropAll(h, "n1", "n3", 1); dropAll(h, "n2", "n3", 2) })
	moved := sinks["n2"].get("hoardmq_replicas_kept_total", map[string]string{"reason": "moved-target"})
	if !n2Holds || n3Holds || moved < 1 {
		t.Fatalf("after a lost whole write: n2 holds %v, n3 holds %v, kept{moved-target} %v; want true, false, at least 1", n2Holds, n3Holds, moved)
	}
}

// TestDefectDeleteWithoutHandoverAck is the control: with the seam on, n2
// deletes its copy under the same loss, and the owner is the last to hold
// the session.
func TestDefectDeleteWithoutHandoverAck(t *testing.T) {
	h, sinks := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, func(o *Options) { quickAck(o); o.DeleteWithoutHandoverAck = true })
	n2Holds, n3Holds, _ := sessionHandOver(t, h, sinks, true, func() { dropAll(h, "n1", "n3", 1); dropAll(h, "n2", "n3", 2) })
	if n2Holds || n3Holds {
		t.Fatalf("defective build: n2 holds %v, n3 holds %v; want both false — the copy is gone but for the owner", n2Holds, n3Holds)
	}
}

// retainedHandOver: under v1 n1 leads shard 0; under v3 n3 does, having held
// nothing of it, so n1 hands its retained messages over and lets go of its
// local copies only once n3 acknowledged them.
func retainedHandOver(t *testing.T, h *harness, sinks map[string]*counters, fault func()) (n1Holds, n3Holds bool, n1Kept float64) {
	t.Helper()
	allAlive(h)
	v1 := shardmap.Compute(1, h.order, 2, 2)
	h.applyMap(v1)
	waitSettled(h, 1)
	topic := topicIn(2, 0) // led by n1
	m := &message.Message{Namespace: "default", Topic: topic, Payload: []byte("kept"), Retain: true}
	if !h.nodes["n1"].rep.StoreRetained(m) {
		t.Fatal("n1 should lead the shard")
	}
	h.nodes["n1"].sessions.InjectRetained(m)
	if err := h.nodes["n1"].barrier(context.Background()); err != nil {
		t.Fatal(err)
	}
	fault()
	v3 := shardmap.Map{Version: 3, Basis: h.order, Replicas: 2, Shards: []shardmap.Shard{{Leader: "n3", Followers: []string{"n2"}}, {Leader: "n2", Followers: []string{"n3"}}}}
	h.applyMap(v3)
	waitSettled(h, 3)
	_, n1Has := h.nodes["n1"].sessions.has("default", topic)
	_, n3Has := h.nodes["n3"].sessions.has("default", topic)
	return n1Has, n3Has, kept(sinks, "n1")
}

func TestRetainedHandOverAcknowledgedThenDropped(t *testing.T) {
	h, sinks := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, quickAck)
	_, n3Holds, n1Kept := retainedHandOver(t, h, sinks, func() {})
	// n1 keeps its own copy until n3, the new leader, releases it.
	h.waitFor("n3 to release n1's copy", 3*time.Second, func() bool {
		_, has := h.nodes["n1"].sessions.has("default", topicIn(2, 0))
		return !has
	})
	_, n1Holds := h.nodes["n1"].sessions.has("default", topicIn(2, 0))
	if n1Holds || !n3Holds || n1Kept != 0 {
		t.Fatalf("after an acknowledged retained hand-over: n1 holds %v, n3 holds %v, kept %v; want false, true, 0", n1Holds, n3Holds, n1Kept)
	}
}

func TestRetainedHandOverUnacknowledgedKeepsTheCopy(t *testing.T) {
	h, sinks := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, quickAck)
	// Both holders' hand-overs to n3 are lost: the old leader's and, in
	// its case 3, the old follower's.
	n1Holds, n3Holds, n1Kept := retainedHandOver(t, h, sinks, func() { dropAll(h, "n1", "n3", 3); dropAll(h, "n2", "n3", 4) })
	if !n1Holds || n3Holds || n1Kept != 1 {
		t.Fatalf("after a lost retained hand-over: n1 holds %v, n3 holds %v, kept %v; want true, false, 1", n1Holds, n3Holds, n1Kept)
	}
}

// TestFetchAsksAFormerHolderThatKeptItsCopy: when the shard's holders under
// the current map have nothing, the fetch asks the other live peers — the
// node that kept a copy because its hand-over went unacknowledged.
func TestFetchAsksAFormerHolderThatKeptItsCopy(t *testing.T) {
	h, sinks := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, quickAck)
	n2Holds, n3Holds, _ := sessionHandOver(t, h, sinks, true, func() { dropAll(h, "n1", "n3", 1); dropAll(h, "n2", "n3", 2) })
	if !n2Holds || n3Holds {
		t.Fatalf("precondition: n2 holds %v, n3 holds %v; want true, false", n2Holds, n3Holds)
	}
	id := clientIn(2, 0)
	// The takeover that fetches comes later, with the links whole again:
	// a fetch request lost on the wire would spend the fetch's one timeout
	// on the holder and reach nobody else.
	h.nodes["n1"].tr.SetFault("n3", nil)
	h.nodes["n2"].tr.SetFault("n3", nil)
	// n3, the holder under the map, is asked first and has nothing; n2,
	// no longer a holder, kept the copy.
	got, _, _ := h.nodes["n1"].rep.Fetch(context.Background(), "default", id, "")
	if got == nil || len(got.Entries) != 1 {
		t.Fatalf("the fetch found %v; want the copy n2 kept", got)
	}
	if n := sinks["n1"].get("hoardmq_replication_fetches_total", map[string]string{"outcome": "kept-copy"}); n != 1 {
		outcomes := map[string]float64{}
		for _, o := range []string{"local", "remote", "kept-copy", "none"} {
			outcomes[o] = sinks["n1"].get("hoardmq_replication_fetches_total", map[string]string{"outcome": o})
		}
		t.Fatalf("kept-copy fetches counted %v, want 1 (outcomes %v; n3 holds %v)", n, outcomes, h.nodes["n3"].rep.ReplicaState().Find("default", id) != nil)
	}
}

// TestFetchFindsItsOwnKeptCopy: the node that kept a copy is as likely as
// any peer to be the one fetching — the leader adopting the dead owner's
// lease — and under a map that does not list it among the sources its own
// store is consulted before the session is given up for lost.
func TestFetchFindsItsOwnKeptCopy(t *testing.T) {
	h, sinks := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, quickAck)
	n2Holds, n3Holds, _ := sessionHandOver(t, h, sinks, true, func() { dropAll(h, "n1", "n3", 1); dropAll(h, "n2", "n3", 2) })
	if !n2Holds || n3Holds {
		t.Fatalf("precondition: n2 holds %v, n3 holds %v; want true, false", n2Holds, n3Holds)
	}
	id := clientIn(2, 0)
	h.nodes["n1"].tr.SetFault("n3", nil)
	h.nodes["n2"].tr.SetFault("n3", nil)
	n2 := h.nodes["n2"].rep
	shard := n2.Map().ShardOf("default", id)
	if sources := n2.Map().Current().Targets(shard, "n1"); contains(sources, "n2") {
		t.Fatalf("precondition: n2 is among the sources %v", sources)
	}
	got, _, _ := n2.Fetch(context.Background(), "default", id, "n1")
	if got == nil || len(got.Entries) != 1 {
		t.Fatalf("n2's fetch found %v; want the copy it kept", got)
	}
	if n := sinks["n2"].get("hoardmq_replication_fetches_total", map[string]string{"outcome": "kept-copy"}); n != 1 {
		t.Fatalf("kept-copy fetches counted %v on n2, want 1", n)
	}
}

func dropped(sinks map[string]*counters, node, reason string) float64 {
	return sinks[node].get("hoardmq_replicas_dropped_total", map[string]string{"reason": reason})
}

// TestOwnerDropsItsReplicaCopyAtItsOwnUpsert: n2 holds a copy of X from its
// owner n1; the lease moves to n2 and n2 writes X as its owner. The copy in
// n2's replica store is from the previous owner and goes at that write.
func TestOwnerDropsItsReplicaCopyAtItsOwnUpsert(t *testing.T) {
	h, sinks := withCounters(t, []string{"n1", "n2"}, 2, 2)
	h.applyMap(shardmap.Compute(1, h.order, 2, 2))
	waitSettled(h, 1)
	id := clientIn(2, 0) // leader n1, follower n2
	h.writeSession("n1", id, 1)
	n2 := h.nodes["n2"]
	if err := h.nodes["n1"].barrier(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n2.rep.ReplicaState().Find("default", id) == nil {
		t.Fatal("precondition: n2 does not hold the copy")
	}
	h.setLease("default", id, "n2", 2)
	if err := n2.own.SessionUpsert(store.SessionMeta{Namespace: "default", ClientID: id, Epoch: 2}); err != nil {
		t.Fatal(err)
	}
	if n2.rep.ReplicaState().Find("default", id) != nil {
		t.Fatal("n2 still holds a replica copy of the session it now owns")
	}
	if got := dropped(sinks, "n2", "owned-upsert"); got != 1 {
		t.Fatalf("dropped{owned-upsert} = %v, want 1", got)
	}
}

// TestOwnedCopyIsKeptUntilTheSessionIsServedHere: X's lease has been
// reassigned to n2, which holds X's replica copy and does not yet hold X —
// the reassignment applied, the adoption has not run. The map moves. n2's
// copy is the only one there is: it is kept, and it is not pushed to n3,
// which would take a previous owner's state for the owner's own whole write.
// When n2 then writes the session, as the adoption's import does, the copy
// goes.
func TestOwnedCopyIsKeptUntilTheSessionIsServedHere(t *testing.T) {
	h, sinks := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, quickAck)
	v1, v2 := handOverMaps(h.order)
	h.applyMap(v1)
	waitSettled(h, 1)
	id := clientIn(2, 0)
	h.writeSession("n1", id, 1)
	if err := h.nodes["n1"].barrier(context.Background()); err != nil {
		t.Fatal(err)
	}
	if h.nodes["n2"].rep.ReplicaState().Find("default", id) == nil {
		t.Fatal("precondition: n2 does not hold the copy under v1")
	}
	for _, n := range h.nodes {
		n.leases.mu.Lock()
		n.leases.nodes["n2"] = lease.NodeInfo{Alive: time.Now()}
		n.leases.mu.Unlock()
	}
	h.setLease("default", id, "n2", 2)
	h.applyMap(v2)
	waitSettled(h, 2)
	time.Sleep(200 * time.Millisecond)
	if h.nodes["n2"].rep.ReplicaState().Find("default", id) == nil {
		t.Fatal("n2 dropped the only copy of a session it owns the lease for and does not yet serve")
	}
	if h.nodes["n3"].rep.ReplicaState().Find("default", id) != nil {
		t.Fatal("n3 holds the previous owner's copy as if n2 had handed it over")
	}
	if got := sinks["n2"].get("hoardmq_replicas_kept_total", map[string]string{"reason": "owned-unserved"}); got != 1 {
		t.Fatalf("kept{owned-unserved} = %v, want 1", got)
	}

	h.writeSession("n2", id, 2)
	h.waitFor("n2 to drop the copy once it serves the session", 5*time.Second, func() bool {
		return h.nodes["n2"].rep.ReplicaState().Find("default", id) == nil
	})
	if got := dropped(sinks, "n2", "owned-upsert"); got != 1 {
		t.Fatalf("dropped{owned-upsert} = %v, want 1", got)
	}
}

// TestDropStaleOwnedCopy covers the rule on its own: a copy survives this
// node's own write only when it is provably a later owner's.
func TestDropStaleOwnedCopy(t *testing.T) {
	cases := []struct {
		name                   string
		held                   bool
		copyEpoch, recordEpoch uint64
		want                   bool
	}{
		{"no copy", false, 0, 7, false},
		{"the previous owner's copy", true, 5, 7, true},
		{"a copy at this node's own epoch", true, 7, 7, true},
		{"a later owner's copy", true, 7, 5, false},
		{"an unstamped write still clears what came before it", true, 5, 0, true},
	}
	for _, c := range cases {
		if got := dropStaleOwnedCopy(c.held, c.copyEpoch, c.recordEpoch); got != c.want {
			t.Errorf("%s: dropStaleOwnedCopy(%v, %d, %d) = %v, want %v", c.name, c.held, c.copyEpoch, c.recordEpoch, got, c.want)
		}
	}
}

// TestOwnerDropsItsReplicaCopyAtItsOwnDelete: as at its upsert, n2 deleting
// X as its owner, which a Clean Start with nothing held here writes, drops
// the previous owner's copy from n2's replica store; a later owner's copy
// stays.
func TestOwnerDropsItsReplicaCopyAtItsOwnDelete(t *testing.T) {
	for _, c := range []struct {
		name       string
		copyEpoch  uint64
		wantKept   bool
		wantReason string
	}{
		{"previous owner's copy", 1, false, "owned-delete"},
		{"later owner's copy", 5, true, ""},
	} {
		h, sinks := withCounters(t, []string{"n1", "n2"}, 2, 2)
		h.applyMap(shardmap.Compute(1, h.order, 2, 2))
		waitSettled(h, 1)
		id := clientIn(2, 0) // leader n1, follower n2
		h.writeSession("n1", id, c.copyEpoch)
		n2 := h.nodes["n2"]
		if err := h.nodes["n1"].barrier(context.Background()); err != nil {
			t.Fatal(err)
		}
		if n2.rep.ReplicaState().Find("default", id) == nil {
			t.Fatalf("%s: precondition: n2 does not hold the copy", c.name)
		}
		if err := n2.own.SessionDelete(store.Key{Namespace: "default", ClientID: id, Epoch: 2}); err != nil {
			t.Fatal(err)
		}
		if kept := n2.rep.ReplicaState().Find("default", id) != nil; kept != c.wantKept {
			t.Fatalf("%s: n2 kept the copy %v, want %v", c.name, kept, c.wantKept)
		}
		if c.wantReason != "" {
			if got := dropped(sinks, "n2", c.wantReason); got != 1 {
				t.Fatalf("%s: dropped{%s} = %v, want 1", c.name, c.wantReason, got)
			}
		}
	}
}

// TestOwnedCopyAtALaterEpochIsKept: the lease moved to n1, which replicated
// the session back to n2 at its own higher epoch; n2 has not noticed and
// writes the session at the epoch it last held. The drop runs after the
// store's lock is released, which is long enough for exactly this, so the
// copy it finds is not always the one its write made stale.
func TestOwnedCopyAtALaterEpochIsKept(t *testing.T) {
	h, sinks := withCounters(t, []string{"n1", "n2"}, 2, 2)
	h.applyMap(shardmap.Compute(1, h.order, 2, 2))
	waitSettled(h, 1)
	id := clientIn(2, 0) // leader n1, follower n2
	h.writeSession("n1", id, 5)
	n2 := h.nodes["n2"]
	if err := h.nodes["n1"].barrier(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n2.rep.ReplicaState().Find("default", id) == nil {
		t.Fatal("precondition: n2 does not hold the copy")
	}
	if err := n2.own.SessionUpsert(store.SessionMeta{Namespace: "default", ClientID: id, Epoch: 2}); err != nil {
		t.Fatal(err)
	}
	ss := n2.rep.ReplicaState().Find("default", id)
	if ss == nil {
		t.Fatal("n2 dropped a replica copy written at a later epoch than its own record")
	}
	if ss.Epoch != 5 {
		t.Fatalf("the kept copy is at epoch %d, want the later owner's 5", ss.Epoch)
	}
	if got := sinks["n2"].get("hoardmq_replicas_kept_total", map[string]string{"reason": "owned-later-epoch"}); got != 1 {
		t.Fatalf("kept{owned-later-epoch} = %v, want 1", got)
	}
	if got := dropped(sinks, "n2", "owned-upsert"); got != 0 {
		t.Fatalf("dropped{owned-upsert} = %v, want 0", got)
	}
	if ds := n2.rep.DropStats(); ds.Kept != 1 || ds.Dropped != 0 {
		t.Fatalf("drop stats %+v, want one kept and none dropped", ds)
	}
}

func TestDropOwnedCopy(t *testing.T) {
	if dropOwnedCopy(false) {
		t.Error("a copy of a session this node owns the lease for and does not serve would be dropped")
	}
	if !dropOwnedCopy(true) {
		t.Error("a copy of a session this node serves would be kept")
	}
}

// TestHandOverOfAnOlderCopyIsRefusedAndKept: the lease moved on to epoch 2
// after n2's copy at epoch 1 was made, and its owner is not alive, so n2
// hands the copy over. The hand-over carries the copy's epoch, n3 refuses it,
// and n2 keeps it.
func TestHandOverOfAnOlderCopyIsRefusedAndKept(t *testing.T) {
	h, sinks := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, quickAck)
	n2Holds, n3Holds, _ := sessionHandOver(t, h, sinks, false, func() {
		h.setLease("default", clientIn(2, 0), "n1", 2)
		notAlive(h, "n1")
	})
	if !n2Holds || n3Holds {
		t.Fatalf("after handing over an older copy: n2 holds %v, n3 holds %v; want true, false", n2Holds, n3Holds)
	}
}

// allAlive gives every node an alive record in every node's lease table: the
// retained moves wait on, and release, only nodes alive by the log.
func allAlive(h *harness) {
	for _, n := range h.nodes {
		n.leases.mu.Lock()
		for _, o := range h.order {
			n.leases.nodes[o] = lease.NodeInfo{Alive: time.Now()}
		}
		n.leases.mu.Unlock()
	}
}
