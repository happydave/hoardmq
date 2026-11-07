package replication

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/store"
)

// soleHolderMaps: X is owned by n1 and replicated to n2 under v1. v2 leaves n2
// out while n1 is still alive, so n2 keeps its copy as a moved copy. v3 leaves
// n1 out too, after X's lease has moved to n3, which then holds the shard
// alone and has not fetched the session.
func soleHolderMaps(t *testing.T) (v1, v2, v3 shardmap.Map, id string) {
	t.Helper()
	v1 = shardmap.Compute(1, []string{"n1", "n2", "n3"}, 2, 2)
	v2 = shardmap.Compute(2, []string{"n1", "n3"}, 2, 2)
	v3 = shardmap.Compute(3, []string{"n3"}, 2, 2)
	id = clientTargeting(v1, 2, "n1", "n2", "x")
	shard := shardmap.ShardOf("default", id, 2)
	if v2.Holds(shard, "n2") || !slices.Equal(v2.Targets(shard, "n1"), []string{"n3"}) {
		t.Fatalf("precondition: under v2 n2 holds X's shard %v, or n1's targets are %v, not [n3]", v2.Holds(shard, "n2"), v2.Targets(shard, "n1"))
	}
	if !slices.Equal(v3.Holders(shard), []string{"n3"}) {
		t.Fatalf("precondition: under v3 X's shard is held by %v, not n3 alone", v3.Holders(shard))
	}
	return v1, v2, v3, id
}

// TestCopyWhoseOnlyOtherHolderIsAnAdopterIsKept: n2 holds the only copy of X
// that its new owner n3 can reach, n1's write to n3 having been lost. When
// the map leaves n2 out of X's shard with n3 its only holder, n3 has taken the
// lease and not the session, so nobody holds what n2 would let go. n2 keeps
// the copy, n3's fetch finds it, and it goes once n3 confirms the session at
// its grant. In the other cases n2's note of the moved copy is gone before v3,
// as after a restart, so only v3's own judging can end the keep; in the last,
// n3's confirmation arrived before v3 and the copy goes at v3.
func TestCopyWhoseOnlyOtherHolderIsAnAdopterIsKept(t *testing.T) {
	for _, c := range []struct {
		name                     string
		noteLost, confirmedFirst bool
	}{
		{"moved note held", false, false},
		{"moved note lost", true, false},
		{"confirmed before the map", true, true},
	} {
		t.Run(c.name, func(t *testing.T) { soleHolderKeep(t, c.noteLost, c.confirmedFirst) })
	}
}

func soleHolderKeep(t *testing.T, noteLost, confirmedFirst bool) {
	// No check runs within the test, so a confirmation is what ends the keep.
	h, sinks := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, func(o *Options) {
		quickAck(o)
		o.UnwrittenCheck = time.Minute
	})
	v1, v2, v3, id := soleHolderMaps(t)
	n1 := h.nodes["n1"]
	h.applyMap(v1)
	waitSettled(h, 1)
	h.writeSession("n1", id, 1)
	feedSession(n1, id, queued(1, "queued"))
	alive(h, "n1")
	alive(h, "n3")
	if err := n1.barrier(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !holdsCopy(h, "n2", id) {
		t.Fatal("precondition: n2 does not hold X under v1")
	}
	dropAll(h, "n1", "n3", 1)
	h.applyMap(v2)
	waitSettled(h, 2)
	if !holdsCopy(h, "n2", id) || holdsCopy(h, "n3", id) || keptMovedCount(sinks, "n2") < 1 {
		t.Fatalf("precondition: after v2 n2 holds %v, n3 holds %v, n2 kept{moved-target} %v; want true, false, at least 1",
			holdsCopy(h, "n2", id), holdsCopy(h, "n3", id), keptMovedCount(sinks, "n2"))
	}
	if noteLost {
		h.nodes["n2"].rep.unwritten.forget(store.Scope{Namespace: "default", ClientID: id})
	}

	// n1 is declared and its lease goes to n3, which owns X without the
	// session; then the map leaves n1 out.
	notAlive(h, "n1")
	h.setLease("default", id, "n3", 2)
	if confirmedFirst {
		h.nodes["n2"].rep.NoteWritten("default", id, "n3", 2)
	}
	h.applyMap(v3)
	waitSettled(h, 3)
	if confirmedFirst {
		h.waitFor("n2 to let go at v3 of a copy n3 had confirmed", 3*time.Second, func() bool { return !holdsCopy(h, "n2", id) })
		kept := sinks["n2"].get("hoardmq_replicas_kept_total", map[string]string{"reason": "owner-only-holder"})
		if got := dropped(sinks, "n2", "not-a-target-confirmed"); got != 1 || kept != 1 {
			t.Fatalf("on n2 dropped{not-a-target-confirmed} = %v, kept{owner-only-holder} = %v; want 1 and 1", got, kept)
		}
		return
	}
	if !holdsCopy(h, "n2", id) {
		t.Fatalf("n2 let go of X's copy at v3 with n3, which owns the lease and holds no session, the shard's only holder (dropped{not-a-target-confirmed} %v)",
			dropped(sinks, "n2", "not-a-target-confirmed"))
	}
	if got := sinks["n2"].get("hoardmq_replicas_kept_total", map[string]string{"reason": "owner-only-holder"}); got != 1 {
		t.Fatalf("kept{owner-only-holder} on n2 = %v, want 1", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ss, answered, _ := h.nodes["n3"].rep.Fetch(ctx, "default", id, "n1")
	if ss == nil || len(ss.Entries) != 1 {
		t.Fatalf("n3's fetch of X found %+v (answered %v), want n2's copy with n1's one queued entry", ss, answered)
	}

	h.nodes["n2"].rep.NoteWritten("default", id, "n3", 1)
	time.Sleep(300 * time.Millisecond)
	if !holdsCopy(h, "n2", id) {
		t.Fatal("n2 let go of X's copy on a confirmation below n3's grant")
	}
	h.nodes["n2"].rep.NoteWritten("default", id, "n3", 2)
	h.waitFor("n2 to let go of X's copy on n3's confirmation at its grant", 3*time.Second, func() bool { return !holdsCopy(h, "n2", id) })
	if got := dropped(sinks, "n2", "not-a-target-confirmed"); got != 1 {
		t.Fatalf("dropped{not-a-target-confirmed} on n2 = %v, want 1", got)
	}
}

func TestOwnerMayLackCopy(t *testing.T) {
	for _, c := range []struct {
		name             string
		sent, known      bool
		owner            string
		copyEpoch, token uint64
		want             bool
	}{
		{"an earlier grant's copy sent nowhere", false, true, "n3", 1, 2, true},
		{"sent to a holder", true, true, "n3", 1, 2, false},
		{"at the owner's grant", false, true, "n3", 2, 2, false},
		{"from a later grant than the lease", false, true, "n3", 3, 2, false},
		{"lease unknown", false, false, "n3", 1, 2, false},
		{"owned by this node", false, true, "n2", 1, 2, false},
	} {
		if got := ownerMayLackCopy(c.sent, c.known, c.owner, "n2", c.copyEpoch, c.token); got != c.want {
			t.Errorf("%s: ownerMayLackCopy = %v, want %v", c.name, got, c.want)
		}
	}
}

// TestHandOverWithNoTransportReportsSent: a node with no peers has nobody to
// keep a copy for, so its hand-over reads as sent and its drop is decided as
// before.
func TestHandOverWithNoTransportReportsSent(t *testing.T) {
	r := &Replicator{}
	if acked, sent := r.handOver(&store.SessionState{}, []string{"n2"}); !acked || !sent {
		t.Fatalf("handOver with no transport = (%v, %v), want (true, true)", acked, sent)
	}
}
