package replication

import (
	"context"
	"testing"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/store"
)

// TestTapStatsCountTheDrop: an owner's upsert that finds a replica copy is
// counted as a drop with the time it took; one that finds none is a check
// only. The drop runs after the store's lock is released, so its seconds are
// counted beside the lock's hold and not inside it.
func TestTapStatsCountTheDrop(t *testing.T) {
	h, _ := withCounters(t, []string{"n1", "n2"}, 2, 2)
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
	before, dropBefore, replicaBefore := n2.rep.TapStats(), n2.rep.DropStats(), n2.rep.ReplicaAppendStats()
	h.setLease("default", id, "n2", 2)
	for range 2 {
		if err := n2.own.SessionUpsert(store.SessionMeta{Namespace: "default", ClientID: id, Epoch: 2}); err != nil {
			t.Fatal(err)
		}
	}
	st, ds := n2.rep.TapStats(), n2.rep.DropStats()
	if st.Calls-before.Calls != 2 {
		t.Fatalf("tap stats %+v after %+v, want 2 session records", st, before)
	}
	if ds.Checks-dropBefore.Checks != 2 || ds.Dropped-dropBefore.Dropped != 1 || ds.Kept != 0 || ds.Sec <= dropBefore.Sec {
		t.Fatalf("drop stats %+v after %+v, want 2 checks, 1 dropped, none kept and some time", ds, dropBefore)
	}
	if got := n2.rep.ReplicaAppendStats().Appends - replicaBefore.Appends; got != 1 {
		t.Fatalf("the replica store took %d appends for one dropped copy, want 1", got)
	}
	if own := n2.own.AppendStats(); own.TapSec <= 0 || own.HeldSec < own.TapSec {
		t.Fatalf("the owner store's appends %+v, want tap time inside the held time", own)
	}
}
