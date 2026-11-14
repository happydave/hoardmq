package replication

import (
	"context"
	"testing"

	"github.com/happydave/hoardmq/internal/cluster/lease"
	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/store"
)

func TestOwnedCopyAction(t *testing.T) {
	cases := []struct {
		name                     string
		held                     bool
		copyEpoch, wroteAt, seen uint64
		want                     ownedCopy
	}{
		{"no copy", false, 0, 7, 9, ownedCopyNone},
		{"the previous owner's copy, nothing later seen", true, 5, 7, 0, ownedCopyDrop},
		{"the previous owner's copy, seen equal to the write", true, 5, 7, 7, ownedCopyDrop},
		{"the previous owner's copy, a later grant seen", true, 5, 7, 8, ownedCopyKeepSuperseded},
		{"a later owner's copy, nothing later seen", true, 7, 5, 0, ownedCopyKeepLater},
		{"a later owner's copy at the seen grant", true, 8, 5, 8, ownedCopyKeepSuperseded},
		{"an unstamped write still clears what came before it", true, 5, 0, 9, ownedCopyDrop},
	}
	for _, c := range cases {
		if got := ownedCopyAction(c.held, c.copyEpoch, c.wroteAt, c.seen); got != c.want {
			t.Errorf("%s: ownedCopyAction(%v, %d, %d, %d) = %d, want %d", c.name, c.held, c.copyEpoch, c.wroteAt, c.seen, got, c.want)
		}
	}
}

// TestOwnedWriteBelowASeenGrantKeepsTheCopy: n2 holds the previous owner's
// copy of X at epoch 1 and writes X as owner at epoch 2, its lease table
// behind a grant it has seen. With grant 5 seen, the write is into a
// superseded grant and the copy stays for the node that holds the lease now;
// with none seen (the control) the write drops it, as R8 has an owner do.
func TestOwnedWriteBelowASeenGrantKeepsTheCopy(t *testing.T) {
	for _, c := range []struct {
		name     string
		seen     uint64
		wantKept bool
	}{
		{"a later grant seen", 5, true},
		{"none seen", 0, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			tokens := map[string]*lease.Tokens{}
			h, sinks := withCountersAnd(t, []string{"n1", "n2"}, 2, 2, func(o *Options) {
				tokens[o.Node] = lease.NewTokens()
				o.Tokens = tokens[o.Node]
			})
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
			tokens["n2"].Note("default", id, c.seen)
			if err := n2.own.SessionUpsert(store.SessionMeta{Namespace: "default", ClientID: id, Epoch: 2}); err != nil {
				t.Fatal(err)
			}
			ss := n2.rep.ReplicaState().Find("default", id)
			kept := sinks["n2"].get("hoardmq_replicas_kept_total", map[string]string{"reason": "owned-superseded-write"})
			t.Logf("seen %d: copy held %v, kept{owned-superseded-write} %v, dropped{owned-upsert} %v", c.seen, ss != nil, kept, dropped(sinks, "n2", "owned-upsert"))
			if (ss != nil) != c.wantKept {
				t.Fatalf("n2 holds the copy %v after its write at 2 with %d seen, want %v", ss != nil, c.seen, c.wantKept)
			}
			if c.wantKept {
				if ss.Epoch != 1 || kept != 1 || dropped(sinks, "n2", "owned-upsert") != 0 {
					t.Fatalf("kept copy at %d, kept{owned-superseded-write} %v, dropped{owned-upsert} %v; want 1, 1, 0", ss.Epoch, kept, dropped(sinks, "n2", "owned-upsert"))
				}
				if ds := n2.rep.DropStats(); ds.Kept != 1 || ds.Dropped != 0 {
					t.Fatalf("drop stats %+v, want one kept and none dropped", ds)
				}
			} else if kept != 0 || dropped(sinks, "n2", "owned-upsert") != 1 {
				t.Fatalf("control: kept{owned-superseded-write} %v, dropped{owned-upsert} %v; want 0, 1", kept, dropped(sinks, "n2", "owned-upsert"))
			}
		})
	}
}
