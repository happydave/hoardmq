package replication

import (
	"context"
	"testing"
	"time"
)

func TestDropKeptCopy(t *testing.T) {
	for _, c := range []struct {
		name            string
		confirmed, held bool
		want            unfedVerdict
	}{
		{"unconfirmed, no target seen holding it", false, false, keepUnwritten},
		{"unconfirmed, a target seen holding it", false, true, dropHeldAtTarget},
		{"confirmed", true, false, dropConfirmed},
		{"confirmed and seen held", true, true, dropConfirmed},
	} {
		if got := dropKeptCopy(c.confirmed, c.held); got != c.want {
			t.Errorf("%s: dropKeptCopy = %v, want %v", c.name, got, c.want)
		}
	}
}

// unwrittenFixture: n2 holds a copy of X written by n1 at epoch 1 under v1,
// where it is X's target. The lease is then n1's at a later grant, 5, which
// has written nothing n2 has seen: the copy is from another grant. applyV2
// moves the shard so that n2 still holds it and is not the owner's target.
func unwrittenFixture(t *testing.T, mutate func(*Options)) (h *harness, sinks map[string]*counters, id string, applyV2 func()) {
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
	h.setLease("default", id, "n1", 5)
	applyV2 = func() {
		h.applyMap(v2)
		waitSettled(h, 2)
	}
	return h, sinks, id, applyV2
}

func keptUnwritten(sinks map[string]*counters, node string) float64 {
	return sinks[node].get("hoardmq_replicas_kept_total", map[string]string{"reason": "owner-unwritten"})
}

// TestCopyFromAnotherGrantIsKeptUntilTheOwnerWrites is WI 1765 (c): the owner
// is alive but has not written the session at its grant, so the copy that
// nothing feeds may still be the one it needs. It stays until the owner
// confirms, and goes then.
func TestCopyFromAnotherGrantIsKeptUntilTheOwnerWrites(t *testing.T) {
	h, sinks, id, applyV2 := unwrittenFixture(t, nil)
	applyV2()
	if !holdsCopy(h, "n2", id) || keptUnwritten(sinks, "n2") < 1 {
		t.Fatalf("after the map change: holds %v, kept{owner-unwritten} %v; want the copy kept", holdsCopy(h, "n2", id), keptUnwritten(sinks, "n2"))
	}
	n2 := h.nodes["n2"].rep
	n2.NoteWritten("default", id, "n3", 5)
	n2.NoteWritten("default", id, "n1", 4)
	time.Sleep(200 * time.Millisecond)
	if !holdsCopy(h, "n2", id) {
		t.Fatal("a confirmation from a node that is not the owner, or below the lease's grant, dropped the copy")
	}
	n2.NoteWritten("default", id, "n1", 5)
	h.waitFor("n2 to drop the copy on the owner's confirmation", 3*time.Second, func() bool { return !holdsCopy(h, "n2", id) })
	if got := dropped(sinks, "n2", "not-a-target-confirmed"); got != 1 {
		t.Fatalf("dropped{not-a-target-confirmed} = %v, want 1", got)
	}
	if n := n2.unwritten.keptCount(); n != 0 {
		t.Fatalf("%d copies still recorded as kept", n)
	}
}

// TestConfirmationBeforeTheMapChangeIsNotLost: the owner confirmed before the
// map moved; the rebalance drops the copy at once.
func TestConfirmationBeforeTheMapChangeIsNotLost(t *testing.T) {
	h, sinks, id, applyV2 := unwrittenFixture(t, nil)
	h.nodes["n2"].rep.NoteWritten("default", id, "n1", 5)
	applyV2()
	h.waitFor("n2 to drop the copy at the rebalance", 3*time.Second, func() bool { return !holdsCopy(h, "n2", id) })
	if got, kept := dropped(sinks, "n2", "not-a-target-confirmed"), keptUnwritten(sinks, "n2"); got != 1 || kept != 0 {
		t.Fatalf("dropped{not-a-target-confirmed} %v, kept{owner-unwritten} %v; want 1, 0", got, kept)
	}
}

// TestUnfedHolders: as X's owner under v2, n1 replicates to n3 only; n2
// holds the shard and is the one to tell.
func TestUnfedHolders(t *testing.T) {
	h, _, id, applyV2 := unwrittenFixture(t, nil)
	applyV2()
	if got := h.nodes["n1"].rep.UnfedHolders("default", id); len(got) != 1 || got[0] != "n2" {
		t.Fatalf("n1's unfed holders %v, want [n2]", got)
	}
	if got := h.nodes["n3"].rep.UnfedHolders("default", id); len(got) != 0 {
		t.Fatalf("n3, a holder whose only other holder is its target, names %v", got)
	}
}
