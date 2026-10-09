package replication

import (
	"context"
	"testing"
	"time"
)

// A previous owner's sealed copy ends once one of the new owner's targets
// holds the session at the owner's grant. HeldAtTargets is that question, on
// earlierGrantFixture: under v2 n4 owns X at grant 5 and replicates it to n3
// alone.

// heldFixture is earlierGrantFixture with n2's own checks of its kept copy
// held off, so only the calls a test makes ask anything.
func heldFixture(t *testing.T, lose bool) (*harness, string) {
	t.Helper()
	h, _, id := earlierGrantFixture(t, lose, func(o *Options) { o.UnwrittenCheck = time.Hour })
	return h, id
}

func heldAt(h *harness, node, owner, id string, token uint64, within time.Duration) uint64 {
	ctx, cancel := context.WithTimeout(context.Background(), within)
	defer cancel()
	return h.nodes[node].rep.HeldAtTargets(ctx, "default", id, owner, token)
}

// TestHeldAtTargetsAsksTheOwnersTargets: n3 holds X at 5, so asked from n2
// about n4 at 5 the answer is 5. A copy below the asked grant is not the
// owner's: asked at 6, the answer is zero.
func TestHeldAtTargetsAsksTheOwnersTargets(t *testing.T) {
	h, id := heldFixture(t, false)
	h.waitFor("n3 to hold X at n4's grant", 3*time.Second, func() bool {
		ss := h.nodes["n3"].rep.ReplicaState().Find("default", id)
		return ss != nil && ss.Epoch == 5
	})
	if got := heldAt(h, "n2", "n4", id, 5, 3*time.Second); got != 5 {
		t.Fatalf("asked about n4 at 5: %d, want 5 (n3 holds X at 5)", got)
	}
	if got := heldAt(h, "n2", "n4", id, 6, 3*time.Second); got != 0 {
		t.Fatalf("asked about n4 at 6: %d, want 0 (n3's copy is at 5)", got)
	}
}

// TestHeldAtTargetsFindsNothingWhereTheOwnersWriteWasLost: n4's write to n3 is
// lost, so no target holds X at 5.
func TestHeldAtTargetsFindsNothingWhereTheOwnersWriteWasLost(t *testing.T) {
	h, id := heldFixture(t, true)
	if got := heldAt(h, "n2", "n4", id, 5, 3*time.Second); got != 0 {
		t.Fatalf("asked about n4 at 5 with its write to n3 lost: %d, want 0", got)
	}
}

// TestHeldAtTargetsReadsThisNodesOwnCopy: n4's one target is n3, so asked on
// n3 no peer is asked and only n3's own replica copy can answer, for a grant
// it holds and not for a later one.
func TestHeldAtTargetsReadsThisNodesOwnCopy(t *testing.T) {
	h, id := heldFixture(t, false)
	h.waitFor("n3 to hold X at n4's grant", 3*time.Second, func() bool {
		ss := h.nodes["n3"].rep.ReplicaState().Find("default", id)
		return ss != nil && ss.Epoch == 5
	})
	if got := heldAt(h, "n3", "n4", id, 5, 3*time.Second); got != 5 {
		t.Fatalf("asked on n3, n4's one target, about n4 at 5: %d, want 5 from n3's own copy", got)
	}
	if got := heldAt(h, "n3", "n4", id, 6, 3*time.Second); got != 0 {
		t.Fatalf("asked on n3 about n4 at 6: %d, want 0 (n3's own copy is at 5)", got)
	}
}

// TestHeldAtTargetsWaitsForACheckSlot: with every one of n2's check slots
// taken, a call whose context ends first asks nobody and finds nothing,
// although n3 holds X at 5.
func TestHeldAtTargetsWaitsForACheckSlot(t *testing.T) {
	h, id := heldFixture(t, false)
	h.waitFor("n3 to hold X at n4's grant", 3*time.Second, func() bool {
		ss := h.nodes["n3"].rep.ReplicaState().Find("default", id)
		return ss != nil && ss.Epoch == 5
	})
	r := h.nodes["n2"].rep
	for i := 0; i < unwrittenChecksAtOnce; i++ {
		r.checkSlots <- struct{}{}
	}
	t.Cleanup(func() {
		for i := 0; i < unwrittenChecksAtOnce; i++ {
			<-r.checkSlots
		}
	})
	before := r.fetches.requests.Load()
	answer := make(chan uint64, 1)
	go func() { answer <- heldAt(h, "n2", "n4", id, 5, 300*time.Millisecond) }()
	var got uint64
	select {
	case got = <-answer:
	case <-time.After(3 * time.Second):
		t.Fatal("asked with every check slot taken, the call did not return when its context ended")
	}
	if got != 0 {
		t.Fatalf("asked with every check slot taken: %d, want 0", got)
	}
	if after := r.fetches.requests.Load(); after != before {
		t.Fatalf("%d fetches sent with every check slot taken, want none", after-before)
	}
}
