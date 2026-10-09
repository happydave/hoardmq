package lease

import (
	"context"
	"testing"
	"time"
)

// racedHoldCluster is three nodes with x acquired on n1 at expiry, n2's
// table showing it, and n2's next acquire of x held after its lookup.
func racedHoldCluster(t *testing.T, expiry uint32) (n1, n2 *tnode, hold *acquireHold, first uint64) {
	t.Helper()
	hold = newAcquireHold(false)
	fixtureFallback = foundNowhere
	t.Cleanup(func() { fixtureFallback = nil })
	fixtureOptions = func(o *Options) {
		heldBack(o)
		if o.Node == "n2" {
			o.AcquireGate = hold.gate
		}
	}
	t.Cleanup(func() { fixtureOptions = nil })
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	t.Cleanup(hold.unblock)
	n1, n2 = c.nodes["n1"], c.nodes["n2"]
	res, err := acquire(t, n1, "x", nil, expiry, false)
	if err != nil {
		t.Fatal(err)
	}
	ownedBy(t, n2, "x", "n1")
	return n1, n2, hold, res.Epoch
}

func acquireAsyncAt(n *tnode, id string, expiry uint32) chan acquireOutcome {
	done := make(chan acquireOutcome, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		r, err := n.svc.Acquire(ctx, "default", id, nil, expiry, false)
		done <- acquireOutcome{r, err}
	}()
	return done
}

func outcomeOf(t *testing.T, done chan acquireOutcome) acquireOutcome {
	t.Helper()
	select {
	case o := <-done:
		return o
	case <-time.After(10 * time.Second):
		t.Fatal("acquire did not return")
	}
	return acquireOutcome{}
}

// TestRacedPathReleasedSessionWithoutExpiry: x's session on n1 had no expiry
// and ended (n1 releases the lease) after n2's CONNECT looked the lease up and
// before its acquire applied. The acquire creates the lease afresh, and the
// empty serve is counted as a session with no expiry, not as raced.
func TestRacedPathReleasedSessionWithoutExpiry(t *testing.T) {
	n1, n2, hold, first := racedHoldCluster(t, 0)
	hold.armed.Store(true)
	done := acquireAsyncAt(n2, "x", 0)
	hold.wait(t, "n2's acquire looked x up and is held")
	n1.svc.Release("default", "x", first)
	waitFor(t, "n2's table to drop x", 5*time.Second, func() bool {
		_, ok := n2.svc.table.Lookup("default", "x")
		return !ok
	})
	hold.unblock()
	o := outcomeOf(t, done)
	if o.res.PrevOwner != "" || o.res.Epoch <= first {
		t.Fatalf("precondition not reached: prev %q, token %d (first %d); want the lease created afresh", o.res.PrevOwner, o.res.Epoch, first)
	}
	wantEmpty(t, "released, no expiry", o.res, o.err, EmptyNoExpiry, "n1")
}

// TestRacedPathSecondConnectOnTheSameNode: a second CONNECT of x on n2 looked
// the lease up before n2 applied the first CONNECT's grant, and proposed after
// it. The first takes the lease from n1 and pulls the session; the second is
// counted raced.
func TestRacedPathSecondConnectOnTheSameNode(t *testing.T) {
	n1, n2, hold, _ := racedHoldCluster(t, 300)
	n1.sessions.put("default", "x", sessionFor("x"))
	hold.armed.Store(true)
	second := acquireAsyncAt(n2, "x", 300)
	hold.wait(t, "the second CONNECT looked x up and is held")
	firstOut := outcomeOf(t, acquireAsyncAt(n2, "x", 300))
	hold.unblock()
	o := outcomeOf(t, second)
	if firstOut.err != nil || firstOut.res.PrevOwner != "n1" || firstOut.res.State == nil || o.res.Epoch != firstOut.res.Epoch {
		t.Fatalf("precondition not reached: first %+v (err %v), second's token %d", firstOut.res, firstOut.err, o.res.Epoch)
	}
	wantEmpty(t, "second CONNECT", o.res, o.err, EmptyRaced, "n1")
}

// TestRacedPathHandoffToTheConnectingNode: n1 hands its leases to n2 after
// n2's CONNECT looked x up and before its acquire applied; n2's adoption
// worker is held back. The CONNECT fetches the hand-off's adoption itself.
func TestRacedPathHandoffToTheConnectingNode(t *testing.T) {
	n1, n2, hold, _ := racedHoldCluster(t, 300)
	n1.sessions.put("default", "x", sessionFor("x"))
	hold.armed.Store(true)
	done := acquireAsyncAt(n2, "x", 300)
	hold.wait(t, "n2's acquire looked x up and is held")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	moved, err := n1.svc.Handoff(ctx, "n2")
	t.Logf("handoff: moved %d, err %v", moved, err)
	ownedBy(t, n2, "x", "n2")
	hold.unblock()
	o := outcomeOf(t, done)
	if moved != 1 || err != nil {
		t.Fatalf("precondition not reached: handoff moved %d, %v", moved, err)
	}
	if o.err != nil || o.res.State == nil || o.res.Empty.Reason != "" {
		t.Fatalf("after the hand-off: state %v, evidence %+v, err %v; want the adopted state and no evidence", o.res.State != nil, o.res.Empty, o.err)
	}
}

func TestEmptyReason(t *testing.T) {
	for _, c := range []struct {
		reason string
		found  bool
		expiry uint32
		want   string
	}{
		{EmptyRaced, true, 0, EmptyNoExpiry},
		{EmptyTakeover, true, 0, EmptyNoExpiry},
		{EmptyGrantUnfetched, true, 0, EmptyNoExpiry},
		{EmptyRaced, true, 300, EmptyRaced},
		{EmptyTakeover, false, 0, EmptyTakeover},
		{"", true, 0, ""},
	} {
		if got := emptyReason(c.reason, c.found, c.expiry); got != c.want {
			t.Errorf("emptyReason(%q, %v, %d) = %q, want %q", c.reason, c.found, c.expiry, got, c.want)
		}
	}
}

// TestTakeoverOfAnEndedNoExpirySessionIsCountedApart: x's session on n1 had
// no expiry and ended with its connection, but n1's release has not applied
// when n2 takes the lease: n1 answers that it holds nothing and no replica
// has a copy. Counted as no-expiry, not as a takeover that found nothing.
func TestTakeoverOfAnEndedNoExpirySessionIsCountedApart(t *testing.T) {
	fixtureFallback = foundNowhere
	t.Cleanup(func() { fixtureFallback = nil })
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	a, b := c.nodes["n1"], c.nodes["n2"]
	if _, err := acquire(t, a, "x", nil, 0, false); err != nil {
		t.Fatal(err)
	}
	ownedBy(t, b, "x", "n1")
	res, err := acquire(t, b, "x", nil, 0, false)
	if res.PrevOwner != "n1" {
		t.Fatalf("precondition not reached: prev %q, want a takeover from n1", res.PrevOwner)
	}
	wantEmpty(t, "takeover of an ended no-expiry session", res, err, EmptyNoExpiry, "n1")
}

// TestLocalLeaseWithNoExpiryIsCountedApart: the lease is this node's and
// online, granted by a CONNECT that asked for no session expiry and never
// took a session here; the next CONNECT is served with nothing.
func TestLocalLeaseWithNoExpiryIsCountedApart(t *testing.T) {
	c := newCluster(t, []string{"n1"}, 2*time.Second, nil)
	n := c.nodes["n1"]
	if _, err := acquire(t, n, "g", nil, 0, true); err != nil {
		t.Fatal(err)
	}
	ownedBy(t, n, "g", "n1")
	res, err := acquire(t, n, "g", nil, 0, false)
	wantEmpty(t, "online, no expiry, nothing held", res, err, EmptyNoExpiry, "n1")
}
