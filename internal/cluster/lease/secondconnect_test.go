package lease

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
)

// secondConnectCluster is three nodes with x's session on n1 at a session
// expiry, n2's table showing it, and n2's acquires of x passing through
// before (held before the proposal) and taking (held after a grant from
// another owner, before the pull).
func secondConnectCluster(t *testing.T) (n2 *tnode, before, taking *acquireHold) {
	t.Helper()
	n2, before, taking, _ = secondConnectClusterLooking(t)
	return n2, before, taking
}

// secondConnectClusterLooking is secondConnectCluster with n2's acquires of x
// also passing through looking, held before their lookup once armed.
func secondConnectClusterLooking(t *testing.T) (n2 *tnode, before, taking, looking *acquireHold) {
	t.Helper()
	before, taking, looking = newAcquireHold(false), newAcquireHold(true), newAcquireHold(false)
	fixtureFallback = foundNowhere
	t.Cleanup(func() { fixtureFallback = nil })
	fixtureOptions = func(o *Options) {
		heldBack(o)
		if o.Node == "n2" {
			o.AcquireGate = func(ns, id string, granted bool) {
				before.gate(ns, id, granted)
				taking.gate(ns, id, granted)
			}
			o.LookupGate = func(ns, id string) { looking.gate(ns, id, false) }
		}
	}
	t.Cleanup(func() { fixtureOptions = nil })
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	t.Cleanup(before.unblock)
	t.Cleanup(taking.unblock)
	t.Cleanup(looking.unblock)
	n1, n2 := c.nodes["n1"], c.nodes["n2"]
	if _, err := acquire(t, n1, "x", nil, 300, false); err != nil {
		t.Fatal(err)
	}
	n1.sessions.put("default", "x", sessionFor("x"))
	ownedBy(t, n2, "x", "n1")
	return n2, before, taking, looking
}

// answeredWhileHeld reports the second CONNECT's outcome if it is answered
// within wait, while the first's takeover is still held.
func answeredWhileHeld(done chan acquireOutcome, wait time.Duration) (acquireOutcome, bool) {
	select {
	case o := <-done:
		return o, true
	case <-time.After(wait):
		return acquireOutcome{}, false
	}
}

// TestSecondConnectRacedIsNotAnsweredWhileTheFirstsTakeoverIsInFlight:
// a second CONNECT of x on n2 looks the lease up under n1 and is held before
// it proposes. A first CONNECT on n2 then takes the lease from n1 and is held
// before its pull. The second's acquire applies after the first's grant, so
// the lease is already n2's: it must not be answered before the first's
// takeover, which carries the session, has ended.
func TestSecondConnectRacedIsNotAnsweredWhileTheFirstsTakeoverIsInFlight(t *testing.T) {
	n2, before, taking := secondConnectCluster(t)
	before.armed.Store(true)
	second := acquireAsyncAt(n2, "x", 300)
	before.wait(t, "the second CONNECT looked x up under n1 and is held")
	taking.armed.Store(true)
	first := acquireAsyncAt(n2, "x", 300)
	taking.wait(t, "the first CONNECT took x from n1 and is held before its pull")
	ownedBy(t, n2, "x", "n2")
	before.unblock()
	if o, early := answeredWhileHeld(second, 500*time.Millisecond); early {
		t.Fatalf("the second CONNECT was answered while the first's takeover was in flight: state %v, evidence %+v, err %v", o.res.State != nil, o.res.Empty, o.err)
	}
	taking.unblock()
	f := outcomeOf(t, first)
	if f.err != nil || f.res.PrevOwner != "n1" || f.res.State == nil {
		t.Fatalf("precondition not reached: the first CONNECT %+v (err %v), want the session taken from n1", f.res, f.err)
	}
	o := outcomeOf(t, second)
	if o.err != nil || o.res.Epoch != f.res.Epoch {
		t.Fatalf("the second CONNECT after the first's takeover: %+v, %v; want served at the first's token %d", o.res, o.err, f.res.Epoch)
	}
	if st := n2.svc.LivenessStats(); st.ConnectsInFlight != 0 || st.TakeoverWaitsServed != 1 {
		t.Fatalf("after both: %d CONNECTs in flight, %d takeover waits served; want 0 and 1", st.ConnectsInFlight, st.TakeoverWaitsServed)
	}
}

// TestSecondConnectOnTheOwnedLeaseIsNotAnsweredWhileTheFirstsTakeoverIsInFlight:
// a first CONNECT of x on n2 takes the lease from n1 and is held before its
// pull; n2's table applies the grant. A second CONNECT on n2 then finds the
// lease n2's own: it must not be answered before the first's takeover has
// ended.
func TestSecondConnectOnTheOwnedLeaseIsNotAnsweredWhileTheFirstsTakeoverIsInFlight(t *testing.T) {
	n2, _, taking := secondConnectCluster(t)
	taking.armed.Store(true)
	first := acquireAsyncAt(n2, "x", 300)
	taking.wait(t, "the first CONNECT took x from n1 and is held before its pull")
	ownedBy(t, n2, "x", "n2")
	second := acquireAsyncAt(n2, "x", 300)
	if o, early := answeredWhileHeld(second, 500*time.Millisecond); early {
		t.Fatalf("the second CONNECT was answered while the first's takeover was in flight: state %v, evidence %+v, err %v", o.res.State != nil, o.res.Empty, o.err)
	}
	taking.unblock()
	f := outcomeOf(t, first)
	if f.err != nil || f.res.PrevOwner != "n1" || f.res.State == nil {
		t.Fatalf("precondition not reached: the first CONNECT %+v (err %v), want the session taken from n1", f.res, f.err)
	}
	o := outcomeOf(t, second)
	if o.err != nil || o.res.Epoch != f.res.Epoch {
		t.Fatalf("the second CONNECT after the first's takeover: %+v, %v; want served at the first's token %d", o.res, o.err, f.res.Epoch)
	}
	if st := n2.svc.LivenessStats(); st.ConnectsInFlight != 0 || st.TakeoverWaitsServed != 1 {
		t.Fatalf("after both: %d CONNECTs in flight, %d takeover waits served; want 0 and 1", st.ConnectsInFlight, st.TakeoverWaitsServed)
	}
}

// TestCleanStartOnTheOwnedLeaseWaitsForTheFirstsTakeover: a clean start is not
// answered while the first's takeover is in flight either; it then discards
// what the takeover installed, as any clean start does.
func TestCleanStartOnTheOwnedLeaseWaitsForTheFirstsTakeover(t *testing.T) {
	n2, _, taking := secondConnectCluster(t)
	taking.armed.Store(true)
	first := acquireAsyncAt(n2, "x", 300)
	taking.wait(t, "the first CONNECT took x from n1 and is held before its pull")
	ownedBy(t, n2, "x", "n2")
	second := acquireAsync(n2, "x", true)
	if o, early := answeredWhileHeld(second, 500*time.Millisecond); early {
		t.Fatalf("the clean start was answered while the first's takeover was in flight: state %v, err %v", o.res.State != nil, o.err)
	}
	taking.unblock()
	if f := outcomeOf(t, first); f.err != nil || f.res.State == nil {
		t.Fatalf("precondition not reached: the first CONNECT %+v (err %v)", f.res, f.err)
	}
	if o := outcomeOf(t, second); o.err != nil || o.res.State != nil {
		t.Fatalf("the clean start: state %v, err %v; want served with nothing", o.res.State != nil, o.err)
	}
}

// TestSecondConnectRefusedWhileTheFirstsTakeoverOutlastsItsHandshake: the
// first's takeover is held past the second's handshake bound, so the second
// is refused Server Busy with nothing; the first's count ends with it.
func TestSecondConnectRefusedWhileTheFirstsTakeoverOutlastsItsHandshake(t *testing.T) {
	n2, _, taking := secondConnectCluster(t)
	taking.armed.Store(true)
	first := acquireAsyncAt(n2, "x", 300)
	taking.wait(t, "the first CONNECT took x from n1 and is held before its pull")
	ownedBy(t, n2, "x", "n2")
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	res, err := n2.svc.Acquire(ctx, "default", "x", nil, 300, false)
	if !refusedBusy(err) || res.State != nil {
		t.Fatalf("the second CONNECT past its bound: state %v, err %v; want Server Busy and no state", res.State != nil, err)
	}
	if st := n2.svc.LivenessStats(); st.TakeoverWaitsRefused != 1 || st.ConnectsInFlight != 1 {
		t.Fatalf("takeover waits refused %d, CONNECTs in flight %d; want 1 and 1 (the first's)", st.TakeoverWaitsRefused, st.ConnectsInFlight)
	}
	taking.unblock()
	if f := outcomeOf(t, first); f.err != nil || f.res.State == nil {
		t.Fatalf("precondition not reached: the first CONNECT %+v (err %v)", f.res, f.err)
	}
	if st := n2.svc.LivenessStats(); st.ConnectsInFlight != 0 {
		t.Fatalf("after the first: %d CONNECTs in flight, want 0", st.ConnectsInFlight)
	}
}

// TestSecondConnectWaitingOnATakeoverWhenTheServiceStops: Server Unavailable.
func TestSecondConnectWaitingOnATakeoverWhenTheServiceStops(t *testing.T) {
	n2, _, taking := secondConnectCluster(t)
	taking.armed.Store(true)
	_ = acquireAsyncAt(n2, "x", 300)
	taking.wait(t, "the first CONNECT took x from n1 and is held before its pull")
	ownedBy(t, n2, "x", "n2")
	second := acquireAsyncAt(n2, "x", 300)
	if o, early := answeredWhileHeld(second, 300*time.Millisecond); early {
		t.Fatalf("answered before the stop: state %v, err %v", o.res.State != nil, o.err)
	}
	go n2.svc.Stop()
	o := outcomeOf(t, second)
	var r *Refusal
	if o.res.State != nil || !errors.As(o.err, &r) || r.Reason != packet.ServerUnavailable {
		t.Fatalf("on the service's stop: state %v, err %v; want Server Unavailable", o.res.State != nil, o.err)
	}
}

// TestConnectsRefusedAfterTheirProposalLeaveNothingInFlight: an acquire the log
// refuses for a live owner, one refused for another reason, and one whose
// CONNECT gave up while it was proposed each end their time in flight, so the
// client's next CONNECT here is not held to its bound.
func TestConnectsRefusedAfterTheirProposalLeaveNothingInFlight(t *testing.T) {
	for _, c := range []struct {
		name   string
		answer *Result
	}{
		{"owner alive", &Result{Reason: ownerAlive}},
		{"refused", &Result{Reason: "refused"}},
		{"gave up", nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := ownedElsewhere(t, &fakeTransport{up: []string{"n1", "n3"}})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, err := s.AcquireFrom(ctx, "default", "x", nil, 60, false, Requester{})
				done <- err
			}()
			waitFor(t, "the acquire to be proposed", 2*time.Second, func() bool { return len(pendingOps(s)) >= 1 })
			if got := s.LivenessStats().ConnectsInFlight; got != 1 {
				t.Fatalf("while proposed: %d CONNECTs in flight, want 1", got)
			}
			if c.answer != nil {
				pendingOps(s)[0].done <- *c.answer
			} else {
				cancel()
			}
			if err := <-done; err == nil {
				t.Fatal("the acquire was not refused")
			}
			if got := s.LivenessStats().ConnectsInFlight; got != 0 {
				t.Fatalf("after the refusal: %d CONNECTs in flight, want 0", got)
			}
			if idle := s.connecting.idle(key{"default", "x"}); idle != nil {
				t.Fatal("the client still has a CONNECT in flight here")
			}
		})
	}
}

func TestOvertaken(t *testing.T) {
	for _, c := range []struct {
		newest, arrival uint64
		want            bool
	}{
		{5, 4, true},
		{5, 5, false},
		{5, 6, false},
		{0, 1, false},
	} {
		if got := overtaken(c.newest, c.arrival); got != c.want {
			t.Errorf("overtaken(%d, %d) = %v, want %v", c.newest, c.arrival, got, c.want)
		}
	}
}

// TestOlderWaiterIsRefusedAndTheNewerServed: a second and then a third CONNECT
// of x wait on the first's takeover; when it ends the third is served and the
// second is refused Server Busy, whichever wakes first.
func TestOlderWaiterIsRefusedAndTheNewerServed(t *testing.T) {
	n2, _, taking := secondConnectCluster(t)
	taking.armed.Store(true)
	first := acquireAsyncAt(n2, "x", 300)
	taking.wait(t, "the first CONNECT took x from n1 and is held before its pull")
	ownedBy(t, n2, "x", "n2")
	second := acquireAsyncAt(n2, "x", 300)
	waitFor(t, "the second CONNECT waiting", 5*time.Second, func() bool { return n2.svc.LivenessStats().TakeoverWaiting == 1 })
	third := acquireAsyncAt(n2, "x", 300)
	waitFor(t, "the third CONNECT waiting", 5*time.Second, func() bool { return n2.svc.LivenessStats().TakeoverWaiting == 2 })
	taking.unblock()
	if f := outcomeOf(t, first); f.err != nil || f.res.State == nil {
		t.Fatalf("precondition not reached: the first CONNECT %+v (err %v)", f.res, f.err)
	}
	if o := outcomeOf(t, second); !refusedBusy(o.err) {
		t.Fatalf("the older waiter: %+v, %v; want Server Busy", o.res, o.err)
	}
	if o := outcomeOf(t, third); o.err != nil {
		t.Fatalf("the newer waiter: %v; want served", o.err)
	}
	if st := n2.svc.LivenessStats(); st.TakeoverWaitsOvertaken != 1 || st.TakeoverWaitsServed != 1 || st.TakeoverWaiting != 0 || st.ConnectsInFlight != 0 {
		t.Fatalf("overtaken %d, served %d, waiting %d, in flight %d; want 1, 1, 0, 0", st.TakeoverWaitsOvertaken, st.TakeoverWaitsServed, st.TakeoverWaiting, st.ConnectsInFlight)
	}
}

// TestOlderConnectSlowBeforeItsWaitIsStillTheOlder: the second CONNECT enters
// first but is held before its lookup; the third enters after it and waits
// first. The order is by arrival, so the third is served and the second,
// waiting last, is refused.
func TestOlderConnectSlowBeforeItsWaitIsStillTheOlder(t *testing.T) {
	n2, _, taking, looking := secondConnectClusterLooking(t)
	taking.armed.Store(true)
	first := acquireAsyncAt(n2, "x", 300)
	taking.wait(t, "the first CONNECT took x from n1 and is held before its pull")
	ownedBy(t, n2, "x", "n2")
	looking.armed.Store(true)
	second := acquireAsyncAt(n2, "x", 300)
	looking.wait(t, "the second CONNECT held before its lookup")
	third := acquireAsyncAt(n2, "x", 300)
	waitFor(t, "the third CONNECT waiting", 5*time.Second, func() bool { return n2.svc.LivenessStats().TakeoverWaiting == 1 })
	looking.unblock()
	waitFor(t, "the second CONNECT waiting", 5*time.Second, func() bool { return n2.svc.LivenessStats().TakeoverWaiting == 2 })
	taking.unblock()
	if f := outcomeOf(t, first); f.err != nil || f.res.State == nil {
		t.Fatalf("precondition not reached: the first CONNECT %+v (err %v)", f.res, f.err)
	}
	if o := outcomeOf(t, second); !refusedBusy(o.err) {
		t.Fatalf("the older CONNECT, waiting last: %+v, %v; want Server Busy", o.res, o.err)
	}
	if o := outcomeOf(t, third); o.err != nil {
		t.Fatalf("the newer CONNECT, waiting first: %v; want served", o.err)
	}
}

// TestWaitersKeepTheNewestUntilTheLastWakes: the newer waiter wakes and goes
// first; the older, waking after it, still sees it and is overtaken. Driven on
// the set, so the order of the wakes is the test's.
func TestWaitersKeepTheNewestUntilTheLastWakes(t *testing.T) {
	c := newConnecting()
	k := key{"default", "x"}
	c.wait(k, 5)
	c.wait(k, 8)
	if c.woken(k, 8) {
		t.Fatal("the newest waiter was overtaken")
	}
	if !c.woken(k, 5) {
		t.Fatal("the older waiter, waking after the newer one had gone, was not overtaken")
	}
	if c.waitingTotal() != 0 {
		t.Fatalf("%d waiters left, want 0", c.waitingTotal())
	}
	c.wait(k, 3)
	if c.woken(k, 3) {
		t.Fatal("a lone waiter after the others had gone was overtaken by them")
	}
}
