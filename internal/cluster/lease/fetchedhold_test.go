package lease

import (
	"context"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/store"
)

func TestStateSettled(t *testing.T) {
	for _, c := range []struct {
		state, settle, want bool
	}{
		{true, true, true},
		{true, false, false},
		{false, false, true},
		{false, true, true},
	} {
		if got := stateSettled(c.state, c.settle); got != c.want {
			t.Errorf("stateSettled(state %v, settle %v) = %v, want %v", c.state, c.settle, got, c.want)
		}
	}
}

// TestEndAfterAbandonReleasesOnce: a claim held across an import can be
// abandoned meanwhile; ending it afterwards must not close done again.
func TestEndAfterAbandonReleasesOnce(t *testing.T) {
	a := newAdoptionSet()
	k := key{"default", "x"}
	a.begin(k, adoptJob{epoch: 3, prev: "n9"})
	ad, st := a.claim(k)
	if st != adoptionQueued {
		t.Fatalf("claim: %v, want queued", st)
	}
	a.abandon(k)
	a.end(k, ad)
	select {
	case <-ad.done:
	default:
		t.Fatal("done not closed")
	}
}

// acquireUnsettled is an acquire whose caller has not yet installed the state:
// the result is returned without its settle being run.
func acquireUnsettled(n *tnode, id string, expiry uint32) chan acquireOutcome {
	done := make(chan acquireOutcome, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		r, err := n.svc.AcquireFrom(ctx, "default", id, nil, expiry, false, Requester{})
		done <- acquireOutcome{r, err}
	}()
	return done
}

// confirmQueued reports whether a confirmation of the client is queued.
func confirmQueued(n *tnode, id string) bool {
	q := &n.svc.confirmOut
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, j := range q.jobs {
		if j.k.id == id {
			return true
		}
	}
	return false
}

// heldFirst checks the hold on node n for client id: first is a CONNECT whose
// result carried a fetched state and is not settled. A second CONNECT must
// not be answered while it is held; once the state is installed and the first
// settled, the second is served at the same token, finding the installed
// session. adopted says the state came through an adoption, which must stay
// unfinished, with no confirmation queued, while the first holds it.
func heldFirst(t *testing.T, n *tnode, id string, expiry uint32, f acquireOutcome, adopted bool) {
	t.Helper()
	if f.err != nil || f.res.State == nil {
		t.Fatalf("precondition not reached: the first CONNECT %+v (err %v), want a fetched state", f.res, f.err)
	}
	if f.res.settle == nil {
		t.Fatal("the first CONNECT's result carries a state and no settle")
	}
	if _, unfinished := n.svc.AdoptionState(); adopted && unfinished == 0 {
		t.Error("the adoption was finished while its state was still in the first CONNECT's hands")
	}
	// Looked at several times: the confirmation loop takes a job off the
	// queue for a moment each time it looks at it.
	for i := 0; adopted && i < 10; i++ {
		if confirmQueued(n, id) {
			t.Error("a confirmation was queued while the state was still in the first CONNECT's hands")
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	second := acquireUnsettled(n, id, expiry)
	if o, early := answeredWhileHeld(second, 500*time.Millisecond); early {
		o.res.Settle()
		t.Fatalf("the second CONNECT was answered while the first held the state: state %v, evidence %+v, err %v", o.res.State != nil, o.res.Empty, o.err)
	}
	st := f.res.State
	st.Epoch = f.res.Epoch
	n.sessions.put("default", id, st)
	f.res.Settle()
	o := outcomeOf(t, second)
	o.res.Settle()
	// The session is installed here now, so the caller finds it and serves it
	// present; nothing is left to fetch.
	if o.err != nil || o.res.Epoch != f.res.Epoch || o.res.State != nil {
		t.Fatalf("the second CONNECT after the first settled: %+v, %v; want served at token %d with nothing fetched", o.res, o.err, f.res.Epoch)
	}
	if s := n.svc.LivenessStats(); s.ConnectsInFlight != 0 || s.TakeoverWaitsServed < 1 || s.UnsettledStates != 0 {
		t.Fatalf("after both: %d in flight, %d waits served, %d unsettled; want 0, at least 1, 0", s.ConnectsInFlight, s.TakeoverWaitsServed, s.UnsettledStates)
	}
}

// TestSecondConnectWaitsForAnInlineAdoptionsImport: the own-lease branch's
// inline fetch.
func TestSecondConnectWaitsForAnInlineAdoptionsImport(t *testing.T) {
	f := newAdoptionFixture(t, false)
	f.state.Subscriptions = []store.SubscriptionRecord{{Filter: "t/c", QoS: 1}}
	first := outcomeOf(t, acquireUnsettled(f.n, "c", 60))
	if !first.res.Local || f.n.svc.LivenessStats().AdoptedInline != 1 {
		t.Fatalf("precondition not reached: local %v, adopted inline %d; want the own-lease branch's inline fetch", first.res.Local, f.n.svc.LivenessStats().AdoptedInline)
	}
	heldFirst(t, f.n, "c", 60, first, true)
}

// TestSecondConnectWaitsForAnAbandonedAcquiresFetch: the own-lease branch's
// fetch of the session an abandoned first acquire's grant took from n3.
func TestSecondConnectWaitsForAnAbandonedAcquiresFetch(t *testing.T) {
	_, n2, _ := takenByAbandonedFirst(t, nil, true)
	first := outcomeOf(t, acquireUnsettled(n2, "x", 300))
	if !first.res.Local || n2.svc.LivenessStats().FirstGrantsTakenAdopted != 1 || !hasFilter(first.res.State, "t/live") {
		t.Fatalf("precondition not reached: local %v, first grants taken adopted %d, state %v; want n3's session through fetchAbandoned", first.res.Local, n2.svc.LivenessStats().FirstGrantsTakenAdopted, first.res.State)
	}
	heldFirst(t, n2, "x", 300, first, true)
}

// TestSecondConnectWaitsForAFetchOnThePostGrantBranch: the retry's lookup
// still shows n3, so its acquire is proposed and applies with the lease
// already n2's; it fetches through fetchAbandoned on the post-grant branch.
func TestSecondConnectWaitsForAFetchOnThePostGrantBranch(t *testing.T) {
	_, n2, _ := takenByAbandonedFirst(t, nil, false)
	if l, ok := n2.svc.table.Lookup("default", "x"); !ok || l.Owner != "n3" {
		t.Skipf("precondition not reached: n2's table already shows %s before the retry", l.Owner)
	}
	first := outcomeOf(t, acquireUnsettled(n2, "x", 300))
	if first.err != nil {
		t.Skipf("precondition not reached: the retry was refused (%v), its table behind its grant", first.err)
	}
	if first.res.Local || first.res.PrevOwner != "" && first.res.PrevOwner != "n2" || !hasFilter(first.res.State, "t/live") {
		t.Fatalf("precondition not reached: local %v, previous owner %q, state %v; want the post-grant branch with n3's session", first.res.Local, first.res.PrevOwner, first.res.State)
	}
	heldFirst(t, n2, "x", 300, first, true)
}

// TestSecondConnectWaitsForAFirstAcquiresTakeover: n2's first CONNECT of x
// finds no lease and is held before it proposes; meanwhile x is granted on n3,
// which holds its session. n2's acquire then applies, taking the lease from
// n3, and pulls the session on the takeover branch.
func TestSecondConnectWaitsForAFirstAcquiresTakeover(t *testing.T) {
	hold := newAcquireHold(false)
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
	n2, n3 := c.nodes["n2"], c.nodes["n3"]
	hold.armed.Store(true)
	pending := acquireUnsettled(n2, "x", 300)
	hold.wait(t, "n2's first acquire of x held before its proposal")
	res, err := acquire(t, n3, "x", nil, 300, false)
	if err != nil {
		t.Fatal(err)
	}
	live := sessionFor("x")
	live.Subscriptions = []store.SubscriptionRecord{{Filter: "t/live", QoS: 1}}
	live.Epoch = res.Epoch
	n3.sessions.put("default", "x", live)
	ownedBy(t, n2, "x", "n3")
	hold.unblock()
	first := outcomeOf(t, pending)
	if first.err != nil || first.res.PrevOwner != "n3" || !hasFilter(first.res.State, "t/live") {
		t.Fatalf("precondition not reached: %+v (err %v); want n3's session taken over by a first acquire", first.res, first.err)
	}
	ownedBy(t, n2, "x", "n2")
	heldFirst(t, n2, "x", 300, first, false)
}
