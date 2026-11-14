package lease

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/store"
)

// laggingGrantCluster is three nodes, the adoption worker held back, every
// replica fetch answering x's session with a subscription ("t/live"). A
// follower a owns x at t1 with an adoption queued for it. A first CONNECT on a
// fetches the adoption inline and returns unsettled, its import not yet made.
// Then a's table stops learning (every consensus frame the leader b sends a is
// dropped) and b takes the lease at t2: b pulls a, which answers that it holds
// nothing and notes t2, and b's fallback serves the session. On return a has
// seen t2 while its table still shows a owning x at t1; the drop is still in
// place.
func laggingGrantCluster(t *testing.T) (a, b *tnode, first acquireOutcome, t1, t2 uint64) {
	t.Helper()
	holdWorkerBack(t)
	fixtureFallback = func(context.Context, string, string, string) (*store.SessionState, bool) {
		st := sessionFor("x")
		st.Subscriptions = []store.SubscriptionRecord{{Filter: "t/live", QoS: 1}}
		return st, true
	}
	t.Cleanup(func() { fixtureFallback = nil })
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	b = c.settled()
	for _, n := range c.nodes {
		if n != b {
			a = n
			break
		}
	}
	res, err := acquire(t, a, "x", nil, 300, true)
	if err != nil {
		t.Fatal(err)
	}
	t1 = res.Epoch
	// a's acquire returns before a's own table has applied it; the first
	// CONNECT must find the lease a's to take the own-lease branch.
	ownedBy(t, a, "x", a.name)
	ownedBy(t, b, "x", a.name)
	a.svc.adopting.begin(key{"default", "x"}, adoptJob{epoch: t1, prev: "n9"})

	first = outcomeOf(t, acquireUnsettled(a, "x", 300))
	if first.err != nil || !first.res.Local || !hasFilter(first.res.State, "t/live") || first.res.Epoch != t1 {
		t.Fatalf("precondition: a's first CONNECT %+v (err %v); want the adopted session fetched inline at t1 %d", first.res, first.err, t1)
	}

	b.tr.SetFault(a.name, &transport.Fault{Drop: 1, Classes: []transport.Class{transport.Consensus}, Rand: rand.New(rand.NewSource(1))})
	taken, err := acquire(t, b, "x", nil, 300, false)
	if err != nil || taken.PrevOwner != a.name || !hasFilter(taken.State, "t/live") {
		t.Fatalf("precondition: b's takeover from a: %+v, %v; want a's lease taken with the session from the fallback", taken, err)
	}
	t2 = taken.Epoch
	taken.State.Epoch = t2
	b.sessions.Import(taken.State)
	if seen := a.svc.tokens.Seen("default", "x"); seen != t2 {
		t.Fatalf("precondition: a has seen %d for x; want b's t2 %d from b's pull", seen, t2)
	}
	if l, _ := a.svc.table.Lookup("default", "x"); l.Owner != a.name || l.Epoch != t1 {
		t.Fatalf("precondition: a's table moved to %s at %d while its consensus frames were dropped", l.Owner, l.Epoch)
	}
	return a, b, first, t1, t2
}

// refuseFirstImport is the caller's step for a's first CONNECT: the import is
// admitted only while no later grant has been seen, and it has, so it is
// refused and the CONNECT settled.
func refuseFirstImport(t *testing.T, a *tnode, first acquireOutcome) {
	t.Helper()
	if a.svc.AdmitImport("default", "x", first.res.Epoch)() {
		t.Fatal("precondition: a admitted the first CONNECT's import at t1 after seeing t2")
	}
	a.svc.NoteImportRefused("default", "x", first.res.Epoch, first.res.PrevOwner)
	first.res.Settle()
}

// judgeLaggingServe checks a CONNECT on a answered after the refused import,
// while a's table still showed a owning x at t1 and a had seen t2. Served on
// the lease a's table shows, the client would get a fresh session at t1 while b
// holds its session at t2: it must be refused Server Busy, counted, with its
// count in flight ended and no adoption left open.
func judgeLaggingServe(t *testing.T, a, b *tnode, o acquireOutcome, t1, t2 uint64, refusedBefore int64) {
	t.Helper()
	l, _ := a.svc.table.Lookup("default", "x")
	_, aHolds := a.sessions.Export("default", "x")
	st := a.svc.LivenessStats()
	t.Logf("t1 %d, t2 %d; a's CONNECT: err %v, local %v, token %d, state %v, evidence %+v; a's table %s at %d; a has seen %d; a holds x %v; b holds x with t/live %v; refused below a seen grant %d",
		t1, t2, o.err, o.res.Local, o.res.Epoch, o.res.State != nil, o.res.Empty, l.Owner, l.Epoch, a.svc.tokens.Seen("default", "x"), aHolds, b.sessions.has("default", "x"), st.BelowSeenGrantRefused)
	if l.Owner != a.name || l.Epoch != t1 {
		t.Fatalf("precondition: a's table moved to %s at %d before the CONNECT was judged", l.Owner, l.Epoch)
	}
	if !refusedBusy(o.err) {
		t.Fatalf("a answered x's CONNECT at %d (err %v, state %v, evidence %+v) after seeing t2 %d, its table still at t1; want Server Busy", o.res.Epoch, o.err, o.res.State != nil, o.res.Empty, t2)
	}
	if st.BelowSeenGrantRefused != refusedBefore+1 {
		t.Fatalf("refused below a seen grant %d, want %d", st.BelowSeenGrantRefused, refusedBefore+1)
	}
	// The refused import's hand-on counts in AdoptionState; the adoption itself
	// must be finished.
	if unfinished := a.svc.adopting.unfinished(); st.ConnectsInFlight != 0 || unfinished != 0 {
		t.Fatalf("after the refusal: %d CONNECTs in flight, %d adoptions unfinished; want 0 and 0", st.ConnectsInFlight, unfinished)
	}
}

// afterCatchUp lets a's table apply t2: the client's next CONNECT on a takes
// the lease from b and is served b's session.
func afterCatchUp(t *testing.T, a, b *tnode) {
	t.Helper()
	b.tr.SetFault(a.name, nil)
	ownedBy(t, a, "x", b.name)
	res, err := acquire(t, a, "x", nil, 300, false)
	t.Logf("after a's table caught up: err %v, previous owner %q, token %d, state with t/live %v, evidence %+v", err, res.PrevOwner, res.Epoch, hasFilter(res.State, "t/live"), res.Empty)
	if err != nil || res.PrevOwner != b.name || !hasFilter(res.State, "t/live") || res.Empty.Reason != "" {
		t.Fatalf("after a's table caught up: %+v, %v; want b's session taken over from b", res, err)
	}
}

// TestWaitingConnectNotServedBelowASeenGrant: a second CONNECT on a waits for
// the first, which holds the adopted session. The first's import is refused on
// b's later grant. The second must not then be served a fresh session on a's
// lagging lease.
func TestWaitingConnectNotServedBelowASeenGrant(t *testing.T) {
	a, b, first, t1, t2 := laggingGrantCluster(t)
	second := acquireUnsettled(a, "x", 300)
	if o, early := answeredWhileHeld(second, 300*time.Millisecond); early {
		o.res.Settle()
		t.Fatalf("precondition: the second CONNECT was answered while the first held the state: %+v, %v", o.res, o.err)
	}
	refusedBefore := a.svc.LivenessStats().BelowSeenGrantRefused
	refuseFirstImport(t, a, first)
	o := outcomeOf(t, second)
	o.res.Settle()
	judgeLaggingServe(t, a, b, o, t1, t2, refusedBefore)
	afterCatchUp(t, a, b)
}

// TestRetryNotServedBelowASeenGrant: the same order with no CONNECT waiting.
// The client's first CONNECT is refused at the import, and its retry reaches a
// while a's table still lags.
func TestRetryNotServedBelowASeenGrant(t *testing.T) {
	a, b, first, t1, t2 := laggingGrantCluster(t)
	refusedBefore := a.svc.LivenessStats().BelowSeenGrantRefused
	refuseFirstImport(t, a, first)
	o := outcomeOf(t, acquireUnsettled(a, "x", 300))
	o.res.Settle()
	judgeLaggingServe(t, a, b, o, t1, t2, refusedBefore)
	afterCatchUp(t, a, b)
}

// TestPostGrantConnectNotServedBelowASeenGrant: n2's retry of x looks the lease
// up under n3 and proposes; it applies with the lease already n2's (an earlier
// abandoned acquire of n2's took it from n3), so the retry fetches on the
// post-grant branch, pulling n3, which now holds nothing. While n3's answer is
// delayed, another node takes the lease from n2 at a later grant and pulls n2,
// which notes it. The retry must then not be served at its own grant.
func TestPostGrantConnectNotServedBelowASeenGrant(t *testing.T) {
	c, n2, n3 := takenByAbandonedFirst(t, foundNowhere, false)
	if l, ok := n2.svc.table.Lookup("default", "x"); !ok || l.Owner != "n3" {
		t.Skipf("precondition not reached: n2's table already shows %s before the retry", l.Owner)
	}
	n3.sessions.Evict("default", "x", 0)
	n3.tr.SetFault(n2.name, &transport.Fault{Delay: 2 * time.Second, Classes: []transport.Class{transport.TakeoverAck}, Rand: rand.New(rand.NewSource(1))})
	refusedBefore := n2.svc.LivenessStats().BelowSeenGrantRefused
	retry := acquireUnsettled(n2, "x", 300)
	pullInFlight(t, n2)
	n1 := c.nodes["n1"]
	ownedBy(t, n1, "x", n2.name)
	token := n2.svc.tokens.Seen("default", "x")
	if _, err := acquire(t, n1, "x", nil, 300, false); err != nil {
		t.Logf("n1's CONNECT: %v (its grant applies whatever it is answered)", err)
	}
	waitFor(t, "n2 to note n1's later grant", 5*time.Second, func() bool { return n2.svc.tokens.Seen("default", "x") > token })
	o := outcomeOf(t, retry)
	o.res.Settle()
	st := n2.svc.LivenessStats()
	t.Logf("n2's retry: err %v, local %v, previous owner %q, token %d (seen before n1 %d), state %v, evidence %+v; n2 has seen %d; refused below a seen grant %d", o.err, o.res.Local, o.res.PrevOwner, o.res.Epoch, token, o.res.State != nil, o.res.Empty, n2.svc.tokens.Seen("default", "x"), st.BelowSeenGrantRefused)
	if o.err == nil && (o.res.Local || o.res.PrevOwner != "" && o.res.PrevOwner != n2.name) {
		t.Fatalf("precondition not reached: the retry did not return on the post-grant branch: %+v", o.res)
	}
	if !refusedBusy(o.err) || st.BelowSeenGrantRefused != refusedBefore+1 {
		t.Fatalf("n2's retry answered %+v, %v after n2 saw a later grant; want Server Busy, counted below a seen grant", o.res, o.err)
	}
	if st.ConnectsInFlight != 0 {
		t.Fatalf("%d CONNECTs in flight after the refusal, want 0", st.ConnectsInFlight)
	}
}

func TestServedBelowSeen(t *testing.T) {
	for _, c := range []struct {
		seen, token uint64
		want        bool
	}{
		{0, 10, false},
		{9, 10, false},
		{10, 10, false},
		{11, 10, true},
	} {
		if got := servedBelowSeen(c.seen, c.token); got != c.want {
			t.Errorf("servedBelowSeen(seen %d, token %d) = %v, want %v", c.seen, c.token, got, c.want)
		}
	}
}
