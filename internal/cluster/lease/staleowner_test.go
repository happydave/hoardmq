package lease

import (
	"context"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/store"
)

// staleOwnerCluster is three nodes whose replicas hold nothing for x, the
// adoption worker held back. b leads the group. b owns x, holds its session
// with a subscription only it has ("t/live"), and a, a follower, has taken the
// lease from b at t1 without the session: b's answer to a's pull was lost, so
// a keeps a takeover adoption that waits on b. Then a's table stops learning
// (every consensus frame b sends a is dropped) and the client returns to b,
// which takes the lease back at t2 and serves it from the copy it sealed.
// On return a's table still shows a owning x at t1; the drops that are still
// in place are cleared by the caller.
func staleOwnerCluster(t *testing.T) (a, b *tnode, t1, t2 uint64) {
	t.Helper()
	holdWorkerBack(t)
	fixtureFallback = func(context.Context, string, string, string) (*store.SessionState, bool) { return nil, true }
	t.Cleanup(func() { fixtureFallback = nil })
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	b = c.settled()
	for _, n := range c.nodes {
		if n != b {
			a = n
			break
		}
	}
	rnd := rand.New(rand.NewSource(1))
	dropAcks := &transport.Fault{Drop: 1, Classes: []transport.Class{transport.TakeoverAck}, Rand: rnd}
	if _, err := acquire(t, b, "x", nil, 300, false); err != nil {
		t.Fatal(err)
	}
	live := sessionFor("x")
	live.Subscriptions = []store.SubscriptionRecord{{Filter: "t/live", QoS: 1}}
	b.sessions.put("default", "x", live)
	ownedBy(t, a, "x", b.name)

	// a takes the lease; b seals the session and its answer is lost.
	b.tr.SetFault(a.name, dropAcks)
	if _, _, err := connectAcquire(a, "x"); !refusedBusy(err) {
		t.Fatalf("precondition: a's takeover with b's answer lost: %v; want Server Busy", err)
	}
	l, _ := a.svc.table.Lookup("default", "x")
	if l.Owner != a.name || !a.svc.adopting.unfinishedFor(key{"default", "x"}) {
		t.Fatalf("precondition: a owns x %v and keeps its adoption %v", l.Owner == a.name, a.svc.adopting.unfinishedFor(key{"default", "x"}))
	}
	t1 = l.Epoch
	if _, _, sealed := b.sessions.Sealed("default", "x"); !sealed {
		t.Fatal("precondition: b did not seal the session for a's pull")
	}

	// a's table stops; the client goes back to b, which takes the lease again.
	b.tr.SetFault(a.name, &transport.Fault{Drop: 1, Classes: []transport.Class{transport.TakeoverAck, transport.Consensus}, Rand: rnd})
	ownedBy(t, b, "x", a.name)
	res, err := acquire(t, b, "x", nil, 300, false)
	if err != nil || !hasFilter(res.State, "t/live") {
		t.Fatalf("precondition: b's takeover back: state %v, %v; want b's own sealed copy", res.State, err)
	}
	t2 = res.Epoch
	res.State.Epoch = t2
	b.sessions.Import(res.State)
	if l, _ := a.svc.table.Lookup("default", "x"); l.Owner != a.name || l.Epoch != t1 {
		t.Fatalf("precondition: a's table moved to %s at %d while its consensus frames were dropped", l.Owner, l.Epoch)
	}
	return a, b, t1, t2
}

// pullInFlight waits until n has a takeover pull waiting for its answer.
func pullInFlight(t *testing.T, n *tnode) {
	t.Helper()
	waitFor(t, n.name+"'s pull in flight", 5*time.Second, func() bool {
		n.svc.mu.Lock()
		defer n.svc.mu.Unlock()
		return len(n.svc.pulls) > 0
	})
}

// countSeals counts b's seals of x from now: a takeover pass for x seals it,
// so a pass that runs is a pull b was asked.
func countSeals(n *tnode) *atomic.Int64 {
	var calls atomic.Int64
	n.sessions.setOnTake(func(_, id string) {
		if id == "x" {
			calls.Add(1)
		}
	})
	return &calls
}

// TestAbandonedAfterAStaleSelfLookupFetchesFromTheOwner holds the order a 1M
// storm produced. A CONNECT on a whose lookup read a's own lease at t1 (its
// table behind) fetches a's takeover adoption; while the pull waits, the table
// applies b's t2, so the fetch ends "moved" and the CONNECT proposes an acquire.
// Its time is spent, so it gives up; the acquire is granted afterwards at t3,
// from b. The note names b, the owner the table shows once the fetch found the
// lease moved, and the client's next CONNECT on a pulls the session from b,
// which holds it, rather than asking no previous owner and serving it fresh.
func TestAbandonedAfterAStaleSelfLookupFetchesFromTheOwner(t *testing.T) {
	a, b, t1, t2 := staleOwnerCluster(t)
	done := make(chan error, 1)
	go func() {
		_, _, err := connectAcquire(a, "x")
		done <- err
	}()
	pullInFlight(t, a)
	b.tr.SetFault(a.name, &transport.Fault{Drop: 1, Classes: []transport.Class{transport.TakeoverAck}, Rand: rand.New(rand.NewSource(2))})
	ownedBy(t, a, "x", b.name)
	if err := <-done; err == nil {
		t.Fatal("the CONNECT whose fetch saw the lease move was admitted")
	}
	a.svc.abandoned.mu.Lock()
	note, noted := a.svc.abandoned.m[key{"default", "x"}]
	a.svc.abandoned.mu.Unlock()
	t.Logf("t1 %d, t2 %d; a's abandoned note %v, previous owner %q", t1, t2, note.noted, note.prev)
	if !noted || !note.noted || note.prev != b.name {
		t.Fatalf("a's abandoned note %v names %q; want b, the owner a's table showed once the fetch found the lease moved", noted && note.noted, note.prev)
	}
	ownedBy(t, a, "x", a.name)
	l, _ := a.svc.table.Lookup("default", "x")
	if l.Epoch <= t2 || !l.Online {
		t.Fatalf("a's lease at %d online %v; want the abandoned acquire's grant above t2 %d", l.Epoch, l.Online, t2)
	}
	b.tr.SetFault(a.name, nil)
	// b's reclaim seals its copy two takeover timeouts after t3.
	waitFor(t, "b to seal its copy for t3", 10*time.Second, func() bool { _, tok, ok := b.sessions.Sealed("default", "x"); return ok && tok == l.Epoch })
	asked := countSeals(b)

	res, err := acquire(t, a, "x", nil, 300, false)
	t.Logf("t3 %d; served state %v, evidence %+v; b's passes for x %d", l.Epoch, res.State != nil, res.Empty, asked.Load())
	if err != nil || !hasFilter(res.State, "t/live") || res.Empty.Reason != "" || asked.Load() == 0 {
		t.Fatalf("a's next CONNECT: state %v, evidence %+v, err %v, b asked %d; want b's session pulled from b", res.State, res.Empty, err, asked.Load())
	}
}

// TestCurrentOwnerNoteFetchesFromIt is the control: the same lease history,
// with a's table caught up before the CONNECT that gives up looks the lease
// up, so its note names b. The next CONNECT pulls b and is served b's session.
func TestCurrentOwnerNoteFetchesFromIt(t *testing.T) {
	a, b, _, t2 := staleOwnerCluster(t)
	b.tr.SetFault(a.name, &transport.Fault{Drop: 1, Classes: []transport.Class{transport.TakeoverAck}, Rand: rand.New(rand.NewSource(2))})
	ownedBy(t, a, "x", b.name)
	giveUp(t, a, "x")
	a.svc.abandoned.mu.Lock()
	note, noted := a.svc.abandoned.m[key{"default", "x"}]
	a.svc.abandoned.mu.Unlock()
	if !noted || note.prev != b.name {
		t.Fatalf("a's abandoned note %v names %q; want b", noted, note.prev)
	}
	ownedBy(t, a, "x", a.name)
	l, _ := a.svc.table.Lookup("default", "x")
	if l.Epoch <= t2 {
		t.Fatalf("a's lease at %d; want above t2 %d", l.Epoch, t2)
	}
	b.tr.SetFault(a.name, nil)
	waitFor(t, "b to seal its copy for t3", 10*time.Second, func() bool { _, tok, ok := b.sessions.Sealed("default", "x"); return ok && tok == l.Epoch })
	asked := countSeals(b)

	res, err := acquire(t, a, "x", nil, 300, false)
	t.Logf("t3 %d; served state %v, evidence %+v; b's passes for x %d", l.Epoch, res.State != nil, res.Empty, asked.Load())
	if err != nil || !hasFilter(res.State, "t/live") || res.Empty.Reason != "" || asked.Load() == 0 {
		t.Fatalf("control: state %v, evidence %+v, err %v, b asked %d; want b's session pulled", res.State, res.Empty, err, asked.Load())
	}
}

// TestSupersededAnswerIsNotAnAnswer is the same history with b's answers
// reaching a while a's table stays behind: b answers a's pull for t1
// "superseded" (it has seen t2). That says nothing about where the session is,
// so a refuses the client and keeps the adoption; once a's table has caught
// up, the client's next CONNECT takes the lease from b with b's session.
func TestSupersededAnswerIsNotAnAnswer(t *testing.T) {
	a, b, t1, _ := staleOwnerCluster(t)
	b.tr.SetFault(a.name, &transport.Fault{Drop: 1, Classes: []transport.Class{transport.Consensus}, Rand: rand.New(rand.NewSource(2))})
	res, err := acquire(t, a, "x", nil, 300, false)
	t.Logf("t1 %d; served state %v, evidence %+v, err %v; b holds x %v", t1, res.State != nil, res.Empty, err, b.sessions.has("default", "x"))
	if !refusedBusy(err) || !a.svc.adopting.unfinishedFor(key{"default", "x"}) {
		t.Fatalf("state %v, evidence %+v, err %v, adoption kept %v; want Server Busy and the adoption kept", res.State != nil, res.Empty, err, a.svc.adopting.unfinishedFor(key{"default", "x"}))
	}
	b.tr.SetFault(a.name, nil)
	ownedBy(t, a, "x", b.name)
	res, err = acquire(t, a, "x", nil, 300, false)
	if err != nil || !hasFilter(res.State, "t/live") || res.Empty.Reason != "" {
		t.Fatalf("after a caught up: state %v, evidence %+v, err %v; want b's session", res.State, res.Empty, err)
	}
}
