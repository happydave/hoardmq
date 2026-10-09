package lease

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/store"
)

func TestReclaimOnConfirm(t *testing.T) {
	for _, tc := range []struct {
		name       string
		confirmed  uint64
		ownerHere  bool
		ownerEpoch uint64
		want       bool
	}{
		{"the lease is another node's", 9, false, 9, true},
		{"this node's table has not applied the confirmed grant", 9, true, 5, true},
		{"the client came back here after the confirmed grant", 9, true, 12, false},
		{"this node holds the confirmed grant itself", 9, true, 9, false},
	} {
		if got := reclaimOnConfirm(tc.confirmed, tc.ownerHere, tc.ownerEpoch); got != tc.want {
			t.Errorf("%s: reclaimOnConfirm(%d, %v, %d) = %v, want %v", tc.name, tc.confirmed, tc.ownerHere, tc.ownerEpoch, got, tc.want)
		}
	}
}

// TestSealEnds: a seal's check drops it only when the lease no longer exists,
// or when a target of the lease's owner holds the session at the lease's
// token; no argument says how long it has been kept.
func TestSealEnds(t *testing.T) {
	for _, tc := range []struct {
		name   string
		lease  bool
		owner  string
		token  uint64
		asked  string
		heldAt uint64
		want   sealVerdict
	}{
		{"no lease", false, "", 0, "", 0, sealDropLeaseGone},
		{"the lease is back here, though a target held it", true, "n2", 9, "n2", 9, sealKeep},
		{"a target of the owner holds it at the token", true, "n1", 9, "n1", 9, sealDropHeld},
		{"a target of the owner holds it at a later grant", true, "n1", 9, "n1", 12, sealDropHeld},
		{"a target holds an earlier grant's copy", true, "n1", 9, "n1", 8, sealKeep},
		{"the check asked about an owner the lease has since left", true, "n3", 9, "n1", 9, sealKeep},
		{"nothing asked", true, "n1", 9, "", 0, sealKeep},
		{"no target holds it", true, "n1", 9, "n1", 0, sealKeep},
	} {
		if got := sealEnds(tc.lease, tc.owner, "n2", tc.token, tc.asked, tc.heldAt); got != tc.want {
			t.Errorf("%s: sealEnds = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// fakeHeld stands in for the replicator's check of an owner's targets: it
// answers the epoch set for a client, counts the calls per client, and records
// the most calls in flight at once. With block set, each call waits for
// release; during, when set, runs inside each call before it answers.
type fakeHeld struct {
	mu       sync.Mutex
	epochs   map[string]uint64
	calls    map[string]int
	inFlight int
	most     int
	block    chan struct{}
	during   func(id string)
}

func newFakeHeld() *fakeHeld {
	return &fakeHeld{epochs: map[string]uint64{}, calls: map[string]int{}}
}

func (f *fakeHeld) check(ctx context.Context, _, id, _ string, _ uint64) uint64 {
	f.mu.Lock()
	f.calls[id]++
	f.inFlight++
	f.most = max(f.most, f.inFlight)
	block, during := f.block, f.during
	f.mu.Unlock()
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
		}
	}
	if during != nil {
		during(id)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.inFlight--
	return f.epochs[id]
}

func (f *fakeHeld) set(id string, epoch uint64) {
	f.mu.Lock()
	f.epochs[id] = epoch
	f.mu.Unlock()
}

func (f *fakeHeld) callsFor(id string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[id]
}

func (f *fakeHeld) mostInFlight() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.most
}

// withSealChecks gives every node of the next cluster a 300 ms seal bound and
// held's check of an owner's targets.
func withSealChecks(t *testing.T, held *fakeHeld) {
	t.Helper()
	fixtureOptions = func(o *Options) {
		o.AdoptionDelay, o.AdoptRetryDelay = time.Hour, time.Hour
		o.SealTimeout = 300 * time.Millisecond
		o.HeldAtTargets = held.check
	}
	t.Cleanup(func() { fixtureOptions = nil })
	fixtureFallback = func(context.Context, string, string, string) (*store.SessionState, bool) { return nil, true }
	t.Cleanup(func() { fixtureFallback = nil })
}

// sealTakeover: n2 holds x, n1 takes it over; returns the result and both
// nodes. n2's pass seals x.
func sealTakeover(t *testing.T) (AcquireResult, *tnode, *tnode) {
	t.Helper()
	a, b := takeoverCluster(t)
	if _, err := acquire(t, b, "x", nil, 300, true); err != nil {
		t.Fatal(err)
	}
	b.sessions.put("default", "x", sessionFor("x"))
	res, err := acquire(t, a, "x", nil, 300, false)
	if err != nil || res.State == nil || res.PrevOwner != "n2" {
		t.Fatalf("the takeover: %+v, %v", res, err)
	}
	return res, a, b
}

// TestConfirmationEndsTheSeal: once the new owner holds the session at its
// grant and it is durable, it confirms, and the previous owner drops the seal.
func TestConfirmationEndsTheSeal(t *testing.T) {
	res, a, b := sealTakeover(t)
	if b.sessions.sealedCount() != 1 {
		t.Fatal("the pass did not seal")
	}
	st := res.State
	st.Epoch = res.Epoch
	a.sessions.Import(st) // what the broker does with the result
	waitFor(t, "the seal to end on the confirmation", 5*time.Second, func() bool { return b.sessions.sealedCount() == 0 })
	if ls := b.svc.LivenessStats(); ls.SealsConfirmed != 1 || ls.ConfirmsReceived < 1 {
		t.Fatalf("giver counters %+v", ls)
	}
	if ls := a.svc.LivenessStats(); ls.ConfirmsSent != 1 {
		t.Fatalf("requester counters %+v", ls)
	}
}

// TestConfirmationWaitsForTheSessionHeldAndDurable: nothing is confirmed while
// the requester does not hold the session at its grant, or while its records
// are not durable.
func TestConfirmationWaitsForTheSessionHeldAndDurable(t *testing.T) {
	res, a, b := sealTakeover(t)
	a.sessions.setDurableErr(errors.New("a replica refused"))
	time.Sleep(300 * time.Millisecond) // six polls with nothing held here
	if b.sessions.sealedCount() != 1 || a.svc.LivenessStats().ConfirmsSent != 0 {
		t.Fatal("confirmed before the session was held here")
	}
	st := res.State
	st.Epoch = res.Epoch
	a.sessions.Import(st)
	waitFor(t, "a durability wait to be asked", 5*time.Second, func() bool {
		a.sessions.mu.Lock()
		defer a.sessions.mu.Unlock()
		return a.sessions.durableAsks >= 1
	})
	time.Sleep(200 * time.Millisecond) // polls while the wait fails
	if b.sessions.sealedCount() != 1 || a.svc.LivenessStats().ConfirmsSent != 0 {
		t.Fatal("confirmed while the session's records were not durable")
	}
	a.sessions.setDurableErr(nil)
	waitFor(t, "the seal to end once durable", 10*time.Second, func() bool { return b.sessions.sealedCount() == 0 })
}

// TestUnheldConfirmationIsDroppedAndTheSealStays: a requester that never
// imports (it died, or its CONNECT failed) never confirms; the seal waits for
// the bound.
func TestUnheldConfirmationIsDroppedAndTheSealStays(t *testing.T) {
	_, a, b := sealTakeover(t)
	waitFor(t, "the unheld confirmation to be dropped", 5*time.Second, func() bool { return a.svc.LivenessStats().ConfirmsUnheld >= 1 })
	if b.sessions.sealedCount() != 1 {
		t.Fatal("the seal ended with nothing confirmed")
	}
}

// TestConfirmationBelowTheSealLeavesIt: a copy re-sealed for a later requester
// is not dropped by an earlier requester's confirmation.
func TestConfirmationBelowTheSealLeavesIt(t *testing.T) {
	c := newCluster(t, []string{"n1"}, 2*time.Second, nil)
	n := c.nodes["n1"]
	n.sessions.mu.Lock()
	n.sessions.sealed[n.sessions.key("default", "x")] = &fakeSealed{st: sessionFor("x"), token: 50}
	n.sessions.mu.Unlock()
	n.svc.serveConfirmed(key{"default", "x"}, []any{confirmIn{token: 40, peer: "n9"}})
	if n.sessions.sealedCount() != 1 {
		t.Fatal("a confirmation below the seal's token dropped it")
	}
	n.svc.serveConfirmed(key{"default", "x"}, []any{confirmIn{token: 40, peer: "n9"}, confirmIn{token: 60, peer: "n8"}})
	if n.sessions.sealedCount() != 0 {
		t.Fatal("the highest confirmation queued, above the seal, did not drop it")
	}
}

// TestConfirmationEvictsANeverSealedOfflineSession: a clean start takes the
// lease without a pull, so the previous owner's session was never sealed; the
// confirmation ends it rather than the reclaim's seal and bound.
func TestConfirmationEvictsANeverSealedOfflineSession(t *testing.T) {
	a, b := takeoverCluster(t)
	if _, err := acquire(t, b, "x", nil, 300, true); err != nil {
		t.Fatal(err)
	}
	b.sessions.put("default", "x", sessionFor("x"))
	res, err := acquire(t, a, "x", nil, 300, true)
	if err != nil {
		t.Fatal(err)
	}
	a.sessions.put("default", "x", &store.SessionState{SessionMeta: store.SessionMeta{Namespace: "default", ClientID: "x", Epoch: res.Epoch}})
	waitFor(t, "the previous owner's session to be reclaimed", 5*time.Second, func() bool { return !b.sessions.has("default", "x") })
	if ls := b.svc.LivenessStats(); ls.ReclaimedOnConfirm != 1 || ls.SealedByReclaim != 0 {
		t.Fatalf("counters %+v", ls)
	}
}

// TestSealOutlivesItsBoundUntilATargetHoldsTheSession: n1 takes x from n2
// and never imports it, so nothing confirms n2's seal. While no target of n1
// holds x, n2 keeps the seal past several bounds and asks again at each; once
// one holds it at n1's grant, n2 drops it.
func TestSealOutlivesItsBoundUntilATargetHoldsTheSession(t *testing.T) {
	held := newFakeHeld()
	withSealChecks(t, held)
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	a, b := c.nodes["n1"], c.nodes["n2"]
	if _, err := acquire(t, b, "x", nil, 300, true); err != nil {
		t.Fatal(err)
	}
	b.sessions.put("default", "x", sessionFor("x"))
	res, err := acquire(t, a, "x", nil, 300, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, ok := b.sessions.Sealed("default", "x"); !ok {
		t.Fatal("precondition not reached: n2's pass did not seal x")
	}
	time.Sleep(1300 * time.Millisecond) // four bounds
	if _, _, ok := b.sessions.Sealed("default", "x"); !ok {
		t.Fatalf("n2 dropped x's seal while n1 was alive and no target held x (counters %+v)", b.svc.LivenessStats())
	}
	if n := held.callsFor("x"); n < 2 {
		t.Fatalf("n1's targets asked %d times in four bounds; want the check repeated", n)
	}
	if ls := b.svc.LivenessStats(); ls.SealsExpired != 0 || ls.SealChecksNotHeld < 2 {
		t.Fatalf("counters %+v", ls)
	}

	held.set("x", res.Epoch)
	waitFor(t, "the seal to end once a target holds x", 5*time.Second, func() bool { return b.sessions.sealedCount() == 0 })
	if ls := b.svc.LivenessStats(); ls.SealsHeld != 1 || ls.SealsExpired != 1 || ls.SealChecksHeld < 1 || ls.SealsConfirmed != 0 {
		t.Fatalf("counters %+v", ls)
	}
}

// TestSealCheckOfAnOwnerTheLeaseHasLeftIsNotActedOn: while n2 asks n1's
// targets about x, n3 takes x's lease, and the answer shows x held at n1's
// grant. That is no evidence about n3, so n2 keeps the seal and asks again.
func TestSealCheckOfAnOwnerTheLeaseHasLeftIsNotActedOn(t *testing.T) {
	held := newFakeHeld()
	withSealChecks(t, held)
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	a, b, n3 := c.nodes["n1"], c.nodes["n2"], c.nodes["n3"]
	if _, err := acquire(t, b, "x", nil, 300, true); err != nil {
		t.Fatal(err)
	}
	b.sessions.put("default", "x", sessionFor("x"))
	res, err := acquire(t, a, "x", nil, 300, false)
	if err != nil {
		t.Fatal(err)
	}
	held.set("x", res.Epoch)
	var moved sync.Once
	held.mu.Lock()
	held.during = func(string) {
		moved.Do(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := n3.svc.Acquire(ctx, "default", "x", nil, 300, true); err != nil {
				t.Errorf("n3's acquire of x: %v", err)
				return
			}
			for ctx.Err() == nil {
				if l, ok := b.svc.Table().Lookup("default", "x"); ok && l.Owner == "n3" {
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
			t.Errorf("n2's table never showed n3 owning x")
		})
	}
	held.mu.Unlock()
	waitFor(t, "a second check, or the seal gone", 5*time.Second, func() bool {
		_, _, sealed := b.sessions.Sealed("default", "x")
		return held.callsFor("x") >= 2 || !sealed
	})
	if _, _, ok := b.sessions.Sealed("default", "x"); !ok {
		t.Fatalf("n2 dropped x's seal on an answer about n1 after n3 took the lease (counters %+v)", b.svc.LivenessStats())
	}
}

// TestSealWhoseLeaseIsHereIsKeptUnasked: a copy sealed for a key whose lease
// is this node's waits for the adoption or CONNECT here; nothing is asked.
func TestSealWhoseLeaseIsHereIsKeptUnasked(t *testing.T) {
	held := newFakeHeld()
	withSealChecks(t, held)
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	b := c.nodes["n2"]
	if _, err := acquire(t, b, "y", nil, 300, true); err != nil {
		t.Fatal(err)
	}
	ownedBy(t, b, "y", "n2")
	b.sessions.mu.Lock()
	b.sessions.sealed[b.sessions.key("default", "y")] = &fakeSealed{st: sessionFor("y"), token: 1}
	b.sessions.mu.Unlock()
	held.set("y", 1<<40) // were it asked, a target would hold y past any grant
	b.svc.armSeal(key{"default", "y"}, 1)
	time.Sleep(700 * time.Millisecond) // two bounds
	if _, _, ok := b.sessions.Sealed("default", "y"); !ok {
		t.Fatal("a seal whose lease is back here was dropped at the bound")
	}
	if n := held.callsFor("y"); n != 0 {
		t.Fatalf("asked about a seal whose lease is here %d times", n)
	}
}

// TestSealWhoseLeaseIsGoneEnds: once the client's lease no longer exists, the
// seal goes at its next check, and no owner's targets are asked.
func TestSealWhoseLeaseIsGoneEnds(t *testing.T) {
	held := newFakeHeld()
	withSealChecks(t, held)
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	a, b := c.nodes["n1"], c.nodes["n2"]
	if _, err := acquire(t, b, "z", nil, 300, true); err != nil {
		t.Fatal(err)
	}
	b.sessions.put("default", "z", sessionFor("z"))
	res, err := acquire(t, a, "z", nil, 300, false)
	if err != nil {
		t.Fatal(err)
	}
	a.svc.Release("default", "z", res.Epoch)
	waitFor(t, "n2's table to drop z's lease", 5*time.Second, func() bool {
		_, ok := b.svc.Table().Lookup("default", "z")
		return !ok
	})
	waitFor(t, "the seal to end with its lease", 5*time.Second, func() bool { return b.sessions.sealedCount() == 0 })
	if ls := b.svc.LivenessStats(); ls.SealsLeaseGone != 1 || ls.SealsHeld != 0 {
		t.Fatalf("counters %+v", ls)
	}
	if n := held.callsFor("z"); n != 0 {
		t.Fatalf("asked about a seal whose lease is gone %d times", n)
	}
}

// TestSealChecksRunOnTheirQueue: twelve seals come due together while every
// check blocks; at most the queue's eight workers ask at once.
func TestSealChecksRunOnTheirQueue(t *testing.T) {
	held := newFakeHeld()
	release := make(chan struct{})
	held.block = release
	unblock := sync.OnceFunc(func() { close(release) })
	withSealChecks(t, held)
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	t.Cleanup(unblock)
	a, b := c.nodes["n1"], c.nodes["n2"]
	var ids []string
	for i := 0; i < 12; i++ {
		id := fmt.Sprint("q", i)
		ids = append(ids, id)
		if _, err := acquire(t, a, id, nil, 300, true); err != nil {
			t.Fatal(err)
		}
		ownedBy(t, b, id, "n1")
		b.sessions.mu.Lock()
		b.sessions.sealed[b.sessions.key("default", id)] = &fakeSealed{st: sessionFor(id), token: 1}
		b.sessions.mu.Unlock()
	}
	for _, id := range ids {
		b.svc.armSeal(key{"default", id}, 1)
	}
	waitFor(t, "eight checks in flight", 5*time.Second, func() bool { return held.mostInFlight() >= 8 })
	time.Sleep(300 * time.Millisecond)
	if n := held.mostInFlight(); n != sealCheckWorkers {
		t.Fatalf("%d checks in flight at once, want %d", n, sealCheckWorkers)
	}
	unblock()
}

// TestReclaimBeforeAQueuedPullLeavesTheSessionToIt: the reclaim runs while the
// new owner's pull still waits in this node's queue. The reclaim seals, so the
// pull is answered with the session.
func TestReclaimBeforeAQueuedPullLeavesTheSessionToIt(t *testing.T) {
	fixtureOptions = func(o *Options) {
		o.AdoptionDelay, o.AdoptRetryDelay = time.Hour, time.Hour
		if o.Node == "n2" {
			o.TakeoverDelay = 4 * time.Second // the reclaim runs at 2 s
		}
	}
	t.Cleanup(func() { fixtureOptions = nil })
	fixtureFallback = func(context.Context, string, string, string) (*store.SessionState, bool) { return nil, true }
	t.Cleanup(func() { fixtureFallback = nil })
	c := newCluster(t, []string{"n1", "n2", "n3"}, 5*time.Second, nil)
	a, b := c.nodes["n1"], c.nodes["n2"]
	if _, err := acquire(t, b, "x", nil, 300, true); err != nil {
		t.Fatal(err)
	}
	b.sessions.put("default", "x", sessionFor("x"))
	if _, err := acquire(t, a, "x", nil, 300, false); !refusedBusy(err) {
		t.Fatalf("the takeover whose pull outlived the CONNECT: %v, want Server Busy", err)
	}
	waitFor(t, "the reclaim to have run", 5*time.Second, func() bool { return !b.sessions.has("default", "x") })
	waitFor(t, "the queued pass's late answer", 8*time.Second, func() bool { return a.svc.LivenessStats().OrphanedAcks >= 1 })
	res, err := acquire(t, a, "x", nil, 300, false)
	if err != nil || res.State == nil || len(res.State.Subscriptions) != 1 {
		t.Fatalf("the client's retry: %+v, %v; want the session the pass sent", res, err)
	}
	if ls := b.svc.LivenessStats(); ls.SealedByReclaim != 1 || ls.GivenResent != 1 {
		t.Fatalf("giver counters %+v", ls)
	}
}

// TestLateStateImportIsConfirmedToItsSender: a late answer's state, imported
// by the worker, is confirmed to the node that sent it — here the only record
// of that node is the answer itself.
func TestLateStateImportIsConfirmedToItsSender(t *testing.T) {
	retries(t, 6, time.Hour)
	fixtureFallback = func(context.Context, string, string, string) (*store.SessionState, bool) { return nil, true }
	t.Cleanup(func() { fixtureFallback = nil })
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	b, sender := c.nodes["n2"], c.nodes["n3"]
	first, err := acquire(t, b, "x", nil, 300, true)
	if err != nil {
		t.Fatal(err)
	}
	// The worker judges the lease on this node's own table, which lags the
	// acquire's result.
	ownedBy(t, b, "x", "n2")
	b.svc.receiveTakeoverAck("n3", lateAck("x", first.Epoch))
	waitFor(t, "the worker to import the late state", 5*time.Second, func() bool { return b.sessions.importedCount() >= 1 })
	waitFor(t, "the sender to receive the confirmation", 5*time.Second, func() bool { return sender.svc.LivenessStats().ConfirmsReceived >= 1 })
}

// TestPassBelowThisNodesOwnGrantLeavesTheClient: a request queued before this
// node was granted the client again is answered superseded, and the client
// connected here at the later grant is neither detached nor sealed.
func TestPassBelowThisNodesOwnGrantLeavesTheClient(t *testing.T) {
	c := newCluster(t, []string{"n1"}, 2*time.Second, nil)
	n := c.nodes["n1"]
	st := sessionFor("x")
	st.Epoch = 1000
	n.sessions.put("default", "x", st)
	n.sessions.setConnAge("default", "x", time.Second)
	n.svc.serveTakeover(key{"default", "x"}, []any{pendingTakeover{req: takeoverRequest{Req: 1, Namespace: "default", ClientID: "x", Epoch: 50}, peer: "n9"}})
	n.sessions.mu.Lock()
	detached := len(n.sessions.detached)
	n.sessions.mu.Unlock()
	if detached != 0 || !n.sessions.has("default", "x") || n.sessions.sealedCount() != 0 {
		t.Fatalf("detached %d, held %v, sealed %d; want the session left as it is", detached, n.sessions.has("default", "x"), n.sessions.sealedCount())
	}
}

// TestAdoptionUsesTheSealedCopy: the lease came back through an adoption, no
// replica answered with a copy and nothing is held here, so the adoption
// imports this node's own sealed copy.
func TestAdoptionUsesTheSealedCopy(t *testing.T) {
	retries(t, 6, time.Hour)
	f := newAdoptionFixtureWith(t, false, func(int) (*store.SessionState, bool) { return nil, true })
	f.n.sessions.mu.Lock()
	f.n.sessions.sealed[f.n.sessions.key("default", "c")] = &fakeSealed{st: sessionFor("c"), token: 1}
	f.n.sessions.mu.Unlock()
	f.n.svc.adoptState(f.k)
	if f.n.sessions.importedCount() != 1 || f.n.sessions.sealedCount() != 0 {
		t.Fatalf("imports %d, sealed %d; want the sealed copy imported", f.n.sessions.importedCount(), f.n.sessions.sealedCount())
	}
	if ls := f.n.svc.LivenessStats(); ls.GivenReused != 1 || ls.AdoptWorkerEmpty != 0 {
		t.Fatalf("counters %+v", ls)
	}
}

// TestInlineAdoptionIsConfirmedToItsPreviousOwner: a CONNECT that settles its
// lease's adoption with a state owes the adoption's previous owner a
// confirmation, once the session is held here.
func TestInlineAdoptionIsConfirmedToItsPreviousOwner(t *testing.T) {
	f := newAdoptionFixture(t, false)
	res, err := acquire(t, f.n, "c", nil, 60, false)
	if err != nil || res.State == nil {
		t.Fatalf("the CONNECT: %+v, %v", res, err)
	}
	st := res.State
	st.Epoch = res.Epoch
	f.n.sessions.Import(st) // what the broker does with the result
	// n9 is no member, so the send fails: what is asserted is that one was owed
	// to the adoption's previous owner and attempted.
	waitFor(t, "the confirmation to n9", 5*time.Second, func() bool { return f.n.svc.LivenessStats().ConfirmsUnsent >= 1 })
}
