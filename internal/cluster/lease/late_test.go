package lease

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/store"
)

// lateAck is a takeover acknowledgement for a request nobody waits for,
// carrying the client's state stamped with token.
func lateAck(id string, token uint64) []byte {
	st := sessionFor(id)
	st.Epoch = token
	body, _ := json.Marshal(takeoverAck{Req: 1 << 50, State: st})
	return body
}

func holdWorkerBack(t *testing.T) {
	t.Helper()
	fixtureOptions = func(o *Options) { o.AdoptionDelay, o.AdoptRetryDelay = time.Hour, time.Hour }
	t.Cleanup(func() { fixtureOptions = nil })
}

func TestDeliver(t *testing.T) {
	k := key{"default", "c"}
	st := sessionFor("c")
	job := adoptJob{epoch: 7, takeover: true}

	a := newAdoptionSet()
	a.deliver(k, job, st)
	if ad := a.m[k]; ad == nil || ad.running || ad.job != job || ad.late != st {
		t.Fatalf("no record before: %+v", a.m[k])
	}

	a = newAdoptionSet()
	a.begin(k, adoptJob{epoch: 7, prev: "n9"})
	a.deliver(k, job, st)
	if ad := a.m[k]; ad.job.prev != "n9" || ad.late != st {
		t.Fatalf("a queued record for the same token keeps its job and takes the state: %+v", ad)
	}
	ad, _ := a.claim(k)
	if n, mine := a.giveBack(k, ad, true, false); !mine || n != 1 || a.m[k].late != st || ad.late != nil {
		t.Fatalf("a give-back carries the state to the fresh record: %+v", a.m[k])
	}
	a.begin(k, adoptJob{epoch: 9, prev: "n8"})
	if a.m[k].late != nil {
		t.Fatal("a newer adoption at another token kept a late state that was not given up for it")
	}

	a = newAdoptionSet()
	a.begin(k, adoptJob{epoch: 5, prev: "n9"})
	a.deliver(k, job, st)
	if ad := a.m[k]; ad.job != job || ad.late != st || ad.attempts != 0 {
		t.Fatalf("a queued record for another token takes the job and the state: %+v", ad)
	}

	a = newAdoptionSet()
	a.begin(k, adoptJob{epoch: 5, prev: "n9"})
	running, _ := a.claim(k)
	a.deliver(k, job, st)
	if ad := a.m[k]; ad == running || ad.running || ad.job != job || ad.late != st || running.late != nil {
		t.Fatalf("a running record for another token is left to finish and a new one takes the state: %+v", ad)
	}

	a = newAdoptionSet()
	a.begin(k, job)
	running, _ = a.claim(k)
	a.deliver(k, job, st)
	if got := a.takeLate(running); got != st || a.takeLate(running) != nil {
		t.Fatal("a running record for the same token takes the state, once")
	}
	a.deliver(k, job, st)
	if got := a.end(k, running); got != st {
		t.Fatal("ending a record says what late state was still on it")
	}
}

func TestTakeLateFor(t *testing.T) {
	k := key{"default", "c"}
	st := sessionFor("c")
	a := newAdoptionSet()
	if a.takeLateFor(k) != nil {
		t.Fatal("state from nothing")
	}
	a.deliver(k, adoptJob{epoch: 7, takeover: true}, st)
	ad := a.m[k]
	if got := a.takeLateFor(k); got != st || a.unfinished() != 0 {
		t.Fatalf("a queued record: state %v, unfinished %d; want the state and the adoption over", got != nil, a.unfinished())
	}
	select {
	case <-ad.done:
	default:
		t.Fatal("whoever waited on the adoption was not released")
	}
	a.deliver(k, adoptJob{epoch: 7, takeover: true}, st)
	running, _ := a.claim(k)
	if got := a.takeLateFor(k); got != st || a.unfinished() != 1 || running.late != nil {
		t.Fatalf("a running record loses the state and is left to its fetcher: %v, unfinished %d", got != nil, a.unfinished())
	}
	a.end(k, running) // the fetcher's own end must still be the one close
}

// TestLateStateIsImportedWhenNoSessionIsHeld: the previous owner removed the
// session to send it, so the acknowledgement that came after the pull gave up
// is the session. Nobody is asked for it again.
func TestLateStateIsImportedWhenNoSessionIsHeld(t *testing.T) {
	retries(t, 6, time.Hour)
	f := newAdoptionFixtureWith(t, false, func(int) (*store.SessionState, bool) { return nil, true })
	token := f.n.svc.adopting.m[f.k].job.epoch
	f.n.svc.receiveTakeoverAck("n9", lateAck("c", token))
	waitFor(t, "the worker to import the late state", 5*time.Second, func() bool { return f.n.sessions.importedCount() >= 1 })
	waitFor(t, "the adoption to end", 2*time.Second, func() bool { return f.n.svc.adopting.unfinished() == 0 })
	st, _ := f.n.sessions.Export("default", "c")
	if st == nil || len(st.Subscriptions) != 1 || st.Epoch != token {
		t.Fatalf("imported %+v, want the late state at token %d", st, token)
	}
	if calls := f.calls.Load(); calls != 0 {
		t.Fatalf("the replicas were asked %d times with the session in hand", calls)
	}
	if ls := f.n.svc.LivenessStats(); ls.LateStateUsed != 1 || ls.LateStateSuperfluous != 0 || ls.AdoptedByWorker != 1 || f.n.sessions.importedCount() != 1 {
		t.Fatalf("counters %+v, imports %d", ls, f.n.sessions.importedCount())
	}
}

// TestLateStateNeverReplacesAHeldSession: the client was served from a copy,
// and an import would close its connection.
func TestLateStateNeverReplacesAHeldSession(t *testing.T) {
	retries(t, 6, time.Hour)
	f := newAdoptionFixtureWith(t, false, func(int) (*store.SessionState, bool) { return nil, true })
	token := f.n.svc.adopting.m[f.k].job.epoch
	held := sessionFor("c")
	f.n.sessions.put("default", "c", held)
	f.n.svc.receiveTakeoverAck("n9", lateAck("c", token))
	waitFor(t, "the late state to be set aside", 5*time.Second, func() bool { return f.n.svc.LivenessStats().LateStateSuperfluous >= 1 })
	waitFor(t, "the adoption to end", 2*time.Second, func() bool { return f.n.svc.adopting.unfinished() == 0 })
	if got, _ := f.n.sessions.Export("default", "c"); got != held || f.n.sessions.importedCount() != 0 {
		t.Fatalf("held session replaced: imports %d", f.n.sessions.importedCount())
	}
	if ls := f.n.svc.LivenessStats(); ls.LateStateUsed != 0 || ls.LateStateSuperfluous != 1 || ls.AdoptWorkerEmpty != 0 {
		t.Fatalf("counters %+v", ls)
	}
}

// TestRetryingClientIsAdmittedWithTheLateState: the refused client comes back
// to the same node before the worker has the adoption.
func TestRetryingClientIsAdmittedWithTheLateState(t *testing.T) {
	holdWorkerBack(t)
	f := newAdoptionFixtureWith(t, false, func(int) (*store.SessionState, bool) { return nil, true })
	token := f.n.svc.adopting.m[f.k].job.epoch
	f.n.svc.receiveTakeoverAck("n9", lateAck("c", token))
	res, err := acquire(t, f.n, "c", nil, 60, false)
	if err != nil || res.State == nil || len(res.State.Subscriptions) != 1 || res.State.Epoch != token {
		t.Fatalf("the retry: %+v, %v", res, err)
	}
	if calls := f.calls.Load(); calls != 0 {
		t.Fatalf("the replicas were asked %d times with the session in hand", calls)
	}
	if ls := f.n.svc.LivenessStats(); ls.LateStateUsed != 1 || ls.AdoptedInline != 1 || f.n.svc.adopting.unfinished() != 0 {
		t.Fatalf("counters %+v, unfinished %d", ls, f.n.svc.adopting.unfinished())
	}
}

// TestLateStateWaitsForThisNodesTableToCatchUp: a node is told its token
// before its own table has applied the grant. A state given up for that token
// is not for a lease that moved on.
func TestLateStateWaitsForThisNodesTableToCatchUp(t *testing.T) {
	retries(t, 6, time.Hour)
	f := newAdoptionFixtureWith(t, false, func(int) (*store.SessionState, bool) { return nil, true })
	ahead := f.n.svc.adopting.m[f.k].job.epoch + 100
	f.n.svc.tokens.Note("default", "c", ahead)
	f.n.svc.receiveTakeoverAck("n9", lateAck("c", ahead))
	waitFor(t, "the worker's pass", 5*time.Second, func() bool {
		ls := f.n.svc.LivenessStats()
		return ls.AdoptWorkerUnanswered+ls.AdoptWorkerMoved >= 1
	})
	ls := f.n.svc.LivenessStats()
	f.n.svc.adopting.mu.Lock()
	ad := f.n.svc.adopting.m[f.k]
	kept := ad != nil && ad.late != nil && ad.job.epoch == ahead
	f.n.svc.adopting.mu.Unlock()
	if !kept || ls.LateStateLeaseMoved != 0 || ls.AdoptRetries < 1 || f.n.sessions.importedCount() != 0 {
		t.Fatalf("state kept %v, counters %+v", kept, ls)
	}
}

// TestLateStateForALeaseThatMovedOnIsDropped and said so.
func TestLateStateForALeaseThatMovedOnIsDropped(t *testing.T) {
	f := newAdoptionFixtureWith(t, false, func(int) (*store.SessionState, bool) { return nil, true })
	token := f.n.svc.adopting.m[f.k].job.epoch
	f.n.svc.tokens.Note("default", "c", token+5) // a later grant has been seen
	f.n.svc.receiveTakeoverAck("n9", lateAck("c", token))
	if ls := f.n.svc.LivenessStats(); ls.LateStateLeaseMoved != 1 || ls.OrphanedAckStates != 1 {
		t.Fatalf("counters %+v", ls)
	}
	f.n.svc.adopting.mu.Lock()
	late := f.n.svc.adopting.m[f.k].late
	f.n.svc.adopting.mu.Unlock()
	if late != nil {
		t.Fatal("a state for an older grant was kept")
	}
}

// TestNextOwnersPullIsGivenTheLateState: the client retried on another node
// before this one had imported the state it was keeping.
func TestNextOwnersPullIsGivenTheLateState(t *testing.T) {
	holdWorkerBack(t)
	fixtureFallback = func(context.Context, string, string, string) (*store.SessionState, bool) { return nil, true }
	t.Cleanup(func() { fixtureFallback = nil })
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	a, b := c.nodes["n1"], c.nodes["n2"]
	first, err := acquire(t, b, "x", nil, 300, true)
	if err != nil {
		t.Fatal(err)
	}
	b.svc.receiveTakeoverAck("n3", lateAck("x", first.Epoch))
	if b.svc.adopting.unfinished() != 1 {
		t.Fatal("the late state was not kept")
	}
	res, err := acquire(t, a, "x", nil, 300, false)
	if err != nil || res.PrevOwner != "n2" || res.State == nil || len(res.State.Subscriptions) != 1 || res.State.Epoch != res.Epoch {
		t.Fatalf("the next owner's takeover: %+v, %v", res, err)
	}
	if ls := b.svc.LivenessStats(); ls.LateStateHandedOn != 1 || b.svc.adopting.unfinished() != 0 {
		t.Fatalf("counters %+v, unfinished %d", ls, b.svc.adopting.unfinished())
	}
	if ls := a.svc.LivenessStats(); ls.TakeoverEmpty != 0 {
		t.Fatalf("the next owner found nothing: %+v", ls)
	}
}

// takeoverCluster is three nodes whose replicas answer "not found", with the
// adoption workers held back.
func takeoverCluster(t *testing.T) (a, b *tnode) {
	t.Helper()
	holdWorkerBack(t)
	fixtureFallback = func(context.Context, string, string, string) (*store.SessionState, bool) { return nil, true }
	t.Cleanup(func() { fixtureFallback = nil })
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	return c.nodes["n1"], c.nodes["n2"]
}

// TestGivenStateIsReusedWhenTheLeaseComesBack: the session was sealed for a
// takeover whose requester never had it, and the client is back here. With the
// pass removing it instead (RemoveOnHandOver) there is no copy to come back to.
func TestGivenStateIsReusedWhenTheLeaseComesBack(t *testing.T) {
	a, b := takeoverCluster(t)
	if _, err := acquire(t, b, "x", nil, 300, true); err != nil {
		t.Fatal(err)
	}
	b.sessions.put("default", "x", sessionFor("x"))
	if res, err := acquire(t, a, "x", nil, 300, false); err != nil || res.State == nil {
		t.Fatalf("the takeover: %+v, %v", res, err)
	}
	if b.sessions.has("default", "x") || b.sessions.sealedCount() != 1 {
		t.Fatalf("the pass left held %v, sealed %d; want sealed", b.sessions.has("default", "x"), b.sessions.sealedCount())
	}
	ownedBy(t, b, "x", "n1")
	// The requester never held it: its pull had given up, or it has died.
	res, err := acquire(t, b, "x", nil, 300, false)
	if err != nil || res.PrevOwner != "n1" || res.State == nil || len(res.State.Subscriptions) != 1 {
		t.Fatalf("the lease coming back: %+v, %v", res, err)
	}
	if ls := b.svc.LivenessStats(); ls.GivenReused != 1 || ls.TakeoverEmpty != 0 || ls.Sealed != 1 {
		t.Fatalf("counters %+v", ls)
	}
}

// TestGivenStateIsSentAgainToARequesterAskingTwice: the first answer was not
// waited for.
func TestGivenStateIsSentAgainToARequesterAskingTwice(t *testing.T) {
	a, b := takeoverCluster(t)
	if _, err := acquire(t, b, "x", nil, 300, true); err != nil {
		t.Fatal(err)
	}
	b.sessions.put("default", "x", sessionFor("x"))
	first, err := acquire(t, a, "x", nil, 300, false)
	if err != nil || first.State == nil {
		t.Fatalf("the takeover: %+v, %v", first, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	st, answered, _, _ := a.svc.pull(ctx, "n2", "default", "x", first.Epoch, Requester{})
	if !answered || st == nil || len(st.Subscriptions) != 1 {
		t.Fatalf("asked again: state %v, answered %v", st != nil, answered)
	}
	if ls := b.svc.LivenessStats(); ls.GivenResent != 1 {
		t.Fatalf("counters %+v", ls)
	}
}

// TestTakeoverThatFindsTheSessionStillHereIsNotEmpty: the lease left and came
// back before this node was asked for the session or reclaimed it.
func TestTakeoverThatFindsTheSessionStillHereIsNotEmpty(t *testing.T) {
	a, b := takeoverCluster(t)
	if _, err := acquire(t, b, "x", nil, 300, true); err != nil {
		t.Fatal(err)
	}
	b.sessions.put("default", "x", sessionFor("x"))
	if _, err := acquire(t, a, "x", nil, 300, true); err != nil { // a clean start asks for nothing
		t.Fatal(err)
	}
	ownedBy(t, b, "x", "n1")
	res, err := acquire(t, b, "x", nil, 300, false)
	if err != nil || res.PrevOwner != "n1" {
		t.Fatalf("%+v, %v", res, err)
	}
	if ls := b.svc.LivenessStats(); ls.TakeoverHeld != 1 || ls.TakeoverEmpty != 0 || ls.TakeoverUnanswered != 0 {
		t.Fatalf("counters %+v", ls)
	}
	if _, held := b.sessions.Export("default", "x"); !held {
		t.Fatal("the session the client resumes is gone")
	}
}

// ownedBy waits until the node's own table shows the owner: an acquire reads
// it to choose between renewing locally and taking the lease over.
func ownedBy(t *testing.T, n *tnode, id, owner string) {
	t.Helper()
	waitFor(t, n.name+"'s table to show "+owner+" owning "+id, 5*time.Second, func() bool {
		l, ok := n.svc.table.Lookup("default", id)
		return ok && l.Owner == owner
	})
}

// TestLeaseBackWhileThisNodeIsGivingTheSessionAway: the seal files its copy in
// the same step that removes the session, so a lease that comes back before the
// pass finds the session held, and one that comes back after it finds the
// sealed copy: served either way.
func TestLeaseBackWhileThisNodeIsGivingTheSessionAway(t *testing.T) {
	a, b := takeoverCluster(t)
	if _, err := acquire(t, b, "x", nil, 300, true); err != nil {
		t.Fatal(err)
	}
	b.sessions.put("default", "x", sessionFor("x"))
	if res, err := acquire(t, a, "x", nil, 300, false); err != nil || res.State == nil {
		t.Fatalf("the takeover: %+v, %v", res, err)
	}
	ownedBy(t, b, "x", "n1")
	res, err := acquire(t, b, "x", nil, 300, false)
	if err != nil || res.State == nil || len(res.State.Subscriptions) != 1 {
		t.Fatalf("the lease back after the pass: %+v, %v; want the sealed copy", res, err)
	}

	// Before the pass: the session is still held when the lease comes back.
	b.sessions.put("default", "y", sessionFor("y"))
	if _, err := acquire(t, b, "y", nil, 300, false); err != nil {
		t.Fatal(err)
	}
	b.sessions.put("default", "y", sessionFor("y"))
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	b.sessions.setOnTake(func(_, id string) {
		if id == "y" {
			entered <- struct{}{}
			<-release
		}
	})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	go func() { _, _ = acquire(t, a, "y", nil, 300, false) }()
	<-entered
	ownedBy(t, b, "y", "n1")
	if _, err := acquire(t, b, "y", nil, 300, false); err != nil {
		t.Fatalf("the lease back before the pass: %v, want served", err)
	}
	if ls := b.svc.LivenessStats(); ls.TakeoverHeld != 1 || ls.TakeoverEmpty != 0 || ls.TakeoverUnanswered != 0 {
		t.Fatalf("counters %+v", ls)
	}
	unblock()
}

// TestDiscardLateStateIsTheOldBehaviour: with the injected defect a late
// answer's state is dropped.
func TestDiscardLateStateIsTheOldBehaviour(t *testing.T) {
	fixtureOptions = func(o *Options) { o.DiscardLateState = true }
	t.Cleanup(func() { fixtureOptions = nil })
	fixtureFallback = func(context.Context, string, string, string) (*store.SessionState, bool) { return nil, true }
	t.Cleanup(func() { fixtureFallback = nil })
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	b := c.nodes["n2"]
	first, err := acquire(t, b, "x", nil, 300, true)
	if err != nil {
		t.Fatal(err)
	}
	b.svc.receiveTakeoverAck("n3", lateAck("x", first.Epoch))
	if ls := b.svc.LivenessStats(); ls.OrphanedAckStates != 1 || b.svc.adopting.unfinished() != 0 {
		t.Fatalf("counters %+v, unfinished %d", ls, b.svc.adopting.unfinished())
	}
}

// TestCleanStartForgetsWhatCameLate: a clean start discards the session, and a
// late state kept here must not come back for a later requester. (A sealed copy
// here is discarded by the new session's CONNECT, in the session manager.)
func TestCleanStartForgetsWhatCameLate(t *testing.T) {
	holdWorkerBack(t)
	f := newAdoptionFixtureWith(t, false, func(int) (*store.SessionState, bool) { return nil, true })
	token := f.n.svc.adopting.m[f.k].job.epoch
	f.n.svc.receiveTakeoverAck("n9", lateAck("c", token))
	if _, err := acquire(t, f.n, "c", nil, 60, true); err != nil {
		t.Fatal(err)
	}
	if f.n.svc.adopting.unfinished() != 0 {
		t.Fatalf("after a clean start: adoptions unfinished %d", f.n.svc.adopting.unfinished())
	}
}

// TestGivenStateStaysWhenTheLeaseHasMovedOn: an adoption whose lease has moved
// on reads the sealed copy and leaves it for whoever has the lease now.
func TestGivenStateStaysWhenTheLeaseHasMovedOn(t *testing.T) {
	retries(t, 6, time.Hour)
	f := newAdoptionFixtureWith(t, false, func(int) (*store.SessionState, bool) { return nil, true })
	f.n.sessions.mu.Lock()
	f.n.sessions.sealed[f.n.sessions.key("default", "c")] = &fakeSealed{st: sessionFor("c"), token: 3}
	f.n.sessions.mu.Unlock()
	f.n.svc.adopting.begin(f.k, adoptJob{epoch: f.n.svc.adopting.m[f.k].job.epoch + 7, prev: "n9"}) // not the lease's token
	f.n.svc.adoptState(f.k)
	if ls := f.n.svc.LivenessStats(); ls.AdoptWorkerMoved != 1 || ls.GivenReused != 0 || f.n.sessions.sealedCount() != 1 {
		t.Fatalf("counters %+v, sealed %d", ls, f.n.sessions.sealedCount())
	}
}
