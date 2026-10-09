package lease

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

func TestRetryDelay(t *testing.T) {
	base := 500 * time.Millisecond
	for _, c := range []struct {
		attempts int
		want     time.Duration
	}{
		{0, base}, {1, base}, {2, 2 * base}, {3, 4 * base}, {5, 16 * base}, {6, maxAdoptRetryDelay}, {60, maxAdoptRetryDelay},
	} {
		if got := retryDelay(base, c.attempts); got != c.want {
			t.Errorf("after %d unanswered fetches: %s, want %s", c.attempts, got, c.want)
		}
	}
	if got := retryDelay(time.Minute, 1); got != maxAdoptRetryDelay {
		t.Errorf("a base above the cap: %s", got)
	}
}

// retries sets the worker's retry schedule for the next cluster built.
func retries(t *testing.T, n int, delay time.Duration) {
	t.Helper()
	fixtureOptions = func(o *Options) { o.AdoptRetries, o.AdoptRetryDelay = n, delay }
	t.Cleanup(func() { fixtureOptions = nil })
}

func sessionFor(id string) *store.SessionState {
	return &store.SessionState{SessionMeta: store.SessionMeta{Namespace: "default", ClientID: id}, Subscriptions: []store.SubscriptionRecord{{Filter: "t/x", QoS: 1}}}
}

func refusedBusy(err error) bool {
	var r *Refusal
	return errors.As(err, &r) && r.Reason == packet.ServerBusy
}

// TestUnansweredInlineFetchRefusesAndKeepsTheAdoption: a fetch nobody answered
// has not learned that the session is held nowhere. The client is refused, the
// adoption stays, and the retry carries the state in.
func TestUnansweredInlineFetchRefusesAndKeepsTheAdoption(t *testing.T) {
	retries(t, 6, time.Hour) // the worker stays out of it
	state := sessionFor("c")
	f := newAdoptionFixtureWith(t, false, func(call int) (*store.SessionState, bool) {
		if call == 0 {
			return nil, false
		}
		return state, true
	})
	if _, err := acquire(t, f.n, "c", nil, 60, false); !refusedBusy(err) {
		t.Fatalf("a CONNECT whose fetch went unanswered: %v, want Server Busy", err)
	}
	if n := f.n.sessions.importedCount(); n != 0 {
		t.Fatalf("imported %d times", n)
	}
	if n := f.n.svc.adopting.unfinished(); n != 1 {
		t.Fatalf("%d adoptions unfinished after the refusal, want the one still to fetch", n)
	}
	if st := f.n.svc.LivenessStats(); st.AdoptInlineUnanswered != 1 || st.AdoptInlineEmpty != 0 || st.AdoptedInline != 0 {
		t.Fatalf("counters after the refusal %+v", st)
	}
	res, err := acquire(t, f.n, "c", nil, 60, false)
	if err != nil || res.State != state {
		t.Fatalf("the retry: state %v, err %v", res.State, err)
	}
	if n := f.n.svc.adopting.unfinished(); n != 0 {
		t.Fatalf("%d adoptions unfinished after the retry", n)
	}
}

// TestAnsweredEmptyInlineFetchStillAdmits: every node asked said it holds
// nothing, so a fresh session is what there is to serve.
func TestAnsweredEmptyInlineFetchStillAdmits(t *testing.T) {
	f := newAdoptionFixtureWith(t, false, func(int) (*store.SessionState, bool) { return nil, true })
	res, err := acquire(t, f.n, "c", nil, 60, false)
	if err != nil || res.State != nil {
		t.Fatalf("state %v, err %v", res.State, err)
	}
	if st := f.n.svc.LivenessStats(); st.AdoptInlineEmpty != 1 || st.AdoptInlineUnanswered != 0 || f.n.svc.adopting.unfinished() != 0 {
		t.Fatalf("counters %+v, unfinished %d", st, f.n.svc.adopting.unfinished())
	}
}

// TestWaiterFetchesForItselfWhenTheClaimIsGivenBack: a CONNECT held behind the
// worker's fetch does not wait out its handshake when that fetch settles
// nothing; it takes the adoption and fetches.
func TestWaiterFetchesForItselfWhenTheClaimIsGivenBack(t *testing.T) {
	retries(t, 6, time.Hour)
	state := sessionFor("c")
	f := newAdoptionFixtureWith(t, true, func(call int) (*store.SessionState, bool) {
		if call == 0 {
			return nil, false
		}
		return state, true
	})
	worker := make(chan struct{})
	go func() { defer close(worker); f.n.svc.adoptState(f.k) }()
	<-f.asked
	type result struct {
		res AcquireResult
		err error
	}
	got := make(chan result, 1)
	go func() {
		res, err := acquire(t, f.n, "c", nil, 60, false)
		got <- result{res, err}
	}()
	waitFor(t, "the CONNECT to be waiting on the worker's claim", 2*time.Second, func() bool {
		select {
		case <-f.asked:
			t.Error("the CONNECT fetched while the worker held the claim")
			return true
		default:
		}
		return f.n.svc.adopting.unfinished() == 1 && len(got) == 0
	})
	time.Sleep(50 * time.Millisecond) // let the acquire reach its wait; asserted below by AdoptWaited
	f.release()
	<-worker
	select {
	case r := <-got:
		if r.err != nil || r.res.State != state {
			t.Fatalf("the waiting CONNECT: state %v, err %v", r.res.State, r.err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the waiting CONNECT never returned after the claim was given back")
	}
	if st := f.n.svc.LivenessStats(); st.AdoptWaited != 1 || st.AdoptedInline != 1 || st.AdoptWorkerUnanswered != 1 || st.AdoptRetries != 1 {
		t.Fatalf("counters %+v", st)
	}
}

// TestWorkerRetriesAnUnansweredFetch: the adoption is not ended by a fetch
// that settled nothing; the worker comes back for it.
func TestWorkerRetriesAnUnansweredFetch(t *testing.T) {
	retries(t, 6, 5*time.Millisecond)
	state := sessionFor("c")
	f := newAdoptionFixtureWith(t, false, func(call int) (*store.SessionState, bool) {
		if call < 2 {
			return nil, false
		}
		return state, true
	})
	f.n.svc.adopts.enqueue(f.k, f.n.svc.adopting.m[f.k].job)
	waitFor(t, "the worker to import after its retries", 5*time.Second, func() bool { return f.n.sessions.importedCount() >= 1 })
	waitFor(t, "the adoption to end", 2*time.Second, func() bool { return f.n.svc.adopting.unfinished() == 0 })
	if st := f.n.svc.LivenessStats(); st.AdoptRetries != 2 || st.AdoptWorkerUnanswered != 2 || st.AdoptedByWorker != 1 || st.AdoptGaveUp != 0 {
		t.Fatalf("counters %+v", st)
	}
}

// TestWorkerGivesUpWhenItsRetriesAreSpent: an unfinished adoption keeps the
// lost owner's routes hidden on every node, so it must end.
func TestWorkerGivesUpWhenItsRetriesAreSpent(t *testing.T) {
	retries(t, 2, time.Millisecond)
	f := newAdoptionFixtureWith(t, false, func(int) (*store.SessionState, bool) { return nil, false })
	f.n.svc.adopts.enqueue(f.k, f.n.svc.adopting.m[f.k].job)
	waitFor(t, "the worker to give the adoption up", 5*time.Second, func() bool { return f.n.svc.LivenessStats().AdoptGaveUp >= 1 })
	if n := f.n.svc.adopting.unfinished(); n != 0 {
		t.Fatalf("%d adoptions unfinished after giving up", n)
	}
	time.Sleep(50 * time.Millisecond) // any further retry would land within this
	if st := f.n.svc.LivenessStats(); st.AdoptGaveUp != 1 || st.AdoptGaveUpKept != 0 || st.AdoptRetries != 2 || st.AdoptWorkerUnanswered != 3 || f.n.sessions.importedCount() != 0 {
		t.Fatalf("counters %+v, imports %d", st, f.n.sessions.importedCount())
	}
}

// keptFound has the next cluster's fallback report, beside its answer, a copy
// n4 kept of client: one subscription, a Will, written at token 2.
func keptFound(t *testing.T, client string) *store.SessionState {
	t.Helper()
	kept := &store.SessionState{SessionMeta: store.SessionMeta{Namespace: "default", ClientID: client, Epoch: 2, Will: &store.WillRecord{Delay: 5, Armed: true}},
		Subscriptions: []store.SubscriptionRecord{{Filter: "t/kept", QoS: 1}}}
	fixtureKept = func(ns, id string) (*store.SessionState, string) {
		if id != client {
			return nil, ""
		}
		cp := *kept
		return &cp, "n4"
	}
	t.Cleanup(func() { fixtureKept = nil })
	return kept
}

// TestWorkerGivesUpServingAKeptCopy: every fetch's source stays silent while
// n4 is found keeping a copy. When the retries are spent the worker serves
// that copy, at the adopted token and without its Will, and counts the give-up
// apart from a fresh one.
func TestWorkerGivesUpServingAKeptCopy(t *testing.T) {
	retries(t, 2, time.Millisecond)
	keptFound(t, "c")
	f := newAdoptionFixtureWith(t, false, func(int) (*store.SessionState, bool) { return nil, false })
	token := f.n.svc.adopting.m[f.k].job.epoch
	f.n.svc.adopts.enqueue(f.k, f.n.svc.adopting.m[f.k].job)
	waitFor(t, "the worker to give the adoption up", 5*time.Second, func() bool {
		st := f.n.svc.LivenessStats()
		return st.AdoptGaveUpKept >= 1 || st.AdoptGaveUp >= 1
	})
	waitFor(t, "the adoption to end", 2*time.Second, func() bool { return f.n.svc.adopting.unfinished() == 0 })
	st := f.n.svc.LivenessStats()
	if st.AdoptGaveUpKept != 1 || st.AdoptGaveUp != 0 || f.n.sessions.importedCount() != 1 {
		t.Fatalf("given up serving the kept copy %d, fresh %d, imports %d; want 1, 0, 1", st.AdoptGaveUpKept, st.AdoptGaveUp, f.n.sessions.importedCount())
	}
	got, held := f.n.sessions.Export("default", "c")
	if !held || got.Epoch != token || got.Will != nil || len(got.Subscriptions) != 1 || got.Subscriptions[0].Filter != "t/kept" {
		t.Fatalf("the session held after the give-up: %+v (held %v); want n4's copy at token %d with no Will", got, held, token)
	}
}

// TestKeptCopyAtTheGiveUpIsRefusedForALaterGrant: a later grant for c was seen
// before the give-up's import. The kept copy is not installed, the lease is
// handed on, and nothing is counted as served.
func TestKeptCopyAtTheGiveUpIsRefusedForALaterGrant(t *testing.T) {
	retries(t, 2, time.Millisecond)
	keptFound(t, "c")
	f := newAdoptionFixtureWith(t, false, func(int) (*store.SessionState, bool) { return nil, false })
	token := f.n.svc.adopting.m[f.k].job.epoch
	f.n.svc.tokens.Note("default", "c", token+10)
	f.n.svc.adopts.enqueue(f.k, f.n.svc.adopting.m[f.k].job)
	waitFor(t, "the adoption to end", 5*time.Second, func() bool { return f.n.svc.adopting.unfinished() == 0 })
	if st := f.n.svc.LivenessStats(); st.AdoptImportRefused != 1 || st.AdoptGaveUpKept != 0 || st.AdoptGaveUp != 0 || f.n.sessions.importedCount() != 0 {
		t.Fatalf("import refused %d, kept served %d, fresh %d, imports %d; want 1, 0, 0, 0", st.AdoptImportRefused, st.AdoptGaveUpKept, st.AdoptGaveUp, f.n.sessions.importedCount())
	}
}

// TestUnansweredTakeoverFetchRefusesAndRecordsAnAdoption: the lease is this
// node's once the acquire applies, so the refused client's retry arrives as
// the same owner. The adoption the takeover recorded is what makes that retry
// fetch again.
func TestUnansweredTakeoverFetchRefusesAndRecordsAnAdoption(t *testing.T) {
	retries(t, 6, time.Hour)
	state := sessionFor("x")
	calls := 0
	fixtureFallback = func(ctx context.Context, ns, id, _ string) (*store.SessionState, bool) {
		if id != "x" {
			return nil, true
		}
		calls++ // only the acquiring goroutine reaches here: the worker's retry is an hour away
		if calls == 1 {
			return nil, false
		}
		return state, true
	}
	t.Cleanup(func() { fixtureFallback = nil })
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	a, b := c.nodes["n1"], c.nodes["n2"]
	if _, err := acquire(t, a, "x", nil, 300, true); err != nil {
		t.Fatal(err)
	}
	if _, err := acquire(t, b, "x", nil, 300, false); !refusedBusy(err) {
		t.Fatalf("a takeover whose replica fetch went unanswered: %v, want Server Busy", err)
	}
	k := key{"default", "x"}
	ownedBy(t, b, "x", "n2") // the refusal returns on the acquire's result, which this node's table may not have applied
	l, _ := b.svc.table.Lookup("default", "x")
	ad := b.svc.adopting.m[k]
	if ad == nil || !ad.job.takeover || ad.job.prev != "n1" || ad.job.epoch != l.Epoch || l.Owner != "n2" {
		t.Fatalf("adoption %+v for lease %+v, want one recorded at the takeover's token", ad, l)
	}
	if st := b.svc.LivenessStats(); st.TakeoverUnanswered != 1 || st.TakeoverEmpty != 0 {
		t.Fatalf("counters %+v", st)
	}
	res, err := acquire(t, b, "x", nil, 300, false)
	if err != nil || res.State != state || res.State.Epoch != l.Epoch {
		t.Fatalf("the retry: %+v, err %v, want the state at token %d", res, err, l.Epoch)
	}
	if st := b.svc.LivenessStats(); st.AdoptedInline != 1 || st.AdoptLate != 0 || b.svc.adopting.unfinished() != 0 {
		t.Fatalf("counters after the retry %+v, unfinished %d", st, b.svc.adopting.unfinished())
	}
}

// TestAnsweredEmptyTakeoverStillAdmits and counts it.
func TestAnsweredEmptyTakeoverStillAdmits(t *testing.T) {
	fixtureFallback = func(context.Context, string, string, string) (*store.SessionState, bool) { return nil, true }
	t.Cleanup(func() { fixtureFallback = nil })
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	a, b := c.nodes["n1"], c.nodes["n2"]
	if _, err := acquire(t, a, "x", nil, 300, true); err != nil {
		t.Fatal(err)
	}
	res, err := acquire(t, b, "x", nil, 300, false)
	if err != nil || res.State != nil {
		t.Fatalf("%+v, %v", res, err)
	}
	if st := b.svc.LivenessStats(); st.TakeoverEmpty != 1 || st.TakeoverUnanswered != 0 || b.svc.adopting.unfinished() != 0 {
		t.Fatalf("counters %+v", st)
	}
}

// TestLateForIgnoresAnAdoptionATakeoverRecorded: "late" means a reassignment
// applied while the CONNECT's acquire was proposed. A takeover's own record
// for a refused client is found the same way by that client's retry, when the
// retry comes before this node's table shows the grant, and is not that.
func TestLateForIgnoresAnAdoptionATakeoverRecorded(t *testing.T) {
	a := newAdoptionSet()
	reassigned, tookOver := key{"default", "r"}, key{"default", "t"}
	a.begin(reassigned, adoptJob{epoch: 3, prev: "n9"})
	a.begin(tookOver, adoptJob{epoch: 4, prev: "n9", takeover: true})
	if !a.lateFor(reassigned) || a.lateFor(tookOver) || a.lateFor(key{"default", "none"}) {
		t.Fatalf("late: reassigned %v, took over %v, none %v; want true, false, false", a.lateFor(reassigned), a.lateFor(tookOver), a.lateFor(key{"default", "none"}))
	}
}

// TestGiveBack: the claim returns to queued with its job, its recording time,
// its attempt and spent counts and its past mark; a newer adoption of the key
// is left alone.
func TestGiveBack(t *testing.T) {
	a := newAdoptionSet()
	k := key{"default", "c"}
	a.begin(k, adoptJob{epoch: 3, prev: "n9"})
	ad, st := a.claim(k)
	if st != adoptionQueued {
		t.Fatal("not claimable")
	}
	if !a.markPast(ad) || a.markPast(ad) {
		t.Fatal("the past mark was not set once")
	}
	if n, mine := a.giveBack(k, ad, true, true); n != 1 || !mine {
		t.Fatalf("attempts %d, mine %v", n, mine)
	}
	select {
	case <-ad.done:
	default:
		t.Fatal("whoever waited on the claim was not released")
	}
	again, st := a.claim(k)
	if st != adoptionQueued || again == ad || again.job != ad.job || again.begun != ad.begun || again.attempts != 1 || again.spent != 1 || !again.past {
		t.Fatalf("after the give-back: %+v, state %v", again, st)
	}
	if a.pastBound() != 1 {
		t.Fatalf("%d adoptions past the bound, want 1", a.pastBound())
	}
	if n, _ := a.giveBack(k, again, true, false); n != 2 {
		t.Fatalf("a worker's give-back that spent nothing: attempts %d, want 2", n)
	}
	again, _ = a.claim(k)
	if again.spent != 1 {
		t.Fatalf("a give-back that spent nothing raised spent to %d", again.spent)
	}
	if n, _ := a.giveBack(k, again, false, false); n != 2 {
		t.Fatalf("a CONNECT's give-back counted an attempt: %d", n)
	}
	running, _ := a.claim(k)
	a.begin(k, adoptJob{epoch: 5, prev: "n8"}) // a newer adoption while the fetch runs
	if _, mine := a.giveBack(k, running, true, false); mine {
		t.Fatal("gave back over a newer adoption")
	}
	if cur := a.m[k]; cur == nil || cur.job.epoch != 5 || cur.running || cur.attempts != 0 {
		t.Fatalf("the newer adoption: %+v", cur)
	}
}

// TestAdmitUnansweredFetchIsTheOldRule: with the injected defect an unanswered
// fetch is acted on as answered, and still counted for what it was.
func TestAdmitUnansweredFetchIsTheOldRule(t *testing.T) {
	fixtureOptions = func(o *Options) { o.AdmitUnansweredFetch = true }
	t.Cleanup(func() { fixtureOptions = nil })
	f := newAdoptionFixtureWith(t, false, func(int) (*store.SessionState, bool) { return nil, false })
	res, err := acquire(t, f.n, "c", nil, 60, false)
	if err != nil || res.State != nil || f.n.svc.adopting.unfinished() != 0 {
		t.Fatalf("in the handshake: %+v, %v, unfinished %d", res, err, f.n.svc.adopting.unfinished())
	}
	f.n.svc.adopting.begin(f.k, adoptJob{epoch: res.Epoch, prev: "n9"})
	f.n.svc.adoptState(f.k)
	if st := f.n.svc.LivenessStats(); st.AdoptInlineUnanswered != 1 || st.AdoptWorkerUnanswered != 1 || st.AdoptRetries != 0 || f.n.svc.adopting.unfinished() != 0 {
		t.Fatalf("counters %+v, unfinished %d", st, f.n.svc.adopting.unfinished())
	}
}

// TestAnAckNobodyWaitsForIsCounted: the previous owner drops the session to
// send it, so an acknowledgement that outlives its pull is state held nowhere.
func TestAnAckNobodyWaitsForIsCounted(t *testing.T) {
	f := newAdoptionFixture(t, false)
	with, _ := json.Marshal(takeoverAck{Req: 1 << 40, State: sessionFor("gone")})
	without, _ := json.Marshal(takeoverAck{Req: 1<<40 + 1})
	f.n.svc.receiveTakeoverAck("n9", with)
	f.n.svc.receiveTakeoverAck("n9", without)
	if st := f.n.svc.LivenessStats(); st.OrphanedAcks != 2 || st.OrphanedAckStates != 1 {
		t.Fatalf("orphaned %d, with state %d; want 2 and 1", st.OrphanedAcks, st.OrphanedAckStates)
	}
}

// TestAdmitUnansweredFetchOnATakeover: the seam's third caller.
func TestAdmitUnansweredFetchOnATakeover(t *testing.T) {
	fixtureOptions = func(o *Options) { o.AdmitUnansweredFetch = true }
	t.Cleanup(func() { fixtureOptions = nil })
	fixtureFallback = func(context.Context, string, string, string) (*store.SessionState, bool) { return nil, false }
	t.Cleanup(func() { fixtureFallback = nil })
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	a, b := c.nodes["n1"], c.nodes["n2"]
	if _, err := acquire(t, a, "x", nil, 300, true); err != nil {
		t.Fatal(err)
	}
	res, err := acquire(t, b, "x", nil, 300, false)
	if err != nil || res.State != nil {
		t.Fatalf("%+v, %v", res, err)
	}
	if st := b.svc.LivenessStats(); st.TakeoverUnanswered != 1 || st.TakeoverEmpty != 0 || b.svc.adopting.unfinished() != 0 {
		t.Fatalf("counters %+v, unfinished %d", st, b.svc.adopting.unfinished())
	}
}

// TestTakeoverDoesNotServeAnUnconfirmedKeptCopy: a CONNECT's takeover whose
// fallback finds a kept copy while a source stays silent is refused, and the
// adoption kept; only an adoption that gives up serves such a copy.
func TestTakeoverDoesNotServeAnUnconfirmedKeptCopy(t *testing.T) {
	keptFound(t, "x")
	_, b, _ := holderCluster(t, func(context.Context, string, string, string) (*store.SessionState, bool) { return nil, false }, nil)
	res, err := acquire(t, b, "x", nil, 300, false)
	if !refusedBusy(err) || res.State != nil {
		t.Fatalf("a takeover with a source silent and a kept copy found: state %v, %v; want Server Busy", res.State, err)
	}
	if b.svc.adopting.unfinished() != 1 || b.sessions.importedCount() != 0 {
		t.Fatalf("adoptions unfinished %d, imports %d; want 1, 0", b.svc.adopting.unfinished(), b.sessions.importedCount())
	}
}
