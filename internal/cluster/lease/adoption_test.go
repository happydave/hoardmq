package lease

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

func TestDecideAdoption(t *testing.T) {
	cases := []struct {
		st    adoptionState
		clean bool
		want  adoptionAction
	}{
		{adoptionNone, false, adoptProceed},
		{adoptionNone, true, adoptProceed},
		{adoptionQueued, false, adoptFetch},
		{adoptionQueued, true, adoptDiscard},
		{adoptionRunning, false, adoptWait},
		{adoptionRunning, true, adoptWait},
	}
	for _, tc := range cases {
		if got := decideAdoption(tc.st, tc.clean); got != tc.want {
			t.Errorf("state %d clean %v: %d, want %d", tc.st, tc.clean, got, tc.want)
		}
	}
}

// TestAdoptionIsClaimedOnce: the state as found and the taking are one step,
// so of any number of claimants exactly one finds it queued.
func TestAdoptionIsClaimedOnce(t *testing.T) {
	a := newAdoptionSet()
	k := key{"default", "c"}
	a.begin(k, adoptJob{epoch: 2, prev: "n9"})
	var wg sync.WaitGroup
	var mu sync.Mutex
	found := map[adoptionState]int{}
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, st := a.claim(k)
			mu.Lock()
			found[st]++
			mu.Unlock()
		}()
	}
	wg.Wait()
	if found[adoptionQueued] != 1 || found[adoptionRunning] != 63 {
		t.Fatalf("claims found %v, want exactly one queued and 63 running", found)
	}
}

// adoptionFixture is one node that owns client "c" with an adoption queued for
// it, and a replica fallback the test controls. The worker queue is not
// enqueued to, so the test decides who reaches the adoption first.
type adoptionFixture struct {
	n       *tnode
	k       key
	state   *store.SessionState
	release func()
	asked   chan struct{}
	// answer, when set, is what the fallback returns once released, given how
	// many times it has been called before.
	answer func(call int) (*store.SessionState, bool)
	calls  atomic.Int64
}

func newAdoptionFixture(t *testing.T, block bool) *adoptionFixture {
	t.Helper()
	return newAdoptionFixtureWith(t, block, nil)
}

// newAdoptionFixtureWith sets answer before the cluster exists, so no
// goroutine of the service reads it while the test writes it.
func newAdoptionFixtureWith(t *testing.T, block bool, answer func(call int) (*store.SessionState, bool)) *adoptionFixture {
	t.Helper()
	f := &adoptionFixture{k: key{"default", "c"}, asked: make(chan struct{}, 64), answer: answer}
	f.state = &store.SessionState{SessionMeta: store.SessionMeta{Namespace: "default", ClientID: "c", Will: &store.WillRecord{}}}
	gate := make(chan struct{})
	f.release = sync.OnceFunc(func() { close(gate) })
	if !block {
		f.release()
	}
	fixtureFallback = func(ctx context.Context, ns, id, prev string) (*store.SessionState, bool) {
		f.asked <- struct{}{}
		select {
		case <-gate:
			if call := int(f.calls.Add(1)) - 1; f.answer != nil {
				return f.answer(call)
			}
			return f.state, true
		case <-ctx.Done():
			return nil, false
		}
	}
	t.Cleanup(func() { fixtureFallback = nil })
	c := newCluster(t, []string{"n1"}, 2*time.Second, nil)
	// Registered after the cluster's own cleanups, so it runs before them: a
	// fetch still blocked must be released before the service is stopped.
	t.Cleanup(f.release)
	f.n = c.nodes["n1"]
	res, err := acquire(t, f.n, "c", nil, 60, true)
	if err != nil {
		t.Fatal(err)
	}
	f.n.svc.adopting.begin(f.k, adoptJob{epoch: res.Epoch, prev: "n9"})
	return f
}

// TestConnectFetchesItsQueuedAdoption: a CONNECT for a lease this node adopted
// carries the adopted state into the handshake, as a takeover does, and the
// worker then finds the adoption gone and imports nothing.
func TestConnectFetchesItsQueuedAdoption(t *testing.T) {
	f := newAdoptionFixture(t, false)
	res, err := acquire(t, f.n, "c", nil, 60, false)
	if err != nil {
		t.Fatal(err)
	}
	if res.State != f.state || !res.Local {
		t.Fatalf("the acquire returned state %v local %v, want the adopted state on a local acquire", res.State, res.Local)
	}
	if res.State.Will != nil {
		t.Fatal("the adopted state kept its Will; the lease timers own it")
	}
	f.n.svc.adoptState(f.k)
	if got := f.n.sessions.importedCount(); got != 0 {
		t.Fatalf("the worker imported %d times after the CONNECT had the adoption", got)
	}
	if n := f.n.svc.adopting.unfinished(); n != 0 {
		t.Fatalf("%d adoptions left unfinished", n)
	}
	if st := f.n.svc.LivenessStats(); st.AdoptedInline != 1 || st.AdoptedByWorker != 0 {
		t.Fatalf("counters %+v", st)
	}
}

// TestConnectWaitsForARunningAdoption: while the worker is fetching, the
// CONNECT is held, and it is admitted only after the state is installed — so
// the import never finds a live connection to close.
func TestConnectWaitsForARunningAdoption(t *testing.T) {
	f := newAdoptionFixture(t, true)
	worker := make(chan struct{})
	go func() { defer close(worker); f.n.svc.adoptState(f.k) }()
	<-f.asked // the worker has claimed the adoption and is fetching

	got := make(chan AcquireResult, 1)
	go func() {
		res, err := acquire(t, f.n, "c", nil, 60, false)
		if err != nil {
			t.Error(err)
		}
		got <- res
	}()
	select {
	case <-got:
		t.Fatal("the CONNECT was admitted while its adoption was still being fetched")
	case <-time.After(200 * time.Millisecond):
	}
	f.release()
	<-worker
	res := <-got
	if f.n.sessions.importedCount() != 1 {
		t.Fatalf("imports %d, want the worker's one", f.n.sessions.importedCount())
	}
	if res.State != nil {
		t.Fatal("the waiting CONNECT also carried the state: it would be imported twice")
	}
	if st := f.n.svc.LivenessStats(); st.AdoptWaited != 1 || st.AdoptedByWorker != 1 {
		t.Fatalf("counters %+v", st)
	}
}

// TestConnectIsRefusedWhenTheAdoptionOutlastsTheHandshake: the wait is bounded
// by the handshake's context, and what the client gets is a refusal it can
// retry, never an early admission.
func TestConnectIsRefusedWhenTheAdoptionOutlastsTheHandshake(t *testing.T) {
	f := newAdoptionFixture(t, true)
	worker := make(chan struct{})
	go func() { defer close(worker); f.n.svc.adoptState(f.k) }()
	<-f.asked
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := f.n.svc.Acquire(ctx, "default", "c", nil, 60, false)
	var r *Refusal
	if !errors.As(err, &r) || r.Reason != packet.ServerBusy {
		t.Fatalf("acquire during a long adoption: %v, want a ServerBusy refusal", err)
	}
	f.release()
	<-worker
	if f.n.sessions.importedCount() != 1 {
		t.Fatalf("imports %d: the refused CONNECT must not stop the adoption", f.n.sessions.importedCount())
	}
	if st := f.n.svc.LivenessStats(); st.AdoptRefused != 1 {
		t.Fatalf("counters %+v", st)
	}
}

// TestCleanStartDiscardsItsAdoption: the state is about to be discarded, so
// nothing is fetched and nothing is imported afterwards over the new session.
func TestCleanStartDiscardsItsAdoption(t *testing.T) {
	f := newAdoptionFixture(t, true) // a fetch would block: there must be none
	res, err := acquire(t, f.n, "c", nil, 60, true)
	if err != nil || res.State != nil {
		t.Fatalf("clean start: %+v %v", res, err)
	}
	select {
	case <-f.asked:
		t.Fatal("a clean start fetched the adopted state")
	default:
	}
	f.n.svc.adoptState(f.k)
	if f.n.sessions.importedCount() != 0 {
		t.Fatal("the worker imported over a clean start's session")
	}
}

// TestConnectConsultsAnAdoptionThatOvertookItsAcquire: a CONNECT that found
// another owner, or none, proposes its acquire; a reassignment applying first
// leaves the lease on this node with an adoption queued, and the acquire then
// applies as a same-owner one. It must still fetch the adopted state, or it is
// admitted on an empty session and evicted when the worker imports.
func TestConnectConsultsAnAdoptionThatOvertookItsAcquire(t *testing.T) {
	f := &adoptionFixture{k: key{"default", "late"}, asked: make(chan struct{}, 8)}
	f.state = &store.SessionState{SessionMeta: store.SessionMeta{Namespace: "default", ClientID: "late"}}
	f.release = func() {}
	fixtureFallback = func(ctx context.Context, ns, id, prev string) (*store.SessionState, bool) { return f.state, true }
	t.Cleanup(func() { fixtureFallback = nil })
	// No lease exists, so the acquire cannot take the local branch: it is
	// proposed. The adoption is recorded as the reassignment would have, at
	// the token the lease is granted at — the log index of the acquire, which
	// the table's probe sees just before the operation applies.
	fixtureProbe = func(string) func(index, level uint64, op Op) {
		return func(index, _ uint64, op Op) {
			if op.Kind == OpAcquire && op.ClientID == f.k.id {
				f.n.svc.adopting.begin(f.k, adoptJob{epoch: index, prev: "n9"})
			}
		}
	}
	t.Cleanup(func() { fixtureProbe = nil })
	c := newCluster(t, []string{"n1"}, 2*time.Second, nil)
	f.n = c.nodes["n1"]
	res, err := acquire(t, f.n, "late", nil, 60, false)
	if err != nil {
		t.Fatal(err)
	}
	if res.Local {
		t.Fatal("the acquire took the local branch; this test is about the proposed one")
	}
	if res.State != f.state {
		t.Fatalf("the acquire returned state %v, want the adopted state", res.State)
	}
	f.n.svc.adoptState(f.k)
	if got := f.n.sessions.importedCount(); got != 0 {
		t.Fatalf("the worker imported %d times after the CONNECT had the adoption", got)
	}
	if st := f.n.svc.LivenessStats(); st.AdoptLate != 1 || st.AdoptedInline != 1 {
		t.Fatalf("counters %+v", st)
	}
	// A key with no adoption pays nothing and counts nothing.
	if _, err := acquire(t, f.n, "fresh", nil, 60, false); err != nil {
		t.Fatal(err)
	}
	if st := f.n.svc.LivenessStats(); st.AdoptLate != 1 {
		t.Fatalf("a fresh client was counted as late: %+v", st)
	}
}

// TestAdoptedStateIsImportedAtTheAdoptedEpoch: the state a worker fetches for
// an adopted lease carries its previous owner's epoch; it is imported at the
// epoch of the lease this node now holds, or every follower that has applied
// the reassignment refuses the whole write (WI 1564).
func TestAdoptedStateIsImportedAtTheAdoptedEpoch(t *testing.T) {
	f := newAdoptionFixture(t, false)
	if f.state.Epoch != 0 {
		t.Fatalf("the fixture's fetched state carries epoch %d; the test wants one below the lease's", f.state.Epoch)
	}
	l, ok := f.n.svc.table.Lookup(f.k.ns, f.k.id)
	if !ok || l.Epoch == 0 {
		t.Fatalf("lease %+v %v", l, ok)
	}
	f.n.svc.adoptState(f.k)
	got, ok := f.n.sessions.Export(f.k.ns, f.k.id)
	if !ok || f.n.sessions.importedCount() != 1 {
		t.Fatalf("imported %d times, state %v", f.n.sessions.importedCount(), got)
	}
	if got.Epoch != l.Epoch {
		t.Fatalf("the adopted state was imported at epoch %d; the lease is at %d", got.Epoch, l.Epoch)
	}
}
