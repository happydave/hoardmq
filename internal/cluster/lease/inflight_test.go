package lease

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/store"
)

// TestAPullServedWhileTheWorkerHoldsTheAdoptionNamesItsOwner: n2's adoption
// worker has claimed the adoption and is fetching when n3's pull is served.
// The claim leaves the adoption where the pass reads it, so the answer names
// n9 and n3 counts a taken entry.
func TestAPullServedWhileTheWorkerHoldsTheAdoptionNamesItsOwner(t *testing.T) {
	var hold atomic.Bool
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	held := make(chan struct{}, 1)
	fb := func(ctx context.Context, ns, id, prev string) (*store.SessionState, bool) {
		if hold.CompareAndSwap(true, false) {
			held <- struct{}{}
			<-release
		}
		return nil, true
	}
	_, n2, n3, _ := chainCluster(t, fb, func(o *Options) {
		heldBack(o)
		if o.Node == "n2" {
			// The test runs the worker's pass itself.
			o.AdoptionDelay = 0
		}
	})
	k := key{"default", "x"}
	job := adoptJob{epoch: adoptionOf(n2, "x").job.epoch, prev: "n9", takeover: true}
	hold.Store(true)
	go n2.svc.adoptState(k)
	select {
	case <-held:
	case <-time.After(5 * time.Second):
		t.Fatal("precondition not reached: n2's worker did not reach its fetch")
	}
	if ad := adoptionOf(n2, "x"); ad == nil || !ad.running || ad.job != job {
		t.Fatalf("precondition not reached: n2's adoption %+v; want it claimed for its fetch", ad)
	}
	_, err := acquire(t, n3, "x", nil, 300, false)
	unblock()
	if !refusedBusy(err) {
		t.Fatalf("retry on n3 while n2's worker held the adoption: %v; want Server Busy", err)
	}
	if g, tr := n2.svc.LivenessStats(), n3.svc.TransitStats(); g.TakeoverAnsweredPending != 1 || g.TakeoverAnsweredPendingNoted != 0 || g.TakeoverAnsweredPendingTaking != 0 || tr.Taken != 1 {
		t.Fatalf("n2 answered pending %d (noted %d, from a takeover's note %d), n3 taken %d; want 1 (0, 0), 1", g.TakeoverAnsweredPending, g.TakeoverAnsweredPendingNoted, g.TakeoverAnsweredPendingTaking, tr.Taken)
	}
}

// acquireHold holds one acquire of x at one stage of AcquireGate once armed.
type acquireHold struct {
	granted bool
	armed   atomic.Bool
	reached chan struct{}
	release chan struct{}
	unblock func()
}

func newAcquireHold(granted bool) *acquireHold {
	h := &acquireHold{granted: granted, reached: make(chan struct{}, 1), release: make(chan struct{})}
	h.unblock = sync.OnceFunc(func() { close(h.release) })
	return h
}

func (h *acquireHold) gate(ns, id string, granted bool) {
	if id != "x" || granted != h.granted || !h.armed.CompareAndSwap(true, false) {
		return
	}
	h.reached <- struct{}{}
	<-h.release
}

func (h *acquireHold) wait(t *testing.T, what string) {
	t.Helper()
	select {
	case <-h.reached:
	case <-time.After(5 * time.Second):
		t.Fatalf("precondition not reached: %s", what)
	}
}

// inFlightCluster is three nodes with x owned by n1, every adoption worker
// held back, and n2's acquires passing through hold. fb is the replica
// fallback.
func inFlightCluster(t *testing.T, hold *acquireHold, fb func(context.Context, string, string, string) (*store.SessionState, bool)) (n1, n2, n3 *tnode) {
	t.Helper()
	fixtureFallback = fb
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
	n1, n2, n3 = c.nodes["n1"], c.nodes["n2"], c.nodes["n3"]
	if _, err := acquire(t, n1, "x", nil, 300, false); err != nil {
		t.Fatal(err)
	}
	ownedBy(t, n2, "x", "n1")
	return n1, n2, n3
}

type acquireOutcome struct {
	res AcquireResult
	err error
}

func acquireAsync(n *tnode, id string, clean bool) chan acquireOutcome {
	done := make(chan acquireOutcome, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		r, err := n.svc.Acquire(ctx, "default", id, nil, 300, clean)
		done <- acquireOutcome{r, err}
	}()
	return done
}

// TestAPullServedBeforeThisNodeSeesItsGrantNamesTheOwnerItTookFrom: n2's
// acquire of x is granted, and n3 takes the lease from n2 and pulls it before
// n2's acquire has gone on from its grant. n2 names n1, the owner its table
// showed when it proposed: n3 counts a taken entry, and with no copy anywhere
// refuses the client and keeps an adoption naming n1.
func TestAPullServedBeforeThisNodeSeesItsGrantNamesTheOwnerItTookFrom(t *testing.T) {
	hold := newAcquireHold(true)
	_, n2, n3 := inFlightCluster(t, hold, foundNowhere)
	hold.armed.Store(true)
	done := acquireAsync(n2, "x", false)
	hold.wait(t, "n2's acquire granted and held")
	ownedBy(t, n3, "x", "n2")
	res, err := acquire(t, n3, "x", nil, 300, false)
	if !refusedBusy(err) {
		t.Fatalf("n3's takeover from n2 while n2's own takeover was in flight: %+v, %v; want Server Busy", res, err)
	}
	if prev := adoptionPrev(n3, "x"); prev != "n1" {
		t.Fatalf("n3's adoption names %q; want n1", prev)
	}
	if g, tr := n2.svc.LivenessStats(), n3.svc.TransitStats(); g.TakeoverAnsweredPending != 1 || g.TakeoverAnsweredPendingNoted != 1 || g.TakeoverAnsweredPendingTaking != 1 || tr.Taken != 1 {
		t.Fatalf("n2 answered pending %d (from a note %d, a takeover's %d), n3 taken %d; want 1 (1, 1), 1", g.TakeoverAnsweredPending, g.TakeoverAnsweredPendingNoted, g.TakeoverAnsweredPendingTaking, tr.Taken)
	}
	hold.unblock()
	<-done
}

// TestAPullServedDuringTheFetchNamesTheOwnerTheGrantWasTakenFrom: n2's table
// showed n1 when n2 proposed, but n9's acquire applied first, so n2's grant
// took the lease from n9. n3's pull, served while n2's fetch is under way,
// names n9.
func TestAPullServedDuringTheFetchNamesTheOwnerTheGrantWasTakenFrom(t *testing.T) {
	hold := newAcquireHold(false)
	var fbHold atomic.Bool
	fbReached := make(chan struct{}, 1)
	fbRelease := make(chan struct{})
	fbUnblock := sync.OnceFunc(func() { close(fbRelease) })
	fb := func(ctx context.Context, ns, id, prev string) (*store.SessionState, bool) {
		if fbHold.CompareAndSwap(true, false) {
			fbReached <- struct{}{}
			<-fbRelease
		}
		return nil, true
	}
	_, n2, n3 := inFlightCluster(t, hold, fb)
	t.Cleanup(fbUnblock)
	hold.armed.Store(true)
	done := acquireAsync(n2, "x", false)
	hold.wait(t, "n2's acquire held before its proposal")
	undeclaredOwner(t, n3, "x")
	ownedBy(t, n2, "x", "n9")
	fbHold.Store(true)
	hold.unblock()
	select {
	case <-fbReached:
	case r := <-done:
		t.Fatalf("precondition not reached: n2's acquire returned (%+v, %v) before its fallback", r.res, r.err)
	case <-time.After(5 * time.Second):
		t.Fatal("precondition not reached: n2's fallback was not reached")
	}
	ownedBy(t, n3, "x", "n2")
	res, err := acquire(t, n3, "x", nil, 300, false)
	fbUnblock()
	if !refusedBusy(err) {
		t.Fatalf("n3's takeover from n2 during n2's fetch: %+v, %v; want Server Busy", res, err)
	}
	if prev := adoptionPrev(n3, "x"); prev != "n9" {
		t.Fatalf("n3's adoption names %q; want n9, the owner n2's grant took the lease from", prev)
	}
	if g := n2.svc.LivenessStats(); g.TakeoverAnsweredPendingTaking != 1 {
		t.Fatalf("n2 answered from a takeover's note %d times; want 1", g.TakeoverAnsweredPendingTaking)
	}
	<-done
}

// TestACleanStartInFlightNamesNobody: a clean start asks nobody for the
// session, so a pull served while it is in flight names nobody, as before.
func TestACleanStartInFlightNamesNobody(t *testing.T) {
	hold := newAcquireHold(true)
	_, n2, n3 := inFlightCluster(t, hold, foundNowhere)
	hold.armed.Store(true)
	done := acquireAsync(n2, "x", true)
	hold.wait(t, "n2's clean-start acquire granted and held")
	ownedBy(t, n3, "x", "n2")
	if _, err := acquire(t, n3, "x", nil, 300, false); err != nil {
		t.Fatalf("n3's takeover from n2 while n2's clean start was in flight: %v; want it served", err)
	}
	if g := n2.svc.LivenessStats(); g.TakeoverAnsweredPending != 0 || g.PendingOwnerNotes != 0 {
		t.Fatalf("n2 answered pending %d, notes held %d; want 0, 0", g.TakeoverAnsweredPending, g.PendingOwnerNotes)
	}
	hold.unblock()
	<-done
	if n := n2.svc.LivenessStats().PendingOwnerNotes; n != 0 {
		t.Fatalf("n2 holds %d notes after its clean start went on from its grant; want 0", n)
	}
}

// TestAnAdoptionIsNamedBeforeATakeoversNote: n2's refused takeover left both
// an adoption and a takeover's note, naming n9. Once the adoption waits on
// another owner, n1 here, a pass names the adoption's owner: it is the newer
// of the two waits.
func TestAnAdoptionIsNamedBeforeATakeoversNote(t *testing.T) {
	_, n2, n3, _ := chainCluster(t, foundNowhere, heldBack)
	ad := adoptionOf(n2, "x")
	if ad == nil || ad.job.prev != "n9" {
		t.Fatalf("precondition not reached: n2's adoption %+v; want one naming n9", ad)
	}
	if owner, takeover := n2.svc.waitNotes.owner(key{"default", "x"}, n2.svc.steady(), n2.svc.o.SealTimeout); owner != "n9" || !takeover {
		t.Fatalf("precondition not reached: n2's note names %q (takeover %v); want n9 from its takeover", owner, takeover)
	}
	n2.svc.adopting.repoint(ad, "n1")
	if _, err := acquire(t, n3, "x", nil, 300, false); !refusedBusy(err) {
		t.Fatalf("n3's takeover from n2: %v; want Server Busy", err)
	}
	if prev := adoptionPrev(n3, "x"); prev != "n1" {
		t.Fatalf("n3's adoption names %q; want n1, the owner n2's adoption waits on", prev)
	}
	if g := n2.svc.LivenessStats(); g.TakeoverAnsweredPending != 1 || g.TakeoverAnsweredPendingNoted != 0 {
		t.Fatalf("n2 answered pending %d, from a note %d; want 1, 0", g.TakeoverAnsweredPending, g.TakeoverAnsweredPendingNoted)
	}
}

// TestAPullServedAfterARefusedImportNamesTheOwner: n2 pulled x's session from
// n1, and its import was refused because n3's later grant had applied there.
// n2 holds nothing; the pass serving n3's pull, after that refusal, names n1,
// which holds the session sealed for n2. With no copy elsewhere, n3 refuses
// the client and keeps an adoption naming n1 rather than serving it fresh.
func TestAPullServedAfterARefusedImportNamesTheOwner(t *testing.T) {
	fixtureFallback = foundNowhere
	t.Cleanup(func() { fixtureFallback = nil })
	fixtureOptions = func(o *Options) {
		heldBack(o)
		switch o.Node {
		case "n2":
			o.TakeoverDelay = 500 * time.Millisecond
		case "n3":
			o.TakeoverTimeout = 3 * time.Second
		}
	}
	t.Cleanup(func() { fixtureOptions = nil })
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	n1, n2, n3 := c.nodes["n1"], c.nodes["n2"], c.nodes["n3"]
	if _, err := acquire(t, n1, "x", nil, 300, false); err != nil {
		t.Fatal(err)
	}
	n1.sessions.put("default", "x", sessionFor("x"))
	ownedBy(t, n2, "x", "n1")
	res, err := acquire(t, n2, "x", nil, 300, false)
	if err != nil || res.State == nil {
		t.Fatalf("precondition not reached: n2's takeover from n1 brought %+v, %v; want n1's session", res, err)
	}
	ownedBy(t, n3, "x", "n2")
	done := acquireAsync(n3, "x", false)
	ownedBy(t, n2, "x", "n3")
	if !n2.svc.AdmitImport("default", "x", res.Epoch)() {
		n2.svc.NoteImportRefused("default", "x", res.Epoch)
	} else {
		t.Fatal("precondition not reached: n2 would still admit the import with n3's grant applied")
	}
	var r acquireOutcome
	select {
	case r = <-done:
		t.Fatalf("precondition not reached: n3's acquire returned (%+v, %v) before n2's import was refused", r.res, r.err)
	default:
	}
	r = <-done
	if !refusedBusy(r.err) {
		t.Fatalf("n3's takeover after n2's refused import: %+v, %v; want Server Busy", r.res, r.err)
	}
	if prev := adoptionPrev(n3, "x"); prev != "n1" {
		t.Fatalf("n3's adoption names %q; want n1, which holds the session sealed for n2", prev)
	}
	if g := n2.svc.LivenessStats(); g.TakeoverAnsweredPendingTaking != 1 || g.ConnectImportRefused != 1 {
		t.Fatalf("n2 answered from a takeover's note %d, refused imports %d; want 1, 1", g.TakeoverAnsweredPendingTaking, g.ConnectImportRefused)
	}
}
