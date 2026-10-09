package routing

import (
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/transport"
)

// TestForgetTimelineDatesEachStepAndWhatItWaitedOn: an owner is lost, kept
// first because it owns leases and then because this node is still adopting,
// and forgotten once that ends. The timeline dates each step in order and
// names what held it last.
func TestForgetTimelineDatesEachStepAndWhatItWaitedOn(t *testing.T) {
	// The proposal's count is held, so a count and stamp made after the
	// in-flight flag cleared would be read as missing.
	f := &servedFixture{owned: map[string]int{"n1": 3, "n2": 1, "n3": 4}, applied: 5, hooked: 5, commit: true,
		metrics: servedMetrics{slow: 20 * time.Millisecond}}
	p, tr := newServedPlane(t, f)
	tr.setState("n1", transport.Up)
	tr.setState("n3", transport.Up)
	tr.deliverFull(t, "n1", []Entry{{Namespace: "default", Filter: "t/x", Version: 1, Count: 1}}, 100)
	tr.deliverFull(t, "n3", nil, 200)
	if got := p.ForgetTimeline(); len(got) != 0 {
		t.Fatalf("timeline %+v before any owner was lost", got)
	}
	tr.setState("n1", transport.Down)

	check := func() {
		t.Helper()
		// Checks a millisecond apart, so the order of the stamps is the
		// order of the steps and not a tie.
		time.Sleep(time.Millisecond)
		p.checkServed()
	}
	check()
	f.set(func(f *servedFixture) {
		f.owned["n1"], f.owned["n3"], f.applied, f.hooked, f.adopting = 0, 7, 10, 10, 3
	})
	check()
	f.set(func(f *servedFixture) { f.adopting, f.facts["n3"] = 0, servedFact{11, 201} })
	tr.deliverDelta(t, "n3", []Entry{{Namespace: "default", Filter: "t/x", Version: 2, Count: 1}}, 201)
	check()
	waitProposed(t, p)
	if hidden := p.Table().HiddenOwners(); len(hidden) != 0 {
		t.Fatalf("precondition: n1 is still hidden (%v) after every reason to keep it ended", hidden)
	}

	got := p.ForgetTimeline()
	if len(got) != 1 || got[0].Owner != "n1" {
		t.Fatalf("timeline %+v; want one event, for n1", got)
	}
	e := got[0]
	if e.HiddenNs == 0 || e.HiddenNs >= e.EmptiedNs || e.EmptiedNs >= e.ServedHereNs || e.ServedHereNs > e.ForgottenNs {
		t.Fatalf("steps out of order or missing: hidden %d, emptied %d, served here %d, forgotten %d", e.HiddenNs, e.EmptiedNs, e.ServedHereNs, e.ForgottenNs)
	}
	if e.ProposedNs < e.ServedHereNs {
		t.Fatalf("this node's fact dated %d, before it was served here at %d, or not at all", e.ProposedNs, e.ServedHereNs)
	}
	if e.ReturnedNs != 0 {
		t.Fatalf("n1 was forgotten, not returned: %+v", e)
	}
	leases, adopting := e.LastHeldNs[keepLeases], e.LastHeldNs[keepAdopting]
	if leases == 0 || adopting == 0 || adopting <= leases || adopting >= e.ForgottenNs {
		t.Fatalf("last held %v, forgotten %d; want leases, then adopting last, both before the forget", e.LastHeldNs, e.ForgottenNs)
	}
	// Ended events stay readable once nothing is hidden.
	check()
	if again := p.ForgetTimeline(); len(again) != 1 || again[0].ForgottenNs != e.ForgottenNs {
		t.Fatalf("after a further check the timeline is %+v", again)
	}
}

// TestAProposalIsInFlightUntilItIsStamped: a proposal that has returned
// stays in flight while its stamp waits on the timeline, so a reader that
// finds none in flight finds the stamp.
func TestAProposalIsInFlightUntilItIsStamped(t *testing.T) {
	f := &servedFixture{owned: map[string]int{"n1": 3, "n2": 1, "n3": 4}, applied: 5, hooked: 5, commit: true, hold: make(chan struct{})}
	p, tr := newServedPlane(t, f)
	release := sync.OnceFunc(func() { close(f.hold) })
	t.Cleanup(release)
	tr.setState("n1", transport.Up)
	tr.deliverFull(t, "n1", []Entry{{Namespace: "default", Filter: "t/x", Version: 1, Count: 1}}, 100)
	tr.setState("n1", transport.Down)
	f.set(func(f *servedFixture) { f.owned["n1"], f.applied, f.hooked = 0, 10, 10 })
	p.checkServed()
	if got := p.ForgetTimeline(); len(got) != 1 || got[0].ServedHereNs == 0 {
		t.Fatalf("precondition: timeline %+v; want n1 served here", got)
	}

	// The test holds the timeline's lock, so the stamp waits on it.
	p.forgets.mu.Lock()
	unlock := sync.OnceFunc(p.forgets.mu.Unlock)
	t.Cleanup(unlock)
	release()
	deadline := time.Now().Add(5 * time.Second)
	for len(f.proposals()) == 0 {
		if time.Now().After(deadline) {
			unlock()
			t.Fatal("precondition: this node's fact was never proposed")
		}
		time.Sleep(time.Millisecond)
	}
	for end := time.Now().Add(50 * time.Millisecond); time.Now().Before(end); time.Sleep(time.Millisecond) {
		p.mu.Lock()
		busy := p.proposing
		p.mu.Unlock()
		if !busy {
			unlock()
			t.Fatal("no proposal in flight while its stamp waits on the timeline")
		}
	}
	unlock()
	waitProposed(t, p)
	if got := p.ForgetTimeline(); len(got) != 1 || got[0].ProposedNs < got[0].ServedHereNs {
		t.Fatalf("timeline %+v; want this node's fact dated once no proposal is in flight", got)
	}
}

// TestForgetTimelineMarksAnOwnerThatCameBack: an owner hidden and then up
// again, never forgotten, is dated as returned.
func TestForgetTimelineMarksAnOwnerThatCameBack(t *testing.T) {
	f := &servedFixture{owned: map[string]int{"n1": 3, "n2": 1, "n3": 4}, applied: 5, hooked: 5}
	p, tr := newServedPlane(t, f)
	tr.setState("n1", transport.Up)
	tr.setState("n3", transport.Up)
	tr.deliverFull(t, "n1", []Entry{{Namespace: "default", Filter: "t/x", Version: 1, Count: 1}}, 100)
	tr.setState("n1", transport.Down)
	p.checkServed()
	tr.setState("n1", transport.Up)
	tr.deliverFull(t, "n1", []Entry{{Namespace: "default", Filter: "t/x", Version: 3, Count: 1}}, 101)
	if hidden := p.Table().HiddenOwners(); len(hidden) != 0 {
		t.Fatalf("precondition: n1 is still hidden (%v) after it came back", hidden)
	}
	time.Sleep(time.Millisecond)
	p.checkServed()
	got := p.ForgetTimeline()
	if len(got) != 1 || got[0].ReturnedNs <= got[0].HiddenNs || got[0].ForgottenNs != 0 {
		t.Fatalf("timeline %+v; want n1 hidden, then returned, never forgotten", got)
	}
}
