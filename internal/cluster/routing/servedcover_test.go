package routing

import (
	"errors"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/transport"
)

// coverPlane is n2 after n1's loss: n1 emptied at index 10, n3 owns leases,
// n2's own part done, and n1's filter t/x hidden.
func coverPlane(t *testing.T) (*Plane, *stubTransport, *servedFixture) {
	t.Helper()
	f := &servedFixture{owned: map[string]int{"n1": 3, "n2": 1, "n3": 4}, applied: 5, hooked: 5, commit: true}
	p, tr := newServedPlane(t, f)
	tr.setState("n1", transport.Up)
	tr.setState("n3", transport.Up)
	tr.deliverFull(t, "n1", []Entry{{Namespace: "default", Filter: "t/x", Version: 1, Count: 1}}, 100)
	tr.deliverFull(t, "n3", nil, 200)
	tr.setState("n1", transport.Down)
	f.set(func(f *servedFixture) { f.owned["n1"], f.owned["n3"], f.applied, f.hooked = 0, 7, 10, 10 })
	return p, tr, f
}

// TestTheRouteProofReadsAMembersHigherMark: a member whose cover came after
// its finished index carries its routes up to the cover's mark, so a node
// holding them only to the index's mark keeps the lost owner hidden.
func TestTheRouteProofReadsAMembersHigherMark(t *testing.T) {
	p, tr, f := coverPlane(t)
	f.set(func(f *servedFixture) {
		f.facts["n3"] = servedFact{11, 202}
		f.covers = map[string]servedFact{"n3": {15, 204}}
	})
	tr.deliverDelta(t, "n3", []Entry{{Namespace: "default", Filter: "t/y", Version: 2, Count: 1}}, 201)
	tr.deliverDelta(t, "n3", []Entry{{Namespace: "default", Filter: "t/z", Version: 3, Count: 1}}, 202)
	p.checkServed()
	waitProposed(t, p)
	if hidden := p.Table().HiddenOwners(); len(hidden) != 1 {
		t.Fatalf("hidden owners %v with n3's routes held to 202 of its cover's 204; want n1 kept", hidden)
	}
	if got := f.metrics.get("hoardmq_route_owners_hidden{" + keepUnsynced + "}"); got < 1 {
		t.Fatalf("hoardmq_route_owners_hidden{unsynced} = %v, want n1 kept for n3's routes", got)
	}
	tr.deliverDelta(t, "n3", []Entry{{Namespace: "default", Filter: "t/w", Version: 4, Count: 1}}, 203)
	tr.deliverDelta(t, "n3", []Entry{{Namespace: "default", Filter: "t/v", Version: 5, Count: 1}}, 204)
	p.checkServed()
	if hidden := p.Table().HiddenOwners(); len(hidden) != 0 {
		t.Fatalf("hidden owners %v with n3's routes held to its cover's mark", hidden)
	}
}

// TestACoverAloneForgetsNothing: a member's cover past the emptied index
// does not stand in for its finished index.
func TestACoverAloneForgetsNothing(t *testing.T) {
	p, tr, f := coverPlane(t)
	f.set(func(f *servedFixture) {
		f.facts["n3"] = servedFact{9, 202}
		f.covers = map[string]servedFact{"n3": {15, 202}}
	})
	tr.deliverDelta(t, "n3", []Entry{{Namespace: "default", Filter: "t/y", Version: 2, Count: 1}}, 201)
	tr.deliverDelta(t, "n3", []Entry{{Namespace: "default", Filter: "t/z", Version: 3, Count: 1}}, 202)
	p.checkServed()
	waitProposed(t, p)
	if _, err := p.Forward(qos1("t/x")); !errors.Is(err, ErrRoutesUnavailable) {
		t.Fatalf("publish to the lost owner's filter with n3's finished index below it: %v, want refused", err)
	}
	if got := f.metrics.get("hoardmq_route_owners_hidden{" + keepUnsynced + "}"); got < 1 {
		t.Fatalf("hoardmq_route_owners_hidden{unsynced} = %v, want n1 kept", got)
	}
}

// coversProposed is every cover the plane proposed, read under the fixture's lock.
func (f *servedFixture) coversProposed() []servedFact {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]servedFact(nil), f.covered...)
}

// TestAFailedCoverIsProposedAgain: a cover that does not commit is counted
// and proposed again at the next check, and once committed is not.
func TestAFailedCoverIsProposedAgain(t *testing.T) {
	f := &servedFixture{owned: map[string]int{"n2": 1}, coverDue: true, dueGrant: 15, coverFail: errors.New("no leader")}
	p, _ := newServedPlane(t, f)
	p.checkServed()
	waitCovered(t, p)
	p.checkServed()
	waitCovered(t, p)
	if got := f.coversProposed(); len(got) != 2 {
		t.Fatalf("covers proposed %v; want a failed one made again", got)
	}
	f.set(func(f *servedFixture) { f.coverFail = nil })
	p.checkServed()
	waitCovered(t, p)
	p.checkServed()
	waitCovered(t, p)
	if got := f.coversProposed(); len(got) != 3 {
		t.Fatalf("covers proposed %v; want one more, then none once committed", got)
	}
	if got := f.metrics.get("hoardmq_route_cover_proposals_total{failed}"); got != 2 {
		t.Errorf("failed covers counted %v, want 2", got)
	}
	if got := f.metrics.get("hoardmq_route_cover_proposals_total{committed}"); got != 1 {
		t.Errorf("committed covers counted %v, want 1", got)
	}
}

// TestACoverIsDatedOnTheForgetTimeline: a committed cover is stamped on the
// event of each owner hidden at the time, for the failover artifact.
func TestACoverIsDatedOnTheForgetTimeline(t *testing.T) {
	p, _, f := coverPlane(t)
	p.checkServed()
	waitProposed(t, p)
	if got := p.ForgetTimeline(); len(got) != 1 || got[0].CoveredNs != 0 {
		t.Fatalf("timeline %+v; want n1's event with no cover yet", got)
	}
	before := time.Now().UnixNano()
	f.set(func(f *servedFixture) { f.coverDue, f.dueGrant = true, 15 })
	p.checkServed()
	waitCovered(t, p)
	if got := p.ForgetTimeline(); len(got) != 1 || got[0].CoveredNs < before {
		t.Fatalf("timeline %+v; want n1's event stamped with the cover's commit", got)
	}
}

// waitCovered returns once no cover proposal is in flight.
func waitCovered(t *testing.T, p *Plane) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		p.mu.Lock()
		busy := p.coverProposing
		p.mu.Unlock()
		if !busy {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("a proposal of this node's cover never returned")
}

// TestACoverIsProposedWhenDueWithNoOwnerHidden: the cover is about this
// node's takeovers, so it is proposed on a check with no owner hidden, once
// per grant, with the mark read when it is proposed.
func TestACoverIsProposedWhenDueWithNoOwnerHidden(t *testing.T) {
	f := &servedFixture{owned: map[string]int{"n2": 1}, dueGrant: 15}
	p, _ := newServedPlane(t, f)
	p.checkServed()
	waitCovered(t, p)
	if got := f.coversProposed(); len(got) != 0 {
		t.Fatalf("covers proposed %v with none due", got)
	}
	f.set(func(f *servedFixture) { f.coverDue, f.dueGrant = true, 15 })
	p.checkServed()
	waitCovered(t, p)
	p.checkServed()
	waitCovered(t, p)
	if got := f.coversProposed(); len(got) != 1 || got[0].index != 15 {
		t.Fatalf("covers proposed %v; want one at 15", got)
	}
	if got := f.metrics.get("hoardmq_route_cover_proposals_total{committed}"); got != 1 {
		t.Fatalf("committed covers counted %v, want 1", got)
	}
	f.set(func(f *servedFixture) { f.dueGrant = 18 })
	p.checkServed()
	waitCovered(t, p)
	if got := f.coversProposed(); len(got) != 2 || got[1].index != 18 {
		t.Fatalf("covers proposed %v; want a raised one at 18", got)
	}
}
