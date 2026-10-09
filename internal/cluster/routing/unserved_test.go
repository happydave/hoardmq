package routing

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/transport"
)

func TestOwnerRoutesKnown(t *testing.T) {
	idle := servedSelf{index: 40}
	m := func(owns int, up bool, served uint64, routes bool) servedMember {
		return servedMember{name: "n3", owns: owns, up: up, served: served, routes: routes}
	}
	for _, c := range []struct {
		name    string
		owns    int
		known   bool
		emptied uint64
		settled uint64
		self    servedSelf
		members []servedMember
		want    bool
		why     string
	}{
		{"the lease state cannot be read", 0, false, 40, 0, idle, nil, false, keepUnknown},
		{"the owner holds a lease", 2, true, 40, 40, idle, []servedMember{m(5, true, 40, true)}, false, keepLeases},
		{"nothing was ever taken from the owner", 0, true, 0, 0, idle, []servedMember{m(5, true, 0, false)}, true, ""},
		{"emptied, a member's fact one below the index", 0, true, 40, 0, idle, []servedMember{m(5, true, 39, true)}, false, keepUnsynced},
		{"emptied, a member's fact at the index, its routes not held", 0, true, 40, 0, idle, []servedMember{m(5, true, 40, false)}, false, keepUnsynced},
		{"emptied, the member down with no fact", 0, true, 40, 0, idle, []servedMember{m(5, false, 0, false)}, false, keepUnreachable},
		{"emptied, this node adopting", 0, true, 40, 0, servedSelf{index: 40, adopting: 1}, []servedMember{m(5, true, 40, true)}, false, keepAdopting},
		{"emptied, this node's hooks behind the index", 0, true, 40, 0, servedSelf{index: 39}, []servedMember{m(5, true, 40, true)}, false, keepUnsynced},
		{"settled one below the emptied index is judged again", 0, true, 40, 39, idle, []servedMember{m(5, true, 39, true)}, false, keepUnsynced},
		{"settled at the emptied index", 0, true, 40, 40, idle, []servedMember{m(5, true, 0, false)}, true, ""},
		{"emptied, every owning member's fact and routes held", 0, true, 40, 0, idle, []servedMember{m(5, true, 41, true), m(0, false, 0, false)}, true, ""},
	} {
		got, why := ownerRoutesKnown(c.owns, c.known, c.emptied, c.settled, c.self, c.members)
		if got != c.want || why != c.why {
			t.Errorf("%s: known=%v (%q), want %v (%q)", c.name, got, why, c.want, c.why)
		}
	}
}

// unservedPlane is n2 restarted beside n1: n3 is up and has synced, n1 never
// has, and n1 holds three leases.
func unservedPlane(t *testing.T) (*Plane, *stubTransport, *servedFixture) {
	t.Helper()
	f := &servedFixture{owned: map[string]int{"n1": 3, "n2": 0, "n3": 4}, applied: 5, hooked: 5,
		emptied: map[string]uint64{}}
	p, tr := newServedPlane(t, f)
	tr.setState("n3", transport.Up)
	tr.deliverFull(t, "n3", []Entry{{Namespace: "default", Filter: "t/other", Version: 1, Count: 1}}, 200)
	return p, tr, f
}

// emptyN1 applies the reassignment of n1's leases to n3 at index 10, with this
// node's hooks run to it.
func emptyN1(f *servedFixture) {
	f.set(func(f *servedFixture) {
		f.owned["n1"], f.owned["n3"], f.emptied["n1"], f.applied, f.hooked = 0, 7, 10, 10, 10
	})
}

func refusedNow(t *testing.T, p *Plane, step string) {
	t.Helper()
	p.refreshOwnerKnowledge()
	if _, err := p.Forward(qos1("t/x")); !errors.Is(err, ErrRoutesUnavailable) {
		t.Fatalf("%s: a QoS 1 publish was %v, want it refused", step, err)
	}
	if p.OwnersKnown() {
		t.Fatalf("%s: every owner's routes read as known", step)
	}
}

func acceptedNow(t *testing.T, p *Plane, step string) {
	t.Helper()
	p.refreshOwnerKnowledge()
	if _, err := p.Forward(qos1("t/x")); err != nil {
		t.Fatalf("%s: a QoS 1 publish was refused: %v", step, err)
	}
	if !p.OwnersKnown() {
		t.Fatalf("%s: an owner's routes still read as unknown", step)
	}
}

// TestAnEmptiedOwnerStaysUnknownUntilItsSessionsAreServed: n1's leases moved
// to n3 do not make n1's routes known to a node that never had them; n3's
// fact at the emptied index and n3's routes up to its mark do.
func TestAnEmptiedOwnerStaysUnknownUntilItsSessionsAreServed(t *testing.T) {
	p, tr, f := unservedPlane(t)
	refusedNow(t, p, "n1 holds leases")

	emptyN1(f)
	refusedNow(t, p, "n1 emptied, n3 adopting with no fact")
	if got := f.metrics.get("hoardmq_route_owners_unserved{" + keepUnsynced + "}"); got != 1 {
		t.Fatalf("hoardmq_route_owners_unserved{unsynced} = %v, want 1", got)
	}

	f.set(func(f *servedFixture) { f.facts["n3"] = servedFact{9, 202} })
	refusedNow(t, p, "n3's fact below the emptied index")
	f.set(func(f *servedFixture) { f.facts["n3"] = servedFact{11, 202} })
	refusedNow(t, p, "n3's fact at the index, its routes held only to 200 of 202")
	tr.deliverDelta(t, "n3", []Entry{{Namespace: "default", Filter: "t/x", Version: 2, Count: 1}}, 201)
	refusedNow(t, p, "n3's routes held to 201 of 202")

	tr.deliverDelta(t, "n3", []Entry{{Namespace: "default", Filter: "t/y", Version: 3, Count: 1}}, 202)
	acceptedNow(t, p, "n3's fact and its routes to its mark held")
	if got := f.metrics.get("hoardmq_route_owners_unserved{" + keepUnsynced + "}"); got != 0 {
		t.Fatalf("hoardmq_route_owners_unserved{unsynced} = %v once n1's sessions are served, want 0", got)
	}

	// A later gap in n3's route updates proves nothing about n3's routes from
	// then on; n1's sessions were already found served at its emptied index.
	tr.deliverDelta(t, "n3", []Entry{{Namespace: "default", Filter: "t/z", Version: 4, Count: 1}}, 210)
	if p.routesHeld("n3", 202) {
		t.Fatal("precondition: the gap did not break n3's route proof")
	}
	acceptedNow(t, p, "a gap in n3's routes after n1 settled")

	// n1's sessions were served; an adoption this node starts afterwards is
	// another owner's.
	f.set(func(f *servedFixture) { f.adopting = 1 })
	acceptedNow(t, p, "this node adopting after n1 settled")
}

// TestAnEmptiedOwnerStaysUnknownWhileThisNodeAdopts: this node is n1's
// adopter; n3 has said its own part. Its own unfinished adoption keeps n1's
// routes unknown here.
func TestAnEmptiedOwnerStaysUnknownWhileThisNodeAdopts(t *testing.T) {
	p, tr, f := unservedPlane(t)
	emptyN1(f)
	f.set(func(f *servedFixture) { f.adopting, f.facts["n3"] = 1, servedFact{11, 201} })
	tr.deliverDelta(t, "n3", []Entry{{Namespace: "default", Filter: "t/y", Version: 2, Count: 1}}, 201)
	refusedNow(t, p, "this node adopting n1's sessions")
	if got := f.metrics.get("hoardmq_route_owners_unserved{" + keepAdopting + "}"); got != 1 {
		t.Fatalf("hoardmq_route_owners_unserved{adopting} = %v, want 1", got)
	}
	f.set(func(f *servedFixture) { f.adopting = 0 })
	acceptedNow(t, p, "this node's adoptions finished")
}

// TestAnEmptiedOwnersRefusalEndsAtTheServedCheck: once a member's routes
// reach its mark, the served check that wakes on them ends the refusal, with
// no peer event and no sync tick (both an hour away in the fixture).
func TestAnEmptiedOwnersRefusalEndsAtTheServedCheck(t *testing.T) {
	p, tr, f := unservedPlane(t)
	emptyN1(f)
	f.set(func(f *servedFixture) { f.facts["n3"] = servedFact{11, 201} })
	refusedNow(t, p, "n3's routes not held to its mark")
	tr.deliverDelta(t, "n3", []Entry{{Namespace: "default", Filter: "t/y", Version: 2, Count: 1}}, 201)
	if p.OwnersKnown() {
		t.Fatal("precondition: the routes' arrival alone re-judged the owners")
	}
	p.checkServed()
	if _, err := p.Forward(qos1("t/x")); err != nil || !p.OwnersKnown() {
		t.Fatalf("after one served check: publish %v, owners known %v; want it accepted", err, p.OwnersKnown())
	}
}

// TestANodeProposesItsFactForAnOwnerItNeverHadRoutesFrom: with no owner
// hidden here, an emptied owner this node never synced from still makes it
// say, once, that its own adoptions are finished; a failed proposal is made
// again, and an adoption here holds it back.
func TestANodeProposesItsFactForAnOwnerItNeverHadRoutesFrom(t *testing.T) {
	p, _, f := unservedPlane(t)
	f.set(func(f *servedFixture) { f.emptied["n1"], f.applied, f.hooked = 10, 10, 10 })
	p.checkServed()
	waitProposed(t, p)
	if got := f.proposals(); len(got) != 0 {
		t.Fatalf("proposals %v while n1 holds leases; want none", got)
	}
	emptyN1(f)
	f.set(func(f *servedFixture) { f.adopting, f.fail = 1, errors.New("no leader") })
	p.checkServed()
	waitProposed(t, p)
	if got := f.proposals(); len(got) != 0 {
		t.Fatalf("proposals %v while this node is adopting; want none", got)
	}
	f.set(func(f *servedFixture) { f.adopting = 0 })
	p.checkServed()
	waitProposed(t, p)
	if got := f.proposals(); len(got) != 1 || got[0].index != 10 {
		t.Fatalf("proposals %v; want this node's fact at index 10", got)
	}
	f.set(func(f *servedFixture) { f.fail = nil })
	p.checkServed()
	waitProposed(t, p)
	if got := f.proposals(); len(got) != 2 || got[1].index != 10 {
		t.Fatalf("proposals %v; want the failed one made again", got)
	}
	p.checkServed()
	p.checkServed()
	waitProposed(t, p)
	if got := f.proposals(); len(got) != 2 {
		t.Fatalf("proposals %v; want none once one is committed", got)
	}
	if hidden := p.Table().HiddenOwners(); len(hidden) != 0 {
		t.Fatalf("precondition: hidden owners %v", hidden)
	}
}

// TestAStaleOwnerJudgementIsNotStoredOverANewerOne: an evaluation that found
// n1's sessions served is held between its judgement and its store while n1
// holds a lease again and a second evaluation runs. The verdict left is the
// second's.
func TestAStaleOwnerJudgementIsNotStoredOverANewerOne(t *testing.T) {
	p, tr, f := unservedPlane(t)
	emptyN1(f)
	f.set(func(f *servedFixture) { f.facts["n3"] = servedFact{11, 201} })
	tr.deliverDelta(t, "n3", []Entry{{Namespace: "default", Filter: "t/y", Version: 2, Count: 1}}, 201)

	held, release := make(chan struct{}), make(chan struct{})
	var judged atomic.Int32
	p.afterJudge = func() {
		if judged.Add(1) == 1 {
			close(held)
			<-release
		}
	}
	first := make(chan struct{})
	go func() { defer close(first); p.refreshOwnerKnowledge() }()
	<-held
	f.set(func(f *servedFixture) { f.owned["n1"] = 1 })
	second := make(chan struct{})
	go func() { defer close(second); p.refreshOwnerKnowledge() }()
	select {
	case <-second:
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	<-first
	<-second
	if p.OwnersKnown() {
		t.Fatal("n1 holds a lease again, but the evaluation that read it before that stored its verdict last")
	}
}

// TestAForgottenOwnerStaysKnownWhenAMembersRouteProofBreaks: a hidden owner
// forgotten on the served rule has no sync record left, so it is judged as an
// unsynced owner from then on, and settled at its emptied index by the
// judgement after the forget: a later gap in a member's route updates does not
// bring the refusal back.
func TestAForgottenOwnerStaysKnownWhenAMembersRouteProofBreaks(t *testing.T) {
	f := &servedFixture{owned: map[string]int{"n1": 3, "n2": 0, "n3": 4}, applied: 5, hooked: 5,
		emptied: map[string]uint64{}, commit: true}
	p, tr := newServedPlane(t, f)
	tr.setState("n1", transport.Up)
	tr.setState("n3", transport.Up)
	tr.deliverFull(t, "n1", []Entry{{Namespace: "default", Filter: "t/x", Version: 1, Count: 1}}, 100)
	tr.deliverFull(t, "n3", nil, 200)
	tr.setState("n1", transport.Down)
	emptyN1(f)
	f.set(func(f *servedFixture) { f.facts["n3"] = servedFact{11, 201} })
	tr.deliverDelta(t, "n3", []Entry{{Namespace: "default", Filter: "t/x", Version: 2, Count: 1}}, 201)
	p.checkServed()
	waitProposed(t, p)
	p.checkServed()
	if hidden := p.Table().HiddenOwners(); len(hidden) != 0 {
		t.Fatalf("precondition: n1 not forgotten (hidden %v)", hidden)
	}
	if p.syncedFrom("n1") {
		t.Fatal("precondition: n1's sync record outlived its forget")
	}
	tr.deliverDelta(t, "n3", []Entry{{Namespace: "default", Filter: "t/z", Version: 3, Count: 1}}, 210)
	if p.routesHeld("n3", 201) {
		t.Fatal("precondition: the gap did not break n3's route proof")
	}
	acceptedNow(t, p, "n1 forgotten, then a gap in n3's routes")
}

// TestAServedCheckCountsNoLeasesOnceItsFactIsIn: with n1's routes known and
// this node's fact at n1's emptied index in the log, a served check reads no
// lease count for n1.
func TestAServedCheckCountsNoLeasesOnceItsFactIsIn(t *testing.T) {
	p, tr, f := unservedPlane(t)
	f.set(func(f *servedFixture) { f.commit = true })
	emptyN1(f)
	f.set(func(f *servedFixture) { f.facts["n3"] = servedFact{11, 201} })
	tr.deliverDelta(t, "n3", []Entry{{Namespace: "default", Filter: "t/y", Version: 2, Count: 1}}, 201)
	p.checkServed()
	waitProposed(t, p)
	acceptedNow(t, p, "n1's sessions served and this node's fact committed")
	var own uint64
	f.set(func(f *servedFixture) { own = f.facts["n2"].index })
	if own != 10 {
		t.Fatalf("precondition: this node's fact at %d, want 10", own)
	}
	f.set(func(f *servedFixture) { f.countedFor = map[string]int{} })
	p.checkServed()
	if n := f.counted("n1"); n != 0 {
		t.Fatalf("a served check counted n1's leases %d times with no fact owed and n1 known", n)
	}
}
