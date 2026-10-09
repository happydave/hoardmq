package routing

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/transport"
)

func TestServedElsewhere(t *testing.T) {
	idle := servedSelf{index: 40}
	m := func(owns int, up bool, served uint64, routes bool) servedMember {
		return servedMember{name: "p", owns: owns, up: up, served: served, routes: routes}
	}
	for _, c := range []struct {
		name    string
		owns    int
		known   bool
		zeroAt  uint64
		self    servedSelf
		members []servedMember
		want    bool
		why     string
	}{
		{"the lease state cannot be read", 0, false, 40, idle, nil, false, keepUnknown},
		{"no index recorded yet", 0, true, 0, idle, nil, false, keepUnknown},
		{"the owner still owns leases", 3, true, 40, idle, nil, false, keepLeases},
		{"this node has not applied up to the index", 0, true, 40, servedSelf{index: 39}, nil, false, keepUnsynced},
		{"this node is adopting", 0, true, 40, servedSelf{index: 40, adopting: 1}, nil, false, keepAdopting},
		{"a member up with no fact", 0, true, 40, idle, []servedMember{m(5, true, 0, false)}, false, keepUnsynced},
		{"a member down with no fact", 0, true, 40, idle, []servedMember{m(5, false, 0, false)}, false, keepUnreachable},
		{"a member's fact predates the index", 0, true, 40, idle, []servedMember{m(5, true, 39, true)}, false, keepUnsynced},
		{"a member's fact at the index, its routes not held, up", 0, true, 40, idle, []servedMember{m(5, true, 40, false)}, false, keepUnsynced},
		{"a member's fact at the index, its routes not held, down", 0, true, 40, idle, []servedMember{m(5, false, 41, false)}, false, keepUnreachable},
		{"a member down with its fact and routes held", 0, true, 40, idle, []servedMember{m(5, false, 41, true)}, true, ""},
		{"a member that owns nothing is not waited for, down or silent", 0, true, 40, idle, []servedMember{m(0, false, 0, false), m(0, true, 0, false)}, true, ""},
		{"every owning member's fact and routes held", 0, true, 40, idle, []servedMember{m(5, true, 40, true), m(2, true, 90, true)}, true, ""},
		{"alone in the cluster", 0, true, 40, idle, nil, true, ""},
	} {
		got, why := servedElsewhere(c.owns, c.known, c.zeroAt, c.self, c.members)
		if got != c.want || why != c.why {
			t.Errorf("%s: forget=%v (%q), want %v (%q)", c.name, got, why, c.want, c.why)
		}
	}
}

// servedFact is one node's served fact as the fixture's lease view holds it.
type servedFact struct{ index, routes uint64 }

// servedFixture is a plane on a stub transport whose lease view the test
// moves by hand, judged only when the test says so.
type servedFixture struct {
	metrics  servedMetrics
	mu       sync.Mutex
	owned    map[string]int
	applied  uint64 // the lease index applied, hooks run or not
	hooked   uint64 // … with its hooks run
	adopting int
	// emptied is each owner's emptied index (lease.Table.LostAt); zero for
	// an owner nothing was taken from. countedFor, when set, counts the
	// lease-table counts read per owner.
	emptied    map[string]uint64
	countedFor map[string]int
	facts      map[string]servedFact
	// proposed is every fact the plane proposed; commit, when set, records
	// it in the lease view as the log would.
	proposed []servedFact
	commit   bool
	fail     error
	// through is the highest grant of a session in transit this node took
	// and now holds (lease.Service.AdoptedThrough).
	through uint64
	// hold, when set, keeps each proposal waiting until it is closed.
	hold chan struct{}
	// covers is each node's cover in the lease view; coverDue and dueGrant
	// what this node's lease service says of its own; covered every cover
	// the plane proposed, recorded in covers as the log would.
	covers   map[string]servedFact
	coverDue bool
	dueGrant uint64
	covered  []servedFact
	// coverFail, when set, is what each cover proposal returns, uncommitted.
	coverFail error
}

// servedMetrics keeps the last value of each gauge and the sum of each
// counter, keyed by name and the one label these metrics carry. slow holds
// each count of a served proposal's outcome for that long before it lands.
type servedMetrics struct {
	mu   sync.Mutex
	v    map[string]float64
	slow time.Duration
}

func (m *servedMetrics) key(name string, labels map[string]string) string {
	return name + "{" + labels["reason"] + labels["outcome"] + "}"
}

func (m *servedMetrics) Add(name string, labels map[string]string, delta float64) {
	if name == "hoardmq_route_served_proposals_total" {
		time.Sleep(m.slow)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.v == nil {
		m.v = map[string]float64{}
	}
	m.v[m.key(name, labels)] += delta
}

func (m *servedMetrics) Set(name string, labels map[string]string, value float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.v == nil {
		m.v = map[string]float64{}
	}
	m.v[m.key(name, labels)] = value
}

func (m *servedMetrics) get(key string) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.v[key]
}

func (f *servedFixture) set(fn func(*servedFixture)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(f)
}

func (f *servedFixture) counted(node string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.countedFor[node]
}

func (f *servedFixture) proposals() []servedFact {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]servedFact(nil), f.proposed...)
}

func newServedPlane(t *testing.T, f *servedFixture) (*Plane, *stubTransport) {
	t.Helper()
	if f.facts == nil {
		f.facts = map[string]servedFact{}
	}
	tr := newStubTransport("n1", "n3")
	p := New(Options{Node: "n2", Transport: tr, SyncInterval: time.Hour, ServedInterval: time.Hour, Logger: quiet, Metrics: &f.metrics,
		Members: func() []string { return []string{"n1", "n2", "n3"} },
		OwnedSessions: func(node string) (int, bool) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.countedFor != nil {
				f.countedFor[node]++
			}
			return f.owned[node], true
		},
		AdoptionState: func() (uint64, int, bool) {
			f.mu.Lock()
			defer f.mu.Unlock()
			return f.hooked, f.adopting, true
		},
		LeaseIndex: func() uint64 {
			f.mu.Lock()
			defer f.mu.Unlock()
			return f.applied
		},
		EmptiedAt: func(node string) uint64 {
			f.mu.Lock()
			defer f.mu.Unlock()
			return f.emptied[node]
		},
		ServedFact: func(node string) (uint64, uint64) {
			f.mu.Lock()
			defer f.mu.Unlock()
			return f.facts[node].index, f.facts[node].routes
		},
		ProposeServed: func(_ context.Context, index, routes uint64) error {
			if f.hold != nil {
				<-f.hold
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			f.proposed = append(f.proposed, servedFact{index, routes})
			if f.fail != nil {
				return f.fail
			}
			if f.commit {
				f.facts["n2"] = servedFact{index, routes}
			}
			return nil
		},
		AdoptedThrough: func() uint64 {
			f.mu.Lock()
			defer f.mu.Unlock()
			return f.through
		},
		Cover: func(node string) (uint64, uint64) {
			f.mu.Lock()
			defer f.mu.Unlock()
			return f.covers[node].index, f.covers[node].routes
		},
		CoverDue: func() (uint64, bool) {
			f.mu.Lock()
			defer f.mu.Unlock()
			return f.dueGrant, f.coverDue
		},
		ProposeCover: func(_ context.Context, grant, routes uint64) error {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.covered = append(f.covered, servedFact{grant, routes})
			if f.coverFail != nil {
				return f.coverFail
			}
			if f.covers == nil {
				f.covers = map[string]servedFact{}
			}
			f.covers["n2"] = servedFact{grant, routes}
			return nil
		}})
	p.SetSink(&fakeSink{})
	p.Start()
	t.Cleanup(p.Stop)
	return p, tr
}

// deliverFull hands the plane a peer's full set, numbered as covering its
// batches up to batch (zero: an older sender's, unnumbered).
func (s *stubTransport) deliverFull(t *testing.T, peer string, entries []Entry, batch uint64) {
	t.Helper()
	s.deliverRaw(t, peer, routeFull, fullSync{Snapshot: uint64(time.Now().UnixNano()), Entries: entries, Batch: batch})
}

// deliverDelta hands the plane one of a peer's delta batches; the number
// rides the first entry, as the sender puts it.
func (s *stubTransport) deliverDelta(t *testing.T, peer string, entries []Entry, batch uint64) {
	t.Helper()
	entries = append([]Entry(nil), entries...)
	entries[0].Batch = batch
	s.deliverRaw(t, peer, routeDelta, entries)
}

func (s *stubTransport) deliverRaw(t *testing.T, peer string, kind byte, body any) {
	t.Helper()
	s.mu.Lock()
	h := s.handlers[transport.RouteUpdate]
	s.mu.Unlock()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	h(peer, append([]byte{kind}, b...))
}

// waitProposed returns once no proposal of this node's fact is in flight.
func waitProposed(t *testing.T, p *Plane) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		p.mu.Lock()
		busy := p.proposing
		p.mu.Unlock()
		if !busy {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("a proposal of this node's fact never returned")
}

// TestLostOwnersRoutesAreForgottenOnlyOnceItsSessionsAreServed walks one node
// through a peer's loss. Each step removes one reason to keep refusing, and
// the publish stays refused until the last is gone.
func TestLostOwnersRoutesAreForgottenOnlyOnceItsSessionsAreServed(t *testing.T) {
	f := &servedFixture{owned: map[string]int{"n1": 3, "n2": 1, "n3": 4}, applied: 5, hooked: 5}
	p, tr := newServedPlane(t, f)
	tr.setState("n1", transport.Up)
	tr.setState("n3", transport.Up)
	tr.deliverFull(t, "n1", []Entry{{Namespace: "default", Filter: "t/x", Version: 1, Count: 1}}, 100)
	tr.deliverFull(t, "n3", nil, 200)
	if _, err := p.Forward(qos1("t/x")); err != nil {
		t.Fatalf("publish with every node up: %v", err)
	}
	tr.setState("n1", transport.Down)

	refused := func(step, heldFor string) {
		t.Helper()
		p.checkServed()
		if _, err := p.Forward(qos1("t/x")); !errors.Is(err, ErrRoutesUnavailable) {
			t.Fatalf("%s: publish to the lost owner's filter: %v, want it refused", step, err)
		}
		if got := f.metrics.get("hoardmq_route_owners_hidden{" + heldFor + "}"); got < 1 {
			t.Fatalf("%s: hoardmq_route_owners_hidden{reason=%q} = %v, want the operator told why", step, heldFor, got)
		}
	}
	refused("n1 still owns leases", keepLeases)

	// The reassignment applies here at index 10; its hooks have not run.
	f.set(func(f *servedFixture) { f.owned["n1"], f.owned["n3"], f.applied = 0, 7, 10 })
	refused("this node's hooks have not run for the reassignment", keepUnsynced)
	if got := f.proposals(); len(got) != 0 {
		t.Fatalf("this node proposed %v before its hooks had run to the index", got)
	}
	f.set(func(f *servedFixture) { f.hooked = 10 })
	refused("n3 owns leases and has no fact in the log", keepUnsynced)

	// This node is idle at the index: it says so once, in the log.
	waitProposed(t, p)
	if got := f.proposals(); len(got) != 1 || got[0].index != 10 {
		t.Fatalf("proposals %v; want this node's fact at index 10, once", got)
	}
	p.checkServed()
	p.checkServed()
	waitProposed(t, p)
	if got := f.proposals(); len(got) != 1 {
		t.Errorf("proposals %v; a node proposes its fact once per lost owner", got)
	}

	f.set(func(f *servedFixture) { f.facts["n3"] = servedFact{9, 200} })
	refused("n3's fact predates the reassignment", keepUnsynced)
	f.set(func(f *servedFixture) { f.adopting = 1 })
	refused("this node is adopting again", keepAdopting)
	f.set(func(f *servedFixture) { f.adopting = 0 })

	// n3's fact is in the log; its routes up to the mark it gave are not all
	// here yet.
	f.set(func(f *servedFixture) { f.facts["n3"] = servedFact{11, 202} })
	refused("n3's routes up to its mark have not arrived", keepUnsynced)
	tr.setState("n3", transport.Down)
	refused("n3 is down and its routes up to its mark have not arrived", keepUnreachable)
	tr.setState("n3", transport.Up)
	tr.deliverDelta(t, "n3", []Entry{{Namespace: "default", Filter: "t/x", Version: 2, Count: 1}}, 201)
	refused("n3's routes are held up to 201 of 202", keepUnsynced)

	tr.deliverDelta(t, "n3", []Entry{{Namespace: "default", Filter: "t/y", Version: 3, Count: 1}}, 202)
	p.checkServed()
	sent, err := p.Forward(qos1("t/x"))
	if err != nil || sent != 1 {
		t.Fatalf("n1's sessions are served on n3: forward sent %d, err %v; want 1 forward to n3 and no refusal", sent, err)
	}
	if hidden := p.Table().HiddenOwners(); len(hidden) != 0 {
		t.Fatalf("hidden owners %v after n1 was forgotten", hidden)
	}
	if got := f.metrics.get("hoardmq_route_owners_forgotten_total{sessions-served}"); got != 1 {
		t.Fatalf("hoardmq_route_owners_forgotten_total = %v, want 1", got)
	}
	p.checkServed()
	for _, why := range []string{keepUnknown, keepLeases, keepAdopting, keepUnsynced, keepUnreachable} {
		if got := f.metrics.get("hoardmq_route_owners_hidden{" + why + "}"); got != 0 {
			t.Fatalf("hoardmq_route_owners_hidden{reason=%q} = %v with no owner hidden", why, got)
		}
	}
}

// TestALostOwnerIsForgottenWhileItsAdopterIsUnreachable: this node has the
// adopter's routes, then loses its link to the adopter, and only then does
// the adopter's fact reach the log. The fact is read from the log, so the
// lost owner is forgotten with the adopter still out of reach; a publish to
// the adopted filter is then refused on the adopter's own hidden entry.
func TestALostOwnerIsForgottenWhileItsAdopterIsUnreachable(t *testing.T) {
	f := &servedFixture{owned: map[string]int{"n1": 3, "n2": 1, "n3": 4}, applied: 5, hooked: 5, commit: true}
	p, tr := newServedPlane(t, f)
	tr.setState("n1", transport.Up)
	tr.setState("n3", transport.Up)
	tr.deliverFull(t, "n1", []Entry{{Namespace: "default", Filter: "t/x", Version: 1, Count: 1}}, 100)
	tr.deliverFull(t, "n3", nil, 200)
	tr.setState("n1", transport.Down)
	f.set(func(f *servedFixture) { f.owned["n1"], f.owned["n3"], f.applied, f.hooked = 0, 7, 10, 10 })
	// The adoption's import advertised x's filter on n3, in its batch 201.
	tr.deliverDelta(t, "n3", []Entry{{Namespace: "default", Filter: "t/x", Version: 2, Count: 1}}, 201)
	p.checkServed()
	waitProposed(t, p)
	tr.setState("n3", transport.Down)
	p.checkServed()
	if hidden := p.Table().HiddenOwners(); len(hidden) != 2 {
		t.Fatalf("precondition: hidden owners %v, want n1 and n3 before n3's fact", hidden)
	}
	if got := f.metrics.get("hoardmq_route_owners_hidden{" + keepUnreachable + "}"); got != 1 {
		t.Fatalf("precondition: n1 held unreachable %v times, want 1 before n3's fact", got)
	}

	f.set(func(f *servedFixture) { f.facts["n3"] = servedFact{12, 201} })
	p.checkServed()
	if hidden := p.Table().HiddenOwners(); len(hidden) != 1 || hidden[0] != "n3" {
		t.Fatalf("hidden owners %v after n3's fact reached the log; want n1 forgotten, n3 still hidden", hidden)
	}
	if got := f.metrics.get("hoardmq_route_owners_forgotten_total{sessions-served}"); got != 1 {
		t.Fatalf("hoardmq_route_owners_forgotten_total = %v, want 1", got)
	}
	if _, err := p.Forward(qos1("t/x")); !errors.Is(err, ErrRoutesUnavailable) {
		t.Fatalf("publish to t/x with its adopter unreachable: %v, want it refused on n3's hidden entry", err)
	}
	if !p.Table().HiddenMatch("default", "t/x") {
		t.Fatal("the refusal is not n3's hidden entry")
	}
}

// TestAnOlderMemberNeverLetsANewerNodeForget: a member on a build from before
// the fact sends its full sets with the adoption fields and no batch number,
// and proposes no fact. However idle it says it is, a newer node keeps the
// lost owner while that member owns leases.
func TestAnOlderMemberNeverLetsANewerNodeForget(t *testing.T) {
	f := &servedFixture{owned: map[string]int{"n1": 3, "n2": 1, "n3": 4}, applied: 5, hooked: 5, commit: true}
	p, tr := newServedPlane(t, f)
	tr.setState("n1", transport.Up)
	tr.setState("n3", transport.Up)
	tr.deliverFull(t, "n1", []Entry{{Namespace: "default", Filter: "t/x", Version: 1, Count: 1}}, 100)
	tr.setState("n1", transport.Down)
	f.set(func(f *servedFixture) { f.owned["n1"], f.owned["n3"], f.applied, f.hooked = 0, 7, 10, 10 })
	older := map[string]any{"snapshot": time.Now().UnixNano(), "leaseIndex": 50, "adopting": 0,
		"entries": []Entry{{Namespace: "default", Filter: "t/x", Owner: "n3", Version: 2, Count: 1}}}
	for i := 0; i < 3; i++ {
		tr.deliverRaw(t, "n3", routeFull, older)
		p.checkServed()
	}
	if hidden := p.Table().HiddenOwners(); len(hidden) != 1 || hidden[0] != "n1" {
		t.Fatalf("hidden owners %v; an older member's report let this node forget n1", hidden)
	}
	if got := f.metrics.get("hoardmq_route_owners_hidden{" + keepUnsynced + "}"); got != 1 {
		t.Fatalf("n1 held unsynced %v times, want 1", got)
	}
}

// TestAnOlderNodeReadsNoAdoptionStateFromANewerOne: an older build decodes a
// full set's adoption state from two members this build no longer sends. It
// reads zeros, and zero is below any index an owner is emptied at, so the
// older node keeps the owner.
func TestAnOlderNodeReadsNoAdoptionStateFromANewerOne(t *testing.T) {
	body, err := json.Marshal(fullSync{Snapshot: 9, Entries: []Entry{{Namespace: "default", Filter: "t/x", Owner: "n3", Version: 2, Count: 1}}, Batch: 7001})
	if err != nil {
		t.Fatal(err)
	}
	var olderReading struct {
		Entries    []Entry `json:"entries"`
		LeaseIndex uint64  `json:"leaseIndex"`
		Adopting   int     `json:"adopting"`
	}
	if err := json.Unmarshal(body, &olderReading); err != nil {
		t.Fatal(err)
	}
	if len(olderReading.Entries) != 1 {
		t.Fatalf("the older reading lost the entries: %s", body)
	}
	if olderReading.LeaseIndex != 0 || olderReading.Adopting != 0 {
		t.Fatalf("a full set from this build carries adoption state %d/%d: %s", olderReading.LeaseIndex, olderReading.Adopting, body)
	}
}

// TestAFailedProposalIsMadeAgain: a fact the log did not take is proposed at
// the next check, and one it took is not.
func TestAFailedProposalIsMadeAgain(t *testing.T) {
	// Each count is held, so a count made after the in-flight flag cleared
	// would be read as missing.
	f := &servedFixture{owned: map[string]int{"n1": 3, "n2": 1, "n3": 4}, applied: 5, hooked: 5, fail: errors.New("no leader"),
		metrics: servedMetrics{slow: 20 * time.Millisecond}}
	p, tr := newServedPlane(t, f)
	tr.setState("n1", transport.Up)
	tr.deliverFull(t, "n1", []Entry{{Namespace: "default", Filter: "t/x", Version: 1, Count: 1}}, 100)
	tr.setState("n1", transport.Down)
	f.set(func(f *servedFixture) { f.owned["n1"], f.applied, f.hooked = 0, 10, 10 })
	p.checkServed()
	waitProposed(t, p)
	p.checkServed()
	waitProposed(t, p)
	if got := f.proposals(); len(got) != 2 {
		t.Fatalf("proposals %v; want a failed one made again", got)
	}
	if got := p.ForgetTimeline(); len(got) != 1 || got[0].ServedHereNs == 0 || got[0].ProposedNs != 0 {
		t.Fatalf("timeline %+v; want n1 served here, and this node's fact undated while no proposal of it was committed", got)
	}
	f.set(func(f *servedFixture) { f.fail = nil })
	p.checkServed()
	waitProposed(t, p)
	p.checkServed()
	waitProposed(t, p)
	if got := f.proposals(); len(got) != 3 {
		t.Fatalf("proposals %v; want one more, then none once it was committed", got)
	}
	if got := f.metrics.get("hoardmq_route_served_proposals_total{failed}"); got != 2 {
		t.Errorf("failed proposals counted %v, want 2", got)
	}
	if got := f.metrics.get("hoardmq_route_served_proposals_total{committed}"); got != 1 {
		t.Errorf("committed proposals counted %v, want 1", got)
	}
}

// TestAFactAlreadyInTheLogIsNotProposedAgain: a node whose own fact already
// covers the lost owner's index — from before a restart, which this plane's
// memory of its proposals does not survive — proposes nothing.
func TestAFactAlreadyInTheLogIsNotProposedAgain(t *testing.T) {
	f := &servedFixture{owned: map[string]int{"n1": 3, "n2": 1, "n3": 4}, applied: 5, hooked: 5}
	p, tr := newServedPlane(t, f)
	tr.setState("n1", transport.Up)
	tr.deliverFull(t, "n1", []Entry{{Namespace: "default", Filter: "t/x", Version: 1, Count: 1}}, 100)
	tr.setState("n1", transport.Down)
	f.set(func(f *servedFixture) {
		f.owned["n1"], f.applied, f.hooked, f.facts["n2"] = 0, 12, 12, servedFact{12, 300}
	})
	p.checkServed()
	waitProposed(t, p)
	if got := f.proposals(); len(got) != 0 {
		t.Fatalf("proposals %v with this node's fact at 12 already in the log for an owner emptied at 12", got)
	}
	// Non-vacuity: the same check does propose once the log's fact is below
	// the index.
	f.set(func(f *servedFixture) { f.facts["n2"] = servedFact{11, 300} })
	p.checkServed()
	waitProposed(t, p)
	if got := f.proposals(); len(got) != 1 {
		t.Fatalf("proposals %v with this node's fact below the index, want one", got)
	}
}

// TestANewerFactFollowsATakeoverOfASessionInTransit: a node whose fact is
// already at the lost owner's emptied index took a session the adopter had
// not imported, and now holds it. It puts a newer fact in the log, at an
// index at or past the takeover's grant, once its hooks have run that far,
// and once only (WI 1833).
func TestANewerFactFollowsATakeoverOfASessionInTransit(t *testing.T) {
	f := &servedFixture{owned: map[string]int{"n1": 3, "n2": 1, "n3": 4}, applied: 5, hooked: 5, commit: true}
	p, tr := newServedPlane(t, f)
	tr.setState("n1", transport.Up)
	tr.setState("n3", transport.Up)
	tr.deliverFull(t, "n1", []Entry{{Namespace: "default", Filter: "t/x", Version: 1, Count: 1}}, 100)
	tr.setState("n1", transport.Down)
	f.set(func(f *servedFixture) { f.owned["n1"], f.owned["n3"], f.applied, f.hooked = 0, 7, 10, 10 })
	p.checkServed()
	waitProposed(t, p)
	if got := f.proposals(); len(got) != 1 || got[0].index != 10 {
		t.Fatalf("precondition: proposals %v; want this node's fact at the emptied index", got)
	}
	f.set(func(f *servedFixture) { f.through = 15; f.applied = 14; f.hooked = 14 })
	p.checkServed()
	waitProposed(t, p)
	if got := f.proposals(); len(got) != 1 {
		t.Fatalf("proposals %v; a fact below the takeover's grant says nothing about it", got)
	}
	f.set(func(f *servedFixture) { f.applied, f.hooked = 16, 16 })
	p.checkServed()
	waitProposed(t, p)
	p.checkServed()
	waitProposed(t, p)
	if got := f.proposals(); len(got) != 2 || got[1].index != 16 {
		t.Fatalf("proposals %v; want one newer fact at index 16", got)
	}
}
