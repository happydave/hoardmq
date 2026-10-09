package lease

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
)

func applyOps(t *testing.T, tb *Table, ops ...Op) []Result {
	t.Helper()
	payload, err := json.Marshal(ops)
	if err != nil {
		t.Fatal(err)
	}
	out, err := tb.Apply(0, payload)
	if err != nil {
		t.Fatal(err)
	}
	results, ok := out.([]Result)
	if !ok {
		t.Fatalf("apply returned %T", out)
	}
	return results
}

// TestClusterLevelOnlyRises: the cluster's feature level is what every node
// reads to decide which behaviour a gated operation has, so it changes at a
// log position and never goes back — a node that has switched cannot switch
// again.
func TestClusterLevelOnlyRises(t *testing.T) {
	tb := NewTable()
	if got := tb.Level(); got != 1 {
		t.Fatalf("a table that has never been raised reads %d, want 1", got)
	}
	if r := applyOps(t, tb, Op{Kind: OpSetLevel, Level: 2})[0]; !r.OK {
		t.Fatalf("raise to 2 refused: %s", r.Reason)
	}
	if got := tb.Level(); got != 2 {
		t.Fatalf("level %d after a raise to 2", got)
	}
	for _, level := range []uint64{2, 1} {
		r := applyOps(t, tb, Op{Kind: OpSetLevel, Level: level})[0]
		if r.OK {
			t.Fatalf("set-level to %d applied; the level only rises", level)
		}
		if r.Reason != "not a raise" {
			t.Fatalf("set-level to %d refused with %q", level, r.Reason)
		}
	}
	if got := tb.Level(); got != 2 {
		t.Fatalf("level %d after two refused sets, want 2", got)
	}
}

// TestClusterLevelSurvivesASnapshot, and a snapshot written before the level
// existed restores as the baseline.
func TestClusterLevelSurvivesASnapshot(t *testing.T) {
	tb := NewTable()
	applyOps(t, tb, Op{Kind: OpSetLevel, Level: 3})
	data, err := tb.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	restored := NewTable()
	if err := restored.Restore(data); err != nil {
		t.Fatal(err)
	}
	if got := restored.Level(); got != 3 {
		t.Fatalf("restored level %d, want 3", got)
	}
	old := NewTable()
	if err := old.Restore([]byte(`{"leases":[],"nodes":{}}`)); err != nil {
		t.Fatal(err)
	}
	if got := old.Level(); got != 1 {
		t.Fatalf("a snapshot written before the level restored as %d, want 1", got)
	}
}

// TestNodesAdvertiseTheLevelTheyUnderstand: the alive operation every node
// already sends carries it, and the lowest of them is what a raise waits for.
func TestNodesAdvertiseTheLevelTheyUnderstand(t *testing.T) {
	tb := NewTable()
	now := time.Now()
	applyOps(t, tb,
		Op{Kind: OpAlive, Node: "n1", Now: now, Level: 2},
		Op{Kind: OpAlive, Node: "n2", Now: now, Level: 2},
		Op{Kind: OpAlive, Node: "n3", Now: now}, // a build that predates the field
	)
	if min, known := tb.MinAdvertisedLevel([]string{"n1", "n2"}); !known || min != 2 {
		t.Fatalf("min over the two that advertise 2 = %d (known %v), want 2", min, known)
	}
	if min, known := tb.MinAdvertisedLevel([]string{"n1", "n2", "n3"}); !known || min != 1 {
		t.Fatalf("min including a build with no level = %d (known %v), want 1", min, known)
	}
	if _, known := tb.MinAdvertisedLevel([]string{"n1", "n4"}); known {
		t.Fatal("a member with no record read as known; absence is not a level")
	}
	// A node's advertisement only rises: an alive from an older build in
	// flight behind a newer one does not take the level back.
	applyOps(t, tb, Op{Kind: OpAlive, Node: "n1", Now: now.Add(time.Second)})
	if min, _ := tb.MinAdvertisedLevel([]string{"n1"}); min != 2 {
		t.Fatalf("n1's advertised level fell to %d", min)
	}
}

func TestRaiseTo(t *testing.T) {
	for _, c := range []struct {
		name         string
		current, min uint64
		every        bool
		wantTo       uint64
		wantRaise    bool
	}{
		{"every member above the table", 1, 2, true, 2, true},
		{"the lowest member is what it raises to", 1, 3, true, 3, true},
		{"already there", 2, 2, true, 0, false},
		{"a member behind the table", 3, 2, true, 0, false},
		{"a member with no record", 1, 2, false, 0, false},
	} {
		to, ok := raiseTo(c.current, c.min, c.every)
		if to != c.wantTo || ok != c.wantRaise {
			t.Errorf("%s: raiseTo(%d, %d, %v) = %d, %v; want %d, %v", c.name, c.current, c.min, c.every, to, ok, c.wantTo, c.wantRaise)
		}
	}
}

// TestLeaderRaisesTheLevelOnceEveryMemberUnderstandsIt is a rolling upgrade:
// a formed cluster on one build, its members moved to the next one at a
// time. The member still on the older build holds the whole cluster at its
// level, and the raise follows its upgrade.
func TestLeaderRaisesTheLevelOnceEveryMemberUnderstandsIt(t *testing.T) {
	names := []string{"n1", "n2", "n3"}
	c := newCluster(t, names, 400*time.Millisecond, nil)
	lead := c.settled()
	c.nodes["n1"].svc.levelPin.Store(2)
	c.nodes["n2"].svc.levelPin.Store(2)

	// The hold is only evidence once the leader has seen what each member
	// advertises: before that, nothing would have been raised anyway.
	waitFor(t, "every advertisement on the leader", 10*time.Second, func() bool {
		for _, name := range []string{"n1", "n2"} {
			if n, _ := lead.svc.Table().Node(name); n.Level != 2 {
				return false
			}
		}
		n, ok := lead.svc.Table().Node("n3")
		return ok && n.Level == 1
	})
	// The raiser runs on the leader's tick; give it several to be wrong in.
	ticks, last := 0, lead.svc.loopLastNs.Load()
	waitFor(t, "four more leader ticks", 10*time.Second, func() bool {
		if now := lead.svc.loopLastNs.Load(); now != last {
			ticks, last = ticks+1, now
		}
		return ticks >= 4
	})
	for _, n := range c.nodes {
		if got := n.svc.Table().Level(); got != 1 {
			t.Fatalf("%s reads level %d while n3 advertises 1", n.name, got)
		}
	}

	c.nodes["n3"].svc.levelPin.Store(2)
	waitFor(t, "level 2 on every node", 10*time.Second, func() bool {
		for _, n := range c.nodes {
			if n.svc.Table().Level() != 2 {
				return false
			}
		}
		return true
	})
}

// gatedJournal is a semantic keyed to level 2 that exists only here: each
// acquire is journaled as applied the old way or the new way. What decides
// is the level the table held at that log position — or, for the negative
// control, the level of the node's own build, which is the mistake a gated
// change is most likely to make.
type gatedJournal struct {
	mu       sync.Mutex
	byNode   map[string][]string
	build    func(node string) uint64
	useBuild bool
}

func (j *gatedJournal) probe(node string) func(index, level uint64, op Op) {
	return func(index, level uint64, op Op) {
		if op.Kind != OpAcquire {
			return
		}
		if j.useBuild {
			level = j.build(node)
		}
		how := "old"
		if level >= 2 {
			how = "new"
		}
		j.mu.Lock()
		j.byNode[node] = append(j.byNode[node], fmt.Sprintf("%d %s %s", index, op.ClientID, how))
		j.mu.Unlock()
	}
}

func (j *gatedJournal) of(node string) []string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return append([]string(nil), j.byNode[node]...)
}

// gatedJournalBuildLevel is the negative control's switch: the gated
// semantic reads the build's level instead of the table's.
var gatedJournalBuildLevel = false

// TestMixedVersionClusterAppliesIdenticallyOrRefuses is the criterion the
// mechanism exists for. One member understands only level 1 and the others
// level 2. While the table is at 1 every node applies every operation the
// old way. Once it is at 2 every node — the old build included, since it
// applies the same log — switches at the same log position, and the old
// build refuses to serve rather than act on semantics it does not have.
func TestMixedVersionClusterAppliesIdenticallyOrRefuses(t *testing.T) {
	builds := map[string]uint64{"n1": 2, "n2": 2, "n3": 1}
	var bmu sync.Mutex
	j := &gatedJournal{byNode: map[string][]string{}, useBuild: gatedJournalBuildLevel, build: func(node string) uint64 {
		bmu.Lock()
		defer bmu.Unlock()
		return builds[node]
	}}
	fixtureProbe = j.probe
	t.Cleanup(func() { fixtureProbe = nil })
	names := []string{"n1", "n2", "n3"}
	c := newCluster(t, names, 400*time.Millisecond, nil)
	c.settled()
	for name, l := range builds {
		c.nodes[name].svc.SetBuildLevel(l)
	}
	serving := c.nodes["n1"]

	connect := func(from, to int) {
		t.Helper()
		for i := from; i < to; i++ {
			if _, err := acquire(t, serving, fmt.Sprintf("c%02d", i), nil, 0, true); err != nil {
				t.Fatalf("acquire c%02d: %v", i, err)
			}
		}
	}
	journaled := func(n int) func() bool {
		return func() bool {
			for _, name := range names {
				if len(j.of(name)) < n {
					return false
				}
			}
			return true
		}
	}

	connect(0, 5)
	waitFor(t, "five acquires applied on every node", 10*time.Second, journaled(5))
	if got := c.nodes["n3"].svc.Table().Level(); got != 1 {
		t.Fatalf("the table is at %d with a member that understands only 1", got)
	}

	// The old member is upgraded for as long as the raise takes and then put
	// back: a node that joins, or is rolled back, below a level the cluster
	// has already reached.
	c.nodes["n3"].svc.SetBuildLevel(2)
	waitFor(t, "level 2 on every node", 10*time.Second, func() bool {
		for _, n := range c.nodes {
			if n.svc.Table().Level() != 2 {
				return false
			}
		}
		return true
	})
	c.nodes["n3"].svc.SetBuildLevel(1)

	connect(5, 10)
	waitFor(t, "ten acquires applied on every node", 10*time.Second, journaled(10))

	// Divergence first: it is the property. The span check after it only
	// says the comparison covered both sides of the raise.
	want := j.of("n1")
	for _, name := range []string{"n2", "n3"} {
		if got := j.of(name); !reflect.DeepEqual(got, want) {
			t.Fatalf("%s applied the same log differently from n1:\n n1 %v\n %s %v", name, want, name, got)
		}
	}
	olds, news := 0, 0
	for _, line := range want {
		if line[len(line)-3:] == "old" {
			olds++
		} else {
			news++
		}
	}
	if olds < 5 || news < 5 {
		t.Fatalf("the journal does not span the raise: %d applied the old way, %d the new: %v", olds, news, want)
	}

	if !c.nodes["n3"].svc.BelowLevel() {
		t.Fatal("n3 understands level 1, the table is at 2, and it does not report itself below the level")
	}
	_, err := acquire(t, c.nodes["n3"], "c-n3", nil, 0, true)
	var refusal *Refusal
	if !errors.As(err, &refusal) {
		t.Fatalf("n3 admitted a client below the cluster's level: %v", err)
	}
	if _, err := acquire(t, serving, "c-n1", nil, 0, true); err != nil {
		t.Fatalf("n1 understands the level and refused: %v", err)
	}
}

type levelSink struct {
	mu sync.Mutex
	n  map[string]float64
}

func (s *levelSink) Add(name string, labels map[string]string, delta float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.n[name+" "+labels["outcome"]] += delta
}
func (s *levelSink) Set(string, map[string]string, float64) {}

// TestARaiseIsCountedByHowItEnded: applied, refused by the monotonic check
// because another raise reached the log first, or never applied — and each
// ending frees the next tick to propose again.
func TestARaiseIsCountedByHowItEnded(t *testing.T) {
	sink := &levelSink{n: map[string]float64{}}
	s, err := New(Options{Node: "me", Core: &fakeCore{role: "leader"}, Machine: consensus.NewMachine(), Sessions: newFakeSessions(), TTL: 2 * time.Second, Logger: quiet, Metrics: sink})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		res     Result
		err     error
		outcome string
	}{
		{Result{OK: true}, nil, "applied"},
		{Result{Reason: "not a raise"}, nil, "refused"},
		{Result{}, errors.New("no leader"), "failed"},
	} {
		s.raising.Store(2)
		s.noteRaise(2, c.res, c.err)
		if got := sink.n["hoardmq_cluster_feature_level_raises_total "+c.outcome]; got != 1 {
			t.Errorf("%s counted %v times, want 1 (%v)", c.outcome, got, sink.n)
		}
		if s.raising.Load() != 0 {
			t.Errorf("a raise that ended %s still reads as in flight", c.outcome)
		}
	}
}
