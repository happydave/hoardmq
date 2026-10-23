package lease

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/store"
)

// opCounts records the lease service's proposals by operation.
type opCounts struct {
	mu sync.Mutex
	by map[string]float64
}

func (m *opCounts) Add(name string, labels map[string]string, delta float64) {
	if name != "hoardmq_lease_proposals_total" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.by[labels["op"]] += delta
}
func (m *opCounts) Set(string, map[string]string, float64) {}

func (m *opCounts) snapshot() map[string]float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]float64{}
	for k, v := range m.by {
		out[k] = v
	}
	return out
}

// The gated semantic of these tests: at level 2 an acquire stores one
// expiry interval and below it another, in the lease the snapshot carries.
// The old build stores the old value whatever the level, as a binary built
// before the change would.
const (
	oldExpiry uint32 = 1000
	newExpiry uint32 = 2000
)

func newSemantic(level uint64, l *Lease) {
	if level >= 2 {
		l.ExpiryInterval = newExpiry
	} else {
		l.ExpiryInterval = oldExpiry
	}
}

func oldSemantic(_ uint64, l *Lease) { l.ExpiryInterval = oldExpiry }

// holdDisabled is the negative control's switch: the old build's table
// never holds, so it applies what it does not understand.
var holdDisabled = false

func asBuild(level uint64, sem func(uint64, *Lease), metrics *opCounts) (func(o *Options), func(s *Service)) {
	return func(o *Options) {
			o.BuildLevel = level
			if metrics != nil {
				o.Metrics = metrics
			}
		}, func(s *Service) {
			s.table.gated = sem
			if holdDisabled && level < 2 {
				s.table.build = nil
			}
		}
}

func leasesOf(n *tnode) []Lease {
	var out []Lease
	n.svc.Table().each(func(l *Lease) { out = append(out, *l) })
	sort.Slice(out, func(i, j int) bool { return out[i].ClientID < out[j].ClientID })
	return out
}

// TestBelowLevelNodeHoldsAndCatchesUpAfterUpgrade is the mixed-version
// criterion: a node put back on a build below the cluster's level holds the
// log instead of applying a gated semantic the old way; it hands leadership
// over, the leader takes its leases and leaves it out of the shard map; and
// restarted on the new build its table equals the leader's, the gated value
// included, because nothing it applied wrongly reached its snapshot.
func TestBelowLevelNodeHoldsAndCatchesUpAfterUpgrade(t *testing.T) {
	fixtureSnapshotEntries = 20
	fixtureFallback = func(context.Context, string, string, string) (*store.SessionState, bool) { return nil, true }
	t.Cleanup(func() { fixtureSnapshotEntries, fixtureFallback, fixtureOptions, fixtureService = 0, nil, nil, nil })
	fixtureOptions = func(o *Options) { o.BuildLevel = 2 }
	fixtureService = func(_ string, s *Service) { s.table.gated = newSemantic }
	c := newCluster(t, []string{"n1", "n2", "n3"}, 400*time.Millisecond, nil)
	old := c.nodes["n3"]

	// Before the raise n3 owns a client with a session, at the old value on
	// every build.
	if _, err := acquire(t, old, "on-n3", nil, 60, true); err != nil {
		t.Fatal(err)
	}
	old.sessions.put("default", "on-n3", &store.SessionState{SessionMeta: store.SessionMeta{Namespace: "default", ClientID: "on-n3"}})
	waitFor(t, "level 2 on every node", 10*time.Second, func() bool {
		for _, n := range c.nodes {
			if n.svc.Table().Level() != 2 {
				return false
			}
		}
		return true
	})

	// n3 put back on the old build.
	moveLeadOff(t, c, "n3")
	oldCounts := &opCounts{by: map[string]float64{}}
	oldOpts, oldSvc := asBuild(1, oldSemantic, oldCounts)
	c.restartNode(old, oldOpts, oldSvc)
	if !holdDisabled {
		waitFor(t, "n3 holds", 10*time.Second, func() bool { _, ok := old.core.Held(); return ok })
		if !old.svc.BelowLevel() {
			t.Fatal("n3 holds and does not read below the level")
		}
	}

	lead := c.settled()
	for i := 0; i < 30; i++ {
		if _, err := acquire(t, lead, fmt.Sprintf("c%02d", i), nil, 60, true); err != nil {
			t.Fatalf("acquire c%02d on %s: %v", i, lead.name, err)
		}
	}

	// The leader takes n3's lease and leaves n3 out of the placement facts.
	waitFor(t, "n3's lease handed to the leader", 10*time.Second, func() bool {
		l := c.settled()
		return l.svc.Table().CountOwnedBy("n3") == 0 && l.svc.Table().MemberBelow("n3")
	})

	if !holdDisabled {
		checkHeldOldBuild(t, c, old, oldCounts)
	}

	// n3 upgraded.
	newOpts, newSvc := asBuild(2, newSemantic, nil)
	c.restartNode(old, newOpts, newSvc)
	lead = c.settled()
	waitFor(t, "n3 caught up", 10*time.Second, func() bool {
		_, holds := old.core.Held()
		return !holds && old.core.Status().Applied >= lead.core.Status().Applied && !old.core.Recovering()
	})
	want, got := leasesOf(lead), leasesOf(old)
	if len(want) != 31 || len(got) != len(want) {
		t.Fatalf("leases: leader %d, n3 %d, want 31 each", len(want), len(got))
	}
	for i := range want {
		w, g := want[i], got[i]
		if w.ClientID != g.ClientID || w.Owner != g.Owner || w.Epoch != g.Epoch || w.ExpiryInterval != g.ExpiryInterval {
			t.Fatalf("n3 after its upgrade differs from the leader at %s:\n leader %+v\n n3     %+v", w.ClientID, w, g)
		}
		if w.ClientID != "on-n3" && g.ExpiryInterval != newExpiry {
			t.Fatalf("%s acquired at level 2 carries %d, want the level 2 value %d", g.ClientID, g.ExpiryInterval, newExpiry)
		}
	}
}

// checkHeldOldBuild: the held old build hands leadership over when made
// leader, its applied index and snapshot stay below its hold, and it
// proposes nothing but its alive record.
func checkHeldOldBuild(t *testing.T, c *tcluster, old *tnode, counts *opCounts) {
	t.Helper()
	held, _ := old.core.Held()
	lead := c.settled()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	_ = lead.core.TransferLeadership(ctx, "n3")
	cancel()
	waitFor(t, "n3 leads", 5*time.Second, func() bool { return old.core.Status().Role == "leader" })
	waitFor(t, "n3 hands leadership over", 5*time.Second, func() bool {
		st := old.core.Status()
		return st.Role != "leader" && st.Leader != "" && st.Leader != "n3"
	})

	if got := old.core.Status().Applied; got >= held.Index {
		t.Fatalf("n3 applied index %d at or past its hold at %d", got, held.Index)
	}
	if got := old.core.Status().SnapshotIndex; got >= held.Index {
		t.Fatalf("n3 holds a snapshot at %d, past its hold at %d", got, held.Index)
	}
	for op, n := range counts.snapshot() {
		if op != "alive" && n > 0 {
			t.Fatalf("n3 below the level proposed %v %s operations", n, op)
		}
	}
	if counts.snapshot()["alive"] == 0 {
		t.Fatal("n3 below the level proposed no alive record, so the count shows nothing")
	}
}

// TestCorrectRollNeverHolds: a cluster rolled from one build to the next, a
// node at a time, raises its level once, after the last node, and no node
// ever holds or reads behind on the way.
func TestCorrectRollNeverHolds(t *testing.T) {
	fixtureSnapshotEntries = 20
	t.Cleanup(func() { fixtureSnapshotEntries, fixtureOptions, fixtureService = 0, nil, nil })
	fixtureOptions = func(o *Options) { o.BuildLevel = 1 }
	fixtureService = func(_ string, s *Service) { s.table.gated = oldSemantic }
	c := newCluster(t, []string{"n1", "n2", "n3"}, 400*time.Millisecond, nil)
	// Every core the roll runs, the stopped ones included: a hold or a stall
	// is counted on the core for good.
	var cores []*tnode
	for _, n := range c.nodes {
		cores = append(cores, &tnode{name: n.name, core: n.core})
	}
	for i, name := range []string{"n1", "n2", "n3"} {
		for k := 0; k < 10; k++ {
			if _, err := acquire(t, c.settled(), fmt.Sprintf("s%d-%d", i, k), nil, 60, true); err != nil {
				t.Fatal(err)
			}
		}
		newOpts, newSvc := asBuild(2, newSemantic, nil)
		c.restartNode(c.nodes[name], newOpts, newSvc)
		cores = append(cores, &tnode{name: name, core: c.nodes[name].core})
		if i == 2 {
			break
		}
		// Once the leader has the upgraded node's advertisement and has ticked
		// several times with it, a raiser that did not wait for the rest would
		// have raised.
		lead := c.settled()
		waitFor(t, name+"'s advertisement on the leader", 10*time.Second, func() bool {
			n, ok := lead.svc.Table().Node(name)
			return ok && n.Level == 2
		})
		ticks, last := 0, lead.svc.loopLastNs.Load()
		waitFor(t, "four more leader ticks", 10*time.Second, func() bool {
			if now := lead.svc.loopLastNs.Load(); now != last {
				ticks, last = ticks+1, now
			}
			return ticks >= 4
		})
		for _, n := range c.nodes {
			if lv := n.svc.Table().Level(); lv != 1 {
				t.Fatalf("%s reads level %d with %d of 3 nodes upgraded", n.name, lv, i+1)
			}
			if _, held := n.core.Held(); held {
				t.Fatalf("%s holds with %d of 3 nodes upgraded", n.name, i+1)
			}
		}
	}
	waitFor(t, "level 2 on every node", 10*time.Second, func() bool {
		for _, n := range c.nodes {
			if n.svc.Table().Level() != 2 {
				return false
			}
		}
		return true
	})
	for k := 0; k < 10; k++ {
		if _, err := acquire(t, c.settled(), fmt.Sprintf("after-%d", k), nil, 60, true); err != nil {
			t.Fatal(err)
		}
	}
	for _, n := range cores {
		if h := n.core.Holds(); h != 0 || n.core.Stalls() != 0 {
			t.Fatalf("a core of %s in a correct roll held %d times and stalled %d times", n.name, h, n.core.Stalls())
		}
	}
}
