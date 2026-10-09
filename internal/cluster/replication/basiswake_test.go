package replication

import (
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/lease"
)

// wakeHarness is three nodes whose drivers check the basis once at start and
// then not for an hour, so any later map is the wake's doing.
func wakeHarness(t *testing.T) *harness {
	t.Helper()
	h := newHarnessWith(t, []string{"n1", "n2", "n3"}, 4, 2, func(o *Options) { o.Interval = time.Hour })
	now := time.Now()
	for _, n := range h.nodes {
		for _, o := range h.order {
			n.leases.nodes[o] = lease.NodeInfo{Alive: now}
		}
	}
	h.nodes["n1"].core.role = "leader"
	for _, n := range h.order {
		h.nodes[n].rep.Start()
	}
	h.waitFor("first map", 5*time.Second, func() bool {
		for _, n := range h.nodes {
			if n.table.Version() != 1 {
				return false
			}
		}
		return true
	})
	return h
}

func (h *harness) declare(node string) {
	for _, n := range h.nodes {
		n.leases.mu.Lock()
		info := n.leases.nodes[node]
		info.DeadAt = 7
		n.leases.nodes[node] = info
		n.leases.mu.Unlock()
	}
}

func eventOf(tl MapTimeline, version uint64) (MapEvent, bool) {
	for _, e := range tl.Events {
		if e.Version == version {
			return e, true
		}
	}
	return MapEvent{}, false
}

// TestDeclarationWakesTheDriver: a declaration applied on the leader moves
// the map at once. With the driver's interval an hour, no periodic check can
// fall inside the wait, so a wake that did nothing fails here outright.
func TestDeclarationWakesTheDriver(t *testing.T) {
	h := wakeHarness(t)
	h.declare("n3")
	h.nodes["n1"].rep.BasisChanged()
	h.waitFor("every node on a map without n3", 2*time.Second, func() bool {
		for _, n := range h.nodes {
			if m := n.table.Current(); m.Version != 2 || len(m.Basis) != 2 || m.Basis[0] != "n1" || m.Basis[1] != "n2" {
				return false
			}
		}
		return true
	})
	e, ok := eventOf(h.nodes["n1"].rep.MapTimeline(), 2)
	if !ok || e.Cause != causeDeclaration {
		t.Fatalf("leader's record of version 2: %+v (found %v); want it decided on the declaration", e, ok)
	}
	if e.DecidedNs == 0 || e.ProposedNs < e.DecidedNs || e.AppliedNs == 0 {
		t.Fatalf("leader's record of version 2 is not dated in order: %+v", e)
	}
	for _, name := range []string{"n2", "n3"} {
		e, ok := eventOf(h.nodes[name].rep.MapTimeline(), 2)
		if !ok || e.AppliedNs == 0 || e.Cause != "" {
			t.Fatalf("%s's record of version 2: %+v (found %v); want it applied and no cause, which only the proposer knows", name, e, ok)
		}
	}
	h.waitFor("every node reconciled to version 2", 5*time.Second, func() bool {
		for _, n := range h.nodes {
			if e, ok := eventOf(n.rep.MapTimeline(), 2); !ok || e.ReconciledNs < e.AppliedNs {
				return false
			}
		}
		return true
	})
}

// TestWakeWithoutABasisChangeProposesNothing: the woken check is the periodic
// one run early. It reads the same basis and leaves the map alone, on the
// leader and on a follower.
func TestWakeWithoutABasisChangeProposesNothing(t *testing.T) {
	h := wakeHarness(t)
	for _, name := range []string{"n1", "n2"} {
		h.nodes[name].rep.BasisChanged()
	}
	h.waitFor("both woken checks run", 2*time.Second, func() bool {
		return h.nodes["n1"].rep.MapTimeline().Checks[causeDeclaration] >= 1 &&
			h.nodes["n2"].rep.MapTimeline().Checks[causeDeclaration] >= 1
	})
	for _, n := range h.nodes {
		if v := n.table.Version(); v != 1 {
			t.Fatalf("%s is on map version %d after a wake with no basis change; want 1", n.name, v)
		}
	}
	// A follower's woken check reads the declaration and still proposes
	// nothing: only the leader's driver proposes.
	h.declare("n3")
	h.nodes["n2"].rep.BasisChanged()
	h.waitFor("the follower's second woken check", 2*time.Second, func() bool {
		return h.nodes["n2"].rep.MapTimeline().Checks[causeDeclaration] >= 2
	})
	for _, n := range h.nodes {
		if v := n.table.Version(); v != 1 {
			t.Fatalf("%s is on map version %d after a follower's woken check; want 1", n.name, v)
		}
	}
}
