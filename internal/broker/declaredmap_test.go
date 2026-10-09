package broker

import (
	"slices"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/replication"
)

// TestMapDropsACrashedNodeOnItsDeclaration: the node that leads is crashed,
// and on the new leader the map that leaves it out is proposed because the
// declaration applied, not on the driver's next periodic check. It is never
// decided before the declaration: the basis is the declaration.
func TestMapDropsACrashedNodeOnItsDeclaration(t *testing.T) {
	nodes := startTrio(t, nil)
	waitMap(t, nodes, 3)
	var victim string
	for name, f := range nodes {
		if f.srv.Consensus().Status().Role == "leader" {
			victim = name
		}
	}
	if victim == "" {
		t.Fatal("precondition not reached: no node leads the settled trio")
	}
	nodes[victim].srv.Crash()
	nodes[victim].stopped = true

	dropping := func(f *fixture) (replication.MapEvent, bool) {
		for _, e := range f.srv.Replication().MapTimeline().Events {
			if len(e.Basis) > 0 && !slices.Contains(e.Basis, victim) && e.AppliedNs != 0 {
				return e, true
			}
		}
		return replication.MapEvent{}, false
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		all := true
		for _, f := range nodes {
			if _, ok := dropping(f); !f.stopped && !ok {
				all = false
			}
		}
		if all {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("precondition not reached: no map leaving %s out applied on every survivor within 20 s", victim)
		}
		time.Sleep(20 * time.Millisecond)
	}
	var proposer *fixture
	for _, f := range nodes {
		if e, _ := dropping(f); !f.stopped && e.Cause != "" {
			proposer = f
		}
	}
	if proposer == nil {
		t.Fatal("no survivor recorded proposing the map that left the crashed node out")
	}
	e, _ := dropping(proposer)
	if e.Cause != "declaration" {
		t.Fatalf("the map without %s (version %d) was proposed on a %q check; want it proposed on the declaration", victim, e.Version, e.Cause)
	}
	declared := proposer.srv.Leases().LivenessStats().Timeline[victim].DeclarationAppliedNs
	if declared == 0 || e.DecidedNs < declared {
		t.Fatalf("the map without %s was decided at %d, its declaration applied at %d on the same node; the map may not move before the declaration", victim, e.DecidedNs, declared)
	}
}
