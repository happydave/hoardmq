package lease

import (
	"slices"
	"sync"
	"testing"
	"time"
)

// TestDeclarationReachesTheCallbackAndTheTimeline: a node stops, the leader
// declares it, and every survivor passes the declaration on and dates each
// step of it — the last alive record it applied, the leader's proposal, and
// its own apply — in that order.
func TestDeclarationReachesTheCallbackAndTheTimeline(t *testing.T) {
	var mu sync.Mutex
	told := map[string][]string{}
	fixtureOptions = func(o *Options) {
		node := o.Node
		o.Declared = func(declared string) {
			mu.Lock()
			told[node] = append(told[node], declared)
			mu.Unlock()
		}
	}
	t.Cleanup(func() { fixtureOptions = nil })
	ttl := time.Second
	c := newCluster(t, []string{"n1", "n2", "n3"}, ttl, nil)
	leader := c.settled()
	var gone *tnode
	var survivors []*tnode
	for _, name := range c.names {
		if n := c.nodes[name]; n != leader && gone == nil {
			gone = n
		} else {
			survivors = append(survivors, n)
		}
	}
	c.stopNode(gone)
	waitFor(t, "every survivor told of the declaration", 15*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, n := range survivors {
			if !slices.Contains(told[n.name], gone.name) {
				return false
			}
		}
		return true
	})
	mu.Lock()
	for _, n := range survivors {
		for _, name := range told[n.name] {
			if name != gone.name {
				t.Errorf("%s was told %s was declared; only %s stopped", n.name, name, gone.name)
			}
		}
	}
	mu.Unlock()
	for _, n := range survivors {
		tl := n.svc.LivenessStats().Timeline[gone.name]
		if tl.LastAliveAppliedNs == 0 || tl.DeclarationAppliedNs == 0 {
			t.Fatalf("%s's timeline for %s: %+v; want its last alive record and the declaration dated", n.name, gone.name, tl)
		}
		if tl.LastAliveAppliedNs >= tl.DeclarationAppliedNs {
			t.Errorf("%s: last alive record applied at %d, declaration applied at %d; want the declaration after it", n.name, tl.LastAliveAppliedNs, tl.DeclarationAppliedNs)
		}
	}
	tl := leader.svc.LivenessStats().Timeline[gone.name]
	if tl.DeclarationProposedNs == 0 || tl.DeclarationProposedNs > tl.DeclarationAppliedNs {
		t.Fatalf("leader %s: declaration proposed at %d, applied at %d; want a proposal no later than its apply", leader.name, tl.DeclarationProposedNs, tl.DeclarationAppliedNs)
	}
	// The leader judges silence on its steady clock and stamps these on the
	// wall clock a few nanoseconds apart; the slack is for that, not for a
	// declaration made early.
	if silence := time.Duration(tl.DeclarationProposedNs - tl.LastAliveAppliedNs); silence < ttl-10*time.Millisecond {
		t.Fatalf("leader %s proposed the declaration %v after the last alive record it applied; want at least the ttl %v", leader.name, silence, ttl)
	}
	for _, n := range survivors {
		if other := n.svc.LivenessStats().Timeline[n.name]; other.DeclarationAppliedNs != 0 {
			t.Errorf("%s's timeline declares itself: %+v", n.name, other)
		}
	}
}
