package lease

import (
	"reflect"
	"testing"
	"time"
)

func at(ms int64) func() int64 { return func() int64 { return ms } }

func leaseOf(owner string, online bool) *Lease {
	return &Lease{Namespace: "default", ClientID: "c", Owner: owner, Online: online}
}

// TestClaimRace: claims are counted per previous owner, the snapshot is the
// count when that owner's first lease was moved and later claims leave it
// alone, and nothing else is counted.
func TestClaimRace(t *testing.T) {
	var c claimRace
	c.observe(leaseOf("n9", true), leaseOf("n2", true), at(100)) // a client claims from n9
	c.observe(leaseOf("n9", false), leaseOf("n3", true), at(110))
	c.observe(leaseOf("n8", true), leaseOf("n2", true), at(115)) // another owner's client
	// The reassignment's moved leases, with a claim landing among them: the
	// snapshot is the count at the first, so that claim must not reach it.
	c.observe(leaseOf("n9", true), leaseOf("n1", false), at(200))
	c.observe(leaseOf("n9", true), leaseOf("n2", true), at(201))
	c.observe(leaseOf("n9", true), leaseOf("n1", false), at(202))
	c.observe(leaseOf("n9", true), leaseOf("n1", false), at(203))
	// None of these is an owner change.
	c.observe(nil, leaseOf("n1", true), at(310))
	c.observe(leaseOf("n1", true), nil, at(320))
	c.observe(leaseOf("n1", true), leaseOf("n1", false), at(330))

	claims, moved, claimedAt, movedAt := c.snapshot()
	if want := map[string]int64{"n9": 3, "n8": 1}; !reflect.DeepEqual(claims, want) {
		t.Errorf("claims %v, want %v", claims, want)
	}
	if want := map[string]int64{"n9": 3}; !reflect.DeepEqual(moved, want) {
		t.Errorf("moved %v, want %v", moved, want)
	}
	if want := map[string]int64{"n9": 2}; !reflect.DeepEqual(claimedAt, want) {
		t.Errorf("claimed when the reassignment applied %v, want %v", claimedAt, want)
	}
	if want := map[string]int64{"n9": 200}; !reflect.DeepEqual(movedAt, want) {
		t.Errorf("reassignment times %v, want %v", movedAt, want)
	}

	var empty claimRace
	if a, b, c2, d := empty.snapshot(); a != nil || b != nil || c2 != nil || d != nil {
		t.Error("an idle recorder reported something")
	}
}

// TestEveryNodeCountsTheReassignment: the recorder is fed on every node, so
// each survivor reports the same leases moved from a stopped owner, and that
// is what the adopting leader counted as adoptions.
func TestEveryNodeCountsTheReassignment(t *testing.T) {
	ttl := 1500 * time.Millisecond
	c := newCluster(t, []string{"n1", "n2", "n3"}, ttl, nil)
	lead := c.settled()
	var owner *tnode
	for _, n := range c.nodes {
		if n != lead {
			owner = n
			break
		}
	}
	for _, id := range []string{"a", "b", "c"} {
		if _, err := acquire(t, owner, id, nil, 60, true); err != nil {
			t.Fatal(err)
		}
	}
	c.stopNode(owner)
	waitFor(t, "the leader adopting all three", ttl+6*time.Second, func() bool {
		return lead.svc.LivenessStats().Adoptions == 3
	})
	for _, n := range c.nodes {
		if n == owner {
			continue
		}
		n := n
		waitFor(t, n.name+" seeing the three moved", 5*time.Second, func() bool {
			return n.svc.LivenessStats().MovedFrom[owner.name] == 3
		})
		st := n.svc.LivenessStats()
		if st.ClaimedAtReassign[owner.name] != 0 || st.ReassignAtMs[owner.name] == 0 {
			t.Errorf("%s: claimed at the reassignment %d (want 0: no client came back), at %d ms (want a time)", n.name, st.ClaimedAtReassign[owner.name], st.ReassignAtMs[owner.name])
		}
	}
}
