package broker

import (
	"testing"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
)

// TestDecideRemoval: the leader's removal predicate, one case per refusal.
// Each case's figures differ from what a looser rule named beside it would
// give.
func TestDecideRemoval(t *testing.T) {
	voters := func(names ...string) []consensus.Member {
		var out []consensus.Member
		for _, n := range names {
			out = append(out, consensus.Member{Name: n, Role: consensus.Voter})
		}
		return out
	}
	set := func(names ...string) map[string]bool {
		out := map[string]bool{}
		for _, n := range names {
			out[n] = true
		}
		return out
	}
	three := voters("n1", "n2", "n3")
	five := voters("n1", "n2", "n3", "n4", "n5")
	cases := []struct {
		name string
		f    removalFacts
		want string // "" for allowed
	}{
		{"not a member", removalFacts{Target: "n9", Self: "n1", Members: three, Alive: set("n1", "n2", "n3"), Reachable: set("n1", "n2", "n3")}, "not-a-member"},
		// Without the rule the group would be left with no voter.
		{"the last voter", removalFacts{Target: "n1", Self: "n1", Members: voters("n1"), Alive: set("n1"), Reachable: set("n1"), Transfer: "n2"}, "last-voter"},
		{"a learner beside the only voter", removalFacts{Target: "n2", Self: "n1", Members: append(voters("n1"), consensus.Member{Name: "n2", Role: consensus.Learner}), Alive: set("n1"), Reachable: set("n1")}, ""},
		// n3 down: removing it leaves 2 of 2.
		{"a down voter", removalFacts{Target: "n3", Self: "n1", Members: three, Alive: set("n1", "n2"), Reachable: set("n1", "n2")}, ""},
		// Removing a live voter beside a down one leaves 1 of 2; counted
		// before the change it is 2 of 3, and counting n3 reachable 2 of 2.
		{"a live voter beside a down one", removalFacts{Target: "n2", Self: "n1", Members: three, Alive: set("n1", "n2"), Reachable: set("n1", "n2"), Leaving: true}, "quorum"},
		// Alive by the log but not reachable from the leader: not counted.
		{"a voter alive by the log but unreachable", removalFacts{Target: "n2", Self: "n1", Members: three, Alive: set("n1", "n2", "n3"), Reachable: set("n1", "n2"), Leaving: true}, "quorum"},
		{"five voters, two down, a live one", removalFacts{Target: "n3", Self: "n1", Members: five, Alive: set("n1", "n2", "n3"), Reachable: set("n1", "n2", "n3"), Leaving: true}, "quorum"},
		{"five voters, two down, a down one", removalFacts{Target: "n5", Self: "n1", Members: five, Alive: set("n1", "n2", "n3"), Reachable: set("n1", "n2", "n3")}, ""},
		{"leases owned", removalFacts{Target: "n3", Self: "n1", Members: three, Alive: set("n1", "n2"), Reachable: set("n1", "n2"), OwnedLeases: 3}, "owns-leases"},
		// The leases outrank the drain.
		{"leases owned by a live member", removalFacts{Target: "n3", Self: "n1", Members: three, Alive: set("n1", "n2", "n3"), Reachable: set("n1", "n2", "n3"), OwnedLeases: 3}, "owns-leases"},
		{"a live member not drained", removalFacts{Target: "n3", Self: "n1", Members: three, Alive: set("n1", "n2", "n3"), Reachable: set("n1", "n2", "n3")}, "not-drained"},
		{"a live member leaving", removalFacts{Target: "n3", Self: "n1", Members: three, Alive: set("n1", "n2", "n3"), Reachable: set("n1", "n2", "n3"), Leaving: true}, ""},
		{"a live member held below the level", removalFacts{Target: "n3", Self: "n1", Members: three, Alive: set("n1", "n2", "n3"), Reachable: set("n1", "n2", "n3"), Held: true}, ""},
		{"the leader with nobody to lead", removalFacts{Target: "n1", Self: "n1", Members: three, Alive: set("n1", "n2", "n3"), Reachable: set("n1", "n2", "n3"), Leaving: true}, "no-transfer-target"},
		{"the leader with a successor", removalFacts{Target: "n1", Self: "n1", Members: three, Alive: set("n1", "n2", "n3"), Reachable: set("n1", "n2", "n3"), Leaving: true, Transfer: "n2"}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ""
			if r := decideRemoval(tc.f); r != nil {
				got = r.Reason
				if r.Detail == "" {
					t.Fatal("a refusal with no sentence")
				}
			}
			if got != tc.want {
				t.Fatalf("decided %q, want %q", got, tc.want)
			}
		})
	}
}
