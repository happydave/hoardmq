package etcdraft

import (
	"testing"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
)

// TestDecideJoinCharacterisation records what the leader decides for every
// reachable state of a join request. Work item 1350 requires the current
// behaviour to be characterised before it is changed, so this table is the
// record, not only the assertion.
func TestDecideJoinCharacterisation(t *testing.T) {
	const name = "n1"
	voter := consensus.Member{ID: 7, Name: name, Role: consensus.Voter}
	learner := consensus.Member{ID: 7, Name: name, Role: consensus.Learner}

	cases := []struct {
		what      string
		known     bool
		member    consensus.Member
		joining   string
		readded   bool
		matchZero bool
		want      joinDisposition
	}{
		{"an id the configuration does not carry", false, consensus.Member{}, name, false, false, joinAddNew},
		{"an id held by a member under another name", true, consensus.Member{ID: 7, Name: "other"}, name, false, true, joinIDCollision},
		{"a member already being re-admitted", true, voter, name, true, true, joinAccept},
		{"a voter, progress reset by an election", true, voter, name, false, true, joinReadmit},
		{"a voter the leader has replicated to", true, voter, name, false, false, joinReadmit},
		{"a learner the leader has never replicated to", true, learner, name, false, true, joinAccept},
		{"a learner the leader has replicated to", true, learner, name, false, false, joinReadmit},
	}
	for _, tc := range cases {
		t.Run(tc.what, func(t *testing.T) {
			got := decideJoin(tc.known, tc.member, tc.joining, tc.readded, tc.matchZero)
			if got != tc.want {
				t.Fatalf("decideJoin(%s) = %v, want %v", tc.what, got, tc.want)
			}
		})
	}
}

// TestVoterWithAnEmptyLogIsNeverFastPathed is work item 1350's negative
// control. Every joiner arrives with an empty log, and a member that is a
// voter in the committed configuration has held a log by construction --
// promotion requires a match index above zero. Admitting one without
// resetting its progress returns it to the group as a voter with an empty
// log and no recorded vote, which is the double-vote hazard.
//
// The leader's progress for a member reads zero for every member
// immediately after an election, so the "match zero" arm is not an exotic
// state; it is the ordinary state of a freshly elected leader.
func TestVoterWithAnEmptyLogIsNeverFastPathed(t *testing.T) {
	voter := consensus.Member{ID: 7, Name: "n1", Role: consensus.Voter}
	for _, tc := range []struct {
		what      string
		matchZero bool
	}{
		{"match zero after an election", true},
		{"match above zero", false},
	} {
		t.Run(tc.what, func(t *testing.T) {
			if got := decideJoin(true, voter, "n1", false, tc.matchZero); got != joinReadmit {
				t.Fatalf("a voter with an empty log was fast-pathed: got %v, want %v", got, joinReadmit)
			}
		})
	}
}
