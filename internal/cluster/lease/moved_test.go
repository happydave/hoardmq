package lease

import (
	"strings"
	"testing"
	"time"
)

// TestMovedReportsALeaseThatMovesHere: the change hook reports a lease whose
// owner changes from another node to this one, by a client's takeover or a
// drain's hand-off, and nothing else: not a first grant, a renewal, a move
// away, a release, or history replayed while recovering.
func TestMovedReportsALeaseThatMovesHere(t *testing.T) {
	now := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	s := bareService(t)
	s.o.Node = "me"
	s.o.Clock = func() time.Time { return now }
	var moves []string
	s.o.Moved = func(ns, id, from, to string) { moves = append(moves, ns+"/"+id+" "+from+">"+to) }
	s.table.onChange = s.changed
	index := uint64(10)
	step := func(want string, ops ...Op) {
		t.Helper()
		moves = nil
		index++
		applyAt(t, s.table, index, ops...)
		if got := strings.Join(moves, ", "); got != want {
			t.Fatalf("after %+v: moves %q, want %q", ops, got, want)
		}
	}
	step("", Op{Kind: OpAlive, Node: "me", Now: now}, Op{Kind: OpAlive, Node: "other", Now: now})
	step("", Op{Kind: OpAcquire, Namespace: "d", ClientID: "a", Node: "me", Now: now})
	step("", Op{Kind: OpAcquire, Namespace: "d", ClientID: "a", Node: "me", Now: now})
	step("", Op{Kind: OpAcquire, Namespace: "d", ClientID: "b", Node: "other", Now: now})
	step("d/b other>me", Op{Kind: OpAcquire, Namespace: "d", ClientID: "b", Node: "me", Now: now})
	step("", Op{Kind: OpAcquire, Namespace: "d", ClientID: "a", Node: "other", Now: now})
	step("d/a other>me", Op{Kind: OpHandoff, Node: "other", To: "me", Now: now})
	l, _ := s.table.Lookup("d", "b")
	step("", Op{Kind: OpRelease, Namespace: "d", ClientID: "b", Node: "me", Epoch: l.Epoch})

	step("", Op{Kind: OpAcquire, Namespace: "d", ClientID: "c", Node: "other", Now: now})
	s.table.SetRecovering(true)
	step("", Op{Kind: OpAcquire, Namespace: "d", ClientID: "c", Node: "me", Now: now})
	s.table.SetRecovering(false)
	if l, _ := s.table.Lookup("d", "c"); l.Owner != "me" {
		t.Fatalf("precondition: the replayed takeover did not move c (%+v)", l)
	}
}
