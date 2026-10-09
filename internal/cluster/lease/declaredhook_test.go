package lease

import (
	"testing"
	"time"
)

// declaredOf applies one command to a table whose declaration hook records
// what it is told, and returns what it was told.
func declaredOf(t *testing.T, tb *Table, index uint64, ops ...Op) []string {
	t.Helper()
	var got []string
	tb.onDeclared = func(node string) { got = append(got, node) }
	defer func() { tb.onDeclared = nil }()
	applyAt(t, tb, index, ops...)
	return got
}

// TestTableReportsEachDeclarationItApplies: the shard map's driver acts on
// what this hook reports, so it must fire for a declaration and for nothing
// else — not a hand-off, not a reassignment the log refused, not an alive
// record, not history replayed during recovery.
func TestTableReportsEachDeclarationItApplies(t *testing.T) {
	now := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	fresh := func() *Table {
		tb := NewTable()
		applyAt(t, tb, 10, Op{Kind: OpAlive, Node: "n1", Now: now}, Op{Kind: OpAlive, Node: "n2", Now: now},
			Op{Kind: OpAcquire, Namespace: "d", ClientID: "c", Node: "n1", Now: now})
		return tb
	}

	tb := fresh()
	if got := declaredOf(t, tb, 20, Op{Kind: OpReassign, Node: "n1", To: "n2", SeenAliveIndex: 10, Now: now}); len(got) != 1 || got[0] != "n1" {
		t.Fatalf("a reassignment that declares n1 reported %v, want [n1]", got)
	}
	if tb.Live("n1") {
		t.Fatal("precondition: n1 is still live after its declaration")
	}

	tb = fresh()
	if got := declaredOf(t, tb, 20, Op{Kind: OpHandoff, Node: "n1", To: "n2", Now: now}); len(got) != 0 {
		t.Fatalf("a hand-off reported %v; a drain declares nothing", got)
	}
	if l, _ := tb.Lookup("d", "c"); l.Owner != "n2" {
		t.Fatalf("precondition: the hand-off did not move the lease (%+v)", l)
	}

	tb = fresh()
	applyAt(t, tb, 15, Op{Kind: OpAlive, Node: "n1", Now: now.Add(time.Second)})
	if got := declaredOf(t, tb, 20, Op{Kind: OpReassign, Node: "n1", To: "n2", SeenAliveIndex: 10, Now: now}); len(got) != 0 {
		t.Fatalf("a reassignment refused because n1 renewed after the leader looked reported %v", got)
	}
	if !tb.Live("n1") {
		t.Fatal("precondition: the refused reassignment declared n1")
	}

	tb = fresh()
	if got := declaredOf(t, tb, 20, Op{Kind: OpAlive, Node: "n1", Now: now.Add(time.Second)}); len(got) != 0 {
		t.Fatalf("an alive record reported %v", got)
	}

	tb = fresh()
	tb.SetRecovering(true)
	got := declaredOf(t, tb, 20, Op{Kind: OpReassign, Node: "n1", To: "n2", SeenAliveIndex: 10, Now: now})
	tb.SetRecovering(false)
	if len(got) != 0 {
		t.Fatalf("a recovering table reported %v", got)
	}
	if tb.Live("n1") {
		t.Fatal("precondition: the replayed reassignment did not declare n1")
	}

	// Two reassignments of one node in one command are one declaration.
	tb = fresh()
	if got := declaredOf(t, tb, 20, Op{Kind: OpReassign, Node: "n1", To: "n2", SeenAliveIndex: 10, Now: now},
		Op{Kind: OpReassign, Node: "n1", To: "n2", SeenAliveIndex: 10, Now: now}); len(got) != 1 {
		t.Fatalf("one command declaring n1 twice reported %v, want it once", got)
	}
}
