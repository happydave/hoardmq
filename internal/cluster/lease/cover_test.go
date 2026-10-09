package lease

import (
	"testing"
	"time"
)

// TestCoverIsRecordedApartFromTheFinishedIndex: a served operation raises a
// node's finished index, its cover, or both, each with its own mark, and is
// refused only when it raises neither.
func TestCoverIsRecordedApartFromTheFinishedIndex(t *testing.T) {
	tb := NewTable()
	now := time.Now()
	apply := func(index uint64, op Op) Result {
		t.Helper()
		op.Kind, op.Now = OpServed, now
		v, err := tb.Apply(index, Encode([]Op{op}))
		if err != nil {
			t.Fatal(err)
		}
		return v.([]Result)[0]
	}
	if _, err := tb.Apply(2, Encode([]Op{{Kind: OpAlive, Node: "n", Now: now}})); err != nil {
		t.Fatal(err)
	}
	if r := apply(3, Op{Node: "n", Served: 10, Routes: 5}); !r.OK {
		t.Fatalf("precondition: n's first fact: %+v", r)
	}
	want := func(step string, index, mark, cover, coverMark uint64) {
		t.Helper()
		if i, m := tb.ServedFact("n"); i != index || m != mark {
			t.Fatalf("%s: finished index %d at mark %d, want %d at %d", step, i, m, index, mark)
		}
		if c, m := tb.Cover("n"); c != cover || m != coverMark {
			t.Fatalf("%s: cover %d at mark %d, want %d at %d", step, c, m, cover, coverMark)
		}
	}
	steps := []struct {
		name                          string
		op                            Op
		ok                            bool
		index, mark, cover, coverMark uint64
	}{
		{"the index alone", Op{Node: "n", Served: 12, Routes: 6}, true, 12, 6, 0, 0},
		{"the cover alone", Op{Node: "n", Cover: 20, CoverRoutes: 7}, true, 12, 6, 20, 7},
		{"neither rises", Op{Node: "n", Served: 9, Routes: 8, Cover: 15, CoverRoutes: 8}, false, 12, 6, 20, 7},
		{"both rise", Op{Node: "n", Served: 30, Routes: 9, Cover: 25, CoverRoutes: 10}, true, 30, 9, 25, 10},
	}
	for i, s := range steps {
		r := apply(uint64(4+i), s.op)
		if r.OK != s.ok || (!s.ok && r.Reason != "not a raise") {
			t.Fatalf("%s: %+v, want OK %v", s.name, r, s.ok)
		}
		want(s.name, s.index, s.mark, s.cover, s.coverMark)
	}
	if r := apply(9, Op{Node: "n9", Cover: 40, CoverRoutes: 11}); r.OK || r.Reason != "no record" {
		t.Fatalf("a cover for a node with no record: %+v, want refused", r)
	}
	if tb.Live("n9") {
		t.Fatal("a refused cover made its node read as alive")
	}

	data, err := tb.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	restored := NewTable()
	if err := restored.Restore(data); err != nil {
		t.Fatal(err)
	}
	if c, m := restored.Cover("n"); c != 25 || m != 10 {
		t.Fatalf("after a snapshot round trip: cover %d at %d, want 25 at 10", c, m)
	}
	older := NewTable()
	if err := older.Restore([]byte(`{"leases":[],"nodes":{"n":{"alive":"2026-09-25T00:00:00Z","aliveIndex":2,"servedIndex":4}}}`)); err != nil {
		t.Fatal(err)
	}
	if c, m := older.Cover("n"); c != 0 || m != 0 {
		t.Fatalf("a snapshot from before the cover: %d, %d, want none", c, m)
	}
}
