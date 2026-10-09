package routing

import (
	"reflect"
	"testing"
)

func TestMergeVersionsAndTombstones(t *testing.T) {
	tb := NewTable("a")
	ch := tb.Merge([]Entry{{Namespace: "d", Filter: "t/1", Owner: "b", Version: 5, Count: 1}})
	if len(ch) != 1 {
		t.Fatal("first entry not applied")
	}
	// An older version does not replace; an equal version with a tombstone
	// does; a newer live version replaces the tombstone.
	if ch := tb.Merge([]Entry{{Namespace: "d", Filter: "t/1", Owner: "b", Version: 4, Count: 1}}); len(ch) != 0 {
		t.Fatal("older version applied")
	}
	if ch := tb.Merge([]Entry{{Namespace: "d", Filter: "t/1", Owner: "b", Version: 5, Tombstone: true}}); len(ch) != 1 {
		t.Fatal("tombstone at equal version not applied")
	}
	if nodes, _ := tb.Match("d", "t/1"); len(nodes) != 0 {
		t.Fatalf("tombstoned entry still matches: %v", nodes)
	}
	if ch := tb.Merge([]Entry{{Namespace: "d", Filter: "t/1", Owner: "b", Version: 5, Count: 1}}); len(ch) != 0 {
		t.Fatal("live entry beat a tombstone at equal version")
	}
	if ch := tb.Merge([]Entry{{Namespace: "d", Filter: "t/1", Owner: "b", Version: 6, Count: 1}}); len(ch) != 1 {
		t.Fatal("newer live entry not applied over the tombstone")
	}
	if nodes, _ := tb.Match("d", "t/1"); !reflect.DeepEqual(nodes, []string{"b"}) {
		t.Fatalf("match %v", nodes)
	}
	// Own entries never match; hidden owners are excluded; forgotten owners
	// are gone.
	tb.Merge([]Entry{{Namespace: "d", Filter: "t/+", Owner: "a", Version: 1, Count: 1}, {Namespace: "d", Filter: "#", Owner: "c", Version: 1, Count: 1}})
	nodes, _ := tb.Match("d", "t/1")
	if !reflect.DeepEqual(nodes, []string{"b", "c"}) {
		t.Fatalf("match with wildcards %v", nodes)
	}
	tb.Hide("c", true)
	if nodes, _ := tb.Match("d", "t/1"); !reflect.DeepEqual(nodes, []string{"b"}) {
		t.Fatalf("hidden owner still matches: %v", nodes)
	}
	tb.Hide("c", false)
	tb.Forget("c")
	if nodes, _ := tb.Match("d", "t/1"); !reflect.DeepEqual(nodes, []string{"b"}) {
		t.Fatalf("forgotten owner still matches: %v", nodes)
	}
	if _, ok := tb.Lookup("c", "d", "#"); ok {
		t.Fatal("forgotten entry still held")
	}
	local, remote := tb.Count()
	if local != 1 || remote != 1 {
		t.Fatalf("counts %d %d", local, remote)
	}
	if own := tb.Own(); len(own) != 1 || own[0].Filter != "t/+" {
		t.Fatalf("own %+v", own)
	}
}

func TestDollarTopicsAndGroups(t *testing.T) {
	tb := NewTable("a")
	tb.Merge([]Entry{
		{Namespace: "d", Filter: "#", Owner: "b", Version: 1, Count: 1},
		{Namespace: "d", Filter: "$SYS/#", Owner: "c", Version: 1, Count: 1},
		{Namespace: "d", Filter: "$share/g/t/#", Owner: "b", Version: 1, Count: 2},
		{Namespace: "d", Filter: "$share/g/t/#", Owner: "c", Version: 1, Count: 3},
		{Namespace: "d", Filter: "$share/g/t/#", Owner: "a", Version: 1, Count: 3},
	})
	if nodes, _ := tb.Match("d", "$SYS/x"); !reflect.DeepEqual(nodes, []string{"c"}) {
		t.Fatalf("$ topic matched a leading wildcard: %v", nodes)
	}
	nodes, groups := tb.Match("d", "t/1")
	if !reflect.DeepEqual(nodes, []string{"b"}) || len(groups) != 1 {
		t.Fatalf("match %v %+v", nodes, groups)
	}
	g := groups[0]
	if g.Key != "$share/g/t/#" || g.Members["a"] != 3 || g.Members["c"] != 3 || g.Members["b"] != 2 {
		t.Fatalf("group %+v", g)
	}
	// Most members wins; ties by lowest name.
	if g.Owner() != "a" {
		t.Fatalf("owner %s", g.Owner())
	}
	if !reflect.DeepEqual(g.Nodes(), []string{"a", "b", "c"}) {
		t.Fatalf("nodes %v", g.Nodes())
	}
	tb.Merge([]Entry{{Namespace: "d", Filter: "$share/g/t/#", Owner: "a", Version: 2, Tombstone: true}})
	_, groups = tb.Match("d", "t/1")
	if groups[0].Owner() != "c" {
		t.Fatalf("owner after tombstone %s", groups[0].Owner())
	}
}

// A full sync is authoritative about absence: an entry the owner no longer
// carries is withdrawn, which is the only way a restarted node can retire the
// entries its previous incarnation published.
func TestMergeFullWithdrawsAbsentEntries(t *testing.T) {
	tb := NewTable("a")
	tb.Merge([]Entry{
		{Namespace: "d", Filter: "gone", Owner: "b", Version: 5, Count: 1},
		{Namespace: "d", Filter: "kept", Owner: "b", Version: 5, Count: 1},
		{Namespace: "d", Filter: "other", Owner: "c", Version: 5, Count: 1},
	})
	// b restarts and now holds only "kept". Its snapshot version is above
	// everything its previous incarnation published.
	_, withdrawn := tb.MergeFull("b", []Entry{{Namespace: "d", Filter: "kept", Owner: "b", Version: 20, Count: 1}}, 10)
	if len(withdrawn) != 1 || withdrawn[0].Filter != "gone" {
		t.Fatalf("expected only \"gone\" withdrawn, got %v", withdrawn)
	}
	if nodes, _ := tb.Match("d", "gone"); len(nodes) != 0 {
		t.Fatalf("withdrawn entry still matches: %v", nodes)
	}
	if nodes, _ := tb.Match("d", "kept"); len(nodes) != 1 {
		t.Fatal("an entry present in the sync was withdrawn")
	}
	if nodes, _ := tb.Match("d", "other"); len(nodes) != 1 {
		t.Fatal("another owner's entry was withdrawn")
	}
}

// An entry published after the sync was composed carries a higher version and
// must survive it, so a withdrawal cannot outrun a newer advertisement.
func TestMergeFullKeepsEntriesAboveTheSnapshot(t *testing.T) {
	tb := NewTable("a")
	tb.Merge([]Entry{{Namespace: "d", Filter: "new", Owner: "b", Version: 30, Count: 1}})
	if _, withdrawn := tb.MergeFull("b", nil, 10); len(withdrawn) != 0 {
		t.Fatalf("an entry newer than the snapshot was withdrawn: %v", withdrawn)
	}
	if nodes, _ := tb.Match("d", "new"); len(nodes) != 1 {
		t.Fatal("the newer entry no longer matches")
	}
	// An empty sync whose snapshot is above it does withdraw it.
	if _, withdrawn := tb.MergeFull("b", nil, 40); len(withdrawn) != 1 {
		t.Fatal("an empty authoritative sync did not withdraw the entry")
	}
}
