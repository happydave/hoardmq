package routing

import (
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
)

// barePlane is a plane with no transport, for driving the receiver directly.
func barePlane(t *testing.T) (*Plane, *labelMetrics) {
	t.Helper()
	m := newLabelMetrics()
	return New(Options{Node: "a", Logger: quiet, Metrics: m}), m
}

// partOf encodes one part of b's set.
func partOf(t *testing.T, snapshot, cover uint64, index uint32, last bool, filters ...string) []byte {
	t.Helper()
	sp := setPart{Snapshot: snapshot, Cover: cover, Index: index, Last: last, Entries: []Entry{}}
	for _, f := range filters {
		sp.Entries = append(sp.Entries, Entry{Namespace: "default", Filter: f, Owner: "b", Version: snapshot - 1, Count: 1})
	}
	body, err := json.Marshal(sp)
	if err != nil {
		t.Fatal(err)
	}
	return append([]byte{routePart}, body...)
}

func delta(t *testing.T, number, version uint64, filter string) []byte {
	t.Helper()
	body, err := json.Marshal([]Entry{{Namespace: "default", Filter: filter, Owner: "b", Version: version, Count: 1, Batch: number}})
	if err != nil {
		t.Fatal(err)
	}
	return append([]byte{routeDelta}, body...)
}

func synced(p *Plane, peer string) bool {
	_, ok := p.SyncAges()[peer]
	return ok
}

// A set whose parts stop before its last proves nothing: a node holding a
// gap in a peer's deltas keeps it until a set completes.
func TestAbandonedSetProvesNothing(t *testing.T) {
	p, _ := barePlane(t)
	p.receiveRoutes("b", delta(t, 10, 5, "x/1"))
	p.receiveRoutes("b", delta(t, 12, 6, "x/2")) // 11 missing: a gap
	if p.routesHeld("b", 12) {
		t.Fatal("a gap left the proof whole")
	}
	p.receiveRoutes("b", partOf(t, 100, 12, 0, false, "x/1"))
	if p.routesHeld("b", 12) {
		t.Fatal("part 0 of a set proved the peer's routes before the set completed")
	}
	p.receiveRoutes("b", partOf(t, 100, 12, 2, true, "x/2")) // part 1 missing
	if p.routesHeld("b", 12) || synced(p, "b") {
		t.Fatal("a set missing a part proved the peer's routes")
	}
}

// A set missing a part is never applied: its absence would withdraw what the
// missing part carried.
func TestSetMissingAPartIsNotApplied(t *testing.T) {
	p, m := barePlane(t)
	p.receiveRoutes("b", delta(t, 10, 5, "keep/1"))
	p.receiveRoutes("b", partOf(t, 100, 10, 0, false, "a/1"))
	p.receiveRoutes("b", partOf(t, 100, 10, 2, true, "a/3"))
	if synced(p, "b") {
		t.Fatal("a set applied with part 1 missing")
	}
	if e, ok := p.Table().Lookup("b", "default", "keep/1"); !ok || e.Tombstone {
		t.Fatalf("an entry the missing part could carry was withdrawn: %+v", e)
	}
	if n := m.get("hoardmq_route_sets_abandoned_total", "side", "receive", "reason", "part-missing"); n != 1 {
		t.Fatalf("part-missing counted %v", n)
	}
	// The next complete set applies.
	p.receiveRoutes("b", partOf(t, 101, 10, 0, false, "a/1"))
	p.receiveRoutes("b", partOf(t, 101, 10, 1, true, "a/2", "keep/1"))
	if !synced(p, "b") || !p.routesHeld("b", 10) {
		t.Fatal("a complete set was not applied")
	}
}

// A part of an older set, arriving late on a replaced link, is dropped
// without touching the newer set in progress.
func TestStalePartDoesNotDiscardANewerSet(t *testing.T) {
	p, m := barePlane(t)
	p.receiveRoutes("b", partOf(t, 200, 10, 0, false, "n/1"))
	p.receiveRoutes("b", partOf(t, 100, 10, 1, false, "o/2"))
	p.receiveRoutes("b", partOf(t, 200, 10, 1, true, "n/2"))
	if !synced(p, "b") {
		t.Fatal("the newer set did not complete after a stale part")
	}
	if n := m.get("hoardmq_route_sets_abandoned_total", "side", "receive", "reason", "stale"); n != 1 {
		t.Fatalf("stale counted %v", n)
	}
}

// A complete set at or below the snapshot of the one applied, in either form,
// is dropped: a duplicate or older set would withdraw what arrived since and
// move the proof back to its cover.
func TestStaleCompleteSetIsDropped(t *testing.T) {
	p, _ := barePlane(t)
	p.receiveRoutes("b", partOf(t, 200, 20, 0, true, "n/1"))
	p.receiveRoutes("b", delta(t, 21, 300, "n/2"))
	if !p.routesHeld("b", 21) {
		t.Fatal("the proof does not reach 21")
	}
	body, _ := json.Marshal(fullSync{Snapshot: 200, Batch: 10, Entries: []Entry{}})
	p.receiveRoutes("b", append([]byte{routeFull}, body...))
	if e, ok := p.Table().Lookup("b", "default", "n/1"); !ok || e.Tombstone {
		t.Fatal("a stale set withdrew a newer set's entry")
	}
	if !p.routesHeld("b", 21) {
		t.Fatal("a stale set moved the proof back")
	}
}

// A delta that arrives while a set is in flight is merged and proved at once
// and, chained from the set's cover, keeps the proof whole when the set
// completes: the set's cover alone would move it back below the delta.
func TestDeltaDuringASetKeepsTheProofWhole(t *testing.T) {
	p, _ := barePlane(t)
	p.receiveRoutes("b", partOf(t, 100, 10, 0, false, "s/1"))
	p.receiveRoutes("b", delta(t, 11, 150, "d/1"))
	p.receiveRoutes("b", partOf(t, 100, 10, 1, true, "s/2"))
	p.receiveRoutes("b", delta(t, 12, 151, "d/2"))
	if !p.routesHeld("b", 12) {
		t.Fatal("a delta proved during the set read as a gap after it")
	}
	if e, ok := p.Table().Lookup("b", "default", "d/1"); !ok || e.Tombstone {
		t.Fatal("the set withdrew a delta's entry above its snapshot")
	}
}

// A set completes acknowledged: the receiver owes the sender a held, and a
// held received is recorded.
func TestCompleteSetOwesAHeld(t *testing.T) {
	p, _ := barePlane(t)
	p.receiveRoutes("b", partOf(t, 100, 10, 0, true, "s/1"))
	p.mu.Lock()
	owed := p.heldOwed["b"]
	p.mu.Unlock()
	if owed != 100 {
		t.Fatalf("held owed %d", owed)
	}
	body, _ := json.Marshal(setHeld{Snapshot: 77})
	p.receiveRoutes("c", append([]byte{routeHeld}, body...))
	p.mu.Lock()
	ack := p.acks["c"]
	p.mu.Unlock()
	if ack != 77 {
		t.Fatalf("held recorded %d", ack)
	}
}

// An unknown kind is counted and dropped, never merged as a delta.
func TestUnknownKindIsDropped(t *testing.T) {
	p, m := barePlane(t)
	body, _ := json.Marshal([]Entry{{Namespace: "default", Filter: "u/1", Owner: "b", Version: 5, Count: 1, Batch: 3}})
	p.receiveRoutes("b", append([]byte{9}, body...))
	if _, ok := p.Table().Lookup("b", "default", "u/1"); ok {
		t.Fatal("an unknown kind was merged")
	}
	if n := m.get("hoardmq_route_updates_dropped_total", "reason", "unknown-kind"); n != 1 {
		t.Fatalf("unknown-kind counted %v", n)
	}
}

// Parts of a set stay under the part size, an entry larger than it travels
// alone, the last is flagged, and the parts carry every entry once in order.
func TestPartsCarryTheSetUnderThePartSize(t *testing.T) {
	p, _ := barePlane(t)
	p.partSize = 4 << 10
	var entries []Entry
	for i := 0; i < 400; i++ {
		entries = append(entries, Entry{Namespace: "default", Filter: frameFilter(i), Owner: "a", Version: uint64(i + 1), Count: 1})
	}
	big := Entry{Namespace: "default", Filter: "big/" + strings.Repeat("x", 6<<10), Owner: "a", Version: 999, Count: 1}
	entries = append(entries[:200], append([]Entry{big}, entries[200:]...)...)
	c := &composition{snapshot: 1000, cover: 7, entries: entries}
	var got []Entry
	pos := 0
	for index := uint32(0); pos < len(entries); index++ {
		payload, next := p.encodePart(c, pos, index)
		var sp setPart
		if err := json.Unmarshal(payload[1:], &sp); err != nil {
			t.Fatal(err)
		}
		alone := len(sp.Entries) == 1 && sp.Entries[0].Filter == big.Filter
		if len(payload) > p.partSize && !alone {
			t.Fatalf("part %d is %d bytes, over %d, with %d entries", index, len(payload), p.partSize, len(sp.Entries))
		}
		if sp.Index != index || sp.Snapshot != 1000 || sp.Cover != 7 || sp.Last != (next == len(entries)) {
			t.Fatalf("part %d header %+v", index, sp)
		}
		got = append(got, sp.Entries...)
		pos = next
	}
	if len(got) != len(entries) {
		t.Fatalf("parts carried %d of %d entries", len(got), len(entries))
	}
	for i := range got {
		if got[i].Filter != entries[i].Filter {
			t.Fatalf("entry %d is %s, want %s", i, got[i].Filter, entries[i].Filter)
		}
	}
}

// An empty own table is still a set: part 0, flagged last, with no entries.
func TestEmptySetIsOnePart(t *testing.T) {
	p, _ := barePlane(t)
	payload, next := p.encodePart(&composition{snapshot: 5, cover: 1}, 0, 0)
	var sp setPart
	if err := json.Unmarshal(payload[1:], &sp); err != nil {
		t.Fatal(err)
	}
	if next != 0 || !sp.Last || sp.Index != 0 || len(sp.Entries) != 0 {
		t.Fatalf("empty set's part %+v, next %d", sp, next)
	}
}

// EntryFits decides at the frame limit by the entry's largest encoding,
// with JSON's six-byte escapes counted.
func TestEntryFits(t *testing.T) {
	const limit = 65536
	room := limit - entryEnvelope - partEnvelope - len("default") - len("n1")
	cases := []struct {
		name   string
		filter string
		fits   bool
	}{
		{"at the limit", strings.Repeat("a", room), true},
		{"one over", strings.Repeat("a", room+1), false},
		{"escaped bytes counted at six", strings.Repeat("<", room/6+1), false},
		{"escaped bytes under", strings.Repeat("<", room/6), true},
		{"short", "t/1", true},
	}
	for _, tc := range cases {
		if got := EntryFits("default", tc.filter, "n1", limit); got != tc.fits {
			t.Errorf("%s: fits %v, want %v", tc.name, got, tc.fits)
		}
	}
	if !EntryFits("default", strings.Repeat("a", 1<<20), "n1", 0) {
		t.Error("no limit refused an entry")
	}
}

// pruneBelow prunes nothing while the membership is unreadable or a member
// has acknowledged nothing, and otherwise below the lowest acknowledgement.
func TestPruneBelow(t *testing.T) {
	cases := []struct {
		name    string
		members []string
		acks    map[string]uint64
		below   uint64
		ok      bool
	}{
		{"membership unreadable", nil, map[string]uint64{"b": 9}, 0, false},
		{"a member never acknowledged", []string{"a", "b", "c"}, map[string]uint64{"b": 9}, 0, false},
		{"the lowest acknowledgement", []string{"a", "b", "c"}, map[string]uint64{"b": 9, "c": 4}, 4, true},
		{"a configured peer that is not a member", []string{"a", "b"}, map[string]uint64{"b": 9, "c": 1}, 9, true},
		{"this node alone", []string{"a"}, nil, 0, false},
	}
	for _, tc := range cases {
		below, ok := pruneBelow(tc.members, "a", tc.acks)
		if below != tc.below || ok != tc.ok {
			t.Errorf("%s: %d %v, want %d %v", tc.name, below, ok, tc.below, tc.ok)
		}
	}
}

// The prune deletes only own tombstones below its bound, never a live entry
// or a tombstone at or above it.
func TestPruneOwnTombstones(t *testing.T) {
	tb := NewTable("a")
	var v atomic.Uint64
	tb.MergeOwn(Entry{Namespace: "d", Filter: "live", Owner: "a", Count: 1}, &v)                 // 1
	tb.MergeOwn(Entry{Namespace: "d", Filter: "old", Owner: "a", Tombstone: true}, &v)           // 2
	tb.MergeOwn(Entry{Namespace: "d", Filter: "new", Owner: "a", Tombstone: true}, &v)           // 3
	tb.Merge([]Entry{{Namespace: "d", Filter: "peer", Owner: "b", Version: 1, Tombstone: true}}) // a peer's
	if n := tb.PruneOwnTombstones(3); n != 1 {
		t.Fatalf("pruned %d", n)
	}
	for _, f := range []string{"live", "new"} {
		if _, ok := tb.Lookup("a", "d", f); !ok {
			t.Errorf("%s pruned", f)
		}
	}
	if _, ok := tb.Lookup("a", "d", "old"); ok {
		t.Error("old kept")
	}
	if _, ok := tb.Lookup("b", "d", "peer"); !ok {
		t.Error("a peer's tombstone pruned")
	}
	if tb.OwnTombstones() != 1 {
		t.Errorf("own tombstones %d", tb.OwnTombstones())
	}
}

// A complete set removes the sender's stored tombstones it does not carry,
// below its snapshot.
func TestSetRemovesThePeersTombstonesItNoLongerCarries(t *testing.T) {
	p, _ := barePlane(t)
	p.Table().Merge([]Entry{{Namespace: "default", Filter: "gone", Owner: "b", Version: 5, Tombstone: true},
		{Namespace: "default", Filter: "later", Owner: "b", Version: 500, Tombstone: true}})
	p.receiveRoutes("b", partOf(t, 100, 10, 0, true, "s/1"))
	if _, ok := p.Table().Lookup("b", "default", "gone"); ok {
		t.Error("a tombstone the set does not carry, below its snapshot, was kept")
	}
	if _, ok := p.Table().Lookup("b", "default", "later"); !ok {
		t.Error("a tombstone above the snapshot was removed")
	}
}
