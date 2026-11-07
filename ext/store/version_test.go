package store

import (
	"encoding/json"
	"testing"

	"github.com/happydave/hoardmq/ext/mqtt"
)

// TestMergeRetained: the decision as a table, each refusing row included.
func TestMergeRetained(t *testing.T) {
	v := func(e, n uint64) Version { return Version{E: e, N: n} }
	for _, c := range []struct {
		name     string
		held     Version
		exists   bool
		in       Version
		handOver bool
		want     bool
	}{
		{"into an absent topic", Version{}, false, Version{}, false, true},
		{"into an absent topic, hand-over", Version{}, false, Version{}, true, true},
		{"a higher epoch", v(2, 1), true, v(3, 1), false, true},
		{"a higher number in the epoch", v(2, 1), true, v(2, 2), false, true},
		{"stamped over unversioned", Version{}, true, v(1, 1), true, true},
		{"refused: a lower epoch", v(3, 1), true, v(2, 9), false, false},
		{"refused: a lower number", v(2, 5), true, v(2, 4), false, false},
		{"refused: an equal stamped version", v(2, 5), true, v(2, 5), false, false},
		{"refused: unversioned over stamped", v(1, 1), true, Version{}, false, false},
		{"unversioned over unversioned, by arrival", Version{}, true, Version{}, false, true},
		{"refused: unversioned over unversioned on a hand-over", Version{}, true, Version{}, true, false},
	} {
		if got := MergeRetained(c.held, c.exists, c.in, c.handOver); got != c.want {
			t.Errorf("%s: MergeRetained = %v, want %v", c.name, got, c.want)
		}
	}
}

func retainedOf(st *State, ns, topic string) string {
	for _, m := range st.Retained {
		if m.Namespace == ns && m.Topic == topic {
			return string(m.Payload)
		}
	}
	return ""
}

func apply(t *testing.T, st *State, r Record) {
	t.Helper()
	if err := st.Apply(r.Type, r.Payload); err != nil {
		t.Fatal(err)
	}
}

// TestPreChangeJournalReloadsAsBefore: unversioned records, as every journal
// before versions holds, apply by arrival: the later message replaces, and a
// delete removes.
func TestPreChangeJournalReloadsAsBefore(t *testing.T) {
	st := NewState()
	m := func(p string) *mqtt.Message { return &mqtt.Message{Namespace: "ns", Topic: "r", Payload: []byte(p)} }
	apply(t, st, RetainRecord(m("a")))
	apply(t, st, RetainRecord(m("b")))
	if got := retainedOf(st, "ns", "r"); got != "b" {
		t.Fatalf("after two unversioned messages the state holds %q, want b", got)
	}
	apply(t, st, RetainRecord(&mqtt.Message{Namespace: "ns", Topic: "r"}))
	if _, held, _ := st.RetainedHeld("ns", "r"); held {
		t.Fatal("an unversioned delete left the topic held")
	}
}

// TestVersionedRecords: a stamped message outranks an older one whatever the
// order; a clear is a tombstone that an older message does not undo; a copy
// drop removes a tombstone too; a compare-and-delete takes only the named
// tombstone.
func TestVersionedRecords(t *testing.T) {
	st := NewState()
	m := func(p string) *mqtt.Message { return &mqtt.Message{Namespace: "ns", Topic: "r", Payload: []byte(p)} }
	apply(t, st, RetainRecordVersioned(m("new"), Version{E: 2, N: 7}))
	apply(t, st, RetainRecordVersioned(m("old"), Version{E: 1, N: 9}))
	if got := retainedOf(st, "ns", "r"); got != "new" {
		t.Fatalf("an older stamped message replaced a newer one: %q", got)
	}
	apply(t, st, RetainRecordVersioned(&mqtt.Message{Namespace: "ns", Topic: "r"}, Version{E: 2, N: 8}))
	if v, held, tomb := st.RetainedHeld("ns", "r"); !held || !tomb || v != (Version{E: 2, N: 8}) || retainedOf(st, "ns", "r") != "" {
		t.Fatalf("the clear: held %v tombstone %v at %v, message %q", held, tomb, v, retainedOf(st, "ns", "r"))
	}
	apply(t, st, RetainRecordVersioned(m("new"), Version{E: 2, N: 7}))
	if retainedOf(st, "ns", "r") != "" {
		t.Fatal("an older message undid the clear")
	}
	if st.DropTombstoneIf("ns", "r", Version{E: 2, N: 7}) {
		t.Fatal("a compare-and-delete took a tombstone at another version")
	}
	apply(t, st, RetainRecord(&mqtt.Message{Namespace: "ns", Topic: "r"}))
	if _, held, _ := st.RetainedHeld("ns", "r"); held {
		t.Fatal("a copy drop left the tombstone")
	}
	apply(t, st, RetainRecordVersioned(&mqtt.Message{Namespace: "ns", Topic: "s"}, Version{E: 3, N: 1}))
	if !st.DropTombstoneIf("ns", "s", Version{E: 3, N: 1}) {
		t.Fatal("a compare-and-delete did not take the named tombstone")
	}
}

// TestVersionedStateRoundTrips: a checkpoint keeps versions and tombstones;
// a version's numbers past 2^53 survive JSON as typed uint64.
func TestVersionedStateRoundTrips(t *testing.T) {
	st := NewState()
	big := Version{E: 7, N: 1_800_000_000_000_000_123}
	apply(t, st, RetainRecordVersioned(&mqtt.Message{Namespace: "ns", Topic: "a", Payload: []byte("x")}, big))
	apply(t, st, RetainRecordVersioned(&mqtt.Message{Namespace: "ns", Topic: "b"}, Version{E: 7, N: 2}))
	b, err := json.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	var back State
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if v, held, tomb := back.RetainedHeld("ns", "a"); !held || tomb || v != big {
		t.Fatalf("a's version after the round trip: %v held %v tombstone %v", v, held, tomb)
	}
	if v, held, tomb := back.RetainedHeld("ns", "b"); !held || !tomb || v != (Version{E: 7, N: 2}) {
		t.Fatalf("b's tombstone after the round trip: %v held %v tombstone %v", v, held, tomb)
	}
	if v, clear := RecordVersion(RetainRecordVersioned(&mqtt.Message{Namespace: "ns", Topic: "b"}, Version{E: 7, N: 2})); !clear || v != (Version{E: 7, N: 2}) {
		t.Fatalf("RecordVersion of a clear: %v %v", v, clear)
	}
	if v, clear := RecordVersion(RetainRecord(&mqtt.Message{Namespace: "ns", Topic: "b"})); clear || !v.Zero() {
		t.Fatalf("RecordVersion of a copy drop: %v %v", v, clear)
	}
}

// TestRetainClearIsAlwaysStamped: a clear's record at the zero Version is the
// copy drop's, so no tombstone at (0, 0) is ever written; a stamped clear
// carries its version.
func TestRetainClearIsAlwaysStamped(t *testing.T) {
	zero, err := EncodeRetainClear("default", "t", Version{})
	if err != nil {
		t.Fatal(err)
	}
	drop, err := EncodeRetainDrop("default", "t")
	if err != nil {
		t.Fatal(err)
	}
	if string(zero.Payload) != string(drop.Payload) || zero.Type != drop.Type {
		t.Fatalf("a clear at the zero Version is %s, want the drop's %s", zero.Payload, drop.Payload)
	}
	if _, clear := RecordVersion(zero); clear {
		t.Fatal("a clear at the zero Version reads back as a clear")
	}
	v := Version{E: 3, N: 7}
	stamped, err := EncodeRetainClear("default", "t", v)
	if err != nil {
		t.Fatal(err)
	}
	if got, clear := RecordVersion(stamped); !clear || got != v {
		t.Fatalf("a stamped clear reads back as %v (clear %v), want %v", got, clear, v)
	}
}
