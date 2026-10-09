package shardmap

import (
	"reflect"
	"testing"
)

func TestComputeIsDeterministicAndBounded(t *testing.T) {
	a := Compute(1, []string{"n3", "n1", "n2"}, 2, 8)
	b := Compute(1, []string{"n1", "n2", "n3"}, 2, 8)
	if !reflect.DeepEqual(a, b) {
		t.Fatal("assignment depends on basis order")
	}
	if a.Shards[0].Leader != "n1" || a.Shards[1].Leader != "n2" || a.Shards[3].Leader != "n1" {
		t.Fatalf("leaders %+v", a.Shards)
	}
	if !reflect.DeepEqual(a.Shards[2].Followers, []string{"n1"}) {
		t.Fatalf("followers of shard 2: %v", a.Shards[2].Followers)
	}
	// Copies are bounded by the live set.
	one := Compute(1, []string{"solo"}, 3, 4)
	for _, s := range one.Shards {
		if s.Leader != "solo" || len(s.Followers) != 0 {
			t.Fatalf("one node: %+v", s)
		}
	}
	empty := Compute(1, nil, 2, 4)
	if empty.Shards[0].Leader != "" {
		t.Fatal("empty basis assigned a leader")
	}
	if !a.SameBasis([]string{"n2", "n3", "n1"}, 2) || a.SameBasis([]string{"n1", "n2"}, 2) || a.SameBasis([]string{"n1", "n2", "n3"}, 3) {
		t.Fatal("SameBasis")
	}
}

func TestTargetsAndHolders(t *testing.T) {
	m := Compute(1, []string{"n1", "n2", "n3"}, 2, 3)
	// Shard 0: leader n1, follower n2.
	if got := m.Targets(0, "n1"); !reflect.DeepEqual(got, []string{"n2"}) {
		t.Fatalf("owner is leader: %v", got)
	}
	if got := m.Targets(0, "n2"); !reflect.DeepEqual(got, []string{"n1"}) {
		t.Fatalf("owner is follower: %v", got)
	}
	if got := m.Targets(0, "n3"); !reflect.DeepEqual(got, []string{"n1"}) {
		t.Fatalf("owner outside the shard: %v", got)
	}
	if !m.Holds(0, "n2") || m.Holds(0, "n3") {
		t.Fatal("Holds")
	}
	if got := m.Leaders(); !reflect.DeepEqual(got, []string{"n1", "n2", "n3"}) {
		t.Fatalf("leaders %v", got)
	}
}

func TestApplyIsMonotoneAndObserved(t *testing.T) {
	tb := New(4)
	var seen []uint64
	tb.Observe(func(_, cur Map) { seen = append(seen, cur.Version) })
	if tb.Leader(2, "me") != "me" || tb.Holders(2) != nil {
		t.Fatal("empty table defaults")
	}
	r, err := tb.Apply(1, Encode(Compute(1, []string{"a", "b"}, 2, 4)))
	if err != nil || !r.(Result).Applied {
		t.Fatalf("first apply %v %v", r, err)
	}
	r, _ = tb.Apply(2, Encode(Compute(1, []string{"a"}, 2, 4)))
	if r.(Result).Applied {
		t.Fatal("same version applied")
	}
	if _, err := tb.Apply(3, Encode(Compute(2, []string{"a"}, 2, 5))); err == nil {
		t.Fatal("wrong shard count accepted")
	}
	r, _ = tb.Apply(4, Encode(Compute(2, []string{"a", "b", "c"}, 2, 4)))
	if !r.(Result).Applied || tb.Version() != 2 || !reflect.DeepEqual(seen, []uint64{1, 2}) {
		t.Fatalf("second apply %v version %d seen %v", r, tb.Version(), seen)
	}
	snap, _ := tb.Snapshot()
	other := New(4)
	if err := other.Restore(snap); err != nil || other.Version() != 2 || other.Leader(1, "x") != "b" {
		t.Fatalf("restore %v %d", err, other.Version())
	}
	first := ShardOf("ns", "client", 64)
	if first >= 64 || first != New(64).ShardOf("ns", "client") || ShardOf("ns", "client", 64) != first {
		t.Fatal("ShardOf")
	}
}

func TestRecoveryHoldsObserversUntilRecovered(t *testing.T) {
	tb := New(4)
	var seen []uint64
	tb.Observe(func(_, cur Map) { seen = append(seen, cur.Version) })
	tb.SetRecovering(true)
	if _, err := tb.Apply(1, Encode(Compute(1, []string{"a", "b"}, 2, 4))); err != nil {
		t.Fatal(err)
	}
	if _, err := tb.Apply(2, Encode(Compute(2, []string{"a", "b", "c"}, 2, 4))); err != nil {
		t.Fatal(err)
	}
	snap, _ := tb.Snapshot()
	if err := tb.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 0 || tb.Version() != 2 {
		t.Fatalf("observers ran during recovery: %v (version %d)", seen, tb.Version())
	}
	tb.SetRecovering(false)
	tb.Recovered()
	if len(seen) != 1 || seen[0] != 2 {
		t.Fatalf("one notification with the final map expected, got %v", seen)
	}
	// An empty table recovered notifies nothing.
	empty := New(4)
	empty.Observe(func(_, cur Map) { t.Fatalf("empty table notified %d", cur.Version) })
	empty.Recovered()
	// Outside recovery, behaviour is unchanged.
	if _, err := tb.Apply(3, Encode(Compute(3, []string{"a", "b"}, 2, 4))); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 2 || seen[1] != 3 {
		t.Fatalf("live notification missing: %v", seen)
	}
}
