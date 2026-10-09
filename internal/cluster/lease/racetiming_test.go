package lease

import (
	"testing"
	"time"
)

// TestRaceTimestamps: the three timestamps WI 1514 reads the adoption regime
// from. What matters is that a "first" is never overwritten — the whole
// measurement is the *order* of two firsts, so a later event moving one would
// invert the answer rather than merely blur it.
func TestRaceTimestamps(t *testing.T) {
	base := time.Date(2026, 9, 16, 2, 0, 0, 0, time.UTC)
	now := base
	s := &Service{table: NewTable(), timers: map[key]*adopted{}}
	s.o.Node = "me"
	s.o.Clock = func() time.Time { return now }

	adopt := func(id string) {
		s.changed(&Lease{Namespace: "default", ClientID: id, Owner: "dead"},
			&Lease{Namespace: "default", ClientID: id, Owner: "me", Online: false})
	}
	clientTake := func(id string) {
		s.changed(&Lease{Namespace: "default", ClientID: id, Owner: "other"},
			&Lease{Namespace: "default", ClientID: id, Owner: "me", Online: true})
	}

	st := s.LivenessStats()
	if st.FirstAdoptMs != 0 || st.LastAdoptMs != 0 || st.FirstClientTakeMs != 0 {
		t.Fatalf("a service that has done nothing reports timestamps: %+v", st)
	}

	adopt("a")
	first := base.UnixMilli()
	now = base.Add(3 * time.Second)
	adopt("b")
	now = base.Add(5 * time.Second)
	clientTake("c")
	firstTake := now.UnixMilli()
	now = base.Add(9 * time.Second)
	adopt("d")
	clientTake("e")

	st = s.LivenessStats()
	if st.FirstAdoptMs != first {
		t.Errorf("first adoption %d, want %d — a later adoption moved it, which would invert the ordering the regime is read from", st.FirstAdoptMs, first)
	}
	if st.LastAdoptMs != base.Add(9*time.Second).UnixMilli() {
		t.Errorf("last adoption %d, want the 9 s one", st.LastAdoptMs)
	}
	if st.FirstClientTakeMs != firstTake {
		t.Errorf("first client takeover %d, want %d", st.FirstClientTakeMs, firstTake)
	}
	if st.FirstAdoptMs > st.LastAdoptMs {
		t.Error("first adoption is after the last")
	}
	// The quantity B3 reads: positive means the reassignment landed before any
	// client took its own lease.
	if got := st.FirstClientTakeMs - st.FirstAdoptMs; got != 5000 {
		t.Errorf("takeover minus adoption = %d ms, want 5000", got)
	}
}

// TestRaceTimestampsDistinguishTheTwoCases: an adoption is an *offline* lease
// arriving, a client takeover an *online* one. Confusing them would make the
// two timestamps the same event and the measurement vacuous.
func TestRaceTimestampsDistinguishTheTwoCases(t *testing.T) {
	now := time.Date(2026, 9, 16, 2, 0, 0, 0, time.UTC)
	s := &Service{table: NewTable(), timers: map[key]*adopted{}}
	s.o.Node = "me"
	s.o.Clock = func() time.Time { return now }

	// Only a client takeover: no adoption should be recorded.
	s.changed(&Lease{Namespace: "default", ClientID: "x", Owner: "other"},
		&Lease{Namespace: "default", ClientID: "x", Owner: "me", Online: true})
	if st := s.LivenessStats(); st.FirstAdoptMs != 0 {
		t.Errorf("a client takeover was recorded as an adoption: %d", st.FirstAdoptMs)
	}

	// A lease this node already owned coming online is neither: no client took
	// it from anyone.
	s2 := &Service{table: NewTable(), timers: map[key]*adopted{}}
	s2.o.Node = "me"
	s2.o.Clock = func() time.Time { return now }
	s2.changed(&Lease{Namespace: "default", ClientID: "y", Owner: "me"},
		&Lease{Namespace: "default", ClientID: "y", Owner: "me", Online: true})
	if st := s2.LivenessStats(); st.FirstClientTakeMs != 0 {
		t.Errorf("a client reconnecting to the node that already owned its lease was counted as a takeover: %d", st.FirstClientTakeMs)
	}
}

// TestFirstTakeFromIsPerPreviousOwner is the fix for the flaw the twenty-run
// batch exposed: the aggregate timestamp fires for a client re-homing from the
// *other survivor*, which happens at about the lease ttl in every run whatever
// the adoption regime, so it measured the wrong thing entirely.
func TestFirstTakeFromIsPerPreviousOwner(t *testing.T) {
	base := time.Date(2026, 9, 16, 3, 0, 0, 0, time.UTC)
	now := base
	s := &Service{table: NewTable(), timers: map[key]*adopted{}}
	s.o.Node = "me"
	s.o.Clock = func() time.Time { return now }

	take := func(id, from string) {
		s.changed(&Lease{Namespace: "default", ClientID: id, Owner: from},
			&Lease{Namespace: "default", ClientID: id, Owner: "me", Online: true})
	}

	// A client re-homes from the surviving peer first — this is the event that
	// used to set the only timestamp there was.
	take("a", "survivor")
	survivorAt := now.UnixMilli()
	// Then, twenty seconds later, the first of the crashed node's leases moves.
	now = base.Add(20 * time.Second)
	take("b", "crashed")
	crashedAt := now.UnixMilli()
	// More of each afterwards must not move either first.
	now = base.Add(30 * time.Second)
	take("c", "survivor")
	take("d", "crashed")

	st := s.LivenessStats()
	if st.FirstTakeFrom == nil {
		t.Fatal("no per-owner timestamps recorded")
	}
	if got := st.FirstTakeFrom["survivor"]; got != survivorAt {
		t.Errorf("first from the survivor: %d, want %d", got, survivorAt)
	}
	if got := st.FirstTakeFrom["crashed"]; got != crashedAt {
		t.Errorf("first from the crashed node: %d, want %d", got, crashedAt)
	}
	// The aggregate is the earlier of the two, which is exactly why it could
	// not answer the question: it reports the survivor re-home.
	if st.FirstClientTakeMs != survivorAt {
		t.Errorf("aggregate %d, want the survivor's %d", st.FirstClientTakeMs, survivorAt)
	}
	if st.FirstTakeFrom["crashed"]-st.FirstClientTakeMs != 20000 {
		t.Error("the per-owner entry and the aggregate are not distinguishable, which is the defect this fixes")
	}
	// A node that was never a previous owner has no entry, rather than a zero
	// that reads as an epoch time.
	if _, ok := st.FirstTakeFrom["never"]; ok {
		t.Error("an owner that never lost a lease has an entry")
	}
}
