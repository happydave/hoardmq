package lease

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/store"
)

// TestHandedSettles: an adoption whose lease left before its import is
// counted until the new owner's served fact reaches the grant it holds the
// lease at, and no longer once the lease has moved on, its owner is not
// alive, or the bound has passed.
func TestHandedSettles(t *testing.T) {
	e := handedEntry{owner: "n3", epoch: 30}
	at := Lease{Owner: "n3", Epoch: 30}
	bound := 4 * time.Second
	for _, tc := range []struct {
		name string
		cur  Lease
		held bool
		live bool
		fact uint64
		cvr  uint64
		age  time.Duration
		want int
	}{
		{"the new owner has no fact yet", at, true, true, 0, 0, time.Second, transitOpen},
		{"the new owner's fact is below the grant", at, true, true, 29, 0, time.Second, transitOpen},
		{"the new owner's fact reaches the grant", at, true, true, 30, 0, time.Second, transitServed},
		{"the new owner's fact is past the grant", at, true, true, 31, 0, time.Second, transitServed},
		{"the new owner's cover reaches the grant, its fact below it", at, true, true, 29, 30, time.Second, transitServed},
		{"the new owner's cover is below the grant", at, true, true, 0, 29, time.Second, transitOpen},
		{"the lease moved on to another owner", Lease{Owner: "n4", Epoch: 40}, true, true, 0, 0, time.Second, transitMoved},
		{"the lease was granted again to the same owner", Lease{Owner: "n3", Epoch: 41}, true, true, 0, 0, time.Second, transitMoved},
		{"the lease is gone", Lease{}, false, true, 0, 0, time.Second, transitMoved},
		{"the new owner is not alive", at, true, false, 0, 0, time.Second, transitDead},
		{"the bound passed", at, true, true, 0, 0, bound, transitExpired},
	} {
		if got := handedSettles(e, tc.cur, tc.held, tc.live, tc.fact, tc.cvr, tc.age, bound); got != tc.want {
			t.Errorf("%s: handedSettles = %d, want %d", tc.name, got, tc.want)
		}
	}
}

// TestCoverDue: a cover is owed only with no takeover in transit open, through
// the highest one held here, below the lowest one that ended at its bound
// unless this node's finished index has passed it.
func TestCoverDue(t *testing.T) {
	for _, tc := range []struct {
		name                            string
		open                            int
		through, capped, held, finished uint64
		want                            uint64
		due                             bool
	}{
		{"a takeover still open", 1, 20, 0, 0, 0, 20, false},
		{"every takeover ended held", 0, 20, 0, 0, 0, 20, true},
		{"the cover already there", 0, 20, 0, 20, 0, 20, false},
		{"a takeover timed out below the held one", 0, 20, 15, 0, 0, 14, true},
		{"capped, and the cover already at the cap", 0, 20, 15, 14, 0, 14, false},
		{"the cap above every held one", 0, 10, 15, 0, 0, 10, true},
		{"the cap passed by the finished index", 0, 20, 15, 0, 15, 20, true},
	} {
		got, due := coverDue(tc.open, tc.through, tc.capped, tc.held, tc.finished)
		if got != tc.want || due != tc.due {
			t.Errorf("%s: coverDue = %d, %v, want %d, %v", tc.name, got, due, tc.want, tc.due)
		}
	}
}

// TestATimedOutTakeCapsTheCoverUntilTheFinishedIndexPassesIt: a takeover in
// transit that ends at its bound caps this node's cover at the grant below
// it; a second at a higher grant leaves the cap; this node's own finished
// index at the cap clears it.
func TestATimedOutTakeCapsTheCoverUntilTheFinishedIndexPassesIt(t *testing.T) {
	s, _, at := transitTable(t)
	s.o.SealTimeout = time.Millisecond
	ended := 0
	s.o.TakesEnded = func() { ended++ }
	s.transit.through.Store(40)
	s.noteTaken(key{"d", "y"}, 30)
	s.noteTaken(key{"d", "z"}, 35)
	time.Sleep(5 * time.Millisecond)
	s.settleTransit()
	if ended == 0 {
		t.Fatal("ending takeovers at their bound did not wake the cover's check")
	}
	if got, due := s.CoverDue(); got != 29 || !due {
		t.Fatalf("after takes at 30 and 35 timed out with 40 held: CoverDue = %d, %v, want 29, true", got, due)
	}
	at(3, Op{Kind: OpServed, Node: "a", Served: 30, Routes: 1})
	if got, due := s.CoverDue(); got != 40 || !due {
		t.Fatalf("with a's finished index at the cap: CoverDue = %d, %v, want 40, true", got, due)
	}
	s.transit.mu.Lock()
	capped := s.transit.capped
	s.transit.mu.Unlock()
	if capped != 0 {
		t.Fatalf("the cap is %d after the finished index reached it, want cleared", capped)
	}
}

// TestTakenSettles: a takeover of a session in transit is counted until the
// session is held here at the takeover's grant or later, or the bound
// passes.
func TestTakenSettles(t *testing.T) {
	e := takenEntry{epoch: 30}
	bound := 4 * time.Second
	for _, tc := range []struct {
		name   string
		heldAt uint64
		held   bool
		age    time.Duration
		want   int
	}{
		{"not held yet", 0, false, time.Second, transitOpen},
		{"held at an earlier grant", 29, true, time.Second, transitOpen},
		{"held at the grant", 30, true, time.Second, transitServed},
		{"held at a later grant", 31, true, time.Second, transitServed},
		{"the bound passed", 0, false, bound, transitExpired},
	} {
		if got := takenSettles(e, tc.heldAt, tc.held, tc.age, bound); got != tc.want {
			t.Errorf("%s: takenSettles = %d, want %d", tc.name, got, tc.want)
		}
	}
}

// transitTable is a lease table with nodes a and b alive and client x
// acquired by a at index 2, and a service on a over it.
func transitTable(t *testing.T) (*Service, *Table, func(idx uint64, ops ...Op)) {
	t.Helper()
	tb := NewTable()
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	at := func(idx uint64, ops ...Op) {
		t.Helper()
		for i := range ops {
			ops[i].Now = now
		}
		if _, err := tb.Apply(idx, Encode(ops)); err != nil {
			t.Fatal(err)
		}
	}
	at(1, Op{Kind: OpAlive, Node: "a", Advertise: "a:1883"}, Op{Kind: OpAlive, Node: "b", Advertise: "b:1883"})
	at(2, Op{Kind: OpAcquire, Namespace: "d", ClientID: "x", Node: "a", ExpiryInterval: 60})
	s := &Service{table: tb, tokens: NewTokens(), transit: newTransitSet(), adopting: newAdoptionSet(), log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	s.o.Node, s.o.SealTimeout, s.o.Sessions = "a", time.Hour, newFakeSessions()
	return s, tb, at
}

// TestHandedOnWaitsForTheNewOwnersFact: an adoption whose lease went to b
// before its import is counted until b's served fact reaches b's grant; a
// fact below it does not end it.
func TestHandedOnWaitsForTheNewOwnersFact(t *testing.T) {
	s, _, at := transitTable(t)
	at(5, Op{Kind: OpAcquire, Namespace: "d", ClientID: "x", Node: "b", ExpiryInterval: 60})
	s.handOn(key{"d", "x"})
	if _, n := s.AdoptionState(); n != 1 {
		t.Fatalf("precondition: %d unfinished after the hand-on, want 1", n)
	}
	at(6, Op{Kind: OpServed, Node: "b", Served: 4, Routes: 1})
	s.settleTransit()
	if _, n := s.AdoptionState(); n != 1 {
		t.Fatalf("b's fact below its grant ended the entry: %d unfinished", n)
	}
	at(7, Op{Kind: OpServed, Node: "b", Served: 6, Routes: 2})
	s.settleTransit()
	if st := s.TransitStats(); st.HandedServed != 1 || st.HandedCovered != 0 {
		t.Fatalf("b's fact at its grant: %+v, want one ended served and none on a cover", st)
	}
	if _, n := s.AdoptionState(); n != 0 {
		t.Fatalf("b's fact at its grant: %d unfinished", n)
	}
}

// TestHandedOnEndsOnTheNewOwnersCover: b's cover at its grant ends the entry
// while b's finished index is below it, and is counted as a cover.
func TestHandedOnEndsOnTheNewOwnersCover(t *testing.T) {
	s, _, at := transitTable(t)
	at(5, Op{Kind: OpAcquire, Namespace: "d", ClientID: "x", Node: "b", ExpiryInterval: 60})
	s.handOn(key{"d", "x"})
	at(6, Op{Kind: OpServed, Node: "b", Served: 4, Routes: 1, Cover: 4, CoverRoutes: 1})
	s.settleTransit()
	if _, n := s.AdoptionState(); n != 1 {
		t.Fatalf("b's cover below its grant ended the entry: %d unfinished", n)
	}
	at(7, Op{Kind: OpServed, Node: "b", Cover: 5, CoverRoutes: 2})
	s.settleTransit()
	if st := s.TransitStats(); st.HandedServed != 1 || st.HandedCovered != 1 {
		t.Fatalf("b's cover at its grant: %+v, want one ended served, on a cover", st)
	}
	if _, n := s.AdoptionState(); n != 0 {
		t.Fatalf("b's cover at its grant: %d unfinished", n)
	}
}

// TestHandedOnReadsAGrantItsTableHasNotApplied: a refused import can be
// decided on a later grant this node was told of before its table applied it.
// The entry names that grant and learns its owner when the table reaches it.
func TestHandedOnReadsAGrantItsTableHasNotApplied(t *testing.T) {
	s, _, at := transitTable(t)
	s.tokens.Note("d", "x", 5)
	s.handOn(key{"d", "x"})
	s.settleTransit()
	if _, n := s.AdoptionState(); n != 1 {
		t.Fatalf("an entry for a grant not yet applied: %d unfinished, want 1", n)
	}
	at(5, Op{Kind: OpAcquire, Namespace: "d", ClientID: "x", Node: "b", ExpiryInterval: 60})
	s.settleTransit()
	if _, n := s.AdoptionState(); n != 1 {
		t.Fatalf("the grant applied, b's fact not yet in: %d unfinished, want 1", n)
	}
	at(6, Op{Kind: OpServed, Node: "b", Served: 5, Routes: 1})
	s.settleTransit()
	if _, n := s.AdoptionState(); n != 0 || s.TransitStats().HandedServed != 1 {
		t.Fatalf("b's fact at its grant: %d unfinished, %+v", n, s.TransitStats())
	}
}

// TestTakenEndsWhenTheSessionIsHeld: a takeover of a session in transit is
// counted until the session is held here at its grant, and AdoptedThrough
// then reads the grant.
func TestTakenEndsWhenTheSessionIsHeld(t *testing.T) {
	s, _, _ := transitTable(t)
	f := s.o.Sessions.(*fakeSessions)
	s.noteTaken(key{"d", "x"}, 9)
	s.settleTransit()
	if _, n := s.AdoptionState(); n != 1 || s.AdoptedThrough() != 0 {
		t.Fatalf("before the import: %d unfinished, through %d", n, s.AdoptedThrough())
	}
	f.mu.Lock()
	f.states[f.key("d", "x")] = &store.SessionState{SessionMeta: store.SessionMeta{Namespace: "d", ClientID: "x", Epoch: 9}}
	f.mu.Unlock()
	s.settleTransit()
	if _, n := s.AdoptionState(); n != 0 || s.AdoptedThrough() != 9 || s.TransitStats().TakenHeld != 1 {
		t.Fatalf("after the import: %d unfinished, through %d, %+v", n, s.AdoptedThrough(), s.TransitStats())
	}
}
