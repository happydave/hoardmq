package lease

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
	"github.com/happydave/hoardmq/internal/packet"
)

// ownedElsewhere builds a service with no flush loop, so a proposal stays in
// s.pending, and a table in which n1 is alive and owns client x.
func ownedElsewhere(t *testing.T, tr *fakeTransport) *Service {
	t.Helper()
	now := time.Now()
	s, err := New(Options{Node: "me", Core: &fakeCore{role: "follower"}, Machine: consensus.NewMachine(), Transport: tr, Sessions: newFakeSessions(), TTL: time.Second, Logger: quiet, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.table.Apply(1, Encode([]Op{
		{Kind: OpAlive, Node: "n1", Now: now},
		{Kind: OpAlive, Node: "n3", Now: now},
		{Kind: OpAcquire, Namespace: "default", ClientID: "x", Node: "n1", Now: now},
	})); err != nil {
		t.Fatal(err)
	}
	return s
}

func pendingOps(s *Service) []request {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]request(nil), s.pending...)
}

// TestAnAcquireRefusedAtApplyIsTimed: an acquire the log refuses because the
// owner is alive is counted with the time its CONNECT waited for the verdict.
func TestAnAcquireRefusedAtApplyIsTimed(t *testing.T) {
	s := ownedElsewhere(t, &fakeTransport{up: []string{"n1", "n3"}})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := s.AcquireFrom(ctx, "default", "x", nil, 60, false, Requester{})
		done <- err
	}()
	waitFor(t, "the acquire to be proposed", 2*time.Second, func() bool { return len(pendingOps(s)) >= 1 })
	time.Sleep(2 * time.Millisecond)
	pendingOps(s)[0].done <- Result{Reason: ownerAlive}
	var ref *Refusal
	if err := <-done; !errors.As(err, &ref) || ref.Reason != packet.ServerUnavailable {
		t.Fatalf("acquire refused at apply returned %v, want Server Unavailable", err)
	}
	st := s.LivenessStats()
	if st.OwnerAliveApplied != 1 || st.OwnerAliveAppliedMs <= 0 || st.OwnerAliveLocal != 0 {
		t.Fatalf("refused at apply %d over %.3f ms, locally %d; want 1, more than 0, and 0", st.OwnerAliveApplied, st.OwnerAliveAppliedMs, st.OwnerAliveLocal)
	}
}

func TestRefuseUnreachableOwner(t *testing.T) {
	cases := []struct {
		name      string
		owner     string
		live      bool
		unreached []string
		want      bool
	}{
		{"this node's own lease", "me", true, []string{"me", "n1"}, false},
		{"owner alive and unreached", "n1", true, []string{"n3", "n1"}, true},
		{"owner alive and reached", "n1", true, []string{"n3"}, false},
		{"owner declared dead and unreached", "n1", false, []string{"n1"}, false},
		{"owner with no record and unreached", "gone", false, []string{"gone"}, false},
		{"nobody unreached", "n1", true, nil, false},
	}
	for _, c := range cases {
		if got := refuseUnreachableOwner(c.owner, "me", c.live, c.unreached); got != c.want {
			t.Errorf("%s: refuse %v, want %v", c.name, got, c.want)
		}
	}
}

// TestUnreachableLiveOwnerIsRefusedWithoutAProposal: the refusal the log
// would give is given from this node's table, and nothing is proposed.
func TestUnreachableLiveOwnerIsRefusedWithoutAProposal(t *testing.T) {
	s := ownedElsewhere(t, &fakeTransport{up: []string{"n3"}, down: []string{"n1"}})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := s.AcquireFrom(ctx, "default", "x", nil, 60, false, Requester{})
	var ref *Refusal
	if !errors.As(err, &ref) || ref.Reason != packet.ServerUnavailable {
		t.Fatalf("acquire from an unreachable live owner returned %v, want Server Unavailable", err)
	}
	if n := len(pendingOps(s)); n != 0 {
		t.Fatalf("%d operations proposed for an acquire this node could refuse itself", n)
	}
	if st := s.LivenessStats(); st.OwnerAliveLocal != 1 || st.OwnerAliveApplied != 0 || st.PendingOwnerNotes != 0 {
		t.Fatalf("refused locally %d, at apply %d, notes held %d; want 1, 0 and 0: nothing was proposed, so nothing waits", st.OwnerAliveLocal, st.OwnerAliveApplied, st.PendingOwnerNotes)
	}
}

// proposedAcquire runs an acquire that must reach the log, and returns the
// operation it proposed; the acquire itself ends on its context.
func proposedAcquire(t *testing.T, s *Service) Op {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = s.AcquireFrom(ctx, "default", "x", nil, 60, false, Requester{})
	}()
	t.Cleanup(func() { cancel(); <-done })
	waitFor(t, "the acquire to be proposed", 2*time.Second, func() bool { return len(pendingOps(s)) >= 1 })
	return pendingOps(s)[0].op
}

func TestDeclaredDeadUnreachableOwnerIsStillProposed(t *testing.T) {
	s := ownedElsewhere(t, &fakeTransport{up: []string{"n3"}, down: []string{"n1"}})
	// The leader's verdict moves n1's leases to n3 and declares n1 dead.
	if _, err := s.table.Apply(2, Encode([]Op{{Kind: OpReassign, Node: "n1", To: "n3", SeenAliveIndex: 1, Now: time.Now()}})); err != nil {
		t.Fatal(err)
	}
	// An acquire n1 sent before it died applies after the verdict: a lease
	// owned by a node that is declared dead and unreachable.
	if _, err := s.table.Apply(3, Encode([]Op{{Kind: OpAcquire, Namespace: "default", ClientID: "x", Node: "n1", Now: time.Now()}})); err != nil {
		t.Fatal(err)
	}
	if l, _ := s.table.Lookup("default", "x"); l.Owner != "n1" || s.table.Live("n1") {
		t.Fatalf("x owned by %s, n1 alive %v; want n1 and declared dead", l.Owner, s.table.Live("n1"))
	}
	if op := proposedAcquire(t, s); op.Kind != OpAcquire || !contains(op.Unreached, "n1") {
		t.Fatalf("proposed %+v, want an acquire naming n1 unreached", op)
	}
	if n := s.LivenessStats().OwnerAliveLocal; n != 0 {
		t.Fatalf("refused locally %d times with the owner declared dead", n)
	}
}

func TestReachableOwnerIsStillProposed(t *testing.T) {
	s := ownedElsewhere(t, &fakeTransport{up: []string{"n1", "n3"}})
	if op := proposedAcquire(t, s); op.Kind != OpAcquire || len(op.Unreached) != 0 {
		t.Fatalf("proposed %+v, want an acquire naming nobody unreached", op)
	}
	if owner, takeover := s.waitNotes.owner(key{"default", "x"}, s.steady(), s.o.SealTimeout); owner != "n1" || !takeover {
		t.Fatalf("the note once the acquire is proposed names %q (takeover %v); want n1, from this takeover", owner, takeover)
	}
}

// TestATakeoverNotesNoOwnerThatIsThisNode: an acquire whose table names this
// node, or no owner, writes no note, and a pass never names this node, whatever
// a note says.
func TestATakeoverNotesNoOwnerThatIsThisNode(t *testing.T) {
	s := ownedElsewhere(t, &fakeTransport{up: []string{"n1", "n3"}})
	k := key{"default", "x"}
	s.noteTaking(k, "me")
	s.noteTaking(k, "")
	if n := s.LivenessStats().PendingOwnerNotes; n != 0 {
		t.Fatalf("%d notes held after noting this node and no owner; want 0", n)
	}
	s.waitNotes.note(k, "me", s.steady(), s.o.SealTimeout, true)
	if got := s.waitingOn(k); got != "" {
		t.Fatalf("a pass names %q from a note naming this node; want nobody", got)
	}
}
