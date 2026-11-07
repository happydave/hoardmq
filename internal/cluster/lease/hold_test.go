package lease

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
	"github.com/happydave/hoardmq/internal/store"
)

// TestTableHoldsBelowItsBuild: the table holds the machine exactly while the
// cluster's level is above the build it is given; with none it never holds.
func TestTableHoldsBelowItsBuild(t *testing.T) {
	tb := NewTable()
	if held, _ := tb.Holds(); held {
		t.Fatal("a table with no build holds")
	}
	build := uint64(1)
	tb.build = func() uint64 { return build }
	if held, _ := tb.Holds(); held {
		t.Fatal("level 1 under build 1 holds")
	}
	applyAt(t, tb, 3, Op{Kind: OpSetLevel, Level: 2})
	if held, why := tb.Holds(); !held || why == "" {
		t.Fatalf("level 2 under build 1: holds %v %q, want a hold with its reason", held, why)
	}
	build = 2
	if held, _ := tb.Holds(); held {
		t.Fatal("level 2 under build 2 holds")
	}
}

// TestRaiseIsACommandOfItsOwn: a raise sharing its command is refused on
// every node alike, the other operations applied; alone it applies.
func TestRaiseIsACommandOfItsOwn(t *testing.T) {
	tb := NewTable()
	now := time.Now()
	r := applyAt(t, tb, 4, Op{Kind: OpAlive, Node: "n1", Now: now, Level: 2}, Op{Kind: OpSetLevel, Level: 2})
	if !r[0].OK || r[1].OK || r[1].Reason != notAlone || r[1].Index != 4 {
		t.Fatalf("batched raise: %+v, want the alive applied and the raise refused %q at 4", r, notAlone)
	}
	if got := tb.Level(); got != 1 {
		t.Fatalf("level %d after a batched raise, want 1", got)
	}
	if r := applyAt(t, tb, 5, Op{Kind: OpSetLevel, Level: 2}); !r[0].OK || tb.Level() != 2 {
		t.Fatalf("a raise alone: %+v, level %d", r, tb.Level())
	}
}

// TestAliveCarriesTheLatestLevel: a node put back on an older build reads as
// that build's level, so it is below the cluster's and a raise waits for it.
func TestAliveCarriesTheLatestLevel(t *testing.T) {
	tb := NewTable()
	now := time.Now()
	applyAt(t, tb, 1, Op{Kind: OpAlive, Node: "n1", Now: now, Level: 2}, Op{Kind: OpAlive, Node: "n2", Now: now, Level: 2})
	applyAt(t, tb, 2, Op{Kind: OpSetLevel, Level: 2})
	applyAt(t, tb, 3, Op{Kind: OpAlive, Node: "n1", Now: now.Add(time.Second), Level: 1})
	if min, ok := tb.MinAdvertisedLevel([]string{"n1", "n2"}); !ok || min != 1 {
		t.Fatalf("after n1's alive at 1: min %d (known %v), want 1", min, ok)
	}
	if !tb.MemberBelow("n1") || tb.MemberBelow("n2") || tb.MemberBelow("n9") {
		t.Fatalf("below: n1 %v n2 %v n9 %v; want true false false", tb.MemberBelow("n1"), tb.MemberBelow("n2"), tb.MemberBelow("n9"))
	}
}

func TestProposable(t *testing.T) {
	for _, k := range []OpKind{OpAcquire, OpDisconnected, OpRelease, OpReassign, OpWillDone, OpHandoff, OpSetLevel, OpServed} {
		if proposable(k, true) {
			t.Errorf("kind %d proposable below the level", k)
		}
		if !proposable(k, false) {
			t.Errorf("kind %d refused at the level", k)
		}
	}
	if !proposable(OpAlive, true) {
		t.Error("the alive record refused below the level")
	}
}

func TestMemberBelow(t *testing.T) {
	for _, c := range []struct {
		adv, cluster uint64
		want         bool
	}{{0, 1, false}, {1, 1, false}, {0, 2, true}, {1, 2, true}, {2, 2, false}, {3, 2, false}} {
		if got := memberBelow(c.adv, c.cluster); got != c.want {
			t.Errorf("memberBelow(%d, %d) = %v, want %v", c.adv, c.cluster, got, c.want)
		}
	}
}

func TestTransferTarget(t *testing.T) {
	v := func(name string, caught bool) consensus.Member {
		return consensus.Member{Name: name, Role: consensus.Voter, CaughtUp: caught}
	}
	below := func(n string) bool { return n == "a" || n == "c" }
	for _, c := range []struct {
		name    string
		members []consensus.Member
		want    string
	}{
		{"an at-level voter over a below-level one", []consensus.Member{v("me", true), v("a", true), v("b", true)}, "b"},
		{"only below-level voters", []consensus.Member{v("me", true), v("c", true), v("a", true)}, "a"},
		{"not one that is behind", []consensus.Member{v("me", true), v("b", false), v("a", true)}, "a"},
		{"not a learner", []consensus.Member{v("me", true), {Name: "b", Role: consensus.Learner, CaughtUp: true}}, ""},
		{"not itself", []consensus.Member{v("me", true)}, ""},
	} {
		if got := transferTarget("me", c.members, below); got != c.want {
			t.Errorf("%s: %q, want %q", c.name, got, c.want)
		}
	}
}

// belowService is a follower whose table has raised the level to 2 while it
// runs build 1: one offline lease of its own with an armed Will, one lease of
// another node's whose session it holds, and a session with no lease.
func belowService(t *testing.T, recovering bool) (*Service, *fakeCore, *fakeSessions) {
	t.Helper()
	core := &fakeCore{role: "follower", leader: "n9"}
	core.recovering.Store(recovering)
	sess := newFakeSessions()
	s, err := New(Options{Node: "me", Core: core, Machine: consensus.NewMachine(), Sessions: sess, TTL: 2 * time.Second, TakeoverTimeout: time.Second, Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	applyAt(t, s.table, 1, Op{Kind: OpAlive, Node: "me", Now: now, Level: 2}, Op{Kind: OpAlive, Node: "n9", Now: now, Level: 2})
	applyAt(t, s.table, 2, Op{Kind: OpAcquire, Namespace: "default", ClientID: "mine", Node: "me", Will: willFor("w/mine", 1), ExpiryInterval: 60, Now: now})
	applyAt(t, s.table, 3, Op{Kind: OpDisconnected, Namespace: "default", ClientID: "mine", Node: "me", Epoch: 2, ExpiresAt: now.Add(time.Hour), WillArmed: true, Now: now})
	applyAt(t, s.table, 4, Op{Kind: OpAcquire, Namespace: "default", ClientID: "moved", Node: "n9", Now: now})
	sess.put("default", "moved", &store.SessionState{SessionMeta: store.SessionMeta{Namespace: "default", ClientID: "moved", Epoch: 1}})
	sess.put("default", "unleased", &store.SessionState{SessionMeta: store.SessionMeta{Namespace: "default", ClientID: "unleased", Epoch: 1}})
	l, _ := s.table.Lookup("default", "mine")
	l.DisconnectedAt = time.Now()
	s.startTimers(l, time.Time{})
	applyAt(t, s.table, 5, Op{Kind: OpSetLevel, Level: 2})
	s.SetBuildLevel(1)
	return s, core, sess
}

// TestFallingBelowTheLevelStopsWhatTheTableWouldDo: the node disconnects its
// clients and disarms the Will it had armed, since its table no longer
// learns that the client came back elsewhere.
func TestFallingBelowTheLevelStopsWhatTheTableWouldDo(t *testing.T) {
	s, _, sess := belowService(t, false)
	s.noteLevel()
	if sess.fenceCount() != 1 {
		t.Fatalf("clients disconnected %d times at the fall, want 1", sess.fenceCount())
	}
	time.Sleep(1500 * time.Millisecond)
	if n := sess.willCount(); n != 0 {
		t.Fatalf("a node below the level published %d Wills from its frozen table", n)
	}
	if sess.has("default", "moved") == false {
		t.Fatal("a live fall reconciled sessions: recovery had already done it")
	}
}

// TestBelowTheLevelRecoveryOnlySeals: a node below the level that is still
// recovering, and one whose reconciliation runs below the level (a snapshot
// installed while it holds), seal what their table names another owner of
// and do nothing else: no eviction, no timer, no adoption.
func TestBelowTheLevelRecoveryOnlySeals(t *testing.T) {
	for _, via := range []string{"fall while recovering", "recovered below the level"} {
		t.Run(via, func(t *testing.T) {
			s, _, sess := belowService(t, true)
			s.cancelTimers(key{"default", "mine"})
			if via == "fall while recovering" {
				s.noteLevel()
			} else {
				s.recovered()
			}
			if _, tok, ok := sess.Sealed("default", "moved"); !ok || tok != 4 {
				t.Fatalf("the session owned elsewhere: sealed %v at %d, want sealed at 4", ok, tok)
			}
			if !sess.has("default", "unleased") {
				t.Fatal("a session with no lease in a frozen table was evicted")
			}
			s.mu.Lock()
			timers := len(s.timers)
			s.mu.Unlock()
			if timers != 0 {
				t.Fatalf("%d timers armed below the level", timers)
			}
			if _, unfinished := s.AdoptionState(); unfinished != 0 {
				t.Fatalf("%d adoptions queued below the level", unfinished)
			}
		})
	}
}

// TestBelowTheLevelProposesOnlyItsAliveRecord: every other proposal is
// refused on the node; the alive record still reaches the core.
func TestBelowTheLevelProposesOnlyItsAliveRecord(t *testing.T) {
	s, core, _ := belowService(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := s.submit(ctx, Op{Kind: OpRelease, Namespace: "default", ClientID: "mine", Node: "me", Epoch: 2}); !errors.Is(err, ErrBelowLevel) {
		t.Fatalf("a release below the level: %v, want ErrBelowLevel", err)
	}
	if _, err := s.ProposeCommand(ctx, consensus.KindReserved+2, []byte("{}")); !errors.Is(err, ErrBelowLevel) {
		t.Fatalf("a records command below the level: %v, want ErrBelowLevel", err)
	}
	if n := pendingOf(s, OpRelease); n != 0 {
		t.Fatalf("%d releases pending", n)
	}
	before := core.calls.Load()
	if !s.renew() || core.calls.Load() != before+1 {
		t.Fatalf("the alive record did not reach the core: calls %d → %d", before, core.calls.Load())
	}
}

// TestHeldLeaderHandsOver: a leader whose core holds asks for a transfer to
// an at-level voter on its tick; one that does not hold asks for none.
func TestHeldLeaderHandsOver(t *testing.T) {
	core := &fakeCore{role: "leader", members: []consensus.Member{
		{Name: "me", Role: consensus.Voter, CaughtUp: true},
		{Name: "n8", Role: consensus.Voter, CaughtUp: true},
		{Name: "n9", Role: consensus.Voter, CaughtUp: true},
	}}
	s, err := New(Options{Node: "me", Core: core, Machine: consensus.NewMachine(), Sessions: newFakeSessions(), TTL: 2 * time.Second, Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	applyAt(t, s.table, 1, Op{Kind: OpAlive, Node: "n8", Now: now, Level: 1}, Op{Kind: OpAlive, Node: "n9", Now: now, Level: 2})
	applyAt(t, s.table, 2, Op{Kind: OpSetLevel, Level: 2})
	s.transferIfHeld()
	if len(core.transfers) != 0 {
		t.Fatalf("a leader that does not hold transferred to %v", core.transfers)
	}
	core.held.Store(&consensus.Hold{Index: 3, Reason: "test"})
	s.transferIfHeld()
	if len(core.transfers) != 1 || core.transfers[0] != "n9" {
		t.Fatalf("a held leader transferred to %v, want [n9]", core.transfers)
	}
}

// TestLeaderTakesTheLeasesOfAMemberBelowTheLevel: the leader proposes the
// drain's hand-off of a below-level member's leases to itself; of a member at
// the level, or one owning nothing, none.
func TestLeaderTakesTheLeasesOfAMemberBelowTheLevel(t *testing.T) {
	core := &fakeCore{role: "leader", members: []consensus.Member{
		{Name: "me", Role: consensus.Voter}, {Name: "n8", Role: consensus.Voter}, {Name: "n9", Role: consensus.Voter},
	}}
	s, err := New(Options{Node: "me", Core: core, Machine: consensus.NewMachine(), Sessions: newFakeSessions(), TTL: 2 * time.Second, TakeoverTimeout: time.Second, Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	applyAt(t, s.table, 1, Op{Kind: OpAlive, Node: "me", Now: now, Level: 2}, Op{Kind: OpAlive, Node: "n8", Now: now, Level: 2}, Op{Kind: OpAlive, Node: "n9", Now: now, Level: 2})
	applyAt(t, s.table, 2, Op{Kind: OpAcquire, Namespace: "default", ClientID: "c8", Node: "n8", Now: now})
	applyAt(t, s.table, 3, Op{Kind: OpSetLevel, Level: 2})
	s.SetBuildLevel(2)
	s.handOffBelow()
	time.Sleep(50 * time.Millisecond)
	if n := pendingOf(s, OpHandoff); n != 0 {
		t.Fatalf("%d hand-offs with every member at the level", n)
	}
	applyAt(t, s.table, 4, Op{Kind: OpAlive, Node: "n9", Now: now, Level: 1})
	s.handOffBelow()
	time.Sleep(50 * time.Millisecond)
	if n := pendingOf(s, OpHandoff); n != 0 {
		t.Fatalf("%d hand-offs of a below-level member owning nothing", n)
	}
	applyAt(t, s.table, 5, Op{Kind: OpAlive, Node: "n8", Now: now, Level: 1})
	s.handOffBelow()
	waitFor(t, "the hand-off of n8's leases", 2*time.Second, func() bool { return pendingOf(s, OpHandoff) == 1 })
	s.mu.Lock()
	op := s.pending[len(s.pending)-1].op
	s.mu.Unlock()
	if op.Node != "n8" || op.To != "me" {
		t.Fatalf("hand-off %+v, want n8's leases to me", op)
	}
}

// TestBelowTheLevelImportsNoAdoption: an adoption queued before the fall
// fetches and imports nothing while the node is below the level.
func TestBelowTheLevelImportsNoAdoption(t *testing.T) {
	core := &fakeCore{role: "follower", leader: "n9"}
	sess := newFakeSessions()
	fetched := 0
	s, err := New(Options{Node: "me", Core: core, Machine: consensus.NewMachine(), Sessions: sess, TTL: 2 * time.Second, TakeoverTimeout: time.Second, Logger: quiet,
		Fallback: func(context.Context, string, string, string) Fetched {
			fetched++
			return Fetched{State: &store.SessionState{SessionMeta: store.SessionMeta{Namespace: "default", ClientID: "a"}}, Answered: true}
		}})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	applyAt(t, s.table, 1, Op{Kind: OpAlive, Node: "me", Now: now, Level: 2})
	applyAt(t, s.table, 2, Op{Kind: OpAcquire, Namespace: "default", ClientID: "a", Node: "me", Now: now})
	applyAt(t, s.table, 3, Op{Kind: OpSetLevel, Level: 2})
	s.SetBuildLevel(1)
	k := key{"default", "a"}
	s.adopting.begin(k, adoptJob{epoch: 2, recovered: true})
	s.adoptState(k)
	if fetched != 0 || sess.importedCount() != 0 {
		t.Fatalf("below the level an adoption fetched %d times and imported %d sessions", fetched, sess.importedCount())
	}
}

// TestRaiseIsProposedAlone: a pending batch holding a raise is proposed as
// the raise alone and the rest together.
func TestRaiseIsProposedAlone(t *testing.T) {
	batch := []request{{op: Op{Kind: OpAlive}}, {op: Op{Kind: OpSetLevel, Level: 2}}, {op: Op{Kind: OpAcquire}}}
	got := commands(batch)
	if len(got) != 2 || len(got[0]) != 1 || got[0][0].op.Kind != OpSetLevel || len(got[1]) != 2 {
		t.Fatalf("commands %+v, want the raise alone, then the alive and the acquire", got)
	}
	if got := commands(batch[:1]); len(got) != 1 || len(got[0]) != 1 {
		t.Fatalf("a batch with no raise: %+v, want one command", got)
	}
}

// TestRaiseWaitsForEveryMemberWhereverListed: the raiser proposes nothing
// while any member's latest advertisement is below the level, whichever place
// the membership lists it in, and proposes once every member's is at it.
func TestRaiseWaitsForEveryMemberWhereverListed(t *testing.T) {
	for _, order := range [][]string{{"n8", "me", "n9"}, {"me", "n9", "n8"}, {"n9", "n8", "me"}} {
		t.Run(order[0]+"-first", func(t *testing.T) {
			var members []consensus.Member
			for _, n := range order {
				members = append(members, consensus.Member{Name: n, Role: consensus.Voter})
			}
			core := &fakeCore{role: "leader", members: members}
			s, err := New(Options{Node: "me", Core: core, Machine: consensus.NewMachine(), Sessions: newFakeSessions(), TTL: 2 * time.Second, Logger: quiet})
			if err != nil {
				t.Fatal(err)
			}
			s.SetBuildLevel(2)
			now := time.Now()
			low := order[0]
			if low == "me" {
				low = order[1]
			}
			for _, n := range order {
				lv := uint64(2)
				if n == low {
					lv = 1
				}
				applyAt(t, s.table, 1, Op{Kind: OpAlive, Node: n, Now: now, Level: lv})
			}
			s.raiseLevel()
			if n := pendingOf(s, OpSetLevel); n != 0 {
				t.Fatalf("raised with %s at level 1", low)
			}
			applyAt(t, s.table, 2, Op{Kind: OpAlive, Node: low, Now: now, Level: 2})
			s.raiseLevel()
			if n := pendingOf(s, OpSetLevel); n != 1 {
				t.Fatalf("%d raises with every member at 2, want 1", n)
			}
		})
	}
}
