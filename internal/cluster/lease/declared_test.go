package lease

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
	"github.com/happydave/hoardmq/internal/store"
)

func applyAt(t *testing.T, tb *Table, index uint64, ops ...Op) []Result {
	t.Helper()
	r, err := tb.Apply(index, Encode(ops))
	if err != nil {
		t.Fatal(err)
	}
	return r.([]Result)
}

// TestDeclarationIsALogFactClearedByTheNextAliveRecord: a reassignment
// declares the node it empties, every reader sees it, a snapshot carries it,
// and the node's next alive record clears it.
func TestDeclarationIsALogFactClearedByTheNextAliveRecord(t *testing.T) {
	tb := NewTable()
	now := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)
	applyAt(t, tb, 10, Op{Kind: OpAlive, Node: "n1", Now: now}, Op{Kind: OpAlive, Node: "n2", Now: now})
	if !tb.Live("n1") || tb.Live("never-seen") {
		t.Fatalf("live: n1 %v, a node with no record %v; want true, false", tb.Live("n1"), tb.Live("never-seen"))
	}
	if r := applyAt(t, tb, 20, Op{Kind: OpReassign, Node: "n1", To: "n2", SeenAliveIndex: 10, Now: now}); !r[0].OK {
		t.Fatalf("reassign %+v", r[0])
	}
	if n, _ := tb.Node("n1"); tb.Live("n1") || n.DeadAt != 20 {
		t.Fatalf("after the reassignment n1 is live=%v, declared at %d; want false, 20", tb.Live("n1"), n.DeadAt)
	}
	if got := tb.DeclaredDead(); len(got) != 1 || got[0] != "n1" {
		t.Fatalf("declared dead %v", got)
	}
	snap, err := tb.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	restored := NewTable()
	if err := restored.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if restored.Live("n1") || !restored.Live("n2") {
		t.Fatal("the declaration did not survive a snapshot")
	}
	// A drain's hand-off empties a node that is alive.
	applyAt(t, tb, 25, Op{Kind: OpHandoff, Node: "n2", To: "n1", Now: now})
	if !tb.Live("n2") {
		t.Fatal("a hand-off declared the draining node dead")
	}
	applyAt(t, tb, 30, Op{Kind: OpAlive, Node: "n1", Now: now.Add(time.Second)})
	if n, _ := tb.Node("n1"); !tb.Live("n1") || n.AliveIndex != 30 {
		t.Fatalf("after its next alive record n1 is live=%v at index %d; want true, 30", tb.Live("n1"), n.AliveIndex)
	}
}

// TestReassignIsJudgedByLogPositionNotByStamp: the alive record that reached
// the log after the leader looked carries an older stamp than the one the
// leader saw — another leader's clock, or this one's stepped back. The node
// is alive all the same.
func TestReassignIsJudgedByLogPositionNotByStamp(t *testing.T) {
	tb := NewTable()
	now := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)
	applyAt(t, tb, 10, Op{Kind: OpAlive, Node: "n1", Now: now}, Op{Kind: OpAcquire, Namespace: "d", ClientID: "c", Node: "n1", Now: now})
	applyAt(t, tb, 11, Op{Kind: OpAlive, Node: "n1", Now: now.Add(-time.Hour)})
	if r := applyAt(t, tb, 12, Op{Kind: OpReassign, Node: "n1", To: "n2", SeenAliveIndex: 10, Now: now}); r[0].OK {
		t.Fatal("a node whose alive record followed the leader's look was reassigned")
	}
	if l, _ := tb.Lookup("d", "c"); l.Owner != "n1" || !tb.Live("n1") {
		t.Fatalf("lease %+v, n1 live %v", l, tb.Live("n1"))
	}
	if r := applyAt(t, tb, 13, Op{Kind: OpReassign, Node: "n1", To: "n2", SeenAliveIndex: 11, Now: now}); !r[0].OK || r[0].Moved != 1 {
		t.Fatalf("a reassignment that saw the node's last alive record: %+v", r[0])
	}
}

// TestConditionalAcquire: the table that knows the owner at apply refuses an
// acquire from a requester that could not reach it, while the log says it is
// alive.
func TestConditionalAcquire(t *testing.T) {
	now := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)
	for _, c := range []struct {
		name      string
		unreached []string
		declare   bool
		ok        bool
		prevAlive bool
	}{
		{"owner alive and unreached", []string{"n3", "n1"}, false, false, false},
		{"owner alive and reached", []string{"n3"}, false, true, true},
		{"owner declared dead and unreached", []string{"n1"}, true, true, false},
		{"nobody unreached", nil, false, true, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			tb := NewTable()
			applyAt(t, tb, 10, Op{Kind: OpAlive, Node: "n1", Now: now}, Op{Kind: OpAcquire, Namespace: "d", ClientID: "c", Node: "n1", Now: now})
			if c.declare {
				// The declaration moves n1's leases to n9; one is handed back
				// so that there is a lease of a declared node to acquire.
				applyAt(t, tb, 11, Op{Kind: OpReassign, Node: "n1", To: "n9", SeenAliveIndex: 10, Now: now})
				applyAt(t, tb, 12, Op{Kind: OpAcquire, Namespace: "d", ClientID: "c", Node: "n1", Now: now})
			}
			r := applyAt(t, tb, 20, Op{Kind: OpAcquire, Namespace: "d", ClientID: "c", Node: "n2", Unreached: c.unreached, Now: now})[0]
			if r.OK != c.ok || r.PrevAlive != c.prevAlive {
				t.Fatalf("result %+v, want ok=%v prevAlive=%v", r, c.ok, c.prevAlive)
			}
			if !c.ok && r.Reason != ownerAlive {
				t.Fatalf("refused for %q, want %q", r.Reason, ownerAlive)
			}
			if l, _ := tb.Lookup("d", "c"); (l.Owner == "n2") != c.ok {
				t.Fatalf("lease owner %s after ok=%v", l.Owner, c.ok)
			}
		})
	}
	// The requester's own lease is never conditional on itself.
	tb := NewTable()
	applyAt(t, tb, 10, Op{Kind: OpAlive, Node: "n2", Now: now}, Op{Kind: OpAcquire, Namespace: "d", ClientID: "c", Node: "n2", Now: now})
	if r := applyAt(t, tb, 11, Op{Kind: OpAcquire, Namespace: "d", ClientID: "c", Node: "n2", Unreached: []string{"n2"}, Now: now})[0]; !r.OK {
		t.Fatalf("a node's acquire of its own lease refused: %+v", r)
	}
}

func TestSilent(t *testing.T) {
	ttl := 2 * time.Second
	at := func(d time.Duration) int64 { return time.Unix(1000, 0).Add(d).UnixNano() }
	now := time.Unix(1000, 0).Add(10 * time.Second)
	for _, c := range []struct {
		name            string
		watching, heard time.Duration
		never           bool
		want            bool
	}{
		{"heard inside the ttl", 0, 9 * time.Second, false, false},
		{"unheard for over a ttl, watching longer", 0, 7 * time.Second, false, true},
		{"unheard for exactly a ttl", 0, 8 * time.Second, false, false},
		{"unheard for long, but the watch began inside the ttl", 9 * time.Second, time.Second, false, false},
		{"never heard, watching for over a ttl", 0, 0, true, true},
		{"never heard, the watch just began", 9 * time.Second, 0, true, false},
	} {
		heard := at(c.heard)
		if c.never {
			heard = 0
		}
		if got := silent(now, at(c.watching), heard, ttl); got != c.want {
			t.Errorf("%s: silent = %v, want %v", c.name, got, c.want)
		}
	}
}

// steppedLeader is a leader whose wall clock the test can step, with a steady
// clock that does not move with it, and a peer n9 that owns a lease.
func steppedLeader(t *testing.T, ttl time.Duration, wall *atomic.Int64) *Service {
	t.Helper()
	base := time.Now()
	core := &fakeCore{role: "leader", members: []consensus.Member{{Name: "me", Role: consensus.Voter}, {Name: "n9", Role: consensus.Voter}, {Name: "idle", Role: consensus.Voter}}}
	s, err := New(Options{Node: "me", Core: core, Machine: consensus.NewMachine(), Sessions: newFakeSessions(), TTL: ttl, Logger: quiet,
		Clock:  func() time.Time { return base.Add(time.Duration(wall.Load())) },
		Steady: func() time.Time { return base }})
	if err != nil {
		t.Fatal(err)
	}
	s.leaderSince.Store(base.Add(-time.Hour).UnixNano())
	s.observingSince.Store(base.Add(-time.Hour).UnixNano())
	if _, err := s.table.Apply(5, Encode([]Op{
		{Kind: OpAlive, Node: "n9", Now: base},
		{Kind: OpAlive, Node: "idle", Now: base},
		{Kind: OpAcquire, Namespace: "default", ClientID: "c", Node: "n9", Now: base},
	})); err != nil {
		t.Fatal(err)
	}
	return s
}

// TestWallClockStepDeclaresNobody: every alive record in the table was
// stamped before the leader's wall clock stepped three ttls ahead. The leader
// heard from both nodes a moment ago by its own steady time, and that is what
// it judges on.
func TestWallClockStepDeclaresNobody(t *testing.T) {
	ttl := 2 * time.Second
	var wall atomic.Int64
	s := steppedLeader(t, ttl, &wall)
	wall.Store(int64(3 * ttl))
	s.adopt()
	if n := pendingOf(s, OpReassign); n != 0 {
		t.Fatalf("a leader whose wall clock stepped three ttls ahead proposed %d reassignments of nodes it had just heard from", n)
	}
}

// TestLeaderDeclaresEveryLapsedNodeOnce: a node that owns nothing is declared
// like one that does, and neither is proposed again once the declaration has
// applied and nothing of theirs is left to move.
func TestLeaderDeclaresEveryLapsedNodeOnce(t *testing.T) {
	ttl := 2 * time.Second
	var wall atomic.Int64
	s := steppedLeader(t, ttl, &wall)
	unheardFor(s, "n9", 2*ttl)
	s.heard.Store("idle", s.steady().Add(-2*ttl).UnixNano())
	s.adopt()
	s.mu.Lock()
	var ops []Op
	for _, r := range s.pending {
		if r.op.Kind == OpReassign {
			ops = append(ops, r.op)
		}
	}
	s.pending = nil
	s.mu.Unlock()
	if len(ops) != 2 || ops[0].Node != "idle" || ops[1].Node != "n9" || ops[0].SeenAliveIndex != 5 {
		t.Fatalf("proposed %+v, want a reassignment of idle and of n9, each carrying alive index 5", ops)
	}
	if _, err := s.table.Apply(9, Encode(ops)); err != nil {
		t.Fatal(err)
	}
	if s.table.Live("idle") || s.table.Live("n9") {
		t.Fatal("a reassigned node still reads alive")
	}
	s.adopt()
	if n := pendingOf(s, OpReassign); n != 0 {
		t.Fatalf("proposed %d more reassignments of nodes already declared with nothing left to move", n)
	}
}

// TestLapseWaitEndsAtTheDeclaration: the wait for a previous owner that did
// not answer ends when the leader declares it dead, well inside the ttl it is
// otherwise bounded by.
func TestLapseWaitEndsAtTheDeclaration(t *testing.T) {
	ttl := 20 * time.Second
	var wall atomic.Int64
	s := steppedLeader(t, ttl, &wall)
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.waitLapse(context.Background(), "n9")
	}()
	select {
	case <-done:
		t.Fatal("the wait ended while the owner was alive by the log")
	case <-time.After(100 * time.Millisecond):
	}
	if _, err := s.table.Apply(9, Encode([]Op{{Kind: OpReassign, Node: "n9", To: "me", SeenAliveIndex: 5}})); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the wait did not end when the owner was declared dead")
	}
}

// TestSkewedFollowerTakesNoLeaseFromAnOwnerItCannotReach: the follower's
// wall clock is three ttls fast, so every alive record in its table reads as
// long lapsed. The owner is healthy, and the follower cannot reach it. The
// lease stays where it is for as long as that lasts; once the link is back the
// follower takes it and is handed the session by the owner.
func TestSkewedFollowerTakesNoLeaseFromAnOwnerItCannotReach(t *testing.T) {
	ttl := 1500 * time.Millisecond
	skew := map[string]*atomic.Int64{"n1": {}, "n2": {}, "n3": {}}
	fixtureClock = func(node string) func() time.Time {
		return func() time.Time { return time.Now().Add(time.Duration(skew[node].Load())) }
	}
	t.Cleanup(func() { fixtureClock = nil })
	c := newCluster(t, []string{"n1", "n2", "n3"}, ttl, nil)
	lead := c.settled()
	var fast, owner *tnode
	for _, name := range c.names {
		switch n := c.nodes[name]; {
		case n == lead:
		case fast == nil:
			fast = n
		default:
			owner = n
		}
	}
	res, err := acquire(t, owner, "x", nil, 300, true)
	if err != nil {
		t.Fatal(err)
	}
	owner.sessions.put("default", "x", &store.SessionState{SessionMeta: store.SessionMeta{Namespace: "default", ClientID: "x", Epoch: res.Epoch}})
	waitFor(t, "the lease on the follower", 5*time.Second, func() bool {
		l, ok := fast.svc.Table().Lookup("default", "x")
		return ok && l.Owner == owner.name
	})

	skew[fast.name].Store(int64(3 * ttl))
	fast.tr.Isolate(owner.name, true)
	owner.tr.Isolate(fast.name, true)
	time.Sleep(300 * time.Millisecond) // let the transport notice the cut
	deadline := time.Now().Add(2 * ttl)
	for time.Now().Before(deadline) {
		if _, err := acquire(t, fast, "x", nil, 300, false); err == nil {
			t.Fatal("a follower with a fast clock took a healthy owner's lease while it could not reach it")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if l, _ := lead.svc.Table().Lookup("default", "x"); l.Owner != owner.name || l.Epoch != res.Epoch {
		t.Fatalf("lease after the refusals %+v, want %s at %d", l, owner.name, res.Epoch)
	}
	if owner.svc.Fenced() || owner.sessions.fenceCount() != 0 || !owner.sessions.has("default", "x") {
		t.Fatal("the healthy owner was fenced or lost its session")
	}

	fast.tr.Isolate(owner.name, false)
	owner.tr.Isolate(fast.name, false)
	var got AcquireResult
	waitFor(t, "the takeover once the owner can be reached", 10*time.Second, func() bool {
		r, err := acquire(t, fast, "x", nil, 300, false)
		got = r
		return err == nil
	})
	if got.PrevOwner != owner.name || got.State == nil {
		t.Fatalf("takeover %+v: want the session handed over by %s", got, owner.name)
	}
}

type declaredSink struct {
	mu     sync.Mutex
	added  map[string]float64
	gauges map[string]float64
}

func (s *declaredSink) Add(name string, _ map[string]string, delta float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.added[name] += delta
}

func (s *declaredSink) Set(name string, _ map[string]string, v float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gauges[name] = v
}

// TestDeclarationsAreVisible: the proposals are counted, and the gauge counts
// the declared nodes that are still members — a decommissioned node keeps its
// declaration for good and must not hold the gauge up.
func TestDeclarationsAreVisible(t *testing.T) {
	ttl := 2 * time.Second
	var wall atomic.Int64
	s := steppedLeader(t, ttl, &wall)
	sink := &declaredSink{added: map[string]float64{}, gauges: map[string]float64{}}
	s.o.Metrics = sink
	if _, err := s.table.Apply(6, Encode([]Op{{Kind: OpAlive, Node: "gone"}})); err != nil {
		t.Fatal(err)
	}
	unheardFor(s, "n9", 2*ttl)
	s.heard.Store("gone", s.steady().Add(-2*ttl).UnixNano())
	s.adopt()
	if got := sink.added["hoardmq_lease_declarations_total"]; got != 2 {
		t.Fatalf("declarations counted %v, want 2 (n9 and the former member)", got)
	}
	if _, err := s.table.Apply(9, Encode([]Op{
		{Kind: OpReassign, Node: "n9", To: "me", SeenAliveIndex: 5},
		{Kind: OpReassign, Node: "gone", To: "me", SeenAliveIndex: 6},
	})); err != nil {
		t.Fatal(err)
	}
	s.noteDeclared()
	if got := sink.gauges["hoardmq_lease_nodes_declared_dead"]; got != 1 {
		t.Fatalf("declared-dead gauge %v, want 1: n9 is a member, the other is not", got)
	}
}
