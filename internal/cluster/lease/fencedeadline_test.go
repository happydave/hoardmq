package lease

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
	"github.com/happydave/hoardmq/internal/cluster/transport"
)

// TestDecideFenceRefuses holds the fence predicate at the arguments it must
// refuse: a record exactly a ttl old, or just under, is no decision; a lapsed
// record under no settled leader with a reachable quorum defers.
func TestDecideFenceRefuses(t *testing.T) {
	ttl := time.Second
	now := time.Unix(5000, 0)
	settled := now.Add(-2 * ttl).UnixNano()
	follower := consensus.Status{Role: "follower", Leader: "l"}
	gone := func(n string) transport.PeerInfo {
		return transport.PeerInfo{Name: n, State: transport.Connecting, DownForMs: 60000}
	}
	cases := []struct {
		name       string
		age        time.Duration
		st         consensus.Status
		knownSince int64
		peers      []transport.PeerInfo
		want       FenceOutcome
	}{
		{"exactly a ttl old", ttl, follower, settled, nil, FenceNone},
		{"a nanosecond under the ttl", ttl - time.Nanosecond, follower, settled, nil, FenceNone},
		{"a nanosecond over the ttl, a settled leader", ttl + time.Nanosecond, follower, settled, nil, FenceNow},
		{"lapsed, no leader, a quorum reachable", 2 * ttl, consensus.Status{Role: "candidate"}, 0, nil, FenceDefer},
		{"lapsed, a leader known for half a ttl, a quorum reachable", 2 * ttl, follower, now.Add(-ttl / 2).UnixNano(), nil, FenceDefer},
		{"lapsed, no leader, no quorum", 2 * ttl, consensus.Status{Role: "candidate"}, 0, []transport.PeerInfo{gone("n2"), gone("n3")}, FenceNow},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			voters := []string{"me", "n2", "n3"}
			hasTransport := c.peers != nil
			if !hasTransport {
				voters = nil
			}
			got, rec := decideFence(now, now.Add(-c.age), ttl, c.st, c.knownSince, "me", voters, c.peers, hasTransport)
			if got != c.want || rec.Outcome != c.want {
				t.Fatalf("decideFence = %s (record %s), want %s", got, rec.Outcome, c.want)
			}
		})
	}
}

// TestDeclarableRefuses holds the leader's declaration predicate at the
// arguments it must refuse: silence of a ttl, of a ttl and less than the
// margin, of exactly the ttl and the margin, and a watch no longer than that.
func TestDeclarableRefuses(t *testing.T) {
	ttl := 2 * time.Second
	if m := declarationMargin(ttl); m != 62500*time.Microsecond {
		t.Fatalf("margin at 2 s is %s, want 62.5ms", m)
	}
	now := time.Unix(5000, 0)
	ago := func(d time.Duration) int64 { return now.Add(-d).UnixNano() }
	long := ago(time.Hour)
	cases := []struct {
		name            string
		watching, heard int64
		want            bool
	}{
		{"heard a ttl ago", long, ago(ttl), false},
		{"heard 2.05 s ago, past the ttl and inside the margin", long, ago(2050 * time.Millisecond), false},
		{"heard exactly the ttl and the margin ago", long, ago(2062500 * time.Microsecond), false},
		{"heard a nanosecond past the ttl and the margin", long, ago(2062500*time.Microsecond + time.Nanosecond), true},
		{"never heard, watching for 2.05 s", ago(2050 * time.Millisecond), 0, false},
		{"never heard, watching a nanosecond past the ttl and the margin", ago(2062500*time.Microsecond + time.Nanosecond), 0, true},
	}
	for _, c := range cases {
		if got := declarable(now, c.watching, c.heard, ttl); got != c.want {
			t.Errorf("%s: declarable = %v, want %v", c.name, got, c.want)
		}
	}
}

// TestLeaderDeclaresOnlyPastTheMargin: a leader that heard a node 2.05 s ago
// at a 2 s ttl proposes nothing; at 2.1 s it declares the node.
func TestLeaderDeclaresOnlyPastTheMargin(t *testing.T) {
	ttl := 2 * time.Second
	base := time.Now()
	var steady atomic.Int64
	core := &fakeCore{role: "leader", members: []consensus.Member{{Name: "me", Role: consensus.Voter}, {Name: "n9", Role: consensus.Voter}}}
	s, err := New(Options{Node: "me", Core: core, Machine: consensus.NewMachine(), Sessions: newFakeSessions(), TTL: ttl, Logger: quiet,
		Clock:  func() time.Time { return base },
		Steady: func() time.Time { return base.Add(time.Duration(steady.Load())) }})
	if err != nil {
		t.Fatal(err)
	}
	s.leaderSince.Store(base.Add(-time.Hour).UnixNano())
	s.observingSince.Store(base.Add(-time.Hour).UnixNano())
	if _, err := s.table.Apply(5, Encode([]Op{
		{Kind: OpAlive, Node: "n9", Now: base},
		{Kind: OpAcquire, Namespace: "default", ClientID: "c", Node: "n9", Now: base},
	})); err != nil {
		t.Fatal(err)
	}
	steady.Store(int64(2050 * time.Millisecond))
	s.adopt()
	if n := pendingOf(s, OpReassign); n != 0 {
		t.Fatalf("heard 2.05 s ago at a 2 s ttl: %d reassignments proposed, want 0", n)
	}
	steady.Store(int64(2100 * time.Millisecond))
	s.adopt()
	if n := pendingOf(s, OpReassign); n != 1 {
		t.Fatalf("heard 2.1 s ago at a 2 s ttl: %d reassignments proposed, want 1", n)
	}
}

// gatedCore is a follower's core with no transport, so renewals propose
// through it; answer decides each proposal, and a proposal it holds waits for
// its context.
type gatedCore struct {
	*fakeCore
	answer func(ctx context.Context) (any, error)
}

func (g *gatedCore) Propose(ctx context.Context, _ consensus.Kind, _ []byte) (any, error) {
	return g.answer(ctx)
}

// TestRenewalRecordsItsSendTime: the record is when the renewal left, not
// when it came back, and an older renewal answered later does not move it
// back.
func TestRenewalRecordsItsSendTime(t *testing.T) {
	release := make(chan struct{})
	first := make(chan struct{}, 1)
	core := &gatedCore{fakeCore: &fakeCore{role: "follower", leader: "l"}, answer: func(ctx context.Context) (any, error) {
		select {
		case first <- struct{}{}:
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		default:
			time.Sleep(300 * time.Millisecond)
		}
		return []Result{{OK: true}}, nil
	}}
	s, err := New(Options{Node: "me", Core: core, Machine: consensus.NewMachine(), Sessions: newFakeSessions(), TTL: 5 * time.Second, Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	// The first renewal is held; the second is sent after it and answered
	// after 300 ms.
	older := make(chan bool, 1)
	olderSent := time.Now()
	go func() { older <- s.renew() }()
	waitFor(t, "the first renewal to be held", time.Second, func() bool { return len(first) == 1 })
	before := s.steady()
	if !s.renew() {
		t.Fatal("the second renewal failed")
	}
	returned := s.steady()
	rec := time.Unix(0, s.lastAlive.Load())
	if d := rec.Sub(before); d < 0 || d > 50*time.Millisecond {
		t.Fatalf("record %s after the send, want within 50 ms of it", d)
	}
	if d := returned.Sub(rec); d < 250*time.Millisecond {
		t.Fatalf("record only %s before the return: it is the return time, not the send", d)
	}
	close(release)
	if !<-older {
		t.Fatal("the first renewal failed")
	}
	if got := time.Unix(0, s.lastAlive.Load()); !got.Equal(rec) {
		t.Fatalf("the older renewal, sent %s earlier and answered later, moved the record to %s from %s",
			before.Sub(olderSent).Round(time.Millisecond), got, rec)
	}
}

// TestFenceTimerFencesWhileRenewalsHang: with every renewal of the tick loop
// hung, so launches stop at the in-flight bound and nothing wakes the loop
// early, the fence still comes at the record plus the ttl. The record is put off
// the tick grid by an answered connect-path renewal 100 ms after start, so a
// fence from the 250 ms tick alone would come at least 150 ms late.
func TestFenceTimerFencesWhileRenewalsHang(t *testing.T) {
	ttl := time.Second
	var answerOne atomic.Bool
	core := &gatedCore{fakeCore: &fakeCore{role: "follower", leader: "l"}, answer: func(ctx context.Context) (any, error) {
		if answerOne.CompareAndSwap(true, false) {
			return []Result{{OK: true}}, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	sessions := newFakeSessions()
	s, err := New(Options{Node: "me", Core: core, Machine: consensus.NewMachine(), Sessions: sessions, TTL: ttl, Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	s.Start()
	t.Cleanup(s.Stop)
	time.Sleep(100 * time.Millisecond)
	answerOne.Store(true)
	if !s.alive() {
		t.Fatal("the answered renewal failed")
	}
	deadline := time.Unix(0, s.lastAlive.Load()).Add(ttl)
	waitFor(t, "the fence", 3*ttl, func() bool { return sessions.fenceCount() > 0 })
	sessions.mu.Lock()
	at := sessions.fencedAt
	sessions.mu.Unlock()
	if late := at.Sub(deadline); late < 0 || late > 100*time.Millisecond {
		t.Fatalf("fenced %s after the record plus the ttl, want within 0..100 ms", late.Round(time.Millisecond))
	}
}

// TestRecheckInRefuses holds the re-check's predicate at what it must
// refuse: a node heard inside the ttl, and one already past the margin (the
// tick declares that one); between the two it waits for the margin's end.
func TestRecheckInRefuses(t *testing.T) {
	ttl := 2 * time.Second
	margin := declarationMargin(ttl)
	now := time.Unix(5000, 0)
	ago := func(d time.Duration) int64 { return now.Add(-d).UnixNano() }
	long := ago(time.Hour)
	if _, ok := recheckIn(now, long, ago(ttl), ttl); ok {
		t.Error("heard exactly a ttl ago: a re-check armed, want none")
	}
	if _, ok := recheckIn(now, long, ago(ttl+margin+time.Nanosecond), ttl); ok {
		t.Error("past the margin: a re-check armed, want none (declarable now)")
	}
	wait, ok := recheckIn(now, long, ago(ttl+margin/2), ttl)
	if !ok || wait != margin/2+time.Nanosecond {
		t.Errorf("half the margin in: wait %s armed %v, want %s", wait, ok, margin/2+time.Nanosecond)
	}
	if !redeclare(declareMark{alive: 4, at: now.Add(-100 * time.Millisecond).UnixNano()}, 4, now, ttl) {
		t.Error("a declaration 100 ms ago against the same record: proposed again, want held")
	}
	if redeclare(declareMark{alive: 4, at: now.Add(-ttl / 4).UnixNano()}, 4, now, ttl) {
		t.Error("a declaration a tick ago: held, want proposed again")
	}
	if redeclare(declareMark{alive: 3, at: now.UnixNano()}, 4, now, ttl) {
		t.Error("a declaration against an older record: held, want proposed")
	}
}

// marginLeader is a leader whose steady clock runs offset ahead of the real
// one, so a test can place "now" past a node's ttl without waiting it out,
// with a peer n9 heard at the real time of the call.
func marginLeader(t *testing.T, ttl time.Duration, offset *atomic.Int64) *Service {
	t.Helper()
	core := &fakeCore{role: "leader", members: []consensus.Member{{Name: "me", Role: consensus.Voter}, {Name: "n9", Role: consensus.Voter}}}
	s, err := New(Options{Node: "me", Core: core, Machine: consensus.NewMachine(), Sessions: newFakeSessions(), TTL: ttl, Logger: quiet,
		Steady: func() time.Time { return time.Now().Add(time.Duration(offset.Load())) }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		s.recheckMu.Lock()
		for _, tm := range s.rechecks {
			tm.Stop()
		}
		s.recheckMu.Unlock()
	})
	base := time.Now().Add(-time.Hour)
	s.leaderSince.Store(base.UnixNano())
	s.observingSince.Store(base.UnixNano())
	if _, err := s.table.Apply(5, Encode([]Op{
		{Kind: OpAlive, Node: "n9", Now: time.Now()},
		{Kind: OpAcquire, Namespace: "default", ClientID: "c", Node: "n9", Now: time.Now()},
	})); err != nil {
		t.Fatal(err)
	}
	return s
}

// TestLeaderRechecksAtTheMarginsEnd: a tick that finds a node past its ttl
// but inside the margin declares nothing then; a re-check declares it when
// the margin ends, not a tick later, and the next tick does not declare it
// again. An alive record before the margin ends cancels the re-check.
func TestLeaderRechecksAtTheMarginsEnd(t *testing.T) {
	ttl := 2 * time.Second
	margin := declarationMargin(ttl)
	t.Run("declared at the margin's end", func(t *testing.T) {
		var offset atomic.Int64
		s := marginLeader(t, ttl, &offset)
		offset.Store(int64(ttl + margin/2))
		armed := time.Now()
		s.adopt()
		if n := pendingOf(s, OpReassign); n != 0 {
			t.Fatalf("inside the margin: %d reassignments proposed, want 0", n)
		}
		waitFor(t, "the re-check's declaration", margin+200*time.Millisecond, func() bool { return pendingOf(s, OpReassign) == 1 })
		if took := time.Since(armed); took > margin/2+30*time.Millisecond {
			t.Fatalf("declared %s after the tick, want within %s (the margin left) and 30 ms", took.Round(time.Millisecond), margin/2)
		}
		s.adopt()
		if n := pendingOf(s, OpReassign); n != 1 {
			t.Fatalf("the next tick proposed again: %d reassignments, want 1", n)
		}
	})
	t.Run("an alive record cancels it", func(t *testing.T) {
		var offset atomic.Int64
		s := marginLeader(t, ttl, &offset)
		offset.Store(int64(ttl + margin/2))
		s.adopt()
		s.recheckMu.Lock()
		armed := len(s.rechecks)
		s.recheckMu.Unlock()
		if armed != 1 {
			t.Fatalf("%d re-checks armed inside the margin, want 1", armed)
		}
		if _, err := s.table.Apply(6, Encode([]Op{{Kind: OpAlive, Node: "n9", Now: time.Now()}})); err != nil {
			t.Fatal(err)
		}
		s.recheckMu.Lock()
		armed = len(s.rechecks)
		s.recheckMu.Unlock()
		if armed != 0 {
			t.Fatalf("%d re-checks still armed after the node's alive record, want 0", armed)
		}
		time.Sleep(margin + 50*time.Millisecond)
		if n := pendingOf(s, OpReassign); n != 0 {
			t.Fatalf("a node heard again before the margin's end was declared: %d reassignments", n)
		}
	})
}
