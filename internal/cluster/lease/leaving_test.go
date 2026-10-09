package lease

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

// TestHandoffMarksTheNodeLeaving: a drain's hand-off, even of a node that owns
// nothing, marks the node leaving; its next alive record clears the mark. A
// refused hand-off marks nothing, the mark survives a snapshot, and the hook
// is not run while the log is replayed.
func TestHandoffMarksTheNodeLeaving(t *testing.T) {
	tb := NewTable()
	var told []string
	tb.onLeaving = func(node string) { told = append(told, node) }
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	apply(t, tb, Op{Kind: OpAlive, Node: "n1", Now: now}, Op{Kind: OpAlive, Node: "n2", Now: now})
	if r := apply(t, tb, Op{Kind: OpHandoff, Node: "n1", To: "n1", Now: now}); r[0].OK || tb.Leaving("n1") {
		t.Fatalf("a refused hand-off: %+v, leaving %v; want refused and no mark", r[0], tb.Leaving("n1"))
	}
	r := apply(t, tb, Op{Kind: OpHandoff, Node: "n1", To: "n2", Now: now})
	if !r[0].OK || r[0].Moved != 0 {
		t.Fatalf("a hand-off of nothing: %+v; want OK, 0 moved", r[0])
	}
	if !tb.Leaving("n1") || tb.Leaving("n2") || !slices.Equal(tb.LeavingNodes(), []string{"n1"}) {
		t.Fatalf("after the hand-off: n1 leaving %v, n2 leaving %v, marked %v", tb.Leaving("n1"), tb.Leaving("n2"), tb.LeavingNodes())
	}
	if !slices.Equal(told, []string{"n1"}) {
		t.Fatalf("hook told %v, want [n1]", told)
	}
	snap, err := tb.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	restored := NewTable()
	if err := restored.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if !restored.Leaving("n1") {
		t.Fatal("the mark did not survive a snapshot")
	}
	apply(t, tb, Op{Kind: OpAlive, Node: "n1", Now: now.Add(time.Second)})
	if tb.Leaving("n1") {
		t.Fatal("the node's next alive record did not clear the mark")
	}
	replay := NewTable()
	var replayTold []string
	replay.onLeaving = func(node string) { replayTold = append(replayTold, node) }
	replay.SetRecovering(true)
	apply(t, replay, Op{Kind: OpAlive, Node: "n1", Now: now}, Op{Kind: OpHandoff, Node: "n1", To: "n2", Now: now})
	replay.SetRecovering(false)
	if !replay.Leaving("n1") || len(replayTold) != 0 {
		t.Fatalf("replayed hand-off: leaving %v, hook told %v; want marked and nothing told", replay.Leaving("n1"), replayTold)
	}
}

// TestPausedRenewalsProposeNothingAndDoNotFence: a node whose renewals are
// paused proposes no alive record and does not fence when its record lapses;
// resumed, it renews again.
func TestPausedRenewalsProposeNothingAndDoNotFence(t *testing.T) {
	ttl := time.Second
	c := newCluster(t, []string{"n1", "n2", "n3"}, ttl, nil)
	lead := c.settled()
	f := c.follower(lead)
	if n := f.svc.PauseRenewals(2 * time.Second); n != 0 {
		t.Fatalf("%d renewals still in flight after the wait", n)
	}
	before := f.svc.LivenessStats().RenewOK
	time.Sleep(2 * ttl)
	st := f.svc.LivenessStats()
	if st.RenewOK != before {
		t.Fatalf("renewals while paused: %d, want 0", st.RenewOK-before)
	}
	if f.sessions.fenceCount() != 0 || st.Fenced {
		t.Fatalf("fenced while paused: %+v", st)
	}
	f.svc.ResumeRenewals()
	waitFor(t, "a renewal once resumed", 3*ttl, func() bool { return f.svc.LivenessStats().RenewOK > before })
}

// TestConfirmMovedWaitsForDurability: a confirmation to a node that stopped
// being the session's target goes only once the session's records are durable
// at its targets now, and carries the token the session is held at. n2 serves
// each confirmation 300 ms after it counts it received, so a read of the token
// on the received counter alone finds none.
func TestConfirmMovedWaitsForDurability(t *testing.T) {
	fixtureService = func(node string, s *Service) {
		if node == "n2" {
			s.confirmed = s.newQueue("confirmed", confirmWorkers, func(k key, payloads []any) {
				time.Sleep(300 * time.Millisecond)
				s.serveConfirmed(k, payloads)
			})
		}
	}
	t.Cleanup(func() { fixtureService = nil })
	c := newCluster(t, []string{"n1", "n2"}, 2*time.Second, nil)
	c.settled()
	a, b := c.nodes["n1"], c.nodes["n2"]
	st := sessionFor("x")
	st.Epoch = 7
	a.sessions.put("default", "x", st)
	a.sessions.setDurableErr(errors.New("a replica refused"))
	a.svc.ConfirmMoved("default", "x", []string{"n2"})
	waitFor(t, "a durability wait to be asked", 5*time.Second, func() bool {
		a.sessions.mu.Lock()
		defer a.sessions.mu.Unlock()
		return a.sessions.durableAsks >= 1
	})
	time.Sleep(200 * time.Millisecond)
	if n := a.svc.LivenessStats().ConfirmsSentMoved; n != 0 {
		t.Fatalf("confirmed %d times while the records were not durable", n)
	}
	a.sessions.setDurableErr(nil)
	waitFor(t, "the confirmation to be sent", 10*time.Second, func() bool { return a.svc.LivenessStats().ConfirmsSentMoved == 1 })
	waitFor(t, "the confirmation's token to be noted", 5*time.Second, func() bool { return b.svc.tokens.Seen("default", "x") > 0 })
	if seen := b.svc.tokens.Seen("default", "x"); seen != 7 {
		t.Fatalf("the confirmation carried token %d, want 7", seen)
	}
	if n := b.svc.LivenessStats().ConfirmsReceived; n < 1 {
		t.Fatalf("a token was noted with %d confirmations received", n)
	}
}

// TestHandoffRefusalIsTyped: the table's refusal of a hand-off, here to the
// node's own name, reaches the caller as ErrHandoffRefused with its reason,
// on the leader and through a follower's forward, so a drain can tell it from
// a hand-off whose proposal failed.
func TestHandoffRefusalIsTyped(t *testing.T) {
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	lead := c.settled()
	for _, n := range []*tnode{lead, c.follower(lead)} {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := n.svc.Handoff(ctx, n.name)
		cancel()
		if !errors.Is(err, ErrHandoffRefused) {
			t.Fatalf("%s's hand-off to itself: %v, want ErrHandoffRefused", n.name, err)
		}
		if err.Error() != "lease: hand-off refused: no target" {
			t.Fatalf("%s's refusal reads %q", n.name, err)
		}
	}
}
