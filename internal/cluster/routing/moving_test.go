package routing

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/dispatch"
	"github.com/happydave/hoardmq/internal/message"
)

// movingPlane is a plane on a stub transport with no peers: nothing is
// unknown, nothing is hidden, so the only rule that can refuse is the one
// under test.
func movingPlane(t *testing.T, grace time.Duration) *Plane {
	t.Helper()
	tr := newStubTransport()
	p := New(Options{Node: "n1", Transport: tr, SyncInterval: time.Hour, MoveGrace: grace, Logger: quiet,
		Members: func() []string { return []string{"n1"} }, OwnedSessions: func(string) (int, bool) { return 0, true }})
	p.SetSink(&fakeSink{})
	p.Start()
	t.Cleanup(p.Stop)
	return p
}

func qos1On(topic string) *message.Message {
	return &message.Message{Namespace: "default", Topic: topic, QoS: 1, Payload: []byte("x"), Seq: 1, Arrived: time.Now()}
}

// handOver is what the session manager does when a session leaves: mark the
// filters moving, then remove them. The route stays advertised for the
// grace so the other nodes keep forwarding here.
func handOver(p *Plane, filter string) {
	p.Moving("default", []string{filter})
	p.Observe("default", filter, 0)
}

// TestPublishRefusedWhileTheSessionIsMovingAway is WI 1353's defect: the old
// owner answered its own publishes as though the route it is still
// advertising did not exist. On the unchanged tree Forward returns nil.
func TestPublishRefusedWhileTheSessionIsMovingAway(t *testing.T) {
	p := movingPlane(t, time.Hour) // a grace that will not elapse under test
	p.Observe("default", "t/0", 1)
	handOver(p, "t/0")

	// The premise: the route is still advertised to the other nodes, which
	// is why they keep forwarding here and why this node must answer for it.
	own := p.table.Own()
	if len(own) != 1 || own[0].Tombstone {
		t.Fatalf("the route should still be advertised during the grace: %+v", own)
	}
	if _, err := p.Forward(qos1On("t/0")); !errors.Is(err, ErrRoutesUnavailable) {
		t.Fatalf("a publish for a session that has left this node must not be acknowledged; got %v", err)
	}
}

// TestMovingRefusalEndsWithTheHandOver: once the withdrawal is emitted the
// route is gone everywhere and this node answers normally again.
func TestMovingRefusalEndsWithTheHandOver(t *testing.T) {
	p := movingPlane(t, 40*time.Millisecond)
	p.Observe("default", "t/0", 1)
	handOver(p, "t/0")
	if _, err := p.Forward(qos1On("t/0")); !errors.Is(err, ErrRoutesUnavailable) {
		t.Fatalf("expected a refusal during the grace, got %v", err)
	}
	waitFor(t, "the withdrawal to be emitted", 5*time.Second, func() bool { return p.table.Moving() == 0 })
	if _, err := p.Forward(qos1On("t/0")); err != nil {
		t.Fatalf("the hand-over is over; the publish must be accepted: %v", err)
	}
}

// TestMovingRefusalEndsWhenTheClientComesBack: the client reconnected to
// this node during the grace, so the move is cancelled. Refusing after that
// would turn a client's own reconnect into a self-inflicted outage.
func TestMovingRefusalEndsWhenTheClientComesBack(t *testing.T) {
	p := movingPlane(t, time.Hour)
	p.Observe("default", "t/0", 1)
	handOver(p, "t/0")
	if _, err := p.Forward(qos1On("t/0")); !errors.Is(err, ErrRoutesUnavailable) {
		t.Fatalf("expected a refusal during the grace, got %v", err)
	}
	p.Observe("default", "t/0", 1) // the subscriber is back here
	if p.table.Moving() != 0 {
		t.Fatal("a cancelled move must clear the index")
	}
	if _, err := p.Forward(qos1On("t/0")); err != nil {
		t.Fatalf("the session is here again: %v", err)
	}
	if own := p.table.Own(); len(own) != 1 || own[0].Tombstone {
		t.Fatalf("the route must be advertised again, not withdrawn: %+v", own)
	}
}

// TestMovingMatchesSharedFilters: a shared subscription is recorded as
// $share/g/t/0 and is a route for topic t/0. The two representations differ
// by a prefix, and a matcher that forgot would leave shared groups silently
// uncovered.
func TestMovingMatchesSharedFilters(t *testing.T) {
	p := movingPlane(t, time.Hour)
	p.Observe("default", "$share/g/t/0", 1)
	handOver(p, "$share/g/t/0")
	if _, err := p.Forward(qos1On("t/0")); !errors.Is(err, ErrRoutesUnavailable) {
		t.Fatalf("a moving shared filter must answer for its topic; got %v", err)
	}
}

// TestMovingLeavesQoS0AndOtherTopicsAlone is the control against
// over-refusing: the rule must be narrow in both dimensions.
func TestMovingLeavesQoS0AndOtherTopicsAlone(t *testing.T) {
	p := movingPlane(t, time.Hour)
	p.Observe("default", "t/0", 1)
	handOver(p, "t/0")

	zero := qos1On("t/0")
	zero.QoS = 0
	if _, err := p.Forward(zero); err != nil {
		t.Fatalf("QoS 0 carries no acknowledgement to refuse: %v", err)
	}
	if _, err := p.Forward(qos1On("t/9")); err != nil {
		t.Fatalf("an unrelated topic must be accepted: %v", err)
	}
	if _, err := p.Forward(qos1On("t/0")); !errors.Is(err, ErrRoutesUnavailable) {
		t.Fatal("the moving topic must still be refused in the same state")
	}
}

// TestMovingIndexIsWildcardAware: the filter is a wildcard and the publish
// is a concrete topic under it, which is the ordinary case for a session
// subscribed to a tree.
func TestMovingIndexIsWildcardAware(t *testing.T) {
	p := movingPlane(t, time.Hour)
	p.Observe("default", "t/#", 1)
	handOver(p, "t/#")
	if _, err := p.Forward(qos1On("t/0/deep")); !errors.Is(err, ErrRoutesUnavailable) {
		t.Fatalf("t/# is moving, so t/0/deep must be refused; got %v", err)
	}
	if _, err := p.Forward(qos1On("other/1")); err != nil {
		t.Fatalf("a topic outside the moving filter must be accepted: %v", err)
	}
}

// TestMovingIndexIsSweptWhenAHandOverNeverResolves: every ordinary path ends
// a move through emit. This covers the one way that can fail — a filter
// marked moving whose removal never reaches Observe — because the
// consequence is that this node refuses that topic for as long as it lives,
// which is worse than the defect the rule prevents.
func TestMovingIndexIsSweptWhenAHandOverNeverResolves(t *testing.T) {
	tr := newStubTransport()
	p := New(Options{Node: "n1", Transport: tr, SyncInterval: 50 * time.Millisecond, MoveGrace: time.Millisecond,
		Logger: quiet, Members: func() []string { return []string{"n1"} },
		OwnedSessions: func(string) (int, bool) { return 0, true }})
	p.SetSink(&fakeSink{})
	p.Start()
	t.Cleanup(p.Stop)

	// A move recorded and then abandoned: no Observe ever arrives for it.
	p.Moving("default", []string{"t/0"})
	if _, err := p.Forward(qos1On("t/0")); !errors.Is(err, ErrRoutesUnavailable) {
		t.Fatalf("expected a refusal while the hand-over is recorded, got %v", err)
	}
	waitFor(t, "the abandoned hand-over to be swept", 5*time.Second, func() bool { return p.table.Moving() == 0 })
	if _, err := p.Forward(qos1On("t/0")); err != nil {
		t.Fatalf("a swept hand-over must stop refusing: %v", err)
	}
}

// TestForwardedPublishRefusedWhileASessionOnTheFilterIsMovingAway: two
// subscribers share a filter on b and one of them is leaving for another
// node. A publish forwarded from a reaches the one that stays, so it landed
// somewhere — and the session that is moving was given up before the frame
// arrived and is imported elsewhere without it. The sender's publish must not
// be acknowledged on the strength of the subscriber that stayed.
func TestForwardedPublishRefusedWhileASessionOnTheFilterIsMovingAway(t *testing.T) {
	c := newCluster(t, []string{"a", "b"}, dispatch.RoundRobin)
	a, b := c.nodes["a"], c.nodes["b"]
	b.plane.Observe("default", "t/1", 2)
	waitFor(t, "route", 5*time.Second, func() bool {
		nodes, _ := a.plane.Table().Match("default", "t/1")
		return len(nodes) == 1
	})
	barrier := func(m *message.Message) error {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		return a.plane.Barrier(ctx, m.Forwards)
	}
	if err := barrier(publish(a, "p", "t/1", "before", 1)); err != nil {
		t.Fatalf("forward with nobody moving: %v", err)
	}

	// One of the two is taken over elsewhere; the filter stays, for the other.
	b.plane.Moving("default", []string{"t/1"})
	b.plane.Observe("default", "t/1", 1)
	during := publish(a, "p", "t/1", "during", 2)
	waitFor(t, "the forward to reach the subscriber that stayed", 5*time.Second, func() bool { return b.sink.directCount() == 2 })
	if err := barrier(during); !errors.Is(err, ErrRoutesUnavailable) {
		t.Fatalf("a forward applied while a session on its filter is moving away: barrier %v, want ErrRoutesUnavailable", err)
	}
}

// TestMovingOutlivesTheSubscriberThatStayed: the session manager marks a
// leaving session's filters moving and then removes its subscriptions, and
// when another session shares the filter the second step reports a count
// that fell but not to zero. That is the moving session going, not the move
// ending: the peers are told the new count and publishes are still refused.
func TestMovingOutlivesTheSubscriberThatStayed(t *testing.T) {
	p := movingPlane(t, time.Hour)
	p.Observe("default", "t/0", 2)
	p.Moving("default", []string{"t/0"})
	p.Observe("default", "t/0", 1)
	if !p.table.MovingMatch("default", "t/0") {
		t.Fatal("the move ended when the moving session's own subscription was removed")
	}
	if _, err := p.Forward(qos1On("t/0")); !errors.Is(err, ErrRoutesUnavailable) {
		t.Fatalf("publish while a session on the filter is moving away: %v, want it refused", err)
	}
	if own := p.table.Own(); len(own) != 1 || own[0].Count != 1 || own[0].Tombstone {
		t.Fatalf("the peers must be told the filter's new count: %+v", own)
	}
	// The client coming back, or anyone joining, is a rise, and ends it.
	p.Observe("default", "t/0", 2)
	if p.table.MovingMatch("default", "t/0") {
		t.Fatal("a rise in the filter's count must end the move")
	}
	if _, err := p.Forward(qos1On("t/0")); err != nil {
		t.Fatalf("publish after the move ended: %v", err)
	}
}

// TestMoveOfASharedFilterEndsWithItsGrace: no removal is held for a filter
// other sessions keep alive, so nothing but its own timer ends the move.
func TestMoveOfASharedFilterEndsWithItsGrace(t *testing.T) {
	p := movingPlane(t, 150*time.Millisecond)
	p.Observe("default", "t/0", 2)
	p.Moving("default", []string{"t/0"})
	p.Observe("default", "t/0", 1)
	if _, err := p.Forward(qos1On("t/0")); !errors.Is(err, ErrRoutesUnavailable) {
		t.Fatalf("inside the grace: %v, want a refusal", err)
	}
	waitFor(t, "the move to end with its grace", 5*time.Second, func() bool { return !p.table.MovingMatch("default", "t/0") })
	if _, err := p.Forward(qos1On("t/0")); err != nil {
		t.Fatalf("after the grace: %v", err)
	}
	if own := p.table.Own(); len(own) != 1 || own[0].Count != 1 || own[0].Tombstone {
		t.Fatalf("the filter must still be advertised for the subscriber that stayed: %+v", own)
	}
}

// TestMoveThatThenEmptiesTheFilterIsHeldOnce: the count falls to one and then
// to zero inside one grace. The held removal takes over from the end-of-move
// timer: the route stays advertised and refused for until the grace, and is
// withdrawn once.
func TestMoveThatThenEmptiesTheFilterIsHeldOnce(t *testing.T) {
	p := movingPlane(t, 200*time.Millisecond)
	p.Observe("default", "t/0", 2)
	p.Moving("default", []string{"t/0"})
	p.Observe("default", "t/0", 1)
	p.Observe("default", "t/0", 0)
	p.mu.Lock()
	ending, held := len(p.ending), len(p.held)
	p.mu.Unlock()
	if ending != 0 || held != 1 {
		t.Fatalf("timers for one filter: %d ending, %d held; want the held removal alone", ending, held)
	}
	if _, err := p.Forward(qos1On("t/0")); !errors.Is(err, ErrRoutesUnavailable) {
		t.Fatalf("inside the grace: %v, want a refusal", err)
	}
	waitFor(t, "the withdrawal", 5*time.Second, func() bool {
		own := p.table.Own()
		return len(own) == 1 && own[0].Tombstone
	})
	if p.table.Moving() != 0 {
		t.Fatal("the index outlived the withdrawal")
	}
}

// TestGroupForwardIsNotRefusedForAMove: a shared-subscription message handed
// to a member that stayed is delivered — one member is all it needs — so a
// session moving off the same filter does not fail it.
func TestGroupForwardIsNotRefusedForAMove(t *testing.T) {
	c := newCluster(t, []string{"a", "b"}, dispatch.RoundRobin)
	a, b := c.nodes["a"], c.nodes["b"]
	b.plane.Observe("default", "$share/g/t/1", 2)
	waitFor(t, "group route", 5*time.Second, func() bool {
		_, groups := a.plane.Table().Match("default", "t/1")
		return len(groups) == 1
	})
	b.plane.Moving("default", []string{"$share/g/t/1"})
	b.plane.Observe("default", "$share/g/t/1", 1)
	m := publish(a, "p", "t/1", "to the group", 1)
	waitFor(t, "the group delivery", 5*time.Second, func() bool { return b.sink.groupCount() == 1 })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := a.plane.Barrier(ctx, m.Forwards); err != nil {
		t.Fatalf("a group delivery to a member that stayed: barrier %v", err)
	}
}

// TestSecondMoveOfAFilterIsFollowedToItsOwnDeadline: two sessions leave one
// filter in turn while a third stays. The first move's timer finds a later
// deadline and follows it; the mark ends with the second grace, not with the
// first and not at the sweep.
func TestSecondMoveOfAFilterIsFollowedToItsOwnDeadline(t *testing.T) {
	p := movingPlane(t, 300*time.Millisecond)
	p.Observe("default", "t/0", 3)
	p.Moving("default", []string{"t/0"})
	p.Observe("default", "t/0", 2)
	time.Sleep(150 * time.Millisecond)
	p.Moving("default", []string{"t/0"})
	p.Observe("default", "t/0", 1)
	second := time.Now()
	time.Sleep(200 * time.Millisecond) // past the first deadline, inside the second
	if !p.table.MovingMatch("default", "t/0") {
		t.Fatal("the first move's timer ended the second move")
	}
	waitFor(t, "the second move to end with its own grace", 5*time.Second, func() bool { return !p.table.MovingMatch("default", "t/0") })
	if took := time.Since(second); took > 2*time.Second {
		t.Fatalf("the mark lasted %s after the second move began; its grace is 300ms", took)
	}
}
