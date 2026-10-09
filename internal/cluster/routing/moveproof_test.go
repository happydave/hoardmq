package routing

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/dispatch"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

func TestMoveProven(t *testing.T) {
	now := time.Unix(1000, 0)
	live, gone := now.Add(time.Second), now.Add(-time.Second)
	nKnown := func(owner, _ string) bool { return owner == "n" }
	noneKnown := func(string, string) bool { return false }
	view := func(filter string, moves ...clientMove) moveView { return moveView{filter: filter, moves: moves} }
	toN := clientMove{until: live, owner: "n", mark: 10}
	local := func(targets ...string) publishPath { return publishPath{targets: targets} }
	from := func(peer string, seq uint64) publishPath { return publishPath{forwarded: true, peer: peer, seq: seq} }
	cases := []struct {
		name  string
		views []moveView
		path  publishPath
		known func(string, string) bool
		want  bool
	}{
		{"nothing read", nil, local("n"), nKnown, false},
		{"unresolved", []moveView{view("t/1", clientMove{until: live})}, local("n"), nKnown, false},
		{"made here, sent to the new owner", []moveView{view("t/1", toN)}, local("n"), nKnown, true},
		{"made here, not sent to the new owner", []moveView{view("t/1", toN)}, local(), nKnown, false},
		{"made here, sent elsewhere only", []moveView{view("t/1", toN)}, local("x"), nKnown, false},
		{"route unknown", []moveView{view("t/1", toN)}, local("n"), noneKnown, false},
		{"from the new owner, above the mark", []moveView{view("t/1", toN)}, from("n", 11), nKnown, true},
		{"from the new owner, at the mark", []moveView{view("t/1", toN)}, from("n", 10), nKnown, true},
		{"from the new owner, just below the mark", []moveView{view("t/1", toN)}, from("n", 9), nKnown, false},
		{"from the new owner, no mark", []moveView{view("t/1", clientMove{until: live, owner: "n"})}, from("n", 11), nKnown, false},
		{"from the new owner, route unknown", []moveView{view("t/1", toN)}, from("n", 11), noneKnown, false},
		{"from a third node", []moveView{view("t/1", toN)}, from("x", 11), nKnown, false},
		{"two moves, one unresolved", []moveView{view("t/1", toN, clientMove{until: live})}, local("n"), nKnown, false},
		{"an ended move beside a proven one", []moveView{view("t/1", toN, clientMove{until: gone})}, local("n"), nKnown, true},
		{"only ended moves", []moveView{view("t/1", clientMove{until: gone, owner: "n", mark: 10})}, local("n"), nKnown, false},
		{"no client recorded", []moveView{view("t/1")}, local("n"), nKnown, false},
		{"a shared filter", []moveView{view("$share/g/t/1", toN)}, local("n"), nKnown, false},
		{"two filters, one proven", []moveView{view("t/1", toN), view("t/+", clientMove{until: live, owner: "m", mark: 1})}, local("n"), nKnown, false},
	}
	for _, c := range cases {
		if got := moveProven(c.views, c.path, now, c.known); got != c.want {
			t.Errorf("%s: moveProven = %v, want %v", c.name, got, c.want)
		}
	}
}

// emptySink is a node with no local subscriber left.
type emptySink struct{}

func (emptySink) InjectForwarded(*message.Message) (int, []store.Scope, error) { return 0, nil, nil }
func (emptySink) InjectGroup(*message.Message, string) (store.Scope, bool, error) {
	return store.Scope{}, false, nil
}
func (emptySink) Durable(context.Context, []store.Scope) error { return nil }

// moveTrio is three planes: o hands client s over to n, x is a third node.
// With coSubscriber, another session stays on o's filter.
type moveTrio struct {
	t       *testing.T
	o, n, x *tnode
	filter  string
}

func newMoveTrio(t *testing.T, grace time.Duration, coSubscriber bool) *moveTrio {
	t.Helper()
	c := newClusterOpts(t, []string{"n", "o", "x"}, dispatch.RoundRobin, func(o *Options) { o.MoveGrace = grace })
	m := &moveTrio{t: t, o: c.nodes["o"], n: c.nodes["n"], x: c.nodes["x"], filter: "t/1"}
	count := 1
	if coSubscriber {
		count = 2
	}
	m.o.plane.Observe("default", m.filter, count)
	for _, peer := range []*tnode{m.n, m.x} {
		waitFor(t, peer.name+" to see o's route", 5*time.Second, func() bool { return peer.plane.Table().Known("o", "default", m.filter) })
	}
	m.o.plane.MovingClient("default", "s", []string{m.filter})
	m.o.plane.Observe("default", m.filter, count-1)
	if !coSubscriber {
		m.o.plane.SetSink(emptySink{})
	}
	return m
}

// newOwnerRoute is the new owner's import advertising the moved session.
func (m *moveTrio) newOwnerRoute() {
	m.n.plane.Observe("default", m.filter, 1)
	waitFor(m.t, "o to know n's route", 5*time.Second, func() bool { return m.o.plane.Table().Known("n", "default", m.filter) })
}

// publishOn runs node's forward step for a QoS 1 publish with the node's
// publish sequence seq, and returns Forward's answer and the barrier's.
func (m *moveTrio) publishOn(node *tnode, seq uint64) (forward, barrier error) {
	msg := &message.Message{Namespace: "default", Topic: m.filter, Payload: []byte("x"), QoS: 1,
		Publisher: message.Publisher{Namespace: "default", ClientID: "p", Node: node.name}, Seq: seq, Arrived: time.Now()}
	_, forward = node.plane.Forward(msg)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return forward, node.plane.Barrier(ctx, msg.Forwards)
}

// refusedOnO: a publish made on o is refused as session-moving.
func (m *moveTrio) refusedOnO(when string) {
	m.t.Helper()
	if err, _ := m.publishOn(m.o, 1); !errors.Is(err, ErrRoutesUnavailable) {
		m.t.Fatalf("%s: a publish made on o was not refused: %v", when, err)
	}
}

func (m *moveTrio) acceptedOnO(when string) {
	m.t.Helper()
	if fwd, bar := m.publishOn(m.o, 1); fwd != nil || bar != nil {
		m.t.Fatalf("%s: a publish made on o: forward %v, barrier %v; want both nil", when, fwd, bar)
	}
}

// refusedFrom: node's forward to o fails at node's barrier.
func (m *moveTrio) refusedFrom(node *tnode, seq uint64, when string) {
	m.t.Helper()
	if _, bar := m.publishOn(node, seq); !errors.Is(bar, ErrRoutesUnavailable) {
		m.t.Fatalf("%s: %s's forward (seq %d) to o: barrier %v, want ErrRoutesUnavailable", when, node.name, seq, bar)
	}
}

func (m *moveTrio) acceptedFrom(node *tnode, seq uint64, when string) {
	m.t.Helper()
	if fwd, bar := m.publishOn(node, seq); fwd != nil || bar != nil {
		m.t.Fatalf("%s: %s's forward (seq %d): forward %v, barrier %v; want both nil", when, node.name, seq, fwd, bar)
	}
}

// TestMoveEndsOnTheConfirmationThenTheRoute: the confirmation is acted on
// before the new owner's route reaches the old node. Nothing is proven until
// the route is known.
func TestMoveEndsOnTheConfirmationThenTheRoute(t *testing.T) {
	m := newMoveTrio(t, time.Hour, true)
	m.o.plane.Resolve("default", "s", "n", 10)
	m.refusedOnO("confirmed, route not known")
	m.refusedFrom(m.n, 11, "confirmed, route not known")
	m.newOwnerRoute()
	m.acceptedOnO("confirmed and route known")
	m.acceptedFrom(m.n, 12, "confirmed and route known")
}

// TestMoveEndsOnTheRouteThenTheConfirmation: the other order.
func TestMoveEndsOnTheRouteThenTheConfirmation(t *testing.T) {
	m := newMoveTrio(t, time.Hour, true)
	m.newOwnerRoute()
	m.refusedOnO("route known, not confirmed")
	m.refusedFrom(m.n, 11, "route known, not confirmed")
	m.o.plane.Resolve("default", "s", "n", 10)
	m.acceptedOnO("route known and confirmed")
	m.acceptedFrom(m.n, 12, "route known and confirmed")
}

// TestMoveConfirmedWithNoRouteWaitsOutTheGrace and its converse: either
// signal alone leaves the grace as the bound.
func TestMoveConfirmedWithNoRouteWaitsOutTheGrace(t *testing.T) {
	m := newMoveTrio(t, time.Second, true)
	m.o.plane.Resolve("default", "s", "n", 10)
	m.refusedOnO("confirmed, no route, inside the grace")
	m.refusedFrom(m.n, 11, "confirmed, no route, inside the grace")
	waitFor(t, "the grace to end", 5*time.Second, func() bool { return m.o.plane.Table().Moving() == 0 })
	m.acceptedOnO("after the grace")
}

func TestMoveWithARouteAndNoConfirmationWaitsOutTheGrace(t *testing.T) {
	m := newMoveTrio(t, time.Second, true)
	m.newOwnerRoute()
	m.refusedOnO("route, no confirmation, inside the grace")
	m.refusedFrom(m.n, 11, "route, no confirmation, inside the grace")
	waitFor(t, "the grace to end", 5*time.Second, func() bool { return m.o.plane.Table().Moving() == 0 })
	m.acceptedOnO("after the grace")
}

// TestMovedSubscriberBesideOneThatStayed is WI 1628's case after the move
// is resolved: a third node's forward still reaches the subscriber that
// stayed and is still refused, and the new owner's forward is accepted only
// from its mark on.
func TestMovedSubscriberBesideOneThatStayed(t *testing.T) {
	m := newMoveTrio(t, time.Hour, true)
	m.newOwnerRoute()
	m.o.plane.Resolve("default", "s", "n", 10)
	waitFor(t, "x to know n's route", 5*time.Second, func() bool { return m.x.plane.Table().Known("n", "default", m.filter) })
	before := m.o.sink.directCount()
	m.refusedFrom(m.x, 50, "a third node's forward")
	if got := m.o.sink.directCount(); got <= before {
		t.Fatalf("the third node's forward did not reach the subscriber that stayed: %d, then %d", before, got)
	}
	m.refusedFrom(m.n, 9, "the new owner's forward just below its mark")
	m.acceptedFrom(m.n, 10, "the new owner's forward at its mark")
}

// TestLoneMovedSubscriber: nobody is left on o's filter. The new owner's
// forward from its mark on is accepted though it reaches nobody here; a third
// node's is refused; o keeps advertising the route for the grace.
func TestLoneMovedSubscriber(t *testing.T) {
	m := newMoveTrio(t, time.Hour, false)
	m.newOwnerRoute()
	m.o.plane.Resolve("default", "s", "n", 10)
	m.acceptedFrom(m.n, 10, "the new owner's forward at its mark")
	m.refusedFrom(m.n, 9, "the new owner's forward below its mark")
	m.refusedFrom(m.x, 11, "a third node's forward")
	if !m.x.plane.Table().Known("o", "default", m.filter) {
		t.Fatal("o's route was withdrawn before its grace ended; a third node would match nothing")
	}
}

// TestConfirmationAfterTheMatchDoesNotVouchForIt: a confirmation acted on
// after a publish made here was matched and sent does not accept it: the
// forward to the new owner may have left before the session was held there.
func TestConfirmationAfterTheMatchDoesNotVouchForIt(t *testing.T) {
	m := newMoveTrio(t, time.Hour, true)
	m.newOwnerRoute()
	m.o.plane.afterSend = func() { m.o.plane.Resolve("default", "s", "n", 10) }
	m.refusedOnO("confirmed between the send and the check")
	m.o.plane.afterSend = nil
	m.acceptedOnO("the next publish")
}

// TestResolveOnlyForTheMovingClientAndAnotherNode: a confirmation for
// another client, or naming this node, ends nothing.
func TestResolveOnlyForTheMovingClientAndAnotherNode(t *testing.T) {
	m := newMoveTrio(t, time.Hour, true)
	m.newOwnerRoute()
	m.o.plane.Resolve("default", "someone-else", "n", 10)
	m.refusedOnO("another client confirmed")
	m.o.plane.Resolve("default", "s", "o", 10)
	m.refusedOnO("this node named as the owner")
}
