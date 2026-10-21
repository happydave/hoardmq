package lease

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
)

// gateHold holds one call of a seam on one node until released.
type gateHold struct {
	node    atomic.Value // string
	armed   atomic.Bool
	reached chan struct{}
	release chan struct{}
	unblock func()
}

func newGateHold() *gateHold {
	h := &gateHold{reached: make(chan struct{}, 1), release: make(chan struct{})}
	h.node.Store("")
	h.unblock = sync.OnceFunc(func() { close(h.release) })
	return h
}

// hold blocks the caller when it is on the hold's node, the hold is armed and
// match is true.
func (h *gateHold) hold(node string, match bool) {
	if !match || h.node.Load().(string) != node || !h.armed.CompareAndSwap(true, false) {
		return
	}
	h.reached <- struct{}{}
	<-h.release
}

func (h *gateHold) wait(t *testing.T, what string) {
	t.Helper()
	select {
	case <-h.reached:
	case <-time.After(5 * time.Second):
		t.Fatalf("precondition not reached: %s", what)
	}
}

func acquiresX(ops []Op) bool {
	for _, op := range ops {
		if op.Kind == OpAcquire && op.ClientID == "x" {
			return true
		}
	}
	return false
}

// TestHandoffAnsweredBeforeTheReceiverRecordsItsAdoption: the leader owns x,
// whose session it holds, and its drain hands its leases to a follower whose
// CONNECT for x has looked the lease up under the leader and forwarded its
// acquire (held at the leader). The hand-off applies first, then the
// acquire, and the leader answers the follower while the follower's own apply
// of the hand-off has not yet recorded the adoption (its hook held). The
// CONNECT must not be answered without the session: it gets the hand-off's
// state once the adoption is recorded.
func TestHandoffAnsweredBeforeTheReceiverRecordsItsAdoption(t *testing.T) {
	fwd, adopt := newGateHold(), newGateHold()
	fixtureFallback = foundNowhere
	t.Cleanup(func() { fixtureFallback = nil })
	fixtureOptions = func(o *Options) {
		heldBack(o)
		name := o.Node
		o.ForwardedGate = func(_ string, ops []Op) { fwd.hold(name, acquiresX(ops)) }
		o.AdoptGate = func(_, id string) { adopt.hold(name, id == "x") }
	}
	t.Cleanup(func() { fixtureOptions = nil })
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	t.Cleanup(fwd.unblock)
	t.Cleanup(adopt.unblock)
	lead := c.settled()
	recv := c.follower(lead)
	fwd.node.Store(lead.name)
	adopt.node.Store(recv.name)

	if _, err := acquire(t, lead, "x", nil, 300, false); err != nil {
		t.Fatal(err)
	}
	lead.sessions.put("default", "x", sessionFor("x"))
	ownedBy(t, recv, "x", lead.name)

	fwd.armed.Store(true)
	adopt.armed.Store(true)
	done := acquireAsyncAt(recv, "x", 300)
	fwd.wait(t, "the receiver's acquire of x, forwarded, held at the leader")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	moved, err := lead.svc.Handoff(ctx, recv.name)
	if err != nil || moved != 1 {
		t.Fatalf("precondition not reached: hand-off moved %d, %v", moved, err)
	}
	adopt.wait(t, "the receiver's hook for the hand-off held")
	fwd.unblock()

	select {
	case o := <-done:
		t.Fatalf("the CONNECT was answered while the receiver had not recorded the hand-off's adoption: state %v, evidence %+v, prev %q, err %v",
			o.res.State != nil, o.res.Empty, o.res.PrevOwner, o.err)
	case <-time.After(500 * time.Millisecond):
	}
	adopt.unblock()
	o := outcomeOf(t, done)
	if o.err != nil || o.res.State == nil || o.res.Empty.Reason != "" {
		t.Fatalf("after the adoption was recorded: state %v, evidence %+v, err %v; want the hand-off's state and no evidence", o.res.State != nil, o.res.Empty, o.err)
	}
}

// TestConnectBetweenTheHandoffsApplyAndItsHook: the receiver's table has
// applied the hand-off of x, and its hook has not yet recorded the adoption
// (held), when a CONNECT for x arrives there and finds the lease its own. The
// CONNECT must not be answered without the session.
func TestConnectBetweenTheHandoffsApplyAndItsHook(t *testing.T) {
	adopt := newGateHold()
	fixtureFallback = foundNowhere
	t.Cleanup(func() { fixtureFallback = nil })
	fixtureOptions = func(o *Options) {
		heldBack(o)
		name := o.Node
		o.AdoptGate = func(_, id string) { adopt.hold(name, id == "x") }
	}
	t.Cleanup(func() { fixtureOptions = nil })
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	t.Cleanup(adopt.unblock)
	lead := c.settled()
	recv := c.follower(lead)
	adopt.node.Store(recv.name)

	if _, err := acquire(t, lead, "x", nil, 300, false); err != nil {
		t.Fatal(err)
	}
	lead.sessions.put("default", "x", sessionFor("x"))
	ownedBy(t, recv, "x", lead.name)

	adopt.armed.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	moved, err := lead.svc.Handoff(ctx, recv.name)
	if err != nil || moved != 1 {
		t.Fatalf("precondition not reached: hand-off moved %d, %v", moved, err)
	}
	adopt.wait(t, "the receiver's hook for the hand-off held")
	if l, ok := recv.svc.table.Lookup("default", "x"); !ok || l.Owner != recv.name {
		t.Fatalf("precondition not reached: the receiver's table has x at %+v (found %v), want its own", l, ok)
	}
	done := acquireAsyncAt(recv, "x", 300)
	select {
	case o := <-done:
		t.Fatalf("the CONNECT was answered while the receiver had not recorded the hand-off's adoption: state %v, evidence %+v, err %v",
			o.res.State != nil, o.res.Empty, o.err)
	case <-time.After(500 * time.Millisecond):
	}
	adopt.unblock()
	o := outcomeOf(t, done)
	if o.err != nil || o.res.State == nil || o.res.Empty.Reason != "" {
		t.Fatalf("after the adoption was recorded: state %v, evidence %+v, err %v; want the hand-off's state and no evidence", o.res.State != nil, o.res.Empty, o.err)
	}
}

// TestConnectPastAdmissionWhileTheHandoffApplies: a CONNECT for x on the
// receiver is admitted and held before its lookup; the hand-off of x then
// applies on the receiver, its table updated and its hook held before it
// records the adoption. The CONNECT, let go, finds the lease its own and must
// not be answered without the session.
func TestConnectPastAdmissionWhileTheHandoffApplies(t *testing.T) {
	look, adopt := newGateHold(), newGateHold()
	fixtureFallback = foundNowhere
	t.Cleanup(func() { fixtureFallback = nil })
	fixtureOptions = func(o *Options) {
		heldBack(o)
		name := o.Node
		o.LookupGate = func(_, id string) { look.hold(name, id == "x") }
		o.AdoptGate = func(_, id string) { adopt.hold(name, id == "x") }
	}
	t.Cleanup(func() { fixtureOptions = nil })
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	t.Cleanup(look.unblock)
	t.Cleanup(adopt.unblock)
	lead := c.settled()
	recv := c.follower(lead)
	look.node.Store(recv.name)
	adopt.node.Store(recv.name)

	if _, err := acquire(t, lead, "x", nil, 300, false); err != nil {
		t.Fatal(err)
	}
	lead.sessions.put("default", "x", sessionFor("x"))
	ownedBy(t, recv, "x", lead.name)

	look.armed.Store(true)
	done := acquireAsyncAt(recv, "x", 300)
	look.wait(t, "the receiver's CONNECT for x admitted and held before its lookup")
	adopt.armed.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	moved, err := lead.svc.Handoff(ctx, recv.name)
	if err != nil || moved != 1 {
		t.Fatalf("precondition not reached: hand-off moved %d, %v", moved, err)
	}
	adopt.wait(t, "the receiver's hook for the hand-off held")
	if l, ok := recv.svc.table.Lookup("default", "x"); !ok || l.Owner != recv.name {
		t.Fatalf("precondition not reached: the receiver's table has x at %+v (found %v), want its own", l, ok)
	}
	look.unblock()
	select {
	case o := <-done:
		t.Fatalf("the CONNECT was answered while the receiver had not recorded the hand-off's adoption: state %v, evidence %+v, err %v",
			o.res.State != nil, o.res.Empty, o.err)
	case <-time.After(500 * time.Millisecond):
	}
	adopt.unblock()
	o := outcomeOf(t, done)
	if o.err != nil || o.res.State == nil || o.res.Empty.Reason != "" {
		t.Fatalf("after the adoption was recorded: state %v, evidence %+v, err %v; want the hand-off's state and no evidence", o.res.State != nil, o.res.Empty, o.err)
	}
	if n := recv.svc.hookWaitsServed.Load(); n != 1 {
		t.Fatalf("hook waits served %d, want 1", n)
	}
}

func TestHooksReached(t *testing.T) {
	for _, c := range []struct {
		hooked, index uint64
		want          bool
	}{
		{hooked: 0, index: 7, want: false},
		{hooked: 6, index: 7, want: false},
		{hooked: 7, index: 7, want: true},
		{hooked: 8, index: 7, want: true},
	} {
		if got := hooksReached(c.hooked, c.index); got != c.want {
			t.Errorf("hooksReached(%d, %d) = %v, want %v", c.hooked, c.index, got, c.want)
		}
	}
}

// TestConnectRefusedWhileTheHandoffsHooksAreBehind: as in
// TestConnectPastAdmissionWhileTheHandoffApplies, with a CONNECT bound that
// ends while the receiver's hook is held. The CONNECT is refused Server Busy
// with nothing served, and the hand-off's adoption is there for its retry.
func TestConnectRefusedWhileTheHandoffsHooksAreBehind(t *testing.T) {
	look, adopt := newGateHold(), newGateHold()
	fixtureFallback = foundNowhere
	t.Cleanup(func() { fixtureFallback = nil })
	fixtureOptions = func(o *Options) {
		heldBack(o)
		name := o.Node
		o.LookupGate = func(_, id string) { look.hold(name, id == "x") }
		o.AdoptGate = func(_, id string) { adopt.hold(name, id == "x") }
	}
	t.Cleanup(func() { fixtureOptions = nil })
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	t.Cleanup(look.unblock)
	t.Cleanup(adopt.unblock)
	lead := c.settled()
	recv := c.follower(lead)
	look.node.Store(recv.name)
	adopt.node.Store(recv.name)

	if _, err := acquire(t, lead, "x", nil, 300, false); err != nil {
		t.Fatal(err)
	}
	lead.sessions.put("default", "x", sessionFor("x"))
	ownedBy(t, recv, "x", lead.name)

	look.armed.Store(true)
	done := make(chan acquireOutcome, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
		defer cancel()
		r, err := recv.svc.Acquire(ctx, "default", "x", nil, 300, false)
		done <- acquireOutcome{r, err}
	}()
	look.wait(t, "the receiver's CONNECT for x admitted and held before its lookup")
	adopt.armed.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if moved, err := lead.svc.Handoff(ctx, recv.name); err != nil || moved != 1 {
		t.Fatalf("precondition not reached: hand-off moved %d, %v", moved, err)
	}
	adopt.wait(t, "the receiver's hook for the hand-off held")
	look.unblock()
	o := outcomeOf(t, done)
	var r *Refusal
	if o.res.State != nil || !errors.As(o.err, &r) || r.Reason != packet.ServerBusy {
		t.Fatalf("with the hooks behind past the bound: state %v, err %v; want Server Busy and no state", o.res.State != nil, o.err)
	}
	if n := recv.svc.hookWaitsRefused.Load(); n != 1 {
		t.Fatalf("hook waits refused %d, want 1", n)
	}
	adopt.unblock()
	waitFor(t, "the hand-off's adoption recorded for the retry", 5*time.Second, func() bool { return adoptionOf(recv, "x") != nil })
}

// pastAdmission is the setup of TestConnectPastAdmissionWhileTheHandoffApplies
// up to the receiver's hook held: x's session on the leader, the leader's
// hand-off of x applied on the receiver's table with its hook held, and an
// acquire of x on the receiver, started by start, held before its lookup and
// then let go.
func pastAdmission(t *testing.T, start func(recv *tnode)) (lead, recv *tnode, adopt *gateHold) {
	t.Helper()
	look := newGateHold()
	adopt = newGateHold()
	fixtureFallback = foundNowhere
	t.Cleanup(func() { fixtureFallback = nil })
	fixtureOptions = func(o *Options) {
		heldBack(o)
		name := o.Node
		o.LookupGate = func(_, id string) { look.hold(name, id == "x") }
		o.AdoptGate = func(_, id string) { adopt.hold(name, id == "x") }
	}
	t.Cleanup(func() { fixtureOptions = nil })
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	t.Cleanup(look.unblock)
	t.Cleanup(adopt.unblock)
	lead = c.settled()
	recv = c.follower(lead)
	look.node.Store(recv.name)
	adopt.node.Store(recv.name)
	if _, err := acquire(t, lead, "x", nil, 300, false); err != nil {
		t.Fatal(err)
	}
	lead.sessions.put("default", "x", sessionFor("x"))
	ownedBy(t, recv, "x", lead.name)
	look.armed.Store(true)
	start(recv)
	look.wait(t, "the receiver's CONNECT for x admitted and held before its lookup")
	adopt.armed.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if moved, err := lead.svc.Handoff(ctx, recv.name); err != nil || moved != 1 {
		t.Fatalf("precondition not reached: hand-off moved %d, %v", moved, err)
	}
	adopt.wait(t, "the receiver's hook for the hand-off held")
	look.unblock()
	return lead, recv, adopt
}

// TestCleanStartPastAdmissionWhileTheHandoffApplies: a clean start waits for
// the hand-off's adoption too, and discards it, so the worker never imports
// the old session over the new one.
func TestCleanStartPastAdmissionWhileTheHandoffApplies(t *testing.T) {
	done := make(chan acquireOutcome, 1)
	_, recv, adopt := pastAdmission(t, func(recv *tnode) {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			r, err := recv.svc.Acquire(ctx, "default", "x", nil, 300, true)
			done <- acquireOutcome{r, err}
		}()
	})
	select {
	case o := <-done:
		t.Fatalf("the clean start was answered before the hand-off's adoption was recorded: err %v", o.err)
	case <-time.After(500 * time.Millisecond):
	}
	adopt.unblock()
	o := outcomeOf(t, done)
	if o.err != nil || o.res.State != nil {
		t.Fatalf("clean start: state %v, err %v; want no state and no error", o.res.State != nil, o.err)
	}
	if n := recv.svc.adoptDiscarded.Load(); n != 1 || adoptionOf(recv, "x") != nil {
		t.Fatalf("adoptions discarded %d, adoption left %v; want the hand-off's adoption discarded", n, adoptionOf(recv, "x") != nil)
	}
}

// TestConnectWaitingOnHooksWhenTheServiceStops: a CONNECT waiting for the
// receiver's hooks when its lease service stops is refused Server
// Unavailable.
func TestConnectWaitingOnHooksWhenTheServiceStops(t *testing.T) {
	var done chan acquireOutcome
	_, recv, _ := pastAdmission(t, func(recv *tnode) { done = acquireAsyncAt(recv, "x", 300) })
	waitFor(t, "the CONNECT waiting on the receiver's hooks", 5*time.Second, func() bool {
		select {
		case o := <-done:
			t.Fatalf("answered before the stop: state %v, err %v", o.res.State != nil, o.err)
		default:
		}
		return recv.svc.table.HookedIndex() < recv.svc.table.AppliedIndex()
	})
	go recv.svc.Stop()
	o := outcomeOf(t, done)
	var r *Refusal
	if o.res.State != nil || !errors.As(o.err, &r) || r.Reason != packet.ServerUnavailable {
		t.Fatalf("on the service's stop: state %v, err %v; want Server Unavailable", o.res.State != nil, o.err)
	}
}
