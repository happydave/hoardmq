package routing

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/dispatch"
	"github.com/happydave/hoardmq/internal/message"
)

// TestNotServingRefusesForwardedDeliveries: a receiver that does not serve
// its sessions yet refuses a QoS 1 forward, so its sender's barrier fails and
// nothing reaches the sink, and counts it; a QoS 0 forward is delivered; once
// it serves, a QoS 1 forward is delivered and its barrier passes.
func TestNotServingRefusesForwardedDeliveries(t *testing.T) {
	var serving atomic.Bool
	m := newCountingMetrics()
	c := newClusterOpts(t, []string{"a", "b"}, dispatch.RoundRobin, func(o *Options) {
		if o.Node == "b" {
			o.Serving = serving.Load
			o.Metrics = m
		}
	})
	a, b := c.nodes["a"], c.nodes["b"]
	b.plane.Observe("default", "t/#", 1)
	waitFor(t, "b's route on a", 5*time.Second, func() bool {
		nodes, _ := a.plane.Table().Match("default", "t/1")
		return len(nodes) == 1
	})
	barrier := func(msg *message.Message) error {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		return a.plane.Barrier(ctx, msg.Forwards)
	}

	refused := publish(a, "p", "t/1", "refused", 1)
	if err := barrier(refused); !errors.Is(err, ErrRoutesUnavailable) {
		t.Fatalf("a QoS 1 forward to a node not serving: barrier %v, want ErrRoutesUnavailable", err)
	}
	if n := b.sink.directCount(); n != 0 {
		t.Fatalf("the sink received %d messages while its node was not serving", n)
	}
	if got := m.count("hoardmq_forward_failures_total", "not-serving"); got != 1 {
		t.Fatalf("not-serving refusals %v, want 1", got)
	}

	q0 := &message.Message{Namespace: "default", Topic: "t/1", Payload: []byte("q0"), Publisher: message.Publisher{ClientID: "p"}, Seq: 2}
	_, _ = a.plane.Forward(q0)
	waitFor(t, "the QoS 0 forward delivered", 5*time.Second, func() bool { return b.sink.directCount() == 1 })

	serving.Store(true)
	served := publish(a, "p", "t/1", "served", 3)
	if err := barrier(served); err != nil {
		t.Fatalf("a QoS 1 forward once serving: barrier %v", err)
	}
	waitFor(t, "the QoS 1 forward delivered", 5*time.Second, func() bool { return b.sink.directCount() == 2 })
}

// TestAwaitLoadHoldsRouteSendsUntilLoaded: a plane that waits for its store
// sends its peer neither its routes nor a full set until Loaded; then the
// peer has both at once, not at the plane's next periodic sync.
func TestAwaitLoadHoldsRouteSendsUntilLoaded(t *testing.T) {
	c := newClusterOpts(t, []string{"a", "b"}, dispatch.RoundRobin, func(o *Options) {
		if o.Node == "b" {
			o.AwaitLoad = true
			o.SyncInterval = time.Minute
		}
	})
	a, b := c.nodes["a"], c.nodes["b"]
	b.plane.Observe("default", "t/#", 1)
	// a's own full set to b arrives as usual: the hold is b's sends only.
	waitFor(t, "a's full set at b", 5*time.Second, func() bool { _, ok := b.plane.SyncAges()["a"]; return ok })
	time.Sleep(500 * time.Millisecond)
	if nodes, _ := a.plane.Table().Match("default", "t/1"); len(nodes) != 0 {
		t.Fatalf("a holds b's route %v before b loaded", nodes)
	}
	if _, ok := a.plane.SyncAges()["b"]; ok {
		t.Fatal("a received a full set from b before b loaded")
	}
	b.plane.Loaded()
	waitFor(t, "b's route and full set at a", 2*time.Second, func() bool {
		nodes, _ := a.plane.Table().Match("default", "t/1")
		_, ok := a.plane.SyncAges()["b"]
		return len(nodes) == 1 && nodes[0] == "b" && ok
	})
}

// TestNotServingRefusesAGroupDecide: a shared group whose only members are on
// a node not serving its sessions yet: a QoS 1 publish from another node, sent
// to that node to decide, is refused and reaches no member; once it serves,
// one member takes the next.
func TestNotServingRefusesAGroupDecide(t *testing.T) {
	var serving atomic.Bool
	c := newClusterOpts(t, []string{"a", "b"}, dispatch.RoundRobin, func(o *Options) {
		if o.Node == "b" {
			o.Serving = serving.Load
		}
	})
	a, b := c.nodes["a"], c.nodes["b"]
	b.plane.Observe("default", "$share/g/t/#", 1)
	waitFor(t, "the group's route on a", 5*time.Second, func() bool {
		_, groups := a.plane.Table().Match("default", "t/1")
		return len(groups) == 1
	})
	send := func(seq uint64) error {
		msg := publish(a, "p", "t/1", "x", seq)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		return a.plane.Barrier(ctx, msg.Forwards)
	}
	if err := send(1); !errors.Is(err, ErrRoutesUnavailable) {
		t.Fatalf("a group decide sent to a node not serving: barrier %v, want ErrRoutesUnavailable", err)
	}
	if n := b.sink.groupCount(); n != 0 {
		t.Fatalf("a member took %d messages while its node was not serving", n)
	}
	serving.Store(true)
	if err := send(2); err != nil {
		t.Fatalf("a group decide once serving: barrier %v", err)
	}
	waitFor(t, "the member takes it", 5*time.Second, func() bool { return b.sink.groupCount() == 1 })
}
