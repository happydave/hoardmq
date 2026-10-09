package session

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

// nopReplication is a replication layer that holds everything instantly. It
// exists so the publish path's naming of covered sessions is exercised and
// measured; nothing in the tree implements this interface otherwise.
type nopReplication struct {
	named    []store.Scope
	forwards []message.ForwardRef
}

func (r *nopReplication) Barrier(_ context.Context, keys []store.Scope, forwards []message.ForwardRef) error {
	r.named = append([]store.Scope(nil), keys...)
	r.forwards = append([]message.ForwardRef(nil), forwards...)
	return nil
}
func (r *nopReplication) Durable(_ context.Context, keys []store.Scope) error {
	r.named = append([]store.Scope(nil), keys...)
	return nil
}

func (r *nopReplication) has(s store.Scope) bool {
	for _, k := range r.named {
		if k == s {
			return true
		}
	}
	return false
}
func (r *nopReplication) StoreRetained(*message.Message) bool { return true }
func (r *nopReplication) QueryRetained(string, string, time.Time) []*message.Message {
	return nil
}

// TestPublishNamesEverySessionItDeliveredInto is the invariant-4 check inside
// this package: a publish's barrier must name the publisher and every
// subscriber it delivered into, or it would acknowledge records it never
// examined. The cross-package version, which drives a real refusal, is in the
// simulation suite.
func TestPublishNamesEverySessionItDeliveredInto(t *testing.T) {
	m, _ := newManager(t, nil)
	rep := &nopReplication{}
	m.SetReplication(rep)

	pub := newFake("pub", packet.V5, 100)
	connectV5(m, pub, true, 0, nil)
	for _, id := range []string{"sub-a", "sub-b", "sub-c"} {
		c := newFake(id, packet.V5, 100)
		connectV5(m, c, true, 0, nil)
		subscribe(m, c, 1, "t/x")
	}

	if code, _ := m.Publish(pub, &packet.Publish{Topic: "t/x", QoS: 1, PacketID: 1, Payload: []byte("x")}); code != packet.Success {
		t.Fatalf("publish: %v", code)
	}
	// The publisher plus the three subscribers it delivered into. The set is
	// compared, not its size: a count cannot tell a duplicate from a miss.
	if len(rep.named) != 4 {
		t.Errorf("barrier named %d scopes, want 4: %v", len(rep.named), rep.named)
	}
	for _, want := range []string{"pub", "sub-a", "sub-b", "sub-c"} {
		if !rep.has(store.Scope{Namespace: "default", ClientID: want}) {
			t.Errorf("barrier did not name %q: %v", want, rep.named)
		}
	}
}

// TestRetainedPublishNamesItsTopic: a retained publish writes a record keyed
// by topic and replicated by shard, so an acknowledgement that named only
// sessions would pass over a retained record a follower refused. The refusal
// side is keyed by topic already; this is the half that names it.
func TestRetainedPublishNamesItsTopic(t *testing.T) {
	m, _ := newManager(t, nil)
	rep := &nopReplication{}
	m.SetReplication(rep)
	pub := newFake("pub", packet.V5, 100)
	connectV5(m, pub, true, 0, nil)

	if code, _ := m.Publish(pub, &packet.Publish{Topic: "t/x", QoS: 1, PacketID: 1, Retain: true, Payload: []byte("x")}); code != packet.Success && code != packet.NoMatchingSubscribers {
		t.Fatalf("publish: %v", code)
	}
	if !rep.has(store.Scope{Namespace: "default", Topic: "t/x"}) {
		t.Errorf("a retained publish must name its topic, named: %v", rep.named)
	}
}

// TestPublishAllocationsAreBoundedByFanOut holds the publish path to the
// budget WI 1510's plan set: naming costs at most one allocation per publish
// plus one per matched subscriber, a QoS 0 publish names nothing, and a node
// with no replication attached pays nothing at any QoS.
func TestPublishAllocationsAreBoundedByFanOut(t *testing.T) {
	measure := func(t *testing.T, attach bool, qos byte, subs int) float64 {
		t.Helper()
		m, _ := newManager(t, nil)
		if attach {
			m.SetReplication(&nopReplication{})
		}
		pub := newFake("pub", packet.V5, 100)
		connectV5(m, pub, true, 0, nil)
		for i := 0; i < subs; i++ {
			c := newFake("sub"+string(rune('a'+i)), packet.V5, 100)
			connectV5(m, c, true, 0, nil)
			subscribe(m, c, 0, "t/x")
		}
		var id uint16
		return testing.AllocsPerRun(200, func() {
			id++
			m.Publish(pub, &packet.Publish{Topic: "t/x", QoS: qos, PacketID: id, Payload: []byte("x")})
		})
	}

	off := measure(t, false, 1, 4)
	on0 := measure(t, true, 0, 4)
	on1 := measure(t, true, 1, 4)
	t.Logf("allocs/publish: no replication QoS 1 = %.1f, replicated QoS 0 = %.1f, replicated QoS 1 (4 subs) = %.1f", off, on0, on1)

	// The naming budget: one for the slice plus one per matched subscriber,
	// over the QoS 0 baseline which names nothing.
	if budget := on0 + 1 + 4; on1 > budget {
		t.Errorf("replicated QoS 1 publish allocates %.1f, over the stated budget of %.1f", on1, budget)
	}
	if on0 > off {
		t.Errorf("a QoS 0 publish must name nothing: %.1f allocs against %.1f with no replication", on0, off)
	}
}

// namingForwarder stands for a routing plane that forwarded the message to
// two peers and named the frames on it, as routing.Plane.send does.
type namingForwarder struct{ refs []message.ForwardRef }

func (f namingForwarder) Forward(msg *message.Message) (int, error) {
	msg.Forwards = append(msg.Forwards, f.refs...)
	return len(f.refs), nil
}

// TestPublishBarrierReceivesItsOwnForwards: the acknowledgement waits on the
// frames this publish sent, so the barrier must be handed exactly those. The
// set is compared, not the count: a count passes a duplicate beside a miss.
func TestPublishBarrierReceivesItsOwnForwards(t *testing.T) {
	sent := []message.ForwardRef{{Peer: "n2", Seq: 41}, {Peer: "n3", Seq: 7}}
	m, _ := newManager(t, func(o *Options) { o.Forwarder = namingForwarder{refs: sent} })
	rep := &nopReplication{}
	m.SetReplication(rep)
	pub := newFake("pub", packet.V5, 100)
	connectV5(m, pub, true, 0, nil)

	if code, _ := m.Publish(pub, &packet.Publish{Topic: "t/x", QoS: 1, PacketID: 1, Payload: []byte("x")}); code != packet.Success {
		t.Fatalf("publish: %v", code)
	}
	if !reflect.DeepEqual(rep.forwards, sent) {
		t.Fatalf("the barrier was handed %v, want %v", rep.forwards, sent)
	}

	// A second publish that forwards nothing hands the barrier nothing: it
	// must not inherit the first one's frames.
	m2, _ := newManager(t, func(o *Options) { o.Forwarder = namingForwarder{} })
	rep2 := &nopReplication{forwards: sent}
	m2.SetReplication(rep2)
	pub2 := newFake("pub2", packet.V5, 100)
	connectV5(m2, pub2, true, 0, nil)
	if code, _ := m2.Publish(pub2, &packet.Publish{Topic: "t/x", QoS: 1, PacketID: 1, Payload: []byte("x")}); code != packet.Success && code != packet.NoMatchingSubscribers {
		t.Fatalf("publish: %v", code)
	}
	if len(rep2.forwards) != 0 {
		t.Fatalf("a publish that forwarded nothing handed the barrier %v", rep2.forwards)
	}
}
