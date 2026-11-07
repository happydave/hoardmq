package session

import (
	"testing"

	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

// TestRetainedTopicLimitRefusesANewTopicWhole: past MaxRetainedTopics a
// retained publish of a new topic is refused with Quota exceeded before it is
// routed, so no subscriber receives it and nothing is stored; a replacement,
// a delete and a publish that is not retained pass, and the refusals are
// counted.
func TestRetainedTopicLimitRefusesANewTopicWhole(t *testing.T) {
	metrics := &seriesMetrics{}
	m, _ := newManager(t, func(o *Options) { o.Metrics = metrics; o.MaxRetainedTopics = 1 })
	pub := newFake("pub", packet.V5, 100)
	sub := newFake("sub", packet.V5, 100)
	connectV5(m, pub, true, 0, nil)
	connectV5(m, sub, true, 0, nil)
	subscribe(m, sub, 1, "a", "b", "c")
	retained := func(topic string, qos byte, id uint16, payload string) packet.ReasonCode {
		t.Helper()
		code, _ := m.Publish(pub, &packet.Publish{Topic: topic, QoS: qos, PacketID: id, Retain: true, Payload: []byte(payload)})
		return code
	}
	if code := retained("a", 1, 1, "1"); code.IsError() {
		t.Fatalf("the first topic: %v", code)
	}
	if code := retained("b", 1, 2, "1"); code != packet.QuotaExceeded {
		t.Fatalf("a second topic past a limit of 1: %v, want Quota exceeded", code)
	}
	if code := retained("c", 0, 0, "1"); code != packet.QuotaExceeded {
		t.Fatalf("a QoS 0 retained publish of a new topic past the limit: %v, want Quota exceeded", code)
	}
	if code := retained("a", 1, 3, "2"); code.IsError() {
		t.Fatalf("a replacement at the limit: %v", code)
	}
	if code := publish(m, pub, "b", 1, 4, "live"); code.IsError() {
		t.Fatalf("a publish to a new topic that is not retained: %v", code)
	}
	var got []string
	for _, p := range sub.publishes() {
		got = append(got, p.Topic+"="+string(p.Payload))
	}
	if len(got) != 3 || got[0] != "a=1" || got[1] != "a=2" || got[2] != "b=live" {
		t.Fatalf("the subscriber received %v, want a=1, a=2, b=live", got)
	}
	if all := m.router.AllRetained(); len(all) != 1 || all[0].Topic != "a" {
		t.Fatalf("stored %v, want only a", all)
	}
	if v := metrics.snapshot()[seriesKey("hoardmq_retained_limit_refusals_total", map[string]string{"namespace": "default", "op": "write"})]; v != 2 {
		t.Fatalf("refusals counted %v, want 2", v)
	}
	if code := retained("a", 1, 5, ""); code.IsError() {
		t.Fatalf("a delete at the limit: %v", code)
	}
	if code := retained("b", 1, 6, "1"); code.IsError() {
		t.Fatalf("a new topic once the delete made room: %v", code)
	}
}

// TestRetainedTopicLimitOffByDefault: with no limit a namespace takes every
// new retained topic.
func TestRetainedTopicLimitOffByDefault(t *testing.T) {
	m, _ := newManager(t, nil)
	pub := newFake("pub", packet.V5, 100)
	connectV5(m, pub, true, 0, nil)
	for i, topic := range []string{"a", "b", "c", "d"} {
		if code, _ := m.Publish(pub, &packet.Publish{Topic: topic, QoS: 1, PacketID: uint16(i + 1), Retain: true, Payload: []byte("1")}); code.IsError() {
			t.Fatalf("%s: %v", topic, code)
		}
	}
	if n := len(m.router.AllRetained()); n != 4 {
		t.Fatalf("stored %d, want 4", n)
	}
}

// TestForwardedRetainedWriteIsDecidedByTheLimit: a write another node
// forwarded to this one as the shard leader is refused past the limit and
// counted; a hand-over or a promotion, which places a message the cluster
// already holds, is installed whatever the count.
func TestForwardedRetainedWriteIsDecidedByTheLimit(t *testing.T) {
	metrics := &seriesMetrics{}
	m, _ := newManager(t, func(o *Options) { o.Metrics = metrics; o.MaxRetainedTopics = 1 })
	msg := func(topic string) *message.Message {
		return &message.Message{Namespace: "default", Topic: topic, Payload: []byte("1"), Retain: true}
	}
	if durable, refusal := m.InjectRetainedWrite(msg("a")); !durable || refusal != store.RetainStored {
		t.Fatalf("the first forwarded write: durable %v, refusal %v", durable, refusal)
	}
	if durable, refusal := m.InjectRetainedWrite(msg("b")); durable || refusal != store.RetainOverLimit {
		t.Fatalf("a forwarded write past the limit: durable %v, refusal %v; want refused by the limit", durable, refusal)
	}
	if durable, _ := m.InjectRetained(msg("c")); !durable {
		t.Fatal("a hand-over past the limit was not installed")
	}
	held := map[string]bool{}
	for _, r := range m.router.AllRetained() {
		held[r.Topic] = true
	}
	if len(held) != 2 || !held["a"] || !held["c"] {
		t.Fatalf("stored %v, want a and c", held)
	}
	if v := metrics.snapshot()[seriesKey("hoardmq_retained_limit_refusals_total", map[string]string{"namespace": "default", "op": "write"})]; v != 1 {
		t.Fatalf("refusals counted %v, want 1", v)
	}
}

// TestRetainedWillPastTheLimitIsDropped: a Will with Retain set, for a new
// topic past the limit, is neither stored nor delivered, and is counted.
func TestRetainedWillPastTheLimitIsDropped(t *testing.T) {
	metrics := &seriesMetrics{}
	m, _ := newManager(t, func(o *Options) { o.Metrics = metrics; o.MaxRetainedTopics = 1 })
	pub := newFake("pub", packet.V5, 100)
	sub := newFake("sub", packet.V5, 100)
	connectV5(m, pub, true, 0, nil)
	connectV5(m, sub, true, 0, nil)
	subscribe(m, sub, 1, "w")
	if code, _ := m.Publish(pub, &packet.Publish{Topic: "a", QoS: 1, PacketID: 1, Retain: true, Payload: []byte("1")}); code.IsError() {
		t.Fatalf("the first topic: %v", code)
	}
	c := newFake("c", packet.V5, 100)
	connectV5(m, c, true, 0, &packet.Will{Topic: "w", Payload: []byte("gone"), Retain: true})
	m.Disconnect(c, false, packet.UnspecifiedError, true)
	if got := sub.publishes(); len(got) != 0 {
		t.Fatalf("the subscriber received %d messages, want none", len(got))
	}
	if all := m.router.AllRetained(); len(all) != 1 {
		t.Fatalf("stored %d retained, want 1", len(all))
	}
	if v := metrics.snapshot()[seriesKey("hoardmq_retained_limit_refusals_total", map[string]string{"namespace": "default", "op": "write"})]; v != 1 {
		t.Fatalf("refusals counted %v, want 1", v)
	}
}
