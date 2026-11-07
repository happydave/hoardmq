package broker

import (
	"testing"

	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

// limits.maxRetainedTopics on the wire: a retained publish of a new topic past
// the limit is answered 0x97 at MQTT 5.0 and closes the connection at 3.1.1,
// the refusals reach the scrape, and a reload that raises the limit lets the
// topic in.
func TestRetainedTopicLimitOnTheWire(t *testing.T) {
	n := startReloadable(t, func(d *config.Document) {
		metricsOn(d)
		d.Limits.MaxRetainedTopics = 1
	})
	retained := func(cl *client, topic string, id uint16) packet.ReasonCode {
		t.Helper()
		cl.send(&packet.Publish{Topic: topic, QoS: 1, PacketID: id, Retain: true, Payload: []byte("m")})
		ack, ok := cl.read().(*packet.Puback)
		if !ok || ack.PacketID != id {
			t.Fatalf("PUBACK %d: %+v", id, ack)
		}
		return ack.ReasonCode
	}
	v5, _ := n.connect("tcp", packet.V5, &packet.Connect{ClientID: "five", CleanStart: true, KeepAlive: 30}, false)
	if code := retained(v5, "r/a", 1); code.IsError() {
		t.Fatalf("the first retained topic: %v", code)
	}
	if code := retained(v5, "r/b", 2); code != packet.QuotaExceeded {
		t.Fatalf("a second retained topic past a limit of 1: %v, want 0x97", code)
	}
	v4, _ := n.connect("tcp", packet.V4, &packet.Connect{ClientID: "four", CleanStart: true, KeepAlive: 30}, false)
	v4.send(&packet.Publish{Topic: "r/c", QoS: 1, PacketID: 1, Retain: true, Payload: []byte("m")})
	if got := v4.readEnd(); len(got) != 0 {
		t.Fatalf("the 3.1.1 publish past the limit was answered %v, want the connection closed", got)
	}
	_, body := scrape(t, n.fixture, "/metrics")
	if v, ok := metricValue(t, body, `hoardmq_retained_limit_refusals_total{namespace="default",op="write"}`); !ok || v != 2 {
		t.Fatalf("refusals on the scrape %v %v, want 2", v, ok)
	}
	n.rewrite(t, func(d *config.Document) { d.Limits.MaxRetainedTopics = 2 })
	if r := n.srv.Reload(); !r.Applied {
		t.Fatalf("reload: %+v", r)
	}
	if code := retained(v5, "r/b", 3); code.IsError() {
		t.Fatalf("after the reload to 2: %v", code)
	}
}
