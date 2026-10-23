package conn

import (
	"fmt"
	"testing"

	"github.com/happydave/hoardmq/internal/packet"
)

// A Will Topic that is not a valid topic name is refused before any
// credential is examined or the core asked: 0x90 at level 5, a close with no
// CONNACK at levels 4 and 3. The refusal is counted and audited with why.
func TestWillTopicRefusal(t *testing.T) {
	for _, c := range []struct {
		topic, why string
	}{
		{"a/#", "will: topic name contains a wildcard character"},
		{"a/+/b", "will: topic name contains a wildcard character"},
		{"", "will: topic name is empty"},
	} {
		for _, v := range []packet.Version{packet.V5, packet.V4, packet.V3} {
			t.Run(fmt.Sprintf("%q/%v", c.topic, v), func(t *testing.T) {
				o, a, m, rec := rateOptions(&gate{})
				core := &fakeCore{}
				h := newHarness(t, o, core)
				h.enc = &packet.Encoder{Version: v}
				h.dec.Version = v
				connect := credentialConnect(v)
				connect.Will = &packet.Will{Topic: c.topic, Payload: []byte("gone")}
				h.send(connect)
				if v == packet.V5 {
					if ack := h.readConnack(); ack.ReasonCode != packet.TopicNameInvalid {
						t.Fatalf("level 5: %+v, want 0x90", ack)
					}
				} else {
					h.expectEOF()
				}
				<-h.done
				core.mu.Lock()
				connects := len(core.connects)
				core.mu.Unlock()
				if calls := a.calls.Load(); calls != 0 || connects != 0 {
					t.Fatalf("%d authenticator calls, %d core connects; want none", calls, connects)
				}
				if got := m.get("hoardmq_auth_failures_total{listener=test,reason=Topic Name invalid (0x90)}"); got != 1 {
					t.Fatalf("CONNECT refusals by reason %v, want 1: %v", got, m.m)
				}
				rec.mu.Lock()
				defer rec.mu.Unlock()
				if len(rec.details) != 1 || rec.details[0]["reason"] != c.why {
					t.Fatalf("audit %v, want one record with reason %q", rec.details, c.why)
				}
			})
		}
	}
}

// A valid Will Topic, one starting with $ included, is accepted at every
// level.
func TestWillTopicValid(t *testing.T) {
	for _, topic := range []string{"a/b", "$will/x"} {
		for _, v := range []packet.Version{packet.V5, packet.V4, packet.V3} {
			t.Run(fmt.Sprintf("%q/%v", topic, v), func(t *testing.T) {
				o, _, _, _ := rateOptions(&gate{})
				h := newHarness(t, o, &fakeCore{})
				connect := credentialConnect(v)
				connect.Will = &packet.Will{Topic: topic, Payload: []byte("gone")}
				if ack := h.connect(v, connect); ack.ReasonCode != packet.Success {
					t.Fatalf("%+v, want Success", ack)
				}
			})
		}
	}
}

// The topic is judged before the Will's QoS against the server's maximum.
func TestWillTopicBeforeCapabilities(t *testing.T) {
	o, _, _, _ := rateOptions(&gate{})
	o.Capabilities.MaximumQoS = 1
	h := newHarness(t, o, &fakeCore{})
	connect := credentialConnect(packet.V5)
	connect.Will = &packet.Will{Topic: "a/#", QoS: 2, Payload: []byte("gone")}
	if ack := h.connect(packet.V5, connect); ack.ReasonCode != packet.TopicNameInvalid {
		t.Fatalf("%+v, want 0x90 ahead of QoS not supported", ack)
	}
}
