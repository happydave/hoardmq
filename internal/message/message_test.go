package message

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/otel"
	"github.com/happydave/hoardmq/internal/packet"
)

func TestEnvelopeRoundTrip(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	exp := uint32(30)
	pf := byte(1)
	ct := "text/plain"
	in := &packet.Publish{Topic: "a/b", QoS: 2, Retain: true, PacketID: 9, Payload: []byte("x"),
		Properties: packet.Properties{MessageExpiryInterval: &exp, PayloadFormatIndicator: &pf, ContentType: &ct, CorrelationData: []byte{1}, UserProperties: []packet.UserProperty{{Name: "k", Value: "v"}}}}
	m := FromPublish(in, Publisher{Namespace: "default", ClientID: "c"}, 7, now)
	if m.Topic != "a/b" || m.QoS != 2 || !m.Retain || m.Seq != 7 || m.Expiry != now.Add(30*time.Second) || m.Namespace != "default" {
		t.Fatalf("%+v", m)
	}
	if m.Expired(now.Add(29*time.Second)) || !m.Expired(now.Add(30*time.Second)) {
		t.Error("Expired")
	}
	out := m.ToPublish(Delivery{QoS: 1, PacketID: 3, Dup: true, SubscriptionIdentifiers: []uint32{5}}, now.Add(10*time.Second+500*time.Millisecond))
	if out.QoS != 1 || out.PacketID != 3 || !out.Dup || out.Retain || out.Topic != "a/b" || *out.Properties.MessageExpiryInterval != 20 || *out.Properties.ContentType != "text/plain" || out.Properties.SubscriptionIdentifiers[0] != 5 {
		t.Fatalf("%+v %d", out, *out.Properties.MessageExpiryInterval)
	}
	// A message with under a second left still reports 1, never 0.
	out = m.ToPublish(Delivery{}, now.Add(29*time.Second+900*time.Millisecond))
	if *out.Properties.MessageExpiryInterval != 1 {
		t.Errorf("expiry %d", *out.Properties.MessageExpiryInterval)
	}
	plain := FromPublish(&packet.Publish{Topic: "t"}, Publisher{}, 1, now)
	if !plain.Expiry.IsZero() || plain.Expired(now.Add(time.Hour)) || plain.ToPublish(Delivery{}, now).Properties.MessageExpiryInterval != nil {
		t.Error("message without expiry")
	}
}

// A deadline already passed must not wrap the unsigned property into a
// centuries-long interval.
func TestExpiryIntervalNeverWraps(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	exp := uint32(10)
	m := FromPublish(&packet.Publish{Topic: "t", Properties: packet.Properties{MessageExpiryInterval: &exp}},
		Publisher{}, 1, now)
	for _, late := range []time.Duration{10 * time.Second, 30 * time.Second, time.Hour} {
		out := m.ToPublish(Delivery{}, now.Add(late))
		if got := *out.Properties.MessageExpiryInterval; got != 1 {
			t.Errorf("%v past the deadline: MessageExpiryInterval %d, want 1", late-10*time.Second, got)
		}
	}
}

// TestTraceIsNotEncoded: a message's span context stays in memory. The
// journal, the replicas and the forward's msg member encode the message as
// JSON, and a traced message must encode as an untraced one does.
func TestTraceIsNotEncoded(t *testing.T) {
	plain := &Message{Namespace: "default", Topic: "t", Payload: []byte("x"), QoS: 1}
	traced := *plain
	traced.Trace = otel.SpanContext{TraceID: [16]byte{1}, SpanID: [8]byte{2}, Flags: 1, State: "k=v"}
	a, err := json.Marshal(plain)
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(&traced)
	if err != nil {
		t.Fatal(err)
	}
	if string(a) != string(b) {
		t.Fatalf("a traced message encodes as %s, an untraced one as %s", b, a)
	}
}
