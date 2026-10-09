package conformance

// Capability declines: a node configured to decline the MQTT 5.0 optional
// features advertises that in CONNACK and refuses the corresponding traffic
// with the specified reason codes.

import (
	"fmt"
	"testing"

	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

func TestCapabilityDeclines(t *testing.T) {
	Statement(t, "MQTT-3.2.2-9", "MQTT-3.2.2-10", "MQTT-3.2.2-12", "MQTT-3.2.2-13", "MQTT-3.8.4-8")
	n := StartNode(t, func(d *config.Document) {
		d.MQTT.MaximumQoS = 1
		d.MQTT.RetainAvailable = false
		d.MQTT.WildcardSubscriptionAvailable = false
		d.MQTT.SubscriptionIdentifiersAvailable = false
		d.MQTT.SharedSubscriptionAvailable = false
	})
	off := func(name string, v any) {
		s := fmt.Sprint(v)
		if s != "0" && s != "false" {
			t.Errorf("%s advertised as %s", name, s)
		}
	}
	r := n.DialRaw(packet.V5)
	ack := r.Connect(&packet.Connect{ClientID: "caps", CleanStart: true})
	p := ack.Properties
	if p.MaximumQoS == nil || *p.MaximumQoS != 1 {
		t.Fatalf("Maximum QoS not advertised: %+v", p.MaximumQoS)
	}
	if p.RetainAvailable == nil || p.WildcardSubscriptionAvailable == nil || p.SubscriptionIdentifierAvailable == nil || p.SharedSubscriptionAvailable == nil {
		t.Fatal("a declined capability was not advertised")
	}
	off("Retain Available", *p.RetainAvailable)
	off("Wildcard Subscription Available", *p.WildcardSubscriptionAvailable)
	off("Subscription Identifier Available", *p.SubscriptionIdentifierAvailable)
	off("Shared Subscription Available", *p.SharedSubscriptionAvailable)

	// SUBSCRIBE with Requested QoS 2 is accepted at the maximum; the message
	// is delivered at the minimum of published and granted QoS.
	r.Send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "caps/a", QoS: 2}}})
	sa, ok := r.Read().(*packet.Suback)
	if !ok || len(sa.ReasonCodes) != 1 || sa.ReasonCodes[0] != packet.ReasonCode(1) {
		t.Fatalf("suback %+v", sa)
	}
	pub := n.DialRaw(packet.V5)
	pub.Connect(&packet.Connect{ClientID: "caps-pub", CleanStart: true})
	pub.Send(&packet.Publish{Topic: "caps/a", QoS: 1, PacketID: 7, Payload: []byte("x")})
	pub.Read()
	if m, ok := r.Read().(*packet.Publish); !ok || m.QoS != 1 {
		t.Fatalf("delivery %+v", m)
	}
	// Wildcard and shared filters are refused per filter in SUBACK; a
	// Subscription Identifier ends the connection.
	r.Send(&packet.Subscribe{PacketID: 2, Subscriptions: []packet.Subscription{{Filter: "caps/#"}, {Filter: "$share/g/caps/b"}, {Filter: "caps/c"}}})
	sa, ok = r.Read().(*packet.Suback)
	if !ok || len(sa.ReasonCodes) != 3 || sa.ReasonCodes[0] != packet.WildcardSubscriptionsNotSupported || sa.ReasonCodes[1] != packet.SharedSubscriptionsNotSupported || sa.ReasonCodes[2] != packet.Success {
		t.Fatalf("suback %+v", sa)
	}
	r.Send(&packet.Subscribe{PacketID: 3, Properties: packet.Properties{SubscriptionIdentifiers: []uint32{5}}, Subscriptions: []packet.Subscription{{Filter: "caps/d"}}})
	if d := r.ReadEnd(); d == nil || d.ReasonCode != packet.SubscriptionIdentifiersNotSupported {
		t.Fatalf("subscription identifier: %+v", d)
	}
	// RETAIN and QoS 2 publishes end the connection with the matching code.
	pub.Send(&packet.Publish{Topic: "caps/a", Retain: true, Payload: []byte("x")})
	if d := pub.ReadEnd(); d == nil || d.ReasonCode != packet.RetainNotSupported {
		t.Fatalf("retain: %+v", d)
	}
	q := n.DialRaw(packet.V5)
	q.Connect(&packet.Connect{ClientID: "caps-q", CleanStart: true})
	q.Send(&packet.Publish{Topic: "caps/a", QoS: 2, PacketID: 1, Payload: []byte("x")})
	if d := q.ReadEnd(); d == nil || d.ReasonCode != packet.QoSNotSupported {
		t.Fatalf("qos 2: %+v", d)
	}
	// A Will beyond the capabilities is refused at CONNECT.
	w := n.DialRaw(packet.V5)
	if ack := w.Connect(&packet.Connect{ClientID: "caps-w", CleanStart: true, Will: &packet.Will{Topic: "caps/w", Retain: true}}); ack.ReasonCode != packet.RetainNotSupported {
		t.Fatalf("will retain: %s", ack.ReasonCode)
	}
	w = n.DialRaw(packet.V5)
	if ack := w.Connect(&packet.Connect{ClientID: "caps-w", CleanStart: true, Will: &packet.Will{Topic: "caps/w", QoS: 2}}); ack.ReasonCode != packet.QoSNotSupported {
		t.Fatalf("will qos: %s", ack.ReasonCode)
	}
}
