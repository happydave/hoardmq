package conformance

// The Paho interoperability scenarios, second half: user properties, payload
// format, publication expiry, subscribe options, assigned client identifier,
// subscription identifiers, request response, client and server topic
// aliases, maximum packet size, server keep alive, flow control, will delay,
// shared subscriptions.

import (
	"strings"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"

	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

func TestUserProperties(t *testing.T) {
	Statement(t, "MQTT-3.3.2-17", "MQTT-3.3.2-18")
	n := StartNode(t, nil)
	a, _ := n.Must5(Connect5{ClientID: "myclientid", CleanStart: true})
	a.Sub(topics[0], 2)
	props := &paho.PublishProperties{User: paho.UserProperties{{Key: "a", Value: "2"}, {Key: "c", Value: "3"}}}
	for qos := byte(0); qos <= 2; qos++ {
		a.Pub(topics[0], qos, false, "", props)
	}
	ms := a.Expect(3, 3*time.Second)
	for _, m := range ms {
		if m.Properties == nil || len(m.Properties.User) != 2 || m.Properties.User[0].Key != "a" || m.Properties.User[1].Key != "c" {
			t.Fatalf("user properties not forwarded in order: %+v", m.Properties)
		}
	}
	if q := qosSet(ms); !q[0] || !q[1] || !q[2] {
		t.Fatalf("qos set %v", q)
	}
	a.Disconnect(nil)
}

func TestPayloadFormat(t *testing.T) {
	Statement(t, "MQTT-3.3.2-4", "MQTT-3.3.2-20")
	n := StartNode(t, nil)
	a, _ := n.Must5(Connect5{ClientID: "myclientid", CleanStart: true})
	a.Sub(topics[0], 2)
	props := &paho.PublishProperties{PayloadFormat: u8(1), ContentType: "My name"}
	for qos := byte(0); qos <= 2; qos++ {
		a.Pub(topics[0], qos, false, "", props)
	}
	for _, m := range a.Expect(3, 3*time.Second) {
		if m.Properties == nil || m.Properties.ContentType != "My name" || m.Properties.PayloadFormat == nil || *m.Properties.PayloadFormat != 1 {
			t.Fatalf("payload format or content type not forwarded: %+v", m.Properties)
		}
	}
	a.Disconnect(nil)
}

func TestPublicationExpiry(t *testing.T) {
	Statement(t, "MQTT-3.3.2-5", "MQTT-3.3.2-6", "MQTT-3.1.2-23")
	n := StartNode(t, nil)
	b, _ := n.Must5(Connect5{ClientID: "myclientid2", CleanStart: true, SessionExpiry: u32(99999)})
	b.Sub(topics[0], 2)
	b.Disconnect(&paho.DisconnectProperties{SessionExpiryInterval: u32(999999999)})
	n.WaitOffline("myclientid2")
	a, _ := n.Must5(Connect5{ClientID: "myclientid", CleanStart: true})
	a.Pub(topics[0], 1, false, "qos 1 - expire", &paho.PublishProperties{MessageExpiry: u32(1)})
	a.Pub(topics[0], 2, false, "qos 2 - expire", &paho.PublishProperties{MessageExpiry: u32(1)})
	a.Pub(topics[0], 1, false, "qos 1 - don't expire", &paho.PublishProperties{MessageExpiry: u32(6)})
	a.Pub(topics[0], 2, false, "qos 2 - don't expire", &paho.PublishProperties{MessageExpiry: u32(6)})
	time.Sleep(2500 * time.Millisecond)
	b, _ = n.Must5(Connect5{ClientID: "myclientid2", CleanStart: false})
	ms := b.Expect(2, 3*time.Second)
	for _, m := range ms {
		if strings.Contains(string(m.Payload), "- expire") {
			t.Fatalf("expired message delivered: %q", m.Payload)
		}
		if m.Properties == nil || m.Properties.MessageExpiry == nil || *m.Properties.MessageExpiry >= 6 || *m.Properties.MessageExpiry == 0 {
			t.Fatalf("remaining expiry not decremented: %+v", m.Properties)
		}
	}
	b.ExpectNone(300 * time.Millisecond)
	a.Disconnect(nil)
	b.Disconnect(nil)
}

func TestSubscribeOptions(t *testing.T) {
	Statement(t, "MQTT-3.8.3-3", "MQTT-3.3.1-12", "MQTT-3.3.1-13", "MQTT-3.3.1-9", "MQTT-3.3.1-10", "MQTT-3.3.1-11", "MQTT-3.8.4-4")
	n := StartNode(t, nil)
	// No Local.
	a, _ := n.Must5(Connect5{ClientID: "myclientid", CleanStart: true})
	b, _ := n.Must5(Connect5{ClientID: "myclientid2", CleanStart: true})
	a.Subscribe(nil, paho.SubscribeOptions{Topic: topics[0], QoS: 2, NoLocal: true})
	b.Subscribe(nil, paho.SubscribeOptions{Topic: topics[0], QoS: 2, NoLocal: true})
	a.Pub(topics[0], 1, false, "noLocal test", nil)
	b.Expect(1, 3*time.Second)
	a.ExpectNone(500 * time.Millisecond)
	a.Disconnect(nil)
	b.Disconnect(nil)

	// Retain As Published.
	a, _ = n.Must5(Connect5{ClientID: "myclientid", CleanStart: true})
	a.Subscribe(nil, paho.SubscribeOptions{Topic: topics[0], QoS: 2, RetainAsPublished: true})
	a.Pub(topics[0], 1, false, "retain as published false", nil)
	a.Pub(topics[0], 1, true, "retain as published true", nil)
	ms := a.Expect(2, 3*time.Second)
	if ms[0].Retain || !ms[1].Retain {
		t.Fatalf("retain as published: %v %v", ms[0].Retain, ms[1].Retain)
	}
	a.Disconnect(nil)

	// Retain Handling 1: only for a new subscription.
	a, _ = n.Must5(Connect5{ClientID: "myclientid", CleanStart: true})
	a.Pub(topics[1], 0, true, "qos 0", nil)
	a.Pub(topics[2], 1, true, "qos 1", nil)
	a.Pub(topics[3], 2, true, "qos 2", nil)
	a.Subscribe(nil, paho.SubscribeOptions{Topic: wildtopics[5], QoS: 2, RetainHandling: 1})
	if q := qosSet(a.Expect(3, 3*time.Second)); !q[0] || !q[1] || !q[2] {
		t.Fatalf("qos set %v", q)
	}
	a.Subscribe(nil, paho.SubscribeOptions{Topic: wildtopics[5], QoS: 2, RetainHandling: 1})
	a.ExpectNone(500 * time.Millisecond)
	a.Disconnect(nil)
	// Retain Handling 2: never.
	a, _ = n.Must5(Connect5{ClientID: "myclientid", CleanStart: true})
	a.Subscribe(nil, paho.SubscribeOptions{Topic: wildtopics[5], QoS: 2, RetainHandling: 2})
	a.ExpectNone(300 * time.Millisecond)
	a.Subscribe(nil, paho.SubscribeOptions{Topic: wildtopics[5], QoS: 2, RetainHandling: 2})
	a.ExpectNone(300 * time.Millisecond)
	a.Disconnect(nil)
	// Retain Handling 0: every time.
	a, _ = n.Must5(Connect5{ClientID: "myclientid", CleanStart: true})
	a.Subscribe(nil, paho.SubscribeOptions{Topic: wildtopics[5], QoS: 2, RetainHandling: 0})
	a.Expect(3, 3*time.Second)
	a.Subscribe(nil, paho.SubscribeOptions{Topic: wildtopics[5], QoS: 2, RetainHandling: 0})
	a.Expect(3, 3*time.Second)
	// Clean the retained messages.
	for _, tp := range topics[1:4] {
		a.Pub(tp, 0, true, "", nil)
	}
	a.Disconnect(nil)
	_ = config.Default
}

func TestAssignedClientID(t *testing.T) {
	Statement(t, "MQTT-3.1.3-6", "MQTT-3.1.3-7", "MQTT-3.2.2-16")
	n := StartNode(t, nil)
	c, ack, err := n.Dial5(Connect5{ClientID: "", CleanStart: true})
	if err != nil || ack.Properties == nil || ack.Properties.AssignedClientID == "" {
		t.Fatalf("assigned client identifier: %v %+v", err, ack)
	}
	c.Disconnect(nil)
}

func TestSubscriptionIdentifiers(t *testing.T) {
	Statement(t, "MQTT-3.3.4-3", "MQTT-3.3.4-4", "MQTT-3.3.4-5", "MQTT-3.8.4-6")
	n := StartNode(t, nil)
	a, _ := n.Must5(Connect5{ClientID: "myclientid", CleanStart: true})
	a.Subscribe(&paho.SubscribeProperties{SubscriptionIdentifier: intp(456789)}, paho.SubscribeOptions{Topic: topics[0], QoS: 2})
	// b holds two overlapping subscriptions with identifiers 2 and 3; the
	// raw client sees both identifiers on the one delivery.
	r := n.DialRaw(packet.V5)
	r.Connect(&packet.Connect{ClientID: "myclientid2", CleanStart: true})
	r.Send(&packet.Subscribe{PacketID: 1, Properties: packet.Properties{SubscriptionIdentifiers: []uint32{2}}, Subscriptions: []packet.Subscription{{Filter: topics[0], QoS: 2}}})
	r.Read()
	r.Send(&packet.Subscribe{PacketID: 2, Properties: packet.Properties{SubscriptionIdentifiers: []uint32{3}}, Subscriptions: []packet.Subscription{{Filter: topics[0] + "/#", QoS: 2}}})
	r.Read()
	r.Send(&packet.Publish{Topic: topics[0], QoS: 1, PacketID: 3, Payload: []byte("sub identifier test")})
	var got *packet.Publish
	for got == nil {
		switch p := r.Read().(type) {
		case *packet.Publish:
			got = p
		case *packet.Puback:
		default:
			t.Fatalf("unexpected %T", p)
		}
	}
	ids := got.Properties.SubscriptionIdentifiers
	both := len(ids) == 2 && ((ids[0] == 2 && ids[1] == 3) || (ids[0] == 3 && ids[1] == 2))
	if !both {
		t.Fatalf("subscription identifiers %v", ids)
	}
	m := a.Expect(1, 3*time.Second)[0]
	if m.Properties == nil || m.Properties.SubscriptionIdentifier == nil || *m.Properties.SubscriptionIdentifier != 456789 {
		t.Fatalf("identifier %+v", m.Properties)
	}
	a.Disconnect(nil)
}

func TestRequestResponse(t *testing.T) {
	Statement(t, "MQTT-3.3.2-13", "MQTT-3.3.2-15", "MQTT-3.3.2-16")
	n := StartNode(t, nil)
	a, _ := n.Must5(Connect5{ClientID: "myclientid", CleanStart: true})
	b, _ := n.Must5(Connect5{ClientID: "myclientid2", CleanStart: true})
	a.Subscribe(nil, paho.SubscribeOptions{Topic: topics[0], QoS: 2, NoLocal: true})
	b.Subscribe(nil, paho.SubscribeOptions{Topic: topics[0], QoS: 2, NoLocal: true})
	a.Pub(topics[0], 1, false, "request", &paho.PublishProperties{ResponseTopic: topics[0], CorrelationData: []byte("334")})
	req := b.Expect(1, 3*time.Second)[0]
	if req.Properties == nil || req.Properties.ResponseTopic != topics[0] || string(req.Properties.CorrelationData) != "334" {
		t.Fatalf("request properties %+v", req.Properties)
	}
	b.Pub(req.Properties.ResponseTopic, 1, false, "response", &paho.PublishProperties{CorrelationData: req.Properties.CorrelationData})
	resp := a.Expect(1, 3*time.Second)[0]
	if string(resp.Payload) != "response" || string(resp.Properties.CorrelationData) != "334" {
		t.Fatalf("response %+v", resp)
	}
	a.Disconnect(nil)
	b.Disconnect(nil)
}

func TestClientTopicAlias(t *testing.T) {
	Statement(t, "MQTT-3.3.2-8", "MQTT-3.3.2-9", "MQTT-3.3.2-10", "MQTT-3.3.2-11", "MQTT-3.3.2-12", "MQTT-3.2.2-17")
	n := StartNode(t, nil)
	// Topic alias 0 is a protocol error answered with 0x94.
	r := n.DialRaw(packet.V5)
	r.Connect(&packet.Connect{ClientID: "myclientid", CleanStart: true})
	r.Send(&packet.Publish{Topic: topics[0], QoS: 1, PacketID: 1, Payload: []byte("topic alias 0"), Properties: packet.Properties{TopicAlias: u16(0)}})
	if d := r.ReadEnd(); d == nil || d.ReasonCode != packet.TopicAliasInvalid {
		t.Fatalf("alias 0: %+v", d)
	}
	// Aliases set and used, then gone after the connection ends.
	r = n.DialRaw(packet.V5)
	ack := r.Connect(&packet.Connect{ClientID: "myclientid", CleanStart: true, Properties: packet.Properties{TopicAliasMaximum: u16(0), SessionExpiryInterval: u32(99999)}})
	if ack.Properties.TopicAliasMaximum == nil || *ack.Properties.TopicAliasMaximum == 0 {
		t.Fatal("server does not accept topic aliases")
	}
	r.Send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: topics[0], QoS: 2}}})
	r.Read()
	r.Send(&packet.Publish{Topic: topics[0], QoS: 1, PacketID: 2, Payload: []byte("topic alias 1"), Properties: packet.Properties{TopicAlias: u16(1)}})
	r.Send(&packet.Publish{Topic: "", QoS: 1, PacketID: 3, Payload: []byte("topic alias 2"), Properties: packet.Properties{TopicAlias: u16(1)}})
	got := 0
	for got < 2 {
		switch p := r.Read().(type) {
		case *packet.Publish:
			if p.Topic != topics[0] {
				t.Fatalf("delivered topic %q", p.Topic)
			}
			got++
			r.Send(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID}})
		case *packet.Puback:
		}
	}
	r.Send(&packet.Disconnect{})
	n.WaitOffline("myclientid")
	r = n.DialRaw(packet.V5)
	if ack := r.Connect(&packet.Connect{ClientID: "myclientid", CleanStart: false}); !ack.SessionPresent {
		t.Fatal("session not present")
	}
	r.Send(&packet.Publish{Topic: topics[0], QoS: 1, PacketID: 4, Payload: []byte("topic alias 3")})
	got = 0
	for got < 1 {
		switch p := r.Read().(type) {
		case *packet.Publish:
			got++
			r.Send(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID}})
		case *packet.Puback:
		}
	}
	// The alias did not survive the connection.
	r.Send(&packet.Publish{Topic: "", QoS: 1, PacketID: 5, Payload: []byte("topic alias 4"), Properties: packet.Properties{TopicAlias: u16(1)}})
	for {
		_ = r.Conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		p, err := r.Dec.Read()
		if err != nil {
			t.Fatalf("expected DISCONNECT, connection ended: %v", err)
		}
		if d, ok := p.(*packet.Disconnect); ok {
			if d.ReasonCode != packet.ProtocolError {
				t.Fatalf("unknown alias: %s", d.ReasonCode)
			}
			break
		}
	}
}

func TestServerTopicAlias(t *testing.T) {
	Statement(t, "MQTT-3.3.2-8", "MQTT-3.3.2-12", "MQTT-3.1.2-26", "MQTT-3.1.2-27")
	n := StartNode(t, nil)
	run := func(max *uint16, wantAlias bool) {
		r := n.DialRaw(packet.V5)
		r.Connect(&packet.Connect{ClientID: "myclientid", CleanStart: true, Properties: packet.Properties{TopicAliasMaximum: max}})
		r.Send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: topics[0], QoS: 2}}})
		r.Read()
		for qos := byte(0); qos <= 2; qos++ {
			r.Send(&packet.Publish{Topic: topics[0], QoS: qos, PacketID: uint16(10 + qos), Payload: []byte("topic alias 1")})
		}
		var pubs []*packet.Publish
		for len(pubs) < 3 {
			switch p := r.Read().(type) {
			case *packet.Publish:
				pubs = append(pubs, p)
				switch p.QoS {
				case 1:
					r.Send(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID}})
				case 2:
					r.Send(&packet.Pubrec{Ack: packet.Ack{PacketID: p.PacketID}})
				}
			case *packet.Pubrec:
				r.Send(&packet.Pubrel{Ack: packet.Ack{PacketID: p.PacketID}})
			case *packet.Pubrel:
				r.Send(&packet.Pubcomp{Ack: packet.Ack{PacketID: p.PacketID}})
			}
		}
		if wantAlias {
			if pubs[0].Properties.TopicAlias == nil || *pubs[0].Properties.TopicAlias == 0 || pubs[0].Topic != topics[0] {
				t.Fatalf("first publish must carry the alias and the topic: %+v", pubs[0])
			}
			for _, p := range pubs[1:] {
				if p.Properties.TopicAlias == nil || *p.Properties.TopicAlias != *pubs[0].Properties.TopicAlias || p.Topic != "" {
					t.Fatalf("later publish must use the alias with an empty topic: %+v", p)
				}
			}
		} else {
			for _, p := range pubs {
				if p.Properties.TopicAlias != nil {
					t.Fatalf("alias used although the client allows none: %+v", p)
				}
			}
		}
		r.Send(&packet.Disconnect{})
	}
	run(u16(1), true)
	run(nil, false)
	run(u16(0), false)
}

func TestMaximumPacketSize(t *testing.T) {
	Statement(t, "MQTT-3.1.2-24", "MQTT-3.1.2-25", "MQTT-3.2.2-15", "MQTT-3.3.2-19")
	// Server maximum: a small limit so an oversize publish is refused.
	n := StartNode(t, func(d *config.Document) { d.Limits.MaxPacketSize = 200 })
	r := n.DialRaw(packet.V5)
	ack := r.Connect(&packet.Connect{ClientID: "myclientid", CleanStart: true})
	if ack.Properties.MaximumPacketSize == nil || *ack.Properties.MaximumPacketSize != 200 {
		t.Fatalf("server maximum not advertised: %+v", ack.Properties)
	}
	go func() {
		_ = r.Enc.Encode(r.Conn, &packet.Publish{Topic: topics[0], Payload: make([]byte, 300)})
	}()
	if d := r.ReadEnd(); d == nil || d.ReasonCode != packet.PacketTooLarge {
		t.Fatalf("oversize publish: %+v", d)
	}
	// Client maximum: the server drops what does not fit.
	a, _ := n.Must5(Connect5{ClientID: "myclientid", CleanStart: true, MaxPacketSize: u32(64)})
	a.Sub(topics[0], 2)
	a.Pub(topics[0], 0, false, strings.Repeat(".", 32), nil)
	a.Expect(1, 3*time.Second)
	a.Pub(topics[0], 1, false, strings.Repeat(".", 64), nil)
	a.ExpectNone(500 * time.Millisecond)
	a.Disconnect(nil)
}

func TestServerKeepAlive(t *testing.T) {
	Statement(t, "MQTT-3.1.2-21", "MQTT-3.2.2-21", "MQTT-3.2.2-22")
	n := StartNode(t, func(d *config.Document) { d.Limits.MaxKeepAlive = 60 })
	a, ack := n.Must5(Connect5{ClientID: "myclientid", CleanStart: true, KeepAlive: 120})
	if ack.Properties == nil || ack.Properties.ServerKeepAlive == nil || *ack.Properties.ServerKeepAlive != 60 {
		t.Fatalf("server keep alive: %+v", ack.Properties)
	}
	a.Disconnect(nil)
}

func TestFlowControl1(t *testing.T) {
	Statement(t, "MQTT-4.9.0-1", "MQTT-4.9.0-2", "MQTT-3.3.4-9")
	n := StartNode(t, nil)
	// The client's Receive Maximum is 2 and it does not acknowledge.
	c, ack := n.Must5(Connect5{ClientID: "myclientid", CleanStart: true, ReceiveMax: u16(2), SessionExpiry: u32(0), ManualAck: true})
	if ack.Properties == nil || ack.Properties.ReceiveMaximum == nil {
		t.Fatal("server receive maximum not advertised")
	}
	c.Sub(topics[0], 2)
	for i := 1; i <= 3; i++ {
		c.Pub(topics[0], 1, false, "message", nil)
	}
	first := c.Expect(2, 3*time.Second)
	c.ExpectNone(500 * time.Millisecond)
	c.Ack(first[0])
	c.Expect(1, 3*time.Second)
	c.Disconnect(nil)
}

func TestFlowControl2(t *testing.T) {
	Statement(t, "MQTT-3.3.4-7", "MQTT-3.3.4-8")
	n := StartNode(t, func(d *config.Document) { d.Limits.ReceiveMaximum = 5 })
	r := n.DialRaw(packet.V5)
	ack := r.Connect(&packet.Connect{ClientID: "myclientid", CleanStart: true})
	if ack.Properties.ReceiveMaximum == nil || *ack.Properties.ReceiveMaximum != 5 {
		t.Fatalf("receive maximum %+v", ack.Properties)
	}
	// Six unacknowledged QoS 2 publishes exceed a Receive Maximum of 5.
	for i := 1; i <= 6; i++ {
		r.Send(&packet.Publish{Topic: topics[0], QoS: 2, PacketID: uint16(i), Payload: []byte("message")})
	}
	for {
		_ = r.Conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		p, err := r.Dec.Read()
		if err != nil {
			t.Fatalf("expected DISCONNECT 0x93, connection ended: %v", err)
		}
		if d, ok := p.(*packet.Disconnect); ok {
			if d.ReasonCode != packet.ReceiveMaximumExceeded {
				t.Fatalf("reason %s", d.ReasonCode)
			}
			return
		}
	}
}

func TestWillDelay(t *testing.T) {
	Statement(t, "MQTT-3.1.3-9", "MQTT-3.1.2-8", "MQTT-3.1.2-9", "MQTT-3.1.2-10")
	n := StartNode(t, nil)
	run := func(delay, expiry uint32, want time.Duration, tolerance time.Duration) {
		t.Helper()
		a, ack := n.Must5(Connect5{ClientID: "myclientid", CleanStart: true, SessionExpiry: u32(expiry),
			Will: &paho.WillMessage{Topic: topics[0], Payload: []byte("test_will_delay will message")}, WillProps: &paho.WillProperties{WillDelayInterval: u32(delay)}})
		if ack.SessionPresent {
			t.Fatal("session present")
		}
		b, _ := n.Must5(Connect5{ClientID: "myclientid2", CleanStart: true})
		b.Sub(topics[0], 2)
		a.Terminate()
		start := time.Now()
		ms := b.Expect(1, want+tolerance+2*time.Second)
		if d := time.Since(start); d < want-tolerance || d > want+tolerance {
			t.Fatalf("will after %v, want about %v", d, want)
		}
		if string(ms[0].Payload) != "test_will_delay will message" {
			t.Fatalf("%q", ms[0].Payload)
		}
		b.Disconnect(nil)
	}
	run(3, 5, 3*time.Second, 1500*time.Millisecond) // delay before expiry
	run(5, 0, 0, 1200*time.Millisecond)             // expiry 0: at once
	run(5, 2, 2*time.Second, 1500*time.Millisecond) // expiry before delay
}

func TestSharedSubscriptions(t *testing.T) {
	Statement(t, "MQTT-4.8.2-1", "MQTT-4.8.2-3", "MQTT-4.8.2-4", "MQTT-4.8.2-6")
	n := StartNode(t, nil)
	shared := "$share/sharename/TopicA/x"
	pubTopic := "TopicA/x"
	a, _ := n.Must5(Connect5{ClientID: "myclientid", CleanStart: true})
	if ack := a.Subscribe(nil, paho.SubscribeOptions{Topic: shared, QoS: 2}, paho.SubscribeOptions{Topic: topics[0], QoS: 2}); ack.Reasons[0] != 2 || ack.Reasons[1] != 2 {
		t.Fatalf("suback %v", ack.Reasons)
	}
	b, _ := n.Must5(Connect5{ClientID: "myclientid2", CleanStart: true})
	if ack := b.Subscribe(nil, paho.SubscribeOptions{Topic: shared, QoS: 2}, paho.SubscribeOptions{Topic: topics[0], QoS: 2}); ack.Reasons[0] != 2 {
		t.Fatalf("suback %v", ack.Reasons)
	}
	// A plain topic reaches both.
	b.Pub(topics[0], 0, false, "message 0", nil)
	a.Expect(1, 3*time.Second)
	b.Expect(1, 3*time.Second)
	// The shared topic reaches exactly one member per message.
	for i := 0; i < 4; i++ {
		b.Pub(pubTopic, 0, false, "shared", nil)
	}
	total := 0
	deadline := time.After(3 * time.Second)
	for total < 4 {
		select {
		case <-a.Messages:
			total++
		case <-b.Messages:
			total++
		case <-deadline:
			t.Fatalf("got %d of 4 shared deliveries", total)
		}
	}
	a.ExpectNone(300 * time.Millisecond)
	b.ExpectNone(300 * time.Millisecond)
	a.Disconnect(nil)
	b.Disconnect(nil)
}
