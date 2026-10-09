package conformance

// The Paho interoperability scenarios (client_test5.py) ported to Go, first
// half: basic, retained, will, zero-length client identifier, offline
// queueing, overlapping subscriptions, keepalive, redelivery on reconnect,
// subscribe failure, dollar topics, unsubscribe, session expiry.

import (
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"

	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

var (
	topics     = []string{"TopicA", "TopicA/B", "Topic/C", "TopicA/C", "/TopicA"}
	wildtopics = []string{"TopicA/+", "+/C", "#", "/#", "/+", "+/+", "TopicA/#"}
)

func qosSet(ms []*paho.Publish) map[byte]bool {
	out := map[byte]bool{}
	for _, m := range ms {
		out[m.QoS] = true
	}
	return out
}

func TestBasic(t *testing.T) {
	Statement(t, "MQTT-3.1.4-1", "MQTT-3.1.4-5", "MQTT-3.2.0-1", "MQTT-3.3.4-1", "MQTT-3.12.4-1", "MQTT-3.14.4-1", "MQTT-3.1.0-2", "MQTT-3.1.2-1", "MQTT-3.2.2-8")
	n := StartNode(t, nil)
	a, _ := n.Must5(Connect5{ClientID: "myclientid", CleanStart: true})
	a.Disconnect(nil)
	a, ack := n.Must5(Connect5{ClientID: "myclientid", CleanStart: true})
	if ack.ReasonCode != 0 {
		t.Fatalf("connack %d", ack.ReasonCode)
	}
	a.Sub(topics[0], 2)
	a.Pub(topics[0], 0, false, "qos 0", nil)
	a.Pub(topics[0], 1, false, "qos 1", nil)
	a.Pub(topics[0], 2, false, "qos 2", nil)
	ms := a.Expect(3, 3*time.Second)
	if q := qosSet(ms); !q[0] || !q[1] || !q[2] {
		t.Fatalf("qos set %v", q)
	}
	a.Disconnect(nil)

	// A second CONNECT on an open connection is a protocol error.
	r := n.DialRaw(packet.V5)
	r.Connect(&packet.Connect{ClientID: "twice", CleanStart: true})
	r.Send(&packet.Connect{ProtocolLevel: packet.V5, ClientID: "twice", CleanStart: true})
	if d := r.ReadEnd(); d == nil || d.ReasonCode != packet.ProtocolError {
		t.Fatalf("second CONNECT: %+v", d)
	}
	// A wrong protocol name is refused in the client's framing.
	r2 := n.DialRaw(packet.V5)
	raw := []byte{0x10, 0x0B, 0x00, 0x02, 'h', 'j', 0x05, 0x02, 0x00, 0x0A, 0x00, 0x00, 0x00}
	r2.SendBytes(raw)
	if ack, ok := r2.Read().(*packet.Connack); !ok || ack.ReasonCode != packet.UnsupportedProtocolVersion {
		t.Fatalf("wrong protocol name: %+v", ack)
	}
}

func TestRetainedMessage(t *testing.T) {
	Statement(t, "MQTT-3.3.1-5", "MQTT-3.3.1-6", "MQTT-3.3.1-8", "MQTT-3.3.1-9", "MQTT-3.3.2-17", "MQTT-3.3.2-18", "MQTT-3.8.4-4")
	n := StartNode(t, nil)
	props := &paho.PublishProperties{User: paho.UserProperties{{Key: "a", Value: "2"}, {Key: "c", Value: "3"}}}
	a, _ := n.Must5(Connect5{ClientID: "myclientid", CleanStart: true})
	a.Pub(topics[1], 0, true, "qos 0", props)
	a.Pub(topics[2], 1, true, "qos 1", props)
	a.Pub(topics[3], 2, true, "qos 2", props)
	a.Sub(wildtopics[5], 2)
	ms := a.Expect(3, 3*time.Second)
	for _, m := range ms {
		if !m.Retain {
			t.Errorf("retained message delivered without RETAIN: %+v", m)
		}
		if m.Properties == nil || m.Properties.User.Get("a") != "2" || m.Properties.User.Get("c") != "3" {
			t.Errorf("user properties lost: %+v", m.Properties)
		}
	}
	if q := qosSet(ms); !q[0] || !q[1] || !q[2] {
		t.Fatalf("qos set %v", q)
	}
	// An empty retained payload clears the entry. QoS 1 so the PUBACKs
	// order the clears before the next subscription.
	a.Pub(topics[1], 1, true, "", nil)
	a.Pub(topics[2], 1, true, "", nil)
	a.Pub(topics[3], 1, true, "", nil)
	a.Drain()
	a.Disconnect(nil)
	b, _ := n.Must5(Connect5{ClientID: "myclientid2", CleanStart: true})
	b.Sub(wildtopics[5], 2)
	b.ExpectNone(500 * time.Millisecond)
}

func TestWillMessage(t *testing.T) {
	Statement(t, "MQTT-3.1.2-8", "MQTT-3.1.2-9", "MQTT-3.1.2-10", "MQTT-3.1.2-22", "MQTT-3.1.3-11")
	n := StartNode(t, nil)
	delay := uint32(0)
	// The Will publisher pings never: a raw connection with keep alive 1.
	r := n.DialRaw(packet.V5)
	ack := r.Connect(&packet.Connect{ClientID: "willer", CleanStart: true, KeepAlive: 1,
		Will: &packet.Will{Topic: topics[2], Payload: []byte("will message"), Properties: packet.Properties{WillDelayInterval: &delay, UserProperties: []packet.UserProperty{{Name: "a", Value: "2"}, {Name: "c", Value: "3"}}}}})
	if ack.ReasonCode != packet.Success {
		t.Fatal(ack.ReasonCode)
	}
	b, _ := n.Must5(Connect5{ClientID: "myclientid2", CleanStart: false})
	b.Sub(topics[2], 2)
	// Keep alive 1 s means the server closes after 1.5 s and publishes the Will.
	ms := b.Expect(1, 4*time.Second)
	if string(ms[0].Payload) != "will message" || ms[0].Properties == nil || ms[0].Properties.User.Get("a") != "2" {
		t.Fatalf("will %+v", ms[0])
	}
	b.Disconnect(nil)
}

func TestZeroLengthClientID(t *testing.T) {
	Statement(t, "MQTT-3.1.3-6", "MQTT-3.1.3-7", "MQTT-3.1.3-5")
	n := StartNode(t, nil)
	c, ack, err := n.Dial5(Connect5{ClientID: "", CleanStart: false})
	if err != nil {
		t.Fatalf("zero-length client id with Clean Start 0 refused: %v", err)
	}
	if ack.Properties == nil || ack.Properties.AssignedClientID == "" {
		t.Fatal("no Assigned Client Identifier")
	}
	c.Disconnect(nil)
	c, ack, err = n.Dial5(Connect5{ClientID: "", CleanStart: true})
	if err != nil || ack.Properties.AssignedClientID == "" {
		t.Fatalf("zero-length client id with Clean Start 1: %v", err)
	}
	c.Disconnect(nil)
}

func TestOfflineMessageQueueing(t *testing.T) {
	Statement(t, "MQTT-3.1.2-5", "MQTT-3.1.2-23", "MQTT-3.2.2-2", "MQTT-4.4.0-1", "MQTT-3.3.1-3")
	n := StartNode(t, nil)
	a, _ := n.Must5(Connect5{ClientID: "myclientid", CleanStart: true, SessionExpiry: u32(99999)})
	a.Sub(wildtopics[5], 2)
	a.Disconnect(nil)
	n.WaitOffline("myclientid")
	b, _ := n.Must5(Connect5{ClientID: "myclientid2", CleanStart: true})
	b.Pub(topics[1], 0, false, "qos 0", nil)
	b.Pub(topics[2], 1, false, "qos 1", nil)
	b.Pub(topics[3], 2, false, "qos 2", nil)
	b.Disconnect(nil)
	a, ack := n.Must5(Connect5{ClientID: "myclientid", CleanStart: false, SessionExpiry: u32(99999)})
	if !ack.SessionPresent {
		t.Fatal("session not present")
	}
	// HoardMQ queues QoS 0 for offline sessions too.
	ms := a.Expect(3, 3*time.Second)
	if q := qosSet(ms); !q[0] || !q[1] || !q[2] {
		t.Fatalf("qos set %v", q)
	}
	a.Disconnect(nil)
}

func TestOverlappingSubscriptions(t *testing.T) {
	Statement(t, "MQTT-3.3.4-2", "MQTT-3.8.4-3", "MQTT-3.8.4-6")
	n := StartNode(t, nil)
	a, _ := n.Must5(Connect5{ClientID: "myclientid", CleanStart: true})
	a.Subscribe(nil, paho.SubscribeOptions{Topic: wildtopics[6], QoS: 2}, paho.SubscribeOptions{Topic: wildtopics[0], QoS: 1})
	a.Pub(topics[3], 2, false, "overlapping topic filters", nil)
	ms := a.Expect(1, 3*time.Second)
	if ms[0].QoS != 2 {
		t.Fatalf("one delivery at the highest QoS expected, got QoS %d", ms[0].QoS)
	}
	a.ExpectNone(500 * time.Millisecond)
	a.Disconnect(nil)
}

func TestKeepalive(t *testing.T) {
	Statement(t, "MQTT-3.1.2-22", "MQTT-3.1.2-8")
	n := StartNode(t, nil)
	r := n.DialRaw(packet.V5)
	r.Connect(&packet.Connect{ClientID: "myclientid", CleanStart: true, KeepAlive: 1, Will: &packet.Will{Topic: topics[4], Payload: []byte("keepalive expiry")}})
	b, _ := n.Must5(Connect5{ClientID: "myclientid2", CleanStart: true, KeepAlive: 0})
	b.Sub(topics[4], 2)
	start := time.Now()
	ms := b.Expect(1, 5*time.Second)
	if string(ms[0].Payload) != "keepalive expiry" {
		t.Fatalf("%q", ms[0].Payload)
	}
	if el := time.Since(start); el < 1200*time.Millisecond {
		t.Fatalf("closed too early: %v", el)
	}
	// The idle raw connection was closed with 0x8D.
	if d := r.ReadEnd(); d == nil || d.ReasonCode != packet.KeepAliveTimeout {
		t.Fatalf("keep alive close: %+v", d)
	}
	b.Disconnect(nil)
}

func TestRedeliveryOnReconnect(t *testing.T) {
	Statement(t, "MQTT-4.4.0-1", "MQTT-3.3.1-1", "MQTT-3.1.2-5", "MQTT-4.3.2-2", "MQTT-4.3.3-3")
	n := StartNode(t, nil)
	// The client does not acknowledge; the server must resend on resumption.
	b, _ := n.Must5(Connect5{ClientID: "myclientid2", CleanStart: false, SessionExpiry: u32(99999), ManualAck: true})
	b.Sub(wildtopics[6], 2)
	b.Pub(topics[1], 1, false, "", nil)
	b.Pub(topics[3], 2, false, "", nil)
	first := b.Expect(2, 3*time.Second)
	b.Terminate()
	b2, ack := n.Must5(Connect5{ClientID: "myclientid2", CleanStart: false, SessionExpiry: u32(99999)})
	if !ack.SessionPresent {
		t.Fatal("session not present")
	}
	again := b2.Expect(2, 3*time.Second)
	for i, m := range again {
		if !m.Duplicate() && m.QoS == 1 {
			t.Errorf("resent QoS 1 message without DUP: %+v", m)
		}
		if m.PacketID != first[i].PacketID {
			t.Errorf("resent with a different packet identifier: %d vs %d", m.PacketID, first[i].PacketID)
		}
	}
	b2.Disconnect(nil)
}

func TestSubscribeFailure(t *testing.T) {
	Statement(t, "MQTT-3.8.4-3", "MQTT-3.8.4-7", "MQTT-3.9.3-2")
	n := StartNode(t, nil)
	a, _ := n.Must5(Connect5{ClientID: "myclientid", CleanStart: true})
	ack := a.Sub(nosubscribeTopic, 2)
	if len(ack.Reasons) != 1 || ack.Reasons[0] < 0x80 {
		t.Fatalf("suback %v", ack.Reasons)
	}
	if ack.Reasons[0] != byte(packet.NotAuthorized) {
		t.Errorf("reason %#x, want Not authorized", ack.Reasons[0])
	}
	a.Disconnect(nil)
}

func TestDollarTopics(t *testing.T) {
	Statement(t, "MQTT-4.7.2-1")
	n := StartNode(t, nil)
	b, _ := n.Must5(Connect5{ClientID: "myclientid2", CleanStart: true})
	b.Sub(wildtopics[5], 2)
	b.Drain()
	b.Pub("$"+topics[1], 1, false, "", nil)
	b.ExpectNone(500 * time.Millisecond)
	// An explicit $ filter does match.
	b.Sub("$TopicA/#", 1)
	b.Pub("$"+topics[1], 1, false, "explicit", nil)
	b.Expect(1, 2*time.Second)
	b.Disconnect(nil)
}

func TestUnsubscribe(t *testing.T) {
	Statement(t, "MQTT-3.10.4-1", "MQTT-3.10.4-2", "MQTT-3.10.4-4", "MQTT-3.11.3-1", "MQTT-3.11.3-2")
	n := StartNode(t, nil)
	b, _ := n.Must5(Connect5{ClientID: "myclientid2", CleanStart: true})
	b.Sub(topics[0], 2)
	b.Sub(topics[1], 2)
	b.Sub(topics[2], 2)
	un := b.Unsubscribe(topics[0])
	if len(un.Reasons) != 1 || un.Reasons[0] != 0 {
		t.Fatalf("unsuback %v", un.Reasons)
	}
	if again := b.Unsubscribe(topics[0]); again.Reasons[0] != byte(packet.NoSubscriptionExisted) {
		t.Fatalf("second unsubscribe %v", again.Reasons)
	}
	a, _ := n.Must5(Connect5{ClientID: "myclientid", CleanStart: true})
	a.Pub(topics[0], 1, false, "topic 0 - unsubscribed", nil)
	a.Pub(topics[1], 1, false, "topic 1", nil)
	a.Pub(topics[2], 1, false, "topic 2", nil)
	ms := b.Expect(2, 3*time.Second)
	for _, m := range ms {
		if m.Topic == topics[0] {
			t.Fatal("received on the unsubscribed topic")
		}
	}
	b.ExpectNone(300 * time.Millisecond)
	a.Disconnect(nil)
	b.Disconnect(nil)
}

func TestSessionExpiry(t *testing.T) {
	Statement(t, "MQTT-3.1.2-23", "MQTT-3.1.2-4", "MQTT-3.1.2-5", "MQTT-3.1.2-6", "MQTT-3.2.2-1", "MQTT-3.2.2-2", "MQTT-3.2.2-3", "MQTT-3.14.2-2", "MQTT-3.14.4-3")
	n := StartNode(t, nil)
	connect := func(clean bool, expiry uint32) (*Client5, *paho.Connack) {
		return n.Must5(Connect5{ClientID: "myclientid", CleanStart: clean, SessionExpiry: u32(expiry)})
	}
	// Expiry 0: the session ends with the connection.
	a, ack := connect(true, 0)
	if ack.SessionPresent {
		t.Fatal("present after clean start")
	}
	a.Sub(topics[0], 2)
	a.Disconnect(nil)
	a, ack = connect(false, 0)
	if ack.SessionPresent {
		t.Fatal("session survived expiry 0")
	}
	a.Disconnect(nil)
	// Expiry 2: alive after 1 second, gone after 3.
	a, _ = connect(true, 2)
	a.Sub(topics[0], 2)
	a.Disconnect(nil)
	time.Sleep(time.Second)
	a, ack = connect(false, 2)
	if !ack.SessionPresent {
		t.Fatal("session expired early")
	}
	a.Disconnect(nil)
	time.Sleep(3 * time.Second)
	a, ack = connect(false, 2)
	if ack.SessionPresent {
		t.Fatal("session did not expire")
	}
	a.Disconnect(nil)
	// DISCONNECT may lengthen a non-zero interval, and set it to 0.
	a, _ = connect(true, 1)
	a.Sub(topics[0], 2)
	a.Disconnect(&paho.DisconnectProperties{SessionExpiryInterval: u32(4)})
	time.Sleep(2 * time.Second)
	a, ack = connect(false, 1)
	if !ack.SessionPresent {
		t.Fatal("DISCONNECT override not applied")
	}
	a.Disconnect(&paho.DisconnectProperties{SessionExpiryInterval: u32(0)})
	a, ack = connect(false, 1)
	if ack.SessionPresent {
		t.Fatal("override to 0 did not end the session")
	}
	a.Disconnect(nil)
	_ = config.Default
}
