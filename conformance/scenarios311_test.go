package conformance

// The 3.1.1 equivalents of the Paho scenarios over paho.mqtt.golang and the
// raw codec: basic, retained, will, offline queueing, overlapping
// subscriptions, keepalive, redelivery, unsubscribe, dollar topics,
// zero-length client identifier, unsupported protocol level.

import (
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
)

func TestBasic311(t *testing.T) {
	Statement(t, "MQTT-3.1.4-4", "MQTT-3.2.0-1", "MQTT-3.3.4-1", "MQTT-3.12.4-1", "MQTT-3.1.0-2", "MQTT-3.8.4-1", "MQTT-3.8.4-2", "MQTT-3.8.4-5", "MQTT-2.3.1-7")
	n := StartNode(t, nil)
	a := n.Must4(Connect4{ClientID: "myclientid", Clean: true})
	a.Disconnect()
	a = n.Must4(Connect4{ClientID: "myclientid", Clean: true})
	a.Sub(topics[0], 2)
	a.Pub(topics[0], 0, false, "qos 0")
	a.Pub(topics[0], 1, false, "qos 1")
	a.Pub(topics[0], 2, false, "qos 2")
	ms := a.Expect(3, 3*time.Second)
	seen := map[byte]bool{}
	for _, m := range ms {
		seen[m.Qos()] = true
	}
	if !seen[0] || !seen[1] || !seen[2] {
		t.Fatalf("qos set %v", seen)
	}
	a.Disconnect()
	// A second CONNECT is a protocol violation: the connection is closed
	// without a DISCONNECT, which 3.1.1 has no server form of.
	r := n.DialRaw(packet.V4)
	r.Connect(&packet.Connect{ClientID: "twice", CleanStart: true})
	r.Send(&packet.Connect{ProtocolLevel: packet.V4, ClientID: "twice", CleanStart: true})
	r.ExpectClosed()
	// PINGREQ is answered.
	r = n.DialRaw(packet.V4)
	r.Connect(&packet.Connect{ClientID: "ping", CleanStart: true})
	r.Send(&packet.Pingreq{})
	if _, ok := r.Read().(*packet.Pingresp); !ok {
		t.Fatal("no PINGRESP")
	}
}

func TestUnsupportedProtocolLevel311(t *testing.T) {
	Statement(t, "MQTT-3.1.2-2", "MQTT-3.2.2-4", "MQTT-3.2.2-5", "MQTT-3.1.4-1")
	n := StartNode(t, nil)
	r := n.DialRaw(packet.V4)
	// An unsupported 3.x-shaped protocol level (2) is answered with return
	// code 1 in the 3.x framing, then the connection closes.
	r.SendBytes([]byte{0x10, 0x0C, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x02, 0x02, 0x00, 0x0A, 0x00, 0x00})
	ack, ok := r.Read().(*packet.Connack)
	if !ok || ack.ReasonCode != packet.UnsupportedProtocolVersion || ack.SessionPresent {
		t.Fatalf("connack %+v", ack)
	}
	r.ExpectClosed()
	// A CONNECT with the reserved flag set is closed without a CONNACK.
	r = n.DialRaw(packet.V4)
	r.SendBytes([]byte{0x10, 0x0C, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x03, 0x00, 0x0A, 0x00, 0x00})
	r.ExpectClosed()
}

func TestRetainedMessage311(t *testing.T) {
	Statement(t, "MQTT-3.3.1-5", "MQTT-3.3.1-6", "MQTT-3.3.1-7", "MQTT-3.3.1-8", "MQTT-3.3.1-9", "MQTT-3.3.1-10", "MQTT-3.3.1-11", "MQTT-3.3.1-12", "MQTT-3.1.2-7")
	n := StartNode(t, nil)
	a := n.Must4(Connect4{ClientID: "myclientid", Clean: true})
	a.Pub(topics[1], 0, true, "qos 0")
	a.Pub(topics[2], 1, true, "qos 1")
	a.Pub(topics[3], 2, true, "qos 2")
	a.Sub(wildtopics[5], 2)
	ms := a.Expect(3, 3*time.Second)
	for _, m := range ms {
		if !m.Retained() {
			t.Fatalf("retained flag not set on subscribe delivery: %s", m.Topic())
		}
	}
	// A live publish to an established subscription arrives with RETAIN 0.
	a.Pub(topics[1], 1, true, "live")
	if m := a.Expect(1, 3*time.Second)[0]; m.Retained() {
		t.Fatal("RETAIN set on a live delivery at 3.1.1")
	}
	// A non-retained publish does not disturb the stored message.
	a.Pub(topics[1], 0, false, "plain")
	a.Expect(1, 3*time.Second)
	a.Disconnect()
	// Retained messages are not session state: a new session still sees them.
	b := n.Must4(Connect4{ClientID: "myclientid2", Clean: true})
	b.Sub(topics[1], 0)
	if m := b.Expect(1, 3*time.Second)[0]; string(m.Payload()) != "live" {
		t.Fatalf("retained replaced wrongly: %q", m.Payload())
	}
	// Zero-byte retained publish removes it.
	b.Pub(topics[1], 0, true, "")
	b.Expect(1, 3*time.Second)
	b.Disconnect()
	c := n.Must4(Connect4{ClientID: "myclientid3", Clean: true})
	c.Sub(topics[1], 0)
	c.ExpectNone(500 * time.Millisecond)
	for _, tp := range topics[2:4] {
		c.Pub(tp, 0, true, "")
	}
	c.Disconnect()
}

func TestWillMessage311(t *testing.T) {
	Statement(t, "MQTT-3.1.2-8", "MQTT-3.1.2-9", "MQTT-3.1.2-10", "MQTT-3.1.2-12", "MQTT-3.1.2-16", "MQTT-3.1.2-24", "MQTT-3.14.4-3")
	n := StartNode(t, nil)
	b := n.Must4(Connect4{ClientID: "myclientid2", Clean: true})
	b.Sub(topics[2], 2)
	// A will publisher that never pings is closed at 1.5 × keep alive.
	r := n.DialRaw(packet.V4)
	r.Connect(&packet.Connect{ClientID: "myclientid", CleanStart: true, KeepAlive: 1, Will: &packet.Will{Topic: topics[2], Payload: []byte("will message"), QoS: 1}})
	ms := b.Expect(1, 4*time.Second)
	if string(ms[0].Payload()) != "will message" || ms[0].Retained() {
		t.Fatalf("will %+v", ms[0])
	}
	r.ExpectClosed()
	// A clean DISCONNECT discards the Will; a connection without a Will
	// flag publishes nothing.
	r = n.DialRaw(packet.V4)
	r.Connect(&packet.Connect{ClientID: "myclientid", CleanStart: true, Will: &packet.Will{Topic: topics[2], Payload: []byte("not sent")}})
	r.Send(&packet.Disconnect{})
	_ = r.Conn.Close()
	r = n.DialRaw(packet.V4)
	r.Connect(&packet.Connect{ClientID: "nowill", CleanStart: true})
	_ = r.Conn.Close()
	b.ExpectNone(700 * time.Millisecond)
	b.Disconnect()
}

func TestOfflineQueueingAndSessionPresent311(t *testing.T) {
	Statement(t, "MQTT-3.1.2-4", "MQTT-3.1.2-5", "MQTT-3.1.2-6", "MQTT-3.2.2-1", "MQTT-3.2.2-2", "MQTT-3.2.2-3", "MQTT-4.1.0-1")
	n := StartNode(t, nil)
	a := n.Must4(Connect4{ClientID: "myclientid", Clean: false})
	if a.SessionPresent() {
		t.Fatal("new session reported present")
	}
	a.Sub(wildtopics[5], 2)
	a.Disconnect()
	n.WaitOffline("myclientid")
	b := n.Must4(Connect4{ClientID: "myclientid2", Clean: true})
	b.Pub(topics[1], 0, false, "qos 0")
	b.Pub(topics[2], 1, false, "qos 1")
	b.Pub(topics[3], 2, false, "qos 2")
	b.Disconnect()
	a = n.Must4(Connect4{ClientID: "myclientid", Clean: false})
	if !a.SessionPresent() {
		t.Fatal("session not present")
	}
	a.Expect(3, 3*time.Second)
	a.Disconnect()
	n.WaitOffline("myclientid")
	// Clean Session 1 discards it.
	a = n.Must4(Connect4{ClientID: "myclientid", Clean: true})
	if a.SessionPresent() {
		t.Fatal("clean session reported present")
	}
	a.Disconnect()
	n.WaitOffline("myclientid")
	a = n.Must4(Connect4{ClientID: "myclientid", Clean: false})
	if a.SessionPresent() {
		t.Fatal("session survived clean session")
	}
	a.Disconnect()
}

func TestOverlappingSubscriptions311(t *testing.T) {
	Statement(t, "MQTT-3.3.5-1", "MQTT-3.8.4-3", "MQTT-3.8.4-6", "MQTT-3.3.2-3")
	n := StartNode(t, nil)
	a := n.Must4(Connect4{ClientID: "myclientid", Clean: true})
	a.Sub(wildtopics[6], 2)
	a.Sub(wildtopics[0], 1)
	a.Pub(topics[3], 2, false, "overlapping topic filters")
	ms := a.Expect(1, 3*time.Second)
	if ms[0].Qos() != 2 {
		t.Fatalf("qos %d", ms[0].Qos())
	}
	a.ExpectNone(500 * time.Millisecond)
	// Granted QoS caps delivery (Topic/C matches neither wildcard filter).
	a.Sub(topics[2], 0)
	a.Pub(topics[2], 2, false, "capped")
	if m := a.Expect(1, 3*time.Second)[0]; m.Qos() != 0 {
		t.Fatalf("delivered at qos %d, granted 0", m.Qos())
	}
	a.Disconnect()
}

func TestRedeliveryOnReconnect311(t *testing.T) {
	Statement(t, "MQTT-4.4.0-1", "MQTT-3.3.1-1", "MQTT-3.3.1-3", "MQTT-2.3.1-4", "MQTT-4.3.2-1", "MQTT-4.3.3-1")
	n := StartNode(t, nil)
	// A raw subscriber that never acknowledges.
	r := n.DialRaw(packet.V4)
	r.Connect(&packet.Connect{ClientID: "myclientid2", CleanStart: false})
	r.Send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: wildtopics[6], QoS: 2}}})
	r.Read()
	p := n.Must4(Connect4{ClientID: "pub", Clean: true})
	p.Pub(topics[1], 1, false, "one")
	p.Pub(topics[3], 2, false, "two")
	var first []*packet.Publish
	for len(first) < 2 {
		if pub, ok := r.Read().(*packet.Publish); ok {
			first = append(first, pub)
		}
	}
	_ = r.Conn.Close()
	n.WaitOffline("myclientid2")
	r = n.DialRaw(packet.V4)
	if ack := r.Connect(&packet.Connect{ClientID: "myclientid2", CleanStart: false}); !ack.SessionPresent {
		t.Fatal("session not present")
	}
	var again []*packet.Publish
	for len(again) < 2 {
		if pub, ok := r.Read().(*packet.Publish); ok {
			again = append(again, pub)
		}
	}
	for i := range again {
		if !again[i].Dup || again[i].PacketID != first[i].PacketID || string(again[i].Payload) != string(first[i].Payload) {
			t.Fatalf("redelivery %d: %+v vs %+v", i, again[i], first[i])
		}
	}
	// The received copies never carried DUP the first time.
	for _, f := range first {
		if f.Dup {
			t.Fatal("first delivery carried DUP")
		}
	}
	r.Send(&packet.Puback{Ack: packet.Ack{PacketID: again[0].PacketID}})
	r.Send(&packet.Pubrec{Ack: packet.Ack{PacketID: again[1].PacketID}})
	if _, ok := r.Read().(*packet.Pubrel); !ok {
		t.Fatal("no PUBREL")
	}
	r.Send(&packet.Pubcomp{Ack: packet.Ack{PacketID: again[1].PacketID}})
}

func TestUnsubscribe311(t *testing.T) {
	Statement(t, "MQTT-3.10.4-1", "MQTT-3.10.4-2", "MQTT-3.10.4-4", "MQTT-3.10.4-5", "MQTT-3.10.4-6")
	n := StartNode(t, nil)
	b := n.Must4(Connect4{ClientID: "myclientid2", Clean: true})
	b.Sub(topics[0], 2)
	b.Sub(topics[1], 2)
	b.Sub(topics[2], 2)
	b.Unsub(topics[0])
	b.Unsub(topics[0], "never/subscribed") // still acknowledged
	a := n.Must4(Connect4{ClientID: "myclientid", Clean: true})
	a.Pub(topics[0], 1, false, "topic 0 - unsubscribed")
	a.Pub(topics[1], 1, false, "topic 1")
	a.Pub(topics[2], 1, false, "topic 2")
	for _, m := range b.Expect(2, 3*time.Second) {
		if m.Topic() == topics[0] {
			t.Fatal("received on the unsubscribed topic")
		}
	}
	b.ExpectNone(300 * time.Millisecond)
	a.Disconnect()
	b.Disconnect()
}

func TestDollarTopics311(t *testing.T) {
	Statement(t, "MQTT-4.7.2-1")
	n := StartNode(t, nil)
	b := n.Must4(Connect4{ClientID: "myclientid2", Clean: true})
	b.Sub(wildtopics[5], 2)
	b.Pub("$"+topics[1], 1, false, "")
	b.ExpectNone(500 * time.Millisecond)
	b.Disconnect()
}

func TestZeroLengthClientID311(t *testing.T) {
	Statement(t, "MQTT-3.1.3-6", "MQTT-3.1.3-8", "MQTT-3.1.3-9")
	n := StartNode(t, nil)
	r := n.DialRaw(packet.V4)
	if ack := r.Connect(&packet.Connect{ClientID: "", CleanStart: true}); ack.ReasonCode != packet.Success {
		t.Fatalf("zero-length id with clean session 1: %s", ack.ReasonCode)
	}
	r.Send(&packet.Disconnect{})
	r = n.DialRaw(packet.V4)
	ack := r.Connect(&packet.Connect{ClientID: "", CleanStart: false})
	if ack.ReasonCode != packet.ClientIdentifierNotValid || ack.SessionPresent {
		t.Fatalf("zero-length id with clean session 0: %+v", ack)
	}
	r.ExpectClosed()
}

func TestSubscribeFailure311(t *testing.T) {
	Statement(t, "MQTT-3.8.4-5", "MQTT-3.9.3-1", "MQTT-3.9.3-2", "MQTT-3.3.5-2")
	n := StartNode(t, nil)
	r := n.DialRaw(packet.V4)
	r.Connect(&packet.Connect{ClientID: "myclientid", CleanStart: true})
	r.Send(&packet.Subscribe{PacketID: 7, Subscriptions: []packet.Subscription{{Filter: topics[0], QoS: 1}, {Filter: nosubscribeTopic, QoS: 2}, {Filter: topics[1], QoS: 2}}})
	ack, ok := r.Read().(*packet.Suback)
	if !ok || ack.PacketID != 7 || len(ack.ReasonCodes) != 3 || ack.ReasonCodes[0] != 1 || ack.ReasonCodes[1] != 0x80 || ack.ReasonCodes[2] != 2 {
		t.Fatalf("suback %+v", ack)
	}
}
