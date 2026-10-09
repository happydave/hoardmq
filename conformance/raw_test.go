package conformance

// Statement-directed tests over hand-crafted frames: the fixed header,
// Remaining Length, UTF-8, properties, reason codes and CONNECT rules that
// the codec and the connection layer enforce, observed here through the
// node's answer on the wire.

import (
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

type rawCase struct {
	name  string
	ids   []string
	v     packet.Version
	after bool   // send after a successful CONNECT
	raw   []byte // bytes to send (after CONNECT when after is set)
	want  packet.ReasonCode
	close bool // expect a plain close (levels 3 and 4) instead of a DISCONNECT
}

func TestRawStatements(t *testing.T) {
	// A small alias limit so an alias above it fits in a short frame.
	node := StartNode(t, func(d *config.Document) { d.MQTT.TopicAliasMaximum = 10 })
	cases := []rawCase{
		// Fixed header and Remaining Length.
		{"PINGREQ flags", []string{"MQTT-2.1.3-1", "MQTT-3.14.1-1", "MQTT-4.13.1-1"}, packet.V5, true, []byte{0xC1, 0x00}, packet.MalformedPacket, false},
		{"SUBSCRIBE flags", []string{"MQTT-3.8.1-1"}, packet.V5, true, []byte{0x80, 0x07, 0x00, 0x01, 0x00, 0x00, 0x01, 'a', 0x00}, packet.MalformedPacket, false},
		{"UNSUBSCRIBE flags", []string{"MQTT-3.10.1-1"}, packet.V5, true, []byte{0xA0, 0x06, 0x00, 0x01, 0x00, 0x00, 0x01, 'a'}, packet.MalformedPacket, false},
		{"PUBREL flags", []string{"MQTT-3.6.1-1"}, packet.V5, true, []byte{0x60, 0x02, 0x00, 0x01}, packet.MalformedPacket, false},
		{"AUTH flags", []string{"MQTT-3.15.1-1"}, packet.V5, true, []byte{0xF1, 0x00}, packet.MalformedPacket, false},
		{"Remaining Length five bytes", []string{"MQTT-1.5.5-1"}, packet.V5, true, []byte{0xC0, 0x80, 0x80, 0x80, 0x80, 0x01}, packet.MalformedPacket, false},
		{"PUBLISH QoS 3", []string{"MQTT-3.3.1-4"}, packet.V5, true, []byte{0x36, 0x05, 0x00, 0x01, 't', 0x00, 0x01}, packet.MalformedPacket, false},
		{"PUBLISH DUP on QoS 0", []string{"MQTT-3.3.1-2"}, packet.V5, true, []byte{0x38, 0x04, 0x00, 0x01, 't', 0x00}, packet.MalformedPacket, false},
		{"QoS 0 with packet identifier is a topic byte", []string{"MQTT-2.2.1-2"}, packet.V5, true, []byte{0x30, 0x03, 0x00, 0x01, 't'}, packet.MalformedPacket, false},
		// Strings.
		{"invalid UTF-8 topic", []string{"MQTT-1.5.4-1", "MQTT-3.3.2-1"}, packet.V5, true, []byte{0x30, 0x05, 0x00, 0x02, 0xFF, 0xFE, 0x00}, packet.MalformedPacket, false},
		{"U+0000 in topic", []string{"MQTT-1.5.4-2", "MQTT-4.7.3-2"}, packet.V5, true, []byte{0x30, 0x05, 0x00, 0x02, 't', 0x00, 0x00}, packet.MalformedPacket, false},
		{"empty topic without alias", []string{"MQTT-4.7.3-1"}, packet.V5, true, []byte{0x30, 0x03, 0x00, 0x00, 0x00}, packet.ProtocolError, false},
		{"wildcard in topic name", []string{"MQTT-3.3.2-2", "MQTT-4.7.0-1"}, packet.V5, true, []byte{0x30, 0x06, 0x00, 0x03, 'a', '/', '#', 0x00}, packet.ProtocolError, false},
		// Properties.
		{"unknown property", []string{"MQTT-2.2.2-1"}, packet.V5, true, []byte{0x30, 0x06, 0x00, 0x01, 't', 0x02, 0x7F, 0x00}, packet.MalformedPacket, false},
		{"Subscription Identifier in PUBLISH", []string{"MQTT-3.3.4-6"}, packet.V5, true, []byte{0x30, 0x06, 0x00, 0x01, 't', 0x02, 0x0B, 0x01}, packet.ProtocolError, false},
		{"Topic Alias 0", []string{"MQTT-3.3.2-8"}, packet.V5, true, []byte{0x30, 0x07, 0x00, 0x01, 't', 0x03, 0x23, 0x00, 0x00}, packet.TopicAliasInvalid, false},
		{"Topic Alias above maximum", []string{"MQTT-3.3.2-12", "MQTT-3.2.2-17"}, packet.V5, true, []byte{0x30, 0x07, 0x00, 0x01, 't', 0x03, 0x23, 0xFF, 0xFF}, packet.TopicAliasInvalid, false},
		{"Payload Format 2", []string{"MQTT-3.3.2-4"}, packet.V5, true, []byte{0x30, 0x06, 0x00, 0x01, 't', 0x02, 0x01, 0x02}, packet.ProtocolError, false},
		// Subscribe payload rules.
		{"SUBSCRIBE without filters", []string{"MQTT-3.8.3-2"}, packet.V5, true, []byte{0x82, 0x03, 0x00, 0x01, 0x00}, packet.ProtocolError, false},
		{"SUBSCRIBE reserved bits", []string{"MQTT-3.8.3-5"}, packet.V5, true, []byte{0x82, 0x07, 0x00, 0x01, 0x00, 0x00, 0x01, 'a', 0x40}, packet.MalformedPacket, false},
		{"No Local on shared", []string{"MQTT-3.8.3-4", "MQTT-4.8.2-1"}, packet.V5, true, []byte{0x82, 0x10, 0x00, 0x01, 0x00, 0x00, 0x0A, '$', 's', 'h', 'a', 'r', 'e', '/', 'g', '/', 'a', 0x04}, packet.ProtocolError, false},
		{"UNSUBSCRIBE without filters", []string{"MQTT-3.10.3-2"}, packet.V5, true, []byte{0xA2, 0x03, 0x00, 0x01, 0x00}, packet.ProtocolError, false},
		{"SUBSCRIBE packet identifier 0", []string{"MQTT-2.2.1-3"}, packet.V5, true, []byte{0x82, 0x07, 0x00, 0x00, 0x00, 0x00, 0x01, 'a', 0x00}, packet.ProtocolError, false},
		// Acknowledgements.
		{"PUBREL bad reason", []string{"MQTT-3.6.2-1"}, packet.V5, true, []byte{0x62, 0x03, 0x00, 0x01, 0x10}, packet.ProtocolError, false},
		{"AUTH outside an exchange", []string{"MQTT-4.12.0-7", "MQTT-4.12.0-6"}, packet.V5, true, []byte{0xF0, 0x00}, packet.ProtocolError, false},
		// Level 4: the same violations close the connection without a DISCONNECT.
		{"3.1.1 PUBLISH QoS 3", []string{"MQTT-3.3.1-4"}, packet.V4, true, []byte{0x36, 0x05, 0x00, 0x01, 't', 0x00, 0x01}, 0, true},
		{"3.1.1 SUBSCRIBE reserved bits", []string{"MQTT-3.8.3-4"}, packet.V4, true, []byte{0x82, 0x06, 0x00, 0x01, 0x00, 0x01, 'a', 0x04}, 0, true},
		{"3.1.1 SUBSCRIBE without filters", []string{"MQTT-3.8.3-3"}, packet.V4, true, []byte{0x82, 0x02, 0x00, 0x01}, 0, true},
		{"3.1.1 UNSUBSCRIBE without filters", []string{"MQTT-3.10.3-2"}, packet.V4, true, []byte{0xA2, 0x02, 0x00, 0x01}, 0, true},
		{"3.1.1 invalid flags", []string{"MQTT-2.2.2-1", "MQTT-2.2.2-2"}, packet.V4, true, []byte{0xC1, 0x00}, 0, true},
		{"3.1.1 reserved DISCONNECT bits", []string{"MQTT-3.14.1-1"}, packet.V4, true, []byte{0xE1, 0x00}, 0, true},
		{"3.1.1 PUBREL flags", []string{"MQTT-3.6.1-1"}, packet.V4, true, []byte{0x60, 0x02, 0x00, 0x01}, 0, true},
		{"3.1.1 SUBSCRIBE flags", []string{"MQTT-3.8.1-1"}, packet.V4, true, []byte{0x80, 0x06, 0x00, 0x01, 0x00, 0x01, 'a', 0x00}, 0, true},
		{"3.1.1 UNSUBSCRIBE flags", []string{"MQTT-3.10.1-1"}, packet.V4, true, []byte{0xA0, 0x05, 0x00, 0x01, 0x00, 0x01, 'a'}, 0, true},
		{"3.1.1 invalid UTF-8", []string{"MQTT-1.5.3-1", "MQTT-3.3.2-1"}, packet.V4, true, []byte{0x30, 0x04, 0x00, 0x02, 0xFF, 0xFE}, 0, true},
		{"3.1.1 U+0000", []string{"MQTT-1.5.3-2"}, packet.V4, true, []byte{0x30, 0x04, 0x00, 0x02, 't', 0x00}, 0, true},
		{"3.1.1 wildcard in topic name", []string{"MQTT-3.3.2-2", "MQTT-4.7.1-1"}, packet.V4, true, []byte{0x30, 0x03, 0x00, 0x01, '#'}, 0, true},
		{"3.1.1 QoS 1 packet identifier 0", []string{"MQTT-2.3.1-1"}, packet.V4, true, []byte{0x32, 0x05, 0x00, 0x01, 't', 0x00, 0x00}, 0, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			Statement(t, c.ids...)
			n := node.For(t)
			r := n.DialRaw(c.v)
			if c.after {
				id := "raw-" + strings.ReplaceAll(c.name, " ", "-")
				if ack := r.Connect(&packet.Connect{ClientID: id[:min(len(id), 23)], CleanStart: true}); ack.ReasonCode != packet.Success {
					t.Fatalf("connect %s", ack.ReasonCode)
				}
			}
			r.SendBytes(c.raw)
			d := r.ReadEnd()
			if c.close {
				if d != nil {
					t.Fatalf("expected a plain close, got DISCONNECT %s", d.ReasonCode)
				}
				return
			}
			if d == nil || d.ReasonCode != c.want {
				t.Fatalf("got %+v, want DISCONNECT %s", d, c.want)
			}
		})
	}
}

func TestRawConnectRules(t *testing.T) {
	node := StartNode(t, nil)
	connect := func(name string, level byte, flags byte, rest ...byte) []byte {
		body := []byte{0, byte(len(name))}
		body = append(body, name...)
		body = append(body, level, flags, 0, 10)
		body = append(body, rest...)
		return append([]byte{0x10, byte(len(body))}, body...)
	}
	t.Run("reserved flag closes without CONNACK", func(t *testing.T) {
		Statement(t, "MQTT-3.1.2-3", "MQTT-3.1.4-1")
		n := node.For(t)
		r := n.DialRaw(packet.V5)
		r.SendBytes(connect("MQTT", 5, 0x03, 0x00, 0x00, 0x00))
		r.ExpectClosed()
	})
	t.Run("will flags without will", func(t *testing.T) {
		Statement(t, "MQTT-3.1.2-11", "MQTT-3.1.2-13", "MQTT-3.1.2-15")
		n := node.For(t)
		r := n.DialRaw(packet.V5)
		r.SendBytes(connect("MQTT", 5, 0x2A, 0x00, 0x00, 0x00))
		r.ExpectClosed()
	})
	t.Run("will QoS 3", func(t *testing.T) {
		Statement(t, "MQTT-3.1.2-12", "MQTT-3.1.2-14")
		n := node.For(t)
		r := n.DialRaw(packet.V5)
		r.SendBytes(connect("MQTT", 5, 0x1E, 0x00, 0x00, 0x00))
		r.ExpectClosed()
	})
	t.Run("password without username at 3.1.1", func(t *testing.T) {
		Statement(t, "MQTT-3.1.2-22")
		n := node.For(t)
		r := n.DialRaw(packet.V4)
		r.SendBytes(connect("MQTT", 4, 0x42, 0x00, 0x00, 0x00, 0x00))
		r.ExpectClosed()
	})
	t.Run("username flag without username", func(t *testing.T) {
		Statement(t, "MQTT-3.1.2-17", "MQTT-3.1.2-19", "MQTT-3.1.3-1")
		n := node.For(t)
		r := n.DialRaw(packet.V5)
		r.SendBytes(connect("MQTT", 5, 0x82, 0x00, 0x00, 0x00))
		r.ExpectClosed()
	})
	t.Run("first packet not CONNECT", func(t *testing.T) {
		Statement(t, "MQTT-3.1.0-1")
		n := node.For(t)
		r := n.DialRaw(packet.V5)
		r.Send(&packet.Pingreq{})
		r.ExpectClosed()
	})
	t.Run("unsupported protocol version 5.0-shaped", func(t *testing.T) {
		Statement(t, "MQTT-3.1.2-2", "MQTT-3.2.2-6", "MQTT-3.2.2-7", "MQTT-4.13.2-1")
		n := node.For(t)
		r := n.DialRaw(packet.V5)
		r.SendBytes(connect("MQTT", 6, 0x02, 0x00, 0x00, 0x00))
		ack, ok := r.Read().(*packet.Connack)
		if !ok || ack.ReasonCode != packet.UnsupportedProtocolVersion || ack.SessionPresent {
			t.Fatalf("connack %+v", ack)
		}
		r.ExpectClosed()
	})
	t.Run("level 3 client identifier rules", func(t *testing.T) {
		Statement(t, "MQTT-3.1.3-5", "MQTT-3.1.3-9")
		n := node.For(t)
		r := n.DialRaw(packet.V3)
		long := strings.Repeat("a", 24)
		r.SendBytes(connect("MQIsdp", 3, 0x02, append([]byte{0, 24}, []byte(long)...)...))
		ack, ok := r.Read().(*packet.Connack)
		if !ok || ack.ReasonCode != packet.ClientIdentifierNotValid {
			t.Fatalf("connack %+v", ack)
		}
		r.ExpectClosed()
		r = n.DialRaw(packet.V3)
		ok23 := strings.Repeat("Z", 23)
		r.SendBytes(connect("MQIsdp", 3, 0x02, append([]byte{0, 23}, []byte(ok23)...)...))
		if ack, ok := r.Read().(*packet.Connack); !ok || ack.ReasonCode != packet.Success {
			t.Fatalf("23-character identifier refused: %+v", ack)
		}
	})
	t.Run("client identifier with the base character set", func(t *testing.T) {
		Statement(t, "MQTT-3.1.3-5", "MQTT-3.1.3-2")
		n := node.For(t)
		r := n.DialRaw(packet.V5)
		id := "0123456789abcdefghijklm"
		if ack := r.Connect(&packet.Connect{ClientID: id, CleanStart: true}); ack.ReasonCode != packet.Success {
			t.Fatalf("connack %s", ack.ReasonCode)
		}
	})
	t.Run("session state discarded when expiry passed", func(t *testing.T) {
		Statement(t, "MQTT-4.1.0-1", "MQTT-4.1.0-2")
		n := node.For(t)
		r := n.DialRaw(packet.V5)
		exp := uint32(1)
		r.Connect(&packet.Connect{ClientID: "expiring", CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}})
		r.Send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "e", QoS: 0}}})
		r.Read()
		// Still connected: the state is held.
		if rec, ok := n.Srv.Sessions().Record("expiring"); !ok || rec.Subscriptions != 1 {
			t.Fatalf("state not held while connected: %+v", rec)
		}
		r.Send(&packet.Disconnect{})
		_ = r.Conn.Close()
		n.WaitOffline("expiring")
		r2 := n.DialRaw(packet.V5)
		ack := r2.Connect(&packet.Connect{ClientID: "expiring", CleanStart: false, Properties: packet.Properties{SessionExpiryInterval: &exp}})
		if !ack.SessionPresent {
			t.Fatal("session gone before its expiry")
		}
		r2.Send(&packet.Disconnect{})
		_ = r2.Conn.Close()
		n.WaitOffline("expiring")
		waitFor := func() bool {
			_, ok := n.Srv.Sessions().Record("expiring")
			return !ok
		}
		deadline := 40
		for !waitFor() && deadline > 0 {
			deadline--
			time.Sleep(100 * time.Millisecond)
		}
		if !waitFor() {
			t.Fatal("session did not expire")
		}
	})
}
