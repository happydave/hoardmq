package packet

import (
	"bytes"
	"testing"
)

// Golden byte sequences written from the specification text, so the encoder
// is anchored to the standard rather than to the decoder.
func goldens() []struct {
	name string
	v    Version
	raw  []byte
	pkt  Packet
} {
	return []struct {
		name string
		v    Version
		raw  []byte
		pkt  Packet
	}{
		{"CONNECT v3", V3, hex(0x10, 0x0F, 0x00, 0x06, 'M', 'Q', 'I', 's', 'd', 'p', 0x03, 0x02, 0x00, 0x0A, 0x00, 0x01, 'a'),
			&Connect{ProtocolName: "MQIsdp", ProtocolLevel: V3, CleanStart: true, KeepAlive: 10, ClientID: "a"}},
		{"CONNECT v4", V4, hex(0x10, 0x0C, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x02, 0x00, 0x0A, 0x00, 0x00),
			&Connect{ProtocolName: "MQTT", ProtocolLevel: V4, CleanStart: true, KeepAlive: 10, ClientID: ""}},
		{"CONNECT v5", V5, hex(0x10, 0x0D, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x05, 0x02, 0x00, 0x0A, 0x00, 0x00, 0x00),
			&Connect{ProtocolName: "MQTT", ProtocolLevel: V5, CleanStart: true, KeepAlive: 10, ClientID: ""}},
		{"CONNECT v5 will+auth", V5, hex(0x10, 0x26, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x05, 0xEE, 0x00, 0x3C,
			0x05, 0x11, 0x00, 0x00, 0x00, 0x1E, // session expiry 30
			0x00, 0x02, 'c', '1',
			0x05, 0x18, 0x00, 0x00, 0x00, 0x05, // will delay 5
			0x00, 0x01, 'w', 0x00, 0x01, 'p',
			0x00, 0x01, 'u', 0x00, 0x01, 'x'),
			&Connect{ProtocolName: "MQTT", ProtocolLevel: V5, CleanStart: true, KeepAlive: 60, ClientID: "c1",
				Properties:  Properties{SessionExpiryInterval: u32(30)},
				Will:        &Will{Topic: "w", Payload: []byte("p"), QoS: 1, Retain: true, Properties: Properties{WillDelayInterval: u32(5)}},
				HasUsername: true, Username: "u", HasPassword: true, Password: []byte("x")}},
		{"CONNACK v4", V4, hex(0x20, 0x02, 0x01, 0x00), &Connack{SessionPresent: true, ReasonCode: Success}},
		{"CONNACK v4 refused", V4, hex(0x20, 0x02, 0x00, 0x05), &Connack{ReasonCode: NotAuthorized}},
		{"CONNACK v5", V5, hex(0x20, 0x03, 0x00, 0x00, 0x00), &Connack{ReasonCode: Success}},
		{"CONNACK v5 props", V5, hex(0x20, 0x09, 0x00, 0x87, 0x06, 0x1F, 0x00, 0x03, 'b', 'a', 'd'),
			&Connack{ReasonCode: NotAuthorized, Properties: Properties{ReasonString: str("bad")}}},
		{"PUBLISH v4 qos1", V4, hex(0x32, 0x09, 0x00, 0x03, 'a', '/', 'b', 0x00, 0x01, 'h', 'i'),
			&Publish{QoS: 1, PacketID: 1, Topic: "a/b", Payload: []byte("hi")}},
		{"PUBLISH v4 qos0 retain dup-less", V4, hex(0x31, 0x05, 0x00, 0x01, 't', 'x', 'y'),
			&Publish{Retain: true, Topic: "t", Payload: []byte("xy")}},
		{"PUBLISH v5 qos2 dup", V5, hex(0x3D, 0x0B, 0x00, 0x03, 'a', '/', 'b', 0x00, 0x02, 0x02, 0x01, 0x01, 'h'),
			&Publish{Dup: true, QoS: 2, Retain: true, PacketID: 2, Topic: "a/b", Properties: Properties{PayloadFormatIndicator: u8(1)}, Payload: []byte("h")}},
		{"PUBLISH v5 alias", V5, hex(0x30, 0x06, 0x00, 0x00, 0x03, 0x23, 0x00, 0x07),
			&Publish{Topic: "", Properties: Properties{TopicAlias: u16(7)}, Payload: []byte{}}},
		{"PUBACK v4", V4, hex(0x40, 0x02, 0x00, 0x01), &Puback{Ack{PacketID: 1}}},
		{"PUBACK v5 short", V5, hex(0x40, 0x02, 0x00, 0x01), &Puback{Ack{PacketID: 1}}},
		{"PUBACK v5 reason", V5, hex(0x40, 0x03, 0x00, 0x01, 0x10), &Puback{Ack{PacketID: 1, ReasonCode: NoMatchingSubscribers}}},
		{"PUBREC v5 props", V5, hex(0x50, 0x0A, 0x00, 0x02, 0x97, 0x06, 0x1F, 0x00, 0x03, 'f', 'u', 'l'),
			&Pubrec{Ack{PacketID: 2, ReasonCode: QuotaExceeded, Properties: Properties{ReasonString: str("ful")}}}},
		{"PUBREL v4", V4, hex(0x62, 0x02, 0x00, 0x03), &Pubrel{Ack{PacketID: 3}}},
		{"PUBCOMP v5", V5, hex(0x70, 0x02, 0x00, 0x04), &Pubcomp{Ack{PacketID: 4}}},
		{"SUBSCRIBE v3", V3, hex(0x82, 0x08, 0x00, 0x01, 0x00, 0x03, 'a', '/', 'b', 0x01),
			&Subscribe{PacketID: 1, Subscriptions: []Subscription{{Filter: "a/b", QoS: 1}}}},
		{"SUBSCRIBE v4", V4, hex(0x82, 0x0C, 0x00, 0x01, 0x00, 0x03, 'a', '/', 'b', 0x01, 0x00, 0x01, '#', 0x02),
			&Subscribe{PacketID: 1, Subscriptions: []Subscription{{Filter: "a/b", QoS: 1}, {Filter: "#", QoS: 2}}}},
		{"SUBSCRIBE v5", V5, hex(0x82, 0x0B, 0x00, 0x01, 0x02, 0x0B, 0x05, 0x00, 0x03, 'a', '/', 'b', 0x2E),
			&Subscribe{PacketID: 1, Properties: Properties{SubscriptionIdentifiers: []uint32{5}},
				Subscriptions: []Subscription{{Filter: "a/b", QoS: 2, NoLocal: true, RetainAsPublished: true, RetainHandling: 2}}}},
		{"SUBACK v4", V4, hex(0x90, 0x04, 0x00, 0x01, 0x01, 0x80), &Suback{PacketID: 1, ReasonCodes: []ReasonCode{GrantedQoS1, 0x80}}},
		{"SUBACK v5", V5, hex(0x90, 0x05, 0x00, 0x01, 0x00, 0x02, 0xA2), &Suback{PacketID: 1, ReasonCodes: []ReasonCode{GrantedQoS2, WildcardSubscriptionsNotSupported}}},
		{"UNSUBSCRIBE v4", V4, hex(0xA2, 0x07, 0x00, 0x01, 0x00, 0x03, 'a', '/', 'b'), &Unsubscribe{PacketID: 1, Filters: []string{"a/b"}}},
		{"UNSUBSCRIBE v5", V5, hex(0xA2, 0x08, 0x00, 0x01, 0x00, 0x00, 0x03, 'a', '/', 'b'), &Unsubscribe{PacketID: 1, Filters: []string{"a/b"}}},
		{"UNSUBACK v4", V4, hex(0xB0, 0x02, 0x00, 0x01), &Unsuback{PacketID: 1}},
		{"UNSUBACK v5", V5, hex(0xB0, 0x05, 0x00, 0x01, 0x00, 0x00, 0x11), &Unsuback{PacketID: 1, ReasonCodes: []ReasonCode{Success, NoSubscriptionExisted}}},
		{"PINGREQ", V4, hex(0xC0, 0x00), &Pingreq{}},
		{"PINGRESP", V5, hex(0xD0, 0x00), &Pingresp{}},
		{"DISCONNECT v4", V4, hex(0xE0, 0x00), &Disconnect{}},
		{"DISCONNECT v5 normal", V5, hex(0xE0, 0x00), &Disconnect{}},
		{"DISCONNECT v5 reason", V5, hex(0xE0, 0x01, 0x8E), &Disconnect{ReasonCode: SessionTakenOver}},
		{"DISCONNECT v5 props", V5, hex(0xE0, 0x0D, 0x9C, 0x0B, 0x1C, 0x00, 0x08, 'o', 't', 'h', 'e', 'r', ':', '1', '8'),
			&Disconnect{ReasonCode: UseAnotherServer, Properties: Properties{ServerReference: str("other:18")}}},
		{"AUTH v5 empty", V5, hex(0xF0, 0x00), &Auth{}},
		{"AUTH v5 continue", V5, hex(0xF0, 0x07, 0x18, 0x05, 0x15, 0x00, 0x02, 'A', 'B'),
			&Auth{ReasonCode: ContinueAuthentication, Properties: Properties{AuthenticationMethod: str("AB")}}},
	}
}

func TestGoldenBytes(t *testing.T) {
	for _, g := range goldens() {
		t.Run(g.name, func(t *testing.T) {
			raw, err := (&Encoder{Version: g.v}).Bytes(g.pkt)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			if !bytes.Equal(raw, g.raw) {
				t.Errorf("encode\n got % x\nwant % x", raw, g.raw)
			}
			roundTrip(t, g.v, g.pkt)
			var got Packet
			if g.pkt.Type() == CONNECT {
				got, err = NewDecoder(bytes.NewReader(g.raw)).ReadConnect()
			} else {
				got, err = decodeRaw(g.v, g.raw)
			}
			if err != nil {
				t.Fatalf("decode golden: %v", err)
			}
			enc, _ := (&Encoder{Version: g.v}).Bytes(got)
			if !bytes.Equal(enc, g.raw) {
				t.Errorf("decode(golden) re-encodes to % x", enc)
			}
		})
	}
}

func TestUnsupportedVersionConnack(t *testing.T) {
	var b bytes.Buffer
	if err := EncodeUnsupportedVersion(&b, 6); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b.Bytes(), hex(0x20, 0x03, 0x00, 0x84, 0x00)) {
		t.Errorf("level 6: % x", b.Bytes())
	}
	b.Reset()
	if err := EncodeUnsupportedVersion(&b, 2); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b.Bytes(), hex(0x20, 0x02, 0x00, 0x01)) {
		t.Errorf("level 2: % x", b.Bytes())
	}
}
