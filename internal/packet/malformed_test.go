package packet

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

func connectBytes(name string, level byte, flags byte, rest ...byte) []byte {
	body := []byte{0, byte(len(name))}
	body = append(body, name...)
	body = append(body, level, flags, 0, 10)
	body = append(body, rest...)
	return append([]byte{0x10, byte(len(body))}, body...)
}

func TestMalformedAndProtocolErrors(t *testing.T) {
	cases := []struct {
		name string
		v    Version
		raw  []byte
		want ReasonCode
	}{
		// Fixed header.
		{"type 0", V5, hex(0x00, 0x00), MalformedPacket},
		{"AUTH at v4", V4, hex(0xF0, 0x00), MalformedPacket},
		{"AUTH at v3", V3, hex(0xF0, 0x00), MalformedPacket},
		{"PINGREQ flags", V4, hex(0xC1, 0x00), MalformedPacket},
		{"SUBSCRIBE flags 0000", V4, hex(0x80, 0x06, 0x00, 0x01, 0x00, 0x01, 'a', 0x00), MalformedPacket},
		{"PUBREL flags 0000", V5, hex(0x60, 0x02, 0x00, 0x01), MalformedPacket},
		{"PUBLISH QoS 3", V4, hex(0x36, 0x04, 0x00, 0x01, 't', 0x00), MalformedPacket},
		{"PUBLISH DUP with QoS 0", V5, hex(0x38, 0x04, 0x00, 0x01, 't', 0x00), MalformedPacket},
		{"remaining length 5 bytes", V4, hex(0xC0, 0x80, 0x80, 0x80, 0x80, 0x01), MalformedPacket},
		{"PINGREQ with body", V4, hex(0xC0, 0x01, 0x00), MalformedPacket},
		{"PINGRESP with body", V5, hex(0xD0, 0x01, 0x00), MalformedPacket},
		{"leftover after PUBACK", V4, hex(0x40, 0x03, 0x00, 0x01, 0x00), MalformedPacket},
		{"leftover after PUBACK v5", V5, hex(0x40, 0x05, 0x00, 0x01, 0x00, 0x00, 0xFF), MalformedPacket},
		// Data representations.
		{"string overruns", V4, hex(0x30, 0x03, 0x00, 0x05, 't'), MalformedPacket},
		{"invalid UTF-8", V4, hex(0x30, 0x04, 0x00, 0x02, 0xFF, 0xFE), MalformedPacket},
		{"U+0000 in string", V4, hex(0x30, 0x04, 0x00, 0x02, 't', 0x00), MalformedPacket},
		{"surrogate in string", V4, hex(0x30, 0x05, 0x00, 0x03, 0xED, 0xA0, 0x80), MalformedPacket},
		{"truncated two byte int", V4, hex(0x32, 0x04, 0x00, 0x01, 't', 0x00), MalformedPacket},
		// Properties.
		{"unknown property id", V5, hex(0x30, 0x06, 0x00, 0x01, 't', 0x02, 0x7F, 0x00), MalformedPacket},
		{"property not valid here", V5, hex(0x30, 0x07, 0x00, 0x01, 't', 0x03, 0x21, 0x00, 0x05), MalformedPacket},
		{"property repeated", V5, hex(0x30, 0x08, 0x00, 0x01, 't', 0x04, 0x01, 0x01, 0x01, 0x01), ProtocolError},
		{"property length overruns", V5, hex(0x30, 0x04, 0x00, 0x01, 't', 0x09), MalformedPacket},
		{"property value truncated", V5, hex(0x30, 0x05, 0x00, 0x01, 't', 0x01, 0x02), MalformedPacket},
		{"payload format 2", V5, hex(0x30, 0x06, 0x00, 0x01, 't', 0x02, 0x01, 0x02), ProtocolError},
		{"topic alias 0", V5, hex(0x30, 0x07, 0x00, 0x01, 't', 0x03, 0x23, 0x00, 0x00), TopicAliasInvalid},
		{"subscription id 0", V5, hex(0x30, 0x06, 0x00, 0x01, 't', 0x02, 0x0B, 0x00), ProtocolError},
		{"subscription id twice in SUBSCRIBE", V5, hex(0x82, 0x0B, 0x00, 0x01, 0x04, 0x0B, 0x01, 0x0B, 0x02, 0x00, 0x01, 'a', 0x00), ProtocolError},
		{"receive maximum 0", V5, connectBytes("MQTT", 5, 0x02, 0x03, 0x21, 0x00, 0x00, 0x00, 0x00), ProtocolError},
		{"maximum packet size 0", V5, connectBytes("MQTT", 5, 0x02, 0x05, 0x27, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00), ProtocolError},
		{"request response info 2", V5, connectBytes("MQTT", 5, 0x02, 0x02, 0x19, 0x02, 0x00, 0x00), ProtocolError},
		{"request problem info 2", V5, connectBytes("MQTT", 5, 0x02, 0x02, 0x17, 0x02, 0x00, 0x00), ProtocolError},
		{"auth data without method", V5, connectBytes("MQTT", 5, 0x02, 0x03, 0x16, 0x00, 0x00, 0x00, 0x00), ProtocolError},
		{"maximum qos 2 in CONNACK", V5, hex(0x20, 0x05, 0x00, 0x00, 0x02, 0x24, 0x02), ProtocolError},
		{"retain available 2", V5, hex(0x20, 0x05, 0x00, 0x00, 0x02, 0x25, 0x02), ProtocolError},
		{"wildcard available 2", V5, hex(0x20, 0x05, 0x00, 0x00, 0x02, 0x28, 0x02), ProtocolError},
		{"sub id available 2", V5, hex(0x20, 0x05, 0x00, 0x00, 0x02, 0x29, 0x02), ProtocolError},
		{"shared available 2", V5, hex(0x20, 0x05, 0x00, 0x00, 0x02, 0x2A, 0x02), ProtocolError},
		// CONNECT.
		{"CONNECT reserved flag", V4, connectBytes("MQTT", 4, 0x03, 0x00, 0x00), MalformedPacket},
		{"will qos without will flag", V4, connectBytes("MQTT", 4, 0x0A, 0x00, 0x00), MalformedPacket},
		{"will retain without will flag", V4, connectBytes("MQTT", 4, 0x22, 0x00, 0x00), MalformedPacket},
		{"will qos 3", V4, connectBytes("MQTT", 4, 0x1E, 0x00, 0x00), MalformedPacket},
		{"password without username v4", V4, connectBytes("MQTT", 4, 0x42, 0x00, 0x00, 0x00, 0x00), MalformedPacket},
		{"password without username v3", V3, connectBytes("MQIsdp", 3, 0x42, 0x00, 0x01, 'a', 0x00, 0x00), MalformedPacket},
		{"unsupported level 6", V5, connectBytes("MQTT", 6, 0x02, 0x00, 0x00), UnsupportedProtocolVersion},
		{"unsupported name", V4, connectBytes("MQTX", 4, 0x02, 0x00, 0x00), UnsupportedProtocolVersion},
		{"MQIsdp at level 4", V4, connectBytes("MQIsdp", 4, 0x02, 0x00, 0x00), UnsupportedProtocolVersion},
		{"MQTT at level 3", V3, connectBytes("MQTT", 3, 0x02, 0x00, 0x00), UnsupportedProtocolVersion},
		{"level 3 empty client id", V3, connectBytes("MQIsdp", 3, 0x02, 0x00, 0x00), ClientIdentifierNotValid},
		{"level 3 client id 24 chars", V3, connectBytes("MQIsdp", 3, 0x02, append([]byte{0x00, 24}, []byte(strings.Repeat("a", 24))...)...), ClientIdentifierNotValid},
		{"CONNECT leftover", V4, connectBytes("MQTT", 4, 0x02, 0x00, 0x00, 0xFF), MalformedPacket},
		{"CONNECT flags nibble", V4, hex(0x11, 0x0C, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x02, 0x00, 0x0A, 0x00, 0x00), MalformedPacket},
		{"first packet not CONNECT", V4, hex(0xC0, 0x00), ProtocolError},
		// CONNACK.
		{"CONNACK reserved flag", V5, hex(0x20, 0x03, 0x02, 0x00, 0x00), MalformedPacket},
		{"CONNACK bad reason v5", V5, hex(0x20, 0x03, 0x00, 0x8D, 0x00), ProtocolError},
		{"CONNACK bad return code v4", V4, hex(0x20, 0x02, 0x00, 0x06), ProtocolError},
		// PUBLISH.
		{"topic with +", V4, hex(0x30, 0x05, 0x00, 0x03, 'a', '+', 'b'), ProtocolError},
		{"topic with #", V5, hex(0x30, 0x04, 0x00, 0x01, '#', 0x00), ProtocolError},
		{"empty topic v4", V4, hex(0x30, 0x02, 0x00, 0x00), ProtocolError},
		{"empty topic v5 without alias", V5, hex(0x30, 0x03, 0x00, 0x00, 0x00), ProtocolError},
		{"packet id 0", V4, hex(0x32, 0x05, 0x00, 0x01, 't', 0x00, 0x00), ProtocolError},
		// Acknowledgements.
		{"PUBACK id 0", V5, hex(0x40, 0x02, 0x00, 0x00), ProtocolError},
		{"PUBACK bad reason", V5, hex(0x40, 0x03, 0x00, 0x01, 0x92), ProtocolError},
		{"PUBREL bad reason", V5, hex(0x62, 0x03, 0x00, 0x01, 0x10), ProtocolError},
		{"PUBCOMP truncated", V5, hex(0x70, 0x01, 0x00), MalformedPacket},
		// SUBSCRIBE family.
		{"SUBSCRIBE no filters v4", V4, hex(0x82, 0x02, 0x00, 0x01), ProtocolError},
		{"SUBSCRIBE no filters v5", V5, hex(0x82, 0x03, 0x00, 0x01, 0x00), ProtocolError},
		{"SUBSCRIBE id 0", V4, hex(0x82, 0x06, 0x00, 0x00, 0x00, 0x01, 'a', 0x00), ProtocolError},
		{"SUBSCRIBE qos 3", V4, hex(0x82, 0x06, 0x00, 0x01, 0x00, 0x01, 'a', 0x03), MalformedPacket},
		{"SUBSCRIBE reserved bits v4", V4, hex(0x82, 0x06, 0x00, 0x01, 0x00, 0x01, 'a', 0x04), MalformedPacket},
		{"SUBSCRIBE reserved bits v5", V5, hex(0x82, 0x07, 0x00, 0x01, 0x00, 0x00, 0x01, 'a', 0x40), MalformedPacket},
		{"SUBSCRIBE retain handling 3", V5, hex(0x82, 0x07, 0x00, 0x01, 0x00, 0x00, 0x01, 'a', 0x30), MalformedPacket},
		{"SUBSCRIBE no local on shared", V5, hex(0x82, 0x10, 0x00, 0x01, 0x00, 0x00, 0x0A, '$', 's', 'h', 'a', 'r', 'e', '/', 'g', '/', 'a', 0x04), ProtocolError},
		{"SUBSCRIBE option missing", V4, hex(0x82, 0x05, 0x00, 0x01, 0x00, 0x01, 'a'), MalformedPacket},
		{"SUBACK no codes", V4, hex(0x90, 0x02, 0x00, 0x01), ProtocolError},
		{"SUBACK bad code v4", V4, hex(0x90, 0x03, 0x00, 0x01, 0x03), ProtocolError},
		{"SUBACK bad code v5", V5, hex(0x90, 0x04, 0x00, 0x01, 0x00, 0x10), ProtocolError},
		{"UNSUBSCRIBE no filters", V5, hex(0xA2, 0x03, 0x00, 0x01, 0x00), ProtocolError},
		{"UNSUBSCRIBE id 0", V4, hex(0xA2, 0x05, 0x00, 0x00, 0x00, 0x01, 'a'), ProtocolError},
		{"UNSUBACK no codes v5", V5, hex(0xB0, 0x03, 0x00, 0x01, 0x00), ProtocolError},
		{"UNSUBACK bad code v5", V5, hex(0xB0, 0x04, 0x00, 0x01, 0x00, 0x01), ProtocolError},
		{"UNSUBACK leftover v4", V4, hex(0xB0, 0x03, 0x00, 0x01, 0x00), MalformedPacket},
		// DISCONNECT and AUTH.
		{"DISCONNECT body v4", V4, hex(0xE0, 0x01, 0x00), MalformedPacket},
		{"DISCONNECT bad reason", V5, hex(0xE0, 0x01, 0x84), ProtocolError},
		{"DISCONNECT prop not valid", V5, hex(0xE0, 0x05, 0x00, 0x03, 0x23, 0x00, 0x01), MalformedPacket},
		{"AUTH bad reason", V5, hex(0xF0, 0x01, 0x80), ProtocolError},
		{"AUTH leftover", V5, hex(0xF0, 0x03, 0x18, 0x00, 0x00), MalformedPacket},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var err error
			if Type(c.raw[0]>>4) == CONNECT || c.name == "first packet not CONNECT" {
				_, err = NewDecoder(bytes.NewReader(c.raw)).ReadConnect()
			} else {
				_, err = decodeRaw(c.v, c.raw)
			}
			if err == nil {
				t.Fatalf("decoded % x without error", c.raw)
			}
			if got := reasonOf(t, err); got != c.want {
				t.Errorf("got %v, want %s", err, c.want)
			}
		})
	}
}

func TestUnsupportedVersionCarriesLevel(t *testing.T) {
	_, err := NewDecoder(bytes.NewReader(connectBytes("MQTT", 7, 0x02, 0x00, 0x00))).ReadConnect()
	var e *Error
	if !errors.As(err, &e) || e.Reason != UnsupportedProtocolVersion || e.Level != 7 {
		t.Fatalf("got %v", err)
	}
}

func TestTruncatedPacketIsUnexpectedEOF(t *testing.T) {
	full := hex(0x32, 0x09, 0x00, 0x03, 'a', '/', 'b', 0x00, 0x01, 'h', 'i')
	for cut := 1; cut < len(full); cut++ {
		_, err := decodeRaw(V4, full[:cut])
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Errorf("cut at %d: got %v", cut, err)
		}
	}
	_, err := decodeRaw(V4, nil)
	if !errors.Is(err, io.EOF) {
		t.Errorf("empty stream: got %v", err)
	}
}

type countingReader struct {
	r io.Reader
	n int
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += n
	return n, err
}

func TestPacketTooLargeDoesNotReadBody(t *testing.T) {
	body := append(hex(0x00, 0x01, 't'), bytes.Repeat([]byte{0}, 997)...)
	raw := append(hex(0x30, 0xE8, 0x07), body...)
	cr := &countingReader{r: bytes.NewReader(raw)}
	d := NewDecoder(cr)
	d.Version = V4
	d.MaxPacketSize = 512
	_, err := d.Read()
	if reasonOf(t, err) != PacketTooLarge {
		t.Fatal(err)
	}
	if cr.n != 3 {
		t.Errorf("read %d bytes, want only the 3 header bytes", cr.n)
	}
	d.MaxPacketSize = 0
	_, err = decodeRawWith(V4, raw, 0)
	if err != nil {
		t.Fatalf("no limit: %v", err)
	}
}

func decodeRawWith(v Version, raw []byte, max uint32) (Packet, error) {
	d := NewDecoder(bytes.NewReader(raw))
	d.Version = v
	d.MaxPacketSize = max
	return d.Read()
}

func TestMaximumRemainingLength(t *testing.T) {
	// Four bytes of 0xFF 0xFF 0xFF 0x7F is the maximum; the body is not present so the read fails with EOF, not Malformed.
	_, err := decodeRaw(V4, hex(0x30, 0xFF, 0xFF, 0xFF, 0x7F))
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("got %v", err)
	}
}

func TestReadBeforeVersionSet(t *testing.T) {
	if _, err := NewDecoder(bytes.NewReader(hex(0xC0, 0x00))).Read(); err == nil {
		t.Fatal("expected error")
	}
	if _, err := (&Encoder{}).Bytes(&Pingreq{}); err == nil {
		t.Fatal("expected error")
	}
}

func TestVarintErratum(t *testing.T) {
	r := &reader{buf: hex(0x80, 0x80, 0x80, 0x80, 0x01)}
	if _, err := r.varint(); err == nil || err.Reason != MalformedPacket {
		t.Fatalf("got %v", err)
	}
	r = &reader{buf: hex(0xFF, 0xFF, 0xFF, 0x7F)}
	if v, err := r.varint(); err != nil || v != MaxRemainingLength {
		t.Fatalf("got %d %v", v, err)
	}
	for _, v := range []uint32{0, 127, 128, 16383, 16384, 2097151, 2097152, MaxRemainingLength} {
		var w writer
		w.varint(v)
		if len(w.buf) != varintLen(v) {
			t.Errorf("%d: encoded %d bytes, varintLen %d", v, len(w.buf), varintLen(v))
		}
		got, err := (&reader{buf: w.buf}).varint()
		if err != nil || got != v {
			t.Errorf("%d: got %d %v", v, got, err)
		}
	}
}

func TestReasonCodeStrings(t *testing.T) {
	if s := MalformedPacket.String(); s != "Malformed Packet (0x81)" {
		t.Error(s)
	}
	if s := ReasonCode(0x77).String(); s != "reason 0x77" {
		t.Error(s)
	}
	if V5.String() != "MQTT 5.0" || Version(9).String() != "protocol level 9" || Type(0).String() != "type 0" || AUTH.String() != "AUTH" {
		t.Error("names")
	}
	if (&Error{Reason: ProtocolError}).Error() != "Protocol Error (0x82)" {
		t.Error("error text")
	}
}
