package packet

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
)

func u8(v byte) *byte      { return &v }
func u16(v uint16) *uint16 { return &v }
func u32(v uint32) *uint32 { return &v }
func str(v string) *string { return &v }
func boolp(v bool) *bool   { return &v }
func hex(b ...byte) []byte { return b }

// decodeRaw feeds a whole packet through the streaming decoder at level v.
func decodeRaw(v Version, raw []byte) (Packet, error) {
	d := NewDecoder(bytes.NewReader(raw))
	d.Version = v
	return d.Read()
}

func reasonOf(t *testing.T, err error) ReasonCode {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) {
		t.Fatalf("expected *Error, got %T %v", err, err)
	}
	return e.Reason
}

func roundTrip(t *testing.T, v Version, p Packet) Packet {
	t.Helper()
	enc := Encoder{Version: v}
	raw, err := enc.Bytes(p)
	if err != nil {
		t.Fatalf("encode %T at %s: %v", p, v, err)
	}
	var got Packet
	if c, ok := p.(*Connect); ok {
		d := NewDecoder(bytes.NewReader(raw))
		got, err = d.ReadConnect()
		if err == nil && d.Version != c.ProtocolLevel {
			t.Fatalf("decoder version %s, want %s", d.Version, c.ProtocolLevel)
		}
	} else {
		got, err = decodeRaw(v, raw)
	}
	if err != nil {
		t.Fatalf("decode %T at %s from % x: %v", p, v, raw, err)
	}
	if !reflect.DeepEqual(p, got) {
		t.Fatalf("round trip %T at %s differs\nwant %+v\ngot  %+v\nbytes % x", p, v, p, got, raw)
	}
	return got
}
