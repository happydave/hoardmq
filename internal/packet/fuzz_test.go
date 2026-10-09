package packet

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"testing"
)

func fuzzSeeds(f *testing.F, v Version) {
	for _, g := range goldens() {
		if g.v == v || v == 0 {
			f.Add(g.raw)
		}
	}
	for _, p := range samplePackets(v) {
		if raw, err := (&Encoder{Version: v}).Bytes(p); err == nil {
			f.Add(raw)
		}
	}
}

// fuzzDecode checks that arbitrary bytes never panic or hang, and that a
// packet the decoder accepts survives encode and decode unchanged.
func fuzzDecode(t *testing.T, v Version, raw []byte) {
	d := NewDecoder(bytes.NewReader(raw))
	d.Version = v
	p, err := d.Read()
	if err != nil {
		var e *Error
		if !errors.As(err, &e) && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("unexpected error type %T: %v", err, err)
		}
		return
	}
	enc, err := (&Encoder{Version: v}).Bytes(p)
	if err != nil {
		t.Fatalf("re-encode of accepted packet failed: %v (%+v)", err, p)
	}
	again, err := decodeRaw(v, enc)
	if err != nil {
		t.Fatalf("re-decode failed: %v\nfirst % x\nre-encoded % x", err, raw, enc)
	}
	if !reflect.DeepEqual(p, again) {
		t.Fatalf("packet changed across encode/decode\nfirst %+v\nagain %+v", p, again)
	}
}

func FuzzDecodeV5(f *testing.F) {
	fuzzSeeds(f, V5)
	f.Fuzz(func(t *testing.T, raw []byte) { fuzzDecode(t, V5, raw) })
}

func FuzzDecodeV4(f *testing.F) {
	fuzzSeeds(f, V4)
	f.Fuzz(func(t *testing.T, raw []byte) { fuzzDecode(t, V4, raw) })
}

func FuzzDecodeV3(f *testing.F) {
	fuzzSeeds(f, V3)
	f.Fuzz(func(t *testing.T, raw []byte) { fuzzDecode(t, V3, raw) })
}

func FuzzConnect(f *testing.F) {
	for _, g := range goldens() {
		if g.pkt.Type() == CONNECT {
			f.Add(g.raw)
		}
	}
	for _, v := range []Version{V3, V4, V5} {
		for _, p := range samplePackets(v) {
			if p.Type() == CONNECT {
				raw, _ := (&Encoder{Version: v}).Bytes(p)
				f.Add(raw)
			}
		}
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		c, err := NewDecoder(bytes.NewReader(raw)).ReadConnect()
		if err != nil {
			var e *Error
			if !errors.As(err, &e) && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("unexpected error type %T: %v", err, err)
			}
			return
		}
		enc, err := (&Encoder{Version: c.ProtocolLevel}).Bytes(c)
		if err != nil {
			t.Fatalf("re-encode: %v", err)
		}
		d := NewDecoder(bytes.NewReader(enc))
		again, err := d.ReadConnect()
		if err != nil {
			t.Fatalf("re-decode: %v", err)
		}
		if d.Version != c.ProtocolLevel || !reflect.DeepEqual(c, again) {
			t.Fatalf("CONNECT changed\nfirst %+v\nagain %+v", c, again)
		}
	})
}
