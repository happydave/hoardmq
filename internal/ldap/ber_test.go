package ldap

import (
	"bytes"
	"encoding/asn1"
	"encoding/hex"
	"errors"
	"testing"
)

// The requests are checked against encoding/asn1 marshalling RFC 4511's ASN.1
// directly, an encoder that shares nothing with this package's.

type asnMessage struct {
	ID int
	Op asn1.RawValue
}

func asnWrap(t *testing.T, id int, op []byte) []byte {
	t.Helper()
	b, err := asn1.Marshal(asnMessage{ID: id, Op: asn1.RawValue{FullBytes: op}})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestBindRequestMatchesASN1(t *testing.T) {
	type simpleBind struct {
		Version int
		Name    []byte
		Simple  []byte `asn1:"tag:0"`
	}
	op, err := asn1.MarshalWithParams(simpleBind{Version: 3, Name: []byte("cn=svc,dc=x"), Simple: []byte("pw")}, "application,tag:0")
	if err != nil {
		t.Fatal(err)
	}
	want := asnWrap(t, 1, op)
	got := BindRequest(1, "cn=svc,dc=x", []byte("pw"))
	if !bytes.Equal(got, want) {
		t.Fatalf("bind request\n got %x\nwant %x", got, want)
	}
	byHand, _ := hex.DecodeString("3019020101601402010304" + "0b636e3d7376632c64633d78" + "80027077")
	if !bytes.Equal(got, byHand) {
		t.Fatalf("bind request %x, want the hand-derived %x", got, byHand)
	}
}

func TestSearchRequestMatchesASN1(t *testing.T) {
	type ava struct{ Attr, Value []byte }
	eq := func(a, v string) []byte {
		b, err := asn1.MarshalWithParams(ava{[]byte(a), []byte(v)}, "tag:3")
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	and, err := asn1.Marshal(asn1.RawValue{Class: asn1.ClassContextSpecific, Tag: 0, IsCompound: true,
		Bytes: append(eq("objectClass", "person"), eq("uid", "alice")...)})
	if err != nil {
		t.Fatal(err)
	}
	type search struct {
		Base      []byte
		Scope     asn1.Enumerated
		Deref     asn1.Enumerated
		Size      int
		Time      int
		TypesOnly bool
		Filter    asn1.RawValue
		Attrs     [][]byte
	}
	op, err := asn1.MarshalWithParams(search{Base: []byte("ou=people,dc=x"), Scope: 2, Size: 2, Time: 5,
		Filter: asn1.RawValue{FullBytes: and}, Attrs: [][]byte{[]byte("1.1")}}, "application,tag:3")
	if err != nil {
		t.Fatal(err)
	}
	want := asnWrap(t, 7, op)
	f, err := ParseTemplate("(&(objectClass=person)(uid=${username}))")
	if err != nil {
		t.Fatal(err)
	}
	got := SearchRequest(7, "ou=people,dc=x", ScopeSub, 2, 5, f.Encode([]byte("alice")))
	if !bytes.Equal(got, want) {
		t.Fatalf("search request\n got %x\nwant %x", got, want)
	}
}

func TestStartTLSAndUnbindMatchASN1(t *testing.T) {
	type ext struct {
		Name []byte `asn1:"tag:0"`
	}
	op, err := asn1.MarshalWithParams(ext{Name: []byte("1.3.6.1.4.1.1466.20037")}, "application,tag:23")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := StartTLSRequest(3), asnWrap(t, 3, op); !bytes.Equal(got, want) {
		t.Fatalf("StartTLS\n got %x\nwant %x", got, want)
	}
	if got, want := UnbindRequest(4), []byte{0x30, 0x05, 0x02, 0x01, 0x04, 0x42, 0x00}; !bytes.Equal(got, want) {
		t.Fatalf("unbind %x, want %x", got, want)
	}
}

func TestLengths(t *testing.T) {
	for _, c := range []struct {
		n    int
		want []byte
	}{{0, []byte{0}}, {127, []byte{0x7f}}, {128, []byte{0x81, 0x80}}, {255, []byte{0x81, 0xff}}, {256, []byte{0x82, 0x01, 0x00}}, {70000, []byte{0x83, 0x01, 0x11, 0x70}}} {
		if got := appendLength(nil, c.n); !bytes.Equal(got, c.want) {
			t.Errorf("length %d: %x, want %x", c.n, got, c.want)
		}
		n, used, err := decodeLength(c.want)
		if err != nil || n != c.n || used != len(c.want) {
			t.Errorf("decode %x: %d %d %v", c.want, n, used, err)
		}
	}
	for _, v := range []int64{0, 127, 128, -1, -128, -129, 256, 1 << 40} {
		b := intContent(v)
		want, _ := asn1.Marshal(v)
		if !bytes.Equal(integer(tagInteger, classUniversal, v), want) {
			t.Errorf("integer %d: %x, want %x", v, integer(tagInteger, classUniversal, v), want)
		}
		if got, err := parseInt(b); err != nil || got != v {
			t.Errorf("integer %d round trip: %d %v", v, got, err)
		}
	}
}

func TestDecoderRefuses(t *testing.T) {
	for name, b := range map[string][]byte{
		"indefinite length":      {0x30, 0x80, 0x00, 0x00},
		"length past input":      {0x30, 0x0a, 0x02, 0x01, 0x01, 0x61, 0x03, 0x0a, 0x01, 0x00},
		"five length octets":     {0x30, 0x85, 0, 0, 0, 0, 1},
		"high tag number":        {0x1f, 0x01, 0x00},
		"nine-octet integer":     {0x30, 0x0b, 0x02, 0x09, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		"trailing after message": append(append([]byte{}, bindResponse(1, 0)...), 0x00),
	} {
		if _, err := ParseResponse(b); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: %v, want ErrMalformed", name, err)
		}
	}
}

// Each refusal of the element decoder on its own, where no later check could
// refuse the same bytes.
func TestElementDecoderRefuses(t *testing.T) {
	if _, _, err := decodeLength([]byte{0x80}); !errors.Is(err, ErrMalformed) {
		t.Errorf("indefinite length: %v", err)
	}
	if _, _, err := decodeLength([]byte{0x85, 0, 0, 0, 0, 1}); !errors.Is(err, ErrMalformed) {
		t.Errorf("five length octets: %v", err)
	}
	if _, err := parseInt([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9}); !errors.Is(err, ErrMalformed) {
		t.Errorf("nine-octet integer: %v", err)
	}
	if _, _, err := readTLV([]byte{0x1f, 0x01, 0x00}); !errors.Is(err, ErrMalformed) {
		t.Errorf("high tag number: %v", err)
	}
}

func bindResponse(id, code int) []byte {
	op := element(classApplication, true, opBindResponse, concat([][]byte{integer(tagEnumerated, classUniversal, int64(code)), octets(nil), octets(nil)}))
	return message(id, op)
}

func TestParseResponses(t *testing.T) {
	r, err := ParseResponse(bindResponse(9, 49))
	if err != nil || r.ID != 9 || r.Op != opBindResponse || r.Code != 49 {
		t.Fatalf("bind response %+v %v", r, err)
	}
	entry := message(2, element(classApplication, true, opSearchEntry, concat([][]byte{octets([]byte("uid=a,dc=x")), seq()})))
	if r, err := ParseResponse(entry); err != nil || r.DN != "uid=a,dc=x" || r.Op != opSearchEntry {
		t.Fatalf("entry %+v %v", r, err)
	}
	notice := message(0, element(classApplication, true, opExtendedResult, concat([][]byte{integer(tagEnumerated, classUniversal, 52), octets(nil), octets(nil), element(classContext, false, 10, []byte(noticeOfDisconnectN))})))
	if r, err := ParseResponse(notice); err != nil || r.ID != 0 || r.Code != 52 || r.Name != noticeOfDisconnectN {
		t.Fatalf("notice %+v %v", r, err)
	}
}

func TestReadMessageRefusesOversize(t *testing.T) {
	big := append([]byte{0x30, 0x83, 0x20, 0x00, 0x00}, make([]byte, 16)...)
	if _, err := readMessage(bufioReader(big), MaxMessage); !errors.Is(err, ErrMalformed) {
		t.Fatalf("a 2 MiB message: %v", err)
	}
	ok := bindResponse(1, 0)
	if got, err := readMessage(bufioReader(ok), MaxMessage); err != nil || !bytes.Equal(got, ok) {
		t.Fatalf("read %x %v", got, err)
	}
}
