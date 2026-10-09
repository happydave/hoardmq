// Package ldap is a small LDAP client: the RFC 4511 messages a directory
// authenticator needs (simple bind, search, StartTLS, unbind) over
// definite-length BER, a filter template whose placeholder can only stand for
// an assertion value, DN value escaping, and a bounded connection pool.
package ldap

import (
	"errors"
	"fmt"
)

// BER classes, as the identifier octet's top two bits.
const (
	classUniversal   byte = 0x00
	classApplication byte = 0x40
	classContext     byte = 0x80
	constructed      byte = 0x20
)

// Universal tags used here.
const (
	tagBoolean     = 1
	tagInteger     = 2
	tagOctetString = 4
	tagEnumerated  = 10
	tagSequence    = 16
	tagSet         = 17
)

// MaxMessage bounds one message read from a directory.
const MaxMessage = 1 << 20

// ErrMalformed is a BER encoding this package refuses.
var ErrMalformed = errors.New("ldap: malformed BER")

// appendLength appends a definite length: short form below 128, else the
// long form with the fewest octets.
func appendLength(b []byte, n int) []byte {
	if n < 0x80 {
		return append(b, byte(n))
	}
	var octets []byte
	for v := n; v > 0; v >>= 8 {
		octets = append([]byte{byte(v)}, octets...)
	}
	b = append(b, 0x80|byte(len(octets)))
	return append(b, octets...)
}

// element encodes one TLV. tag must be below 31: every tag LDAP uses is.
func element(class byte, cons bool, tag int, content []byte) []byte {
	id := class | byte(tag)
	if cons {
		id |= constructed
	}
	b := appendLength([]byte{id}, len(content))
	return append(b, content...)
}

func seq(parts ...[]byte) []byte { return element(classUniversal, true, tagSequence, concat(parts)) }

func concat(parts [][]byte) []byte {
	var out []byte
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

func octets(s []byte) []byte { return element(classUniversal, false, tagOctetString, s) }

func integer(tag int, class byte, v int64) []byte {
	return element(class, false, tag, intContent(v))
}

// intContent is the minimal two's-complement encoding of v.
func intContent(v int64) []byte {
	var out []byte
	for {
		out = append([]byte{byte(v)}, out...)
		if (v >= -0x80 && v < 0x80) || len(out) == 8 {
			return out
		}
		v >>= 8
	}
}

func boolean(v bool) []byte {
	if v {
		return element(classUniversal, false, tagBoolean, []byte{0xff})
	}
	return element(classUniversal, false, tagBoolean, []byte{0x00})
}

// tlv is one decoded element.
type tlv struct {
	class byte
	cons  bool
	tag   int
	value []byte
}

// readTLV decodes the first element of b and returns the rest. It refuses a
// high tag number, an indefinite length, a length of more than four octets and
// a value that runs past b.
func readTLV(b []byte) (tlv, []byte, error) {
	if len(b) < 2 {
		return tlv{}, nil, ErrMalformed
	}
	id := b[0]
	if id&0x1f == 0x1f {
		return tlv{}, nil, fmt.Errorf("%w: high tag number", ErrMalformed)
	}
	n, hdr, err := decodeLength(b[1:])
	if err != nil {
		return tlv{}, nil, err
	}
	start := 1 + hdr
	if n > len(b)-start {
		return tlv{}, nil, fmt.Errorf("%w: length past the input", ErrMalformed)
	}
	return tlv{class: id & 0xc0, cons: id&constructed != 0, tag: int(id & 0x1f), value: b[start : start+n]}, b[start+n:], nil
}

// decodeLength reads a definite length and says how many octets it took.
func decodeLength(b []byte) (n, used int, err error) {
	if len(b) == 0 {
		return 0, 0, ErrMalformed
	}
	first := b[0]
	if first < 0x80 {
		return int(first), 1, nil
	}
	count := int(first & 0x7f)
	if count == 0 {
		return 0, 0, fmt.Errorf("%w: indefinite length", ErrMalformed)
	}
	if count > 4 || len(b) < 1+count {
		return 0, 0, fmt.Errorf("%w: length octets", ErrMalformed)
	}
	for _, c := range b[1 : 1+count] {
		n = n<<8 | int(c)
	}
	if n < 0 {
		return 0, 0, ErrMalformed
	}
	return n, 1 + count, nil
}

// parseInt reads an INTEGER or ENUMERATED value of at most 8 octets.
func parseInt(v []byte) (int64, error) {
	if len(v) == 0 || len(v) > 8 {
		return 0, fmt.Errorf("%w: integer of %d octets", ErrMalformed, len(v))
	}
	n := int64(int8(v[0]))
	for _, c := range v[1:] {
		n = n<<8 | int64(c)
	}
	return n, nil
}

// expect reads the next element and checks its identity.
func expect(b []byte, class byte, cons bool, tag int) (tlv, []byte, error) {
	t, rest, err := readTLV(b)
	if err != nil {
		return t, nil, err
	}
	if t.class != class || t.cons != cons || t.tag != tag {
		return t, nil, fmt.Errorf("%w: want class %#x tag %d, got class %#x tag %d", ErrMalformed, class, tag, t.class, t.tag)
	}
	return t, rest, nil
}
