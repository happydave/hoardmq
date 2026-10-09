package packet

import (
	"sync"
	"unicode/utf8"
)

// MaxRemainingLength is the largest Remaining Length the Variable Byte
// Integer encoding can carry (four bytes).
const MaxRemainingLength = 268435455

// MaxPacketSize is the largest packet on the wire: five header bytes plus the
// maximum Remaining Length.
const MaxPacketSize = MaxRemainingLength + 5

// reader walks a packet body held in memory. Every method reports overrun as
// Malformed Packet, so the packet decoders read without bounds arithmetic.
type reader struct {
	buf []byte
	pos int
}

func (r *reader) remaining() int { return len(r.buf) - r.pos }

func (r *reader) byte() (byte, *Error) {
	if r.pos >= len(r.buf) {
		return 0, malformed("packet body ends inside a field")
	}
	b := r.buf[r.pos]
	r.pos++
	return b, nil
}

func (r *reader) uint16() (uint16, *Error) {
	if r.remaining() < 2 {
		return 0, malformed("packet body ends inside a two byte integer")
	}
	v := uint16(r.buf[r.pos])<<8 | uint16(r.buf[r.pos+1])
	r.pos += 2
	return v, nil
}

func (r *reader) uint32() (uint32, *Error) {
	if r.remaining() < 4 {
		return 0, malformed("packet body ends inside a four byte integer")
	}
	b := r.buf[r.pos:]
	v := uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
	r.pos += 4
	return v, nil
}

// varint decodes a Variable Byte Integer with the range check ahead of the
// multiplier update, as the 3.1.1 erratum has it.
func (r *reader) varint() (uint32, *Error) {
	var value uint32
	multiplier := uint32(1)
	for {
		if multiplier > 128*128*128 {
			return 0, malformed("variable byte integer longer than four bytes")
		}
		b, err := r.byte()
		if err != nil {
			return 0, err
		}
		value += uint32(b&0x7F) * multiplier
		if b&0x80 == 0 {
			return value, nil
		}
		multiplier *= 128
	}
}

func (r *reader) bytes() ([]byte, *Error) {
	n, err := r.uint16()
	if err != nil {
		return nil, err
	}
	if r.remaining() < int(n) {
		return nil, malformed("binary data length %d overruns the packet", n)
	}
	out := make([]byte, n)
	copy(out, r.buf[r.pos:r.pos+int(n)])
	r.pos += int(n)
	return out, nil
}

func (r *reader) string() (string, *Error) {
	n, err := r.uint16()
	if err != nil {
		return "", err
	}
	if r.remaining() < int(n) {
		return "", malformed("string length %d overruns the packet", n)
	}
	s := r.buf[r.pos : r.pos+int(n)]
	r.pos += int(n)
	if !validUTF8(s) {
		return "", malformed("string is not well-formed UTF-8")
	}
	return string(s), nil
}

// rest returns the unread remainder as a copy.
func (r *reader) rest() []byte {
	out := make([]byte, r.remaining())
	copy(out, r.buf[r.pos:])
	r.pos = len(r.buf)
	return out
}

// validUTF8 applies MQTT 1.5.4: well-formed UTF-8 with no U+0000 and no
// surrogate code points. utf8.Valid already rejects encoded surrogates.
func validUTF8(s []byte) bool {
	if !utf8.Valid(s) {
		return false
	}
	for _, b := range s {
		if b == 0 {
			return false
		}
	}
	return true
}

// writer builds a packet body in a growable buffer.
type writer struct {
	buf []byte
}

func (w *writer) byte(b byte)     { w.buf = append(w.buf, b) }
func (w *writer) uint16(v uint16) { w.buf = append(w.buf, byte(v>>8), byte(v)) }
func (w *writer) uint32(v uint32) {
	w.buf = append(w.buf, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func (w *writer) varint(v uint32) {
	for {
		b := byte(v % 128)
		v /= 128
		if v > 0 {
			b |= 0x80
		}
		w.buf = append(w.buf, b)
		if v == 0 {
			return
		}
	}
}

func (w *writer) bytes(b []byte) {
	w.uint16(uint16(len(b)))
	w.buf = append(w.buf, b...)
}

func (w *writer) string(s string) {
	w.uint16(uint16(len(s)))
	w.buf = append(w.buf, s...)
}

func varintLen(v uint32) int {
	switch {
	case v < 128:
		return 1
	case v < 16384:
		return 2
	case v < 2097152:
		return 3
	}
	return 4
}

// Body buffers are pooled by size class so that a connection holds no buffer
// while idle and a busy connection reuses one.
var sizeClasses = []int{256, 4096, 65536, 1 << 20}

var pools = func() []*sync.Pool {
	ps := make([]*sync.Pool, len(sizeClasses))
	for i, n := range sizeClasses {
		size := n
		ps[i] = &sync.Pool{New: func() any {
			b := make([]byte, size)
			return &b
		}}
	}
	return ps
}()

// acquire returns a buffer of at least n bytes and the pool index it came
// from, or -1 when n exceeds every size class and the buffer is a plain
// allocation.
func acquire(n int) ([]byte, int) {
	for i, size := range sizeClasses {
		if n <= size {
			b := pools[i].Get().(*[]byte)
			return (*b)[:n], i
		}
	}
	return make([]byte, n), -1
}

func release(b []byte, class int) {
	if class < 0 {
		return
	}
	b = b[:cap(b)]
	pools[class].Put(&b)
}
