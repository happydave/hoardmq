//go:build !fips

// Package argon2 is Argon2 (RFC 9106) on the BLAKE2b (RFC 7693) it is built
// from, written on the standard library. A build with the fips tag compiles
// neither.
package argon2

import (
	"encoding/binary"
	"math/bits"
)

// blake2bIV is BLAKE2b's initialization vector (RFC 7693 § 2.6).
var blake2bIV = [8]uint64{
	0x6a09e667f3bcc908, 0xbb67ae8584caa73b, 0x3c6ef372fe94f82b, 0xa54ff53a5f1d36f1,
	0x510e527fade682d1, 0x9b05688c2b3e6c1f, 0x1f83d9abfb41bd6b, 0x5be0cd19137e2179,
}

// sigma is the message schedule (RFC 7693 § 2.7); round i uses sigma[i%10].
var sigma = [10][16]byte{
	{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
	{14, 10, 4, 8, 9, 15, 13, 6, 1, 12, 0, 2, 11, 7, 5, 3},
	{11, 8, 12, 0, 5, 2, 15, 13, 10, 14, 3, 6, 7, 1, 9, 4},
	{7, 9, 3, 1, 13, 12, 11, 14, 2, 6, 5, 10, 4, 0, 15, 8},
	{9, 0, 5, 7, 2, 4, 10, 15, 14, 1, 11, 12, 6, 8, 3, 13},
	{2, 12, 6, 10, 0, 11, 8, 3, 4, 13, 7, 5, 15, 14, 1, 9},
	{12, 5, 1, 15, 14, 13, 4, 10, 0, 7, 6, 3, 9, 2, 8, 11},
	{13, 11, 7, 14, 12, 1, 3, 9, 5, 0, 15, 4, 8, 6, 2, 10},
	{6, 15, 14, 9, 11, 3, 0, 8, 12, 2, 13, 7, 1, 4, 10, 5},
	{10, 2, 8, 4, 7, 6, 1, 5, 15, 11, 9, 14, 3, 12, 13, 0},
}

const blake2bBlock = 128

// blake2b is an unkeyed BLAKE2b hash with an output of 1 to 64 bytes.
type blake2b struct {
	h    [8]uint64
	t    [2]uint64 // bytes compressed so far
	buf  [blake2bBlock]byte
	n    int // bytes held in buf
	size int
}

func newBlake2b(size int) *blake2b {
	if size < 1 || size > 64 {
		panic("argon2: BLAKE2b output length out of range")
	}
	d := &blake2b{h: blake2bIV, size: size}
	d.h[0] ^= 0x01010000 ^ uint64(size)
	return d
}

// write absorbs p. The last block is held back so the final compression can
// be marked as the last.
func (d *blake2b) write(p []byte) {
	for len(p) > 0 {
		if d.n == blake2bBlock {
			d.count(blake2bBlock)
			d.compress(false)
			d.n = 0
		}
		k := copy(d.buf[d.n:], p)
		d.n += k
		p = p[k:]
	}
}

func (d *blake2b) count(n int) {
	d.t[0] += uint64(n)
	if d.t[0] < uint64(n) {
		d.t[1]++
	}
}

// sum finishes the hash and returns the digest.
func (d *blake2b) sum() []byte {
	d.count(d.n)
	clear(d.buf[d.n:])
	d.compress(true)
	var out [64]byte
	for i, v := range d.h {
		binary.LittleEndian.PutUint64(out[8*i:], v)
	}
	return out[:d.size]
}

func (d *blake2b) compress(last bool) {
	var m [16]uint64
	for i := range m {
		m[i] = binary.LittleEndian.Uint64(d.buf[8*i:])
	}
	var v [16]uint64
	copy(v[:8], d.h[:])
	copy(v[8:], blake2bIV[:])
	v[12] ^= d.t[0]
	v[13] ^= d.t[1]
	if last {
		v[14] = ^v[14]
	}
	for r := 0; r < 12; r++ {
		s := &sigma[r%10]
		mix(&v, 0, 4, 8, 12, m[s[0]], m[s[1]])
		mix(&v, 1, 5, 9, 13, m[s[2]], m[s[3]])
		mix(&v, 2, 6, 10, 14, m[s[4]], m[s[5]])
		mix(&v, 3, 7, 11, 15, m[s[6]], m[s[7]])
		mix(&v, 0, 5, 10, 15, m[s[8]], m[s[9]])
		mix(&v, 1, 6, 11, 12, m[s[10]], m[s[11]])
		mix(&v, 2, 7, 8, 13, m[s[12]], m[s[13]])
		mix(&v, 3, 4, 9, 14, m[s[14]], m[s[15]])
	}
	for i := range d.h {
		d.h[i] ^= v[i] ^ v[i+8]
	}
}

// mix is BLAKE2b's G (RFC 7693 § 3.1).
func mix(v *[16]uint64, a, b, c, d int, x, y uint64) {
	v[a] = v[a] + v[b] + x
	v[d] = bits.RotateLeft64(v[d]^v[a], -32)
	v[c] = v[c] + v[d]
	v[b] = bits.RotateLeft64(v[b]^v[c], -24)
	v[a] = v[a] + v[b] + y
	v[d] = bits.RotateLeft64(v[d]^v[a], -16)
	v[c] = v[c] + v[d]
	v[b] = bits.RotateLeft64(v[b]^v[c], -63)
}

// blake2bSum is the digest of the concatenation of parts, size bytes long.
func blake2bSum(size int, parts ...[]byte) []byte {
	d := newBlake2b(size)
	for _, p := range parts {
		d.write(p)
	}
	return d.sum()
}

// hashLong is Argon2's variable-length hash H' (RFC 9106 § 3.3): up to 64
// bytes it is one BLAKE2b of the length and the input; longer, a chain of
// 64-byte BLAKE2b outputs of which the first 32 bytes of each are kept.
func hashLong(size int, parts ...[]byte) []byte {
	var le [4]byte
	binary.LittleEndian.PutUint32(le[:], uint32(size))
	in := append([][]byte{le[:]}, parts...)
	if size <= 64 {
		return blake2bSum(size, in...)
	}
	out := make([]byte, 0, size)
	v := blake2bSum(64, in...)
	r := (size+31)/32 - 2
	for i := 1; i < r; i++ {
		out = append(out, v[:32]...)
		v = blake2bSum(64, v)
	}
	out = append(out, v[:32]...)
	return append(out, blake2bSum(size-32*r, v)...)
}
