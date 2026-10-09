//go:build !fips

package argon2

import (
	"encoding/binary"
	"math/bits"
	"sync"
)

// Version is the Argon2 version this package computes, 0x13 (19).
const Version = 0x13

// Argon2 types, as the y input of H0 names them.
const (
	argon2d  = 0
	argon2i  = 1
	argon2id = 2
)

const (
	blockWords = 128 // 64-bit words in a 1 KiB block
	syncPoints = 4   // slices per pass
	addrPerBlk = blockWords
)

type block [blockWords]uint64

// IDKey derives keyLen bytes from the password and salt with Argon2id: time
// passes over memory KiB in threads lanes. The caller bounds the parameters;
// memory is rounded down to a multiple of 4×threads, and must be at least
// 8×threads.
func IDKey(password, salt []byte, time, memory uint32, threads uint8, keyLen uint32) []byte {
	return derive(argon2id, password, salt, nil, nil, time, memory, uint32(threads), keyLen)
}

func derive(mode int, password, salt, secret, data []byte, time, memory, lanes, keyLen uint32) []byte {
	if time < 1 || lanes < 1 || memory < 8*lanes || keyLen < 4 {
		panic("argon2: parameters out of range")
	}
	h0 := initialHash(mode, password, salt, secret, data, time, memory, lanes, keyLen)
	blocks := memory / (syncPoints * lanes) * (syncPoints * lanes)
	laneLen := blocks / lanes
	segLen := laneLen / syncPoints
	B := make([]block, blocks)

	// The first two blocks of every lane.
	var le [8]byte
	for l := uint32(0); l < lanes; l++ {
		for j := uint32(0); j < 2; j++ {
			binary.LittleEndian.PutUint32(le[:4], j)
			binary.LittleEndian.PutUint32(le[4:], l)
			bytesToBlock(&B[l*laneLen+j], hashLong(1024, h0, le[:]))
		}
	}

	for pass := uint32(0); pass < time; pass++ {
		for slice := uint32(0); slice < syncPoints; slice++ {
			// Within a slice the lanes read only blocks of earlier slices
			// from each other, so they run at once.
			var wg sync.WaitGroup
			for l := uint32(0); l < lanes; l++ {
				wg.Add(1)
				go func(l uint32) {
					defer wg.Done()
					fillSegment(B, mode, pass, slice, l, lanes, laneLen, segLen, blocks, time)
				}(l)
			}
			wg.Wait()
		}
	}

	var c block
	for l := uint32(0); l < lanes; l++ {
		last := &B[l*laneLen+laneLen-1]
		for i := range c {
			c[i] ^= last[i]
		}
	}
	var cb [1024]byte
	for i, w := range c {
		binary.LittleEndian.PutUint64(cb[8*i:], w)
	}
	return hashLong(int(keyLen), cb[:])
}

// initialHash is H0 (RFC 9106 § 3.2).
func initialHash(mode int, password, salt, secret, data []byte, time, memory, lanes, keyLen uint32) []byte {
	d := newBlake2b(64)
	var le [4]byte
	u32 := func(v uint32) {
		binary.LittleEndian.PutUint32(le[:], v)
		d.write(le[:])
	}
	for _, v := range []uint32{lanes, keyLen, memory, time, Version, uint32(mode)} {
		u32(v)
	}
	for _, p := range [][]byte{password, salt, secret, data} {
		u32(uint32(len(p)))
		d.write(p)
	}
	return d.sum()
}

func bytesToBlock(b *block, p []byte) {
	for i := range b {
		b[i] = binary.LittleEndian.Uint64(p[8*i:])
	}
}

// fillSegment computes one lane's blocks in one slice of one pass.
func fillSegment(B []block, mode int, pass, slice, lane, lanes, laneLen, segLen, blocks, time uint32) {
	independent := mode == argon2i || (mode == argon2id && pass == 0 && slice < syncPoints/2)
	var addrIn, addr, zero block
	if independent {
		addrIn[0], addrIn[1], addrIn[2] = uint64(pass), uint64(lane), uint64(slice)
		addrIn[3], addrIn[4], addrIn[5] = uint64(blocks), uint64(time), uint64(mode)
	}
	nextAddresses := func() {
		addrIn[6]++
		compress(&addr, &zero, &addrIn, false)
		compress(&addr, &zero, &addr, false)
	}
	start := uint32(0)
	if pass == 0 && slice == 0 {
		start = 2 // the first two blocks are H' of H0
	}
	if independent && start != 0 {
		nextAddresses()
	}
	offset := lane*laneLen + slice*segLen + start
	for i := start; i < segLen; i, offset = i+1, offset+1 {
		prev := offset - 1
		if offset%laneLen == 0 {
			prev = offset + laneLen - 1
		}
		var rand uint64
		if independent {
			if i%addrPerBlk == 0 {
				nextAddresses()
			}
			rand = addr[i%addrPerBlk]
		} else {
			rand = B[prev][0]
		}
		refLane := uint32(rand>>32) % lanes
		if pass == 0 && slice == 0 {
			refLane = lane
		}
		ref := refLane*laneLen + refIndex(pass, slice, i, uint32(rand), refLane == lane, laneLen, segLen)
		compress(&B[offset], &B[prev], &B[ref], pass > 0)
	}
}

// refIndex maps J1 to a block of the reference lane (RFC 9106 § 3.4.2): the
// area that may be referenced, then a position within it biased towards the
// most recent blocks.
func refIndex(pass, slice, index, j1 uint32, sameLane bool, laneLen, segLen uint32) uint32 {
	var area uint32
	switch {
	case pass == 0 && slice == 0:
		area = index - 1
	case pass == 0 && sameLane:
		area = slice*segLen + index - 1
	case pass == 0:
		area = slice * segLen
		if index == 0 {
			area--
		}
	case sameLane:
		area = laneLen - segLen + index - 1
	default:
		area = laneLen - segLen
		if index == 0 {
			area--
		}
	}
	x := uint64(j1) * uint64(j1) >> 32
	rel := uint64(area) - 1 - (uint64(area) * x >> 32)
	startPos := uint32(0)
	if pass != 0 && slice != syncPoints-1 {
		startPos = (slice + 1) * segLen
	}
	return uint32((uint64(startPos) + rel) % uint64(laneLen))
}

// compress is Argon2's G: dst = P(x ⊕ y) ⊕ (x ⊕ y), and with xor it is laid
// over dst's previous content (passes after the first, version 0x13).
func compress(dst, x, y *block, xor bool) {
	var r, q block
	for i := range r {
		r[i] = x[i] ^ y[i]
	}
	q = r
	for i := 0; i < blockWords; i += 16 {
		permute(&q[i], &q[i+1], &q[i+2], &q[i+3], &q[i+4], &q[i+5], &q[i+6], &q[i+7],
			&q[i+8], &q[i+9], &q[i+10], &q[i+11], &q[i+12], &q[i+13], &q[i+14], &q[i+15])
	}
	for i := 0; i < 16; i += 2 {
		permute(&q[i], &q[i+1], &q[16+i], &q[16+i+1], &q[32+i], &q[32+i+1], &q[48+i], &q[48+i+1],
			&q[64+i], &q[64+i+1], &q[80+i], &q[80+i+1], &q[96+i], &q[96+i+1], &q[112+i], &q[112+i+1])
	}
	if xor {
		for i := range dst {
			dst[i] ^= q[i] ^ r[i]
		}
		return
	}
	for i := range dst {
		dst[i] = q[i] ^ r[i]
	}
}

// permute is Argon2's P over eight 16-byte registers: BLAKE2b's round with
// the multiplication-hardened G (RFC 9106 § 3.6).
func permute(v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15 *uint64) {
	gb(v0, v4, v8, v12)
	gb(v1, v5, v9, v13)
	gb(v2, v6, v10, v14)
	gb(v3, v7, v11, v15)
	gb(v0, v5, v10, v15)
	gb(v1, v6, v11, v12)
	gb(v2, v7, v8, v13)
	gb(v3, v4, v9, v14)
}

func gb(a, b, c, d *uint64) {
	fb := func(x, y uint64) uint64 { return x + y + 2*uint64(uint32(x))*uint64(uint32(y)) }
	*a = fb(*a, *b)
	*d = bits.RotateLeft64(*d^*a, -32)
	*c = fb(*c, *d)
	*b = bits.RotateLeft64(*b^*c, -24)
	*a = fb(*a, *b)
	*d = bits.RotateLeft64(*d^*a, -16)
	*c = fb(*c, *d)
	*b = bits.RotateLeft64(*b^*c, -63)
}
