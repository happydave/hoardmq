package wal

import (
	"encoding/binary"
	"errors"
	"os"
	"runtime"
	"testing"
)

// recordOffsets lists the byte offset of each record in a segment written by
// appendN, in order.
func recordOffsets(t *testing.T, path string) []int64 {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var out []int64
	for off := int64(headerSize); off+recordHeader <= int64(len(b)); {
		out = append(out, off)
		off += recordHeader + int64(binary.BigEndian.Uint32(b[off:off+4]))
	}
	return out
}

// setLength overwrites the length field of the record at off.
func setLength(t *testing.T, path string, off int64, length uint32) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], length)
	if _, err := f.WriteAt(b[:], off); err != nil {
		t.Fatal(err)
	}
}

// TestCorruptedLengthIsNotATornTail: a synced record in the last segment whose
// length field is damaged so that it reaches past the end of the file is
// corruption, not a torn write: Open fails naming the segment, the offset and
// the record, and the records after it stay on the disk.
func TestCorruptedLengthIsNotATornTail(t *testing.T) {
	cases := []struct {
		name   string
		record int // 1-based; the damaged record
		length func(orig uint32) uint32
	}{
		{"middle record, just past the end", 3, func(o uint32) uint32 { return o + 1<<16 }},
		{"middle record, near 4 GiB", 3, func(uint32) uint32 { return 0xFFFFFFF0 }},
		{"last record, just past the end", 8, func(o uint32) uint32 { return o + 1 }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			l := open(t, dir, Options{})
			appendN(t, l, 1, 8)
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}
			path := lastSegment(t, dir)
			offs := recordOffsets(t, path)
			if len(offs) != 8 {
				t.Fatalf("precondition: %d records in the segment, want 8", len(offs))
			}
			off := offs[c.record-1]
			b, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			setLength(t, path, off, c.length(binary.BigEndian.Uint32(b[off:off+4])))
			before, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}

			re, err := Open(dir, "state", Options{Logger: quiet()})
			if err == nil {
				last := re.LastIndex()
				_ = re.Close()
				t.Fatalf("opened with last index %d over a damaged length at record %d; want corruption", last, c.record)
			}
			var ce *CorruptError
			if !errors.As(err, &ce) || ce.Index != uint64(c.record) || ce.Offset != off {
				t.Fatalf("got %v, want a CorruptError at record %d, offset %d", err, c.record, off)
			}
			after, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if after.Size() != before.Size() {
				t.Fatalf("the segment changed size over a failed open: %d bytes, was %d", after.Size(), before.Size())
			}
		})
	}
}

// record encodes one record as Append writes it.
func record(typ byte, payload []byte) []byte {
	b := make([]byte, recordHeader, recordHeader+len(payload))
	binary.BigEndian.PutUint32(b[0:4], uint32(len(payload)))
	b[4] = typ
	binary.BigEndian.PutUint32(b[5:9], crc(typ, payload))
	return append(b, payload...)
}

func TestLengthDamaged(t *testing.T) {
	a, b := []byte("payload-a"), []byte("payload-b")
	sum := crc(3, a)
	cases := []struct {
		name string
		rest []byte
		want bool
	}{
		{"whole record at the end of the file", a, true},
		{"whole record followed by a sound record", append(append([]byte{}, a...), record(4, b)...), true},
		{"whole record followed by a cut record", append(append([]byte{}, a...), record(4, b)[:12]...), false},
		{"cut record", a[:5], false},
		{"no bytes", nil, false},
		{"other bytes", []byte("something else entirely"), false},
	}
	for _, c := range cases {
		if got := lengthDamaged(3, sum, c.rest); got != c.want {
			t.Errorf("%s: lengthDamaged = %v, want %v", c.name, got, c.want)
		}
	}
	// A record of length 0 matches at once: its checksum is over its type.
	if !lengthDamaged(5, crc(5, nil), record(4, b)) {
		t.Error("an empty record followed by a sound record: not damaged")
	}
}

func TestSoundRecord(t *testing.T) {
	r := record(2, []byte("abc"))
	if !soundRecord(r) || !soundRecord(append(r, 0xFF)) {
		t.Fatal("a whole record is not sound")
	}
	bad := append([]byte{}, r...)
	bad[len(bad)-1] ^= 1
	for name, b := range map[string][]byte{"cut header": r[:5], "cut payload": r[:len(r)-1], "bad checksum": bad} {
		if soundRecord(b) {
			t.Errorf("%s: sound", name)
		}
	}
}

// TestLongTornRecordIsStillTruncated: a crash can cut a large record after
// its first buffer, leaving its header far from the end of the file. That is
// a torn tail, not a damaged length, and Open truncates it.
func TestLongTornRecordIsStillTruncated(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, Options{})
	appendN(t, l, 1, 3)
	big := make([]byte, 200<<10)
	for i := range big {
		big[i] = byte(i * 7)
	}
	if _, err := l.Append(9, big); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	path := lastSegment(t, dir)
	offs := recordOffsets(t, path)
	cut := offs[3] + recordHeader + 70<<10
	if err := os.Truncate(path, cut); err != nil {
		t.Fatal(err)
	}
	re, err := Open(dir, "state", Options{Logger: quiet()})
	if err != nil {
		t.Fatalf("open over a long torn record: %v", err)
	}
	defer func() { _ = re.Close() }()
	if re.LastIndex() != 3 {
		t.Fatalf("last index %d, want 3", re.LastIndex())
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != offs[3] {
		t.Fatalf("segment size after open %d, want %d", info.Size(), offs[3])
	}
}

// TestDamagedLengthAllocatesNoMoreThanTheFile: a length over the maximum, or
// under it and past the end of a small segment, is refused without being
// allocated.
func TestDamagedLengthAllocatesNoMoreThanTheFile(t *testing.T) {
	for _, length := range []uint32{0xFFFFFFF0, 1 << 29} {
		dir := t.TempDir()
		l := open(t, dir, Options{})
		appendN(t, l, 1, 8)
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}
		path := lastSegment(t, dir)
		setLength(t, path, recordOffsets(t, path)[2], length)
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		_, err := Open(dir, "state", Options{Logger: quiet()})
		runtime.ReadMemStats(&after)
		if err == nil {
			t.Fatalf("length %d: opened over a damaged length", length)
		}
		if got := after.TotalAlloc - before.TotalAlloc; got > 1<<20 {
			info, _ := os.Stat(path)
			t.Fatalf("length %d: Open allocated %d bytes over a segment of %d bytes", length, got, info.Size())
		}
	}
}

// TestAppendRefusesARecordOverTheMaximum: nothing is written for a payload
// over the maximum record size.
func TestAppendRefusesARecordOverTheMaximum(t *testing.T) {
	saved := maxRecordBytes
	maxRecordBytes = 64
	t.Cleanup(func() { maxRecordBytes = saved })
	l := open(t, t.TempDir(), Options{})
	defer func() { _ = l.Close() }()
	if _, err := l.Append(1, make([]byte, 64)); err != nil {
		t.Fatalf("a record at the maximum: %v", err)
	}
	if _, err := l.Append(1, make([]byte, 65)); err == nil {
		t.Fatal("a record over the maximum was appended")
	}
	if l.LastIndex() != 1 {
		t.Fatalf("last index %d, want 1", l.LastIndex())
	}
	if got := collect(t, l, 1); len(got) != 1 {
		t.Fatalf("%d records read back, want 1", len(got))
	}
}

// TestLengthOverTheMaximumIsCorruption: a length above the maximum is damage
// even where the file holds that many bytes after it.
func TestLengthOverTheMaximumIsCorruption(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, Options{})
	if _, err := l.Append(1, make([]byte, 100)); err != nil {
		t.Fatal(err)
	}
	appendN(t, l, 2, 3)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	saved := maxRecordBytes
	maxRecordBytes = 64
	t.Cleanup(func() { maxRecordBytes = saved })
	_, err := Open(dir, "state", Options{Logger: quiet()})
	var ce *CorruptError
	if !errors.As(err, &ce) || ce.Index != 1 || ce.Reason == "" {
		t.Fatalf("got %v, want a CorruptError at record 1 naming the length", err)
	}
}

// TestIterateBoundsTheLength: Iterate refuses a length over the maximum, and
// stops at one past the end of the file without allocating it.
func TestIterateBoundsTheLength(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, Options{})
	defer func() { _ = l.Close() }()
	if _, err := l.Append(1, make([]byte, 100)); err != nil {
		t.Fatal(err)
	}
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	saved := maxRecordBytes
	maxRecordBytes = 64
	var ce *CorruptError
	if err := l.Iterate(1, func(Record) error { return nil }); !errors.As(err, &ce) {
		maxRecordBytes = saved
		t.Fatalf("iterate over a length above the maximum: %v, want a CorruptError", err)
	}
	maxRecordBytes = saved
	setLength(t, lastSegment(t, dir), headerSize, 1<<29) // under the maximum, past the end
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	n := 0
	err := l.Iterate(1, func(Record) error { n++; return nil })
	runtime.ReadMemStats(&after)
	if err != nil || n != 0 {
		t.Fatalf("iterate over a length past the end: %d records, %v", n, err)
	}
	if got := after.TotalAlloc - before.TotalAlloc; got > 1<<20 {
		t.Fatalf("Iterate allocated %d bytes", got)
	}
}

// TestDamagedLengthWithIgnoreChecksumIsTorn: the harness's checksum defect
// keeps the torn-tail reading of an overshooting length.
func TestDamagedLengthWithIgnoreChecksumIsTorn(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, Options{})
	appendN(t, l, 1, 4)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	path := lastSegment(t, dir)
	setLength(t, path, recordOffsets(t, path)[2], 1<<16)
	re, err := Open(dir, "state", Options{Logger: quiet(), IgnoreChecksum: true})
	if err != nil {
		t.Fatalf("open with the checksum defect: %v", err)
	}
	defer func() { _ = re.Close() }()
	if re.LastIndex() != 2 {
		t.Fatalf("last index %d, want 2 (truncated as a torn tail)", re.LastIndex())
	}
}
