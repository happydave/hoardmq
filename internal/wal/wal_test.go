package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func open(t *testing.T, dir string, opts Options) *Log {
	t.Helper()
	if opts.Logger == nil {
		opts.Logger = quiet()
	}
	l, err := Open(dir, "state", opts)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func appendN(t *testing.T, l *Log, from, to int) {
	t.Helper()
	for i := from; i <= to; i++ {
		idx, err := l.Append(byte(i%7), []byte(fmt.Sprintf("record-%d", i)))
		if err != nil {
			t.Fatal(err)
		}
		if idx != uint64(i) {
			t.Fatalf("append %d got index %d", i, idx)
		}
	}
}

func collect(t *testing.T, l *Log, from uint64) []Record {
	t.Helper()
	var out []Record
	if err := l.Iterate(from, func(r Record) error {
		out = append(out, r)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestAppendIterateAcrossRollAndReopen(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, Options{SegmentBytes: 200})
	appendN(t, l, 1, 50)
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	if l.Segments() < 3 {
		t.Fatalf("expected several segments, got %d", l.Segments())
	}
	recs := collect(t, l, 1)
	if len(recs) != 50 || recs[49].Index != 50 || string(recs[49].Payload) != "record-50" || recs[49].Type != 50%7 {
		t.Fatalf("iterate: %d records, last %+v", len(recs), recs[len(recs)-1])
	}
	if got := collect(t, l, 40); len(got) != 11 || got[0].Index != 40 {
		t.Fatalf("iterate from 40: %d records, first %d", len(got), got[0].Index)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l2 := open(t, dir, Options{SegmentBytes: 200})
	if l2.LastIndex() != 50 || l2.FirstIndex() != 1 {
		t.Fatalf("reopen: last %d first %d", l2.LastIndex(), l2.FirstIndex())
	}
	appendN(t, l2, 51, 60)
	if got := collect(t, l2, 1); len(got) != 60 {
		t.Fatalf("after reopen: %d records", len(got))
	}
	_ = l2.Close()
}

func TestTruncateDropsWholeSegmentsOnly(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, Options{SegmentBytes: 200})
	appendN(t, l, 1, 50)
	before := l.Segments()
	var cut uint64 = 30
	if err := l.Truncate(cut); err != nil {
		t.Fatal(err)
	}
	if l.Segments() >= before || l.FirstIndex() > cut {
		t.Fatalf("segments %d -> %d, first %d", before, l.Segments(), l.FirstIndex())
	}
	got := collect(t, l, cut)
	if len(got) != 21 || got[0].Index != cut {
		t.Fatalf("after truncate: %d from %d", len(got), got[0].Index)
	}
	// The active segment survives even when everything is below the cut.
	if err := l.Truncate(1000); err != nil {
		t.Fatal(err)
	}
	if l.Segments() != 1 || l.LastIndex() != 50 {
		t.Fatalf("active segment removed: %d segments", l.Segments())
	}
	_ = l.Close()
}

func lastSegment(t *testing.T, dir string) string {
	t.Helper()
	entries, _ := os.ReadDir(filepath.Join(dir, "state"))
	var segs []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), segmentExt) {
			segs = append(segs, filepath.Join(dir, "state", e.Name()))
		}
	}
	if len(segs) == 0 {
		t.Fatal("no segments")
	}
	return segs[len(segs)-1]
}

func TestTornTailIsTruncated(t *testing.T) {
	for _, cut := range []int{3, recordHeader + 2} {
		dir := t.TempDir()
		l := open(t, dir, Options{})
		appendN(t, l, 1, 5)
		_ = l.Close()
		path := lastSegment(t, dir)
		info, _ := os.Stat(path)
		// Append a partial record: header (and maybe some payload) only.
		f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
		var hdr [recordHeader]byte
		binary.BigEndian.PutUint32(hdr[0:4], 100)
		_, _ = f.Write(append(hdr[:], []byte("partial")...)[:cut])
		_ = f.Close()
		l2 := open(t, dir, Options{})
		if l2.LastIndex() != 5 {
			t.Fatalf("cut %d: last index %d", cut, l2.LastIndex())
		}
		if after, _ := os.Stat(path); after.Size() != info.Size() {
			t.Fatalf("cut %d: tail not truncated (%d vs %d)", cut, after.Size(), info.Size())
		}
		appendN(t, l2, 6, 7)
		_ = l2.Sync()
		if got := collect(t, l2, 1); len(got) != 7 || string(got[6].Payload) != "record-7" {
			t.Fatalf("cut %d: after repair %d records", cut, len(got))
		}
		_ = l2.Close()
	}
}

func TestCorruptionFailsClosed(t *testing.T) {
	flip := func(t *testing.T, path string, offset int64) {
		f, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		b := make([]byte, 1)
		_, _ = f.ReadAt(b, offset)
		b[0] ^= 0xFF
		_, _ = f.WriteAt(b, offset)
		_ = f.Close()
	}
	t.Run("payload byte", func(t *testing.T) {
		dir := t.TempDir()
		l := open(t, dir, Options{})
		appendN(t, l, 1, 3)
		_ = l.Close()
		path := lastSegment(t, dir)
		flip(t, path, headerSize+recordHeader+2) // inside record 1's payload
		_, err := Open(dir, "state", Options{Logger: quiet()})
		var ce *CorruptError
		if !errors.As(err, &ce) || ce.Index != 1 || ce.Offset != headerSize || !strings.Contains(ce.Segment, ".seg") {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("crc byte", func(t *testing.T) {
		dir := t.TempDir()
		l := open(t, dir, Options{})
		appendN(t, l, 1, 3)
		_ = l.Close()
		flip(t, lastSegment(t, dir), headerSize+6)
		if _, err := Open(dir, "state", Options{Logger: quiet()}); err == nil {
			t.Fatal("corrupted CRC accepted")
		}
	})
	t.Run("earlier segment tail is corruption not torn", func(t *testing.T) {
		dir := t.TempDir()
		l := open(t, dir, Options{SegmentBytes: 100})
		appendN(t, l, 1, 20)
		_ = l.Close()
		entries, _ := os.ReadDir(filepath.Join(dir, "state"))
		first := filepath.Join(dir, "state", entries[0].Name())
		info, _ := os.Stat(first)
		_ = os.Truncate(first, info.Size()-3)
		_, err := Open(dir, "state", Options{Logger: quiet()})
		var ce *CorruptError
		if !errors.As(err, &ce) {
			t.Fatalf("got %v", err)
		}
	})
}

func TestHeaderValidation(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, Options{})
	appendN(t, l, 1, 2)
	_ = l.Close()
	path := lastSegment(t, dir)
	f, _ := os.OpenFile(path, os.O_RDWR, 0)
	_, _ = f.WriteAt([]byte{0, 9}, 8) // version 9
	_ = f.Close()
	if _, err := Open(dir, "state", Options{Logger: quiet()}); err == nil || !strings.Contains(err.Error(), "format version 9") {
		t.Fatalf("got %v", err)
	}
	f, _ = os.OpenFile(path, os.O_RDWR, 0)
	_, _ = f.WriteAt([]byte("NOTALOG!"), 0)
	_ = f.Close()
	if _, err := Open(dir, "state", Options{Logger: quiet()}); err == nil || !strings.Contains(err.Error(), "bad magic") {
		t.Fatalf("got %v", err)
	}
	// A fresh segment carries the magic, the version and its first index.
	dir2 := t.TempDir()
	l2 := open(t, dir2, Options{SegmentBytes: 100})
	appendN(t, l2, 1, 20)
	_ = l2.Close()
	seg := lastSegment(t, dir2)
	h := make([]byte, headerSize)
	f, _ = os.Open(seg)
	_, _ = io.ReadFull(f, h)
	_ = f.Close()
	if string(h[:8]) != magic || binary.BigEndian.Uint16(h[8:10]) != formatVersion || binary.BigEndian.Uint64(h[16:24]) <= 1 {
		t.Fatalf("header % x", h)
	}
}

func TestSyncHookAndCheckpoints(t *testing.T) {
	dir := t.TempDir()
	var syncs atomic.Int32
	l := open(t, dir, Options{Sync: func(f *os.File) error { syncs.Add(1); return f.Sync() }})
	appendN(t, l, 1, 3)
	before := syncs.Load()
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	if syncs.Load() != before+1 {
		t.Fatalf("Sync did not call the hook once: %d -> %d", before, syncs.Load())
	}
	if _, _, ok := l.LatestCheckpoint(); ok {
		t.Fatal("checkpoint before any written")
	}
	if err := l.WriteCheckpoint(2, []byte("state@2")); err != nil {
		t.Fatal(err)
	}
	if err := l.WriteCheckpoint(3, []byte("state@3")); err != nil {
		t.Fatal(err)
	}
	idx, payload, ok := l.LatestCheckpoint()
	if !ok || idx != 3 || string(payload) != "state@3" {
		t.Fatalf("latest: %d %q %v", idx, payload, ok)
	}
	entries, _ := os.ReadDir(filepath.Join(dir, "state"))
	n := 0
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), checkpointExt) {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("%d checkpoint files, want 1", n)
	}
	// A damaged checkpoint is skipped.
	path := filepath.Join(dir, "state", "checkpoint-00000000000000000003.ckpt")
	_ = os.WriteFile(path, []byte{1, 2, 3, 4, 'x'}, 0o640)
	if _, _, ok := l.LatestCheckpoint(); ok {
		t.Fatal("damaged checkpoint accepted")
	}
	_ = l.Close()
	if _, err := l.Append(1, nil); err == nil {
		t.Fatal("append after close")
	}
}
