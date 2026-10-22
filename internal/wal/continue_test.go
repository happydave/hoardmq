package wal

import (
	"os"
	"path/filepath"
	"testing"
)

// TestContinueAfterStartsASegmentPastTheIndex: a log at 5 told to continue
// after 7 appends its next record at 8, in a segment of its own, and reads
// back 1 to 5 and 8 after a reopen. An index the log already reaches changes
// nothing.
func TestContinueAfterStartsASegmentPastTheIndex(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, Options{})
	appendN(t, l, 1, 5)
	for _, idx := range []uint64{3, 5} {
		if err := l.ContinueAfter(idx); err != nil {
			t.Fatal(err)
		}
		if l.LastIndex() != 5 || l.Segments() != 1 {
			t.Fatalf("after ContinueAfter(%d) on a log at 5: last %d, %d segments, want 5 and 1", idx, l.LastIndex(), l.Segments())
		}
	}
	if err := l.ContinueAfter(7); err != nil {
		t.Fatal(err)
	}
	if l.LastIndex() != 7 {
		t.Fatalf("last index %d after ContinueAfter(7), want 7", l.LastIndex())
	}
	if _, err := os.Stat(filepath.Join(dir, "state", "00000000000000000008.seg")); err != nil {
		t.Fatalf("no segment at 8: %v", err)
	}
	appendN(t, l, 8, 8)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	re := open(t, dir, Options{})
	defer func() { _ = re.Close() }()
	if re.LastIndex() != 8 {
		t.Fatalf("reopened at %d, want 8", re.LastIndex())
	}
	var got []uint64
	for _, r := range collect(t, re, 1) {
		got = append(got, r.Index)
	}
	if want := []uint64{1, 2, 3, 4, 5, 8}; len(got) != len(want) || got[4] != 5 || got[5] != 8 {
		t.Fatalf("read back %v, want %v", got, want)
	}
}

// TestStartSegmentAfterReopenOverAnEmptySegment: a last segment with no
// record, read back by Open, is not followed by a second segment at the same
// index.
func TestStartSegmentAfterReopenOverAnEmptySegment(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, Options{})
	appendN(t, l, 1, 2)
	if err := l.StartSegment(); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	re := open(t, dir, Options{})
	defer func() { _ = re.Close() }()
	segs := re.Segments()
	if err := re.StartSegment(); err != nil {
		t.Fatalf("StartSegment over the reopened empty segment: %v", err)
	}
	if re.Segments() != segs {
		t.Fatalf("%d segments after StartSegment, want %d", re.Segments(), segs)
	}
	appendN(t, re, 3, 3)
}

// TestContinueAfterAnEmptyLastSegment: a last segment holding no record
// (a roll's, its records lost) stays where it is; the new segment follows it,
// and a truncate past the checkpoint drops both older segments.
func TestContinueAfterAnEmptyLastSegment(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, Options{})
	appendN(t, l, 1, 5)
	if err := l.StartSegment(); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	re := open(t, dir, Options{})
	if err := re.ContinueAfter(7); err != nil {
		t.Fatal(err)
	}
	if re.Segments() != 3 {
		t.Fatalf("%d segments, want 3 (1, the empty 6, the new 8)", re.Segments())
	}
	appendN(t, re, 8, 8)
	if err := re.Truncate(8); err != nil {
		t.Fatal(err)
	}
	if re.Segments() != 1 || re.FirstIndex() != 8 {
		t.Fatalf("after the truncate: %d segments from %d, want 1 from 8", re.Segments(), re.FirstIndex())
	}
	if err := re.Close(); err != nil {
		t.Fatal(err)
	}
	again := open(t, dir, Options{})
	defer func() { _ = again.Close() }()
	if got := collect(t, again, 1); len(got) != 1 || got[0].Index != 8 {
		t.Fatalf("read back %d records, want record 8 alone", len(got))
	}
}
