package wal

import (
	"os"
	"path/filepath"
	"testing"
)

// TestTruncateKeepsEverySegmentOnceWhenARemoveFails: a segment that cannot be
// removed mid-truncate leaves the log listing exactly the segments it still
// has, each once, so every record reads back once and in order, and a later
// truncate removes it.
func TestTruncateKeepsEverySegmentOnceWhenARemoveFails(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, Options{SegmentBytes: 60})
	defer func() { _ = l.Close() }()
	appendN(t, l, 1, 8) // two records a segment: 1-2, 3-4, 5-6, 7-8
	if _, _, err := l.Flush(); err != nil {
		t.Fatal(err)
	}
	if l.Segments() != 4 {
		t.Fatalf("precondition: %d segments, want 4", l.Segments())
	}
	// Segment 3's file replaced by a directory that is not empty, so removing
	// it fails.
	seg := filepath.Join(dir, "state", "00000000000000000003.seg")
	data, err := os.ReadFile(seg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(seg); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(seg, "x"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := l.Truncate(7); err == nil {
		t.Fatal("precondition: the truncate removed a segment it could not remove")
	}
	if err := os.RemoveAll(seg); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(seg, data, 0o640); err != nil {
		t.Fatal(err)
	}

	var got []uint64
	for _, r := range collect(t, l, 1) {
		got = append(got, r.Index)
	}
	want := []uint64{3, 4, 5, 6, 7, 8}
	if len(got) != len(want) {
		t.Fatalf("read back %v after the failed truncate, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("read back %v after the failed truncate, want %v", got, want)
		}
	}
	if err := l.Truncate(7); err != nil {
		t.Fatal(err)
	}
	if l.Segments() != 1 || l.FirstIndex() != 7 {
		t.Fatalf("after the second truncate: %d segments from %d, want 1 from 7", l.Segments(), l.FirstIndex())
	}
}
