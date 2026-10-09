package wal

import (
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"
)

// TestFailedRollLeavesTheLogAppending: a roll whose new segment cannot be
// synced fails its append, removes the new file and keeps the old segment
// active, so the next append rolls again and every record is read back.
func TestFailedRollLeavesTheLogAppending(t *testing.T) {
	dir := t.TempDir()
	var failNew atomic.Bool
	l := open(t, dir, Options{SegmentBytes: 60, Sync: func(f *os.File) error {
		if failNew.Load() && strings.HasSuffix(f.Name(), "00000000000000000003.seg") {
			return errors.New("injected fsync failure")
		}
		return f.Sync()
	}})
	appendN(t, l, 1, 2) // the second record fills the first segment
	failNew.Store(true)
	if _, err := l.Append(1, []byte("record-3")); err == nil {
		t.Fatal("precondition: the new segment's header sync did not fail")
	}
	failNew.Store(false)
	appendN(t, l, 3, 5)
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	re := open(t, dir, Options{SegmentBytes: 60})
	defer func() { _ = re.Close() }()
	var got []uint64
	if err := re.Iterate(1, func(r Record) error { got = append(got, r.Index); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(got) != 5 || got[4] != 5 {
		t.Fatalf("read back %v, want records 1 to 5", got)
	}
}
