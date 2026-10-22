package wal

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

// copyDir copies every regular file of src into dst as it is now.
func copyDir(t *testing.T, src, dst string) {
	t.Helper()
	if err := os.MkdirAll(dst, 0o750); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !e.Type().IsRegular() {
			continue
		}
		in, err := os.Open(filepath.Join(src, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		out, err := os.Create(filepath.Join(dst, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.Copy(out, in); err != nil {
			t.Fatal(err)
		}
		_ = in.Close()
		if err := out.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// TestCrashMidRollLeavesAShortSegmentThatOpenRemoves: a crash after a roll
// created its new segment and before the segment's header reached the disk
// leaves a last segment shorter than a header, or a header of zeros. It holds
// no record, so Open removes it as a torn tail and the log goes on from the
// segment before it.
func TestCrashMidRollLeavesAShortSegmentThatOpenRemoves(t *testing.T) {
	for _, keep := range []int{0, 1, 17, headerSize - 1, -headerSize} {
		name := "bytes " + itoa(keep)
		if keep < 0 {
			name = "a header of zeros"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			crashed := t.TempDir()
			var rolled atomic.Bool
			l := open(t, dir, Options{SegmentBytes: 60, Sync: func(f *os.File) error {
				// The roll to record 3's segment: its header is written and
				// not yet synced. Copy the log as a crash would leave it.
				if strings.HasSuffix(f.Name(), "00000000000000000003.seg") && rolled.CompareAndSwap(false, true) {
					copyDir(t, filepath.Join(dir, "state"), filepath.Join(crashed, "state"))
					seg := filepath.Join(crashed, "state", "00000000000000000003.seg")
					switch {
					case keep >= 0:
						if err := os.Truncate(seg, int64(keep)); err != nil {
							t.Fatal(err)
						}
					default:
						if err := os.WriteFile(seg, make([]byte, headerSize), 0o640); err != nil {
							t.Fatal(err)
						}
					}
				}
				return f.Sync()
			}})
			appendN(t, l, 1, 3) // the third append rolls
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}
			if !rolled.Load() {
				t.Fatal("precondition: the roll to record 3's segment was not seen")
			}

			re, err := Open(crashed, "state", Options{SegmentBytes: 60, Logger: quiet()})
			if err != nil {
				t.Fatalf("open after a crash mid-roll: %v", err)
			}
			if re.LastIndex() != 2 {
				t.Fatalf("last index %d, want 2", re.LastIndex())
			}
			if torn, removed := re.OpenRepairs(); torn != 0 || removed != 1 {
				t.Fatalf("counted %d torn tails and %d removed segments, want 0 and 1", torn, removed)
			}
			if _, err := os.Stat(filepath.Join(crashed, "state", "00000000000000000003.seg")); !os.IsNotExist(err) {
				t.Fatalf("the short segment is still there (stat: %v)", err)
			}
			appendN(t, re, 3, 6)
			if err := re.Close(); err != nil {
				t.Fatal(err)
			}
			again := open(t, crashed, Options{SegmentBytes: 60})
			defer func() { _ = again.Close() }()
			if got := collect(t, again, 1); len(got) != 6 || string(got[5].Payload) != "record-6" {
				t.Fatalf("read back %d records after the crash and further appends, want 6", len(got))
			}
		})
	}
}

// TestShortEarlierSegmentIsCorruption: only the last segment can be cut short
// by a crash mid-roll; a short segment with a later one after it fails Open.
func TestShortEarlierSegmentIsCorruption(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, Options{SegmentBytes: 60})
	appendN(t, l, 1, 6)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	seg := filepath.Join(dir, "state", "00000000000000000003.seg")
	if err := os.Truncate(seg, 10); err != nil {
		t.Fatal(err)
	}
	_, err := Open(dir, "state", Options{SegmentBytes: 60, Logger: quiet()})
	if err == nil || !strings.Contains(err.Error(), "00000000000000000003.seg") {
		t.Fatalf("got %v, want an error naming the short earlier segment", err)
	}
	if _, statErr := os.Stat(seg); statErr != nil {
		t.Fatalf("the short earlier segment was removed: %v", statErr)
	}
}

func itoa(i int) string {
	if i < 0 {
		return "-" + itoa(-i)
	}
	if i < 10 {
		return string(rune('0' + i))
	}
	return itoa(i/10) + string(rune('0'+i%10))
}

func TestTornSegment(t *testing.T) {
	zeros := make([]byte, headerSize)
	sound := make([]byte, headerSize)
	copy(sound, magic)
	cases := []struct {
		name string
		head []byte
		size int64
		want bool
	}{
		{"empty", nil, 0, true},
		{"one byte", sound[:1], 1, true},
		{"31 bytes", sound[:31], 31, true},
		{"zeros", zeros, headerSize, true},
		{"a header", sound, headerSize, false},
		{"zeros then a byte", zeros, headerSize + 1, false},
	}
	for _, c := range cases {
		if got := tornSegment(c.head, c.size); got != c.want {
			t.Errorf("%s: tornSegment = %v, want %v", c.name, got, c.want)
		}
	}
}

// TestLastSegmentShapesThatStay: a last segment with a sound header and no
// records is kept; one of zeros with a byte after its header is corruption.
func TestLastSegmentShapesThatStay(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, Options{SegmentBytes: 60})
	appendN(t, l, 1, 2)
	if err := l.StartSegment(); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	re := open(t, dir, Options{SegmentBytes: 60})
	if _, err := os.Stat(filepath.Join(dir, "state", "00000000000000000003.seg")); err != nil || re.LastIndex() != 2 {
		t.Fatalf("an empty segment with a sound header: stat %v, last index %d", err, re.LastIndex())
	}
	if torn, removed := re.OpenRepairs(); torn != 0 || removed != 0 {
		t.Fatalf("a clean open counted %d torn tails and %d removed segments", torn, removed)
	}
	if err := re.Close(); err != nil {
		t.Fatal(err)
	}
	seg := filepath.Join(dir, "state", "00000000000000000003.seg")
	if err := os.WriteFile(seg, make([]byte, headerSize+1), 0o640); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, "state", Options{Logger: quiet()}); err == nil || !strings.Contains(err.Error(), "bad magic") {
		t.Fatalf("zeros followed by a byte: got %v, want bad magic", err)
	}
	if _, err := os.Stat(seg); err != nil {
		t.Fatalf("the segment was removed: %v", err)
	}
}

// TestOnlySegmentCutShortRestartsAtItsIndex: a log whose only segment was cut
// short goes on at that segment's first index, not at 1.
func TestOnlySegmentCutShortRestartsAtItsIndex(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "state"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "state", "00000000000000000005.seg"), nil, 0o640); err != nil {
		t.Fatal(err)
	}
	l := open(t, dir, Options{})
	defer func() { _ = l.Close() }()
	if l.LastIndex() != 4 {
		t.Fatalf("last index %d, want 4", l.LastIndex())
	}
	idx, err := l.Append(1, []byte("next"))
	if err != nil || idx != 5 {
		t.Fatalf("next append at %d (%v), want 5", idx, err)
	}
	if _, removed := l.OpenRepairs(); removed != 1 {
		t.Fatalf("removed segments %d, want 1", removed)
	}
}

// TestTornTailIsCounted: a torn tail truncated at open is counted once.
func TestTornTailIsCounted(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, Options{})
	appendN(t, l, 1, 3)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(lastSegment(t, dir), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	re := open(t, dir, Options{})
	defer func() { _ = re.Close() }()
	if torn, removed := re.OpenRepairs(); torn != 1 || removed != 0 {
		t.Fatalf("counted %d torn tails and %d removed segments, want 1 and 0", torn, removed)
	}
}
