package wal

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// files reads every regular file of dir.
func files(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !e.Type().IsRegular() {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		out[e.Name()] = b
	}
	return out
}

func segmentFiles(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	for name := range files(t, filepath.Join(dir, "state")) {
		if strings.HasSuffix(name, segmentExt) {
			out = append(out, filepath.Join(dir, "state", name))
		}
	}
	sort.Strings(out)
	return out
}

func writeAt(t *testing.T, path string, off int64, b []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if _, err := f.WriteAt(b, off); err != nil {
		t.Fatal(err)
	}
}

func cut(t *testing.T, path string, n int64) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, info.Size()-n); err != nil {
		t.Fatal(err)
	}
}

// threeSegments is a log of 12 records over three segments, closed.
func threeSegments(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	l := open(t, dir, Options{SegmentBytes: 80})
	appendN(t, l, 1, 12)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if n := len(segmentFiles(t, dir)); n < 3 {
		t.Fatalf("%d segments, want at least 3", n)
	}
	return dir
}

// tornNext adds a segment after the last that a crash left before its header.
func tornNext(t *testing.T, dir string, size int) {
	t.Helper()
	l := open(t, dir, Options{})
	next := l.LastIndex() + 1
	_ = l.Close()
	name := filepath.Join(dir, "state", fmtSegment(next))
	if err := os.WriteFile(name, make([]byte, size), 0o640); err != nil {
		t.Fatal(err)
	}
}

func fmtSegment(first uint64) string {
	s := itoa(int(first))
	return strings.Repeat("0", 20-len(s)) + s + segmentExt
}

// TestInspectAgreesWithOpen: for each start outcome, Inspect on one copy of a
// log and Open on another agree on the refusal and on the repairs, and
// Inspect leaves its copy as it was.
func TestInspectAgreesWithOpen(t *testing.T) {
	cases := []struct {
		name    string
		build   func(t *testing.T) string
		refuses bool
		torn    bool // a torn tail
		removed bool // a torn last segment
	}{
		{"clean", threeSegments, false, false, false},
		{"torn tail", func(t *testing.T) string {
			dir := threeSegments(t)
			segs := segmentFiles(t, dir)
			cut(t, segs[len(segs)-1], 3)
			return dir
		}, false, true, false},
		{"torn last segment", func(t *testing.T) string {
			dir := threeSegments(t)
			tornNext(t, dir, 10)
			return dir
		}, false, false, true},
		{"torn last segment over a torn tail", func(t *testing.T) string {
			dir := threeSegments(t)
			segs := segmentFiles(t, dir)
			cut(t, segs[len(segs)-1], 3)
			if err := os.WriteFile(filepath.Join(dir, "state", fmtSegment(99)), make([]byte, headerSize), 0o640); err != nil {
				t.Fatal(err)
			}
			return dir
		}, false, true, true},
		{"torn last segment and a damaged record before it", func(t *testing.T) string {
			dir := threeSegments(t)
			tornNext(t, dir, 5)
			writeAt(t, segmentFiles(t, dir)[0], headerSize+recordHeader, []byte{'X'})
			return dir
		}, true, false, true},
		{"checksum mismatch in an earlier segment", func(t *testing.T) string {
			dir := threeSegments(t)
			writeAt(t, segmentFiles(t, dir)[1], headerSize+recordHeader+1, []byte{'X'})
			return dir
		}, true, false, false},
		{"damaged length on the last segment", func(t *testing.T) string {
			dir := threeSegments(t)
			segs := segmentFiles(t, dir)
			last := segs[len(segs)-1]
			offs := recordOffsets(t, last)
			b, _ := os.ReadFile(last)
			off := offs[len(offs)-1]
			setLength(t, last, off, binary.BigEndian.Uint32(b[off:off+4])+50)
			return dir
		}, true, false, false},
		{"length over the maximum", func(t *testing.T) string {
			dir := threeSegments(t)
			setLength(t, segmentFiles(t, dir)[0], headerSize, 0xFFFFFFFF)
			return dir
		}, true, false, false},
		{"a record cut short in an earlier segment", func(t *testing.T) string {
			dir := threeSegments(t)
			cut(t, segmentFiles(t, dir)[0], 3)
			return dir
		}, true, false, false},
		{"bad magic", func(t *testing.T) string {
			dir := threeSegments(t)
			writeAt(t, segmentFiles(t, dir)[1], 0, []byte{'Z'})
			return dir
		}, true, false, false},
		{"a header naming another first index", func(t *testing.T) string {
			dir := threeSegments(t)
			var b [8]byte
			binary.BigEndian.PutUint64(b[:], 77)
			writeAt(t, segmentFiles(t, dir)[1], 16, b[:])
			return dir
		}, true, false, false},
		{"an unexpected segment name", func(t *testing.T) string {
			dir := threeSegments(t)
			if err := os.WriteFile(filepath.Join(dir, "state", "junk"+segmentExt), []byte("x"), 0o640); err != nil {
				t.Fatal(err)
			}
			return dir
		}, true, false, false},
		{"the only segment torn away", func(t *testing.T) string {
			dir := t.TempDir()
			if err := os.MkdirAll(filepath.Join(dir, "state"), 0o750); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "state", fmtSegment(41)), make([]byte, 7), 0o640); err != nil {
				t.Fatal(err)
			}
			return dir
		}, false, false, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			src := c.build(t)
			a, b := t.TempDir(), t.TempDir()
			copyDir(t, filepath.Join(src, "state"), filepath.Join(a, "state"))
			copyDir(t, filepath.Join(src, "state"), filepath.Join(b, "state"))
			before := files(t, filepath.Join(a, "state"))

			sv, il := Inspect(a, "state", Options{Logger: quiet()})
			if after := files(t, filepath.Join(a, "state")); !sameFiles(before, after) {
				t.Fatalf("Inspect changed its copy")
			}
			if (sv.Err != nil) != c.refuses || (sv.TornTail != nil) != c.torn || (sv.TornSegment != nil) != c.removed {
				t.Fatalf("survey: err %v, torn tail %v, torn segment %v; want refuses %v, torn %v, removed %v",
					sv.Err, sv.TornTail, sv.TornSegment, c.refuses, c.torn, c.removed)
			}

			l, err := Open(b, "state", Options{Logger: quiet()})
			if sv.Err != nil {
				if err == nil {
					_ = l.Close()
					t.Fatalf("survey refuses (%v), Open does not", sv.Err)
				}
				if got, want := strings.ReplaceAll(err.Error(), b, "DIR"), strings.ReplaceAll(sv.Err.Error(), a, "DIR"); got != want {
					t.Fatalf("Open refuses with %q, the survey with %q", got, want)
				}
			} else {
				if err != nil {
					t.Fatalf("Open refuses (%v), the survey does not", err)
				}
				torn, removed := l.OpenRepairs()
				if (torn == 1) != c.torn || (removed == 1) != c.removed || torn > 1 || removed > 1 {
					t.Fatalf("Open repaired %d torn tails and %d segments; the survey %v, %v", torn, removed, sv.TornTail, sv.TornSegment)
				}
				if l.LastIndex() != il.LastIndex() {
					t.Fatalf("Open's last index %d, the survey's %d", l.LastIndex(), il.LastIndex())
				}
				_ = l.Close()
			}

			// The files Open left are the survey's: the torn segment gone, the
			// torn tail cut (only when nothing was refused), a new segment when
			// none was left.
			want := map[string][]byte{}
			for name, data := range before {
				want[name] = data
			}
			if ts := sv.TornSegment; ts != nil {
				delete(want, ts.Segment)
			}
			if tt := sv.TornTail; tt != nil && sv.Err == nil {
				want[tt.Segment] = want[tt.Segment][:tt.Offset]
			}
			got := files(t, filepath.Join(b, "state"))
			if sv.NewSegment > 0 && sv.Err == nil {
				name := fmtSegment(sv.NewSegment)
				if len(got[name]) != headerSize {
					t.Fatalf("no new segment %s of a header's size: %d bytes", name, len(got[name]))
				}
				want[name] = got[name]
			}
			if !sameFiles(want, got) {
				t.Fatalf("Open left %v, the survey said %v", names(got), names(want))
			}
			if sv.Err == nil {
				var segs []string
				for _, s := range sv.Segments {
					segs = append(segs, s.File)
				}
				for _, s := range segs {
					if _, ok := got[s]; !ok {
						t.Fatalf("the survey keeps %s, Open did not", s)
					}
				}
			}
		})
	}
}

func sameFiles(a, b map[string][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || !bytes.Equal(v, w) {
			return false
		}
	}
	return true
}

func names(m map[string][]byte) []string {
	var out []string
	for k, v := range m {
		out = append(out, k+":"+itoa(len(v)))
	}
	sort.Strings(out)
	return out
}

// An inspected log reads but does not write.
func TestInspectedLogOnlyReads(t *testing.T) {
	dir := threeSegments(t)
	sv, l := Inspect(dir, "state", Options{Logger: quiet()})
	if sv.Err != nil || l.LastIndex() != 12 {
		t.Fatalf("survey %v, last %d", sv.Err, l.LastIndex())
	}
	if got := collect(t, l, 1); len(got) != 12 {
		t.Fatalf("Iterate read %d records", len(got))
	}
	if _, err := l.Append(1, []byte("x")); err == nil {
		t.Fatal("an inspected log took an append")
	}
	if err := l.ContinueAfter(100); err == nil {
		t.Fatal("an inspected log started a segment")
	}
}
