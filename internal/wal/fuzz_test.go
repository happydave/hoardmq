package wal

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/happydave/hoardmq/internal/fuzzseed"
)

// segmentBodies are the bytes after the header of a test log's segments:
// whole, cut inside its last record, and with a record's length damaged.
func segmentBodies(f *testing.F) [][]byte {
	f.Helper()
	dir := f.TempDir()
	l, err := Open(dir, "state", Options{Logger: quiet()})
	if err != nil {
		f.Fatal(err)
	}
	for i := 1; i <= 5; i++ {
		if _, err := l.Append(byte(i%7), bytes.Repeat([]byte{byte('a' + i)}, i*3)); err != nil {
			f.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		f.Fatal(err)
	}
	seg, err := os.ReadFile(filepath.Join(dir, "state", "00000000000000000001.seg"))
	if err != nil {
		f.Fatal(err)
	}
	body := seg[headerSize:]
	damaged := append([]byte(nil), body...)
	damaged[3] += 40
	return [][]byte{body, body[:len(body)-2], body[:5], damaged, nil}
}

// FuzzSegment: a segment of first index 1 holding arbitrary bytes after its
// header is read by Inspect as Open reads it, and an inspected log that
// refuses nothing iterates exactly its records.
func FuzzSegment(f *testing.F) {
	for _, b := range segmentBodies(f) {
		fuzzseed.Add(f, b)
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		var seg bytes.Buffer
		if err := writeHeader(&seg, 1); err != nil {
			t.Fatal(err)
		}
		seg.Write(body)
		a, b := t.TempDir(), t.TempDir()
		for _, d := range []string{a, b} {
			if err := os.MkdirAll(filepath.Join(d, "state"), 0o750); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(d, "state", "00000000000000000001.seg"), seg.Bytes(), 0o640); err != nil {
				t.Fatal(err)
			}
		}
		sv, il := Inspect(a, "state", Options{Logger: quiet()})
		l, err := Open(b, "state", Options{Logger: quiet()})
		if err == nil {
			defer func() { _ = l.Close() }()
		}
		switch {
		case (sv.Err == nil) != (err == nil):
			t.Fatalf("survey refuses %v, Open %v", sv.Err, err)
		case err != nil:
			if got, want := strings.ReplaceAll(err.Error(), b, "DIR"), strings.ReplaceAll(sv.Err.Error(), a, "DIR"); got != want {
				t.Fatalf("Open refuses %q, the survey %q", got, want)
			}
			return
		}
		torn, removed := l.OpenRepairs()
		if (torn == 1) != (sv.TornTail != nil) || (removed == 1) != (sv.TornSegment != nil) || l.LastIndex() != il.LastIndex() {
			t.Fatalf("Open repaired %d tails and %d segments to %d, the survey %v %v to %d", torn, removed, l.LastIndex(), sv.TornTail, sv.TornSegment, il.LastIndex())
		}
		n := uint64(0)
		if err := il.Iterate(1, func(r Record) error {
			n++
			if r.Index != n {
				t.Fatalf("record %d read as %d", n, r.Index)
			}
			return nil
		}); err != nil {
			t.Fatalf("an inspected log that refuses nothing failed to iterate: %v", err)
		}
		if n != il.LastIndex() {
			t.Fatalf("iterated %d records of %d", n, il.LastIndex())
		}
	})
}
