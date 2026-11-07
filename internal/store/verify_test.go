package store

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/happydave/hoardmq/internal/wal"
)

// verifyDir runs VerifyLog over an inspected copy of dir's state log.
func verifyDir(t *testing.T, dir string) Verified {
	t.Helper()
	sv, l := wal.Inspect(dir, "state", wal.Options{Logger: quiet()})
	if sv.Err != nil {
		t.Fatalf("precondition: the survey refuses: %v", sv.Err)
	}
	return VerifyLog(l)
}

// recoverDir opens and recovers dir as a start does.
func recoverDir(t *testing.T, dir string) error {
	t.Helper()
	s := openStore(t, dir, Options{})
	defer func() { _ = s.Close() }()
	_, err := s.Recover()
	return err
}

// TestVerifyLogAgreesWithRecover: for each recovery a start refuses, and a
// clean one, VerifyLog over an inspected log refuses with the same error, and
// writes nothing.
func TestVerifyLogAgreesWithRecover(t *testing.T) {
	cases := []struct {
		name    string
		build   func(t *testing.T, dir string)
		refuses string // a substring of the refusal; "" recovers
	}{
		{"clean", func(t *testing.T, dir string) {
			s := openStore(t, dir, Options{})
			upsert(t, s, "a")
			_ = s.Close()
		}, ""},
		{"a bad checkpoint over dropped segments", func(t *testing.T, dir string) {
			s := openStore(t, dir, Options{SegmentBytes: 256})
			for _, id := range []string{"a", "b", "c", "d", "e", "f"} {
				upsert(t, s, id)
			}
			st, err := s.Recover()
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Checkpoint(fixed(st)); err != nil {
				t.Fatal(err)
			}
			upsert(t, s, "g")
			_ = s.Close()
			corrupt(t, checkpointFile(t, dir))
		}, "are not held"},
		{"a checkpoint that does not decode", func(t *testing.T, dir string) {
			s := openStore(t, dir, Options{})
			upsert(t, s, "a")
			_ = s.Close()
			l, err := wal.Open(dir, "state", wal.Options{Logger: quiet()})
			if err != nil {
				t.Fatal(err)
			}
			if err := l.WriteCheckpoint(1, []byte("not json")); err != nil {
				t.Fatal(err)
			}
			_ = l.Close()
		}, "store: checkpoint 1"},
		{"a record that does not apply", func(t *testing.T, dir string) {
			s := openStore(t, dir, Options{})
			upsert(t, s, "a")
			_ = s.Close()
			l, err := wal.Open(dir, "state", wal.Options{Logger: quiet()})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := l.Append(250, []byte("x")); err != nil {
				t.Fatal(err)
			}
			_ = l.Close()
		}, "store: record 2"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			c.build(t, dir)
			v := verifyDir(t, dir)
			err := recoverDir(t, dir)
			switch {
			case c.refuses == "" && (v.Err != nil || err != nil):
				t.Fatalf("verify %v, start %v; want both to recover", v.Err, err)
			case c.refuses != "" && (v.Err == nil || err == nil || v.Err.Error() != err.Error() || !strings.Contains(err.Error(), c.refuses)):
				t.Fatalf("verify %v, start %v; want the same refusal naming %q", v.Err, err, c.refuses)
			}
		})
	}
}

// A checkpoint past the log's end is what Open appends after; VerifyLog
// names it and writes no segment.
func TestVerifyLogNamesACheckpointAhead(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir, Options{})
	upsert(t, s, "a")
	_ = s.Close()
	l, err := wal.Open(dir, "state", wal.Options{Logger: quiet()})
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(NewState())
	if err := l.WriteCheckpoint(100, payload); err != nil {
		t.Fatal(err)
	}
	_ = l.Close()
	segments := len(segmentNames(t, dir))
	if v := verifyDir(t, dir); v.AppendAfter != 100 || v.Err != nil {
		t.Fatalf("VerifyLog %+v, want AppendAfter 100 and no refusal", v)
	}
	if n := len(segmentNames(t, dir)); n != segments {
		t.Fatalf("verify wrote a segment: %d, was %d", n, segments)
	}
	s = openStore(t, dir, Options{})
	defer func() { _ = s.Close() }()
	if s.LastIndex() != 100 {
		t.Fatalf("a start continues at %d, want after 100", s.LastIndex())
	}
}

func segmentNames(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	sv, _ := wal.Inspect(dir, "state", wal.Options{Logger: quiet()})
	for _, s := range sv.Segments {
		out = append(out, s.File)
	}
	return out
}
