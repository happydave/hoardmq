package store

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/happydave/hoardmq/internal/store/faultdisk"
)

// checkpointFile is the one checkpoint file under dir, by name.
func checkpointFile(t *testing.T, dir string) string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "state", "*.ckpt"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 1 {
		t.Fatalf("precondition: %d checkpoint files, want 1: %v", len(matches), matches)
	}
	return matches[0]
}

// corrupt flips one byte of the file's payload, past its checksum.
func corrupt(t *testing.T, path string) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) < 6 {
		t.Fatalf("precondition: %s holds %d bytes", path, len(b))
	}
	b[5] ^= 0xFF
	if err := os.WriteFile(path, b, 0o640); err != nil {
		t.Fatal(err)
	}
}

// firstSegment is the name of the oldest segment file under dir.
func firstSegment(t *testing.T, dir string) string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "state", "*.seg"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) == 0 {
		t.Fatal("precondition: no segment files")
	}
	sort.Strings(matches)
	return filepath.Base(matches[0])
}

// sessionIDs lists the recovered sessions' client identifiers, sorted.
func sessionIDs(st *State) []string {
	var ids []string
	for _, s := range st.Sessions {
		ids = append(ids, s.ClientID)
	}
	sort.Strings(ids)
	return ids
}

// TestBadCheckpointOverDroppedSegmentsRefusesRecovery: once a checkpoint has
// dropped the segments before it, a checksum failure in that checkpoint leaves
// no record of what it held. Recovery refuses, naming the checkpoint and the
// first segment it still has, rather than replaying what remains as if it were
// the whole journal.
func TestBadCheckpointOverDroppedSegmentsRefusesRecovery(t *testing.T) {
	cases := []struct {
		name  string
		write func(t *testing.T, dir string)
	}{
		{"broker's checkpoint", func(t *testing.T, dir string) {
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
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
		}},
		{"repair's checkpoint", func(t *testing.T, dir string) {
			d := faultdisk.New()
			s := openStore(t, dir, Options{Sync: d.Sync})
			for _, id := range []string{"a", "b", "c", "d", "e", "f"} {
				upsert(t, s, id)
			}
			d.FailNext(1)
			if err := s.Sync(); err == nil {
				t.Fatal("precondition: the armed fsync did not fail")
			}
			upsert(t, s, "g")
			if err := s.Sync(); err != nil {
				t.Fatalf("the repair: %v", err)
			}
			if got := s.SyncStats().Repairs; got != 1 {
				t.Fatalf("precondition: repairs = %d, want 1", got)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			d.Crash(t)
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			c.write(t, dir)
			if seg := firstSegment(t, dir); seg == "00000000000000000001.seg" {
				t.Fatalf("precondition: the checkpoint dropped no segment (first is %s)", seg)
			}
			ckpt := checkpointFile(t, dir)
			corrupt(t, ckpt)

			s := openStore(t, dir, Options{})
			defer func() { _ = s.Close() }()
			st, err := s.Recover()
			if err == nil {
				t.Fatalf("recovered sessions %v over a bad checkpoint whose segments are gone; want a refusal", sessionIDs(st))
			}
			msg := err.Error()
			if !strings.Contains(msg, filepath.Base(ckpt)) {
				t.Errorf("the refusal does not name the checkpoint %s: %v", filepath.Base(ckpt), err)
			}
			if seg := firstSegment(t, dir); !strings.Contains(msg, seg) {
				t.Errorf("the refusal does not name the first segment held, %s: %v", seg, err)
			}
		})
	}
}

// TestRepairOverABadCheckpointRefuses: a repair rebuilds the journal from the
// latest checkpoint and the records after it. When that checkpoint fails its
// checksum and the segments before it are gone, the repair fails, the caller
// is refused, and the journal is left for recovery to refuse as well: the
// repair writes no checkpoint that would make the loss look whole.
func TestRepairOverABadCheckpointRefuses(t *testing.T) {
	dir := t.TempDir()
	d := faultdisk.New()
	s := openStore(t, dir, Options{SegmentBytes: 256, Sync: d.Sync})
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
	if seg := firstSegment(t, dir); seg == "00000000000000000001.seg" {
		t.Fatalf("precondition: the checkpoint dropped no segment (first is %s)", seg)
	}
	ckpt := checkpointFile(t, dir)
	corrupt(t, ckpt)

	upsert(t, s, "g")
	d.FailNext(1)
	if err := s.Sync(); err == nil {
		t.Fatal("precondition: the armed fsync did not fail")
	}
	err = s.Sync()
	if err == nil {
		t.Fatal("the repair rebuilt the journal over a bad checkpoint whose segments are gone; want it refused")
	}
	if !strings.Contains(err.Error(), filepath.Base(ckpt)) {
		t.Errorf("the repair's error does not name the checkpoint %s: %v", filepath.Base(ckpt), err)
	}
	if got := s.SyncStats().Repairs; got != 0 {
		t.Errorf("repairs = %d, want 0", got)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// No power cut: the journal's files are as the process left them, so
	// what refuses is the gap, not the record the failed fsync covered.
	r := openStore(t, dir, Options{})
	defer func() { _ = r.Close() }()
	if got, err := r.Recover(); err == nil {
		t.Fatalf("recovered sessions %v after the refused repair; want a refusal", sessionIDs(got))
	}
}

// TestBadCheckpointOverTheWholeLogRecovers: a checkpoint that fails its
// checksum while every record from the first is still held is passed over,
// and recovery replays the whole log.
func TestBadCheckpointOverTheWholeLogRecovers(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir, Options{})
	for _, id := range []string{"a", "b", "c"} {
		upsert(t, s, id)
	}
	st, err := s.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(fixed(st)); err != nil {
		t.Fatal(err)
	}
	upsert(t, s, "d")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if seg := firstSegment(t, dir); seg != "00000000000000000001.seg" {
		t.Fatalf("precondition: the first segment is %s, want the one holding record 1", seg)
	}
	corrupt(t, checkpointFile(t, dir))

	r := openStore(t, dir, Options{})
	defer func() { _ = r.Close() }()
	got, err := r.Recover()
	if err != nil {
		t.Fatalf("recover over the whole log: %v", err)
	}
	if ids := strings.Join(sessionIDs(got), ","); ids != "a,b,c,d" {
		t.Fatalf("recovered %q, want a,b,c,d", ids)
	}
}

// TestReachableJournalsAreNeverRefused: after appends over small segments,
// checkpoints, failed fsyncs repaired by a Sync or a checkpoint, closes, power
// cuts and reopens, every recovery succeeds and holds every session whose Sync
// returned nil.
func TestReachableJournalsAreNeverRefused(t *testing.T) {
	dir := t.TempDir()
	var released []string
	n := 0
	next := func() string { n++; return "c" + itoa(n) }
	for round := 0; round < 12; round++ {
		d := faultdisk.New()
		s := openStore(t, dir, Options{SegmentBytes: 64, Sync: d.Sync})
		st, err := s.Recover()
		if err != nil {
			t.Fatalf("round %d: recovery refused a journal a correct node wrote: %v", round, err)
		}
		for _, id := range released {
			if st.Find("ns", id) == nil {
				t.Fatalf("round %d: released session %s not recovered", round, id)
			}
		}
		for i := 0; i < 5; i++ {
			id := next()
			upsert(t, s, id)
			if err := s.Sync(); err == nil {
				released = append(released, id)
			}
		}
		switch round % 4 {
		case 1:
			st, err := s.Recover()
			if err != nil {
				t.Fatalf("round %d: %v", round, err)
			}
			if err := s.Checkpoint(fixed(st)); err != nil {
				t.Fatalf("round %d: checkpoint: %v", round, err)
			}
		case 2:
			id := next()
			upsert(t, s, id)
			d.FailNext(1)
			if err := s.Sync(); err == nil {
				t.Fatalf("round %d: precondition: the armed fsync did not fail", round)
			}
			if err := s.Sync(); err != nil {
				t.Fatalf("round %d: the repair: %v", round, err)
			}
		case 3:
			d.FailNext(1)
			if err := s.Checkpoint(fixed(NewState())); err == nil {
				t.Fatalf("round %d: precondition: the checkpoint's fsync did not fail", round)
			}
			if err := s.Checkpoint(fixed(NewState())); err != nil {
				t.Fatalf("round %d: the repair over an empty active segment: %v", round, err)
			}
		}
		id := next()
		upsert(t, s, id)
		if err := s.Sync(); err == nil {
			released = append(released, id)
		}
		if err := s.Close(); err != nil {
			t.Fatalf("round %d: close: %v", round, err)
		}
		d.Crash(t)
	}
	if got := firstSegment(t, dir); got == "00000000000000000001.seg" {
		t.Fatalf("precondition: no segment was ever dropped (first is %s)", got)
	}
	if len(released) < 50 {
		t.Fatalf("precondition: %d sessions released", len(released))
	}
}

// TestHoleBetweenSegmentsRefusesRecovery: a segment missing between two that
// are held leaves its records in no file the replay reads; recovery refuses,
// naming the segment after the hole, rather than replaying around it.
func TestHoleBetweenSegmentsRefusesRecovery(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir, Options{SegmentBytes: 64})
	for _, id := range []string{"a", "b", "c", "d", "e", "f"} {
		upsert(t, s, id)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	segs, err := filepath.Glob(filepath.Join(dir, "state", "*.seg"))
	if err != nil || len(segs) < 3 {
		t.Fatalf("precondition: segments %v (%v), want at least 3", segs, err)
	}
	sort.Strings(segs)
	if err := os.Remove(segs[1]); err != nil {
		t.Fatal(err)
	}
	r := openStore(t, dir, Options{})
	defer func() { _ = r.Close() }()
	st, err := r.Recover()
	if err == nil {
		t.Fatalf("recovered sessions %v around a missing segment; want a refusal", sessionIDs(st))
	}
	if !strings.Contains(err.Error(), filepath.Base(segs[2])) {
		t.Errorf("the refusal does not name the segment after the hole, %s: %v", filepath.Base(segs[2]), err)
	}
}

// TestRepairOverABadRepairCheckpointRefuses: after a repair the only segment
// is the empty one it started. If the repair's checkpoint then fails its
// checksum, a second repair finds no record from index 1 on and refuses,
// rather than rebuilding the journal from nothing.
func TestRepairOverABadRepairCheckpointRefuses(t *testing.T) {
	dir := t.TempDir()
	d := faultdisk.New()
	s := openStore(t, dir, Options{Sync: d.Sync})
	defer func() { _ = s.Close() }()
	for _, id := range []string{"a", "b", "c"} {
		upsert(t, s, id)
	}
	d.FailNext(1)
	if err := s.Sync(); err == nil {
		t.Fatal("precondition: the armed fsync did not fail")
	}
	if err := s.Sync(); err != nil {
		t.Fatalf("precondition: the first repair: %v", err)
	}
	if seg := firstSegment(t, dir); seg != "00000000000000000004.seg" {
		t.Fatalf("precondition: first segment %s, want the repair's empty 00000000000000000004.seg", seg)
	}
	ckpt := checkpointFile(t, dir)
	corrupt(t, ckpt)
	d.FailNext(1)
	if err := s.Checkpoint(fixed(NewState())); err == nil {
		t.Fatal("precondition: the checkpoint's fsync did not fail")
	}
	err := s.Checkpoint(fixed(NewState()))
	if err == nil {
		t.Fatal("the second repair rebuilt the journal over a bad checkpoint with no record held; want it refused")
	}
	if !strings.Contains(err.Error(), filepath.Base(ckpt)) {
		t.Errorf("the repair's error does not name the checkpoint %s: %v", filepath.Base(ckpt), err)
	}
	if got := s.SyncStats().Repairs; got != 1 {
		t.Errorf("repairs = %d, want 1", got)
	}
}
