package wal

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReplayGap(t *testing.T) {
	cases := []struct {
		name    string
		from    uint64
		spans   []span
		missing [2]uint64
		next    int
		gap     bool
	}{
		{"empty log", 1, []span{{1, 0}}, [2]uint64{}, 0, false},
		{"whole log", 1, []span{{1, 4}, {5, 9}}, [2]uint64{}, 0, false},
		{"after a checkpoint inside the first segment", 7, []span{{5, 9}, {10, 12}}, [2]uint64{}, 0, false},
		{"after a checkpoint at the first segment's start", 5, []span{{5, 9}}, [2]uint64{}, 0, false},
		{"after a repair: one empty segment", 13, []span{{13, 12}}, [2]uint64{}, 0, false},
		{"checkpoint newer than the log", 13, []span{{1, 9}}, [2]uint64{}, 0, false},
		{"hole below the replay index", 13, []span{{1, 9}, {13, 12}}, [2]uint64{}, 0, false},
		{"prefix dropped", 1, []span{{5, 9}}, [2]uint64{1, 4}, 0, true},
		{"prefix dropped after a checkpoint", 3, []span{{5, 9}}, [2]uint64{3, 4}, 0, true},
		{"after a repair, no checkpoint", 1, []span{{13, 12}}, [2]uint64{1, 12}, 0, true},
		{"hole between segments", 1, []span{{1, 4}, {9, 12}}, [2]uint64{5, 8}, 1, true},
		{"hole after an empty segment", 1, []span{{1, 4}, {5, 4}, {9, 12}}, [2]uint64{5, 8}, 2, true},
	}
	for _, c := range cases {
		missing, through, next, gap := replayGap(c.from, c.spans)
		if gap != c.gap || [2]uint64{missing, through} != c.missing || next != c.next {
			t.Errorf("%s: replayGap(%d, %v) = %d to %d before %d, %v; want %v before %d, %v",
				c.name, c.from, c.spans, missing, through, next, gap, c.missing, c.next, c.gap)
		}
	}
}

// checkpointed writes records 1 to 20 over small segments, a checkpoint at
// 12, and drops the segments before it, as the store's checkpoint does.
func checkpointed(t *testing.T) (dir string, l *Log, ckpt string) {
	t.Helper()
	dir = t.TempDir()
	l = open(t, dir, Options{SegmentBytes: 60})
	appendN(t, l, 1, 20)
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := l.WriteCheckpoint(12, []byte("state@12")); err != nil {
		t.Fatal(err)
	}
	if err := l.Truncate(13); err != nil {
		t.Fatal(err)
	}
	if first := l.FirstIndex(); first <= 1 || first > 13 {
		t.Fatalf("precondition: first index held %d, want between 2 and 13", first)
	}
	return dir, l, filepath.Join(dir, "state", "checkpoint-00000000000000000012.ckpt")
}

func flipLastByte(t *testing.T, path string) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)-1] ^= 0xFF
	if err := os.WriteFile(path, b, 0o640); err != nil {
		t.Fatal(err)
	}
}

// TestRecoveryCheckpointRefusesAGap: once the segments before a checkpoint are
// dropped, a checkpoint that fails its checksum leaves records 1 to 12 held
// nowhere, and the recovery read says so, naming the checkpoint skipped and
// the first segment held.
func TestRecoveryCheckpointRefusesAGap(t *testing.T) {
	_, l, ckpt := checkpointed(t)
	defer func() { _ = l.Close() }()
	if idx, _, ok, err := l.RecoveryCheckpoint(); err != nil || !ok || idx != 12 {
		t.Fatalf("intact checkpoint: %d %v %v, want 12 and no error", idx, ok, err)
	}
	flipLastByte(t, ckpt)
	_, _, ok, err := l.RecoveryCheckpoint()
	var ge *GapError
	if !errors.As(err, &ge) {
		t.Fatalf("got %v (checkpoint found %v), want a *GapError", err, ok)
	}
	first := l.FirstIndex()
	seg := filepath.Base(l.segments[0].path)
	if ok || ge.From != 1 || ge.Missing != 1 || ge.Through != first-1 || ge.Segment != seg || ge.Checkpoint != "" {
		t.Errorf("gap %+v (checkpoint found %v), want from 1, 1 to %d missing before %s, no checkpoint", ge, ok, first-1, seg)
	}
	if len(ge.Skipped) != 1 || ge.Skipped[0] != (SkippedCheckpoint{File: filepath.Base(ckpt), Reason: "bad checksum"}) {
		t.Errorf("skipped %+v, want %s with a bad checksum", ge.Skipped, filepath.Base(ckpt))
	}
	for _, name := range []string{filepath.Base(ckpt), seg, filepath.Dir(ckpt)} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("the error does not name %s: %v", name, err)
		}
	}
}

// TestRecoveryCheckpointNamesAnOlderCheckpointAndEverySkippedFile: an older
// checkpoint that verifies is used, and a gap after it is still refused, with
// each newer file that was passed over named.
func TestRecoveryCheckpointNamesAnOlderCheckpointAndEverySkippedFile(t *testing.T) {
	dir, l, ckpt := checkpointed(t)
	defer func() { _ = l.Close() }()
	older := filepath.Join(dir, "state", "checkpoint-00000000000000000003.ckpt")
	// An older checkpoint that verifies, written by hand beside the newer
	// one (WriteCheckpoint would remove the other).
	data, err := os.ReadFile(ckpt)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(older, data, 0o640); err != nil {
		t.Fatal(err)
	}
	unreadable := filepath.Join(dir, "state", "checkpoint-00000000000000000015.ckpt")
	if err := os.WriteFile(unreadable, []byte{1, 2}, 0o640); err != nil {
		t.Fatal(err)
	}
	flipLastByte(t, ckpt)
	idx, _, ok, err := l.RecoveryCheckpoint()
	var ge *GapError
	if !errors.As(err, &ge) {
		t.Fatalf("got %v, want a *GapError", err)
	}
	if !ok || idx != 3 || ge.From != 4 || ge.Checkpoint != filepath.Base(older) {
		t.Errorf("checkpoint %d (%v), gap %+v; want the one at 3 and replay from 4", idx, ok, ge)
	}
	want := []SkippedCheckpoint{{File: filepath.Base(unreadable), Reason: "unreadable"}, {File: filepath.Base(ckpt), Reason: "bad checksum"}}
	if len(ge.Skipped) != 2 || ge.Skipped[0] != want[0] || ge.Skipped[1] != want[1] {
		t.Errorf("skipped %+v, want %+v", ge.Skipped, want)
	}
}

// TestRecoveryCheckpointWithoutAGap: a bad checkpoint over a log that still
// holds record 1, and a log with no records, are not gaps.
func TestRecoveryCheckpointWithoutAGap(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, Options{SegmentBytes: 60})
	defer func() { _ = l.Close() }()
	if _, _, ok, err := l.RecoveryCheckpoint(); ok || err != nil {
		t.Fatalf("empty log: checkpoint %v, error %v", ok, err)
	}
	appendN(t, l, 1, 20)
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := l.WriteCheckpoint(12, []byte("state@12")); err != nil {
		t.Fatal(err)
	}
	flipLastByte(t, filepath.Join(dir, "state", "checkpoint-00000000000000000012.ckpt"))
	if first := l.FirstIndex(); first != 1 {
		t.Fatalf("precondition: first index held %d, want 1", first)
	}
	if _, _, ok, err := l.RecoveryCheckpoint(); ok || err != nil {
		t.Fatalf("bad checkpoint over the whole log: checkpoint %v, error %v; want neither", ok, err)
	}
}

// TestRecoveryCheckpointRefusesAHoleBetweenSegments: a segment missing
// between two that are held is a gap too, wherever the first segment starts,
// unless the hole lies below the index replay starts at.
func TestRecoveryCheckpointRefusesAHoleBetweenSegments(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, Options{SegmentBytes: 60})
	appendN(t, l, 1, 20)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "state", "00000000000000000005.seg")); err != nil {
		t.Fatalf("precondition: %v", err)
	}
	re := open(t, dir, Options{SegmentBytes: 60})
	defer func() { _ = re.Close() }()
	if re.FirstIndex() != 1 {
		t.Fatalf("precondition: first index held %d, want 1", re.FirstIndex())
	}
	_, _, _, err := re.RecoveryCheckpoint()
	var ge *GapError
	if !errors.As(err, &ge) {
		t.Fatalf("got %v, want a *GapError", err)
	}
	if ge.Missing != 5 || ge.Through != 6 || ge.Segment != "00000000000000000007.seg" {
		t.Errorf("gap %+v, want 5 to 6 missing before 00000000000000000007.seg", ge)
	}
	if !strings.Contains(err.Error(), "00000000000000000007.seg") {
		t.Errorf("the error does not name the segment after the hole: %v", err)
	}
	if err := re.WriteCheckpoint(8, []byte("state@8")); err != nil {
		t.Fatal(err)
	}
	if idx, _, ok, err := re.RecoveryCheckpoint(); err != nil || !ok || idx != 8 {
		t.Fatalf("hole below a checkpoint at 8: %d %v %v, want 8 and no error", idx, ok, err)
	}
}
