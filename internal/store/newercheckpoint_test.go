package store

import (
	"bytes"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/happydave/hoardmq/internal/store/faultdisk"
	"github.com/happydave/hoardmq/internal/wal"
)

// repairCutBeforeItsSegment journals a to e and syncs them, fails the fsync
// that covers f and g, and has the next Sync repair the journal: a checkpoint
// at 7, then a new segment at 8. The power is cut while that segment's header
// is being synced, and what the cut leaves is copied into crashed, with the
// new segment as seg leaves it.
func repairCutBeforeItsSegment(t *testing.T, dir, crashed string, seg func(t *testing.T, path string)) {
	t.Helper()
	const newSeg = "00000000000000000008.seg"
	d := faultdisk.New()
	var cut atomic.Bool
	d.Before = func(_ int, path string) {
		if filepath.Base(path) == newSeg && cut.CompareAndSwap(false, true) {
			d.CrashCopy(t, filepath.Join(dir, "state"), filepath.Join(crashed, "state"))
			seg(t, filepath.Join(crashed, "state", newSeg))
		}
	}
	s := openStore(t, dir, Options{Sync: d.Sync})
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		upsert(t, s, id)
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	upsert(t, s, "f")
	upsert(t, s, "g")
	d.FailNext(1)
	if err := s.Sync(); err == nil {
		t.Fatal("precondition: the armed fsync did not fail")
	}
	if err := s.Sync(); err != nil {
		t.Fatalf("the repair: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if !cut.Load() {
		t.Fatalf("precondition: the repair's new segment %s was never synced", newSeg)
	}
}

// logEnd opens the log alone and returns its last index.
func logEnd(t *testing.T, dir string) uint64 {
	t.Helper()
	l, err := wal.Open(dir, "state", wal.Options{Logger: quiet()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.LastIndex()
}

// TestRecordAfterARestartOverANewerCheckpointIsRecovered: a power cut inside
// the repair, after its checkpoint is synced and before its new segment
// reaches the disk, leaves a checkpoint at 7 over a log that ends at 5. The
// node recovers a to g from the checkpoint; a record synced after that restart
// is recovered at the next one.
func TestRecordAfterARestartOverANewerCheckpointIsRecovered(t *testing.T) {
	for _, tc := range []struct {
		name string
		seg  func(t *testing.T, path string)
		ends uint64 // where the log alone ends after the cut
	}{
		{"the new segment absent", func(t *testing.T, path string) {
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
		}, 5},
		{"the new segment empty", func(t *testing.T, path string) {
			if err := os.Truncate(path, 0); err != nil {
				t.Fatal(err)
			}
		}, 5},
		{"the new segment a header of zeros", func(t *testing.T, path string) {
			if err := os.WriteFile(path, make([]byte, 32), 0o640); err != nil {
				t.Fatal(err)
			}
		}, 5},
		{"the new segment's header whole", func(*testing.T, string) {}, 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir, crashed := t.TempDir(), t.TempDir()
			repairCutBeforeItsSegment(t, dir, crashed, tc.seg)
			if got := filepath.Base(checkpointFile(t, crashed)); got != "checkpoint-00000000000000000007.ckpt" {
				t.Fatalf("precondition: the cut left checkpoint %s, want the repair's at 7", got)
			}
			if got := logEnd(t, crashed); got != tc.ends {
				t.Fatalf("precondition: after the cut the log ends at %d, want %d", got, tc.ends)
			}

			r := openStore(t, crashed, Options{})
			st, err := r.Recover()
			if err != nil {
				t.Fatal(err)
			}
			if got := sessionIDs(st); !slices.Equal(got, []string{"a", "b", "c", "d", "e", "f", "g"}) {
				t.Fatalf("recovered %v after the cut, want a to g", got)
			}
			upsert(t, r, "h")
			if err := r.Sync(); err != nil {
				t.Fatal(err)
			}
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}

			again := openStore(t, crashed, Options{})
			defer func() { _ = again.Close() }()
			st, err = again.Recover()
			if err != nil {
				t.Fatal(err)
			}
			if got := sessionIDs(st); !slices.Equal(got, []string{"a", "b", "c", "d", "e", "f", "g", "h"}) {
				t.Fatalf("recovered %v at the restart after h was synced, want a to h", got)
			}
		})
	}
}

// TestRepairAfterARestartOverAnEmptySegment: a journal whose last segment
// holds no record (a repair's new segment, then a restart before any append)
// is repaired after a failed fsync, and what it held is recovered.
func TestRepairAfterARestartOverAnEmptySegment(t *testing.T) {
	dir := t.TempDir()
	d := faultdisk.New()
	s := openStore(t, dir, Options{Sync: d.Sync})
	for _, id := range []string{"a", "b"} {
		upsert(t, s, id)
	}
	d.FailNext(1)
	if err := s.Sync(); err == nil {
		t.Fatal("precondition: the armed fsync did not fail")
	}
	if err := s.Sync(); err != nil {
		t.Fatalf("the first repair: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "state", "00000000000000000003.seg")); err != nil {
		t.Fatalf("precondition: the repair's empty segment: %v", err)
	}

	s = openStore(t, dir, Options{Sync: d.Sync})
	if _, err := s.Recover(); err != nil {
		t.Fatal(err)
	}
	d.FailNext(1)
	if err := s.Sync(); err == nil {
		t.Fatal("precondition: the armed fsync did not fail")
	}
	if err := s.Sync(); err != nil {
		t.Fatalf("the repair after the restart: %v", err)
	}
	upsert(t, s, "c")
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	d.Crash(t)

	again := openStore(t, dir, Options{})
	defer func() { _ = again.Close() }()
	st, err := again.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if got := sessionIDs(st); !slices.Equal(got, []string{"a", "b", "c"}) {
		t.Fatalf("recovered %v, want a, b and c", got)
	}
}

// cutRepairAndRestart leaves crashed as a power cut inside the repair leaves
// it, with the new segment never on the disk, and restarts once over it.
func cutRepairAndRestart(t *testing.T) string {
	t.Helper()
	crashed := t.TempDir()
	repairCutBeforeItsSegment(t, t.TempDir(), crashed, func(t *testing.T, path string) {
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
	})
	return crashed
}

// TestTheStateAfterANewerCheckpointIsAccepted: the hole the restart leaves
// between the old segment (to 5) and the new one (from 8) lies below where
// recovery replays from the checkpoint at 7. A checkpoint, a restart and a
// repair over it all recover; with the checkpoint damaged, records 6 and 7 are
// held nowhere else and recovery refuses.
func TestTheStateAfterANewerCheckpointIsAccepted(t *testing.T) {
	restartWithH := func(t *testing.T, dir string) *Store {
		t.Helper()
		r := openStore(t, dir, Options{})
		if _, err := r.Recover(); err != nil {
			t.Fatal(err)
		}
		upsert(t, r, "h")
		if err := r.Sync(); err != nil {
			t.Fatal(err)
		}
		return r
	}
	recoverIDs := func(t *testing.T, dir string, opts Options) []string {
		t.Helper()
		s := openStore(t, dir, opts)
		defer func() { _ = s.Close() }()
		st, err := s.Recover()
		if err != nil {
			t.Fatal(err)
		}
		return sessionIDs(st)
	}
	atoH := []string{"a", "b", "c", "d", "e", "f", "g", "h"}

	t.Run("a checkpoint drops the old segment", func(t *testing.T) {
		dir := cutRepairAndRestart(t)
		r := restartWithH(t, dir)
		st, err := r.Recover()
		if err != nil {
			t.Fatal(err)
		}
		if err := r.Checkpoint(fixed(st)); err != nil {
			t.Fatal(err)
		}
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(filepath.Join(dir, "state", "00000000000000000001.seg")); !os.IsNotExist(err) {
			t.Errorf("the old segment is still there after the checkpoint (stat: %v)", err)
		}
		if got := recoverIDs(t, dir, Options{}); !slices.Equal(got, atoH) {
			t.Fatalf("recovered %v, want a to h", got)
		}
	})

	t.Run("a repair after a restart with no append", func(t *testing.T) {
		dir := cutRepairAndRestart(t)
		r := openStore(t, dir, Options{})
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
		d := faultdisk.New()
		s := openStore(t, dir, Options{Sync: d.Sync})
		if _, err := s.Recover(); err != nil {
			t.Fatal(err)
		}
		d.FailNext(1)
		if err := s.Sync(); err == nil {
			t.Fatal("precondition: the armed fsync did not fail")
		}
		if err := s.Sync(); err != nil {
			t.Fatalf("the repair: %v", err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		d.Crash(t)
		if got := recoverIDs(t, dir, Options{}); !slices.Equal(got, atoH[:7]) {
			t.Fatalf("recovered %v, want a to g", got)
		}
	})

	t.Run("the checkpoint damaged", func(t *testing.T) {
		dir := cutRepairAndRestart(t)
		r := restartWithH(t, dir)
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
		corrupt(t, checkpointFile(t, dir))
		s := openStore(t, dir, Options{})
		defer func() { _ = s.Close() }()
		_, err := s.Recover()
		var gap *wal.GapError
		if !errors.As(err, &gap) || gap.Missing != 6 || gap.Through != 7 {
			t.Fatalf("recovery over the damaged checkpoint: %v, want a gap of records 6 to 7", err)
		}
	})
}

// TestABadCheckpointIsLoggedOnceAtStart: a start reads the checkpoint at Open
// and again at Recover; a checkpoint passed over is logged once.
func TestABadCheckpointIsLoggedOnceAtStart(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir, Options{})
	upsert(t, s, "a")
	st, err := s.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(fixed(st)); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	corrupt(t, checkpointFile(t, dir))
	var logs bytes.Buffer
	r, err := Open(dir, Options{Logger: slog.New(slog.NewTextHandler(&logs, nil))})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	_, _ = r.Recover()
	if n := strings.Count(logs.String(), "skipping checkpoint with a bad checksum"); n != 1 {
		t.Fatalf("the bad checkpoint was logged %d times at start, want once:\n%s", n, logs.String())
	}
}

// TestAnOlderVerifiedCheckpointDecidesWhereAppendsContinue: beside the
// repair's checkpoint at 7, a newer checkpoint file that fails its checksum
// is passed over, as recovery passes it over: the next record is 8, and it is
// recovered.
func TestAnOlderVerifiedCheckpointDecidesWhereAppendsContinue(t *testing.T) {
	dir := cutRepairAndRestart(t)
	bad := filepath.Join(dir, "state", "checkpoint-00000000000000000009.ckpt")
	if err := os.WriteFile(bad, []byte("not a checkpoint"), 0o640); err != nil {
		t.Fatal(err)
	}
	r := openStore(t, dir, Options{})
	if r.LastIndex() != 7 {
		t.Fatalf("opened with the next record at %d, want 8", r.LastIndex()+1)
	}
	if _, err := r.Recover(); err != nil {
		t.Fatal(err)
	}
	upsert(t, r, "h")
	if err := r.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	again := openStore(t, dir, Options{})
	defer func() { _ = again.Close() }()
	st, err := again.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if got := sessionIDs(st); !slices.Equal(got, []string{"a", "b", "c", "d", "e", "f", "g", "h"}) {
		t.Fatalf("recovered %v, want a to h", got)
	}
}

// TestOpenFailsWhenItCannotContinueAfterTheCheckpoint: when the segment past
// the checkpoint cannot be made, Open fails and leaves no segment behind; a
// later Open that can make it recovers and appends after the checkpoint.
func TestOpenFailsWhenItCannotContinueAfterTheCheckpoint(t *testing.T) {
	dir := cutRepairAndRestart(t)
	d := faultdisk.New()
	d.FailNext(1)
	if s, err := Open(dir, Options{Sync: d.Sync, Logger: quiet()}); err == nil {
		_ = s.Close()
		t.Fatal("Open succeeded although the segment past the checkpoint could not be synced")
	}
	if _, err := os.Stat(filepath.Join(dir, "state", "00000000000000000008.seg")); !os.IsNotExist(err) {
		t.Fatalf("the failed segment was left behind (stat: %v)", err)
	}
	r := openStore(t, dir, Options{})
	defer func() { _ = r.Close() }()
	if r.LastIndex() != 7 {
		t.Fatalf("reopened with the next record at %d, want 8", r.LastIndex()+1)
	}
}
