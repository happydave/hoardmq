package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/store/faultdisk"
	"github.com/happydave/hoardmq/internal/wal"
)

// TestDamaged: a rewrite that failed on what it read is damage; one that
// failed on a write or an fsync is not.
func TestDamaged(t *testing.T) {
	gap := &wal.GapError{Log: "state", From: 1, Missing: 1, Through: 3, Segment: "s"}
	corrupt := &wal.CorruptError{Log: "state", Segment: "s", Index: 4}
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"a gap", gap, true},
		{"a gap, wrapped", fmt.Errorf("store: journal repair: %w", gap), true},
		{"a checksum failure", corrupt, true},
		{"a checksum failure, wrapped", fmt.Errorf("x: %w", corrupt), true},
		{"a checkpoint or record that does not decode", fmt.Errorf("store: checkpoint 7: %w: %w", errDamaged, errors.New("bad json")), true},
		{"a full disk", &os.PathError{Op: "write", Path: "s", Err: syscall.ENOSPC}, false},
		{"a file gone", os.ErrNotExist, false},
		{"a failed fsync", faultdisk.ErrInjected, false},
		{"nil", nil, false},
	}
	for _, c := range cases {
		if got := damaged(c.err); got != c.want {
			t.Errorf("%s: damaged = %v, want %v", c.name, got, c.want)
		}
	}
}

// damagedAfterFailure journals a and syncs it, damages a's record on the
// disk, fails the fsync covering b, and has one rewrite fail on the damage.
// It returns the segment's bytes as damaged, to restore from.
func damagedAfterFailure(t *testing.T, dir string, s *Store, d *faultdisk.Disk) (string, []byte) {
	t.Helper()
	upsert(t, s, "a")
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	seg := filepath.Join(dir, "state", "00000000000000000001.seg")
	b, err := os.ReadFile(seg)
	if err != nil {
		t.Fatal(err)
	}
	b[32+9] ^= 0xFF
	if err := os.WriteFile(seg, b, 0o640); err != nil {
		t.Fatal(err)
	}
	upsert(t, s, "b")
	d.FailNext(1)
	if err := s.Sync(); err == nil {
		t.Fatal("precondition: the armed fsync did not fail")
	}
	if err := s.Unrecoverable(); err != nil {
		t.Fatalf("unrecoverable before any rewrite was tried: %v", err)
	}
	return seg, b
}

func restore(t *testing.T, seg string, b []byte) {
	t.Helper()
	b[32+9] ^= 0xFF
	if err := os.WriteFile(seg, b, 0o640); err != nil {
		t.Fatal(err)
	}
}

// TestRewriteOverADamagedRecordIsUnrecoverable: a synced record damaged on the
// disk, then a failed fsync: every rewrite reads the record and fails, and the
// store says so. Once the record is restored, a rewrite that succeeds, or one
// that fails on its own fsync, leaves nothing to report.
func TestRewriteOverADamagedRecordIsUnrecoverable(t *testing.T) {
	saved := repairBackoff
	repairBackoff = 10 * time.Millisecond
	t.Cleanup(func() { repairBackoff = saved })
	for _, then := range []string{"a rewrite that succeeds", "a rewrite whose fsync fails"} {
		t.Run(then, func(t *testing.T) {
			dir := t.TempDir()
			d := faultdisk.New()
			s := openStore(t, dir, Options{Sync: d.Sync})
			defer func() { _ = s.Close() }()
			seg, b := damagedAfterFailure(t, dir, s, d)
			for i := 0; i < 2; i++ {
				time.Sleep(2 * repairBackoff)
				if err := s.Sync(); err == nil {
					t.Fatal("a rewrite over a damaged record succeeded")
				}
				var corrupt *wal.CorruptError
				if err := s.Unrecoverable(); !errors.As(err, &corrupt) {
					t.Fatalf("attempt %d: Unrecoverable() = %v, want the checksum failure", i+1, err)
				}
			}
			restore(t, seg, b)
			time.Sleep(2 * repairBackoff)
			if then == "a rewrite whose fsync fails" {
				d.FailNext(1)
				if err := s.Sync(); err == nil {
					t.Fatal("precondition: the rewrite whose fsync was failed succeeded")
				}
			} else if err := s.Sync(); err != nil {
				t.Fatalf("the rewrite over the restored record: %v", err)
			}
			if err := s.Unrecoverable(); err != nil {
				t.Fatalf("Unrecoverable() = %v after %s", err, then)
			}
		})
	}
}

// TestRewriteOverWhatDoesNotDecodeIsUnrecoverable: a checkpoint whose checksum
// verifies but which does not decode, and a record that does not apply, on
// the disk or held in memory, fail every rewrite as damage.
func TestRewriteOverWhatDoesNotDecodeIsUnrecoverable(t *testing.T) {
	cases := []struct {
		name  string
		setup func(t *testing.T, dir string, s *Store, d *faultdisk.Disk)
	}{
		{"a checkpoint that does not decode", func(t *testing.T, dir string, s *Store, d *faultdisk.Disk) {
			upsert(t, s, "a")
			st, err := s.Recover()
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Checkpoint(fixed(st)); err != nil {
				t.Fatal(err)
			}
			payload := []byte("not a checkpoint")
			sum := make([]byte, 4, 4+len(payload))
			binary.BigEndian.PutUint32(sum, crc32.Checksum(payload, crc32.MakeTable(crc32.Castagnoli)))
			if err := os.WriteFile(checkpointFile(t, dir), append(sum, payload...), 0o640); err != nil {
				t.Fatal(err)
			}
			upsert(t, s, "b")
			d.FailNext(1)
		}},
		{"a synced record that does not apply", func(t *testing.T, dir string, s *Store, d *faultdisk.Disk) {
			if err := s.AppendRaw(recSessionUpsert, []byte("not a record")); err != nil {
				t.Fatal(err)
			}
			if err := s.Sync(); err != nil {
				t.Fatal(err)
			}
			upsert(t, s, "b")
			d.FailNext(1)
		}},
		{"a kept record that does not apply", func(t *testing.T, dir string, s *Store, d *faultdisk.Disk) {
			upsert(t, s, "a")
			if err := s.AppendRaw(recSessionUpsert, []byte("not a record")); err != nil {
				t.Fatal(err)
			}
			d.FailNext(1)
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			d := faultdisk.New()
			s := openStore(t, dir, Options{Sync: d.Sync})
			defer func() { _ = s.Close() }()
			c.setup(t, dir, s, d)
			if err := s.Sync(); err == nil {
				t.Fatal("precondition: the armed fsync did not fail")
			}
			if err := s.Sync(); err == nil {
				t.Fatal("precondition: the rewrite succeeded")
			}
			if err := s.Unrecoverable(); !errors.Is(err, errDamaged) {
				t.Fatalf("Unrecoverable() = %v, want the rewrite's damage", err)
			}
		})
	}
}

// TestFullDiskIsNotUnrecoverable: a rewrite that fails because the disk is
// full is retried and succeeds once it has room, so the store never says it
// cannot recover.
func TestFullDiskIsNotUnrecoverable(t *testing.T) {
	saved := repairBackoff
	repairBackoff = 10 * time.Millisecond
	t.Cleanup(func() { repairBackoff = saved })
	d := faultdisk.New()
	s := openStore(t, t.TempDir(), Options{Sync: d.Sync, Write: d.Write})
	defer func() { _ = s.Close() }()
	upsert(t, s, "a")
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	d.FillAfter(10)
	upsert(t, s, "b")
	if err := s.Sync(); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("precondition: Sync on a full disk: %v, want ENOSPC", err)
	}
	time.Sleep(2 * repairBackoff)
	if err := s.Sync(); err == nil {
		t.Fatal("precondition: the rewrite succeeded on a full disk")
	}
	if got := s.SyncStats().RepairFailures; got < 1 {
		t.Fatalf("precondition: %d failed rewrites, want at least 1", got)
	}
	if err := s.Unrecoverable(); err != nil {
		t.Fatalf("a full disk reported unrecoverable: %v", err)
	}
	d.Free()
	deadline := time.Now().Add(5 * time.Second)
	for s.Sync() != nil {
		if time.Now().After(deadline) {
			t.Fatal("the store did not recover once the disk had room")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := s.Unrecoverable(); err != nil {
		t.Fatalf("Unrecoverable() = %v after the rewrite succeeded", err)
	}
}
