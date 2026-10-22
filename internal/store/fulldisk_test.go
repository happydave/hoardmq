package store

import (
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/store/faultdisk"
)

// TestStoreRecoversOnceAFullDiskHasRoom: a write that fails because the disk
// is full refuses the callers whose records it held, and once the disk has
// room again the store takes writes and acknowledges them without a restart.
// Every record whose Sync returned nil, and the refused one the store still
// held, is recovered after a power cut.
func TestStoreRecoversOnceAFullDiskHasRoom(t *testing.T) {
	saved := repairBackoff
	repairBackoff = 20 * time.Millisecond
	t.Cleanup(func() { repairBackoff = saved })
	dir := t.TempDir()
	d := faultdisk.New()
	s := openStore(t, dir, Options{Sync: d.Sync, Write: d.Write})
	upsert(t, s, "a")
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	d.FillAfter(10) // part of b's record fits, then the disk is full
	upsert(t, s, "b")
	err := s.Sync()
	if !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("Sync on a full disk: %v, want ENOSPC", err)
	}
	d.Free()

	// Room again: once the rewrite's backoff has passed, a new record is
	// taken and acknowledged; inside it the append is refused.
	deadline := time.Now().Add(5 * time.Second)
	for {
		err = s.SessionUpsert(SessionMeta{Namespace: "ns", ClientID: "c", Epoch: 1})
		if err == nil {
			err = s.Sync()
		}
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the store still refuses 5 s after the disk has room: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	upsert(t, s, "d")
	if err := s.Sync(); err != nil {
		t.Fatalf("a Sync after the recovery: %v", err)
	}
	if got := s.SyncStats().Repairs; got != 1 {
		t.Fatalf("rewrites = %d, want 1: the store rewrote again after it recovered", got)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	d.Crash(t)

	r := openStore(t, dir, Options{})
	defer func() { _ = r.Close() }()
	st, err := r.Recover()
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	for _, id := range []string{"a", "b", "c", "d"} {
		if st.Find("ns", id) == nil {
			t.Errorf("%s not recovered", id)
		}
	}
}

// TestFullDiskRefusesUntilItHasRoom: while the disk stays full every append is
// refused, the rewrite is tried no more than once a backoff, and nothing is
// acknowledged; once it has room the store takes and acknowledges writes.
func TestFullDiskRefusesUntilItHasRoom(t *testing.T) {
	saved := repairBackoff
	repairBackoff = 200 * time.Millisecond
	t.Cleanup(func() { repairBackoff = saved })
	d := faultdisk.New()
	s := openStore(t, t.TempDir(), Options{Sync: d.Sync, Write: d.Write})
	defer func() { _ = s.Close() }()
	upsert(t, s, "a")
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	d.FillAfter(0)
	upsert(t, s, "b")
	if err := s.Sync(); err == nil {
		t.Fatal("precondition: a Sync on a full disk succeeded")
	}
	began := time.Now()
	for i := 0; i < 5; i++ {
		if err := s.SessionUpsert(SessionMeta{Namespace: "ns", ClientID: "c", Epoch: 1}); err == nil {
			t.Fatalf("attempt %d: an append on a full disk was taken", i)
		}
		if err := s.Sync(); err == nil {
			t.Fatalf("attempt %d: a Sync on a full disk returned nil", i)
		}
	}
	if time.Since(began) < repairBackoff {
		if got := s.SyncStats().RepairFailures; got != 1 {
			t.Fatalf("rewrites tried inside one backoff = %d, want 1", got)
		}
	}
	d.Free()
	time.Sleep(repairBackoff)
	upsert(t, s, "c")
	if err := s.Sync(); err != nil {
		t.Fatalf("a Sync once the disk has room: %v", err)
	}
}

// fullDiskAfterB returns a store whose write of b's record failed on a full
// disk that has since been given room; nothing has been appended since.
func fullDiskAfterB(t *testing.T, dir string) (*Store, *faultdisk.Disk) {
	t.Helper()
	saved := repairBackoff
	repairBackoff = 20 * time.Millisecond
	t.Cleanup(func() { repairBackoff = saved })
	d := faultdisk.New()
	s := openStore(t, dir, Options{Sync: d.Sync, Write: d.Write})
	upsert(t, s, "a")
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	d.FillAfter(10)
	upsert(t, s, "b")
	if err := s.Sync(); err == nil {
		t.Fatal("precondition: a Sync on a full disk succeeded")
	}
	d.Free()
	time.Sleep(repairBackoff)
	return s, d
}

// TestSyncAloneRewritesAfterAFailedWrite: with nothing appended after the
// failed write, a Sync still rewrites the journal once the disk has room,
// and b's record, which the store kept, is recovered.
func TestSyncAloneRewritesAfterAFailedWrite(t *testing.T) {
	dir := t.TempDir()
	s, d := fullDiskAfterB(t, dir)
	if err := s.Sync(); err != nil {
		t.Fatalf("a Sync once the disk has room: %v", err)
	}
	if got := s.SyncStats().Repairs; got != 1 {
		t.Fatalf("rewrites = %d, want 1", got)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	d.Crash(t)
	r := openStore(t, dir, Options{})
	defer func() { _ = r.Close() }()
	st, err := r.Recover()
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "b"} {
		if st.Find("ns", id) == nil {
			t.Errorf("%s not recovered", id)
		}
	}
}

// TestCheckpointRewritesAfterAFailedWrite: a failed write makes the store ask
// for a checkpoint, and the checkpoint rewrites the journal, so b's record
// survives a restart with no further Sync.
func TestCheckpointRewritesAfterAFailedWrite(t *testing.T) {
	dir := t.TempDir()
	s, d := fullDiskAfterB(t, dir)
	if !s.NeedsCheckpoint() {
		t.Fatal("a journal whose write failed does not ask for a checkpoint")
	}
	if err := s.Checkpoint(fixed(NewState())); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if got := s.SyncStats().Repairs; got != 1 {
		t.Fatalf("rewrites = %d, want 1", got)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	d.Crash(t)
	r := openStore(t, dir, Options{})
	defer func() { _ = r.Close() }()
	st, err := r.Recover()
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "b"} {
		if st.Find("ns", id) == nil {
			t.Errorf("%s not recovered", id)
		}
	}
}
