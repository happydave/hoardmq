package store

import (
	"errors"
	"os"
	"sync/atomic"
	"testing"
)

// TestSyncFailuresCountsAFailedFsync: the count rises with a failed fsync and
// is not lowered by a later one that succeeds, which is what a caller reading
// it after its own successful sync relies on.
func TestSyncFailuresCountsAFailedFsync(t *testing.T) {
	var fail atomic.Bool
	s := openStore(t, t.TempDir(), Options{Sync: func(f *os.File) error {
		if fail.CompareAndSwap(true, false) {
			return errors.New("injected fsync failure")
		}
		return f.Sync()
	}})
	defer func() { _ = s.Close() }()
	if got := s.SyncFailures(); got != 0 {
		t.Fatalf("SyncFailures on a fresh store = %d, want 0", got)
	}
	if err := s.AppendRaw(recSessionUpsert, []byte(`{"i":0}`)); err != nil {
		t.Fatal(err)
	}
	fail.Store(true)
	if err := s.Sync(); err == nil {
		t.Fatal("precondition: the armed fsync did not fail")
	}
	if got := s.SyncFailures(); got != 1 {
		t.Fatalf("SyncFailures after a failed fsync = %d, want 1", got)
	}
	if err := s.AppendRaw(recSessionUpsert, []byte(`{"i":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := s.Sync(); err != nil {
		t.Fatalf("the next fsync: %v", err)
	}
	if got := s.SyncFailures(); got != 1 {
		t.Fatalf("SyncFailures after a later fsync succeeded = %d, want 1", got)
	}
}
