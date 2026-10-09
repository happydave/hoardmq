package store

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/store/faultdisk"
)

// journaled is a record a caller wrote, and whether the Sync it made
// afterwards returned nil: the node acknowledges a QoS 1 or 2 publish only
// then.
type journaled struct {
	id       string
	released bool
}

func upsert(t *testing.T, s *Store, id string) {
	t.Helper()
	if err := s.SessionUpsert(SessionMeta{Namespace: "ns", ClientID: id, Epoch: 1}); err != nil {
		t.Fatalf("journal %s: %v", id, err)
	}
}

// requireReleasedRecovered cuts the power as the fault disk models it, reopens
// the journal and requires every released caller's record in what it
// recovers.
func requireReleasedRecovered(t *testing.T, dir string, d *faultdisk.Disk, callers []journaled) {
	t.Helper()
	d.Crash(t)
	var released []string
	for _, c := range callers {
		if c.released {
			released = append(released, c.id)
		}
	}
	s, err := Open(dir, Options{Logger: quiet()})
	if err != nil {
		if len(released) > 0 {
			t.Fatalf("released %v after a failed fsync of the journal, and after a power cut the journal does not open: %v", released, err)
		}
		return
	}
	defer func() { _ = s.Close() }()
	st, err := s.Recover()
	if err != nil {
		if len(released) > 0 {
			t.Fatalf("released %v after a failed fsync of the journal, and after a power cut the journal does not recover: %v", released, err)
		}
		return
	}
	for _, id := range released {
		if st.Find("ns", id) == nil {
			t.Errorf("%s was released after a failed fsync of the journal, and is not in the journal after a power cut", id)
		}
	}
}

// TestFailedFsyncReleasesNoCallerItMayHaveDropped: a failed fsync may drop
// every page of the log that was dirty when it ran, and a later fsync that
// succeeds does not write them again. So a caller whose record such an fsync
// covered is released only once its record can be recovered, whichever fsync
// failed and whichever group the caller is in (WI 1812).
func TestFailedFsyncReleasesNoCallerItMayHaveDropped(t *testing.T) {
	t.Run("caller syncs after the failed group", func(t *testing.T) {
		dir := t.TempDir()
		d := faultdisk.New()
		s := openStore(t, dir, Options{Sync: d.Sync})
		upsert(t, s, "a") // a's caller has appended and not synced yet
		upsert(t, s, "b")
		d.FailNext(1)
		if err := s.Sync(); err == nil {
			t.Fatal("precondition: b's fsync, which covers a's record too, did not fail")
		}
		a := journaled{id: "a", released: s.Sync() == nil}
		if err := s.Close(); err != nil {
			t.Logf("close: %v", err)
		}
		requireReleasedRecovered(t, dir, d, []journaled{a, {id: "b"}})
		if !a.released {
			t.Error("a was not released once the journal was repaired")
		}
	})

	t.Run("record written after the failure", func(t *testing.T) {
		dir := t.TempDir()
		d := faultdisk.New()
		s := openStore(t, dir, Options{Sync: d.Sync})
		upsert(t, s, "a")
		d.FailNext(1)
		if err := s.Sync(); err == nil {
			t.Fatal("precondition: a's fsync did not fail")
		}
		// c's bytes reach the disk by the next fsync; a's do not, and they
		// lie before c's in the log.
		upsert(t, s, "c")
		c := journaled{id: "c", released: s.Sync() == nil}
		if err := s.Close(); err != nil {
			t.Logf("close: %v", err)
		}
		requireReleasedRecovered(t, dir, d, []journaled{{id: "a"}, c})
		if !c.released {
			t.Error("c was not released once the journal was repaired")
		}
	})

	t.Run("a roll's fsync fails between the seal and the group's fsync", func(t *testing.T) {
		dir := t.TempDir()
		d := faultdisk.New()
		s := openStore(t, dir, Options{Sync: d.Sync, SegmentBytes: 64})
		upsert(t, s, "a") // fills the first segment past SegmentBytes
		fired := false
		s.syncer.afterTarget = func() {
			if fired {
				return
			}
			fired = true
			// The leader has flushed a and released the store lock. The next
			// append rolls the segment, and the roll's fsync of the file a
			// is in fails: the error goes to the roll, not to the group.
			d.FailNext(1)
			if err := s.SessionUpsert(SessionMeta{Namespace: "ns", ClientID: "c", Epoch: 1}); err == nil {
				t.Error("precondition: the roll's fsync did not fail")
			}
		}
		a := journaled{id: "a", released: s.Sync() == nil}
		if !fired {
			t.Fatal("precondition: the seam never ran")
		}
		if err := s.Close(); err != nil {
			t.Logf("close: %v", err)
		}
		requireReleasedRecovered(t, dir, d, []journaled{a})
		if !a.released {
			t.Error("a was not released once the journal was repaired")
		}
	})

	t.Run("a caller in the failing group", func(t *testing.T) {
		dir := t.TempDir()
		d := faultdisk.New()
		s := openStore(t, dir, Options{Sync: d.Sync})
		upsert(t, s, "a")
		d.FailNext(1)
		a := journaled{id: "a", released: s.Sync() == nil}
		if a.released {
			t.Fatal("the caller whose own fsync failed was released")
		}
		if err := s.Close(); err != nil {
			t.Logf("close: %v", err)
		}
		requireReleasedRecovered(t, dir, d, []journaled{a})
	})
}

// TestFailureReportedToAConcurrentFsyncReleasesNothing: Linux reports a
// write-back error to one fsync of a file. When a roll's or a checkpoint's
// fsync of the active segment runs while a group's is in flight and takes the
// error, the group's returns success over pages the failure dropped (WI 1812;
// known gap 18 of the data-plane design). The group's fsync is held in the
// hook; the other is started and, if it reaches the disk while the held one is
// in flight, it is the one that fails. It reports the failure either before
// the group's returns or only after the group has read the failure count.
func TestFailureReportedToAConcurrentFsyncReleasesNothing(t *testing.T) {
	for _, tc := range []struct {
		name  string
		other func(s *Store) error
		late  bool
	}{
		{"a roll", func(s *Store) error {
			return s.SessionUpsert(SessionMeta{Namespace: "ns", ClientID: "c", Epoch: 1})
		}, false},
		{"a checkpoint", func(s *Store) error { return s.Checkpoint(fixed(NewState())) }, false},
		{"a roll reporting after the group", func(s *Store) error {
			return s.SessionUpsert(SessionMeta{Namespace: "ns", ClientID: "c", Epoch: 1})
		}, true},
		{"a checkpoint reporting after the group", func(s *Store) error { return s.Checkpoint(fixed(NewState())) }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			d := faultdisk.New()
			entered := make(chan int, 8)
			decided := make(chan int, 8)
			release := make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			releaseOther := make(chan struct{})
			unblockOther := sync.OnceFunc(func() { close(releaseOther) })
			t.Cleanup(unblock)
			t.Cleanup(unblockOther)
			var hold atomic.Int32 // the group's call, held until release
			d.Before = func(call int, _ string) {
				entered <- call
				if int32(call) == hold.Load() {
					<-release
				}
			}
			d.After = func(call int, err error) {
				if err != nil && tc.late && hold.Load() != 0 && int32(call) != hold.Load() {
					decided <- call
					<-releaseOther
				}
			}
			s := openStore(t, dir, Options{Sync: d.Sync, SegmentBytes: 64})
			for len(entered) > 0 {
				<-entered
			}
			upsert(t, s, "a")
			next := d.Calls() + 1
			hold.Store(int32(next))
			synced := make(chan error, 1)
			go func() { synced <- s.Sync() }()
			if call := <-entered; call != next {
				t.Fatalf("precondition: the first fsync after a's Sync was call %d, want %d", call, next)
			}
			d.FailNext(1)
			otherDone := make(chan error, 1)
			go func() { otherDone <- tc.other(s) }()
			select {
			case <-entered:
				// It reached the disk while the group's fsync was in flight.
				if tc.late {
					<-decided
				}
			case <-time.After(200 * time.Millisecond):
				// It is waiting for the group's fsync to return.
			}
			unblock()
			a := journaled{id: "a", released: <-synced == nil}
			unblockOther()
			otherErr := <-otherDone
			// The failure goes to whichever fsync reaches the disk first after
			// the hold: the other's while they overlap, else the group's own
			// (a is refused).
			if got := s.SyncFailures(); got < 1 {
				t.Fatalf("precondition: the injected failure never reached an fsync (%d failures)", got)
			}
			t.Logf("a released: %v; the %s: %v", a.released, tc.name, otherErr)
			if !a.released {
				// Its publisher is refused and retries: the retry is released
				// only by the repair.
				if a.released = s.Sync() == nil; !a.released {
					t.Error("a's retry was not released once the journal was repaired")
				}
			}
			if err := s.Close(); err != nil {
				t.Logf("close: %v", err)
			}
			requireReleasedRecovered(t, dir, d, []journaled{a})
		})
	}
}
