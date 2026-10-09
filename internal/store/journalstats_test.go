package store

import (
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/store/faultdisk"
)

// TestJournalStatsTakeNoLock: the metrics read the journal while an append,
// a checkpoint or a repair holds the store's lock, so the read takes none.
func TestJournalStatsTakeNoLock(t *testing.T) {
	s := openStore(t, t.TempDir(), Options{})
	defer func() { _ = s.Close() }()
	upsert(t, s, "a")
	upsert(t, s, "b")
	s.mu.Lock()
	got := make(chan JournalStats, 1)
	go func() { got <- s.JournalStats() }()
	select {
	case js := <-got:
		s.mu.Unlock()
		if js.LastIndex != 2 {
			t.Errorf("last index %d, want 2", js.LastIndex)
		}
	case <-time.After(time.Second):
		s.mu.Unlock()
		t.Fatal("JournalStats waited for the store's lock")
	}
}

// TestJournalStatsTrackCheckpointsAndFsyncs: a checkpoint moves the index
// and time the series report, fsyncs are counted with their time, and a
// repair's checkpoint counts as the latest.
func TestJournalStatsTrackCheckpointsAndFsyncs(t *testing.T) {
	d := faultdisk.New()
	s := openStore(t, t.TempDir(), Options{Sync: d.Sync})
	defer func() { _ = s.Close() }()
	opened := s.JournalStats()
	if opened.CheckpointIndex != 0 || opened.CheckpointAt.IsZero() {
		t.Fatalf("at open: %+v", opened)
	}
	upsert(t, s, "a")
	upsert(t, s, "b")
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	if err := s.Checkpoint(fixed(NewState())); err != nil {
		t.Fatal(err)
	}
	js := s.JournalStats()
	if js.CheckpointIndex != 2 || !js.CheckpointAt.After(opened.CheckpointAt) {
		t.Errorf("after a checkpoint at 2: index %d at %v (opened at %v)", js.CheckpointIndex, js.CheckpointAt, opened.CheckpointAt)
	}
	if js.Fsyncs < 2 || js.FsyncSec <= 0 || js.Sync.Checkpoints != 1 {
		t.Errorf("fsyncs %d taking %v s, checkpoints %d", js.Fsyncs, js.FsyncSec, js.Sync.Checkpoints)
	}
	upsert(t, s, "c")
	d.FailNext(1)
	if err := s.Sync(); err == nil {
		t.Fatal("precondition: the fsync did not fail")
	}
	upsert(t, s, "d")
	if err := s.Sync(); err != nil {
		t.Fatalf("the repair: %v", err)
	}
	js = s.JournalStats()
	if js.CheckpointIndex != 4 || js.Sync.Failures != 1 || js.Sync.Repairs != 1 {
		t.Errorf("after a repair at 4: checkpoint %d, failures %d, repairs %d", js.CheckpointIndex, js.Sync.Failures, js.Sync.Repairs)
	}
}

// TestRecoveredCheckpointIsReported: a reopened store reports the
// checkpoint it recovered from.
func TestRecoveredCheckpointIsReported(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir, Options{})
	upsert(t, s, "a")
	if err := s.Checkpoint(fixed(NewState())); err != nil {
		t.Fatal(err)
	}
	upsert(t, s, "b")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openStore(t, dir, Options{})
	defer func() { _ = s.Close() }()
	if _, err := s.Recover(); err != nil {
		t.Fatal(err)
	}
	if js := s.JournalStats(); js.CheckpointIndex != 1 || js.LastIndex != 2 {
		t.Errorf("reopened: checkpoint %d, last %d; want 1 and 2", js.CheckpointIndex, js.LastIndex)
	}
}
