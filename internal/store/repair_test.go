package store

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/store/faultdisk"
)

// journalScript writes a mix of record types, checkpoints part-way from the
// log's own recovery, and syncs; fail, when set, fails one fsync after the
// checkpoint and has the last Sync repair the journal.
func journalScript(t *testing.T, s *Store, fail func()) {
	t.Helper()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	k := func(id string) Key { return Key{Namespace: "ns", ClientID: id, Epoch: 1} }
	for _, id := range []string{"a", "b", "c"} {
		must(s.SessionUpsert(SessionMeta{Namespace: "ns", ClientID: id, Epoch: 1, ExpiryInterval: 60}))
		must(s.Subscribe(k(id), SubscriptionRecord{Filter: "t/" + id, QoS: 1}))
	}
	must(s.Enqueue(k("a"), Entry{Seq: 1, Message: msg("ns", "t/a", "one"), QoS: 1, PacketID: 1}))
	must(s.Sync())
	st, err := s.Recover()
	must(err)
	must(s.Checkpoint(fixed(st)))
	must(s.Enqueue(k("a"), Entry{Seq: 2, Message: msg("ns", "t/a", "two"), QoS: 1, PacketID: 2}))
	must(s.Sent(k("a"), 2, 2))
	must(s.Retain(msg("ns", "r", "kept")))
	must(s.Sync())
	must(s.Complete(k("a"), 1))
	must(s.SessionDelete(k("b")))
	if fail != nil {
		fail()
		if err := s.Sync(); err == nil {
			t.Fatal("precondition: the armed fsync did not fail")
		}
	}
	must(s.Unsubscribe(k("c"), "t/c"))
	must(s.Enqueue(k("c"), Entry{Seq: 1, Message: msg("ns", "t/c", "three"), QoS: 1, PacketID: 1}))
	must(s.Sync())
}

func recovered(t *testing.T, dir string) string {
	t.Helper()
	s, err := Open(dir, Options{Logger: quiet()})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = s.Close() }()
	st, err := s.Recover()
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	b, err := json.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// TestRepairRecoversWhatAnUndamagedLogWould: after a failed fsync and the
// repair, recovery from what a power cut leaves yields the state recovery
// yields from the same records written with no failure, and the segments that
// held the lost records are gone (J4).
func TestRepairRecoversWhatAnUndamagedLogWould(t *testing.T) {
	clean := t.TempDir()
	s := openStore(t, clean, Options{})
	journalScript(t, s, nil)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	want := recovered(t, clean)

	damaged := t.TempDir()
	d := faultdisk.New()
	s = openStore(t, damaged, Options{Sync: d.Sync})
	journalScript(t, s, func() { d.FailNext(1) })
	if got := s.SyncStats().Repairs; got != 1 {
		t.Fatalf("repairs = %d, want 1", got)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	d.Crash(t)
	if _, err := os.Stat(filepath.Join(damaged, "state", "00000000000000000001.seg")); !os.IsNotExist(err) {
		t.Errorf("the segment holding the lost records is still there (stat: %v)", err)
	}
	if got := recovered(t, damaged); got != want {
		t.Fatalf("recovered after the repair:\n%s\nwant, from the undamaged log:\n%s", got, want)
	}
}

// TestFailedRepairBacksOff: a repair whose own checkpoint fsync fails fails
// its group; a Sync inside the backoff fails at once without touching the
// disk; after it, the next Sync repairs.
func TestFailedRepairBacksOff(t *testing.T) {
	saved := repairBackoff
	repairBackoff = 100 * time.Millisecond
	t.Cleanup(func() { repairBackoff = saved })
	d := faultdisk.New()
	s := openStore(t, t.TempDir(), Options{Sync: d.Sync})
	defer func() { _ = s.Close() }()
	upsert(t, s, "a")
	d.FailNext(2) // the group's fsync, then the repair's checkpoint
	if err := s.Sync(); err == nil {
		t.Fatal("precondition: the group's fsync did not fail")
	}
	failedAt := time.Now()
	if err := s.Sync(); err == nil {
		t.Fatal("the repair succeeded although its checkpoint's fsync failed")
	}
	if got := s.SyncStats().RepairFailures; got != 1 {
		t.Fatalf("repair failures = %d, want 1", got)
	}
	calls := d.Calls()
	if err := s.Sync(); err == nil {
		t.Fatal("a Sync inside the backoff succeeded")
	}
	after := d.Calls()
	if time.Since(failedAt) < repairBackoff && after != calls {
		t.Fatalf("a Sync inside the backoff made %d fsync calls", after-calls)
	}
	time.Sleep(time.Until(failedAt.Add(repairBackoff + 10*time.Millisecond)))
	if err := s.Sync(); err != nil {
		t.Fatalf("the Sync after the backoff: %v", err)
	}
	if got := s.SyncStats().Repairs; got != 1 {
		t.Fatalf("repairs = %d, want 1", got)
	}
}

// TestKeptRecordsAreBounded: records nobody syncs are synced in the
// background once they pass the bound, so the store does not keep them all.
func TestKeptRecordsAreBounded(t *testing.T) {
	d := faultdisk.New()
	s := openStore(t, t.TempDir(), Options{Sync: d.Sync})
	defer func() { _ = s.Close() }()
	most := 0
	for i := 0; i < 10000; i++ {
		if err := s.AppendRaw(recSessionUpsert, []byte(`{"ns":"ns","id":"c`+itoa(i)+`","epoch":1}`)); err != nil {
			t.Fatal(err)
		}
		s.mu.Lock()
		n := len(s.kept)
		s.mu.Unlock()
		if n > most {
			most = n
		}
		if n < keepRecords {
			continue
		}
		// Without this wait the count also holds whatever is appended before
		// the background sync's goroutine is scheduled, which depends on the
		// machine's load rather than on the store.
		deadline := time.Now().Add(10 * time.Second)
		for s.keptSyncing.Load() {
			if time.Now().After(deadline) {
				t.Fatal("no background sync finished")
			}
			time.Sleep(50 * time.Microsecond)
		}
		if c, last := s.covered.Load(), s.appendedIdx.Load(); c < last {
			t.Fatalf("the background sync covered up to %d, not the %d records appended before it", c, last)
		}
	}
	if most > keepRecords {
		t.Fatalf("kept up to %d records, bound %d", most, keepRecords)
	}
	if most < keepRecords {
		t.Fatalf("precondition: the kept records never reached the bound (%d)", most)
	}
	if got := s.SyncStats().Syncs; got < 2 {
		t.Fatalf("background syncs = %d, want at least 2", got)
	}
}

// TestCheckpointRepairsAJournalThatIsNotWhole: after a failed fsync the store
// asks for a checkpoint, and the checkpoint it is given is replaced by the
// repair's, which holds the log's records rather than the caller's state.
func TestCheckpointRepairsAJournalThatIsNotWhole(t *testing.T) {
	dir := t.TempDir()
	d := faultdisk.New()
	s := openStore(t, dir, Options{Sync: d.Sync})
	upsert(t, s, "a")
	upsert(t, s, "b")
	if s.NeedsCheckpoint() {
		t.Fatal("precondition: a fresh store asks for a checkpoint")
	}
	d.FailNext(1)
	if err := s.Sync(); err == nil {
		t.Fatal("precondition: the armed fsync did not fail")
	}
	if !s.NeedsCheckpoint() {
		t.Fatal("a journal that is not whole does not ask for a checkpoint")
	}
	if err := s.Checkpoint(fixed(NewState())); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if s.NeedsCheckpoint() {
		t.Fatal("still asks for a checkpoint after the repair")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	d.Crash(t)
	r, err := Open(dir, Options{Logger: quiet()})
	if err != nil {
		t.Fatalf("reopen after the repair: %v", err)
	}
	defer func() { _ = r.Close() }()
	st, err := r.Recover()
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "b"} {
		if st.Find("ns", id) == nil {
			t.Errorf("%s is not in the journal after the checkpoint's repair", id)
		}
	}
}

// TestKeptRecordIsACopyOfARawPayload: a peer's record is kept as a copy, so a
// caller reusing its buffer does not change what the repair writes.
func TestKeptRecordIsACopyOfARawPayload(t *testing.T) {
	dir := t.TempDir()
	d := faultdisk.New()
	s := openStore(t, dir, Options{Sync: d.Sync})
	buf := []byte(`{"ns":"ns","id":"a","epoch":1}`)
	if err := s.AppendRaw(recSessionUpsert, buf); err != nil {
		t.Fatal(err)
	}
	copy(buf, `{"ns":"ns","id":"z","epoch":1}`)
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
	r, err := Open(dir, Options{Logger: quiet()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	st, err := r.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if st.Find("ns", "a") == nil || st.Find("ns", "z") != nil {
		t.Fatalf("recovered %d sessions; want a, which the caller's buffer held when it appended", len(st.Sessions))
	}
}

// TestRepairWithAnEmptyActiveSegment: a second failure before anything is
// appended after a repair finds the active segment empty; the repair then
// starts no segment and still makes the journal whole.
func TestRepairWithAnEmptyActiveSegment(t *testing.T) {
	dir := t.TempDir()
	d := faultdisk.New()
	s := openStore(t, dir, Options{Sync: d.Sync})
	upsert(t, s, "a")
	d.FailNext(1)
	if err := s.Sync(); err == nil {
		t.Fatal("precondition: the armed fsync did not fail")
	}
	upsert(t, s, "b")
	if err := s.Sync(); err != nil {
		t.Fatalf("the first repair: %v", err)
	}
	d.FailNext(1)
	if err := s.Checkpoint(fixed(NewState())); err == nil {
		t.Fatal("precondition: the checkpoint's fsync did not fail")
	}
	// Nothing appended since the first repair: the second finds the segment
	// it started empty.
	if err := s.Checkpoint(fixed(NewState())); err != nil {
		t.Fatalf("the second repair: %v", err)
	}
	upsert(t, s, "c")
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if got := s.SyncStats().Repairs; got != 2 {
		t.Fatalf("repairs = %d, want 2", got)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	d.Crash(t)
	r, err := Open(dir, Options{Logger: quiet()})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = r.Close() }()
	st, err := r.Recover()
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "b", "c"} {
		if st.Find("ns", id) == nil {
			t.Errorf("%s is not in the journal", id)
		}
	}
}
