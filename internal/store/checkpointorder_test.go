package store

import (
	"encoding/json"
	"testing"

	extstore "github.com/happydave/hoardmq/ext/store"
	"github.com/happydave/hoardmq/internal/store/faultdisk"
)

// entriesOf recovers the journal in dir and lists session id's entries by
// payload, with the retained payload of topic "r".
func entriesOf(t *testing.T, dir, id string) (entries []string, retained string) {
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
	if len(st.Through) != 0 {
		t.Errorf("recovered state still carries the checkpoint's list: %+v", st.Through)
	}
	if ss := st.Find("ns", id); ss != nil {
		for _, e := range ss.Entries {
			entries = append(entries, string(e.Message.Payload))
		}
	}
	for _, m := range st.Retained {
		if m.Topic == "r" {
			retained = string(m.Payload)
		}
	}
	return entries, retained
}

func enqueue(t *testing.T, s *Store, id string, seq uint64, payload string) {
	t.Helper()
	k := Key{Namespace: "ns", ClientID: id, Epoch: 1}
	if err := s.Enqueue(k, Entry{Seq: seq, Message: msg("ns", "t/"+id, payload), QoS: 1, PacketID: uint16(seq)}); err != nil {
		t.Fatal(err)
	}
}

// heldSnapshot is a checkpoint's snapshot of sessions a and b that holds a's
// second entry, appended after the checkpoint's index was read, and names it
// in Through; b's entry appended in the same window and the retained write are
// not named. Then a third entry for a is appended after the state was read.
func heldSnapshot(t *testing.T, s *Store) Snapshot {
	return func(appended func() uint64) *State {
		enqueue(t, s, "a", 2, "a2")
		aRead := appended()
		enqueue(t, s, "b", 1, "b1")
		if err := s.Retain(msg("ns", "r", "v2")); err != nil {
			t.Fatal(err)
		}
		st := NewState()
		st.Sessions = []*SessionState{
			{SessionMeta: SessionMeta{Namespace: "ns", ClientID: "a", Epoch: 1}, Entries: []Entry{
				{Seq: 1, Message: msg("ns", "t/a", "a1"), QoS: 1, PacketID: 1},
				{Seq: 2, Message: msg("ns", "t/a", "a2"), QoS: 1, PacketID: 2},
			}},
			{SessionMeta: SessionMeta{Namespace: "ns", ClientID: "b", Epoch: 1}},
		}
		st.Retained = append(st.Retained, msg("ns", "r", "v2"))
		st.Through = []SessionIndex{{Namespace: "ns", ClientID: "a", Index: aRead}}
		enqueue(t, s, "a", 3, "a3")
		return st
	}
}

func heldFixture(t *testing.T, s *Store) {
	t.Helper()
	upsert(t, s, "a")
	upsert(t, s, "b")
	enqueue(t, s, "a", 1, "a1")
	if err := s.Retain(msg("ns", "r", "v1")); err != nil {
		t.Fatal(err)
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
}

func wantHeld(t *testing.T, dir string) {
	t.Helper()
	a, r := entriesOf(t, dir, "a")
	b, _ := entriesOf(t, dir, "b")
	if len(a) != 3 || a[0] != "a1" || a[1] != "a2" || a[2] != "a3" {
		t.Errorf("recovered a %q; want [a1 a2 a3]: a2 once, as the checkpoint holds it, and a3, after its read", a)
	}
	if len(b) != 1 || b[0] != "b1" {
		t.Errorf("recovered b %q; want [b1], which the checkpoint's b does not hold", b)
	}
	if r != "v2" {
		t.Errorf("recovered retained %q; want v2", r)
	}
}

// TestRecoverSkipsOnlyWhatTheCheckpointHolds: after a checkpoint whose state
// names the index each session was read at, recovery replays a session's
// records above it, every record of a session it does not name, and every
// retained record; a record at or below it is in the state already.
func TestRecoverSkipsOnlyWhatTheCheckpointHolds(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir, Options{})
	heldFixture(t, s)
	if err := s.Checkpoint(heldSnapshot(t, s)); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	wantHeld(t, dir)
}

// TestRepairSkipsWhatTheCheckpointHolds: a repair rebuilds the log's state
// from the latest checkpoint and the records after it, as recovery does, so it
// skips what that checkpoint holds.
func TestRepairSkipsWhatTheCheckpointHolds(t *testing.T) {
	dir := t.TempDir()
	d := faultdisk.New()
	s := openStore(t, dir, Options{Sync: d.Sync})
	heldFixture(t, s)
	if err := s.Checkpoint(heldSnapshot(t, s)); err != nil {
		t.Fatal(err)
	}
	upsert(t, s, "c")
	d.FailNext(1)
	if err := s.Sync(); err == nil {
		t.Fatal("precondition: the armed fsync did not fail")
	}
	if err := s.Sync(); err != nil {
		t.Fatalf("the repair failed: %v", err)
	}
	if got := s.SyncStats().Repairs; got != 1 {
		t.Fatalf("precondition: repairs = %d, want 1", got)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	d.Crash(t)
	wantHeld(t, dir)
}

// TestCheckpointDuringARepairWritesNothingOlder: a failed fsync and the
// repair that follows it happen while a checkpoint's state is read. The
// repair wrote the log's state at a later index and dropped the segments
// before it; the checkpoint, read at the earlier index, writes nothing.
func TestCheckpointDuringARepairWritesNothingOlder(t *testing.T) {
	dir := t.TempDir()
	d := faultdisk.New()
	s := openStore(t, dir, Options{Sync: d.Sync})
	upsert(t, s, "a")
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	err := s.Checkpoint(func(func() uint64) *State {
		enqueue(t, s, "a", 1, "a1")
		d.FailNext(1)
		if err := s.Sync(); err == nil {
			t.Fatal("precondition: the armed fsync did not fail")
		}
		if err := s.Sync(); err != nil {
			t.Fatalf("precondition: the repair failed: %v", err)
		}
		st := NewState()
		st.Sessions = []*SessionState{{SessionMeta: SessionMeta{Namespace: "ns", ClientID: "a", Epoch: 1}}}
		return st
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	d.Crash(t)
	if a, _ := entriesOf(t, dir, "a"); len(a) != 1 || a[0] != "a1" {
		t.Fatalf("recovered a %q; want [a1], which the repair's checkpoint holds", a)
	}
}

// TestCheckpointWhileNotWholeRepairs: an fsync fails while a checkpoint's
// state is read. The journal is not whole, so the checkpoint repairs rather
// than write the state it was given over it, as one begun after the failure
// does (WI 1812).
func TestCheckpointWhileNotWholeRepairs(t *testing.T) {
	dir := t.TempDir()
	d := faultdisk.New()
	s := openStore(t, dir, Options{Sync: d.Sync})
	upsert(t, s, "a")
	enqueue(t, s, "a", 1, "a1")
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	err := s.Checkpoint(func(func() uint64) *State {
		enqueue(t, s, "a", 2, "a2")
		d.FailNext(1)
		if err := s.Sync(); err == nil {
			t.Fatal("precondition: the armed fsync did not fail")
		}
		return NewState()
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := s.SyncStats().Repairs; got != 1 {
		t.Errorf("repairs = %d, want 1: the checkpoint repairs", got)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	d.Crash(t)
	if a, _ := entriesOf(t, dir, "a"); len(a) != 2 || a[0] != "a1" || a[1] != "a2" {
		t.Fatalf("recovered a %q; want [a1 a2]", a)
	}
}

// TestHeldRecordsHoldOnlyTheirSessionUpToItsIndex: a checkpoint's entry for a
// session holds that session's records at or below its index and nothing
// else; a record wrongly held is a record recovery drops.
func TestHeldRecordsHoldOnlyTheirSessionUpToItsIndex(t *testing.T) {
	st := NewState()
	st.Through = []SessionIndex{{Namespace: "ns", ClientID: "a", Index: 10}, {Namespace: "ns", ClientID: "b", Index: 5}}
	h := heldBy(st)
	if st.Through != nil {
		t.Fatalf("the state keeps its list after it was taken: %+v", st.Through)
	}
	rec := func(v any) []byte {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	enqueue := func(ns, id string) []byte {
		r, err := extstore.EncodeEnqueue(Key{Namespace: ns, ClientID: id}, Entry{Seq: 1})
		if err != nil {
			t.Fatal(err)
		}
		return r.Payload
	}
	a := enqueue("ns", "a")
	del, err := extstore.EncodeRetainDelete("ns", "a")
	if err != nil {
		t.Fatal(err)
	}
	retainDelete := del.Payload
	for _, tc := range []struct {
		name    string
		index   uint64
		typ     byte
		payload []byte
		want    bool
	}{
		{"its session, below its index", 9, recEnqueue, a, true},
		{"its session, at its index", 10, recEnqueue, a, true},
		{"its session, above its index", 11, recEnqueue, a, false},
		{"another session's entry, above that one's index", 6, recEnqueue, enqueue("ns", "b"), false},
		{"a session with no entry", 3, recEnqueue, enqueue("ns", "c"), false},
		{"the same client in another namespace", 3, recEnqueue, enqueue("other", "a"), false},
		{"a session delete", 4, recSessionDelete, rec(sessionKey{"ns", "a"}), true},
		{"a retained write", 4, recRetain, rec(msg("ns", "a", "v")), false},
		{"a retained delete", 4, recRetainDelete, retainDelete, false},
		{"a malformed payload", 4, recEnqueue, []byte("{not json"), false},
	} {
		if got := h.holds(tc.index, tc.typ, tc.payload); got != tc.want {
			t.Errorf("%s: held %v, want %v", tc.name, got, tc.want)
		}
	}
	if (*heldRecords)(nil).holds(1, recEnqueue, a) {
		t.Error("a checkpoint with no list holds a record")
	}
}

// TestRecordsDuringTheSnapshotCountTowardTheNextCheckpoint: records appended
// while a checkpoint's state is read are after its index, so the next
// checkpoint is asked for by them.
func TestRecordsDuringTheSnapshotCountTowardTheNextCheckpoint(t *testing.T) {
	s := openStore(t, t.TempDir(), Options{CheckpointEvery: 3})
	defer func() { _ = s.Close() }()
	upsert(t, s, "a")
	err := s.Checkpoint(func(func() uint64) *State {
		for _, id := range []string{"b", "c", "d"} {
			upsert(t, s, id)
		}
		return NewState()
	})
	if err != nil {
		t.Fatal(err)
	}
	if !s.NeedsCheckpoint() {
		t.Fatal("three records after the checkpoint's index, CheckpointEvery 3, and no checkpoint asked for")
	}
	if st := s.SyncStats(); st.Checkpoints != 1 || st.CheckpointSec <= 0 || st.CheckpointHoldSec <= 0 || st.CheckpointHoldSec > st.CheckpointSec {
		t.Fatalf("checkpoint statistics %+v; want one checkpoint, its time, and the part under the lock within it", st)
	}
}
