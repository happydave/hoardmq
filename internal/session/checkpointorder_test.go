package session

import (
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

// checkpointFixture is a session manager over a real store: a persistent
// session "sub" subscribed to "t" at QoS 1, offline, with one message queued
// ("m0"), and a publisher.
type checkpointFixture struct {
	dir string
	st  *store.Store
	m   *Manager
	pub *fakeConn
}

func newCheckpointFixture(t *testing.T) *checkpointFixture {
	t.Helper()
	f := &checkpointFixture{dir: t.TempDir()}
	st, err := store.Open(f.dir, store.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	f.st = st
	f.m, _ = newManager(t, func(o *Options) { o.Journal = st })
	f.pub = newFake("pub", packet.V5, 100)
	connectV5(f.m, f.pub, true, 0, nil)
	sub := newFake("sub", packet.V5, 100)
	connectV5(f.m, sub, true, 3600, nil)
	subscribe(f.m, sub, 1, "t")
	f.m.Disconnect(sub, false, packet.NormalDisconnection, false)
	publish(f.m, f.pub, "t", 1, 1, "m0")
	return f
}

// restart closes the store, reopens it and recovers.
func (f *checkpointFixture) restart(t *testing.T) *store.State {
	t.Helper()
	if err := f.st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(f.dir, store.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	got, err := st.Recover()
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// checkpointChanges are journaled changes a checkpoint's snapshot can race,
// each with what recovery must then hold.
var checkpointChanges = []struct {
	name   string
	change func(m *Manager, pub *fakeConn)
	check  func(t *testing.T, st *store.State)
}{
	{"a QoS 1 message queued for an offline session", func(m *Manager, pub *fakeConn) {
		publish(m, pub, "t", 1, 2, "m1")
	}, func(t *testing.T, st *store.State) {
		ss := st.Find("default", "sub")
		var got []string
		if ss != nil {
			for _, e := range ss.Entries {
				got = append(got, string(e.Message.Payload))
			}
		}
		if len(got) != 2 || got[0] != "m0" || got[1] != "m1" {
			t.Fatalf("recovered sub's queue %q; want [m0 m1], each once and in order", got)
		}
	}},
	{"a subscription", func(m *Manager, pub *fakeConn) {
		c := newFake("sub", packet.V5, 0)
		connectV5(m, c, false, 3600, nil)
		subscribe(m, c, 1, "u")
	}, func(t *testing.T, st *store.State) {
		ss := st.Find("default", "sub")
		if ss == nil || len(ss.Subscriptions) != 2 {
			t.Fatalf("recovered sub %+v; want both subscriptions", ss)
		}
	}},
	{"a new session", func(m *Manager, pub *fakeConn) {
		connectV5(m, newFake("late", packet.V5, 100), true, 3600, nil)
	}, func(t *testing.T, st *store.State) {
		if st.Find("default", "late") == nil {
			t.Fatal("the session created while the checkpoint ran is not recovered")
		}
	}},
	{"a retained message", func(m *Manager, pub *fakeConn) {
		m.Publish(pub, &packet.Publish{Topic: "r", QoS: 1, PacketID: 3, Retain: true, Payload: []byte("kept")})
	}, func(t *testing.T, st *store.State) {
		if len(st.Retained) != 1 || string(st.Retained[0].Payload) != "kept" {
			t.Fatalf("recovered retained %+v; want the one message retained while the checkpoint ran", st.Retained)
		}
	}},
}

// TestRecordAfterTheSnapshotSurvivesTheCheckpoint: a change journaled after
// its session was read for a checkpoint and before the checkpoint is written
// is recovered after a restart. A checkpoint that read the log's index after
// the state would put such a record below its index and outside its state,
// and recovery would never replay it (WI 1818).
func TestRecordAfterTheSnapshotSurvivesTheCheckpoint(t *testing.T) {
	for _, tc := range checkpointChanges {
		t.Run(tc.name, func(t *testing.T) {
			f := newCheckpointFixture(t)
			var before, after uint64
			err := f.st.Checkpoint(func(appended func() uint64) *store.State {
				snap := f.m.CheckpointState(appended)
				before = f.st.LastIndex()
				tc.change(f.m, f.pub)
				after = f.st.LastIndex()
				return snap
			})
			if err != nil {
				t.Fatal(err)
			}
			if after <= before {
				t.Fatalf("precondition: the change journaled nothing (log at %d, then %d)", before, after)
			}
			tc.check(t, f.restart(t))
		})
	}
}

// TestRecordBeforeTheSnapshotIsRecoveredOnce: a change journaled after the
// checkpoint's index was read and before its session is read is both in the
// checkpoint and after its index; recovery applies it once. An enqueue applied
// twice is a second copy of the message.
func TestRecordBeforeTheSnapshotIsRecoveredOnce(t *testing.T) {
	for _, tc := range checkpointChanges {
		t.Run(tc.name, func(t *testing.T) {
			f := newCheckpointFixture(t)
			var before, after uint64
			err := f.st.Checkpoint(func(appended func() uint64) *store.State {
				before = f.st.LastIndex()
				tc.change(f.m, f.pub)
				after = f.st.LastIndex()
				return f.m.CheckpointState(appended)
			})
			if err != nil {
				t.Fatal(err)
			}
			if after <= before {
				t.Fatalf("precondition: the change journaled nothing (log at %d, then %d)", before, after)
			}
			tc.check(t, f.restart(t))
		})
	}
}

// TestSessionSealedAfterTheListIsKept: a checkpoint lists the sessions, then
// reads each; one sealed, or sealed and resumed, in between is no longer the
// session listed, and its state is where its key is now. A seal journals
// nothing, so a checkpoint that read nothing for the key would drop records
// the log still needs. A message queued after the checkpoint's index was read
// and before the list is in the state read, and is recovered once.
func TestSessionSealedAfterTheListIsKept(t *testing.T) {
	for _, resume := range []bool{false, true} {
		name := map[bool]string{false: "sealed", true: "sealed and resumed"}[resume]
		t.Run(name, func(t *testing.T) {
			f := newCheckpointFixture(t)
			f.m.checkpointPause = func() {
				if res := f.m.Seal("default", "sub", packet.SessionTakenOver, 1); res.Outcome != SealSealed {
					t.Errorf("precondition: seal %+v", res)
				}
				if resume {
					if res := connectAt(f.m, newFake("sub", packet.V5, 0), false, 3600, 2); !res.SessionPresent {
						t.Errorf("precondition: the sealed session was not resumed: %+v", res)
					}
				}
			}
			err := f.st.Checkpoint(func(appended func() uint64) *store.State {
				publish(f.m, f.pub, "t", 1, 2, "m1")
				return f.m.CheckpointState(appended)
			})
			if err != nil {
				t.Fatal(err)
			}
			ss := f.restart(t).Find("default", "sub")
			var got []string
			if ss != nil {
				for _, e := range ss.Entries {
					got = append(got, string(e.Message.Payload))
				}
			}
			if ss == nil || len(ss.Subscriptions) != 1 || len(got) != 2 || got[0] != "m0" || got[1] != "m1" {
				t.Fatalf("recovered sub %+v, queue %q; want its subscription and [m0 m1], each once", ss, got)
			}
		})
	}
}

// TestSealedCopyRecordBeforeTheSnapshotIsRecoveredOnce: a message queued
// after the checkpoint's index was read, then the session sealed before the
// list: the sealed copy holds the message and its entry says so.
func TestSealedCopyRecordBeforeTheSnapshotIsRecoveredOnce(t *testing.T) {
	f := newCheckpointFixture(t)
	err := f.st.Checkpoint(func(appended func() uint64) *store.State {
		publish(f.m, f.pub, "t", 1, 2, "m1")
		if res := f.m.Seal("default", "sub", packet.SessionTakenOver, 1); res.Outcome != SealSealed {
			t.Fatalf("precondition: seal %+v", res)
		}
		return f.m.CheckpointState(appended)
	})
	if err != nil {
		t.Fatal(err)
	}
	checkpointChanges[0].check(t, f.restart(t))
}

// TestSessionIndexIsReadUnderTheSessionLock: the index a session's entry
// records is read while nothing can be journaled for it. A message published
// to the session while the checkpoint reads the index is journaled either
// before the read, and so in the state read with it, or after the lock is
// released, and so above the index and replayed.
func TestSessionIndexIsReadUnderTheSessionLock(t *testing.T) {
	f := newCheckpointFixture(t)
	published := make(chan struct{})
	var once sync.Once
	err := f.st.Checkpoint(func(appended func() uint64) *store.State {
		read := func() uint64 {
			once.Do(func() {
				// The first read is sub's: pub is not persistent.
				go func() {
					publish(f.m, f.pub, "t", 1, 2, "m1")
					close(published)
				}()
				select {
				case <-published:
				case <-time.After(100 * time.Millisecond):
				}
			})
			return appended()
		}
		return f.m.CheckpointState(read)
	})
	if err != nil {
		t.Fatal(err)
	}
	<-published
	checkpointChanges[0].check(t, f.restart(t))
}
