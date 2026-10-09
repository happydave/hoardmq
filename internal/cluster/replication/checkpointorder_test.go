package replication

import (
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

// replicaFixture is a follower over a real replica store holding session "a"
// of namespace "ns" with one entry, and the entry record a test applies next.
func replicaFixture(t *testing.T) (dir string, rs *store.Store, f *follower, next store.Record) {
	t.Helper()
	dir = t.TempDir()
	rs, err := store.Open(dir, store.Options{Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rs.Close() })
	f = &follower{r: &Replicator{o: Options{Replica: rs}}, state: store.NewState()}
	meta := store.SessionMeta{Namespace: "ns", ClientID: "a", Epoch: 1, ExpiryInterval: 3600}
	entry := func(seq uint64, payload string) store.Entry {
		return store.Entry{Seq: seq, QoS: 1, PacketID: uint16(seq), Message: &message.Message{Namespace: "ns", Topic: "t", QoS: 1, Payload: []byte(payload)}}
	}
	for _, rec := range store.RecordsFor(&store.SessionState{SessionMeta: meta, Entries: []store.Entry{entry(1, "e1")}}) {
		if _, err := f.apply(rec); err != nil {
			t.Fatal(err)
		}
	}
	recs := store.RecordsFor(&store.SessionState{SessionMeta: meta, Entries: []store.Entry{entry(2, "e2")}})
	return dir, rs, f, recs[len(recs)-1]
}

// recoveredEntries reopens the replica store and lists session a's entries.
func recoveredEntries(t *testing.T, dir string, rs *store.Store) []string {
	t.Helper()
	if err := rs.Close(); err != nil {
		t.Fatal(err)
	}
	rs2, err := store.Open(dir, store.Options{Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rs2.Close() }()
	got, err := rs2.Recover()
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	if ss := got.Find("ns", "a"); ss != nil {
		for _, e := range ss.Entries {
			out = append(out, string(e.Message.Payload))
		}
	}
	return out
}

func wantEntries(t *testing.T, got []string) {
	t.Helper()
	if len(got) != 2 || got[0] != "e1" || got[1] != "e2" {
		t.Fatalf("recovered replica entries %q; want [e1 e2], each once", got)
	}
}

// TestReplicaRecordAfterTheSnapshotSurvivesTheCheckpoint: a record applied
// after the follower's state was read for a checkpoint of the replica store,
// and before the checkpoint is written, is recovered after a restart; one
// applied after the index was read and before the state was, once (WI 1818).
func TestReplicaRecordAfterTheSnapshotSurvivesTheCheckpoint(t *testing.T) {
	for _, after := range []bool{true, false} {
		name := map[bool]string{true: "applied after the snapshot", false: "applied before the snapshot"}[after]
		t.Run(name, func(t *testing.T) {
			dir, rs, f, next := replicaFixture(t)
			err := rs.Checkpoint(func(appended func() uint64) *store.State {
				if !after {
					if _, err := f.apply(next); err != nil {
						t.Fatal(err)
					}
				}
				snap := f.checkpointState(appended)
				if after {
					if _, err := f.apply(next); err != nil {
						t.Fatal(err)
					}
				}
				return snap
			})
			if err != nil {
				t.Fatal(err)
			}
			wantEntries(t, recoveredEntries(t, dir, rs))
		})
	}
}

// TestReplicaAppendedButNotAppliedIsCaptured: a follower appends a record and
// then applies it. A checkpoint that read the state between the two would
// write a state without the record at an index past it, and recovery would
// not replay it.
func TestReplicaAppendedButNotAppliedIsCaptured(t *testing.T) {
	dir, rs, f, next := replicaFixture(t)
	appendedCh, release := make(chan struct{}), make(chan struct{})
	f.afterAppend = func() {
		f.afterAppend = nil
		close(appendedCh)
		<-release
	}
	applied := make(chan error, 1)
	go func() {
		_, err := f.apply(next)
		applied <- err
	}()
	select {
	case <-appendedCh:
	case <-time.After(10 * time.Second):
		t.Fatal("precondition: the record was never appended")
	}
	snapped, checkpointed := make(chan struct{}), make(chan error, 1)
	go func() {
		checkpointed <- rs.Checkpoint(func(appended func() uint64) *store.State {
			st := f.checkpointState(appended)
			close(snapped)
			return st
		})
	}()
	// A snapshot that can read the state now reads it without the record.
	select {
	case <-snapped:
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	if err := <-applied; err != nil {
		t.Fatal(err)
	}
	if err := <-checkpointed; err != nil {
		t.Fatal(err)
	}
	wantEntries(t, recoveredEntries(t, dir, rs))
}
