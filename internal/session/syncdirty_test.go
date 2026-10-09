package session

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

// TestSyncIfDirtyWaitsForTheFsyncCoveringItsRecord: two callers journal, and
// the second to sync finds its record already inside the first's fsync. It
// must not report its record durable before that fsync has returned, and
// that fsync fails here (WI 1812).
func TestSyncIfDirtyWaitsForTheFsyncCoveringItsRecord(t *testing.T) {
	var hold atomic.Bool
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	st, err := store.Open(t.TempDir(), store.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Sync: func(f *os.File) error {
		if hold.CompareAndSwap(true, false) {
			entered <- struct{}{}
			<-release
			return errors.New("injected fsync failure")
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	t.Cleanup(unblock) // runs before the Close above, which waits for a held fsync
	m, _ := newManager(t, func(o *Options) { o.Journal = st })
	w := m.writer()
	for _, id := range []string{"b", "a"} {
		w.record(&Session{persistent: true}, func() error {
			return st.SessionUpsert(store.SessionMeta{Namespace: "ns", ClientID: id, Epoch: 1})
		})
	}
	hold.Store(true)
	aDone := make(chan bool, 1)
	go func() { aDone <- m.syncIfDirty() }()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("precondition: a's sync never reached the fsync")
	}
	syncBefore := st.SyncStats().Callers
	bDone := make(chan bool, 1)
	go func() { bDone <- m.syncIfDirty() }()
	deadline := time.Now().Add(10 * time.Second)
wait:
	for {
		select {
		case ok := <-bDone:
			t.Fatalf("b's syncIfDirty returned %v while the fsync covering b's record was still in flight", ok)
		default:
		}
		if st.SyncStats().Callers > syncBefore {
			break wait // b is in the store's Sync
		}
		if time.Now().After(deadline) {
			t.Fatal("b's syncIfDirty neither returned nor reached the store's Sync")
		}
		time.Sleep(time.Millisecond)
	}
	unblock()
	if <-aDone {
		t.Error("a's syncIfDirty reported success after its own fsync failed")
	}
	<-bDone
}

// syncCounter is a journal that accepts every write and counts its Syncs.
type syncCounter struct {
	scopeJournal
	syncs atomic.Int32
}

func (j *syncCounter) Sync() error { j.syncs.Add(1); return nil }

// TestEveryJournalWriteMakesTheNextSyncReal: each kind of write the manager
// journals makes the next syncIfDirty call Sync, and a second call with
// nothing written since does not.
func TestEveryJournalWriteMakesTheNextSyncReal(t *testing.T) {
	for _, tc := range []struct {
		name  string
		write func(m *Manager)
	}{
		{"a session record", func(m *Manager) {
			m.writer().upsert(&Session{persistent: true, key: key{"ns", "a"}})
		}},
		{"a retained message", func(m *Manager) {
			m.writer().retain(&message.Message{Namespace: "ns", Topic: "r", Payload: []byte("x")})
		}},
		{"a sealed session's delete", func(m *Manager) {
			m.journalSealedDelete(&sealedCopy{st: &store.SessionState{SessionMeta: store.SessionMeta{Namespace: "ns", ClientID: "a"}}, epoch: 1, persistent: true})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			j := &syncCounter{}
			m, _ := newManager(t, func(o *Options) { o.Journal = j })
			tc.write(m)
			if !m.syncIfDirty() {
				t.Fatal("syncIfDirty failed")
			}
			if got := j.syncs.Load(); got != 1 {
				t.Fatalf("Syncs after the write = %d, want 1", got)
			}
			if !m.syncIfDirty() {
				t.Fatal("syncIfDirty failed")
			}
			if got := j.syncs.Load(); got != 1 {
				t.Fatalf("Syncs after a second call with nothing written = %d, want 1", got)
			}
		})
	}
}
