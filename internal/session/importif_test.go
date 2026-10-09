package session

import (
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

func stateOf(m *Manager, id string, epoch uint64, payload string) *store.SessionState {
	return &store.SessionState{
		SessionMeta:   store.SessionMeta{Namespace: m.opts.Namespace, ClientID: id, ExpiryInterval: 600, Version: byte(packet.V5), Epoch: epoch},
		Subscriptions: []store.SubscriptionRecord{{Filter: "t/" + id, QoS: 1}},
		Entries: []store.Entry{{Seq: 1, QoS: 1, State: store.StateQueued,
			Message: &message.Message{Namespace: m.opts.Namespace, Topic: "t/" + id, QoS: 1, Payload: []byte(payload)}}},
	}
}

func waitTrue(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("precondition not reached: %s", what)
}

// holdFirstImport pauses the first import to reach the manager's pause until
// the returned release is called; later imports pass.
func holdFirstImport(t *testing.T, m *Manager) (entered chan struct{}, release func()) {
	t.Helper()
	entered, gate := make(chan struct{}), make(chan struct{})
	var once sync.Once
	m.importPause = func() {
		first := false
		once.Do(func() { first = true })
		if first {
			close(entered)
			<-gate
		}
	}
	release = sync.OnceFunc(func() { close(gate) })
	t.Cleanup(release)
	return entered, release
}

// TestImportIfRefusedChangesNothing: a refused import leaves the client as it
// was — its sealed copy, no session, no journal record — so the replica copy
// an import's write would drop is left for the node that holds the lease now.
// The same call admitted replaces the sealed copy, which is what makes the
// refused call's silence mean something.
func TestImportIfRefusedChangesNothing(t *testing.T) {
	m, j, _ := sealedFixture(t)
	ns := m.opts.Namespace
	st := m.Seal(ns, "c", packet.SessionTakenOver, 9).State
	st.Epoch = 12
	before := len(journalFor(j, "c"))
	if m.ImportIf(st, func() bool { return false }) {
		t.Fatal("a refused import reported that it imported")
	}
	if got := journalFor(j, "c")[before:]; len(got) != 0 {
		t.Fatalf("a refused import journaled %v", got)
	}
	if _, held := m.Export(ns, "c"); held {
		t.Fatal("a refused import installed the session")
	}
	if _, tok, ok := m.Sealed(ns, "c"); !ok || tok != 9 {
		t.Fatalf("a refused import took the sealed copy: sealed %v at %d", ok, tok)
	}

	if !m.ImportIf(st, func() bool { return true }) {
		t.Fatal("an admitted import reported that it did not import")
	}
	if at, ok := m.HeldAt(ns, "c"); !ok || at != 12 {
		t.Fatalf("an admitted import: held %v at %d, want 12", ok, at)
	}
	if got := journalFor(j, "c")[before:]; len(got) < 1 || got[0] != "delete c" {
		t.Fatalf("an admitted import journaled %v; want the whole write", got)
	}
	if m.SealedCount() != 0 {
		t.Fatal("an admitted import left the sealed copy")
	}
}

// TestSealWaitsForAnImportInFlight: a takeover pass that seals while an
// admitted import of the same client is between its admission and the
// session map finds the imported session. Finding nothing, it would answer
// "no state" for a session about to be installed.
func TestSealWaitsForAnImportInFlight(t *testing.T) {
	m, _ := newManager(t, nil)
	ns := m.opts.Namespace
	entered, release := holdFirstImport(t, m)
	imported := make(chan bool, 1)
	go func() { imported <- m.ImportIf(stateOf(m, "d", 20, "queued"), func() bool { return true }) }()
	<-entered
	sealed := make(chan SealResult, 1)
	go func() { sealed <- m.Seal(ns, "d", packet.SessionTakenOver, 21) }()
	waitTrue(t, "the seal to wait on the import, or to return", func() bool {
		seals, _ := m.ImportWaits()
		return seals >= 1 || len(sealed) == 1
	})
	release()
	if !<-imported {
		t.Fatal("the import was not admitted")
	}
	res := <-sealed
	if res.Outcome != SealSealed || res.State == nil || len(res.State.Entries) != 1 || string(res.State.Entries[0].Message.Payload) != "queued" {
		t.Fatalf("a seal during the import: %+v; want the imported session sealed", res)
	}
	if _, held := m.Export(ns, "d"); held {
		t.Fatal("the sealed session is still held")
	}
}

// TestImportWaitsForAnImportInFlight: a second import of a client being
// imported installs after the first, so the first's end cannot clear the
// second's mark and the session is the later one's.
func TestImportWaitsForAnImportInFlight(t *testing.T) {
	m, _ := newManager(t, nil)
	ns := m.opts.Namespace
	entered, release := holdFirstImport(t, m)
	first := make(chan bool, 1)
	go func() { first <- m.ImportIf(stateOf(m, "d", 20, "first"), nil) }()
	<-entered
	second := make(chan bool, 1)
	go func() { second <- m.ImportIf(stateOf(m, "d", 21, "second"), nil) }()
	waitTrue(t, "the second import to wait on the first, or to return", func() bool {
		_, imports := m.ImportWaits()
		return imports >= 1 || len(second) == 1
	})
	release()
	if !<-first || !<-second {
		t.Fatal("an import with no check was not admitted")
	}
	st, held := m.Export(ns, "d")
	if !held || st.Epoch != 21 || len(st.Entries) != 1 || string(st.Entries[0].Message.Payload) != "second" {
		t.Fatalf("after both imports: held %v, %+v; want the second's state", held, st)
	}
}

// TestSealWaitsForAnImportThatBeginsBetweenItsLooks: Seal reads the session,
// marks its filters moving, and takes the lock again to seal. An import
// admitted in between is waited for as at the first look, so the seal takes
// what the import installed, not the session the import is replacing.
func TestSealWaitsForAnImportThatBeginsBetweenItsLooks(t *testing.T) {
	var m *Manager
	var once sync.Once
	imported := make(chan bool, 1)
	m, _ = newManager(t, func(o *Options) {
		o.Moving = func(string, string, []string) {
			once.Do(func() {
				go func() { imported <- m.ImportIf(stateOf(m, "c", 12, "new"), nil) }()
			})
		}
	})
	ns := m.opts.Namespace
	pub := newFake("pub", packet.V5, 100)
	connectAt(m, pub, true, 0, 1)
	c := newFake("c", packet.V5, 100)
	connectAt(m, c, true, 600, 5)
	subscribe(m, c, 1, "t")
	m.Disconnect(c, false, packet.NormalDisconnection, false)
	publish(m, pub, "t", 1, 1, "old")
	entered, release := holdFirstImport(t, m)
	// The import starts from inside Seal's markMoving; Seal goes on to its
	// second lock only once the import holds the mark.
	moving := m.opts.Moving
	m.opts.Moving = func(ns, id string, filters []string) {
		moving(ns, id, filters)
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
		}
	}
	sealed := make(chan SealResult, 1)
	go func() { sealed <- m.Seal(ns, "c", packet.SessionTakenOver, 12) }()
	waitTrue(t, "the seal to wait on the import, or to return", func() bool {
		seals, _ := m.ImportWaits()
		return seals >= 1 || len(sealed) == 1
	})
	release()
	if !<-imported {
		t.Fatal("the import with no check was not admitted")
	}
	res := <-sealed
	if res.Outcome != SealSealed || res.State == nil || len(res.State.Entries) != 1 || string(res.State.Entries[0].Message.Payload) != "new" {
		t.Fatalf("a seal whose second look met an import: %+v; want the imported session sealed", res)
	}
}
