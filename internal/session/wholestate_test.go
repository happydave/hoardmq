package session

import (
	"testing"

	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

// TestWholeStateHoldsTheSessionsLock: a whole write's function runs under the
// session's own mutex, which every record of the session is written under, so
// a record cannot be written between the state it is given and what it queues.
func TestWholeStateHoldsTheSessionsLock(t *testing.T) {
	m, _ := newManager(t, nil)
	c := newFake("c", packet.V5, 1)
	connectV5(m, c, false, 600, nil)
	connectV5(m, newFake("transient", packet.V5, 1), true, 0, nil)
	if got := m.Persistent(); len(got) != 1 || got[0] != (store.Scope{Namespace: "default", ClientID: "c"}) {
		t.Fatalf("Persistent() = %v, want the persistent session alone", got)
	}
	s := m.lookup(c)
	called, free := false, false
	ok := m.WholeState("default", "c", func(ss *store.SessionState) {
		called = true
		if s.mu.TryLock() {
			free = true
			s.mu.Unlock()
		}
		if ss.ClientID != "c" {
			t.Errorf("state for %q, want c", ss.ClientID)
		}
	})
	if !ok || !called {
		t.Fatalf("WholeState = %v, called %v; want the session served", ok, called)
	}
	if free {
		t.Fatal("the session's mutex was free while the whole write's function ran")
	}
}

// TestWholeStateSkipsASessionNoLongerServed: a session listed for a whole
// write and then evicted is not written whole after its delete.
func TestWholeStateSkipsASessionNoLongerServed(t *testing.T) {
	m, _ := newManager(t, nil)
	c := newFake("c", packet.V5, 1)
	connectV5(m, c, false, 600, nil)
	listed := m.Persistent()
	if len(listed) != 1 {
		t.Fatalf("precondition: %d sessions listed", len(listed))
	}
	if !m.Evict("default", "c", packet.SessionTakenOver) {
		t.Fatal("precondition: the session was not evicted")
	}
	if m.WholeState(listed[0].Namespace, listed[0].ClientID, func(*store.SessionState) { t.Error("the function ran for an evicted session") }) {
		t.Fatal("WholeState reported an evicted session served")
	}
}
