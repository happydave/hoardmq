package session

import (
	"errors"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
)

// pubrelOwed leaves sub's session holding one QoS 2 delivery acknowledged by
// PUBREC whose PUBREL is owed, and the session offline, so its next
// connection's drain sends that PUBREL.
func pubrelOwed(t *testing.T, m *Manager, pub *fakeConn) uint16 {
	t.Helper()
	sub := newFake("sub", packet.V5, 100)
	connectV5(m, sub, true, 600, nil)
	subscribe(m, sub, 2, "t/x")
	if code := publish(m, pub, "t/x", 2, 1, "q2"); code != packet.Success {
		t.Fatalf("publish: %s", code)
	}
	ps := sub.publishes()
	if len(ps) != 1 {
		t.Fatalf("sub received %d publishes, want 1", len(ps))
	}
	if r, _ := m.Pubrec(sub, ps[0].PacketID, packet.Success); r != packet.Success {
		t.Fatalf("PUBREC: %s", r)
	}
	m.Disconnect(sub, false, packet.Success, false)
	return ps[0].PacketID
}

var errSyncForTest = errors.New("sync failed")

// TestStalledClientsPubrelDoesNotHoldPublishers: the session's next
// connection cannot take a packet (its writer is stuck). Its resume's drain
// owes a PUBREL; a publish delivered to the same session while that drain
// runs must not wait for the stuck writer, and the PUBREL goes out once the
// connection has room, once.
func TestStalledClientsPubrelDoesNotHoldPublishers(t *testing.T) {
	m, _ := newManager(t, nil)
	pub := newFake("pub", packet.V5, 100)
	connectV5(m, pub, true, 0, nil)
	pubrelOwed(t, m, pub)

	back := newFake("sub", packet.V5, 100)
	back.stall, back.offered = make(chan struct{}), make(chan packet.Packet, 8)
	resumed := make(chan struct{})
	go func() {
		connectV5(m, back, false, 600, nil)
		close(resumed)
	}()
	select {
	case p := <-back.offered:
		if _, ok := p.(*packet.Pubrel); !ok {
			t.Fatalf("the drain's first control packet: %T", p)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the resume's drain did not reach the PUBREL")
	}
	release := time.AfterFunc(time.Second, func() { close(back.stall) })
	defer release.Stop()
	began := time.Now()
	publish(m, pub, "t/x", 0, 0, "while stalled")
	held := time.Since(began)
	if held >= 500*time.Millisecond {
		t.Fatalf("a publish to the session waited %v behind the stalled PUBREL", held)
	}
	t.Logf("STALL a publish to the session took %v while its connection's writer was stuck", held)
	<-resumed

	back.mu.Lock()
	back.stall, back.offered = nil, nil
	back.mu.Unlock()
	m.Resume(back)
	if n := len(pubrels(back)); n != 1 {
		t.Fatalf("%d PUBRELs once the connection had room, want 1", n)
	}
}

// TestHeldPubrelIsNotOffered: a PUBREL held because its PUBREC state is not
// durable is skipped by the drain, as before.
func TestHeldPubrelIsNotOffered(t *testing.T) {
	j := &hookLog{fakeJournal: newFakeJournal()}
	m, _ := newManager(t, func(o *Options) { o.Journal = j })
	pub := newFake("pub", packet.V5, 100)
	connectV5(m, pub, true, 0, nil)
	sub := newFake("sub", packet.V5, 100)
	connectV5(m, sub, true, 600, nil)
	subscribe(m, sub, 2, "t/x")
	publish(m, pub, "t/x", 2, 1, "q2")
	ps := sub.publishes()
	if len(ps) != 1 {
		t.Fatalf("sub received %d publishes", len(ps))
	}
	j.syncErr = errSyncForTest
	if _, send := m.Pubrec(sub, ps[0].PacketID, packet.Success); send {
		t.Fatal("precondition: the PUBREL is held when the journal cannot sync")
	}
	m.Disconnect(sub, false, packet.Success, false)
	// The journal syncs again, so the reconnect is admitted; the hold stays
	// until its retry, which the test clock never reaches.
	j.syncErr = nil
	back := newFake("sub", packet.V5, 100)
	if present := connectV5(m, back, false, 600, nil); !present {
		t.Fatal("precondition: the reconnect resumes the session")
	}
	if n := len(pubrels(back)); n != 0 {
		t.Fatalf("the drain sent %d held PUBRELs", n)
	}
}
