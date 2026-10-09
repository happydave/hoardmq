package session

import (
	"fmt"
	"testing"

	"github.com/happydave/hoardmq/internal/packet"
)

// queuedByWalk is the queued messages summed over every session's record.
func queuedByWalk(m *Manager) int64 {
	var n int64
	for _, r := range m.Records() {
		n += int64(r.Queued)
	}
	return n
}

// A subscriber whose connection is busy has its messages kept, at every QoS,
// and sent in order once the connection is writable again.
func TestBusyConnectionKeepsEveryQoS(t *testing.T) {
	m, _ := newManager(t, nil)
	pub := newFake("pub", packet.V5, 100)
	sub := newFake("sub", packet.V5, 100)
	connectV5(m, pub, true, 0, nil)
	connectV5(m, sub, false, 3600, nil)
	subscribe(m, sub, 1, "t")
	sub.setBusy(true)
	for i := 0; i < 6; i++ {
		qos := byte(i % 2)
		if r := publish(m, pub, "t", qos, uint16(i+1), fmt.Sprint(i)); r.IsError() {
			t.Fatalf("publish %d refused %s", i, r)
		}
	}
	if n := len(sub.publishes()); n != 0 {
		t.Fatalf("%d sent to a busy connection", n)
	}
	if q := m.Queued(); q != 6 {
		t.Fatalf("%d queued for a busy subscriber, want 6", q)
	}
	sub.setBusy(false)
	m.Resume(sub)
	pubs := sub.publishes()
	if len(pubs) != 6 {
		t.Fatalf("%d delivered once writable, want 6", len(pubs))
	}
	for i, p := range pubs {
		if string(p.Payload) != fmt.Sprint(i) {
			t.Fatalf("delivery %d is %q: out of order", i, p.Payload)
		}
	}
	if q := m.Queued(); q != 0 {
		t.Fatalf("%d still counted queued after the drain", q)
	}
}

// The count of queued messages matches a walk of every session through each
// way a queue changes: queued offline, the oldest dropped past the bound,
// drained on resumption, and discarded by a Clean Start.
func TestQueuedCounterMatches(t *testing.T) {
	m, _ := newManager(t, func(o *Options) { o.MaxQueued = 3 })
	pub := newFake("pub", packet.V5, 100)
	connectV5(m, pub, true, 0, nil)
	subs := map[string]*fakeConn{}
	for _, id := range []string{"a", "b", "c"} {
		subs[id] = newFake(id, packet.V5, 100)
		connectV5(m, subs[id], false, 3600, nil)
		subscribe(m, subs[id], 1, "t")
		m.Disconnect(subs[id], false, packet.UnspecifiedError, false)
	}
	check := func(stage string) {
		t.Helper()
		if got, walk := m.Queued(), queuedByWalk(m); got != walk {
			t.Fatalf("%s: counted %d, a walk finds %d", stage, got, walk)
		}
	}
	for i := 0; i < 2; i++ {
		publish(m, pub, "t", 1, uint16(i+1), "x")
	}
	check("queued offline")
	if m.Queued() != 6 {
		t.Fatalf("queued offline: %d, want 6", m.Queued())
	}
	for i := 2; i < 5; i++ {
		publish(m, pub, "t", 1, uint16(i+1), "x")
	}
	check("past the bound")
	if m.Queued() != 9 {
		t.Fatalf("past the bound of 3: %d, want 9", m.Queued())
	}
	a := newFake("a", packet.V5, 100)
	connectV5(m, a, false, 3600, nil)
	check("drained on resumption")
	c := newFake("c", packet.V5, 100)
	connectV5(m, c, true, 0, nil)
	check("discarded by Clean Start")
	if m.Queued() != 3 {
		t.Fatalf("after a drain and a discard: %d, want b's 3", m.Queued())
	}
}
