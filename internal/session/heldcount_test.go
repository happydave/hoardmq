package session

import (
	"fmt"
	"testing"

	"github.com/happydave/hoardmq/internal/packet"
)

// heldByWalk is the queued and in-flight messages summed over every
// session's record.
func heldByWalk(m *Manager) int64 {
	var n int64
	for _, r := range m.Records() {
		n += int64(r.Queued + r.InFlight)
	}
	return n
}

// TestHeldCountsInflightWithTheQueue: the held count, which the overload
// indicator reads, is the queued messages plus those in flight, through a
// send, an acknowledgement and a discard; Queued stays the queue alone.
func TestHeldCountsInflightWithTheQueue(t *testing.T) {
	m, _ := newManager(t, nil)
	pub := newFake("pub", packet.V5, 100)
	sub := newFake("sub", packet.V5, 2)
	connectV5(m, pub, true, 0, nil)
	connectV5(m, sub, false, 3600, nil)
	subscribe(m, sub, 1, "t")
	check := func(stage string, held, queued int64) {
		t.Helper()
		if got := m.Held(); got != held {
			t.Fatalf("%s: held %d, want %d", stage, got, held)
		}
		if got := m.Queued(); got != queued {
			t.Fatalf("%s: queued %d, want %d", stage, got, queued)
		}
		if walk := heldByWalk(m); walk != held {
			t.Fatalf("%s: a walk finds %d held, counted %d", stage, walk, held)
		}
	}
	for i := 0; i < 5; i++ {
		publish(m, pub, "t", 1, uint16(i+1), fmt.Sprint(i))
	}
	check("two in flight, three queued", 5, 3)
	pubs := sub.publishes()
	sub.ack()
	m.Puback(sub, pubs[0].PacketID, packet.Success)
	check("one acknowledged, the next sent", 4, 2)
	c := newFake("sub", packet.V5, 2)
	connectV5(m, c, true, 0, nil)
	check("discarded by a Clean Start", 0, 0)
}
