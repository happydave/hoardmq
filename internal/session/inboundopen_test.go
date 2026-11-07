package session

import (
	"testing"

	"github.com/happydave/hoardmq/internal/packet"
)

// TestInboundOpen: an identifier is held open from its opening to its
// PUBREL, in every state, and never under the harness's injected defect.
func TestInboundOpen(t *testing.T) {
	m, _ := newManager(t, nil)
	c := newFake("pub", packet.V5, 100)
	if m.InboundOpen(c, 9) {
		t.Fatal("open with no session")
	}
	connectV5(m, c, false, 3600, nil)
	if m.InboundOpen(c, 9) {
		t.Fatal("open before any publish")
	}
	if code := publish(m, c, "t", 2, 9, "m"); code.IsError() {
		t.Fatalf("precondition: %v", code)
	}
	if !m.InboundOpen(c, 9) || m.InboundOpen(c, 8) {
		t.Fatalf("accepted 9: %v; 8: %v", m.InboundOpen(c, 9), m.InboundOpen(c, 8))
	}
	s := m.lookup(c)
	s.mu.Lock()
	s.inboundQoS2[5] = &inboundAttempt{state: inboundPending, done: make(chan struct{})}
	s.mu.Unlock()
	if !m.InboundOpen(c, 5) {
		t.Fatal("pending 5 not open")
	}
	m.Pubrel(c, 9)
	if m.InboundOpen(c, 9) {
		t.Fatal("open after its PUBREL")
	}

	forget, _ := newManager(t, func(o *Options) { o.ForgetInboundQoS2 = true })
	f := newFake("pub", packet.V5, 100)
	connectV5(forget, f, false, 3600, nil)
	publish(forget, f, "t", 2, 9, "m")
	if forget.InboundOpen(f, 9) {
		t.Fatal("open under ForgetInboundQoS2")
	}

	m2, _, _, pub := takenOverAt(t, inboundRecordOf9(false), nil)
	if !m2.InboundOpen(pub, 9) {
		t.Fatal("an unconfirmed identifier is not open")
	}
}
