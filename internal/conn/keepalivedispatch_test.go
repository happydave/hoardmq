package conn

import (
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
)

// TestKeepAliveCountsFromTheEndOfDispatch: a PUBLISH whose handling takes
// longer than the keep-alive's one and a half times (a slow journal sync, a
// replication barrier) is not charged to the client. Its PINGREQ, sent while
// the broker was busy, is answered, not met with a keep-alive timeout.
func TestKeepAliveCountsFromTheEndOfDispatch(t *testing.T) {
	core := &fakeCore{publishTakes: 2 * time.Second}
	h := newHarness(t, defaultOptions(), core)
	h.connect(packet.V5, &packet.Connect{ClientID: "c", CleanStart: true, KeepAlive: 1})
	h.send(&packet.Publish{Topic: "t", QoS: 0, Payload: []byte("x")})
	h.sendAll([]packet.Packet{&packet.Pingreq{}})
	if p := h.readWithin(5 * time.Second); p.Type() != packet.PINGRESP {
		t.Fatalf("after a 2 s PUBLISH on a 1 s keep-alive: %T %+v, want PINGRESP", p, p)
	}
}

// TestSilenceAfterASlowPublishIsStillClosed: a client that sends nothing after
// a PUBLISH whose handling took 2 s is closed for keep-alive one and a half
// keep-alives after the handling ended: not at once, and not never.
func TestSilenceAfterASlowPublishIsStillClosed(t *testing.T) {
	core := &fakeCore{publishTakes: 2 * time.Second}
	h := newHarness(t, defaultOptions(), core)
	h.connect(packet.V5, &packet.Connect{ClientID: "c", CleanStart: true, KeepAlive: 1})
	start := time.Now()
	h.send(&packet.Publish{Topic: "t", QoS: 0, Payload: []byte("x")})
	p := h.readWithin(6 * time.Second)
	if d, ok := p.(*packet.Disconnect); !ok || d.ReasonCode != packet.KeepAliveTimeout {
		t.Fatalf("got %T %+v, want a Keep Alive timeout", p, p)
	}
	if el := time.Since(start); el < 3200*time.Millisecond || el > 4500*time.Millisecond {
		t.Fatalf("closed %v after the PUBLISH, want about 3.5 s (2 s of handling, then 1.5 s)", el)
	}
}
