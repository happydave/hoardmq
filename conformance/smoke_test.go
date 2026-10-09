package conformance

import (
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
)

// TestHarnessSmoke checks that every client kind connects and exchanges a
// message; it covers no statements itself.
func TestHarnessSmoke(t *testing.T) {
	n := StartNode(t, nil)
	a, ack := n.Must5(Connect5{ClientID: "smoke5", CleanStart: true})
	if ack.ReasonCode != 0 {
		t.Fatalf("connack %d", ack.ReasonCode)
	}
	a.Sub("smoke/#", 1)
	a.Pub("smoke/1", 1, false, "hello", nil)
	if m := a.Expect(1, 2*time.Second); string(m[0].Payload) != "hello" {
		t.Fatalf("%q", m[0].Payload)
	}
	b := n.Must4(Connect4{ClientID: "smoke4", Clean: true})
	b.Sub("smoke/#", 1)
	b.Pub("smoke/2", 1, false, "from 3.1.1")
	if m := b.Expect(1, 2*time.Second); string(m[0].Payload()) != "from 3.1.1" {
		t.Fatalf("%q", m[0].Payload())
	}
	if b.SessionPresent() {
		t.Fatal("clean session reported present")
	}
	a.Expect(1, 2*time.Second)
	r := n.DialRaw(packet.V5)
	if c := r.Connect(&packet.Connect{ClientID: "raw", CleanStart: true}); c.ReasonCode != packet.Success {
		t.Fatalf("raw connack %s", c.ReasonCode)
	}
	r.Send(&packet.Pingreq{})
	if _, ok := r.Read().(*packet.Pingresp); !ok {
		t.Fatal("no PINGRESP")
	}
	a.Disconnect(nil)
	b.Disconnect()
}
