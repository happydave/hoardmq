package broker

import (
	"fmt"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

// TestNewOwnersPublishesAreNotRefusedPastTheConfirmation is WI 1805's r8 at
// the product's 5 s move grace: a subscriber taken over from n1 to n2, and a
// publisher on n2 publishing to its topic. n1 keeps advertising the topic for
// the grace, so each publish is forwarded there too. Once n2's confirmation
// has reached n1, n1 accepts n2's forwards, so the publisher is refused at
// most until then, not for the whole grace, and the moved subscriber gets
// every message once.
func TestNewOwnersPublishesAreNotRefusedPastTheConfirmation(t *testing.T) {
	nodes := startTrio(t, func(_ string, d *config.Document) { d.Cluster.Leases.TakeoverTimeoutMs = 5000 })
	exp := uint32(300)
	x1, ack := nodes["n1"].connect("tcp", packet.V5, &packet.Connect{ClientID: "x", CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("connect on n1: %s", ack.ReasonCode)
	}
	x1.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "t/x", QoS: 1}}})
	if _, ok := x1.read().(*packet.Suback); !ok {
		t.Fatal("no SUBACK")
	}
	for _, n := range []string{"n2", "n3"} {
		waitUntil(t, n+" to route t/x to n1", 5*time.Second, func() bool {
			return nodes[n].srv.routes.Table().Known("n1", "default", "t/x")
		})
	}
	x2, ack := nodes["n2"].connect("tcp", packet.V5, &packet.Connect{ClientID: "x", CleanStart: false, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	if ack.ReasonCode != packet.Success || !ack.SessionPresent {
		t.Fatalf("connect on n2: %+v", ack)
	}
	moved := time.Now()
	pub, _ := nodes["n2"].connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)

	// One publish every 100 ms for half the grace: the scenario's pace, not a
	// wait for anything.
	const publishes = 25
	codes := make([]packet.ReasonCode, publishes)
	ages := make([]time.Duration, publishes)
	for i := range publishes {
		pub.send(&packet.Publish{Topic: "t/x", QoS: 1, PacketID: uint16(i + 1), Payload: fmt.Appendf(nil, "m%d", i)})
		ackp, ok := pub.read().(*packet.Puback)
		if !ok {
			t.Fatalf("publish %d: no PUBACK", i)
		}
		codes[i], ages[i] = ackp.ReasonCode, time.Since(moved)
		time.Sleep(100 * time.Millisecond)
	}
	if !n1StillMoving(nodes["n1"]) {
		t.Fatal("precondition not reached: n1's move of t/x ended before the publishes did, so nothing here was decided by the confirmation")
	}
	first := -1
	for i, c := range codes {
		if c == packet.Success {
			first = i
			break
		}
	}
	if first < 0 {
		t.Fatalf("every publish refused over %v of a 5 s grace: %v", ages[publishes-1], codes)
	}
	for i := first; i < publishes; i++ {
		if codes[i] != packet.Success {
			t.Fatalf("publish %d, %v after the move, refused (%s) after publish %d was accepted: %v", i, ages[i], codes[i], first, codes)
		}
	}
	seen := map[string]int{}
	for range publishes {
		seen[string(x2.readPublish().Payload)]++
	}
	for i := range publishes {
		if n := seen[fmt.Sprintf("m%d", i)]; n != 1 {
			t.Fatalf("the moved subscriber got m%d %d times: %v", i, n, seen)
		}
	}
	t.Logf("first accepted: publish %d, %v after the move", first, ages[first])
}

// n1StillMoving reports whether n1 still holds t/x's move, so the grace had
// not ended when the publishes did.
func n1StillMoving(f *fixture) bool {
	return f.srv.routes.Table().MovingMatch("default", "t/x")
}
