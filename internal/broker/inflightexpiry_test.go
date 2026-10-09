package broker

import (
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
)

// unappliedOf sums the node's count of replicated records it could not
// apply, of the given record type, over every reason.
func unappliedOf(f *fixture, typ string) float64 {
	n := 0.0
	for _, m := range f.srv.Registry().Metrics().Snapshot() {
		if m.Name == "hoardmq_replication_unapplied_total" && m.Labels["type"] == typ {
			n += m.Value
		}
	}
	return n
}

// TestExpiredInflightMessageIsDroppedOnTheReplicas: a QoS 1 message in
// flight to a durable session, left unacknowledged when the client goes and
// expired when it comes back, is dropped by the owner; its replicas drop it
// too, and no follower refuses the record that drops it.
func TestExpiredInflightMessageIsDroppedOnTheReplicas(t *testing.T) {
	nodes, _ := startReplicatedTrio(t)
	exp := uint32(600)
	x, ack := nodes["n1"].connect("tcp", packet.V5, &packet.Connect{ClientID: "x", CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("connect: %s", ack.ReasonCode)
	}
	x.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "t/x", QoS: 1}}})
	if _, ok := x.read().(*packet.Suback); !ok {
		t.Fatal("no SUBACK")
	}
	pub, _ := nodes["n1"].connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	one := uint32(1)
	var sent time.Time
	for id := uint16(1); ; id++ {
		if id > 40 {
			t.Fatal("setup publish never accepted")
		}
		sent = time.Now()
		pub.send(&packet.Publish{Topic: "t/x", QoS: 1, PacketID: id, Payload: []byte("short"), Properties: packet.Properties{MessageExpiryInterval: &one}})
		if a, ok := pub.read().(*packet.Puback); ok && a.ReasonCode == packet.Success {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if p := x.readPublish(); string(p.Payload) != "short" {
		t.Fatalf("delivered %q", p.Payload)
	}
	// Left unacknowledged: the message is in flight on n1 and on its replicas.
	held := func() []string {
		var out []string
		for _, n := range []string{"n2", "n3"} {
			if ss := nodes[n].srv.Replication().ReplicaState().Find("default", "x"); ss != nil && len(ss.Entries) > 0 {
				out = append(out, n)
			}
		}
		return out
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && len(held()) == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	holders := held()
	if len(holders) == 0 {
		t.Fatal("precondition: no replica holds x's in-flight message")
	}
	_ = x.c.Close()
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if rec, ok := nodes["n1"].srv.Sessions().Record("x"); ok && !rec.Online {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	// The node stamps the expiry when it takes the publish in, after sent.
	time.Sleep(time.Until(sent.Add(2500 * time.Millisecond)))
	x2, ack := nodes["n1"].connect("tcp", packet.V5, &packet.Connect{ClientID: "x", Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	if ack.ReasonCode != packet.Success || !ack.SessionPresent {
		t.Fatalf("reconnect: %s, session present %v", ack.ReasonCode, ack.SessionPresent)
	}
	x2.expectNoPacket()
	if rec, _ := nodes["n1"].srv.Sessions().Record("x"); rec.Expired != 1 || rec.InFlight != 0 {
		t.Fatalf("precondition: n1 did not drop the message as expired: %+v", rec)
	}
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && len(held()) > 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if h := held(); len(h) > 0 {
		t.Fatalf("%v still hold x's expired message", h)
	}
	for _, n := range holders {
		if got := unappliedOf(nodes[n], "dropped") + unappliedOf(nodes[n], "complete"); got != 0 {
			t.Fatalf("%s refused %v records dropping x's expired message", n, got)
		}
	}
}
