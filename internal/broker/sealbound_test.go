package broker

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

// With one configured copy, x's session is held on n1 alone. x reconnects to
// n2, which takes the lease and pulls n1. n1 seals the session and answers,
// and every answer n1 sends n2 is lost, so n2 imports nothing and confirms
// nothing while it stays alive by the log. The sealed copy is then the only
// copy of x's session.

// TestSealedCopyOutlivesItsBoundWhileTheNewOwnerIsAlive: n1's answers are
// lost for twice the seal's bound, then reach n2 again. n2 must serve x its
// session, with the three messages acknowledged before the takeover.
func TestSealedCopyOutlivesItsBoundWhileTheNewOwnerIsAlive(t *testing.T) {
	t.Cleanup(func() { fixtureHooks = Hooks{} })
	nodes := startTrio(t, func(name string, d *config.Document) {
		d.Cluster.Shards = 8
		d.Cluster.Replicas = 1
	})
	waitMap(t, nodes, 3)
	giver, owner := nodes["n1"], nodes["n2"]
	const id = "x"
	exp := uint32(600)
	x, ack := giver.connect("tcp", packet.V5, &packet.Connect{ClientID: id, CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("connect x on n1: %s", ack.ReasonCode)
	}
	x.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "t/x", QoS: 1}}})
	if _, ok := x.read().(*packet.Suback); !ok {
		t.Fatal("no SUBACK")
	}
	x.send(&packet.Disconnect{ReasonCode: packet.NormalDisconnection})
	_ = x.c.Close()
	waitOffline(t, giver, id)
	pub, _ := giver.connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	for i := 1; i <= 3; i++ {
		pub.send(&packet.Publish{Topic: "t/x", QoS: 1, PacketID: uint16(i), Payload: []byte(fmt.Sprint("m", i))})
		if a, ok := pub.read().(*packet.Puback); !ok || a.ReasonCode != packet.Success {
			t.Fatalf("PUBACK %d: %+v", i, a)
		}
	}
	for _, n := range []string{"n2", "n3"} {
		if ss := nodes[n].srv.Replication().ReplicaState().Find("default", id); ss != nil {
			t.Fatalf("precondition not reached: %s holds a replica of x with one configured copy", n)
		}
	}

	// Every answer n1 sends n2 is lost.
	giver.srv.Cluster().SetFault("n2", &transport.Fault{Drop: 1, Classes: []transport.Class{transport.TakeoverAck}, Rand: rand.New(rand.NewSource(1))})
	t.Cleanup(func() { giver.srv.Cluster().SetFault("n2", nil) })
	if _, a := owner.connect("tcp", packet.V5, &packet.Connect{ClientID: id, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false); a.ReasonCode == packet.Success {
		t.Fatalf("precondition not reached: x was accepted on n2 (session present %v) with n1's answers lost", a.SessionPresent)
	}
	sealedAt := time.Now()
	waitUntil(t, "n1 to seal x for n2", 5*time.Second, func() bool {
		_, _, ok := giver.srv.Sessions().Sealed("default", id)
		return ok
	})
	bound := 2 * 2 * time.Second // SealTimeout: twice the trio's ttl
	time.Sleep(time.Until(sealedAt.Add(2 * bound)))

	// Past the bound twice over, n2 is alive by n1's log, owns x's lease, and
	// holds no session for x.
	l, ok := giver.srv.Leases().Table().Lookup("default", id)
	if !ok || l.Owner != "n2" || !giver.srv.Leases().Table().Live("n2") {
		t.Fatalf("precondition not reached: x's lease on n1's table %+v (found %v), n2 alive %v", l, ok, giver.srv.Leases().Table().Live("n2"))
	}
	if _, held := owner.srv.Sessions().Record(id); held {
		t.Fatal("precondition not reached: n2 holds x's session although every answer from n1 was lost")
	}
	_, _, stillSealed := giver.srv.Sessions().Sealed("default", id)
	stats := giver.srv.Leases().LivenessStats()
	t.Logf("past twice the bound: n1 holds x sealed %v; seals expired %d, confirmed %d", stillSealed, stats.SealsExpired, stats.SealsConfirmed)

	giver.srv.Cluster().SetFault("n2", nil)
	var served *packet.Connack
	var x2 *client
	deadline := time.Now().Add(30 * time.Second)
	for served == nil && time.Now().Before(deadline) {
		c, a := owner.connect("tcp", packet.V5, &packet.Connect{ClientID: id, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
		if a.ReasonCode == packet.Success {
			served, x2 = a, c
			break
		}
		_ = c.c.Close()
		time.Sleep(200 * time.Millisecond)
	}
	if served == nil {
		t.Fatal("x was never accepted on n2 once n1's answers reached it")
	}
	if !served.SessionPresent {
		t.Fatalf("x was served a fresh session on n2: n1 dropped the only copy while n2 was alive and had imported nothing (seals expired %d, confirmed %d)",
			giver.srv.Leases().LivenessStats().SealsExpired, giver.srv.Leases().LivenessStats().SealsConfirmed)
	}
	for i := 1; i <= 3; i++ {
		msg := x2.readPublish()
		if string(msg.Payload) != fmt.Sprint("m", i) {
			t.Fatalf("queued message %d: %q", i, msg.Payload)
		}
		x2.send(&packet.Puback{Ack: packet.Ack{PacketID: msg.PacketID}})
	}
}

// TestSealedCopyEndsOnceTheNewOwnersTargetHoldsIt: with two copies, every
// takeover request n2 sends n1 is lost, its pull and its confirmation alike.
// n2 takes x from x's replica, writes it to its own target and confirms
// nothing n1 receives; n1's reclaim seals x. n1 must drop the seal on its
// check, once n2's target holds x at n2's grant, and x must keep its messages
// on n2.
func TestSealedCopyEndsOnceTheNewOwnersTargetHoldsIt(t *testing.T) {
	t.Cleanup(func() { fixtureHooks = Hooks{} })
	nodes := startTrio(t, func(name string, d *config.Document) {
		d.Cluster.Shards = 8
		d.Cluster.Replicas = 2
	})
	waitMap(t, nodes, 3)
	giver, owner := nodes["n1"], nodes["n2"]
	const id = "x"
	exp := uint32(600)
	x, ack := giver.connect("tcp", packet.V5, &packet.Connect{ClientID: id, CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("connect x on n1: %s", ack.ReasonCode)
	}
	x.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "t/x", QoS: 1}}})
	if _, ok := x.read().(*packet.Suback); !ok {
		t.Fatal("no SUBACK")
	}
	x.send(&packet.Disconnect{ReasonCode: packet.NormalDisconnection})
	_ = x.c.Close()
	waitOffline(t, giver, id)
	pub, _ := giver.connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	for i := 1; i <= 3; i++ {
		pub.send(&packet.Publish{Topic: "t/x", QoS: 1, PacketID: uint16(i), Payload: []byte(fmt.Sprint("m", i))})
		if a, ok := pub.read().(*packet.Puback); !ok || a.ReasonCode != packet.Success {
			t.Fatalf("PUBACK %d: %+v", i, a)
		}
	}

	owner.srv.Cluster().SetFault("n1", &transport.Fault{Drop: 1, Classes: []transport.Class{transport.TakeoverRequest}, Rand: rand.New(rand.NewSource(1))})
	t.Cleanup(func() { owner.srv.Cluster().SetFault("n1", nil) })
	var served *packet.Connack
	var x2 *client
	deadline := time.Now().Add(20 * time.Second)
	for served == nil && time.Now().Before(deadline) {
		c, a := owner.connect("tcp", packet.V5, &packet.Connect{ClientID: id, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
		if a.ReasonCode == packet.Success {
			served, x2 = a, c
			break
		}
		_ = c.c.Close()
		time.Sleep(200 * time.Millisecond)
	}
	if served == nil || !served.SessionPresent {
		t.Fatalf("precondition not reached: x on n2 with n2's pulls of n1 lost: %+v", served)
	}
	waitUntil(t, "n1's reclaim to seal x", 10*time.Second, func() bool {
		_, _, ok := giver.srv.Sessions().Sealed("default", id)
		return ok
	})
	sealed := true
	for deadline := time.Now().Add(20 * time.Second); sealed && time.Now().Before(deadline); {
		time.Sleep(100 * time.Millisecond)
		_, _, sealed = giver.srv.Sessions().Sealed("default", id)
	}
	if sealed {
		t.Fatalf("n1 still holds x sealed 20 s after its reclaim, with n2's target holding x: %+v", giver.srv.Leases().LivenessStats())
	}
	if ls := giver.srv.Leases().LivenessStats(); ls.SealsHeld < 1 || ls.SealChecksHeld < 1 || ls.SealsConfirmed != 0 {
		t.Fatalf("n1's seal ended without a target of n2 seen holding x: %+v", ls)
	}
	for i := 1; i <= 3; i++ {
		msg := x2.readPublish()
		if string(msg.Payload) != fmt.Sprint("m", i) {
			t.Fatalf("queued message %d: %q", i, msg.Payload)
		}
		x2.send(&packet.Puback{Ack: packet.Ack{PacketID: msg.PacketID}})
	}
}
