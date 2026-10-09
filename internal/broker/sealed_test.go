package broker

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

// giverDiesAfterTheHandOver: the previous owner n1 serves the takeover pull for x, its answer never reaches
// the requester n2, and n1 dies. x's only replica is on n3. n3 runs with its
// epoch check off: that is the one clause that could refuse n1's delete, and
// it stands for n3's table not yet having applied the grant, as when the dead
// node was the leader and its delete overtook the commit. It returns what x is
// served when it retries on n2.
func giverDiesAfterTheHandOver(t *testing.T, removeOnHandOver bool) (ack *packet.Connack, x2 *client) {
	t.Helper()
	t.Cleanup(func() { fixtureHooks = Hooks{} })
	nodes := startTrio(t, func(name string, d *config.Document) {
		d.Cluster.Shards = 8
		d.Cluster.Replicas = 2
		fixtureHooks = Hooks{Defects: Defects{RemoveOnHandOver: removeOnHandOver}}
		if name == "n3" {
			fixtureHooks.Defects.SkipEpochCheck = true
		}
	})
	fixtureHooks = Hooks{}
	m := waitMap(t, nodes, 3)
	id := ""
	for i := 0; id == ""; i++ {
		if c := fmt.Sprint("x", i); slices.Equal(m.Targets(shardmap.ShardOf("default", c, 8), "n1"), []string{"n3"}) {
			id = c
		}
	}
	exp := uint32(600)
	x, ack := nodes["n1"].connect("tcp", packet.V5, &packet.Connect{ClientID: id, CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("connect on n1: %s", ack.ReasonCode)
	}
	x.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "t/x", QoS: 1}}})
	if _, ok := x.read().(*packet.Suback); !ok {
		t.Fatal("no SUBACK")
	}
	x.send(&packet.Disconnect{ReasonCode: packet.NormalDisconnection})
	_ = x.c.Close()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if rec, ok := nodes["n1"].srv.Sessions().Record(id); ok && !rec.Online {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	pub, _ := nodes["n2"].connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	// A freshly formed cluster can refuse the first QoS 1 publish forwarded to
	// a peer (WI 1572): wait until n2 has forwarded one to a subscriber on n1.
	w, _ := nodes["n1"].connect("tcp", packet.V5, &packet.Connect{ClientID: "warm", CleanStart: true}, false)
	w.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "warm", QoS: 1}}})
	if _, ok := w.read().(*packet.Suback); !ok {
		t.Fatal("no SUBACK for the warm-up subscriber")
	}
	accepted := false
	for pid := uint16(100); pid < 150 && !accepted; pid++ {
		pub.send(&packet.Publish{Topic: "warm", QoS: 1, PacketID: pid})
		a, ok := pub.read().(*packet.Puback)
		accepted = ok && a.ReasonCode == packet.Success
		if !accepted {
			time.Sleep(100 * time.Millisecond)
		}
	}
	if !accepted {
		t.Fatal("precondition not reached: n2 had no QoS 1 publish to n1's subscriber accepted in 50 attempts")
	}
	for i := 1; i <= 3; i++ {
		pub.send(&packet.Publish{Topic: "t/x", QoS: 1, PacketID: uint16(i), Payload: []byte(fmt.Sprint("m", i))})
		if a, ok := pub.read().(*packet.Puback); !ok || a.ReasonCode != packet.Success {
			t.Fatalf("PUBACK %d: %+v", i, a)
		}
	}
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if ss := nodes["n3"].srv.Replication().ReplicaState().Find("default", id); ss != nil && len(ss.Entries) == 3 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if ss := nodes["n3"].srv.Replication().ReplicaState().Find("default", id); ss == nil || len(ss.Entries) != 3 {
		t.Fatalf("precondition not reached: n3 does not hold %s's replica with three queued messages (%+v)", id, ss)
	}
	if ss := nodes["n2"].srv.Replication().ReplicaState().Find("default", id); ss != nil {
		t.Fatalf("precondition not reached: n2 holds a copy of %s itself", id)
	}

	// n1's answers to n2's pulls are lost.
	nodes["n1"].srv.Cluster().SetFault("n2", &transport.Fault{Drop: 1, Classes: []transport.Class{transport.TakeoverAck}, Rand: rand.New(rand.NewSource(1))})
	nc := nodes["n2"].dial("tcp", false)
	t.Cleanup(func() { _ = nc.Close() })
	first := &client{t: t, c: nc, enc: &packet.Encoder{Version: packet.V5}, dec: packet.NewDecoder(nc)}
	first.dec.Version = packet.V5
	first.send(&packet.Connect{ProtocolLevel: packet.V5, ClientID: id, Properties: packet.Properties{SessionExpiryInterval: &exp}})
	answered := make(chan packet.Packet, 1)
	go func() {
		_ = nc.SetReadDeadline(time.Now().Add(15 * time.Second))
		p, err := first.dec.Read()
		if err != nil {
			p = nil
		}
		answered <- p
	}()
	// n1's pass has handed the session over, however it did so.
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := nodes["n1"].srv.Sessions().Record(id); !ok {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, ok := nodes["n1"].srv.Sessions().Record(id); ok {
		t.Fatal("precondition not reached: n1 never served n2's pull")
	}
	// Whatever n1 wrote for x has reached its follower before it dies: a
	// delete, if one was written, has been applied on n3.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := nodes["n1"].srv.Sessions().Durable(ctx, []store.Scope{{Namespace: "default", ClientID: id}}); err != nil {
		t.Fatalf("precondition not reached: n1's records for %s not acknowledged by n3: %v", id, err)
	}
	nodes["n1"].srv.Crash()
	nodes["n1"].stopped = true

	if p, ok := (<-answered).(*packet.Connack); ok && p.ReasonCode == packet.Success {
		return p, first
	}
	deadline = time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		c, a := nodes["n2"].connect("tcp", packet.V5, &packet.Connect{ClientID: id, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
		if a.ReasonCode == packet.Success {
			return a, c
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("precondition not reached: x was never accepted on n2 after n1 died")
	return nil, nil
}

// TestGiverDyingAfterTheHandOverLosesNothing: nothing was deleted when n1
// handed the session over, so n3's replica is there for n2 once n1 is gone.
func TestGiverDyingAfterTheHandOverLosesNothing(t *testing.T) {
	ack, x2 := giverDiesAfterTheHandOver(t, false)
	if !ack.SessionPresent {
		t.Fatal("x was served a fresh session: the session was lost with the previous owner")
	}
	for i := 1; i <= 3; i++ {
		msg, ok := x2.read().(*packet.Publish)
		if !ok || string(msg.Payload) != fmt.Sprint("m", i) {
			t.Fatalf("queued message %d: %+v", i, msg)
		}
		x2.send(&packet.Puback{Ack: packet.Ack{PacketID: msg.PacketID}})
	}
}

// TestGiverDyingAfterARemovingHandOverLosesTheSession: with the pass removing
// the session and journaling its delete, the delete reaches n3 and removes the
// only copy.
func TestGiverDyingAfterARemovingHandOverLosesTheSession(t *testing.T) {
	ack, _ := giverDiesAfterTheHandOver(t, true)
	if ack.SessionPresent {
		t.Fatal("x kept its session with the hand-over removing it: the scenario no longer reaches the loss it is the control for")
	}
}
