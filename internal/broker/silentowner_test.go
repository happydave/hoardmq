package broker

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

// TestSilentOwnersTakeoverResumesTheReplica: the previous owner is up but
// does not answer the takeover pull within the CONNECT's budget, and the
// session's replica is on the third node. The client reconnecting on the
// second node resumes the session from that replica — Session Present and the
// queued messages — rather than being refused because the pull spent the
// budget the fetch needed.
func TestSilentOwnersTakeoverResumesTheReplica(t *testing.T) {
	t.Cleanup(func() { fixtureHooks = Hooks{} })
	nodes := startTrio(t, func(name string, d *config.Document) {
		d.Cluster.Shards = 8
		d.Cluster.Replicas = 2
		// Read by start after this mutation: only n1 answers pulls late.
		fixtureHooks = Hooks{}
		if name == "n1" {
			fixtureHooks = Hooks{TakeoverDelay: 3 * time.Second}
		}
	})
	fixtureHooks = Hooks{}
	m := waitMap(t, nodes, 3)
	// A copy on the requester itself is read without any budget, so the case
	// needs n1's replica for the client to live on n3.
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
	var last packet.ReasonCode
	for id := uint16(100); id < 150 && !accepted; id++ {
		pub.send(&packet.Publish{Topic: "warm", QoS: 1, PacketID: id})
		a, ok := pub.read().(*packet.Puback)
		accepted = ok && a.ReasonCode == packet.Success
		if ok {
			last = a.ReasonCode
		}
		if !accepted {
			time.Sleep(100 * time.Millisecond)
		}
	}
	if !accepted {
		rt := nodes["n2"].srv.Routes()
		t.Fatalf("precondition not reached: n2 had no QoS 1 publish to n1's subscriber accepted in 50 attempts (last %s; ownersKnown %v; streams %+v; refused %v)", last, rt.OwnersKnown(), nodes["n2"].srv.Replication().StreamStates(), nodes["n2"].srv.Replication().RefusedKeys())
	}
	for i := 1; i <= 3; i++ {
		pub.send(&packet.Publish{Topic: "t/x", QoS: 1, PacketID: uint16(i), Payload: []byte(fmt.Sprint("m", i))})
		if a, ok := pub.read().(*packet.Puback); !ok || a.PacketID != uint16(i) || a.ReasonCode != packet.Success {
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
	x2, ack := nodes["n2"].connect("tcp", packet.V5, &packet.Connect{ClientID: id, CleanStart: false, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	if ack.ReasonCode != packet.Success || !ack.SessionPresent {
		t.Fatalf("reconnect on n2 with n1 silent: %s, Session Present %v; want Success with the session", ack.ReasonCode, ack.SessionPresent)
	}
	for i := 1; i <= 3; i++ {
		msg, ok := x2.read().(*packet.Publish)
		if !ok || string(msg.Payload) != fmt.Sprint("m", i) {
			t.Fatalf("queued message %d: %+v", i, msg)
		}
		x2.send(&packet.Puback{Ack: packet.Ack{PacketID: msg.PacketID}})
	}
	if st := nodes["n2"].srv.Leases().LivenessStats(); st.TakeoverOwnerSilent != 1 || st.TakeoverOwnerSilentServed != 1 {
		t.Fatalf("n2 counted silent %d, served %d; want 1 and 1", st.TakeoverOwnerSilent, st.TakeoverOwnerSilentServed)
	}
}
