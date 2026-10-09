package broker

import (
	"fmt"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

// waitMap waits until every live node holds a shard map computed over the
// given number of nodes.
func waitMap(t *testing.T, nodes map[string]*fixture, members int) shardmap.Map {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		var m shardmap.Map
		for _, f := range nodes {
			if f.stopped {
				continue
			}
			m = f.srv.Replication().Map().Current()
			if m.Version == 0 || len(m.Basis) != members {
				ok = false
			}
		}
		if ok {
			settleRetainedMoves(nodes, m.Version)
			return m
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("shard map did not settle")
	return shardmap.Map{}
}

// settleRetainedMoves gives the retained moves a map started time to
// complete, best effort for three seconds: a shard refuses retained writes
// while it moves, and a node's first map moves every shard it leads.
func settleRetainedMoves(nodes map[string]*fixture, version uint64) {
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		settled := true
		for _, f := range nodes {
			if f.stopped {
				continue
			}
			r := f.srv.Replication()
			if r.Reconciled() < version || r.RetainedMovesPending() > 0 {
				settled = false
			}
		}
		if settled {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// topicLedBy finds a topic under prefix whose shard the node leads.
func topicLedBy(m shardmap.Map, prefix, node string, shards int) string {
	for i := 0; ; i++ {
		tp := fmt.Sprintf("%s/%d", prefix, i)
		if m.Shards[shardmap.ShardOf("default", tp, shards)].Leader == node {
			return tp
		}
	}
}

func startReplicatedTrio(t *testing.T) (map[string]*fixture, shardmap.Map) {
	t.Helper()
	nodes := startTrio(t, func(_ string, d *config.Document) {
		d.Cluster.Shards = 8
		d.Cluster.Replicas = 2
	})
	return nodes, waitMap(t, nodes, 3)
}

// TestQueuedMessagesSurviveOwnerLoss (AC 1): messages acknowledged to a
// publisher for an offline durable subscriber on n1 are delivered when the
// subscriber comes back on another node after n1 is gone.
func TestQueuedMessagesSurviveOwnerLoss(t *testing.T) {
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
	x.send(&packet.Disconnect{ReasonCode: packet.NormalDisconnection})
	_ = x.c.Close()
	// Wait for n1 to record the session offline before publishing.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if rec, ok := nodes["n1"].srv.Sessions().Record("x"); ok && !rec.Online {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	pub, _ := nodes["n2"].connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	for i := 1; i <= 3; i++ {
		pub.send(&packet.Publish{Topic: "t/x", QoS: 1, PacketID: uint16(i), Payload: []byte(fmt.Sprint("m", i))})
		if a, ok := pub.read().(*packet.Puback); !ok || a.PacketID != uint16(i) {
			t.Fatalf("PUBACK %d: %+v", i, a)
		}
	}
	// The publisher's acknowledgement covers n2's own state; the forward to
	// n1 is asynchronous (1270: at most once between nodes) and n1's enqueue
	// records reach x's follower on n1's own barrier, so wait for them.
	var holder string
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && holder == "" {
		for _, n := range []string{"n2", "n3"} {
			if ss := nodes[n].srv.Replication().ReplicaState().Find("default", "x"); ss != nil && len(ss.Entries) == 3 {
				holder = n
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if holder == "" {
		t.Fatal("no follower holds x with three queued entries after the PUBACKs")
	}
	nodes["n1"].stop()
	// The lease lapses (ttl 2 s) and the leader adopts x, importing the replica.
	deadline = time.Now().Add(15 * time.Second)
	adopter := ""
	for time.Now().Before(deadline) && adopter == "" {
		for _, n := range []string{"n2", "n3"} {
			if l, ok := nodes[n].srv.Leases().Table().Lookup("default", "x"); ok && l.Owner != "n1" {
				if rec, ok := nodes[l.Owner].srv.Sessions().Record("x"); ok && rec.Queued == 3 {
					adopter = l.Owner
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if adopter == "" {
		t.Fatal("no node adopted x with its three queued messages")
	}
	// The client comes back on the node that did not adopt: the state is
	// pulled from the adopter, as in a live takeover.
	other := "n2"
	if adopter == "n2" {
		other = "n3"
	}
	x2, ack := nodes[other].connect("tcp", packet.V5, &packet.Connect{ClientID: "x", CleanStart: false, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	if ack.ReasonCode != packet.Success || !ack.SessionPresent {
		t.Fatalf("reconnect on %s: %+v", other, ack)
	}
	for i := 1; i <= 3; i++ {
		m, ok := x2.read().(*packet.Publish)
		if !ok || string(m.Payload) != fmt.Sprint("m", i) || m.QoS != 1 {
			t.Fatalf("queued message %d after owner loss: %+v", i, m)
		}
		x2.send(&packet.Puback{Ack: packet.Ack{PacketID: m.PacketID}})
	}
	// The subscription survived too, and the publisher is told so: once n1's
	// sessions are served elsewhere its routes no longer refuse the publish.
	start := time.Now()
	var rc packet.ReasonCode
	for id := uint16(9); id < 60; id++ {
		pub.send(&packet.Publish{Topic: "t/x", QoS: 1, PacketID: id, Payload: []byte("after")})
		a, ok := pub.read().(*packet.Puback)
		if !ok {
			t.Fatalf("no PUBACK for %d", id)
		}
		if rc = a.ReasonCode; rc == packet.Success {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if rc != packet.Success {
		for _, n := range []string{"n2", "n3"} {
			rt := nodes[n].srv.Routes()
			t.Logf("%s: ownersKnown=%v hiddenMatch=%v movingMatch=%v n1 owns %d leases; streams %+v", n, rt.OwnersKnown(), rt.Table().HiddenMatch("default", "t/x"), rt.Table().MovingMatch("default", "t/x"), nodes[n].srv.Leases().Table().CountOwnedBy("n1"), nodes[n].srv.Replication().StreamStates())
		}
		t.Fatalf("publish after owner loss still answered %s after %s", rc, time.Since(start).Round(10*time.Millisecond))
	}
	t.Logf("acknowledged %s after the client returned", time.Since(start).Round(10*time.Millisecond))
	if m, ok := x2.read().(*packet.Publish); !ok || string(m.Payload) != "after" {
		t.Fatalf("subscription after owner loss: %+v", m)
	}
}

// TestRetainedSurvivesNodeLoss (AC 1): a retained message whose shard n1
// leads is served after n1 is gone.
func TestRetainedSurvivesNodeLoss(t *testing.T) {
	nodes, m := startReplicatedTrio(t)
	tp := topicLedBy(m, "r", "n1", 8)
	pub, _ := nodes["n1"].connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	pub.send(&packet.Publish{Topic: tp, QoS: 1, Retain: true, PacketID: 1, Payload: []byte("kept")})
	if a, ok := pub.read().(*packet.Puback); !ok || (a.ReasonCode != packet.Success && a.ReasonCode != packet.NoMatchingSubscribers) {
		t.Fatalf("PUBACK: %+v", a)
	}
	nodes["n1"].stop()
	// The map is recomputed over the two survivors once n1's lease lapses.
	m2 := waitMap(t, nodes, 2)
	leader := m2.Shards[shardmap.ShardOf("default", tp, 8)].Leader
	// The new leader promotes its replica; give it a moment.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(nodes[leader].srv.Sessions().Router().RetainedFor("default", tp, time.Now())) == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	sub, _ := nodes["n2"].connect("tcp", packet.V5, &packet.Connect{ClientID: "sub", CleanStart: true}, false)
	sub.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "r/#", QoS: 1}}})
	sub.read()
	got, ok := sub.read().(*packet.Publish)
	if !ok || got.Topic != tp || string(got.Payload) != "kept" || !got.Retain {
		t.Fatalf("retained after node loss: %+v", got)
	}
}

// TestWildcardRetainedAcrossShards (AC 3): a wildcard subscribe returns
// retained messages from every shard leader.
func TestWildcardRetainedAcrossShards(t *testing.T) {
	nodes, m := startReplicatedTrio(t)
	pub, _ := nodes["n1"].connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	want := map[string]bool{}
	id := uint16(1)
	for _, n := range []string{"n1", "n2", "n3"} {
		for i := 0; i < 2; i++ {
			tp := topicLedBy(m, fmt.Sprintf("w/%s%d", n, i), n, 8)
			want[tp] = true
			pub.send(&packet.Publish{Topic: tp, QoS: 1, Retain: true, PacketID: id, Payload: []byte(tp)})
			if a, ok := pub.read().(*packet.Puback); !ok || (a.ReasonCode != packet.Success && a.ReasonCode != packet.NoMatchingSubscribers) {
				t.Fatalf("PUBACK for %s: %+v", tp, a)
			}
			id++
		}
	}
	sub, _ := nodes["n3"].connect("tcp", packet.V5, &packet.Connect{ClientID: "sub", CleanStart: true}, false)
	sub.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "w/#", QoS: 1}}})
	sub.read()
	got := map[string]bool{}
	for range want {
		m, ok := sub.read().(*packet.Publish)
		if !ok || !m.Retain || string(m.Payload) != m.Topic {
			t.Fatalf("retained delivery: %+v", m)
		}
		got[m.Topic] = true
		if m.QoS == 1 {
			sub.send(&packet.Puback{Ack: packet.Ack{PacketID: m.PacketID}})
		}
	}
	if len(got) != len(want) {
		t.Fatalf("got %d of %d retained topics: %v", len(got), len(want), got)
	}
}

// TestRebalanceKeepsConnections (AC 3): a shard-map change moves replicas
// without closing client connections.
func TestRebalanceKeepsConnections(t *testing.T) {
	nodes, m := startReplicatedTrio(t)
	exp := uint32(600)
	c, ack := nodes["n1"].connect("tcp", packet.V5, &packet.Connect{ClientID: "steady", CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("connect: %s", ack.ReasonCode)
	}
	c.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "s/#", QoS: 1}}})
	c.read()
	shard := shardmap.ShardOf("default", "steady", 8)
	targets := m.Targets(shard, "n1")
	if len(targets) != 1 {
		t.Fatalf("targets %v", targets)
	}
	// Wait for the replica on the target.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if nodes[targets[0]].srv.Replication().ReplicaState().Find("default", "steady") != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Remove the target node: the map changes and the replica moves to the
	// remaining node.
	nodes[targets[0]].stop()
	remaining := "n2"
	if targets[0] == "n2" {
		remaining = "n3"
	}
	m2 := waitMap(t, nodes, 2)
	if m2.Version <= m.Version {
		t.Fatalf("map version did not advance: %d → %d", m.Version, m2.Version)
	}
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if ss := nodes[remaining].srv.Replication().ReplicaState().Find("default", "steady"); ss != nil && len(ss.Subscriptions) == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if ss := nodes[remaining].srv.Replication().ReplicaState().Find("default", "steady"); ss == nil || len(ss.Subscriptions) != 1 {
		t.Fatalf("replica did not move to %s: %+v", remaining, ss)
	}
	// The client was never disconnected.
	c.send(&packet.Pingreq{})
	if _, ok := c.read().(*packet.Pingresp); !ok {
		t.Fatal("client lost its connection across the rebalance")
	}
	pub, _ := nodes[remaining].connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	pub.send(&packet.Publish{Topic: "s/1", QoS: 1, PacketID: 1, Payload: []byte("still")})
	pub.read()
	if got, ok := c.read().(*packet.Publish); !ok || string(got.Payload) != "still" {
		t.Fatalf("delivery after rebalance: %+v", got)
	}
}

// TestPublishesAreAcknowledgedAfterANodeIsGone: losing a node refuses QoS 1
// publishes only until the shard map drops it. The node does not come back.
// The topic has no subscriber the lost node ever served.
func TestPublishesAreAcknowledgedAfterANodeIsGone(t *testing.T) {
	nodes, before := startReplicatedTrio(t)
	exp := uint32(600)
	sub, _ := nodes["n2"].connect("tcp", packet.V5, &packet.Connect{ClientID: "s", CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	sub.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "t/p", QoS: 1}}})
	if _, ok := sub.read().(*packet.Suback); !ok {
		t.Fatal("no SUBACK")
	}
	pub, _ := nodes["n2"].connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	publish := func(id uint16) packet.ReasonCode {
		t.Helper()
		pub.send(&packet.Publish{Topic: "t/p", QoS: 1, PacketID: id, Payload: []byte("m")})
		a, ok := pub.read().(*packet.Puback)
		if !ok || a.PacketID != id {
			t.Fatalf("PUBACK %d: %+v", id, a)
		}
		return a.ReasonCode
	}
	publishUntilAccepted(t, pub, "t/p", "m")

	nodes["n1"].stop()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) && nodes["n2"].srv.Replication().Map().Current().Version == before.Version {
		time.Sleep(50 * time.Millisecond)
	}
	m := nodes["n2"].srv.Replication().Map().Current()
	if m.Version == before.Version || len(m.Basis) != 2 {
		t.Fatalf("the shard map did not drop n1: version %d, basis %v", m.Version, m.Basis)
	}
	var last packet.ReasonCode
	for id := uint16(10); id < 50; id++ {
		if last = publish(id); last == packet.Success {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("ten seconds after the map dropped n1 a QoS 1 publish on n2 is still answered %s; streams %+v", last, nodes["n2"].srv.Replication().StreamStates())
}
