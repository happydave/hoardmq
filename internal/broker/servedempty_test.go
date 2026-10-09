package broker

import (
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/lease"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/module"
	"github.com/happydave/hoardmq/internal/packet"
)

// servedEmpty totals hoardmq_sessions_rebuilt_empty_total on a node.
func servedEmpty(t *testing.T, f *fixture) int {
	t.Helper()
	snap, ok := f.srv.Registry().Metrics().(interface{ Snapshot() []module.Metric })
	if !ok {
		t.Fatal("the metrics module has no snapshot")
	}
	n := 0
	for _, m := range snap.Snapshot() {
		if m.Name == "hoardmq_sessions_rebuilt_empty_total" {
			n += int(m.Value)
		}
	}
	return n
}

// TestSessionServedEmptyIsCountedOnce: with one replica a session lives only
// on its owner. When the owner is gone, a client that held a session there is
// served a fresh one — Session Present false, by design when no copy exists —
// and that CONNECT is counted once. A client that reconnects to the node
// holding its session is served it and counts nothing.
func TestSessionServedEmptyIsCountedOnce(t *testing.T) {
	nodes := startTrio(t, func(_ string, d *config.Document) {
		d.Cluster.Shards = 8
		d.Cluster.Replicas = 1
	})
	waitMap(t, nodes, 3)
	exp := uint32(600)
	subscribe := func(node, id string) {
		c, ack := nodes[node].connect("tcp", packet.V5, &packet.Connect{ClientID: id, CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
		if ack.ReasonCode != packet.Success {
			t.Fatalf("connect %s on %s: %s", id, node, ack.ReasonCode)
		}
		c.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "t/" + id, QoS: 1}}})
		if _, ok := c.read().(*packet.Suback); !ok {
			t.Fatalf("no SUBACK for %s", id)
		}
		c.send(&packet.Disconnect{ReasonCode: packet.NormalDisconnection})
		_ = c.c.Close()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if l, ok := nodes[node].srv.Leases().Table().Lookup("default", id); ok && !l.Online {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("precondition not reached: %s's lease never went offline on %s", id, node)
	}
	subscribe("n1", "a")
	subscribe("n2", "b")
	// b's session is held where it reconnects: served, not counted.
	if _, ack := nodes["n2"].connect("tcp", packet.V5, &packet.Connect{ClientID: "b", Properties: packet.Properties{SessionExpiryInterval: &exp}}, false); ack.ReasonCode != packet.Success || !ack.SessionPresent {
		t.Fatalf("b on its own node: %s, Session Present %v", ack.ReasonCode, ack.SessionPresent)
	}
	nodes["n1"].srv.Crash()
	nodes["n1"].stopped = true
	var got *packet.Connack
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) && got == nil {
		_, ack := nodes["n2"].connect("tcp", packet.V5, &packet.Connect{ClientID: "a", Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
		if ack.ReasonCode == packet.Success {
			got = ack
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if got == nil {
		t.Fatal("precondition not reached: a was never accepted on n2 after n1 stopped")
	}
	if got.SessionPresent {
		t.Fatal("a was told Session Present with its only copy gone")
	}
	total, stats := 0, map[string]int64{}
	for _, name := range []string{"n2", "n3"} {
		total += servedEmpty(t, nodes[name])
		for r, n := range nodes[name].srv.Leases().LivenessStats().ServedEmpty {
			stats[r] += n
		}
	}
	sum := 0
	for _, r := range lease.EmptyReasons {
		sum += int(stats[r])
	}
	if total != 1 || sum != 1 {
		t.Fatalf("served empty: metric %d, stats %v; want exactly 1", total, stats)
	}
}
