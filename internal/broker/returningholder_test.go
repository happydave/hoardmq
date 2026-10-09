package broker

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

// n1 owns two offline sessions with three acknowledged messages each: one's
// replica target is n2, the other's n3. n1 is cut off from both, and n2 and n3
// from each other, until every alive record has lapsed. Then n2 and n3 meet
// again on their control connections only, their data connection held down
// and their renewals to each other dropped. Whichever leads declares the other
// and n1 dead, adopts n1's leases and fetches each session, while the
// session's holder on the other node is reachable on its control connection
// and not Up, as a restarted node is before its data connection comes up.

// TestAdoptionWaitsForAHolderReachableOnlyOnItsControlConnection: once the
// data connection is let up, each client must be served its session on the
// leader, with every message acknowledged before the cut.
func TestAdoptionWaitsForAHolderReachableOnlyOnItsControlConnection(t *testing.T) {
	t.Cleanup(func() { fixtureHooks = Hooks{} })
	nodes := startTrio(t, func(name string, d *config.Document) {
		d.Cluster.Shards = 8
		d.Cluster.Replicas = 2
	})
	m := waitMap(t, nodes, 3)
	pick := func(target string) string {
		for i := 0; ; i++ {
			if c := fmt.Sprint("x", i); slices.Equal(m.Targets(shardmap.ShardOf("default", c, 8), "n1"), []string{target}) {
				return c
			}
		}
	}
	held := map[string]string{"n2": pick("n2"), "n3": pick("n3")}
	holderOf := map[string]string{held["n2"]: "n2", held["n3"]: "n3"}
	exp := uint32(600)
	owner := nodes["n1"]
	for _, id := range held {
		c, ack := owner.connect("tcp", packet.V5, &packet.Connect{ClientID: id, CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
		if ack.ReasonCode != packet.Success {
			t.Fatalf("connect %s on n1: %s", id, ack.ReasonCode)
		}
		c.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "t/" + id, QoS: 1}}})
		if _, ok := c.read().(*packet.Suback); !ok {
			t.Fatalf("no SUBACK for %s", id)
		}
		c.send(&packet.Disconnect{ReasonCode: packet.NormalDisconnection})
		_ = c.c.Close()
		waitOffline(t, owner, id)
	}
	pub, _ := owner.connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	acked := map[string][]string{}
	pid := uint16(0)
	for holder, id := range held {
		// A just-formed cluster can refuse the first publish whose record goes
		// to a peer; a refused one is not counted.
		for i := 1; len(acked[id]) < 3 && i <= 50; i++ {
			pid++
			payload := fmt.Sprint(id, "-m", i)
			pub.send(&packet.Publish{Topic: "t/" + id, QoS: 1, PacketID: pid, Payload: []byte(payload)})
			a, ok := pub.read().(*packet.Puback)
			if !ok || a.PacketID != pid {
				t.Fatalf("PUBACK for %s: %+v", payload, a)
			}
			if a.ReasonCode == packet.Success {
				acked[id] = append(acked[id], payload)
			} else {
				time.Sleep(100 * time.Millisecond)
			}
		}
		if len(acked[id]) < 3 {
			t.Fatalf("precondition not reached: %d of three publishes to %s acknowledged", len(acked[id]), id)
		}
		waitUntil(t, fmt.Sprintf("%s to hold %s's replica with its messages", holder, id), 5*time.Second, func() bool {
			ss := nodes[holder].srv.Replication().ReplicaState().Find("default", id)
			return ss != nil && len(ss.Entries) >= len(acked[id])
		})
	}

	cut := func(a, b string, on bool) {
		nodes[a].srv.Cluster().Isolate(b, on)
		nodes[b].srv.Cluster().Isolate(a, on)
	}
	cut("n1", "n2", true)
	cut("n1", "n3", true)
	cut("n2", "n3", true)
	hold := func(on bool) {
		for _, p := range [][2]string{{"n2", "n3"}, {"n3", "n2"}} {
			tr := nodes[p[0]].srv.Cluster()
			tr.HoldData(p[1], on)
			if on {
				tr.SetFault(p[1], &transport.Fault{Drop: 1, Classes: []transport.Class{transport.LeaseAlive}, Rand: rand.New(rand.NewSource(1))})
			} else {
				tr.SetFault(p[1], nil)
			}
		}
	}
	hold(true)
	t.Cleanup(func() { hold(false) })
	time.Sleep(3 * time.Second) // past the trio's ttl of 2 s: every alive record has lapsed
	cut("n2", "n3", false)

	leader := ""
	waitUntil(t, "n2 or n3 to lead and adopt n1's leases", 20*time.Second, func() bool {
		for _, n := range []string{"n2", "n3"} {
			tbl := nodes[n].srv.Leases().Table()
			a, okA := tbl.Lookup("default", held["n2"])
			b, okB := tbl.Lookup("default", held["n3"])
			if okA && okB && a.Owner == n && b.Owner == n {
				leader = n
				return true
			}
		}
		return false
	})
	holder := "n2"
	if leader == "n2" {
		holder = "n3"
	}
	victim := held[holder]
	waitUntil(t, fmt.Sprintf("%s to read %s declared dead", leader, holder), 10*time.Second, func() bool {
		return !nodes[leader].srv.Leases().Table().Live(holder)
	})
	for _, p := range nodes[leader].srv.Cluster().Peers() {
		if p.Name == holder && (!p.ControlUp || p.State == transport.Up) {
			t.Fatalf("precondition not reached: %s sees %s with control up %v and state %s, want control up and not Up", leader, holder, p.ControlUp, p.State)
		}
	}
	if ss := nodes[holder].srv.Replication().ReplicaState().Find("default", victim); ss == nil || len(ss.Entries) < len(acked[victim]) {
		t.Fatalf("precondition not reached: %s no longer holds %s's replica with its messages (%+v)", holder, victim, ss)
	}
	// The adoption's first fetches run while the holder is reachable only on
	// its control connection.
	time.Sleep(1500 * time.Millisecond)
	st := nodes[leader].srv.Replication().FetchStats()
	t.Logf("%s leads; %s holds %s's copy; fetches so far: %+v", leader, holder, victim, st)

	hold(false)
	waitUntil(t, "n2 and n3 to be Up to each other", 10*time.Second, func() bool {
		for _, p := range nodes[leader].srv.Cluster().Peers() {
			if p.Name == holder {
				return p.State == transport.Up
			}
		}
		return false
	})
	for _, id := range []string{victim, held[leader]} {
		var served *packet.Connack
		var c *client
		deadline := time.Now().Add(30 * time.Second)
		for served == nil && time.Now().Before(deadline) {
			cl, a := nodes[leader].connect("tcp", packet.V5, &packet.Connect{ClientID: id, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
			if a.ReasonCode == packet.Success {
				served, c = a, cl
				break
			}
			_ = cl.c.Close()
			time.Sleep(200 * time.Millisecond)
		}
		if served == nil {
			t.Fatalf("%s was never accepted on %s", id, leader)
		}
		if !served.SessionPresent {
			t.Fatalf("%s was served a fresh session on %s while %s held its copy (%s reachable on its control connection only; leader's fetches %+v)",
				id, leader, holderOf[id], holder, nodes[leader].srv.Replication().FetchStats())
		}
		// A refused publish may also have been queued, so the check is that
		// every acknowledged one arrives, not a count.
		got := map[string]bool{}
		missing := func() int {
			n := 0
			for _, want := range acked[id] {
				if !got[want] {
					n++
				}
			}
			return n
		}
		for missing() > 0 {
			msg := c.readPublish()
			got[string(msg.Payload)] = true
			c.send(&packet.Puback{Ack: packet.Ack{PacketID: msg.PacketID}})
		}
		_ = c.c.Close()
	}
}
