package broker

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

// With one configured copy, x's session is held on the giver alone, a node
// that does not lead. x reconnects to the adopter, the other node that does
// not lead, which takes the lease and pulls the giver. The giver seals the
// session and answers, and every answer it sends the adopter is lost, so the
// adopter refuses x and keeps an adoption naming the giver. Then the two are
// cut off from each other; both still reach the leader, so the giver stays
// alive by the log and is never declared.

// giveUpTrio is the trio for these tests and its roles: the leader, the
// giver holding x's session with three acknowledged messages, and the
// adopter that has taken x's lease, refused x, and kept an adoption naming
// the giver, which has sealed x.
func giveUpTrio(t *testing.T) (nodes map[string]*fixture, giver, adopter string, exp uint32) {
	t.Helper()
	nodes = startTrio(t, func(name string, d *config.Document) {
		d.Cluster.Shards = 8
		d.Cluster.Replicas = 1
	})
	waitMap(t, nodes, 3)
	leader := ""
	waitUntil(t, "a leader", 5*time.Second, func() bool {
		leader = leaderOf(nodes)
		return leader != ""
	})
	var rest []string
	for n := range nodes {
		if n != leader {
			rest = append(rest, n)
		}
	}
	slices.Sort(rest)
	giver, adopter = rest[0], rest[1]
	const id = "x"
	exp = uint32(600)
	g := nodes[giver]
	x, ack := g.connect("tcp", packet.V5, &packet.Connect{ClientID: id, CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("connect x on %s: %s", giver, ack.ReasonCode)
	}
	x.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "t/x", QoS: 1}}})
	if _, ok := x.read().(*packet.Suback); !ok {
		t.Fatal("no SUBACK")
	}
	x.send(&packet.Disconnect{ReasonCode: packet.NormalDisconnection})
	_ = x.c.Close()
	waitOffline(t, g, id)
	pub, _ := g.connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	for i := 1; i <= 3; i++ {
		pub.send(&packet.Publish{Topic: "t/x", QoS: 1, PacketID: uint16(i), Payload: []byte(fmt.Sprint("m", i))})
		if a, ok := pub.read().(*packet.Puback); !ok || a.ReasonCode != packet.Success {
			t.Fatalf("PUBACK %d: %+v", i, a)
		}
	}
	for n := range nodes {
		if ss := nodes[n].srv.Replication().ReplicaState().Find("default", id); n != giver && ss != nil {
			t.Fatalf("precondition not reached: %s holds a replica of x with one configured copy", n)
		}
	}

	// Every answer the giver sends the adopter is lost.
	g.srv.Cluster().SetFault(adopter, &transport.Fault{Drop: 1, Classes: []transport.Class{transport.TakeoverAck}, Rand: rand.New(rand.NewSource(1))})
	t.Cleanup(func() { g.srv.Cluster().SetFault(adopter, nil) })
	if _, a := nodes[adopter].connect("tcp", packet.V5, &packet.Connect{ClientID: id, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false); a.ReasonCode == packet.Success {
		t.Fatalf("precondition not reached: x was accepted on %s (session present %v) with %s's answers lost", adopter, a.SessionPresent, giver)
	}
	waitUntil(t, giver+" to seal x", 5*time.Second, func() bool {
		_, _, ok := g.srv.Sessions().Sealed("default", id)
		return ok
	})
	return nodes, giver, adopter, exp
}

// cutPair isolates two nodes from each other, or joins them again.
func cutPair(nodes map[string]*fixture, a, b string, on bool) {
	nodes[a].srv.Cluster().Isolate(b, on)
	nodes[b].srv.Cluster().Isolate(a, on)
}

// TestAdoptionWaitsForAnUnreachableOwnerHoldingItsSeal: the giver and the
// adopter are cut off from each other for twice the adoption's old bound
// (SealTimeout, twice the ttl). x is not to be served fresh on the adopter
// meanwhile, and once the two meet again x must be served its session there,
// with the three messages acknowledged before the takeover.
func TestAdoptionWaitsForAnUnreachableOwnerHoldingItsSeal(t *testing.T) {
	t.Cleanup(func() { fixtureHooks = Hooks{} })
	nodes, giver, adopter, exp := giveUpTrio(t)
	const id = "x"
	g, a := nodes[giver], nodes[adopter]
	cutPair(nodes, giver, adopter, true)
	t.Cleanup(func() { cutPair(nodes, giver, adopter, false) })
	cut := time.Now()
	bound := 2 * 2 * time.Second // SealTimeout: twice the trio's ttl
	time.Sleep(time.Until(cut.Add(2 * bound)))

	// Past twice the bound the giver is alive by the adopter's log, never
	// declared, holds x sealed, and the adopter owns x's lease and holds
	// nothing for it.
	l, ok := a.srv.Leases().Table().Lookup("default", id)
	if !ok || l.Owner != adopter || !a.srv.Leases().Table().Live(giver) {
		t.Fatalf("precondition not reached: x's lease on %s's table %+v (found %v), %s alive %v", adopter, l, ok, giver, a.srv.Leases().Table().Live(giver))
	}
	if _, _, sealed := g.srv.Sessions().Sealed("default", id); !sealed {
		t.Fatalf("precondition not reached: %s no longer holds x sealed: %+v", giver, g.srv.Leases().LivenessStats())
	}
	if _, held := a.srv.Sessions().Record(id); held {
		t.Fatalf("precondition not reached: %s holds x's session although it could not reach %s", adopter, giver)
	}
	stats := a.srv.Leases().LivenessStats()
	t.Logf("past twice the bound: %s adoptions gave up %d, waits past the bound %d, owner-undeclared verdicts %d", adopter, stats.AdoptGaveUp, stats.AdoptWaitsPastBound, stats.AdoptOwnerUndeclared)

	c, ack := a.connect("tcp", packet.V5, &packet.Connect{ClientID: id, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	_, _, sealed := g.srv.Sessions().Sealed("default", id)
	if ack.ReasonCode == packet.Success {
		t.Fatalf("x was served on %s (session present %v) while %s, alive and unreachable, held it sealed %v (adoptions gave up %d)",
			adopter, ack.SessionPresent, giver, sealed, a.srv.Leases().LivenessStats().AdoptGaveUp)
	}
	_ = c.c.Close()
	t.Logf("x refused on %s while cut off from %s: %s", adopter, giver, ack.ReasonCode)

	cutPair(nodes, giver, adopter, false)
	g.srv.Cluster().SetFault(adopter, nil)
	var served *packet.Connack
	var x2 *client
	deadline := time.Now().Add(30 * time.Second)
	for served == nil && time.Now().Before(deadline) {
		c, ack := a.connect("tcp", packet.V5, &packet.Connect{ClientID: id, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
		if ack.ReasonCode == packet.Success {
			served, x2 = ack, c
			break
		}
		_ = c.c.Close()
		time.Sleep(200 * time.Millisecond)
	}
	if served == nil {
		t.Fatalf("x was never accepted on %s once %s was reachable again", adopter, giver)
	}
	if !served.SessionPresent {
		t.Fatalf("x was served a fresh session on %s once %s was reachable again", adopter, giver)
	}
	for i := 1; i <= 3; i++ {
		msg := x2.readPublish()
		if string(msg.Payload) != fmt.Sprint("m", i) {
			t.Fatalf("queued message %d: %q", i, msg.Payload)
		}
		x2.send(&packet.Puback{Ack: packet.Ack{PacketID: msg.PacketID}})
	}
}

// TestAdoptionWaitsForASilentOwnerHoldingItsSeal: the giver stays reachable
// and every answer it sends the adopter stays lost for 40 s, past the
// adoption worker's retries (31 s at the trio's timings). x is not to be
// served fresh on the adopter meanwhile, and once the answers get through x
// must be served its session there, with its three messages.
func TestAdoptionWaitsForASilentOwnerHoldingItsSeal(t *testing.T) {
	t.Cleanup(func() { fixtureHooks = Hooks{} })
	nodes, giver, adopter, exp := giveUpTrio(t)
	const id = "x"
	g, a := nodes[giver], nodes[adopter]
	time.Sleep(40 * time.Second)
	stats := a.srv.Leases().LivenessStats()
	t.Logf("after 40 s: %s adoptions gave up %d, retries %d, silent-owner verdicts %d", adopter, stats.AdoptGaveUp, stats.AdoptRetries, stats.AdoptOwnerSilent)
	c, ack := a.connect("tcp", packet.V5, &packet.Connect{ClientID: id, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	_, _, sealed := g.srv.Sessions().Sealed("default", id)
	_ = c.c.Close()
	if ack.ReasonCode == packet.Success {
		t.Fatalf("x was served on %s (session present %v) while %s, alive and reachable, held it sealed %v", adopter, ack.SessionPresent, giver, sealed)
	}
	t.Logf("x refused on %s while %s's answers are lost: %s", adopter, giver, ack.ReasonCode)

	g.srv.Cluster().SetFault(adopter, nil)
	var served *packet.Connack
	var x2 *client
	deadline := time.Now().Add(30 * time.Second)
	for served == nil && time.Now().Before(deadline) {
		c, ack := a.connect("tcp", packet.V5, &packet.Connect{ClientID: id, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
		if ack.ReasonCode == packet.Success {
			served, x2 = ack, c
			break
		}
		_ = c.c.Close()
		time.Sleep(200 * time.Millisecond)
	}
	if served == nil || !served.SessionPresent {
		t.Fatalf("x on %s once %s's answers got through: %+v", adopter, giver, served)
	}
	for i := 1; i <= 3; i++ {
		msg := x2.readPublish()
		if string(msg.Payload) != fmt.Sprint("m", i) {
			t.Fatalf("queued message %d: %q", i, msg.Payload)
		}
		x2.send(&packet.Puback{Ack: packet.Ack{PacketID: msg.PacketID}})
	}
}

// TestRefusedClientIsSentToTheLeaderPastTheBound: past twice the bound of the
// cut, the adopter sends x to the leader, which reaches the giver; x connects
// there, with the cut still in place, and is served its session with its
// three messages.
func TestRefusedClientIsSentToTheLeaderPastTheBound(t *testing.T) {
	t.Cleanup(func() { fixtureHooks = Hooks{} })
	nodes, giver, adopter, exp := giveUpTrio(t)
	const id = "x"
	a := nodes[adopter]
	leader := leaderOf(nodes)
	cutPair(nodes, giver, adopter, true)
	t.Cleanup(func() { cutPair(nodes, giver, adopter, false) })
	time.Sleep(2 * 2 * 2 * time.Second) // twice SealTimeout, twice the trio's ttl
	c, ack := a.connect("tcp", packet.V5, &packet.Connect{ClientID: id, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	_ = c.c.Close()
	want := "client." + leader + ":1883"
	if ack.ReasonCode != packet.UseAnotherServer || ack.Properties.ServerReference == nil || *ack.Properties.ServerReference != want {
		ref := ""
		if ack.Properties.ServerReference != nil {
			ref = *ack.Properties.ServerReference
		}
		t.Fatalf("x on %s past the bound: %s, server reference %q; want Use Another Server naming %q", adopter, ack.ReasonCode, ref, want)
	}
	var served *packet.Connack
	var x2 *client
	var codes []packet.ReasonCode
	deadline := time.Now().Add(20 * time.Second)
	for served == nil && time.Now().Before(deadline) {
		c, ack := nodes[leader].connect("tcp", packet.V5, &packet.Connect{ClientID: id, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
		if ack.ReasonCode == packet.Success {
			served, x2 = ack, c
			break
		}
		codes = append(codes, ack.ReasonCode)
		_ = c.c.Close()
		time.Sleep(200 * time.Millisecond)
	}
	t.Logf("leader %s: refusals before service %v", leader, codes)
	if served == nil || !served.SessionPresent {
		t.Fatalf("x on the leader %s with the cut in place: %+v", leader, served)
	}
	for i := 1; i <= 3; i++ {
		msg := x2.readPublish()
		if string(msg.Payload) != fmt.Sprint("m", i) {
			t.Fatalf("queued message %d: %q", i, msg.Payload)
		}
		x2.send(&packet.Puback{Ack: packet.Ack{PacketID: msg.PacketID}})
	}
	if st := a.srv.Leases().LivenessStats(); st.AdoptRedirected < 1 || st.AdoptWaitsPastBound != 1 {
		t.Fatalf("%s: redirected %d, waits past the bound %d; want at least 1, and 1", adopter, st.AdoptRedirected, st.AdoptWaitsPastBound)
	}
}
