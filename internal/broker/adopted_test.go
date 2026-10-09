package broker

import (
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
)

// publishUntilAccepted makes a scenario's setup publish the way a client
// would: a freshly formed cluster can refuse a first QoS 1 publish once, and
// a test that is about something else retries it.
func publishUntilAccepted(t *testing.T, pub *client, topic, payload string) {
	t.Helper()
	var rc packet.ReasonCode
	for id := uint16(1000); id < 1040; id++ {
		pub.send(&packet.Publish{Topic: topic, QoS: 1, PacketID: id, Payload: []byte(payload)})
		a, ok := pub.read().(*packet.Puback)
		if !ok || a.PacketID != id {
			t.Fatalf("PUBACK %d: %+v", id, a)
		}
		if rc = a.ReasonCode; rc == packet.Success {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("setup publish to %s never accepted: %s", topic, rc)
}

// TestAdoptedSessionSurvivesARebalanceBeforeItsImport: a node is lost, its
// offline session's lease is reassigned, and the adopter's import is held
// back long enough for the shard map's rebalance to run first. Whichever
// survivor adopts — the one holding the session's replica or the other —
// the session and its acknowledged queued message must still be there when
// the client returns.
func TestAdoptedSessionSurvivesARebalanceBeforeItsImport(t *testing.T) {
	fixtureHooks = Hooks{AdoptionDelay: 3 * time.Second}
	t.Cleanup(func() { fixtureHooks = Hooks{} })
	nodes, before := startReplicatedTrio(t)
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
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if rec, ok := nodes["n1"].srv.Sessions().Record("x"); ok && !rec.Online {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	pub, _ := nodes["n2"].connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	publishUntilAccepted(t, pub, "t/x", "queued")
	holder := ""
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && holder == "" {
		for _, n := range []string{"n2", "n3"} {
			// At least one: a setup publish the cluster refused was delivered too.
			if ss := nodes[n].srv.Replication().ReplicaState().Find("default", "x"); ss != nil && len(ss.Entries) >= 1 {
				holder = n
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if holder == "" {
		t.Fatal("no survivor holds x's replica with the queued message")
	}

	nodes["n1"].stop()
	// The rebalance must have run while the adoption is still held back, or
	// the run says nothing about the order under test.
	adopter := ""
	deadline = time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		l, _ := nodes["n2"].srv.Leases().Table().Lookup("default", "x")
		m := nodes["n2"].srv.Replication().Map().Current()
		if l.Owner != "n1" && l.Owner != "" && m.Version > before.Version && len(m.Basis) == 2 && nodes[l.Owner].srv.Replication().Reconciled() >= m.Version {
			adopter = l.Owner
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if adopter == "" {
		t.Fatal("x was not reassigned, or the map did not move")
	}
	if _, imported := nodes[adopter].srv.Sessions().Record("x"); imported {
		t.Fatal("the adoption had imported x before the rebalance ran; the delay is too short for this machine")
	}
	t.Logf("replica on %s, adopted by %s", holder, adopter)

	// The adoption runs once its delay is up; a session that is never
	// imported is the defect, so the wait is bounded and the assertion below
	// reports it.
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, imported := nodes[adopter].srv.Sessions().Record("x"); imported {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	rec, ok := nodes[adopter].srv.Sessions().Record("x")
	// A setup publish the cluster refused is still delivered to x, so the
	// queue holds at least the one that was accepted.
	if !ok || rec.Queued < 1 || rec.Subscriptions != 1 {
		t.Fatalf("after the adoption %s serves x: %v, queued %d, subscriptions %d; want the session with its message and its subscription (replica was on %s)", adopter, ok, rec.Queued, rec.Subscriptions, holder)
	}
}
