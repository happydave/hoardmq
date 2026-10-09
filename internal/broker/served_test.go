package broker

import (
	"fmt"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
)

// TestNoAcknowledgementWhileALostNodesSessionIsUnserved: a lost node's leases
// move the moment the reassignment applies, and the adopter imports each
// session afterwards. Between the two nobody advertises the session's
// filter, so the lost node's hidden routes are all that stands between a
// publish and an acknowledgement with no subscriber. They must hold until the
// adoption has finished, and let go once it has.
func TestNoAcknowledgementWhileALostNodesSessionIsUnserved(t *testing.T) {
	fixtureHooks = Hooks{AdoptionDelay: 4 * time.Second}
	t.Cleanup(func() { fixtureHooks = Hooks{} })
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
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if rec, ok := nodes["n1"].srv.Sessions().Record("x"); ok && !rec.Online {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	pub, _ := nodes["n2"].connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	id := uint16(0)
	publish := func(payload string) packet.ReasonCode {
		t.Helper()
		id++
		pub.send(&packet.Publish{Topic: "t/x", QoS: 1, PacketID: id, Payload: []byte(payload)})
		a, ok := pub.read().(*packet.Puback)
		if !ok || a.PacketID != id {
			t.Fatalf("PUBACK %d: %+v", id, a)
		}
		return a.ReasonCode
	}
	// 0x10, no matching subscribers, is an acknowledgement too: it is what a
	// publish is answered once nobody advertises the filter.
	acknowledged := func(rc packet.ReasonCode) bool { return rc < 0x80 }
	// A freshly formed cluster can refuse a first publish once; the setup
	// publish is retried the way a client would.
	accepted := false
	for i := 0; i < 40 && !accepted; i++ {
		if accepted = publish("before") == packet.Success; !accepted {
			time.Sleep(100 * time.Millisecond)
		}
	}
	if !accepted {
		t.Fatal("setup publish never accepted with every node up")
	}

	nodes["n1"].stop()
	unfinished := func() int {
		n := 0
		for _, name := range []string{"n2", "n3"} {
			_, u := nodes[name].srv.Leases().AdoptionState()
			n += u
		}
		return n
	}
	// The window under test opens when n1 owns nothing and the shard map has
	// dropped it (before that the replication barrier refuses, for its own
	// reason), with the adoption still held back.
	deadline = time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		owned := nodes["n2"].srv.Leases().Table().CountOwnedBy("n1")
		basis := len(nodes["n2"].srv.Replication().Map().Current().Basis)
		if owned == 0 && basis == 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if unfinished() == 0 {
		t.Fatal("the adoption had finished before the window could be tested; the delay is too short for this machine")
	}
	var acked []string
	inWindow := 0
	for unfinished() > 0 {
		payload := fmt.Sprint("w", inWindow)
		rc := publish(payload)
		if stillUnfinished := unfinished() > 0; stillUnfinished && acknowledged(rc) {
			t.Fatalf("publish %q acknowledged (%s) while x's adoption is unfinished: nobody is serving x", payload, rc)
		}
		if acknowledged(rc) {
			acked = append(acked, payload)
		}
		inWindow++
		time.Sleep(100 * time.Millisecond)
	}
	if inWindow == 0 {
		t.Fatal("no publish was made inside the window")
	}

	var rc packet.ReasonCode
	for i := 0; i < 60; i++ {
		payload := fmt.Sprint("a", i)
		if rc = publish(payload); rc == packet.Success {
			acked = append(acked, payload)
			break
		}
		if acknowledged(rc) {
			for _, name := range []string{"n2", "n3"} {
				rec, ok := nodes[name].srv.Sessions().Record("x")
				l, _ := nodes[name].srv.Leases().Table().Lookup("default", "x")
				_ = rec
				t.Logf("%s: session x held=%v; lease owner %s epoch %d; replica copy held=%v", name, ok, l.Owner, l.Epoch, nodes[name].srv.Replication().ReplicaState().Find("default", "x") != nil)
			}
			t.Fatalf("publish %q answered %s after the adoption: x is adopted and its filter should match", payload, rc)
		}
		time.Sleep(250 * time.Millisecond)
	}
	if rc != packet.Success {
		for _, name := range []string{"n2", "n3"} {
			rt := nodes[name].srv.Routes()
			t.Logf("%s: ownersKnown=%v hiddenMatch=%v movingMatch=%v hidden=%v; streams %+v; refused %v", name, rt.OwnersKnown(), rt.Table().HiddenMatch("default", "t/x"), rt.Table().MovingMatch("default", "t/x"), rt.Table().HiddenOwners(), nodes[name].srv.Replication().StreamStates(), nodes[name].srv.Replication().RefusedKeys())
		}
		t.Fatalf("fifteen seconds after the adoption finished a publish to t/x is still answered %s", rc)
	}

	// Every message the publisher was told was accepted reaches x.
	x2, ack := nodes["n3"].connect("tcp", packet.V5, &packet.Connect{ClientID: "x", CleanStart: false, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	if ack.ReasonCode != packet.Success || !ack.SessionPresent {
		t.Fatalf("x reconnecting: %+v", ack)
	}
	want := map[string]bool{"before": true}
	for _, p := range acked {
		want[p] = true
	}
	got := map[string]bool{}
	for len(want) > 0 {
		m, ok := x2.read().(*packet.Publish)
		if !ok {
			t.Fatalf("x is missing acknowledged messages %v (received %v)", want, got)
		}
		got[string(m.Payload)] = true
		delete(want, string(m.Payload))
		x2.send(&packet.Puback{Ack: packet.Ack{PacketID: m.PacketID}})
	}
}
