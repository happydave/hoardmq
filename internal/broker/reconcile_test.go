package broker

import (
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
)

// TestReconcileRestoresARouteAnotherNodeNeeds: n1 holds a subscriber whose
// route it no longer advertises, as a lost or inverted notification leaves it.
// A QoS 1 publish on n3 that matches only that subscriber is answered No
// Matching Subscribers; after two passes of n1's reconciler n3 has the route
// and the publish is delivered.
func TestReconcileRestoresARouteAnotherNodeNeeds(t *testing.T) {
	nodes := startTrio(t, nil)
	waitRoute := func(what string, live bool) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if e, ok := nodes["n3"].srv.Routes().Table().Lookup("n1", "default", "rc/x"); ok && e.Tombstone == !live {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for %s", what)
	}
	x, ack := nodes["n1"].connect("tcp", packet.V5, &packet.Connect{ClientID: "x", CleanStart: true}, false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("connect on n1: %s", ack.ReasonCode)
	}
	x.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "rc/x", QoS: 1}}})
	if _, ok := x.read().(*packet.Suback); !ok {
		t.Fatal("no SUBACK")
	}
	waitRoute("n1's route at n3", true)

	// The seeded difference: the plane told the filter has no subscriber.
	nodes["n1"].srv.Routes().Observe("default", "rc/x", 0)
	waitRoute("n1's withdrawal at n3", false)
	pub, _ := nodes["n3"].connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	pub.send(&packet.Publish{Topic: "rc/x", QoS: 1, PacketID: 1, Payload: []byte("lost")})
	if a, ok := pub.read().(*packet.Puback); !ok || a.ReasonCode != packet.NoMatchingSubscribers {
		t.Fatalf("the premise: before the correction the publish on n3 was answered %+v, want No Matching Subscribers", a)
	}

	for i := 0; i < 2; i++ {
		if !nodes["n1"].srv.Sessions().ReconcilePass() {
			t.Fatal("n1's dispatcher cannot be reconciled")
		}
	}
	waitRoute("n1's corrected route at n3", true)
	pub.send(&packet.Publish{Topic: "rc/x", QoS: 1, PacketID: 2, Payload: []byte("found")})
	if a, ok := pub.read().(*packet.Puback); !ok || a.ReasonCode != packet.Success {
		t.Fatalf("after the correction the publish on n3 was answered %+v, want Success", a)
	}
	if m, ok := x.read().(*packet.Publish); !ok || string(m.Payload) != "found" {
		t.Fatalf("after the correction x received %+v, want the publish", m)
	}
}
