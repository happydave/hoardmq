package broker

import (
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

// TestSurvivorsThatHandSessionsToEachOtherForgetOnceTheyAreHeld: two
// survivors each hand a lost node's session in transit on to the other. Each
// hand-on waits for the other's claim that it holds the session, so neither
// may make its own claim wait on its hand-on: both forget the lost node once
// the sessions are held, not at the hand-on's bound.
//
// One copy per session. The victim, n2, answers no takeover pull in time, as
// a node does in the instant before its peers see it gone. Its client y
// reconnects to the survivor that does not lead (A), which refuses it and
// keeps an adoption; the victim crashes; y retries on the leader (B), which
// takes the lease from A, so A hands y on to B. After the declaration B adopts
// z, whose client reconnects to A before B's held worker imports it, so B
// hands z on to A.
//
// The node that adopts z is the one leading at the declaration. When the lead
// has moved from B to A by then, the scenario starts again on a new trio.
func TestSurvivorsThatHandSessionsToEachOtherForgetOnceTheyAreHeld(t *testing.T) {
	for attempt := 1; ; attempt++ {
		planned, adoptedBy := twoWayHandOnOnce(t)
		if adoptedBy == "" {
			return
		}
		if attempt == adopterAttempts {
			t.Fatalf("precondition not reached: in %d attempts the lead moved before the declaration and another node adopted", attempt)
		}
		t.Logf("attempt %d: %s adopted z, not %s, the leader at the start; starting again on a new trio", attempt, adoptedBy, planned)
	}
}

// twoWayHandOnOnce runs the scenario on one trio. It returns B, and the node
// that adopted z instead, which it has stopped the trio for, or "" when the
// scenario ran.
func twoWayHandOnOnce(t *testing.T) (string, string) {
	t.Helper()
	t.Cleanup(func() { fixtureHooks = Hooks{} })
	nodes := startTrio(t, func(name string, d *config.Document) {
		d.Cluster.Shards = 8
		d.Cluster.Replicas = 1
		// A 4 s ttl puts the hand-on's bound at 8 s, well clear of the
		// forget this test waits for.
		d.Cluster.Leases.TTLMs = 4000
		h := Hooks{AdoptionDelay: 1500 * time.Millisecond}
		if name == "n2" {
			h.TakeoverDelay = 6 * time.Second
		}
		fixtureHooks = h
	})
	fixtureHooks = Hooks{}
	waitMap(t, nodes, 3)
	b := ""
	for name, f := range nodes {
		if f.srv.Consensus().Status().Role == "leader" {
			b = name
		}
	}
	if b == "" || b == "n2" {
		t.Fatalf("precondition not reached: the leader is %q; the victim n2 must not lead", b)
	}
	v, a := "n2", map[string]string{"n1": "n3", "n3": "n1"}[b]
	exp := uint32(600)
	for _, id := range []string{"y", "z"} {
		c, ack := nodes[v].connect("tcp", packet.V5, &packet.Connect{ClientID: id, CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
		if ack.ReasonCode != packet.Success {
			t.Fatalf("connect %s on %s: %s", id, v, ack.ReasonCode)
		}
		c.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "t/" + id, QoS: 1}}})
		if _, ok := c.read().(*packet.Suback); !ok {
			t.Fatal("no SUBACK")
		}
		c.send(&packet.Disconnect{ReasonCode: packet.NormalDisconnection})
		_ = c.c.Close()
		waitOffline(t, nodes[v], id)
	}
	for _, n := range []string{a, b} {
		if _, ack := nodes[n].connect("tcp", packet.V5, &packet.Connect{ClientID: "own-" + n, CleanStart: true}, false); ack.ReasonCode != packet.Success {
			t.Fatalf("connect on %s: %s", n, ack.ReasonCode)
		}
	}
	durable := &packet.Connect{Properties: packet.Properties{SessionExpiryInterval: &exp}}
	reconnect := func(node, id string) (*client, *packet.Connack) {
		c := *durable
		c.ClientID = id
		return nodes[node].connect("tcp", packet.V5, &c, false)
	}

	cy, ack := reconnect(a, "y")
	_ = cy.c.Close()
	if ack.ReasonCode == packet.Success {
		t.Fatalf("precondition not reached: y was served on %s while its owner had not answered", a)
	}
	nodes[v].srv.Crash()
	nodes[v].stopped = true
	cy, _ = reconnect(b, "y")
	_ = cy.c.Close()
	waitUntil(t, a+" handing y on to "+b, 15*time.Second, func() bool {
		return nodes[a].srv.Leases().TransitStats().Handed >= 1
	})
	if by := reassignedOwner(t, nodes, a, "z", v); by != b {
		for _, f := range nodes {
			f.stop()
		}
		return b, by
	}
	cz, ack := reconnect(a, "z")
	if ack.ReasonCode != packet.Success {
		t.Fatalf("z on %s: %s", a, ack.ReasonCode)
	}
	t.Cleanup(func() { _ = cz.c.Close() })
	waitUntil(t, b+" handing z on to "+a, 10*time.Second, func() bool {
		return nodes[b].srv.Leases().TransitStats().Handed >= 1
	})
	formed := time.Now()
	cy, ack = reconnect(b, "y")
	if ack.ReasonCode != packet.Success {
		t.Fatalf("y on %s after the declaration: %s", b, ack.ReasonCode)
	}
	t.Cleanup(func() { _ = cy.c.Close() })

	forgot := false
	for time.Now().Before(formed.Add(3 * time.Second)) {
		if len(nodes[a].srv.Routes().Table().HiddenOwners()) == 0 && len(nodes[b].srv.Routes().Table().HiddenOwners()) == 0 {
			forgot = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	at, bt := nodes[a].srv.Leases().TransitStats(), nodes[b].srv.Leases().TransitStats()
	t.Logf("forgot %v %s after both hand-ons; %s transit %+v; %s transit %+v", forgot, time.Since(formed).Round(time.Millisecond), a, at, b, bt)
	if !forgot {
		t.Fatalf("the lost node was not forgotten on both survivors within 3 s of both hand-ons (the bound is 8 s)")
	}
	if at.HandedExpired != 0 || bt.HandedExpired != 0 {
		t.Fatalf("a hand-on ended on its bound: %s %+v, %s %+v", a, at, b, bt)
	}
	if at.HandedServed < 1 || bt.HandedServed < 1 || at.HandedCovered+bt.HandedCovered < 1 {
		t.Fatalf("each hand-on should end on the other's claim, one at least on a cover: %s %+v, %s %+v", a, at, b, bt)
	}

	// Each survivor's routing plane reads the other's cover as its lease
	// table holds it, and at least one cover is in the log.
	covered := false
	for _, pair := range [][2]string{{a, b}, {b, a}} {
		g, m := nodes[pair[0]].srv.cover(pair[1])
		wg, wm := nodes[pair[0]].srv.Leases().Cover(pair[1])
		if g != wg || m != wm {
			t.Fatalf("%s's routing plane reads %s's cover as %d at mark %d; its lease table holds %d at %d", pair[0], pair[1], g, m, wg, wm)
		}
		covered = covered || g > 0
	}
	if !covered {
		t.Fatal("neither survivor's cover is in the log")
	}

	// With one copy, y's and z's sessions died with the victim: each client
	// was served fresh, and subscribes again as a client then does.
	for _, sub := range []struct {
		c     *client
		topic string
	}{{cy, "t/y"}, {cz, "t/z"}} {
		sub.c.send(&packet.Subscribe{PacketID: 2, Subscriptions: []packet.Subscription{{Filter: sub.topic, QoS: 1}}})
		if s, ok := sub.c.read().(*packet.Suback); !ok || len(s.ReasonCodes) != 1 || s.ReasonCodes[0].IsError() {
			t.Fatalf("subscribe to %s after the forget: %+v", sub.topic, s)
		}
	}
	for _, p := range []struct{ on, topic string }{{a, "t/y"}, {b, "t/z"}} {
		pub, _ := nodes[p.on].connect("tcp", packet.V5, &packet.Connect{ClientID: "pub-" + p.on, CleanStart: true}, false)
		publishUntilAccepted(t, pub, p.topic, "after")
	}
	for _, c := range []*client{cy, cz} {
		got := false
		_ = c.c.SetReadDeadline(time.Now().Add(5 * time.Second))
		for !got {
			p, err := c.dec.Read()
			if err != nil {
				break
			}
			if m, ok := p.(*packet.Publish); ok && string(m.Payload) == "after" {
				got = true
				if m.QoS == 1 {
					c.send(&packet.Puback{Ack: packet.Ack{PacketID: m.PacketID}})
				}
			}
		}
		if !got {
			t.Fatal("a publish made after the forget did not reach its subscriber")
		}
	}
	return b, ""
}
