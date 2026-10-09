package broker

import (
	"fmt"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
)

// TestPublishCrossesBrokers: a subscriber on n2 receives a publish made on
// n1 with its payload, QoS and properties intact, and 100 QoS 1 publishes
// from one client arrive in order.
func TestPublishCrossesBrokers(t *testing.T) {
	nodes := startTrio(t, nil)
	sub, _ := nodes["n2"].connect("tcp", packet.V5, &packet.Connect{ClientID: "sub", CleanStart: true}, false)
	sub.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "x/#", QoS: 1}}})
	sub.read()
	// The route reaches n1 before publishing.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if n, _ := nodes["n1"].srv.Routes().Table().Match("default", "x/1"); len(n) == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	pub, _ := nodes["n1"].connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	ct := "text/plain"
	pub.send(&packet.Publish{Topic: "x/1", QoS: 1, PacketID: 1, Payload: []byte("across"), Properties: packet.Properties{ContentType: &ct, UserProperties: []packet.UserProperty{{Name: "k", Value: "v"}}}})
	pub.read()
	m, ok := sub.read().(*packet.Publish)
	if !ok || string(m.Payload) != "across" || m.QoS != 1 || m.Properties.ContentType == nil || *m.Properties.ContentType != ct || len(m.Properties.UserProperties) != 1 {
		t.Fatalf("cross-broker delivery %+v", m)
	}
	sub.send(&packet.Puback{Ack: packet.Ack{PacketID: m.PacketID}})
	for i := 2; i <= 101; i++ {
		pub.send(&packet.Publish{Topic: "x/seq", QoS: 1, PacketID: uint16(i), Payload: []byte(fmt.Sprint(i))})
		pub.read()
	}
	for i := 2; i <= 101; i++ {
		m, ok := sub.read().(*packet.Publish)
		if !ok || string(m.Payload) != fmt.Sprint(i) {
			t.Fatalf("out of order at %d: %+v", i, m)
		}
		sub.send(&packet.Puback{Ack: packet.Ack{PacketID: m.PacketID}})
	}
}

// TestSharedGroupAcrossBrokers: one member on each of two brokers; every
// publish made on a third broker is delivered exactly once between them.
func TestSharedGroupAcrossBrokers(t *testing.T) {
	nodes := startTrio(t, nil)
	m1, _ := nodes["n1"].connect("tcp", packet.V5, &packet.Connect{ClientID: "m1", CleanStart: true}, false)
	m1.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "$share/g/s/#", QoS: 0}}})
	m1.read()
	m2, _ := nodes["n2"].connect("tcp", packet.V5, &packet.Connect{ClientID: "m2", CleanStart: true}, false)
	m2.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "$share/g/s/#", QoS: 0}}})
	m2.read()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, groups := nodes["n3"].srv.Routes().Table().Match("default", "s/1"); len(groups) == 1 && len(groups[0].Members) == 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	pub, _ := nodes["n3"].connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	const total = 20
	for i := 0; i < total; i++ {
		pub.send(&packet.Publish{Topic: "s/1", Payload: []byte(fmt.Sprint(i))})
	}
	got := map[string]int{}
	count := func(c *client) {
		for {
			_ = c.c.SetReadDeadline(time.Now().Add(700 * time.Millisecond))
			p, err := c.dec.Read()
			if err != nil {
				return
			}
			if m, ok := p.(*packet.Publish); ok {
				got[string(m.Payload)]++
			}
		}
	}
	count(m1)
	count(m2)
	if len(got) != total {
		t.Fatalf("%d of %d distinct payloads delivered: %v", len(got), total, got)
	}
	for k, n := range got {
		if n != 1 {
			t.Fatalf("payload %s delivered %d times", k, n)
		}
	}
}
