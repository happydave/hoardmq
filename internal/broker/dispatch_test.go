package broker

import (
	"testing"

	"github.com/happydave/hoardmq/internal/packet"
)

func TestWildcardRetainedAndShared(t *testing.T) {
	f := start(t, nil)
	pub, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	// A retained message published before anyone subscribes.
	pub.send(&packet.Publish{Topic: "home/kitchen/temp", Retain: true, QoS: 1, PacketID: 1, Payload: []byte("21")})
	pub.read()

	sub, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "sub", CleanStart: true}, false)
	sub.send(&packet.Subscribe{PacketID: 1, Properties: packet.Properties{SubscriptionIdentifiers: []uint32{4}}, Subscriptions: []packet.Subscription{{Filter: "home/+/temp", QoS: 1}}})
	sub.read()
	r := sub.readPublish()
	if !r.Retain || string(r.Payload) != "21" || r.Topic != "home/kitchen/temp" || r.Properties.SubscriptionIdentifiers[0] != 4 {
		t.Fatalf("retained on subscribe: %+v", r)
	}
	sub.send(&packet.Puback{Ack: packet.Ack{PacketID: r.PacketID}})
	// A live publish matches the wildcard and is not retained on delivery.
	pub.send(&packet.Publish{Topic: "home/bedroom/temp", Payload: []byte("19")})
	live := sub.readPublish()
	if live.Retain || string(live.Payload) != "19" {
		t.Fatalf("live: %+v", live)
	}
	sub.expectNoPacket()

	// Two members of a shared group receive alternately.
	a, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "a", CleanStart: true}, false)
	b, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "b", CleanStart: true}, false)
	for _, m := range []*client{a, b} {
		m.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "$share/workers/jobs/#", QoS: 0}}})
		m.read()
	}
	for i := 0; i < 4; i++ {
		pub.send(&packet.Publish{Topic: "jobs/1", Payload: []byte{byte('0' + i)}})
	}
	got := map[string]int{}
	for i := 0; i < 2; i++ {
		got[string(a.readPublish().Payload)]++
		got[string(b.readPublish().Payload)]++
	}
	if len(got) != 4 {
		t.Fatalf("group deliveries %v", got)
	}
	a.expectNoPacket()
	b.expectNoPacket()
}
