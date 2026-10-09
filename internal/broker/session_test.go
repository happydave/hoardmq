package broker

import (
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
)

func (c *client) readPublish() *packet.Publish {
	c.t.Helper()
	for {
		p := c.read()
		if pub, ok := p.(*packet.Publish); ok {
			return pub
		}
	}
}

func (c *client) expectNoPacket() {
	c.t.Helper()
	_ = c.c.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	if p, err := c.dec.Read(); err == nil {
		c.t.Fatalf("unexpected %T %+v", p, p)
	}
}

func TestDeliveryBetweenClients(t *testing.T) {
	f := start(t, nil)
	for _, v := range []packet.Version{packet.V4, packet.V5} {
		t.Run(v.String(), func(t *testing.T) {
			sub, _ := f.connect("tcp", v, &packet.Connect{ClientID: "sub-" + v.String(), CleanStart: true}, false)
			pub, _ := f.connect("tcp", v, &packet.Connect{ClientID: "pub-" + v.String(), CleanStart: true}, false)
			sub.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "room/1", QoS: 2}}})
			sub.read()
			pub.send(&packet.Publish{Topic: "room/1", Payload: []byte("q0")})
			pub.send(&packet.Publish{Topic: "room/1", QoS: 1, PacketID: 1, Payload: []byte("q1")})
			pub.read()
			pub.send(&packet.Publish{Topic: "room/1", QoS: 2, PacketID: 2, Payload: []byte("q2")})
			pub.read()
			pub.send(&packet.Pubrel{Ack: packet.Ack{PacketID: 2}})
			pub.read()
			for _, want := range []struct {
				payload string
				qos     byte
			}{{"q0", 0}, {"q1", 1}, {"q2", 2}} {
				p := sub.readPublish()
				if string(p.Payload) != want.payload || p.QoS != want.qos {
					t.Fatalf("got %q qos %d, want %q qos %d", p.Payload, p.QoS, want.payload, want.qos)
				}
				switch p.QoS {
				case 1:
					sub.send(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID}})
				case 2:
					sub.send(&packet.Pubrec{Ack: packet.Ack{PacketID: p.PacketID}})
					if _, ok := sub.read().(*packet.Pubrel); !ok {
						t.Fatal("no PUBREL")
					}
					sub.send(&packet.Pubcomp{Ack: packet.Ack{PacketID: p.PacketID}})
				}
			}
			// Exactly once: the publisher resends QoS 2 with DUP before PUBREL.
			pub.send(&packet.Publish{Topic: "room/1", QoS: 2, PacketID: 3, Payload: []byte("dup")})
			pub.read()
			pub.send(&packet.Publish{Topic: "room/1", QoS: 2, PacketID: 3, Dup: true, Payload: []byte("dup")})
			pub.read()
			pub.send(&packet.Pubrel{Ack: packet.Ack{PacketID: 3}})
			pub.read()
			p := sub.readPublish()
			sub.send(&packet.Pubrec{Ack: packet.Ack{PacketID: p.PacketID}})
			sub.read()
			sub.send(&packet.Pubcomp{Ack: packet.Ack{PacketID: p.PacketID}})
			sub.expectNoPacket()
		})
	}
}

func TestResumeReceivesQueuedInOrder(t *testing.T) {
	f := start(t, nil)
	exp := uint32(600)
	sub, ack := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "durable", CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	if ack.SessionPresent {
		t.Fatal("present on first connect")
	}
	sub.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "q", QoS: 1}}})
	sub.read()
	pub, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "p", CleanStart: true}, false)
	// One in flight (never acknowledged), then the subscriber drops.
	pub.send(&packet.Publish{Topic: "q", QoS: 1, PacketID: 1, Payload: []byte("inflight")})
	pub.read()
	sub.readPublish()
	_ = sub.c.Close()
	// Wait for the broker to notice, then queue more.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if rec, ok := f.srv.Sessions().Record("durable"); ok && !rec.Online {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	for i := 2; i <= 4; i++ {
		pub.send(&packet.Publish{Topic: "q", QoS: 1, PacketID: uint16(i), Payload: []byte{byte('0' + i)}})
		pub.read()
	}
	sub2, ack := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "durable", CleanStart: false, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	if !ack.SessionPresent {
		t.Fatal("session not present on resume")
	}
	var got []string
	for i := 0; i < 4; i++ {
		p := sub2.readPublish()
		got = append(got, string(p.Payload))
		if i == 0 && !p.Dup {
			t.Error("in-flight replay without DUP")
		}
		sub2.send(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID}})
	}
	if want := "inflight,2,3,4"; got[0]+","+got[1]+","+got[2]+","+got[3] != want {
		t.Fatalf("order %v", got)
	}
}

func TestTakeoverAndWill(t *testing.T) {
	f := start(t, nil)
	watcher, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "watcher", CleanStart: true}, false)
	watcher.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "last/words", QoS: 1}}})
	watcher.read()
	will := &packet.Will{Topic: "last/words", Payload: []byte("gone"), QoS: 1}
	first, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "dev", CleanStart: true, Will: will}, false)
	second, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "dev", CleanStart: false, Will: will}, false)
	if d, ok := first.read().(*packet.Disconnect); !ok || d.ReasonCode != packet.SessionTakenOver {
		t.Fatalf("first connection: %+v", d)
	}
	// The takeover published no Will.
	watcher.expectNoPacket()
	// Dropping the second connection without DISCONNECT publishes it.
	_ = second.c.Close()
	p := watcher.readPublish()
	if p.Topic != "last/words" || string(p.Payload) != "gone" || p.QoS != 1 {
		t.Fatalf("will: %+v", p)
	}
}
