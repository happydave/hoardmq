package broker

import (
	"fmt"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/packet"
)

// limits.maxInflightMessages on the wire, and in the overload indicator: a
// 3.1.1 subscriber that acknowledges nothing receives the cap's 2 of 5
// messages and the next once it acknowledges one, and the indicator counts
// the messages in flight with those queued.
func TestInflightCapOnTheWire(t *testing.T) {
	f := start(t, func(d *config.Document) { d.Limits.MaxInflightMessages = 2 })
	cl, ack := f.connect("tcp", packet.V4, &packet.Connect{ClientID: "slow", CleanStart: true, KeepAlive: 30}, false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("connack %v", ack.ReasonCode)
	}
	cl.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "t", QoS: 1}}})
	if _, ok := cl.read().(*packet.Suback); !ok {
		t.Fatal("no SUBACK")
	}
	for i := 0; i < 5; i++ {
		f.srv.sessions.Inject(&message.Message{Topic: "t", QoS: 1, Payload: []byte(fmt.Sprint(i))})
	}
	var first []*packet.Publish
	for len(first) < 2 {
		if p, ok := cl.read().(*packet.Publish); ok {
			first = append(first, p)
		}
	}
	cl.expectNoPacket()
	if got := f.srv.overload.indicators().Queued; got != 5 {
		t.Fatalf("the overload indicator reads %d held, want 5 (2 in flight, 3 queued)", got)
	}
	if got := f.srv.sessions.Queued(); got != 3 {
		t.Fatalf("queued %d, want 3", got)
	}
	cl.send(&packet.Puback{Ack: packet.Ack{PacketID: first[0].PacketID}})
	_ = cl.c.SetReadDeadline(time.Now().Add(2 * time.Second))
	p, ok := cl.read().(*packet.Publish)
	if !ok || string(p.Payload) != "2" {
		t.Fatalf("after one PUBACK: %+v, want the third message", p)
	}
	cl.expectNoPacket()
}
