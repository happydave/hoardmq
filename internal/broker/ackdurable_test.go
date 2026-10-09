package broker

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store/faultdisk"
)

// TestFailedSyncRefusesTheSessionAcknowledgements: on the wire, with the
// node's journal failing its next fsync, a CONNECT is refused with 0x83 and
// its session recorded offline on its lease; a SUBSCRIBE is refused with 0x83
// and routes nothing; a 3.1.1 UNSUBSCRIBE closes the connection without an
// UNSUBACK and leaves the subscription in place.
func TestFailedSyncRefusesTheSessionAcknowledgements(t *testing.T) {
	d := faultdisk.New()
	d.Match = onPath("/data/logs/state/")
	withDisk(t, d)
	f := start(t, nil)
	exp := uint32(600)
	pub, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	publish := func(topic string, id uint16) packet.ReasonCode {
		t.Helper()
		pub.send(&packet.Publish{Topic: topic, QoS: 1, PacketID: id, Payload: []byte("x")})
		a, ok := pub.read().(*packet.Puback)
		if !ok || a.PacketID != id {
			t.Fatalf("PUBACK %d: %+v", id, a)
		}
		return a.ReasonCode
	}

	t.Run("connect", func(t *testing.T) {
		d.FailNext(1)
		connect := &packet.Connect{ClientID: "c1", CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}
		if _, ack := f.connect("tcp", packet.V5, connect, false); ack.ReasonCode != packet.ImplementationSpecificError {
			t.Fatalf("CONNACK %v, want 0x83: the CONNACK ignored its failed sync", ack.ReasonCode)
		}
		deadline := time.Now().Add(5 * time.Second)
		for {
			l, ok := f.srv.Leases().Table().Lookup("default", "c1")
			if ok && !l.Online && !l.DisconnectedAt.IsZero() {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("the refused session's lease reads %+v (found %v), want it recorded offline", l, ok)
			}
			time.Sleep(10 * time.Millisecond)
		}
		again := &packet.Connect{ClientID: "c1", CleanStart: false, Properties: packet.Properties{SessionExpiryInterval: &exp}}
		if _, ack := f.connect("tcp", packet.V5, again, false); ack.ReasonCode != packet.Success || !ack.SessionPresent {
			t.Fatalf("the reconnect answered %+v, want Success with Session Present", ack)
		}
	})

	t.Run("subscribe", func(t *testing.T) {
		sub, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "s1", CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
		d.FailNext(1)
		sub.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "b/1", QoS: 1}}})
		if a, ok := sub.read().(*packet.Suback); !ok || len(a.ReasonCodes) != 1 || a.ReasonCodes[0] != packet.ImplementationSpecificError {
			t.Fatalf("SUBACK %+v, want [0x83]", a)
		}
		if code := publish("b/1", 1); code != packet.NoMatchingSubscribers {
			t.Fatalf("a publish to the refused filter answered %v, want 0x10: the refused subscription is routed", code)
		}
	})

	t.Run("unsubscribe at 3.1.1", func(t *testing.T) {
		connect := &packet.Connect{ClientID: "c3", CleanStart: false}
		c3, _ := f.connect("tcp", packet.V4, connect, false)
		c3.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "c/1", QoS: 1}}})
		if a, ok := c3.read().(*packet.Suback); !ok || a.ReasonCodes[0] != 1 {
			t.Fatalf("precondition: SUBACK %+v", a)
		}
		d.FailNext(1)
		c3.send(&packet.Unsubscribe{PacketID: 2, Filters: []string{"c/1"}})
		_ = c3.c.SetReadDeadline(time.Now().Add(2 * time.Second))
		if p, err := c3.dec.Read(); err == nil {
			t.Fatalf("the refused UNSUBSCRIBE at 3.1.1 was answered %T %+v; want the connection closed with nothing written", p, p)
		} else if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("reading after the refused UNSUBSCRIBE: %v; want the connection closed", err)
		}
		waitOffline(t, f, "c3")
		again, ack := f.connect("tcp", packet.V4, connect, false)
		if !ack.SessionPresent {
			t.Fatal("precondition: the reconnect found no session")
		}
		if code := publish("c/1", 2); code.IsError() {
			t.Fatalf("the publish after the refusal answered %v", code)
		}
		if p, ok := again.read().(*packet.Publish); !ok || p.Topic != "c/1" {
			t.Fatalf("the subscriber read %+v, want the publish on c/1: the refused UNSUBSCRIBE removed the subscription", p)
		}
	})
}
