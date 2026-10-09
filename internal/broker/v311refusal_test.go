package broker

import (
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store/faultdisk"
)

// TestRefusedPublishAt311IsRetransmittedNotAcknowledged: a 3.1.1 client reads
// any PUBACK or PUBREC as acceptance, so a publish the broker refuses (here an
// offline subscriber's queued record fails to append, and its copy is
// withdrawn) closes the connection instead, and the subscriber receives the
// client's retry once. At QoS 1 the client keeps its session (Clean Session
// 0) and retransmits with DUP, as MQTT 3.1.1 §4.4 requires. At QoS 2 it uses
// Clean Session 1 and publishes again: a kept session's own QoS 2 opening is
// the first record to append, and would take the fault in the subscriber's
// place.
func TestRefusedPublishAt311IsRetransmittedNotAcknowledged(t *testing.T) {
	for _, tc := range []struct {
		qos       byte
		keepState bool
	}{{1, true}, {2, false}} {
		t.Run(fmt.Sprintf("qos%d", tc.qos), func(t *testing.T) {
			d := faultdisk.New()
			d.Match = onPath("/data/logs/state/")
			withDisk(t, d)
			f := start(t, func(doc *config.Document) { doc.Node.WALSegmentBytes = 1 << 20 })
			exp := uint32(600)
			durable := &packet.Connect{ClientID: "durable", CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}
			sub, _ := f.connect("tcp", packet.V5, durable, false)
			sub.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "j/#", QoS: 1}}})
			sub.read()
			_ = sub.c.Close()
			waitOffline(t, f, "durable")

			pubConnect := &packet.Connect{ClientID: "pub", CleanStart: !tc.keepState}
			pub, _ := f.connect("tcp", packet.V4, pubConnect, false)
			fill := func(id uint16, payload []byte) packet.ReasonCode {
				t.Helper()
				pub.send(&packet.Publish{Topic: "j/1", QoS: 1, PacketID: id, Payload: payload})
				if a, ok := pub.read().(*packet.Puback); !ok || a.PacketID != id {
					t.Fatalf("precondition: filler PUBACK %d: %+v", id, a)
				}
				return packet.Success
			}
			fillToRoll(t, fill, 1)
			d.FailNext(1)
			refused := &packet.Publish{Topic: "j/1", QoS: tc.qos, PacketID: 3, Payload: []byte("rolls")}
			pub.send(refused)
			_ = pub.c.SetReadDeadline(time.Now().Add(2 * time.Second))
			if p, err := pub.dec.Read(); err == nil {
				t.Fatalf("the refused publish at 3.1.1 was answered %T %+v; want the connection closed with nothing written", p, p)
			} else if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("reading after the refused publish: %v; want the connection closed", err)
			}
			if rec, _ := f.srv.Sessions().Record("durable"); rec.Queued != 2 {
				t.Fatalf("precondition: the subscriber holds %d queued, want the two fillers: the refused copy was not withdrawn", rec.Queued)
			}

			again, ack := f.connect("tcp", packet.V4, pubConnect, false)
			if ack.SessionPresent != tc.keepState {
				t.Fatalf("precondition: Session Present %v on reconnect, want %v", ack.SessionPresent, tc.keepState)
			}
			retry := *refused
			retry.Dup = tc.keepState
			again.send(&retry)
			switch tc.qos {
			case 1:
				if a, ok := again.read().(*packet.Puback); !ok || a.PacketID != 3 {
					t.Fatalf("the retransmission was answered %+v, want its PUBACK", a)
				}
			case 2:
				if r, ok := again.read().(*packet.Pubrec); !ok || r.PacketID != 3 {
					t.Fatalf("the retry was answered %+v, want its PUBREC", r)
				}
				again.send(&packet.Pubrel{Ack: packet.Ack{PacketID: 3}})
				if c, ok := again.read().(*packet.Pubcomp); !ok || c.PacketID != 3 {
					t.Fatalf("the PUBREL was answered %+v, want its PUBCOMP", c)
				}
			}

			durable.CleanStart = false
			sub2, ack := f.connect("tcp", packet.V5, durable, false)
			if !ack.SessionPresent {
				t.Fatal("precondition: the durable session was not present on reconnect")
			}
			rec, _ := f.srv.Sessions().Record("durable")
			copies := 0
			for i := 0; i < rec.Queued+rec.InFlight; i++ {
				p, ok := sub2.read().(*packet.Publish)
				if !ok {
					t.Fatalf("delivery %d was not a PUBLISH", i)
				}
				if string(p.Payload) == "rolls" {
					copies++
				}
				sub2.send(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID}})
			}
			if copies != 1 {
				t.Fatalf("the subscriber received %d copies of the message, want 1", copies)
			}
		})
	}
}
