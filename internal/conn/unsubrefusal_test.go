package conn

import (
	"testing"

	"github.com/happydave/hoardmq/internal/packet"
)

// TestRefusedUnsubscribeClosesBelowLevel5: a 3.1.1 or 3.1 UNSUBACK carries no
// reason code and a client reads it as success, so an UNSUBSCRIBE the core
// refuses closes the connection with nothing written; at level 5 the UNSUBACK
// carries the refusal.
func TestRefusedUnsubscribeClosesBelowLevel5(t *testing.T) {
	for _, v := range []packet.Version{packet.V3, packet.V4, packet.V5} {
		t.Run(v.String(), func(t *testing.T) {
			h := newHarness(t, defaultOptions(), &fakeCore{unsubReason: packet.ImplementationSpecificError})
			h.connect(v, nil)
			h.send(&packet.Unsubscribe{PacketID: 3, Filters: []string{"t"}})
			if v != packet.V5 {
				if d := h.readEnd(); d != nil {
					t.Fatalf("a refused UNSUBSCRIBE at %s was answered %+v; want the connection closed with nothing written", v, d)
				}
				return
			}
			u, ok := h.read().(*packet.Unsuback)
			if !ok {
				t.Fatal("a refused UNSUBSCRIBE at level 5 was not answered with its UNSUBACK")
			}
			if len(u.ReasonCodes) != 1 || u.ReasonCodes[0] != packet.ImplementationSpecificError {
				t.Fatalf("UNSUBACK reasons %v, want [0x83]", u.ReasonCodes)
			}
		})
	}
}

// TestAcceptedUnsubscribeIsAcknowledgedAtEveryLevel: only a refusal closes.
func TestAcceptedUnsubscribeIsAcknowledgedAtEveryLevel(t *testing.T) {
	for _, v := range []packet.Version{packet.V3, packet.V4, packet.V5} {
		t.Run(v.String(), func(t *testing.T) {
			h := newHarness(t, defaultOptions(), &fakeCore{})
			h.connect(v, nil)
			h.send(&packet.Unsubscribe{PacketID: 3, Filters: []string{"t"}})
			if u, ok := h.read().(*packet.Unsuback); !ok || u.PacketID != 3 {
				t.Fatalf("UNSUBSCRIBE at %s answered %+v, want its UNSUBACK", v, u)
			}
		})
	}
}
