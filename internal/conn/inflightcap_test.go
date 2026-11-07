package conn

import (
	"testing"

	"github.com/happydave/hoardmq/internal/packet"
)

// TestSendQuotaIsTheLowerOfReceiveMaximumAndCap: the cap lowers a client's
// Receive Maximum and never raises it.
func TestSendQuotaIsTheLowerOfReceiveMaximumAndCap(t *testing.T) {
	for _, c := range []struct {
		recvMax, limit uint16
		want           int
	}{
		{65535, 0, 65535},     // no cap
		{65535, 65535, 65535}, // the default cap, a client without a Receive Maximum
		{65535, 3, 3},         // a 3.1.1 client under a cap
		{2, 3, 2},             // a Receive Maximum below the cap keeps its own
		{10, 3, 3},
	} {
		if got := sendQuota(c.recvMax, c.limit); got != c.want {
			t.Errorf("sendQuota(%d, %d) = %d, want %d", c.recvMax, c.limit, got, c.want)
		}
	}
}

// TestInflightCapBoundsA311Client: a 3.1.1 client, which sends no Receive
// Maximum, gets the broker's cap: a fourth QoS 1 delivery under a cap of 3 is
// refused, a PUBACK frees one place, and acknowledgements beyond what is in
// flight do not take the quota past the cap.
func TestInflightCapBoundsA311Client(t *testing.T) {
	o := defaultOptions()
	o.MaxInflight = 3
	h := newHarness(t, o, &fakeCore{})
	h.connect(packet.V4, &packet.Connect{ClientID: "c", CleanStart: true})
	if q := h.conn.SendQuota(); q != 3 {
		t.Fatalf("quota of a 3.1.1 client under a cap of 3: %d", q)
	}
	deliver := func(id uint16) error {
		return h.conn.Deliver(&packet.Publish{Topic: "t", QoS: 1, PacketID: id, Payload: []byte("x")})
	}
	for id := uint16(1); id <= 3; id++ {
		if err := deliver(id); err != nil {
			t.Fatalf("delivery %d: %v", id, err)
		}
	}
	if err := deliver(4); err != ErrQuota {
		t.Fatalf("a fourth QoS 1 delivery under a cap of 3: %v, want ErrQuota", err)
	}
	for i := 0; i < 3; i++ {
		h.read()
	}
	h.send(&packet.Puback{Ack: packet.Ack{PacketID: 1}})
	h.send(&packet.Pingreq{})
	h.read()
	if q := h.conn.SendQuota(); q != 1 {
		t.Fatalf("quota after one PUBACK: %d, want 1", q)
	}
	for _, id := range []uint16{2, 3, 3, 1} {
		h.send(&packet.Puback{Ack: packet.Ack{PacketID: id}})
	}
	h.send(&packet.Pingreq{})
	h.read()
	if q := h.conn.SendQuota(); q != 3 {
		t.Fatalf("quota after more PUBACKs than were in flight: %d, want the cap, 3", q)
	}
}
