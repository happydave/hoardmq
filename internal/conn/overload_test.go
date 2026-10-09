package conn

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/hook"
	"github.com/happydave/hoardmq/internal/packet"
)

// pingHooks disconnects on a PINGREQ read (in) or a PINGRESP written (out).
type pingHooks struct{ in, out bool }

func (pingHooks) Connected(context.Context, hook.Info)                             {}
func (pingHooks) Disconnected(context.Context, hook.Info, packet.ReasonCode, bool) {}
func (pingHooks) Subscribe(context.Context, hook.Info, *packet.Subscription) hook.Outcome {
	return hook.OK
}
func (h pingHooks) Inbound(_ context.Context, _ hook.Info, p packet.Packet) hook.Outcome {
	if _, ok := p.(*packet.Pingreq); ok && h.in {
		return hook.Outcome{Action: hook.Disconnect, Reason: packet.AdministrativeAction}
	}
	return hook.OK
}
func (h pingHooks) Outbound(_ context.Context, _ hook.Info, p packet.Packet) hook.Outcome {
	if _, ok := p.(*packet.Pingresp); ok && h.out {
		return hook.Outcome{Action: hook.Disconnect, Reason: packet.AdministrativeAction}
	}
	return hook.OK
}

// limitsOf is a ClientLimits option answering whatever the pointer holds.
func limitsOf(p *atomic.Pointer[ClientLimits]) func(string) *ClientLimits {
	return func(string) *ClientLimits { return p.Load() }
}

// readWithin reads the next packet, waiting up to d.
func (h *harness) readWithin(d time.Duration) packet.Packet {
	h.t.Helper()
	_ = h.client.SetReadDeadline(time.Now().Add(d))
	p, err := h.dec.Read()
	if err != nil {
		h.t.Fatalf("read: %v", err)
	}
	return p
}

// sendAll sends the packets on a goroutine of its own: over net.Pipe a send
// waits for the server to read it, and the server is the side being paced.
func (h *harness) sendAll(ps []packet.Packet) {
	go func() {
		for _, p := range ps {
			_ = h.client.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := h.enc.Encode(h.client, p); err != nil {
				return
			}
		}
	}()
}

func publishes(n, size int) []packet.Packet {
	var out []packet.Packet
	for i := 0; i < n; i++ {
		out = append(out, &packet.Publish{Topic: "t", QoS: 1, PacketID: uint16(i + 1), Payload: make([]byte, size)})
	}
	return out
}

// pubacksWithin reads n PUBACKs and returns when the last arrived.
func (h *harness) pubacksWithin(n int, d time.Duration) time.Time {
	h.t.Helper()
	for i := 0; i < n; i++ {
		p := h.readWithin(d)
		if a, ok := p.(*packet.Puback); !ok || a.ReasonCode != packet.Success {
			h.t.Fatalf("PUBACK %d: %+v", i+1, p)
		}
	}
	return time.Now()
}

func throttleHarness(t *testing.T, lim *ClientLimits) (*harness, *counters, *atomic.Pointer[ClientLimits]) {
	var p atomic.Pointer[ClientLimits]
	p.Store(lim)
	o := defaultOptions()
	o.ReceiveMaximum = 100
	m := &counters{}
	o.Metrics, o.ClientLimits = m, limitsOf(&p)
	h := newHarness(t, o, &fakeCore{})
	if ack := h.connect(packet.V5, nil); ack.ReasonCode != packet.Success {
		t.Fatal(ack)
	}
	return h, m, &p
}

// Fifteen PUBLISHes at a message rate of 5 with a burst of 5: every one is
// acknowledged Success, the last no sooner than the rate allows, and the
// connection stays open.
func TestPublishRateThrottles(t *testing.T) {
	h, m, _ := throttleHarness(t, &ClientLimits{MessageRate: 5, MessageBurst: 5})
	began := time.Now()
	h.sendAll(publishes(15, 1))
	last := h.pubacksWithin(15, 5*time.Second)
	if took := last.Sub(began); took < 1800*time.Millisecond {
		t.Fatalf("15 PUBLISHes at 5 a second with a burst of 5 were acknowledged in %v, want at least 1.8 s", took)
	}
	h.send(&packet.Pingreq{})
	if p := h.readWithin(5 * time.Second); p.Type() != packet.PINGRESP {
		t.Fatalf("after the throttle: %+v, want PINGRESP", p)
	}
	if got := m.get("hoardmq_client_throttled_total{reason=message}"); got < 1 {
		t.Fatalf("throttled %v times by message, want at least 1: %v", got, m.m)
	}
}

// Four PUBLISHes of 900 payload bytes at a byte rate of 1,000 with a burst of
// 1,000: the fourth is read only once the rate has covered the first three.
func TestByteRateThrottles(t *testing.T) {
	h, m, _ := throttleHarness(t, &ClientLimits{ByteRate: 1000, ByteBurst: 1000})
	began := time.Now()
	h.sendAll(publishes(4, 900))
	last := h.pubacksWithin(4, 5*time.Second)
	if took := last.Sub(began); took < 1600*time.Millisecond {
		t.Fatalf("4 × 901 bytes at 1,000 a second were acknowledged in %v, want at least 1.6 s", took)
	}
	if got := m.get("hoardmq_client_throttled_total{reason=byte}"); got < 1 {
		t.Fatalf("throttled %v times by bytes: %v", got, m.m)
	}
}

// A limit changed between two PUBLISHes applies to the second.
func TestClientLimitsChangeLive(t *testing.T) {
	h, _, p := throttleHarness(t, &ClientLimits{MessageRate: 1000, MessageBurst: 1000})
	h.sendAll(publishes(3, 1))
	h.pubacksWithin(3, 2*time.Second)
	p.Store(&ClientLimits{MessageRate: 1, MessageBurst: 1})
	began := time.Now()
	h.sendAll(publishes(3, 1))
	last := h.pubacksWithin(3, 5*time.Second)
	if took := last.Sub(began); took < 900*time.Millisecond {
		t.Fatalf("three PUBLISHes after the rate fell to 1 a second took %v, want at least 0.9 s", took)
	}
}

// A connection paused past its keep-alive is not closed for it: the
// keep-alive runs from the end of the pause.
func TestPauseHoldsKeepAlive(t *testing.T) {
	o := defaultOptions()
	h := newHarness(t, o, &fakeCore{})
	if ack := h.connect(packet.V5, &packet.Connect{ClientID: "k", CleanStart: true, KeepAlive: 1}); ack.ReasonCode != packet.Success {
		t.Fatal(ack)
	}
	h.conn.Shed(2500 * time.Millisecond)
	if got := h.conn.ShedPause(); got != 2500*time.Millisecond {
		t.Fatalf("pause %v", got)
	}
	h.send(&packet.Publish{Topic: "t", QoS: 0, Payload: []byte("x")})
	time.Sleep(2700 * time.Millisecond) // the pause plus a margin, past the 1.5 s keep-alive
	h.sendAll([]packet.Packet{&packet.Pingreq{}})
	if p := h.readWithin(5 * time.Second); p.Type() != packet.PINGRESP {
		t.Fatalf("after a 2.5 s pause on a 1 s keep-alive: %+v, want PINGRESP", p)
	}
}

// Delivery never waits for the client's socket: a client that reads nothing
// fills the outbound queue, and every delivery after that is answered ErrBusy
// at once; once the client reads, OnWritable is called.
func TestDeliverDoesNotBlock(t *testing.T) {
	var writable atomic.Int32
	o := defaultOptions()
	o.OnWritable = func(*Conn) { writable.Add(1) }
	h := newHarness(t, o, &fakeCore{})
	if ack := h.connect(packet.V5, nil); ack.ReasonCode != packet.Success {
		t.Fatal(ack)
	}
	began := time.Now()
	accepted, busy := 0, 0
	for i := 0; i < 200; i++ {
		switch err := h.conn.Deliver(&packet.Publish{Topic: "t", Payload: []byte(fmt.Sprint(i))}); err {
		case nil:
			accepted++
		case ErrBusy:
			busy++
		default:
			t.Fatalf("delivery %d: %v", i, err)
		}
	}
	if took := time.Since(began); took > time.Second {
		t.Fatalf("200 deliveries to a client that reads nothing took %v", took)
	}
	if accepted < 32 || accepted > 34 || busy != 200-accepted {
		t.Fatalf("%d accepted and %d busy: want the queue's 32 (and one the writer holds) accepted, the rest busy", accepted, busy)
	}
	for i := 0; i < accepted; i++ {
		if p := h.readWithin(2 * time.Second); p.Type() != packet.PUBLISH {
			t.Fatalf("read %d: %+v", i, p)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for writable.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if writable.Load() == 0 {
		t.Fatal("OnWritable was not called once the client read the queue")
	}
}

// How each established connection ended reaches OnEnded.
func TestEndedHow(t *testing.T) {
	ping := func(h *harness) { h.send(&packet.Pingreq{}) }
	for _, c := range []struct {
		name   string
		end    func(h *harness)
		want   Ending
		refuse packet.ReasonCode
		hooks  *pingHooks
	}{
		{name: "a client DISCONNECT", end: func(h *harness) { h.send(&packet.Disconnect{}) }, want: EndedGraceful},
		{name: "a takeover", end: func(h *harness) { h.conn.Close(packet.SessionTakenOver) }, want: EndedByServer},
		{name: "the client's socket closed", end: func(h *harness) { _ = h.client.Close() }, want: EndedAbnormal},
		{name: "a second CONNECT", end: func(h *harness) { h.send(&packet.Connect{ProtocolLevel: packet.V5, ClientID: "cid"}) }, want: EndedAbnormal},
		{name: "the keep-alive expired", end: func(h *harness) {}, want: EndedAbnormal},
		{name: "a QoS 0 publish refused", end: func(h *harness) { h.send(&packet.Publish{Topic: "t", Payload: []byte("x")}) },
			want: EndedByServer, refuse: packet.NotAuthorized},
		{name: "an inbound hook's disconnect", end: ping, want: EndedByServer, hooks: &pingHooks{in: true}},
		{name: "an outbound hook's disconnect", end: ping, want: EndedByServer, hooks: &pingHooks{out: true}},
	} {
		t.Run(c.name, func(t *testing.T) {
			var mu sync.Mutex
			var got []Ending
			o := defaultOptions()
			o.OnEnded = func(_ *Conn, how Ending) { mu.Lock(); got = append(got, how); mu.Unlock() }
			if c.hooks != nil {
				o.Hooks = *c.hooks
			}
			h := newHarness(t, o, &fakeCore{publishReason: c.refuse})
			ka := uint16(0)
			if c.want == EndedAbnormal && c.name == "the keep-alive expired" {
				ka = 1
			}
			if ack := h.connect(packet.V5, &packet.Connect{ClientID: "cid", CleanStart: true, KeepAlive: ka}); ack.ReasonCode != packet.Success {
				t.Fatal(ack)
			}
			go func() {
				// drain what the server sends so a write never holds it up
				for {
					_ = h.client.SetReadDeadline(time.Now().Add(5 * time.Second))
					if _, err := h.dec.Read(); err != nil {
						return
					}
				}
			}()
			c.end(h)
			select {
			case <-h.done:
			case <-time.After(5 * time.Second):
				t.Fatal("the connection did not end")
			}
			mu.Lock()
			defer mu.Unlock()
			if len(got) != 1 || got[0] != c.want {
				t.Fatalf("ended %v, want %v", got, c.want)
			}
		})
	}
}

// A banned CONNECT is refused before the gate and before any credential:
// 0x8A at level 5, return code 5 at level 4; counted and audited.
func TestBannedRefusal(t *testing.T) {
	for _, v := range []packet.Version{packet.V5, packet.V4} {
		t.Run(fmt.Sprint(v), func(t *testing.T) {
			g := &gate{}
			o, a, m, rec := rateOptions(g)
			o.Banned = func(id, user string, hasUser bool, remote string) (string, bool) {
				return "clientid", id == "rated"
			}
			h := newHarness(t, o, &fakeCore{})
			h.enc = &packet.Encoder{Version: v}
			h.dec.Version = v
			h.send(credentialConnect(v))
			want := packet.Banned
			if v != packet.V5 {
				want = packet.NotAuthorized
			}
			if ack := h.readConnack(); ack.ReasonCode != want {
				t.Fatalf("a banned CONNECT at %v: %s, want %s", v, ack.ReasonCode, want)
			}
			<-h.done
			if g.asked.Load() != 0 || a.calls.Load() != 0 {
				t.Fatalf("gate asked %d times, authenticator %d: want neither", g.asked.Load(), a.calls.Load())
			}
			if got := m.get("hoardmq_connects_banned_total{kind=clientid,listener=test}"); got != 1 {
				t.Fatalf("banned counted %v: %v", got, m.m)
			}
			rec.mu.Lock()
			defer rec.mu.Unlock()
			if len(rec.details) != 1 || rec.details[0]["reason"] != "ban: clientid" {
				t.Fatalf("audit %v", rec.details)
			}
		})
	}
	// A CONNECT that meets no ban goes on to the gate.
	g := &gate{}
	o, _, _, _ := rateOptions(g)
	o.Banned = func(string, string, bool, string) (string, bool) { return "", false }
	h := newHarness(t, o, &fakeCore{})
	if ack := h.connect(packet.V5, credentialConnect(packet.V5)); ack.ReasonCode != packet.Success || g.asked.Load() != 1 {
		t.Fatalf("not banned: %+v, gate asked %d", ack, g.asked.Load())
	}
}

// While the cluster sheds load a CONNECT is refused Server busy at level 5
// and Server unavailable at level 4.
func TestOverloadRefusal(t *testing.T) {
	for v, want := range map[packet.Version]packet.ReasonCode{packet.V5: packet.ServerBusy, packet.V4: packet.ServerUnavailable} {
		o, a, _, _ := rateOptions(&gate{limit: LimitOverload})
		h := newHarness(t, o, &fakeCore{})
		h.enc = &packet.Encoder{Version: v}
		h.dec.Version = v
		h.send(credentialConnect(v))
		if ack := h.readConnack(); ack.ReasonCode != want {
			t.Fatalf("%v under overload: %s, want %s", v, ack.ReasonCode, want)
		}
		if a.calls.Load() != 0 {
			t.Fatal("a CONNECT refused for overload reached an authenticator")
		}
	}
}

// A delivery refused for a full queue changes nothing about the connection:
// no send quota is spent and no topic alias assigned, so the message's later
// delivery carries its topic.
func TestBusyDeliveryChangesNothing(t *testing.T) {
	h := newHarness(t, defaultOptions(), &fakeCore{})
	max := uint16(10)
	if ack := h.connect(packet.V5, &packet.Connect{ClientID: "a", CleanStart: true, Properties: packet.Properties{TopicAliasMaximum: &max}}); ack.ReasonCode != packet.Success {
		t.Fatal(ack)
	}
	// Fill the queue, then fill it again once the write loop has taken the
	// one packet it holds while the unread pipe blocks it.
	filled := 0
	for range 3 {
		for h.conn.Deliver(&packet.Publish{Topic: "fill", Payload: []byte("x")}) == nil {
			filled++
		}
		time.Sleep(20 * time.Millisecond)
	}
	quota := h.conn.SendQuota()
	if err := h.conn.Deliver(&packet.Publish{Topic: "fresh", QoS: 1, PacketID: 9, Payload: []byte("y")}); err != ErrBusy {
		t.Fatalf("a delivery to a full queue: %v, want ErrBusy", err)
	}
	if got := h.conn.SendQuota(); got != quota {
		t.Fatalf("a busy QoS 1 delivery spent quota: %d, was %d", got, quota)
	}
	for i := 0; i < filled; i++ {
		h.readWithin(2 * time.Second)
	}
	deadline := time.Now().Add(2 * time.Second)
	for h.conn.Deliver(&packet.Publish{Topic: "fresh", QoS: 1, PacketID: 9, Payload: []byte("y")}) != nil {
		if time.Now().After(deadline) {
			t.Fatal("the queue did not drain")
		}
		time.Sleep(5 * time.Millisecond)
	}
	p, ok := h.readWithin(2 * time.Second).(*packet.Publish)
	if !ok || p.Topic != "fresh" {
		t.Fatalf("the later delivery: %+v, want it to carry the topic (no alias the client never saw)", p)
	}
}
