package session

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/packet"
)

type allow struct{}

func (allow) Authorize(context.Context, auth.Action, string, *auth.Identity) auth.Decision {
	return auth.Allow
}

func newManager(t *testing.T, mutate func(*Options)) (*Manager, *fakeClock) {
	t.Helper()
	clock := newFakeClock()
	opts := Options{MaxQueued: 10, Authorizers: auth.AuthorizerChain{allow{}}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Clock: clock}
	if mutate != nil {
		mutate(&opts)
	}
	return New(opts), clock
}

func connectV5(m *Manager, c *fakeConn, clean bool, expiry uint32, will *packet.Will) bool {
	pkt := &packet.Connect{ProtocolLevel: packet.V5, ClientID: c.id, CleanStart: clean, Will: will}
	if expiry != 0 {
		pkt.Properties.SessionExpiryInterval = &expiry
	}
	res := m.Connect(c, pkt, 0)
	m.Resume(c)
	return res.SessionPresent
}

func subscribe(m *Manager, c *fakeConn, qos byte, filters ...string) {
	s := &packet.Subscribe{PacketID: 1}
	for _, f := range filters {
		s.Subscriptions = append(s.Subscriptions, packet.Subscription{Filter: f, QoS: qos})
	}
	m.Subscribe(c, s)
}

func publish(m *Manager, c *fakeConn, topic string, qos byte, id uint16, payload string) packet.ReasonCode {
	code, _ := m.Publish(c, &packet.Publish{Topic: topic, QoS: qos, PacketID: id, Payload: []byte(payload)})
	return code
}

func TestSessionStateSurvivesCleanStartZero(t *testing.T) {
	m, _ := newManager(t, nil)
	pub := newFake("pub", packet.V5, 100)
	sub := newFake("sub", packet.V5, 100)
	connectV5(m, pub, true, 0, nil)
	if present := connectV5(m, sub, false, 3600, &packet.Will{Topic: "w", Payload: []byte("bye")}); present {
		t.Fatal("new session reported present")
	}
	subscribe(m, sub, 2, "t")
	// In flight both ways: an outbound QoS 2 awaiting PUBCOMP and an
	// inbound QoS 2 awaiting PUBREL.
	publish(m, pub, "t", 2, 1, "out")
	m.Publish(sub, &packet.Publish{Topic: "elsewhere", QoS: 2, PacketID: 77})
	// Queued: over the quota.
	sub.quota = 0
	publish(m, pub, "t", 1, 2, "queued")
	m.Disconnect(sub, false, packet.UnspecifiedError, false)
	rec, ok := m.Record("sub")
	if !ok || rec.Online || rec.Subscriptions != 1 || rec.InFlight != 1 || rec.Queued != 1 || rec.HasWill {
		t.Fatalf("offline record %+v", rec)
	}
	sub2 := newFake("sub", packet.V5, 100)
	if present := connectV5(m, sub2, false, 3600, nil); !present {
		t.Fatal("session not present on resume")
	}
	pubs := sub2.publishes()
	if len(pubs) != 2 || string(pubs[0].Payload) != "out" || !pubs[0].Dup || pubs[0].PacketID != 1 || string(pubs[1].Payload) != "queued" || pubs[1].Dup {
		t.Fatalf("replay: %+v", pubs)
	}
	if code, _ := m.Pubrel(sub2, 77); code != packet.Success {
		t.Errorf("inbound QoS 2 identifier lost across reconnect: %s", code)
	}
	if code, _ := m.Pubrel(sub2, 78); code != packet.PacketIdentifierNotFound {
		t.Errorf("unknown PUBREL: %s", code)
	}
	// Subscriptions survived: a new publish arrives.
	publish(m, pub, "t", 0, 0, "again")
	if p := sub2.publishes(); len(p) != 3 || string(p[2].Payload) != "again" {
		t.Fatalf("subscription lost: %+v", p)
	}
	rec, _ = m.Record("sub")
	if rec.Epoch != 0 {
		t.Errorf("epoch changed on plain resume: %d", rec.Epoch)
	}
}

func TestCleanStartDiscardsState(t *testing.T) {
	m, _ := newManager(t, nil)
	sub := newFake("sub", packet.V5, 100)
	connectV5(m, sub, false, 3600, nil)
	subscribe(m, sub, 1, "t")
	m.Disconnect(sub, true, 0, false)
	sub2 := newFake("sub", packet.V5, 100)
	if present := connectV5(m, sub2, true, 3600, nil); present {
		t.Fatal("clean start reported a session present")
	}
	rec, _ := m.Record("sub")
	if rec.Subscriptions != 0 || rec.Epoch != 1 {
		t.Fatalf("%+v", rec)
	}
	if len(m.router.Match(&message.Message{Namespace: "default", Topic: "t"}, nil)) != 0 {
		t.Error("router kept the discarded subscription")
	}
}

func TestExpiryZeroDiscardsOnDisconnect(t *testing.T) {
	m, _ := newManager(t, nil)
	c := newFake("c", packet.V5, 100)
	connectV5(m, c, false, 0, nil)
	m.Disconnect(c, true, 0, false)
	if _, ok := m.Record("c"); ok {
		t.Fatal("session kept with expiry 0")
	}
	// Level 4 Clean Session 1 is the same; Clean Session 0 never expires.
	c4 := newFake("c4", packet.V4, 100)
	m.Connect(c4, &packet.Connect{ProtocolLevel: packet.V4, ClientID: "c4", CleanStart: false}, 0)
	m.Disconnect(c4, true, 0, false)
	if rec, ok := m.Record("c4"); !ok || rec.ExpiryInterval != NeverExpires {
		t.Fatalf("%+v %v", rec, ok)
	}
}

func TestExpiryTimerAndOverride(t *testing.T) {
	m, clock := newManager(t, func(o *Options) { o.MaxSessionExpiry = 100 })
	c := newFake("c", packet.V5, 100)
	res := m.Connect(c, &packet.Connect{ProtocolLevel: packet.V5, ClientID: "c", Properties: packet.Properties{SessionExpiryInterval: u32(500)}}, 0)
	if res.Properties.SessionExpiryInterval == nil || *res.Properties.SessionExpiryInterval != 100 {
		t.Fatalf("cap not advertised: %+v", res.Properties)
	}
	// DISCONNECT lowers it to 10 seconds.
	c.override = u32(10)
	m.Disconnect(c, true, 0, false)
	clock.advance(9 * time.Second)
	if _, ok := m.Record("c"); !ok {
		t.Fatal("expired early")
	}
	clock.advance(2 * time.Second)
	if _, ok := m.Record("c"); ok {
		t.Fatal("did not expire")
	}
	// Reconnect cancels the timer.
	c2 := newFake("c", packet.V5, 100)
	connectV5(m, c2, false, 5, nil)
	m.Disconnect(c2, true, 0, false)
	clock.advance(3 * time.Second)
	c3 := newFake("c", packet.V5, 100)
	if !connectV5(m, c3, false, 5, nil) {
		t.Fatal("not present before expiry")
	}
	clock.advance(10 * time.Second)
	if rec, ok := m.Record("c"); !ok || !rec.Online {
		t.Fatal("timer fired on a reconnected session")
	}
	// An override on a session that set 0 at CONNECT is ignored.
	z := newFake("z", packet.V5, 100)
	connectV5(m, z, true, 0, nil)
	z.override = u32(60)
	m.Disconnect(z, true, 0, false)
	if _, ok := m.Record("z"); ok {
		t.Fatal("override applied over a zero CONNECT interval")
	}
}

func u32(v uint32) *uint32 { return &v }

func TestQoS2ExactlyOnceUnderDuplicate(t *testing.T) {
	m, _ := newManager(t, nil)
	pub := newFake("pub", packet.V5, 100)
	sub := newFake("sub", packet.V5, 100)
	connectV5(m, pub, true, 0, nil)
	connectV5(m, sub, true, 0, nil)
	subscribe(m, sub, 2, "t")
	if code := publish(m, pub, "t", 2, 5, "once"); code != packet.Success {
		t.Fatal(code)
	}
	// The client did not get our PUBREC and resends with DUP.
	if code, _ := m.Publish(pub, &packet.Publish{Topic: "t", QoS: 2, PacketID: 5, Dup: true, Payload: []byte("once")}); code != packet.Success {
		t.Fatal(code)
	}
	if n := len(sub.publishes()); n != 1 {
		t.Fatalf("delivered %d times", n)
	}
	if code, _ := m.Pubrel(pub, 5); code != packet.Success {
		t.Fatal(code)
	}
	// After PUBREL a new publish with the same identifier is new.
	publish(m, pub, "t", 2, 5, "twice")
	if n := len(sub.publishes()); n != 2 {
		t.Fatalf("delivered %d times", n)
	}
}

func TestOutboundQoS2AcrossReconnect(t *testing.T) {
	m, _ := newManager(t, nil)
	pub := newFake("pub", packet.V5, 100)
	sub := newFake("sub", packet.V5, 100)
	connectV5(m, pub, true, 0, nil)
	connectV5(m, sub, false, 3600, nil)
	subscribe(m, sub, 2, "t")
	publish(m, pub, "t", 2, 1, "m")
	id := sub.publishes()[0].PacketID
	if code, send := m.Pubrec(sub, id, packet.Success); code != packet.Success || !send {
		t.Fatal(code)
	}
	m.Disconnect(sub, false, 0, false)
	sub2 := newFake("sub", packet.V5, 100)
	connectV5(m, sub2, false, 3600, nil)
	pk := sub2.packets()
	if len(pk) != 1 {
		t.Fatalf("replay: %+v", pk)
	}
	if rel, ok := pk[0].(*packet.Pubrel); !ok || rel.PacketID != id {
		t.Fatalf("expected PUBREL replay, got %+v", pk[0])
	}
	m.Pubcomp(sub2, id, packet.Success)
	if rec, _ := m.Record("sub"); rec.InFlight != 0 {
		t.Errorf("in flight after PUBCOMP: %d", rec.InFlight)
	}
	// A second PUBCOMP is ignored.
	m.Pubcomp(sub2, id, packet.Success)
	// PUBREC with an error ends the exchange.
	publish(m, pub, "t", 2, 2, "n")
	id2 := sub2.publishes()[len(sub2.publishes())-1].PacketID
	if code, _ := m.Pubrec(sub2, id2, packet.QuotaExceeded); code != packet.QuotaExceeded {
		t.Fatal(code)
	}
	if rec, _ := m.Record("sub"); rec.InFlight != 0 {
		t.Errorf("in flight after error PUBREC: %d", rec.InFlight)
	}
}

func TestQoS1RetransmitOnlyOnResumption(t *testing.T) {
	m, clock := newManager(t, nil)
	pub := newFake("pub", packet.V5, 100)
	sub := newFake("sub", packet.V5, 100)
	connectV5(m, pub, true, 0, nil)
	connectV5(m, sub, false, 3600, nil)
	subscribe(m, sub, 1, "t")
	publish(m, pub, "t", 1, 1, "m")
	clock.advance(time.Hour)
	if n := len(sub.publishes()); n != 1 {
		t.Fatalf("resent without a reconnect: %d", n)
	}
	m.Disconnect(sub, false, 0, false)
	sub2 := newFake("sub", packet.V5, 100)
	connectV5(m, sub2, false, 3600, nil)
	p := sub2.publishes()
	if len(p) != 1 || !p[0].Dup || p[0].PacketID != 1 {
		t.Fatalf("%+v", p)
	}
	m.Puback(sub2, 1, packet.Success)
	m.Puback(sub2, 1, packet.Success)
	if rec, _ := m.Record("sub"); rec.InFlight != 0 {
		t.Error("PUBACK did not clear")
	}
}

func TestOrderingAcrossReconnect(t *testing.T) {
	m, _ := newManager(t, func(o *Options) { o.MaxQueued = 1000 })
	pub := newFake("pub", packet.V5, 1000)
	sub := newFake("sub", packet.V5, 5)
	connectV5(m, pub, true, 0, nil)
	connectV5(m, sub, false, 3600, nil)
	subscribe(m, sub, 1, "t")
	for i := 0; i < 50; i++ {
		publish(m, pub, "t", 1, uint16(i+1), string(rune('A'+i%26))+itoa(i))
	}
	m.Disconnect(sub, false, 0, false)
	for i := 50; i < 100; i++ {
		publish(m, pub, "t", 1, uint16(i+1), string(rune('A'+i%26))+itoa(i))
	}
	sub2 := newFake("sub", packet.V5, 7)
	connectV5(m, sub2, false, 3600, nil)
	var got []string
	seen := 0
	for seen < 100 {
		pubs := sub2.publishes()
		if len(pubs) == seen {
			t.Fatalf("stalled at %d", seen)
		}
		for _, p := range pubs[seen:] {
			got = append(got, string(p.Payload))
			m.Puback(sub2, p.PacketID, packet.Success)
			sub2.ack()
		}
		seen = len(pubs)
	}
	for i, g := range got {
		want := string(rune('A'+i%26)) + itoa(i)
		if g != want {
			t.Fatalf("position %d: got %s want %s", i, g, want)
		}
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

func TestQueueLimitsAndDropStrategies(t *testing.T) {
	for _, newest := range []bool{false, true} {
		m, _ := newManager(t, func(o *Options) { o.MaxQueued = 3; o.DropNewest = newest })
		pub := newFake("pub", packet.V5, 100)
		sub := newFake("sub", packet.V5, 100)
		connectV5(m, pub, true, 0, nil)
		connectV5(m, sub, false, 3600, nil)
		subscribe(m, sub, 1, "t")
		m.Disconnect(sub, false, 0, false)
		for i := 1; i <= 5; i++ {
			publish(m, pub, "t", 1, uint16(i), itoa(i))
		}
		rec, _ := m.Record("sub")
		if rec.Queued != 3 || rec.Drops != 2 {
			t.Fatalf("newest=%v: %+v", newest, rec)
		}
		sub2 := newFake("sub", packet.V5, 100)
		connectV5(m, sub2, false, 3600, nil)
		var got string
		for _, p := range sub2.publishes() {
			got += string(p.Payload)
		}
		want := "345"
		if newest {
			want = "123"
		}
		if got != want {
			t.Errorf("newest=%v: got %s want %s", newest, got, want)
		}
	}
}

func TestQuotaGatedDelivery(t *testing.T) {
	m, _ := newManager(t, nil)
	pub := newFake("pub", packet.V5, 100)
	sub := newFake("sub", packet.V5, 2)
	connectV5(m, pub, true, 0, nil)
	connectV5(m, sub, true, 0, nil)
	subscribe(m, sub, 1, "t")
	for i := 1; i <= 4; i++ {
		publish(m, pub, "t", 1, uint16(i), itoa(i))
	}
	publish(m, pub, "t", 0, 0, "q0")
	pubs := sub.publishes()
	if len(pubs) != 2 {
		t.Fatalf("sent %d with quota 2", len(pubs))
	}
	rec, _ := m.Record("sub")
	if rec.Queued != 3 || rec.InFlight != 2 {
		t.Fatalf("%+v", rec)
	}
	sub.ack()
	m.Puback(sub, pubs[0].PacketID, packet.Success)
	if p := sub.publishes(); len(p) != 3 || string(p[2].Payload) != "3" {
		t.Fatalf("drain after ack: %+v", p)
	}
	// QoS 0 behind QoS 1 in the queue keeps its place.
	sub.ack()
	m.Puback(sub, pubs[1].PacketID, packet.Success)
	sub.ack()
	m.Puback(sub, sub.publishes()[2].PacketID, packet.Success)
	var order string
	for _, p := range sub.publishes() {
		order += string(p.Payload) + ","
	}
	if order != "1,2,3,4,q0," {
		t.Errorf("order %s", order)
	}
}

func TestWillPublication(t *testing.T) {
	will := &packet.Will{Topic: "w", Payload: []byte("gone"), QoS: 1}
	setup := func(t *testing.T, delay uint32, expiry uint32) (*Manager, *fakeClock, *fakeConn, *fakeConn) {
		m, clock := newManager(t, nil)
		watcher := newFake("watcher", packet.V5, 100)
		connectV5(m, watcher, true, 0, nil)
		subscribe(m, watcher, 1, "w")
		c := newFake("c", packet.V5, 100)
		w := *will
		if delay > 0 {
			w.Properties.WillDelayInterval = u32(delay)
		}
		connectV5(m, c, false, expiry, &w)
		return m, clock, c, watcher
	}
	t.Run("immediate", func(t *testing.T) {
		m, _, c, watcher := setup(t, 0, 3600)
		m.Disconnect(c, false, packet.KeepAliveTimeout, true)
		if p := watcher.publishes(); len(p) != 1 || string(p[0].Payload) != "gone" || p[0].Topic != "w" {
			t.Fatalf("%+v", p)
		}
	})
	t.Run("after delay", func(t *testing.T) {
		m, clock, c, watcher := setup(t, 30, 3600)
		m.Disconnect(c, false, 0, true)
		clock.advance(29 * time.Second)
		if len(watcher.publishes()) != 0 {
			t.Fatal("published early")
		}
		clock.advance(2 * time.Second)
		if len(watcher.publishes()) != 1 {
			t.Fatal("not published after delay")
		}
	})
	t.Run("suppressed by reconnect", func(t *testing.T) {
		m, clock, c, watcher := setup(t, 30, 3600)
		m.Disconnect(c, false, 0, true)
		clock.advance(10 * time.Second)
		c2 := newFake("c", packet.V5, 100)
		connectV5(m, c2, false, 3600, nil)
		clock.advance(time.Hour)
		if len(watcher.publishes()) != 0 {
			t.Fatal("published despite reconnect")
		}
	})
	t.Run("at session expiry when earlier", func(t *testing.T) {
		m, clock, c, watcher := setup(t, 300, 60)
		m.Disconnect(c, false, 0, true)
		if len(watcher.publishes()) != 0 {
			t.Fatal("published before the session ended")
		}
		clock.advance(59 * time.Second)
		if len(watcher.publishes()) != 0 {
			t.Fatal("published before expiry")
		}
		clock.advance(2 * time.Second)
		if len(watcher.publishes()) != 1 {
			t.Fatal("not published at session end")
		}
		clock.advance(time.Hour)
		if len(watcher.publishes()) != 1 {
			t.Fatal("published twice")
		}
	})
	t.Run("discarded on normal disconnect", func(t *testing.T) {
		m, clock, c, watcher := setup(t, 0, 3600)
		m.Disconnect(c, true, packet.NormalDisconnection, false)
		clock.advance(time.Hour)
		if len(watcher.publishes()) != 0 {
			t.Fatal("published after normal disconnect")
		}
		if rec, _ := m.Record("c"); rec.HasWill {
			t.Error("will kept after normal disconnect")
		}
	})
	t.Run("kept on 0x04", func(t *testing.T) {
		m, _, c, watcher := setup(t, 0, 3600)
		m.Disconnect(c, true, packet.DisconnectWithWillMessage, true)
		if len(watcher.publishes()) != 1 {
			t.Fatal("not published on 0x04")
		}
	})
	t.Run("not authorized", func(t *testing.T) {
		m, _ := newManager(t, func(o *Options) { o.Authorizers = nil })
		watcher := newFake("watcher", packet.V5, 100)
		connectV5(m, watcher, true, 0, nil)
		subscribe(m, watcher, 1, "w")
		c := newFake("c", packet.V5, 100)
		connectV5(m, c, false, 3600, will)
		m.Disconnect(c, false, 0, true)
		if len(watcher.publishes()) != 0 {
			t.Fatal("unauthorized will published")
		}
	})
}

func TestTakeover(t *testing.T) {
	m, _ := newManager(t, nil)
	watcher := newFake("watcher", packet.V5, 100)
	connectV5(m, watcher, true, 0, nil)
	subscribe(m, watcher, 1, "w")
	first := newFake("c", packet.V5, 100)
	connectV5(m, first, false, 3600, &packet.Will{Topic: "w", Payload: []byte("bye")})
	subscribe(m, first, 1, "t")
	second := newFake("c", packet.V5, 100)
	if !connectV5(m, second, false, 3600, nil) {
		t.Fatal("session not present on takeover")
	}
	if len(first.closed) != 1 || first.closed[0] != packet.SessionTakenOver {
		t.Fatalf("old connection close: %v", first.closed)
	}
	// The old connection's disconnect callback arrives late and must not
	// disturb the new one; its Will is suppressed.
	m.Disconnect(first, false, packet.SessionTakenOver, true)
	rec, _ := m.Record("c")
	if !rec.Online || rec.Epoch != 1 || rec.Subscriptions != 1 {
		t.Fatalf("%+v", rec)
	}
	if len(watcher.publishes()) != 0 {
		t.Error("will published on takeover")
	}
	// Publishes from the old connection are ignored.
	if code := publish(m, first, "t", 1, 1, "stale"); code != packet.UnspecifiedError {
		t.Errorf("stale publish accepted: %s", code)
	}
	if len(second.publishes()) != 0 {
		t.Error("stale publish delivered")
	}
}

func TestMessageExpiry(t *testing.T) {
	m, clock := newManager(t, nil)
	pub := newFake("pub", packet.V5, 100)
	sub := newFake("sub", packet.V5, 100)
	connectV5(m, pub, true, 0, nil)
	connectV5(m, sub, false, 3600, nil)
	subscribe(m, sub, 1, "t")
	m.Disconnect(sub, false, 0, false)
	m.Publish(pub, &packet.Publish{Topic: "t", QoS: 1, PacketID: 1, Payload: []byte("short"), Properties: packet.Properties{MessageExpiryInterval: u32(10)}})
	m.Publish(pub, &packet.Publish{Topic: "t", QoS: 1, PacketID: 2, Payload: []byte("long"), Properties: packet.Properties{MessageExpiryInterval: u32(100)}})
	clock.advance(30 * time.Second)
	sub2 := newFake("sub", packet.V5, 100)
	connectV5(m, sub2, false, 3600, nil)
	p := sub2.publishes()
	if len(p) != 1 || string(p[0].Payload) != "long" || *p[0].Properties.MessageExpiryInterval != 70 {
		t.Fatalf("%+v", p)
	}
	if rec, _ := m.Record("sub"); rec.Expired != 1 {
		t.Errorf("expired count %d", rec.Expired)
	}
}

// A message that went in flight before the disconnect expires like a queued
// one: the resumption must not retransmit it. Only the queued path was
// covered when sessions landed, and the in-flight path emitted a wrapped
// Message Expiry Interval instead of dropping the message.
func TestInflightMessageExpiresBeforeResumption(t *testing.T) {
	m, clock := newManager(t, nil)
	pub := newFake("pub", packet.V5, 100)
	sub := newFake("sub", packet.V5, 100)
	connectV5(m, pub, true, 0, nil)
	connectV5(m, sub, false, 3600, nil)
	subscribe(m, sub, 1, "t")
	// Delivered on the live connection and left unacknowledged: in flight.
	m.Publish(pub, &packet.Publish{Topic: "t", QoS: 1, PacketID: 1, Payload: []byte("short"), Properties: packet.Properties{MessageExpiryInterval: u32(10)}})
	m.Publish(pub, &packet.Publish{Topic: "t", QoS: 1, PacketID: 2, Payload: []byte("long"), Properties: packet.Properties{MessageExpiryInterval: u32(100)}})
	if n := len(sub.publishes()); n != 2 {
		t.Fatalf("delivered %d while online", n)
	}
	m.Disconnect(sub, false, 0, false)
	clock.advance(30 * time.Second)
	sub2 := newFake("sub", packet.V5, 100)
	connectV5(m, sub2, false, 3600, nil)
	p := sub2.publishes()
	if len(p) != 1 || string(p[0].Payload) != "long" || !p[0].Dup {
		t.Fatalf("replay: %+v", p)
	}
	if got := *p[0].Properties.MessageExpiryInterval; got != 70 {
		t.Errorf("MessageExpiryInterval %d, want 70", got)
	}
	rec, _ := m.Record("sub")
	if rec.Expired != 1 || rec.InFlight != 1 {
		t.Errorf("%+v", rec)
	}
	// The dropped message's packet identifier is free again.
	m.Puback(sub2, p[0].PacketID, packet.Success)
	if rec, _ := m.Record("sub"); rec.InFlight != 0 {
		t.Errorf("in flight after PUBACK: %d", rec.InFlight)
	}
}

// A QoS 2 message already acknowledged with PUBREC is completed even when its
// deadline passes: the payload was delivered, only the handshake remains.
func TestExpiredPubrecReceivedStillCompletes(t *testing.T) {
	m, clock := newManager(t, nil)
	pub := newFake("pub", packet.V5, 100)
	sub := newFake("sub", packet.V5, 100)
	connectV5(m, pub, true, 0, nil)
	connectV5(m, sub, false, 3600, nil)
	subscribe(m, sub, 2, "t")
	m.Publish(pub, &packet.Publish{Topic: "t", QoS: 2, PacketID: 1, Payload: []byte("x"), Properties: packet.Properties{MessageExpiryInterval: u32(10)}})
	id := sub.publishes()[0].PacketID
	if code, send := m.Pubrec(sub, id, packet.Success); code != packet.Success || !send {
		t.Fatal(code)
	}
	m.Disconnect(sub, false, 0, false)
	clock.advance(30 * time.Second)
	sub2 := newFake("sub", packet.V5, 100)
	connectV5(m, sub2, false, 3600, nil)
	pk := sub2.packets()
	if len(pk) != 1 {
		t.Fatalf("replay: %+v", pk)
	}
	if rel, ok := pk[0].(*packet.Pubrel); !ok || rel.PacketID != id {
		t.Fatalf("expected PUBREL replay, got %+v", pk[0])
	}
	m.Pubcomp(sub2, id, packet.Success)
	if rec, _ := m.Record("sub"); rec.InFlight != 0 || rec.Expired != 0 {
		t.Errorf("%+v", rec)
	}
}

func TestNoMatchingSubscribersAndSubscriptionReplace(t *testing.T) {
	m, _ := newManager(t, nil)
	pub := newFake("pub", packet.V5, 100)
	connectV5(m, pub, true, 0, nil)
	if code := publish(m, pub, "t", 1, 1, "x"); code != packet.NoMatchingSubscribers {
		t.Fatal(code)
	}
	if code := publish(m, pub, "t", 0, 0, "x"); code != packet.Success {
		t.Fatal(code)
	}
	pub4 := newFake("pub4", packet.V4, 100)
	m.Connect(pub4, &packet.Connect{ProtocolLevel: packet.V4, ClientID: "pub4", CleanStart: true}, 0)
	if code := publish(m, pub4, "t", 1, 1, "x"); code != packet.Success {
		t.Fatal("level 4 must not see 0x10")
	}
	sub := newFake("sub", packet.V5, 100)
	connectV5(m, sub, true, 0, nil)
	codes := m.Subscribe(sub, &packet.Subscribe{PacketID: 1, Properties: packet.Properties{SubscriptionIdentifiers: []uint32{9}}, Subscriptions: []packet.Subscription{{Filter: "t", QoS: 2}}})
	if codes[0] != packet.GrantedQoS2 {
		t.Fatal(codes)
	}
	subscribe(m, sub, 0, "t")
	publish(m, pub, "t", 2, 3, "downgraded")
	p := sub.publishes()
	if len(p) != 1 || p[0].QoS != 0 || len(p[0].Properties.SubscriptionIdentifiers) != 0 {
		t.Fatalf("replaced subscription: %+v", p)
	}
	subscribe(m, sub, 2, "t")
	publish(m, pub, "t", 1, 4, "id")
	m.Subscribe(sub, &packet.Subscribe{PacketID: 1, Properties: packet.Properties{SubscriptionIdentifiers: []uint32{9}}, Subscriptions: []packet.Subscription{{Filter: "t", QoS: 2}}})
	publish(m, pub, "t", 1, 5, "with id")
	p = sub.publishes()
	if last := p[len(p)-1]; len(last.Properties.SubscriptionIdentifiers) != 1 || last.Properties.SubscriptionIdentifiers[0] != 9 {
		t.Fatalf("subscription identifier not attached: %+v", last)
	}
	codes = m.Unsubscribe(sub, &packet.Unsubscribe{PacketID: 2, Filters: []string{"t", "nope"}})
	if codes[0] != packet.Success || codes[1] != packet.NoSubscriptionExisted {
		t.Fatal(codes)
	}
	if m.Count() != 3 {
		t.Errorf("count %d", m.Count())
	}
}

func TestPacketIdentifierAllocation(t *testing.T) {
	s := newSession(key{"default", "x"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	s.nextID = 65533
	ids := []uint16{s.allocateID(), s.allocateID(), s.allocateID(), s.allocateID()}
	if ids[0] != 65534 || ids[1] != 65535 || ids[2] != 1 || ids[3] != 2 {
		t.Fatalf("%v", ids)
	}
	s.inflightByID[3] = &inflight{}
	if id := s.allocateID(); id != 4 {
		t.Fatalf("did not skip in-flight identifier: %d", id)
	}
	for i := 1; i <= 65535; i++ {
		s.inflightByID[uint16(i)] = &inflight{}
	}
	if id := s.allocateID(); id != 0 {
		t.Fatalf("exhausted space returned %d", id)
	}
}

func TestExactRouter(t *testing.T) {
	r := newExactRouter()
	a := newSession(key{"default", "a"}, slog.Default())
	b := newSession(key{"default", "b"}, slog.Default())
	if r.Add(b, Subscription{Filter: "t", QoS: 1}) {
		t.Error("first Add reported existed")
	}
	if !r.Add(b, Subscription{Filter: "t", QoS: 1}) {
		t.Error("second Add did not report existed")
	}
	r.Add(a, Subscription{Filter: "t", QoS: 2, Identifier: 4})
	r.Add(a, Subscription{Filter: "u", QoS: 0, NoLocal: true})
	msg := &message.Message{Namespace: "default", Topic: "t"}
	m := r.Match(msg, nil)
	if len(m) != 2 || m[0].Session != a || m[1].Session != b || m[0].QoS != 2 || m[0].Identifiers[0] != 4 {
		t.Fatalf("%+v", m)
	}
	if len(r.Match(&message.Message{Namespace: "other", Topic: "t"}, nil)) != 0 || len(r.Match(&message.Message{Namespace: "default", Topic: "t/x"}, nil)) != 0 {
		t.Error("exact match only")
	}
	if len(r.Match(&message.Message{Namespace: "default", Topic: "u"}, a)) != 0 {
		t.Error("No Local not applied")
	}
	if !r.Remove(a, "t") || r.Remove(a, "t") {
		t.Error("Remove")
	}
	r.RemoveAll(a, []string{"t", "u"}) // "t" is already gone: a no-op
	if len(r.Match(&message.Message{Namespace: "default", Topic: "u"}, nil)) != 0 || len(r.Match(msg, nil)) != 1 {
		t.Error("RemoveAll")
	}
	ret := &message.Message{Namespace: "default", Topic: "r", Payload: []byte("x")}
	r.Retain(ret)
	if got := r.RetainedFor("default", "r", time.Now()); len(got) != 1 || got[0] != ret {
		t.Error("retained lookup")
	}
	r.Retain(&message.Message{Namespace: "default", Topic: "r"})
	if got := r.RetainedFor("default", "r", time.Now()); len(got) != 0 {
		t.Error("empty payload did not remove")
	}
	if _, ok := r.MatchGroup("default", "g/t", msg, nil); ok {
		t.Error("exact router has no groups")
	}
}

// TestQueueDropsReleaseTheirPacketIdentifiers: a QoS 1 or 2 message reserves
// its packet identifier when it is queued, so the identifier travels in the
// replicated enqueue record and a session rebuilt from a replica re-sends
// under it. A message that is dropped instead of sent must give the
// identifier back — there are only 65535, and a session under sustained
// queue pressure would otherwise run the space down until it can send
// nothing at all.
func TestQueueDropsReleaseTheirPacketIdentifiers(t *testing.T) {
	for _, newest := range []bool{false, true} {
		m, _ := newManager(t, func(o *Options) { o.MaxQueued = 3; o.DropNewest = newest })
		pub := newFake("pub", packet.V5, 100)
		sub := newFake("sub", packet.V5, 100)
		connectV5(m, pub, true, 0, nil)
		connectV5(m, sub, false, 3600, nil)
		subscribe(m, sub, 1, "t")
		m.Disconnect(sub, false, 0, false)
		for i := 1; i <= 200; i++ {
			publish(m, pub, "t", 1, uint16(i%60000+1), itoa(i))
		}
		k := key{m.opts.Namespace, "sub"}
		sh := m.shardFor(k)
		sh.mu.Lock()
		s := sh.sessions[k]
		sh.mu.Unlock()
		if s == nil {
			t.Fatal("no session")
		}
		s.mu.Lock()
		held, queued := len(s.queuedIDs), len(s.queue)
		s.mu.Unlock()
		if held != queued {
			t.Fatalf("newest=%v: %d identifiers reserved for %d queued messages", newest, held, queued)
		}
	}
}
