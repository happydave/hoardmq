package broker

import (
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

// A client of v reconnects to A, whose pull of v goes unanswered for its whole
// budget. While A's CONNECT is still in that pull, the client retries on B,
// which takes the lease from A and pulls A. A holds no session, no adoption
// and no note for the client yet, so its answer names v, the owner its own
// takeover is waiting on: B counts a takeover of a session in transit and
// judges v as it judges its own previous owner. A's CONNECT is then refused
// and keeps an adoption, which finds the lease moved and hands the session on
// to B.

// TestATakeoverInFlightIsNamedWhenItsOwnerCrashed: v crashes while A's pull
// waits. B refuses the client until v is declared dead, then serves it fresh
// (one copy, gone with v); B's taken entry ends with the session held there
// and A's hand-on ends on B's claim.
func TestATakeoverInFlightIsNamedWhenItsOwnerCrashed(t *testing.T) {
	o := takeoverInFlight(t, true)
	o.wantNamed(t)
	o.retryUntilServed(t, 20*time.Second)
	waitUntil(t, o.b+"'s taken entry ending with y held there", 10*time.Second, func() bool {
		return o.nodes[o.b].srv.Leases().TransitStats().TakenHeld >= 1
	})
	waitUntil(t, o.a+"'s hand-on ending on "+o.b+"'s claim", 15*time.Second, func() bool {
		st := o.nodes[o.a].srv.Leases().TransitStats()
		return st.HandedServed+st.HandedExpired >= 1
	})
	if st := o.nodes[o.a].srv.Leases().TransitStats(); st.HandedExpired != 0 || st.HandedServed < 1 {
		t.Fatalf("%s's hand-on: %+v; want it ended on %s's claim, not at its bound", o.a, st, o.b)
	}
}

// TestATakeoverInFlightIsNamedWhileItsOwnerIsAlive: v stays up and only
// answers late. Named, v is the owner B waits on and asks, and the client is
// served its session, subscription included. Answered with nobody named, B
// would serve it fresh while v still held the session.
func TestATakeoverInFlightIsNamedWhileItsOwnerIsAlive(t *testing.T) {
	o := takeoverInFlight(t, false)
	o.wantNamed(t)
	cy := o.retryUntilServed(t, 20*time.Second)
	if !o.served.SessionPresent {
		t.Fatalf("y on %s: served without its session while %s held it", o.b, o.v)
	}
	pub, _ := o.nodes[o.b].connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	publishUntilAccepted(t, pub, "t/y", "after")
	got := false
	_ = cy.c.SetReadDeadline(time.Now().Add(5 * time.Second))
	for !got {
		p, err := cy.dec.Read()
		if err != nil {
			break
		}
		if m, ok := p.(*packet.Publish); ok && string(m.Payload) == "after" {
			got = true
		}
	}
	if !got {
		t.Fatal("y's subscription did not reach its new owner: a publish to t/y was not delivered")
	}
}

type inFlightOutcome struct {
	nodes   map[string]*fixture
	v, a, b string
	// first is B's answer to the client's first CONNECT there, made while A's
	// was still in its pull; served is the answer that admitted the client.
	first, served *packet.Connack
}

func takeoverInFlight(t *testing.T, crash bool) *inFlightOutcome {
	t.Helper()
	t.Cleanup(func() { fixtureHooks = Hooks{} })
	o := &inFlightOutcome{v: "n2", a: "n1", b: "n3"}
	o.nodes = startTrio(t, func(name string, d *config.Document) {
		d.Cluster.Shards = 8
		d.Cluster.Replicas = 1
		// A 4 s ttl puts the hand-on's bound at 8 s.
		d.Cluster.Leases.TTLMs = 4000
		// A's pull of v, and with it A's CONNECT, lasts 2.5 s: the window in
		// which the client's retry reaches B.
		d.Cluster.Leases.TakeoverTimeoutMs = 2500
		h := Hooks{}
		if name == o.v {
			h.TakeoverDelay = 3 * time.Second
		}
		fixtureHooks = h
	})
	fixtureHooks = Hooks{}
	waitMap(t, o.nodes, 3)
	nodes, v, a, b := o.nodes, o.v, o.a, o.b
	exp := uint32(600)
	c, ack := nodes[v].connect("tcp", packet.V5, &packet.Connect{ClientID: "y", CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("connect y on %s: %s", v, ack.ReasonCode)
	}
	c.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "t/y", QoS: 1}}})
	if _, ok := c.read().(*packet.Suback); !ok {
		t.Fatal("no SUBACK")
	}
	c.send(&packet.Disconnect{ReasonCode: packet.NormalDisconnection})
	_ = c.c.Close()
	waitOffline(t, nodes[v], "y")

	// A's CONNECT, read on its own goroutine: it stays in its pull of v.
	type result struct {
		ack *packet.Connack
		err error
	}
	aDone := make(chan result, 1)
	nc := nodes[a].dial("tcp", false)
	t.Cleanup(func() { _ = nc.Close() })
	enc := &packet.Encoder{Version: packet.V5}
	dec := packet.NewDecoder(nc)
	dec.Version = packet.V5
	_ = nc.SetDeadline(time.Now().Add(15 * time.Second))
	if err := enc.Encode(nc, &packet.Connect{ProtocolLevel: packet.V5, ClientID: "y", Properties: packet.Properties{SessionExpiryInterval: &exp}}); err != nil {
		t.Fatal(err)
	}
	go func() {
		p, err := dec.Read()
		ack, _ := p.(*packet.Connack)
		aDone <- result{ack, err}
	}()
	var aResult *result
	inFlight := func() bool {
		if aResult != nil {
			return false
		}
		select {
		case r := <-aDone:
			aResult = &r
			return false
		default:
			return true
		}
	}
	waitUntil(t, b+"'s table naming "+a+" as y's owner", 5*time.Second, func() bool {
		l, ok := nodes[b].srv.Leases().Table().Lookup("default", "y")
		return ok && l.Owner == a
	})
	if crash {
		nodes[v].srv.Crash()
		nodes[v].stopped = true
	}
	if !inFlight() {
		t.Fatalf("precondition not reached: %s's CONNECT returned (%+v) before the retry on %s", a, aResult, b)
	}
	if !crash {
		if _, held := nodes[v].srv.Sessions().Record("y"); !held {
			t.Fatalf("precondition not reached: %s no longer holds y's session when the client retries on %s", v, b)
		}
	}
	cy, ack := o.reconnect(b)
	_ = cy.c.Close()
	o.first = ack
	if !inFlight() {
		t.Fatalf("precondition not reached: %s's CONNECT returned (%+v) before %s's takeover was answered", a, aResult, b)
	}
	if aResult == nil {
		r := <-aDone
		aResult = &r
	}
	if aResult.err != nil || aResult.ack == nil || aResult.ack.ReasonCode == packet.Success {
		t.Fatalf("precondition not reached: %s's CONNECT ended %+v; want it refused once its pull went unanswered", a, aResult)
	}
	waitUntil(t, a+"'s adoption ending with the lease moved to "+b, 10*time.Second, func() bool {
		return nodes[a].srv.Leases().TransitStats().Handed >= 1
	})
	as, bs := nodes[a].srv.Leases().LivenessStats(), nodes[b].srv.Leases().LivenessStats()
	t.Logf("%s's first CONNECT on %s: %s, Session Present %v", "y", b, o.first.ReasonCode, o.first.SessionPresent)
	t.Logf("%s: answered pending %d (noted %d), notes held %d, worker moved %d, takeover unanswered %d, transit %+v",
		a, as.TakeoverAnsweredPending, as.TakeoverAnsweredPendingNoted, as.PendingOwnerNotes, as.AdoptWorkerMoved, as.TakeoverUnanswered, nodes[a].srv.Leases().TransitStats())
	t.Logf("%s: pending owner %d, empty %d, unanswered %d, transit %+v",
		b, bs.TakeoverPendingOwner, bs.TakeoverEmpty, bs.TakeoverUnanswered, nodes[b].srv.Leases().TransitStats())
	return o
}

func (o *inFlightOutcome) reconnect(node string) (*client, *packet.Connack) {
	exp := uint32(600)
	return o.nodes[node].connect("tcp", packet.V5, &packet.Connect{ClientID: "y", Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
}

// wantNamed checks A's answer named v: B counted a taken entry and refused
// the client, as it does for any previous owner it could not account for.
func (o *inFlightOutcome) wantNamed(t *testing.T) {
	t.Helper()
	as := o.nodes[o.a].srv.Leases().LivenessStats()
	bt := o.nodes[o.b].srv.Leases().TransitStats()
	if as.TakeoverAnsweredPending < 1 || bt.Taken < 1 {
		t.Fatalf("%s answered %s's pull naming %d owners, and %s recorded %d taken entries; want %s named and a taken entry", o.a, o.b, as.TakeoverAnsweredPending, o.b, bt.Taken, o.v)
	}
	if o.first.ReasonCode != packet.ServerBusy {
		t.Fatalf("y's first CONNECT on %s: %s, Session Present %v; want Server Busy while %s is undeclared", o.b, o.first.ReasonCode, o.first.SessionPresent, o.v)
	}
}

// retryUntilServed reconnects the client to B until it is admitted.
func (o *inFlightOutcome) retryUntilServed(t *testing.T, within time.Duration) *client {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		cy, ack := o.reconnect(o.b)
		if ack.ReasonCode == packet.Success {
			o.served = ack
			t.Cleanup(func() { _ = cy.c.Close() })
			return cy
		}
		_ = cy.c.Close()
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("y was not served on %s within %s", o.b, within)
	return nil
}
