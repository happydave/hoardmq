package sim

import (
	"context"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
)

// waitFor polls until cond holds, and fails with why when it does not.
func waitFor(t *testing.T, d time.Duration, why string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal(why)
}

// A client is recovered once it is online with its filter granted, and the
// grant is what the reading waits for: before it, the client is online and
// receives nothing.
func TestRecoveredNeedsTheGrant(t *testing.T) {
	b := newFakeBroker(t, func(int, string) packet.ReasonCode { return packet.GrantedQoS1 }, brokerOpts{})
	c := NewClient("rec-1", []string{b.addr()}, 60, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := c.Run(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Subscribe(ctx, "rec/1", 1); err != nil {
		t.Fatal(err)
	}
	if !c.Recovered() {
		t.Fatal("online with its filter granted and not recovered")
	}
}

// A client whose resubscribe is refused stays online and is not recovered:
// it receives nothing, and the headline must not count it as back.
func TestRefusedResubscribeIsNotRecovered(t *testing.T) {
	b := newFakeBroker(t, func(int, string) packet.ReasonCode { return packet.GrantedQoS1 },
		brokerOpts{refusedOn: 2})
	c := NewClient("rec-2", []string{b.addr()}, 60, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := c.Run(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Subscribe(ctx, "rec/2", 1); err != nil {
		t.Fatal(err)
	}
	if !c.Recovered() {
		t.Fatal("not recovered before the drop, so the test proves nothing about after it")
	}
	b.dropOpen()
	waitFor(t, 15*time.Second, "the refused resubscribe was never counted", func() bool { return c.SubRefusals.Load() > 0 })
	waitFor(t, 5*time.Second, "back online after the refusal", func() bool { return c.Online() })
	if c.Recovered() {
		t.Fatal("online with a refused subscription and counted as recovered")
	}
}

// Between the CONNACK and the SUBACK the client is online and not recovered.
// The resubscribe cannot wait for its answers — it runs before the reader
// starts — so this window is the one every displaced client passes through.
func TestRecoveredWaitsForTheResubscribeAck(t *testing.T) {
	b := newFakeBroker(t, func(int, string) packet.ReasonCode { return packet.GrantedQoS1 },
		brokerOpts{dropAfterSuback: true})
	c := NewClient("rec-3", []string{b.addr()}, 60, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := c.Run(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Subscribe(ctx, "rec/3", 1); err != nil {
		t.Fatal(err)
	}
	// The broker drops connection 1 after its SUBACK. The client comes back,
	// is told there is no session, and resubscribes.
	waitFor(t, 15*time.Second, "the client never reconnected", func() bool { return c.Reconnects.Load() > 0 })
	waitFor(t, 15*time.Second, "the resubscribe was never granted", func() bool { return c.Recovered() })
	if b.subscribesOn(2) == 0 {
		t.Fatal("the second connection carried no SUBSCRIBE, so no grant was waited for")
	}
}

// Session Present is the broker stating it holds the session. The client is
// recovered at the CONNACK and sends no SUBSCRIBE at all.
func TestRecoveredOnSessionPresent(t *testing.T) {
	b := newFakeBroker(t, func(int, string) packet.ReasonCode { return packet.GrantedQoS1 },
		brokerOpts{dropAfterSuback: true, presentFrom: 2})
	c := NewClient("rec-4", []string{b.addr()}, 60, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := c.Run(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Subscribe(ctx, "rec/4", 1); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 15*time.Second, "the client never reconnected", func() bool { return c.Reconnects.Load() > 0 })
	waitFor(t, 15*time.Second, "not recovered after a Session Present reconnect", func() bool { return c.Recovered() })
	if got := b.subscribesOn(2); got != 0 {
		t.Fatalf("the client re-subscribed %d times although its session was present", got)
	}
	if got := c.SessionLost.Load(); got != 0 {
		t.Fatalf("SessionLost %d although the session was present on the reconnect", got)
	}
}

// Session Present false is counted only when the client already held a
// session: a first connection legitimately has none. This is WI 1653's
// population, which the resubscribe hid.
func TestSessionLostCountsOnlyReconnects(t *testing.T) {
	b := newFakeBroker(t, func(int, string) packet.ReasonCode { return packet.GrantedQoS1 },
		brokerOpts{dropAfterSuback: true})
	c := NewClient("rec-5", []string{b.addr()}, 60, 5)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := c.Run(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if got := c.SessionLost.Load(); got != 0 {
		t.Fatalf("SessionLost %d on a first connection, want 0", got)
	}
	if err := c.Subscribe(ctx, "rec/5", 1); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 15*time.Second, "the reconnect with no session was not counted",
		func() bool { return c.SessionLost.Load() >= 1 })
}

// A grant belongs to the connection it was made on. A SUBACK that arrives
// carrying a recycled packet identifier from a connection that has gone must
// not grant anything: the client is on a new connection whose subscriptions
// have not been re-established.
func TestGrantIsBoundToItsGeneration(t *testing.T) {
	c := NewClient("rec-6", []string{"127.0.0.1:1"}, 60, 6)
	c.online.Store(true)
	c.gen.Store(7)
	c.subs = []packet.Subscription{{Filter: "rec/6", QoS: 1}}
	c.grantedIn = []uint64{6} // granted on the previous connection
	c.awaiting = []pendingSub{{id: 42, idx: 0, gen: 6}}
	c.refreshRecoveredLocked()
	if c.Recovered() {
		t.Fatal("a grant from the previous connection counts on this one")
	}
	c.settleSuback(&packet.Suback{PacketID: 42, ReasonCodes: []packet.ReasonCode{packet.GrantedQoS1}})
	if c.Recovered() {
		t.Fatal("a SUBACK from a dead connection granted a filter on the live one")
	}
	// The same identifier, sent on this connection, does grant it.
	c.mu.Lock()
	c.awaiting = []pendingSub{{id: 42, idx: 0, gen: 7}}
	c.mu.Unlock()
	c.settleSuback(&packet.Suback{PacketID: 42, ReasonCodes: []packet.ReasonCode{packet.GrantedQoS1}})
	if !c.Recovered() {
		t.Fatal("a SUBACK on the live connection did not grant its filter")
	}
}

// A SUBACK granting less than was asked for is a grant, and is counted.
func TestDowngradeIsGrantedAndCounted(t *testing.T) {
	c := NewClient("rec-7", []string{"127.0.0.1:1"}, 60, 7)
	c.online.Store(true)
	c.gen.Store(1)
	c.subs = []packet.Subscription{{Filter: "rec/7", QoS: 2}}
	c.grantedIn = []uint64{0}
	c.awaiting = []pendingSub{{id: 9, idx: 0, gen: 1}}
	c.settleSuback(&packet.Suback{PacketID: 9, ReasonCodes: []packet.ReasonCode{packet.GrantedQoS1}})
	if !c.Recovered() {
		t.Fatal("a downgrade is a grant and the client is recovered")
	}
	if got := c.SubDowngraded.Load(); got != 1 {
		t.Fatalf("SubDowngraded %d, want 1", got)
	}
}

// A client between connections is not recovered, whatever its grants say.
func TestOfflineIsNotRecovered(t *testing.T) {
	c := NewClient("rec-8", []string{"127.0.0.1:1"}, 60, 8)
	c.online.Store(true)
	c.gen.Store(1)
	c.subs = []packet.Subscription{{Filter: "rec/8", QoS: 1}}
	c.grantedIn = []uint64{1}
	c.refreshRecoveredLocked()
	if !c.Recovered() {
		t.Fatal("online with its filter granted and not recovered")
	}
	c.online.Store(false)
	if c.Recovered() {
		t.Fatal("offline and counted as recovered")
	}
}
