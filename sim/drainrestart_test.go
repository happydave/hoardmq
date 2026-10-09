package sim

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/broker"
	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/packet"
)

// drainRestartOutcome is what a durable subscriber found when it came back
// after the only node holding its session drained and restarted.
type drainRestartOutcome struct {
	present          bool  // the first accepted CONNACK's Session Present
	received         int   // of the five queued messages, how many reached it
	servedEmpty      int   // served-empty sessions counted across the running nodes
	sealedByRecovery int64 // sealed_by_recovery on the restarted node
	sealedAtStop     bool  // the drained node had sealed the session (the reclaim) before it stopped
}

// runDrainRestart holds a session on one node only (one replica) with five
// QoS 1 messages queued, drains that node while every takeover request to it
// is dropped, so its receiver cannot pull the session before it stops, then
// restarts it and reconnects the subscriber to the receiver. The ttl is long
// enough that the restart comes well before the leader could declare the
// drained node dead. The confirmations travel on the dropped class too, so
// the drain ends at confirmTimeout; the deferred reclaim seals the session
// twice the takeover timeout (2 s) after the hand-off, so a confirmTimeout
// past that stops the node with the session sealed.
func runDrainRestart(t *testing.T, defects broker.Defects, confirmTimeout time.Duration) drainRestartOutcome {
	t.Helper()
	const ttl = 6 * time.Second
	c := New(t, Options{Replicas: 1, TTL: ttl, DrainConfirmTimeout: confirmTimeout, Defects: defects, Log: halfLog(t, "drainrestart-"+t.Name())})
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	leaderName, ok := agreedLeader(leaderViews(c, nil))
	if !ok {
		t.Fatal("no agreed leader")
	}
	receiver := c.Node(leaderName)
	var victim *Node
	for _, n := range c.Nodes {
		if n != receiver {
			victim = n
			break
		}
	}
	const id, topic = "drain-sub", "drain/t"

	setup := NewClient(id, []string{victim.TCP}, 600, 1)
	if err := setup.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := setup.Subscribe(ctx, topic, 1); err != nil {
		t.Fatal(err)
	}
	setup.Close()
	waitUntil(t, "the subscriber recorded offline on its node", 5*time.Second, func() bool {
		rec, ok := victim.Server().Sessions().Record(id)
		return ok && !rec.Online
	})
	waitUntil(t, "the route on the receiver", 5*time.Second, func() bool {
		nodes, _ := receiver.Server().Routes().Table().Match("default", topic)
		return len(nodes) == 1
	})
	pub := NewClient("drain-pub", []string{receiver.TCP}, 600, 2)
	if err := pub.Run(ctx); err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 5; i++ {
		code, err := publishWithRetry(ctx, pub, topic, 1, i, 5)
		if err != nil || code.IsError() {
			t.Fatalf("publish %d: %s %v", i, code, err)
		}
	}
	pub.Close()

	for _, n := range c.Nodes {
		if n != victim {
			c.Fault(n, victim, &transport.Fault{Drop: 1, Classes: []transport.Class{transport.TakeoverRequest}, Rand: rand.New(rand.NewSource(1))}) //nolint:gosec // seeded simulation
		}
	}
	drained := victim.Server()
	drainStart := time.Now()
	c.Drain(victim)
	if l, ok := receiver.Server().Leases().Table().Lookup("default", id); !ok || l.Owner != receiver.Name {
		t.Fatalf("precondition not reached: the receiver's table shows %s's lease as %+v, %v", id, l, ok)
	}
	if _, held := receiver.Server().Sessions().Record(id); held {
		t.Fatal("precondition not reached: the receiver holds the session although no pull could reach the drained node")
	}
	if served := drained.Leases().LivenessStats().TakeoversServed; served != 0 {
		t.Fatalf("precondition not reached: the drained node served %d takeover passes before it stopped", served)
	}

	if !receiver.Server().Leases().Table().Live(victim.Name) {
		t.Fatal("precondition not reached: the drained node was declared dead before it restarted")
	}
	_, _, sealedAtStop := drained.Sessions().Sealed("default", id)

	c.HealAll()
	c.Start(victim)
	sub := NewClient(id, []string{receiver.TCP}, 600, 3)
	if err := sub.Run(ctx); err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	// The leader declares a node dead no sooner than a ttl after its last alive
	// record, which came before the drain: a subscriber accepted within a ttl
	// of the drain's start was judged against an undeclared owner.
	if waited := time.Since(drainStart); waited >= ttl {
		t.Fatalf("precondition not reached: the subscriber was accepted %v after the drain began, long enough for the drained node to have been declared dead", waited)
	}
	out := drainRestartOutcome{sealedAtStop: sealedAtStop}
	for _, e := range sub.TakeEvents() {
		if e.Kind == "connack" && e.Code == packet.Success {
			out.present = e.Present
			break
		}
	}
	seen := map[uint64]bool{}
	deadline := time.Now().Add(10 * time.Second)
	for len(seen) < 5 && time.Now().Before(deadline) {
		for _, d := range sub.TakeDeliveries() {
			if d.Publisher == "drain-pub" {
				seen[d.Seq] = true
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	out.received = len(seen)
	for _, byReason := range servedEmptyByNode(c) {
		for _, n := range byReason {
			out.servedEmpty += n
		}
	}
	out.sealedByRecovery = victim.Server().Leases().LivenessStats().SealedByRecovery
	return out
}

// TestDrainedHolderRestartingKeepsTheSession: the drained node's journal is the
// only copy, and its receiver could not fetch it before the node stopped. The
// restarted node's recovery seals the session rather than deleting it, so the
// receiver's next fetch finds it and the subscriber gets its session and its
// five messages (WI 1742).
func TestDrainedHolderRestartingKeepsTheSession(t *testing.T) {
	out := runDrainRestart(t, broker.Defects{}, 2*time.Second)
	if !out.present || out.received != 5 || out.servedEmpty != 0 {
		t.Fatalf("Session Present %v, received %d of 5, served empty %d; want the session and every message", out.present, out.received, out.servedEmpty)
	}
	if out.sealedByRecovery < 1 {
		t.Fatalf("sealed_by_recovery %d on the restarted node; the session reached the receiver another way than the scenario is for", out.sealedByRecovery)
	}
}

// TestDrainedHolderRestartingLosesTheSessionWhenRecoveryEvicts is the control:
// with recovery deleting the session, as it did before the seal, the same
// scenario serves the subscriber a fresh session.
func TestDrainedHolderRestartingLosesTheSessionWhenRecoveryEvicts(t *testing.T) {
	out := runDrainRestart(t, broker.Defects{RemoveOnHandOver: true}, 2*time.Second)
	if out.present {
		t.Fatal("the subscriber kept its session with recovery evicting: the scenario no longer reaches the loss it is the control for")
	}
}

// TestDrainedHolderSealedAtStopRestartingKeepsTheSession: the drain waits
// past the deferred reclaim, which seals the session, and ends at its
// confirmation timeout; the node stops with the session sealed. Its final
// checkpoint keeps the sealed copy, so the restarted node still has it for
// the receiver (WI 1763).
func TestDrainedHolderSealedAtStopRestartingKeepsTheSession(t *testing.T) {
	out := runDrainRestart(t, broker.Defects{}, 3*time.Second)
	if !out.sealedAtStop {
		t.Fatal("precondition not reached: the drained node had not sealed the session when it stopped")
	}
	if !out.present || out.received != 5 || out.servedEmpty != 0 {
		t.Fatalf("Session Present %v, received %d of 5, served empty %d; want the session and every message", out.present, out.received, out.servedEmpty)
	}
}
