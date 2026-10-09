package sim

import (
	"context"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/broker"
	"github.com/happydave/hoardmq/internal/packet"
)

// adoptionOutcome is what a durable subscriber saw when it reconnected to the
// node that had adopted its lease, before that node's adoption worker ran.
type adoptionOutcome struct {
	queued        int   // messages queued for it while it was away
	received      int   // of those, how many reached it
	disconnects   int   // connections it lost after reconnecting
	takenOver     int   // of those, with Session Taken Over
	evictedImport int64 // live connections the adopter's imports closed
	inline        int64 // adoptions the adopter settled inside a CONNECT
	present       bool  // the reconnect's Session Present
}

// runAdoptionScenario queues messages for an offline durable subscriber on a
// follower, crashes that follower, waits for the leader to adopt the lease, and
// reconnects the subscriber to the leader. The adoption worker is held back
// for three seconds, so the CONNECT reliably arrives while the adoption is
// unfinished: without that the scenario passes or fails by scheduling.
func runAdoptionScenario(t *testing.T, defects broker.Defects) adoptionOutcome {
	t.Helper()
	const delay = 3 * time.Second
	c := New(t, Options{AdoptionDelay: delay, Defects: defects, Log: halfLog(t, "adoption-"+t.Name())})
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	leaderName, ok := agreedLeader(leaderViews(c, nil))
	if !ok {
		t.Fatal("no agreed leader")
	}
	leader := c.Node(leaderName)
	var victim *Node
	for _, n := range c.Nodes {
		if n != leader {
			victim = n
			break
		}
	}

	setup := NewClient("adopt-sub", []string{victim.TCP}, 600, 1)
	if err := setup.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := setup.Subscribe(ctx, "adopt/t", 1); err != nil {
		t.Fatal(err)
	}
	setup.Close()
	waitUntil(t, "the subscriber recorded offline on its node", 5*time.Second, func() bool {
		rec, ok := victim.Server().Sessions().Record("adopt-sub")
		return ok && !rec.Online
	})
	waitUntil(t, "the route on the leader", 5*time.Second, func() bool {
		nodes, _ := leader.Server().Routes().Table().Match("default", "adopt/t")
		return len(nodes) == 1
	})

	pub := NewClient("adopt-pub", []string{leader.TCP}, 600, 2)
	if err := pub.Run(ctx); err != nil {
		t.Fatal(err)
	}
	out := adoptionOutcome{}
	for i := uint64(1); i <= 5; i++ {
		// A publish is refused while this node's stream to a peer is repairing
		// — the first frame to a peer can be lost as the cluster forms, and
		// the gap is only discovered when the next one arrives, after which
		// the resync takes a moment (WI 1570). These five are the scenario's
		// setup, not what it tests, so they are retried the way a client
		// would rather than failing the run.
		code, err := publishWithRetry(ctx, pub, "adopt/t", 1, i, 5)
		if err != nil || code.IsError() {
			t.Fatalf("publish %d: %s %v", i, code, err)
		}
		out.queued++
	}
	pub.Close()

	c.Crash(victim)
	waitUntil(t, "the leader adopting the subscriber's lease", 30*time.Second, func() bool {
		srv := leader.Server()
		if srv == nil || srv.Leases() == nil {
			return false
		}
		l, ok := srv.Leases().Table().Lookup("default", "adopt-sub")
		return ok && l.Owner == leader.Name && !l.Online
	})
	adoptedAt := time.Now()

	sub := NewClient("adopt-sub", []string{leader.TCP}, 600, 3)
	if err := sub.Run(ctx); err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	// Watch past the point where the held-back worker runs, so an eviction it
	// causes has happened before anything is counted.
	seen := map[uint64]bool{}
	first := true
	for time.Since(adoptedAt) < delay+4*time.Second {
		for _, d := range sub.TakeDeliveries() {
			if d.Publisher == "adopt-pub" {
				seen[d.Seq] = true
			}
		}
		for _, e := range sub.TakeEvents() {
			switch e.Kind {
			case "connack":
				if first && e.Code == packet.Success {
					out.present, first = e.Present, false
				}
			case "disconnect", "dropped":
				out.disconnects++
				if e.Code == packet.SessionTakenOver {
					out.takenOver++
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	out.received = len(seen)
	out.evictedImport = leader.Server().StormStats().ClosedEvictImport
	out.inline = leader.Server().Leases().LivenessStats().AdoptedInline
	return out
}

// publishWithRetry publishes until the broker takes it or the attempts run
// out, giving a refused publish a moment for whatever was repairing to
// finish. It returns the last code and error.
func publishWithRetry(ctx context.Context, c *Client, topic string, qos byte, seq uint64, attempts int) (packet.ReasonCode, error) {
	var code packet.ReasonCode
	var err error
	for i := 0; i < attempts; i++ {
		pctx, pcancel := context.WithTimeout(ctx, 10*time.Second)
		code, err = c.Publish(pctx, topic, qos, seq)
		pcancel()
		if err == nil && !code.IsError() {
			return code, nil
		}
		time.Sleep(250 * time.Millisecond)
	}
	return code, err
}

func waitUntil(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestReconnectToTheAdopterKeepsItsConnectionAndItsQueue: the client that
// reaches the adopting node before the adoption worker is given the adopted
// state inside its own handshake, so it is never disconnected by the import
// and it loses nothing that was queued for it.
func TestReconnectToTheAdopterKeepsItsConnectionAndItsQueue(t *testing.T) {
	got := runAdoptionScenario(t, broker.Defects{})
	if got.queued != 5 {
		t.Fatalf("queued %d, want 5: the scenario did not set up what it tests", got.queued)
	}
	if got.received != got.queued {
		t.Errorf("received %d of %d queued messages", got.received, got.queued)
	}
	if got.disconnects != 0 || got.evictedImport != 0 {
		t.Errorf("the reconnected client lost %d connections (%d taken over); imports closed %d live connections", got.disconnects, got.takenOver, got.evictedImport)
	}
	if !got.present {
		t.Error("the reconnect was told its session was absent")
	}
	if got.inline == 0 {
		t.Error("no adoption was settled inside a CONNECT: the scenario did not reach the path it tests")
	}
}

// TestDefectAdmitDuringAdoption is the scenario's negative control: with a
// CONNECT admitted while its adoption is unfinished, the import that follows
// closes the live connection.
func TestDefectAdmitDuringAdoption(t *testing.T) {
	got := runAdoptionScenario(t, broker.Defects{AdmitDuringAdoption: true})
	if got.takenOver == 0 || got.evictedImport == 0 {
		t.Fatalf("the defect went unnoticed: %+v", got)
	}
}
