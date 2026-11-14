package sim

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestDriverQueueStep runs the queue exchange through the driver against a
// three-node cluster: sessions taken offline keep their session, every
// message published to them is acknowledged, and on resume each session
// receives every message queued to it, with no session lost. The failover
// exchange then continues to mark.
func TestDriverQueueStep(t *testing.T) {
	const clients, sessions, depth = 120, 12, 5
	c := New(t, Options{})
	d := startPipedDriver(t, c, clients, testDriverOptions())
	t.Cleanup(d.pop.close)
	if _, err := d.pop.hello(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var completed atomic.Int64
	if failed, err := d.pop.fill(ctx, &completed); err != nil || failed != 0 {
		t.Fatalf("fill: %d failed, %v", failed, err)
	}

	off, err := d.pop.queue(ctx, queueRequest{Sessions: sessions, Depth: depth, PayloadBytes: 64})
	if err != nil {
		t.Fatal(err)
	}
	if off.Offline != sessions {
		t.Fatalf("queue: %d offline, want %d", off.Offline, sessions)
	}
	q, err := d.pop.publish(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if q.Offline != sessions || q.Published != sessions*depth || q.Acked["Success"] != sessions*depth {
		t.Fatalf("queue: offline %d, published %d, acknowledged %v; want %d, %d, all Success", q.Offline, q.Published, q.Acked, sessions, sessions*depth)
	}
	r, err := d.pop.resume(ctx, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if r.Reconnected != sessions || r.SessionLost != 0 || r.Complete != sessions || r.Received < sessions*depth {
		t.Fatalf("resume: reconnected %d, lost %d, complete %d, received %d; want %d, 0, %d, at least %d",
			r.Reconnected, r.SessionLost, r.Complete, r.Received, sessions, sessions, sessions*depth)
	}
	if err := d.pop.mark(); err != nil {
		t.Fatalf("mark after the queue step: %v", err)
	}
}

// TestDriverQueueStepOnce refuses a second queue: the exchange allows one
// queue, its publish and its resume between fill and mark, in that order.
func TestDriverQueueStepOnce(t *testing.T) {
	const clients = 30
	c := New(t, Options{})
	d := startPipedDriver(t, c, clients, testDriverOptions())
	t.Cleanup(d.pop.close)
	if _, err := d.pop.hello(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var completed atomic.Int64
	if failed, err := d.pop.fill(ctx, &completed); err != nil || failed != 0 {
		t.Fatalf("fill: %d failed, %v", failed, err)
	}
	if _, err := d.pop.queue(ctx, queueRequest{Sessions: 3, Depth: 1, PayloadBytes: 32}); err != nil {
		t.Fatal(err)
	}
	_, err := d.pop.queue(ctx, queueRequest{Sessions: 3, Depth: 1, PayloadBytes: 32})
	if err == nil || !strings.Contains(err.Error(), "unexpected message \"queue\" while waiting for \"publish\"") {
		t.Fatalf("a second queue: %v, want a refusal naming it", err)
	}
	select {
	case code := <-d.code:
		if code != driverProtocol {
			t.Fatalf("driver exit %d, want %d", code, driverProtocol)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the driver did not exit after the refusal")
	}
}

// TestDriverQueueAfterMarkRefused: the queue step comes between fill and mark
// or not at all, so a queue sent after mark is refused by name and ends the
// driver.
func TestDriverQueueAfterMarkRefused(t *testing.T) {
	const clients = 30
	c := New(t, Options{})
	d := startPipedDriver(t, c, clients, testDriverOptions())
	t.Cleanup(d.pop.close)
	if _, err := d.pop.hello(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var completed atomic.Int64
	if failed, err := d.pop.fill(ctx, &completed); err != nil || failed != 0 {
		t.Fatalf("fill: %d failed, %v", failed, err)
	}
	if err := d.pop.mark(); err != nil {
		t.Fatal(err)
	}
	if err := d.pop.send(driverMsg{Type: msgQueue, Queue: &queueRequest{Sessions: 3, Depth: 1, PayloadBytes: 32}}); err != nil {
		t.Fatal(err)
	}
	_, err := d.pop.await(msgQueued, 30*time.Second)
	if err == nil || !strings.Contains(err.Error(), `unexpected message "queue" while waiting for "crashed"`) {
		t.Fatalf("a queue after mark: %v, want a refusal naming it", err)
	}
	select {
	case code := <-d.code:
		if code != driverProtocol {
			t.Fatalf("driver exit %d, want %d", code, driverProtocol)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the driver did not exit after the refusal")
	}
}

// TestHeldFilter checks Hold's two outcomes against a broker: with the
// session present the client reads recovered with nothing resubscribed; with
// no session the CONNACK's absence is counted as a lost session.
func TestHeldFilter(t *testing.T) {
	c := New(t, Options{Nodes: 1})
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	first := NewClient("held-a", c.Addresses(), 600, 1)
	if err := first.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := first.Subscribe(ctx, "f/held-a", 1); err != nil {
		t.Fatal(err)
	}
	first.Close()

	present := NewClient("held-a", c.Addresses(), 600, 2)
	present.Hold("f/held-a", 1)
	if err := present.Run(ctx); err != nil {
		t.Fatal(err)
	}
	defer present.Close()
	waitUntil(t, "the held client recovered", 10*time.Second, present.Recovered)
	if n := present.SessionLost.Load(); n != 0 {
		t.Fatalf("session present: lost %d, want 0", n)
	}

	absent := NewClient("held-b", c.Addresses(), 600, 3)
	absent.Hold("f/held-b", 1)
	if err := absent.Run(ctx); err != nil {
		t.Fatal(err)
	}
	defer absent.Close()
	waitUntil(t, "a lost session counted", 10*time.Second, func() bool { return absent.SessionLost.Load() >= 1 })
	if n := absent.SessionLost.Load(); n != 1 {
		t.Fatalf("no session: lost %d, want 1", n)
	}
}

// TestResumedClientsHoldTheirFilter checks that a resumed session's client
// holds its subscription, so the storm's recovered reading for it is not
// vacuous and a session lost on reconnect is counted.
func TestResumedClientsHoldTheirFilter(t *testing.T) {
	const clients = 30
	c := New(t, Options{})
	p := newLocalPopulation(clients, c.Addresses(), nil, nil)
	t.Cleanup(p.close)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var completed atomic.Int64
	if failed, err := p.fill(ctx, &completed); err != nil || failed != 0 {
		t.Fatalf("fill: %d failed, %v", failed, err)
	}
	if _, err := p.queue(ctx, queueRequest{Sessions: 3, Depth: 1, PayloadBytes: 32}); err != nil {
		t.Fatal(err)
	}
	offline := append([]int(nil), p.offline...)
	if _, err := p.publish(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := p.resume(ctx, 30*time.Second); err != nil {
		t.Fatal(err)
	}
	for _, i := range offline {
		cl := p.clientsSnapshot()[i]
		cl.mu.Lock()
		subs := append([]string(nil), func() []string {
			out := make([]string, 0, len(cl.subs))
			for _, s := range cl.subs {
				out = append(out, s.Filter)
			}
			return out
		}()...)
		cl.mu.Unlock()
		if len(subs) != 1 || subs[0] != "f/"+cl.ID {
			t.Errorf("resumed client %s holds %v, want [f/%s]", cl.ID, subs, cl.ID)
		}
	}
}
