package sim

import (
	"context"
	"testing"
	"time"
)

// queueAndDrainQoS0 queues one QoS 0 message (seq 1 from "q0-pub") for the
// offline durable subscriber "q0-sub" on node n, reconnects the subscriber
// there so the queue is drained to it, and disconnects it again.
func queueAndDrainQoS0(t *testing.T, ctx context.Context, n *Node, topic string) {
	t.Helper()
	setup := NewClient("q0-sub", []string{n.TCP}, 600, 1)
	if err := setup.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := setup.Subscribe(ctx, topic, 0); err != nil {
		t.Fatal(err)
	}
	setup.Close()
	waitUntil(t, "the subscriber recorded offline", 5*time.Second, func() bool {
		rec, ok := n.Server().Sessions().Record("q0-sub")
		return ok && !rec.Online
	})
	pub := NewClient("q0-pub", []string{n.TCP}, 600, 2)
	if err := pub.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := pub.Publish(ctx, topic, 0, 1); err != nil {
		t.Fatal(err)
	}
	pub.Close()
	waitUntil(t, "precondition: the QoS 0 message queued for the offline subscriber", 5*time.Second, func() bool {
		rec, ok := n.Server().Sessions().Record("q0-sub")
		return ok && rec.Queued == 1
	})
	sub := NewClient("q0-sub", []string{n.TCP}, 600, 3)
	if err := sub.Run(ctx); err != nil {
		t.Fatal(err)
	}
	var got []Delivery
	waitUntil(t, "precondition: the queued message sent to the reconnected subscriber", 5*time.Second, func() bool {
		got = append(got, sub.TakeDeliveries()...)
		return countSeq(got, "q0-pub", 1) >= 1
	})
	waitUntil(t, "precondition: the queue empty after the drain", 5*time.Second, func() bool {
		rec, ok := n.Server().Sessions().Record("q0-sub")
		return ok && rec.Queued == 0
	})
	sub.Close()
	waitUntil(t, "the subscriber recorded offline again", 5*time.Second, func() bool {
		rec, ok := n.Server().Sessions().Record("q0-sub")
		return ok && !rec.Online
	})
}

func countSeq(ds []Delivery, publisher string, seq uint64) int {
	n := 0
	for _, d := range ds {
		if d.Publisher == publisher && d.Seq == seq {
			n++
		}
	}
	return n
}

// reconnectAndCount reconnects "q0-sub" to node n, publishes seq 2 at QoS 0
// to it there, and returns how many times it was given seq 1 by the time seq
// 2 arrived: a queued seq 1 would be sent on the reconnect, ahead of seq 2.
func reconnectAndCount(t *testing.T, ctx context.Context, n *Node, topic string) int {
	t.Helper()
	sub := NewClient("q0-sub", []string{n.TCP}, 600, 4)
	if err := sub.Run(ctx); err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	present := false
	for _, e := range sub.TakeEvents() {
		if e.Kind == "connack" {
			present = e.Present
		}
	}
	if !present {
		t.Fatal("precondition: the subscriber's session was not present on the reconnect")
	}
	pub := NewClient("q0-pub", []string{n.TCP}, 600, 5)
	if err := pub.Run(ctx); err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	var got []Delivery
	waitUntil(t, "the subscriber given seq 2 after the reconnect", 10*time.Second, func() bool {
		if _, err := pub.Publish(ctx, topic, 0, 2); err != nil {
			t.Fatal(err)
		}
		got = append(got, sub.TakeDeliveries()...)
		return countSeq(got, "q0-pub", 2) >= 1
	})
	return countSeq(got, "q0-pub", 1)
}

// TestQoS0SentFromTheQueueIsNotDeliveredAgain: a QoS 0 message sent from a
// durable session's queue is not given to the client again after its node
// crashes and restarts, nor after another node takes the session over from a
// replica. QoS 0 is at most once (WI 1831).
func TestQoS0SentFromTheQueueIsNotDeliveredAgain(t *testing.T) {
	t.Run("a crash and restart", func(t *testing.T) {
		const topic = "q0/restart"
		c := New(t, Options{Nodes: 1, Log: halfLog(t, "q0-restart")})
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		n := c.Nodes[0]
		queueAndDrainQoS0(t, ctx, n, topic)
		c.Crash(n)
		c.Start(n)
		c.WaitAccepting(30 * time.Second)
		if again := reconnectAndCount(t, ctx, n, topic); again != 0 {
			t.Fatalf("the QoS 0 message sent from the queue before the crash was delivered %d more times after the restart", again)
		}
	})
	t.Run("a takeover from a replica", func(t *testing.T) {
		const topic = "q0/takeover"
		c := New(t, Options{Log: halfLog(t, "q0-takeover")})
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
		queueAndDrainQoS0(t, ctx, victim, topic)
		waitUntil(t, "precondition: a replica of the session on another node", 5*time.Second, func() bool {
			for _, n := range c.Nodes {
				if n != victim && n.Server().Replication().ReplicaState().Find("default", "q0-sub") != nil {
					return true
				}
			}
			return false
		})
		c.Crash(victim)
		waitUntil(t, "the leader adopting the subscriber's lease", 30*time.Second, func() bool {
			l, ok := leader.Server().Leases().Table().Lookup("default", "q0-sub")
			return ok && l.Owner == leader.Name && !l.Online
		})
		if again := reconnectAndCount(t, ctx, leader, topic); again != 0 {
			t.Fatalf("the QoS 0 message sent from the queue before the crash was delivered %d more times after the takeover", again)
		}
	})
}
