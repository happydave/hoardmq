package sim

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestCompletedMessageIsNotGivenAgainByALateForward: a publish on node A is
// routed into an online subscriber's session there and forwarded to node B
// for B's own subscriber. B's application of that forward is held while the
// subscriber completes the message on A, disconnects, and connects to B,
// which takes the session over from A no longer holding the message. When
// the forward is applied the subscriber must not be given the message a
// second time (WI 1832).
func TestCompletedMessageIsNotGivenAgainByALateForward(t *testing.T) {
	for _, qos := range []byte{0, 1, 2} {
		t.Run(fmt.Sprintf("QoS %d", qos), func(t *testing.T) { runCompletedLateForward(t, qos) })
	}
}

func runCompletedLateForward(t *testing.T, qos byte) {
	topic := fmt.Sprintf("done/q%d", qos)
	var hold atomic.Bool
	var a, b *Node
	opened := make(chan struct{})
	release := sync.OnceFunc(func() { close(opened) })
	t.Cleanup(release)
	held := make(chan struct{}, 1)
	gate := func(node, peer string) {
		if !hold.Load() || a == nil || b == nil || node != b.Name || peer != a.Name {
			return
		}
		select {
		case held <- struct{}{}:
		default:
		}
		<-opened
	}
	c := New(t, Options{ForwardApplyGate: gate, Log: halfLog(t, fmt.Sprintf("completedforward-q%d", qos))})
	a, b = c.Nodes[0], c.Nodes[1]
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	first := NewClient("done-sub", []string{a.TCP}, 600, 1)
	if err := first.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := first.Subscribe(ctx, topic, qos); err != nil {
		t.Fatal(err)
	}
	other := NewClient("done-other", []string{b.TCP}, 600, 2)
	if err := other.Run(ctx); err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if err := other.Subscribe(ctx, topic, qos); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "A routing the topic to B", 5*time.Second, func() bool {
		nodes, _ := a.Server().Routes().Table().Match("default", topic)
		return slices.Contains(nodes, b.Name)
	})
	pub := NewClient("done-pub", []string{a.TCP}, 600, 3)
	if err := pub.Run(ctx); err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	hold.Store(true)
	go func() {
		pctx, pcancel := context.WithTimeout(ctx, 10*time.Second)
		defer pcancel()
		_, _ = pub.Publish(pctx, topic, qos, 1) // the forward to B is what is under test
	}()
	select {
	case <-held:
	case <-time.After(5 * time.Second):
		t.Fatal("precondition not reached: B's application of A's forward is not held")
	}
	var got []Delivery
	count := func(cl *Client) int {
		got = append(got, cl.TakeDeliveries()...)
		return countSeq(got, "done-pub", 1)
	}
	waitUntil(t, "precondition: the subscriber given the message on A", 5*time.Second, func() bool {
		return count(first) >= 1
	})
	waitUntil(t, "precondition: the subscriber completed the message on A", 5*time.Second, func() bool {
		rec, ok := a.Server().Sessions().Record("done-sub")
		return ok && rec.InFlight == 0 && rec.Queued == 0
	})
	first.Close()
	waitUntil(t, "the subscriber recorded offline on A", 5*time.Second, func() bool {
		rec, ok := a.Server().Sessions().Record("done-sub")
		return ok && !rec.Online
	})

	sub := NewClient("done-sub", []string{b.TCP}, 600, 4)
	if err := sub.Run(ctx); err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	waitUntil(t, "precondition: the session installed on B", 10*time.Second, func() bool {
		rec, ok := b.Server().Sessions().Record("done-sub")
		return ok && rec.Online
	})

	skipped := func() int64 { return b.Server().Sessions().DuplicatesSkipped() }
	before := skipped()
	release()
	waitUntil(t, "B applying A's forward", 10*time.Second, func() bool {
		return skipped() > before || count(sub) >= 2
	})
	// The held frame was the only other copy; once it is applied nothing
	// else can add one, so the count is read once more and judged.
	if n := count(sub); n != 1 {
		t.Fatalf("the subscriber received the message %d times: %+v", n, got)
	}
	if n := skipped() - before; n != 1 {
		t.Fatalf("B skipped %d duplicate deliveries, want 1: the held forward did not reach the moved session", n)
	}
}
