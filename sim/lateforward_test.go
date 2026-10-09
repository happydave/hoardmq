package sim

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestHandedOverSessionIsNotGivenALateForwardAgain: a publish on node A is
// routed into a subscriber's session there and forwarded to node B for B's
// own subscriber. B's application of that forward is held while the
// subscriber connects to B, which takes the session over from A holding the
// message. When the forward is applied the subscriber must not be given the
// message a second time (WI 1521).
func TestHandedOverSessionIsNotGivenALateForwardAgain(t *testing.T) {
	const topic = "late/t"
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
	c := New(t, Options{ForwardApplyGate: gate, Log: halfLog(t, "lateforward")})
	a, b = c.Nodes[0], c.Nodes[1]
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	setup := NewClient("late-sub", []string{a.TCP}, 600, 1)
	if err := setup.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := setup.Subscribe(ctx, topic, 1); err != nil {
		t.Fatal(err)
	}
	setup.Close()
	waitUntil(t, "the subscriber recorded offline on A", 5*time.Second, func() bool {
		rec, ok := a.Server().Sessions().Record("late-sub")
		return ok && !rec.Online
	})
	other := NewClient("late-other", []string{b.TCP}, 600, 2)
	if err := other.Run(ctx); err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if err := other.Subscribe(ctx, topic, 1); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "A routing the topic to B", 5*time.Second, func() bool {
		nodes, _ := a.Server().Routes().Table().Match("default", topic)
		return slices.Contains(nodes, b.Name)
	})
	pub := NewClient("late-pub", []string{a.TCP}, 600, 3)
	if err := pub.Run(ctx); err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	hold.Store(true)
	go func() {
		pctx, pcancel := context.WithTimeout(ctx, 10*time.Second)
		defer pcancel()
		_, _ = pub.Publish(pctx, topic, 1, 1) // refused or not: the forward to B is what is under test
	}()
	waitUntil(t, "precondition: the message queued for the offline subscriber on A", 5*time.Second, func() bool {
		rec, ok := a.Server().Sessions().Record("late-sub")
		return ok && rec.Queued == 1
	})
	select {
	case <-held:
	case <-time.After(5 * time.Second):
		t.Fatal("precondition not reached: B's application of A's forward is not held")
	}

	sub := NewClient("late-sub", []string{b.TCP}, 600, 4)
	if err := sub.Run(ctx); err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	var got []Delivery
	count := func() int {
		got = append(got, sub.TakeDeliveries()...)
		n := 0
		for _, d := range got {
			if d.Publisher == "late-pub" && d.Seq == 1 {
				n++
			}
		}
		return n
	}
	waitUntil(t, "precondition: the subscriber given the message on B while A's forward is held", 10*time.Second, func() bool {
		return count() >= 1
	})

	release()
	skipped := func() int64 { return b.Server().Sessions().DuplicatesSkipped() }
	waitUntil(t, "B applying A's forward", 10*time.Second, func() bool {
		return skipped() >= 1 || count() >= 2
	})
	// The held frame was the only other copy; once it is applied nothing
	// else can add one, so the count is read once more and judged.
	if n := count(); n != 1 {
		t.Fatalf("the subscriber received the message %d times: %+v", n, got)
	}
	if n := skipped(); n != 1 {
		t.Fatalf("B skipped %d duplicate deliveries, want 1: the held forward did not reach the installed session", n)
	}
}
