package routing

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/dispatch"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

// gatedSink holds its first two durability waits, each until released.
type gatedSink struct {
	*fakeSink
	nowhere string // a topic no local session holds
	mu      sync.Mutex
	calls   int
	entered [2]chan struct{}
	release [2]chan struct{}
}

func newGatedSink(inner *fakeSink, nowhere string) *gatedSink {
	g := &gatedSink{fakeSink: inner, nowhere: nowhere}
	for i := range g.entered {
		g.entered[i] = make(chan struct{})
		g.release[i] = make(chan struct{})
	}
	return g
}

func (g *gatedSink) InjectForwarded(m *message.Message) (int, []store.Scope, error) {
	if m.Topic == g.nowhere {
		return 0, nil, nil
	}
	return g.fakeSink.InjectForwarded(m)
}

func (g *gatedSink) Durable(ctx context.Context, keys []store.Scope) error {
	g.mu.Lock()
	i := g.calls
	g.calls++
	g.mu.Unlock()
	if i < len(g.entered) {
		close(g.entered[i])
		<-g.release[i]
	}
	return g.fakeSink.Durable(ctx, keys)
}

// TestRefusedForwardDoesNotAcknowledgeItsBatch: b delivers a forward to t/1 and
// refuses one to t/2 in the same batch, answering the refusal before the batch's
// durability wait. Until that wait returns, the forward to t/1 is neither
// acknowledged nor failed by the refusal; once it returns, it passes.
func TestRefusedForwardDoesNotAcknowledgeItsBatch(t *testing.T) {
	c := newCluster(t, []string{"a", "b"}, dispatch.RoundRobin)
	a, b := c.nodes["a"], c.nodes["b"]
	sink := newGatedSink(b.sink, "t/2")
	b.plane.SetSink(sink)
	releases := []func(){sync.OnceFunc(func() { close(sink.release[0]) }), sync.OnceFunc(func() { close(sink.release[1]) })}
	for _, r := range releases {
		t.Cleanup(r)
	}
	b.plane.Observe("default", "t/1", 1)
	b.plane.Observe("default", "t/2", 1)
	waitFor(t, "routes", 5*time.Second, func() bool {
		n1, _ := a.plane.Table().Match("default", "t/1")
		n2, _ := a.plane.Table().Match("default", "t/2")
		return len(n1) == 1 && len(n2) == 1
	})

	first := publish(a, "p", "t/1", "first", 1)
	select {
	case <-sink.entered[0]:
	case <-time.After(5 * time.Second):
		t.Fatal("precondition not reached: b never began its first durability wait")
	}
	stays := publish(a, "p", "t/1", "stays", 2)
	refused := publish(a, "p", "t/2", "refused", 3)
	if len(stays.Forwards) != 1 || len(refused.Forwards) != 1 {
		t.Fatalf("precondition not reached: forwards %v and %v", stays.Forwards, refused.Forwards)
	}
	waitFor(t, "both forwards queued behind the held wait", 5*time.Second, func() bool {
		b.plane.mu.Lock()
		in := b.plane.in["a"]
		b.plane.mu.Unlock()
		if in == nil {
			return false
		}
		in.mu.Lock()
		defer in.mu.Unlock()
		return len(in.queue) == 2
	})
	releases[0]()
	select {
	case <-sink.entered[1]:
	case <-time.After(5 * time.Second):
		t.Fatal("precondition not reached: b never began the second batch's durability wait")
	}
	staysSeq, refusedSeq := stays.Forwards[0].Seq, refused.Forwards[0].Seq
	waitFor(t, "b's refusal recorded on a", 5*time.Second, func() bool {
		a.plane.mu.Lock()
		defer a.plane.mu.Unlock()
		o := a.plane.out["b"]
		if o == nil {
			return false
		}
		_, ok := o.refused[refusedSeq]
		return ok
	})
	a.plane.mu.Lock()
	o := a.plane.out["b"]
	_, staysRefused := o.refused[staysSeq]
	verdict := forwardVerdict(staysSeq, o.lastFailed, o.acked, staysRefused, false)
	acked := o.acked
	a.plane.mu.Unlock()
	switch verdict {
	case forwardPass:
		t.Errorf("the forward b delivered beside the refused one passed (acked %d, its sequence %d) while b's durability wait is held", acked, staysSeq)
	case forwardWait:
	default:
		t.Errorf("the forward b delivered beside the refused one was failed with it (verdict %d)", verdict)
	}

	releases[1]()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := a.plane.Barrier(ctx, stays.Forwards); err != nil {
		t.Errorf("the forward b delivered, once its batch is durable: %v", err)
	}
	if err := a.plane.Barrier(ctx, refused.Forwards); err == nil {
		t.Error("the forward b refused passed")
	}
	if err := a.plane.Barrier(ctx, first.Forwards); err != nil {
		t.Errorf("the first batch's forward: %v", err)
	}
}
