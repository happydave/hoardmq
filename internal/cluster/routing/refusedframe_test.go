package routing

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/dispatch"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

// TestOneFrameRefusalEncoding: a refusal of one frame says so on the wire, and
// a failed acknowledgement from an older receiver decodes as covering every
// frame up to its sequence.
func TestOneFrameRefusalEncoding(t *testing.T) {
	body, err := json.Marshal(forwardEnvelope{Seq: 7, Failed: true, One: true})
	if err != nil {
		t.Fatal(err)
	}
	var env forwardEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatal(err)
	}
	if !env.Failed || !env.One || env.Seq != 7 {
		t.Fatalf("decoded %+v from %s", env, body)
	}
	var old forwardEnvelope
	if err := json.Unmarshal([]byte(`{"seq":7,"failed":true,"msg":null}`), &old); err != nil {
		t.Fatal(err)
	}
	if !old.Failed || old.One {
		t.Fatalf("an older receiver's failed ack decoded as %+v, want failed and not one frame", old)
	}
}

// TestForwardVerdict is the barrier's decision for one forward.
func TestForwardVerdict(t *testing.T) {
	cases := []struct {
		name                   string
		seq, lastFailed, acked uint64
		refused                bool
		want                   int
	}{
		{"at the failure mark", 5, 5, 9, false, forwardFailed},
		{"below the failure mark", 4, 5, 9, false, forwardFailed},
		{"refused, not yet acknowledged", 6, 5, 5, true, forwardRefused},
		{"refused, acknowledged past", 6, 5, 9, true, forwardRefused},
		{"acknowledged, not refused", 6, 5, 9, false, forwardPass},
		{"neither", 6, 5, 5, false, forwardWait},
	}
	for _, c := range cases {
		if got := forwardVerdict(c.seq, c.lastFailed, c.acked, c.refused, false); got != c.want {
			t.Errorf("%s: forwardVerdict = %d, want %d", c.name, got, c.want)
		}
	}
}

// TestRefusedForwardsAreBounded: the record of refused frames is bounded, and a
// frame pushed out of it is failed by the failure mark instead.
func TestRefusedForwardsAreBounded(t *testing.T) {
	o := &outbound{lastFailed: 0, acked: maxRefusedForwards + 10}
	for seq := uint64(1); seq <= maxRefusedForwards+1; seq++ {
		o.refuseOne(seq)
	}
	if _, kept := o.refused[1]; kept {
		t.Fatal("the oldest refusal was not pushed out")
	}
	if len(o.refused) != maxRefusedForwards || len(o.refusedOrder) != maxRefusedForwards {
		t.Fatalf("record holds %d/%d, want %d", len(o.refused), len(o.refusedOrder), maxRefusedForwards)
	}
	if o.lastFailed != 1 {
		t.Fatalf("failure mark %d, want 1: the forgotten refusal must be failed by the mark", o.lastFailed)
	}
	_, refused := o.refused[1]
	if got := forwardVerdict(1, o.lastFailed, o.acked, refused, false); got != forwardFailed {
		t.Fatalf("the forgotten refusal's forward: verdict %d, want it failed", got)
	}
}

// heldSink is the fake sink with its first durability wait held until
// released, so the forwards sent meanwhile are applied as one batch.
type heldSink struct {
	*fakeSink
	release chan struct{}
	entered chan struct{}
	once    sync.Once
	// nowhere is a topic no local session holds: a forward to it reaches none.
	nowhere string
}

func (h *heldSink) InjectForwarded(m *message.Message) (int, []store.Scope, error) {
	if m.Topic == h.nowhere {
		return 0, nil, nil
	}
	return h.fakeSink.InjectForwarded(m)
}

func (h *heldSink) Durable(ctx context.Context, keys []store.Scope) error {
	first := false
	h.once.Do(func() { first = true })
	if first {
		close(h.entered)
		<-h.release
	}
	return h.fakeSink.Durable(ctx, keys)
}

// TestARefusedForwardFailsOnlyItsOwnPublish: b refuses a forward to t/2,
// because a session on its filter is moving away or because it reaches no
// session there. A forward b applied in the same batch, to a filter nobody is
// leaving, is acknowledged once b's durability wait covers it, and its publish
// is not refused with the one b refused.
func TestARefusedForwardFailsOnlyItsOwnPublish(t *testing.T) {
	t.Run("session moving", func(t *testing.T) { refusedForwardAlone(t, false) })
	t.Run("no local subscriber", func(t *testing.T) { refusedForwardAlone(t, true) })
}

func refusedForwardAlone(t *testing.T, noSubscriber bool) {
	c := newCluster(t, []string{"a", "b"}, dispatch.RoundRobin)
	a, b := c.nodes["a"], c.nodes["b"]
	sink := &heldSink{fakeSink: b.sink, release: make(chan struct{}), entered: make(chan struct{})}
	if noSubscriber {
		sink.nowhere = "t/2"
	}
	b.plane.SetSink(sink)
	var once sync.Once
	release := func() { once.Do(func() { close(sink.release) }) }
	t.Cleanup(release)
	b.plane.Observe("default", "t/1", 1)
	b.plane.Observe("default", "t/2", 2)
	waitFor(t, "routes", 5*time.Second, func() bool {
		n1, _ := a.plane.Table().Match("default", "t/1")
		n2, _ := a.plane.Table().Match("default", "t/2")
		return len(n1) == 1 && len(n2) == 1
	})
	if !noSubscriber {
		// One of t/2's two sessions leaves b; the other stays, so b delivers
		// a forward to t/2 and refuses it as session-moving.
		b.plane.Moving("default", []string{"t/2"})
		b.plane.Observe("default", "t/2", 1)
	}

	barrier := func(m *message.Message) error {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		return a.plane.Barrier(ctx, m.Forwards)
	}
	first := publish(a, "p", "t/1", "first", 1)
	select {
	case <-sink.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("precondition not reached: b never began its first durability wait")
	}
	stays := publish(a, "p", "t/1", "stays", 2)
	moving := publish(a, "p", "t/2", "moving", 3)
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
	release()

	if err := barrier(stays); err != nil {
		t.Fatalf("a forward b delivered in the same batch as the one it refused was refused with it: %v", err)
	}
	err := barrier(moving)
	if !errors.Is(err, ErrRoutesUnavailable) || !strings.Contains(err.Error(), "refused the forward") {
		t.Fatalf("the forward b refused: %v, want its own refusal", err)
	}
	// Acknowledged before the refusal arrived, and judged after it: a
	// high-water mark refuses this one too.
	if err := barrier(first); err != nil {
		t.Fatalf("a forward acknowledged before the refusal was refused with it: %v", err)
	}
	want := 3
	if noSubscriber {
		want = 2
	}
	if got := b.sink.directCount(); got != want {
		t.Fatalf("b delivered %d forwards, want %d", got, want)
	}
}
