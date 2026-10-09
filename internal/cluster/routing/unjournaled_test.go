package routing

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/dispatch"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

// unjournaledSink is the fake sink whose delivery to one topic reaches a
// session and fails to journal there, with its first durability wait held so
// the forwards sent meanwhile are applied as one batch.
type unjournaledSink struct {
	*fakeSink
	failing string
	release chan struct{}
	entered chan struct{}
	once    sync.Once
}

func (u *unjournaledSink) InjectForwarded(m *message.Message) (int, []store.Scope, error) {
	n, into, err := u.fakeSink.InjectForwarded(m)
	if m.Topic == u.failing {
		return n, into, errors.New("injected: the delivery's record failed to append")
	}
	return n, into, err
}

func (u *unjournaledSink) Durable(ctx context.Context, keys []store.Scope) error {
	first := false
	u.once.Do(func() { first = true })
	if first {
		close(u.entered)
		<-u.release
	}
	return u.fakeSink.Durable(ctx, keys)
}

// TestForwardWhoseDeliveryFailedToJournalIsRefusedAlone: b delivers a forward
// to t/2 into a session whose record fails to append, so the publish that
// sent it must be refused; a forward applied in the same batch, whose record
// was journaled, is acknowledged.
func TestForwardWhoseDeliveryFailedToJournalIsRefusedAlone(t *testing.T) {
	metrics := newCountingMetrics()
	c := newClusterMetrics(t, []string{"a", "b"}, dispatch.RoundRobin, metrics)
	a, b := c.nodes["a"], c.nodes["b"]
	sink := &unjournaledSink{fakeSink: b.sink, failing: "t/2", release: make(chan struct{}), entered: make(chan struct{})}
	b.plane.SetSink(sink)
	var once sync.Once
	release := func() { once.Do(func() { close(sink.release) }) }
	t.Cleanup(release)
	b.plane.Observe("default", "t/1", 1)
	b.plane.Observe("default", "t/2", 1)
	waitFor(t, "routes", 5*time.Second, func() bool {
		n1, _ := a.plane.Table().Match("default", "t/1")
		n2, _ := a.plane.Table().Match("default", "t/2")
		return len(n1) == 1 && len(n2) == 1
	})
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
	journaled := publish(a, "p", "t/1", "journaled", 2)
	failed := publish(a, "p", "t/2", "failed", 3)
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

	err := barrier(failed)
	if !errors.Is(err, ErrRoutesUnavailable) || !strings.Contains(err.Error(), "refused the forward") {
		t.Fatalf("the forward whose delivery failed to journal: %v, want b's refusal of it", err)
	}
	if err := barrier(journaled); err != nil {
		t.Fatalf("a forward b journaled, in the same batch as the one that failed, was refused with it: %v", err)
	}
	if err := barrier(first); err != nil {
		t.Fatalf("the forward before the batch was refused: %v", err)
	}
	if got := metrics.count("hoardmq_forward_failures_total", "not-journaled"); got != 1 {
		t.Fatalf("b counted %v not-journaled refusals, want 1", got)
	}
}

// failingGroupSink is the fake sink whose shared-group deliveries fail to
// journal.
type failingGroupSink struct{ *fakeSink }

func (f failingGroupSink) InjectGroup(m *message.Message, group string) (store.Scope, bool, error) {
	s, ok, _ := f.fakeSink.InjectGroup(m, group)
	return s, ok, errors.New("injected: the member's record failed to append")
}

// TestGroupGiveToAThirdNodeIsCoveredByThePublish: a publish on a whose shared
// group's dispatch owner b gives the message to a member on cc that refuses
// it is refused; b's acknowledgement of the decide is not enough.
func TestGroupGiveToAThirdNodeIsCoveredByThePublish(t *testing.T) {
	// b's relay bound past the barrier's context: the refusal must be cc's,
	// not the bound's.
	c := newClusterOpts(t, []string{"a", "b", "cc"}, dispatch.RoundRobin, func(o *Options) { o.GiveTimeout = time.Minute })
	a, b, cc := c.nodes["a"], c.nodes["b"], c.nodes["cc"]
	cc.plane.SetSink(failingGroupSink{cc.sink})
	b.plane.Observe("default", "$share/g/t/#", 2) // b owns the group's dispatch
	cc.plane.Observe("default", "$share/g/t/#", 1)
	waitFor(t, "group routes", 5*time.Second, func() bool {
		_, groups := a.plane.Table().Match("default", "t/1")
		return len(groups) == 1 && len(groups[0].Members) == 2
	})
	barrier := func(m *message.Message) error {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		return a.plane.Barrier(ctx, m.Forwards)
	}
	// Round robin over [b cc] at b: the first goes to b's own member, the
	// second is given to cc.
	if err := barrier(publish(a, "p", "t/1", "to-b", 1)); err != nil {
		t.Fatalf("precondition: the publish b delivered itself was refused: %v", err)
	}
	given := publish(a, "p", "t/1", "to-cc", 2)
	err := barrier(given)
	waitFor(t, "cc received the give", 5*time.Second, func() bool {
		cc.sink.mu.Lock()
		defer cc.sink.mu.Unlock()
		return len(cc.sink.groups) == 1
	})
	if err == nil {
		t.Fatal("a publish whose group member on cc refused the give was acknowledged")
	}
}

// TestGroupGiveWhoseDeliveryFailedToJournalIsRefused: a publish on a, which
// owns its shared group's dispatch, given to cc's member whose record fails,
// is refused through the give a's barrier waits on.
func TestGroupGiveWhoseDeliveryFailedToJournalIsRefused(t *testing.T) {
	c := newCluster(t, []string{"a", "cc"}, dispatch.RoundRobin)
	a, cc := c.nodes["a"], c.nodes["cc"]
	cc.plane.SetSink(failingGroupSink{cc.sink})
	a.plane.Observe("default", "$share/g/t/#", 2)
	cc.plane.Observe("default", "$share/g/t/#", 1)
	waitFor(t, "group routes", 5*time.Second, func() bool {
		_, groups := a.plane.Table().Match("default", "t/1")
		return len(groups) == 1 && len(groups[0].Members) == 2
	})
	// Round robin over [a cc]: the first is a's own, the second given to cc.
	var given *message.Message
	for i := uint64(1); i <= 2 && given == nil; i++ {
		msg := &message.Message{Namespace: "default", Topic: "t/1", QoS: 1, Payload: []byte("g"), Publisher: message.Publisher{Namespace: "default", ClientID: "p", Node: "a"}, Seq: i, Arrived: time.Now()}
		if !a.plane.Decide("default", "g/t/#", msg) {
			given = msg
		}
	}
	if given == nil || len(given.Forwards) != 1 || given.Forwards[0].Peer != "cc" {
		t.Fatalf("precondition: no publish was given to cc: %+v", given)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err := a.plane.Barrier(ctx, given.Forwards)
	if !errors.Is(err, ErrRoutesUnavailable) || !strings.Contains(err.Error(), "refused the forward") {
		t.Fatalf("a give whose member's record failed to journal: %v, want cc's refusal of it", err)
	}
}

// TestProvenMoveDoesNotAcceptAForwardThatFailedToJournal: the new owner's
// forward, numbered from its mark, is accepted on the old node as proven
// (WI 1806) — unless the delivery it made there failed to journal.
func TestProvenMoveDoesNotAcceptAForwardThatFailedToJournal(t *testing.T) {
	m := newMoveTrio(t, time.Hour, true)
	m.newOwnerRoute()
	m.o.plane.Resolve("default", "s", "n", 10)
	m.acceptedFrom(m.n, 11, "precondition: the new owner's forward from its mark")
	failing := &unjournaledSink{fakeSink: m.o.sink, failing: m.filter, release: make(chan struct{}), entered: make(chan struct{})}
	close(failing.release) // no wait is held here
	m.o.plane.SetSink(failing)
	m.refusedFrom(m.n, 12, "the new owner's proven forward whose delivery failed to journal")
}
