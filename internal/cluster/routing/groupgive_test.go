package routing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/dispatch"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

// TestForwardVerdictHoldsAPendingGive: a forward the receiver gave on waits
// for the give's outcome even once acknowledged; a failure or a refusal
// still decides first.
func TestForwardVerdictHoldsAPendingGive(t *testing.T) {
	for _, c := range []struct {
		lastFailed, acked uint64
		refused, pending  bool
		want              int
	}{
		{acked: 5, pending: true, want: forwardWait}, // acknowledged, not yet held by the member
		{acked: 5, want: forwardPass},
		{acked: 4, pending: true, want: forwardWait},
		{acked: 4, want: forwardWait},
		{acked: 5, refused: true, pending: true, want: forwardRefused},
		{acked: 5, refused: true, want: forwardRefused},
		{lastFailed: 5, acked: 5, pending: true, want: forwardFailed},
		{lastFailed: 5, acked: 5, refused: true, pending: true, want: forwardFailed},
	} {
		if got := forwardVerdict(5, c.lastFailed, c.acked, c.refused, c.pending); got != c.want {
			t.Errorf("forwardVerdict(5, lastFailed %d, acked %d, refused %v, pending %v) = %d, want %d",
				c.lastFailed, c.acked, c.refused, c.pending, got, c.want)
		}
	}
}

// oneStream is a plane with a stream to peer "o" of n forwards sent.
func oneStream(n uint64) *Plane {
	p := New(Options{Node: "p", Logger: quiet})
	p.out["o"] = &outbound{sent: n}
	return p
}

func verdictOf(p *Plane, peer string, seq uint64) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	o := p.out[peer]
	_, refused := o.refused[seq]
	_, pending := o.pending[seq]
	return forwardVerdict(seq, o.lastFailed, o.acked, refused, pending)
}

// TestPendingGivesAreBounded: past the bound the oldest pending forward is
// refused, never passed, and the newest still waits.
func TestPendingGivesAreBounded(t *testing.T) {
	p := oneStream(maxPendingGives + 1)
	given := make([]uint64, 0, maxPendingGives+1)
	for s := uint64(1); s <= maxPendingGives+1; s++ {
		given = append(given, s)
	}
	p.acked("o", maxPendingGives+1, false, false, given)
	if v := verdictOf(p, "o", 1); v != forwardRefused {
		t.Fatalf("the oldest pending forward past the bound: verdict %d, want refused", v)
	}
	if v := verdictOf(p, "o", maxPendingGives+1); v != forwardWait {
		t.Fatalf("the newest pending forward: verdict %d, want wait", v)
	}
	if n := len(p.out["o"].pending); n != maxPendingGives {
		t.Fatalf("%d pending, want the bound %d", n, maxPendingGives)
	}
}

// TestAckWithoutGivenPassesAsBefore: an acknowledgement with no given member
// (an older owner's, or one that gave nothing on) passes its forwards, and
// encodes byte for byte as an older build's.
func TestAckWithoutGivenPassesAsBefore(t *testing.T) {
	body, _ := json.Marshal(forwardEnvelope{Seq: 7})
	if string(body) != `{"seq":7,"msg":null}` {
		t.Fatalf("an acknowledgement with nothing given encodes as %s", body)
	}
	p := oneStream(7)
	p.receiveForward("o", append([]byte{fwdAck}, body...))
	if v := verdictOf(p, "o", 7); v != forwardPass {
		t.Fatalf("a forward acknowledged with nothing given: verdict %d, want pass", v)
	}
}

// TestOutcomeWithoutPendingMark: a held outcome settles only a pending mark,
// so it cannot pass a forward nothing acknowledged; a refused outcome refuses
// whatever the mark.
func TestOutcomeWithoutPendingMark(t *testing.T) {
	p := oneStream(3)
	held, _ := json.Marshal(forwardEnvelope{Seq: 3})
	p.receiveForward("o", append([]byte{fwdGiven}, held...))
	if v := verdictOf(p, "o", 3); v != forwardWait {
		t.Fatalf("a held outcome for a forward neither acknowledged nor pending: verdict %d, want wait", v)
	}
	refused, _ := json.Marshal(forwardEnvelope{Seq: 3, Failed: true})
	p.receiveForward("o", append([]byte{fwdGiven}, refused...))
	if v := verdictOf(p, "o", 3); v != forwardRefused {
		t.Fatalf("a refused outcome: verdict %d, want refused", v)
	}
}

// TestGivenForwardWaitsForItsOutcome: an acknowledgement naming a forward as
// given leaves its barrier waiting; the held outcome passes it, and a refused
// one fails it with the give's reason.
func TestGivenForwardWaitsForItsOutcome(t *testing.T) {
	p := oneStream(2)
	ack, _ := json.Marshal(forwardEnvelope{Seq: 2, Given: []uint64{1, 2}})
	p.receiveForward("o", append([]byte{fwdAck}, ack...))
	for _, s := range []uint64{1, 2} {
		if v := verdictOf(p, "o", s); v != forwardWait {
			t.Fatalf("forward %d acknowledged as given: verdict %d, want wait", s, v)
		}
	}
	held, _ := json.Marshal(forwardEnvelope{Seq: 1})
	p.receiveForward("o", append([]byte{fwdGiven}, held...))
	if v := verdictOf(p, "o", 1); v != forwardPass {
		t.Fatalf("forward 1 after its held outcome: verdict %d, want pass", v)
	}
	refused, _ := json.Marshal(forwardEnvelope{Seq: 2, Failed: true})
	p.receiveForward("o", append([]byte{fwdGiven}, refused...))
	if v := verdictOf(p, "o", 2); v != forwardRefused {
		t.Fatalf("forward 2 after its refused outcome: verdict %d, want refused", v)
	}
	p.mu.Lock()
	_, labelled := p.out["o"].giveRefused[2]
	p.mu.Unlock()
	if !labelled {
		t.Fatal("forward 2's refusal is not labelled as a give's")
	}
}

// memberHeldSink is a fake sink whose durability waits are held until release
// closes, so a give is applied on its member and not yet acknowledged.
type memberHeldSink struct {
	*fakeSink
	release chan struct{}
}

func (h memberHeldSink) Durable(ctx context.Context, keys []store.Scope) error {
	select {
	case <-h.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	return h.fakeSink.Durable(ctx, keys)
}

// groupTrio is a publisher's node a, a group's dispatch owner b and a member
// node cc, every plane counting into m. b holds two of the group's three
// members, so it owns the dispatch; round robin over [b cc] at b sends the
// first message b's way and the second to cc.
func groupTrio(t *testing.T, tweak func(o *Options)) (a, b, cc *tnode, m *countingMetrics) {
	t.Helper()
	m = newCountingMetrics()
	c := newClusterOpts(t, []string{"a", "b", "cc"}, dispatch.RoundRobin, func(o *Options) {
		o.Metrics = m
		if tweak != nil {
			tweak(o)
		}
	})
	a, b, cc = c.nodes["a"], c.nodes["b"], c.nodes["cc"]
	b.plane.Observe("default", "$share/g/t/#", 2)
	cc.plane.Observe("default", "$share/g/t/#", 1)
	waitFor(t, "group routes", 5*time.Second, func() bool {
		_, groups := a.plane.Table().Match("default", "t/1")
		return len(groups) == 1 && len(groups[0].Members) == 2
	})
	return a, b, cc, m
}

// holdMember makes cc's durability waits hold until the test releases them,
// from the test or at cleanup.
func holdMember(t *testing.T, cc *tnode) func() {
	t.Helper()
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	cc.plane.SetSink(memberHeldSink{fakeSink: cc.sink, release: release})
	return unblock
}

func barrierWithin(a *tnode, m *message.Message, d time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	return a.plane.Barrier(ctx, m.Forwards)
}

// givenToCC publishes past b's own turn and returns the publish b gives to
// cc, once a's stream to b acknowledges its decide and holds it pending.
func givenToCC(t *testing.T, a, cc *tnode) *message.Message {
	t.Helper()
	if err := barrierWithin(a, publish(a, "p", "t/1", "to-b", 1), 5*time.Second); err != nil {
		t.Fatalf("precondition: the publish b delivered itself was refused: %v", err)
	}
	m := publish(a, "p", "t/1", "to-cc", 2)
	if len(m.Forwards) != 1 || m.Forwards[0].Peer != "b" {
		t.Fatalf("precondition: the publish's forward is %+v, want one decide to b", m.Forwards)
	}
	seq := m.Forwards[0].Seq
	waitFor(t, "a's decide to b acknowledged and held pending", 5*time.Second, func() bool {
		a.plane.mu.Lock()
		defer a.plane.mu.Unlock()
		o := a.plane.out["b"]
		_, pending := o.pending[seq]
		return o.acked >= seq && pending
	})
	waitFor(t, "cc applied the give", 5*time.Second, func() bool { return cc.sink.groupCount() == 1 })
	return m
}

// TestGroupGiveWaitsForTheMember: the publisher's barrier over a message b
// gave to cc does not pass on b's acknowledgement of the decide; it passes
// once cc's durability wait ends and b relays the give held.
func TestGroupGiveWaitsForTheMember(t *testing.T) {
	a, _, cc, m := groupTrio(t, nil)
	release := holdMember(t, cc)
	given := givenToCC(t, a, cc)
	done := make(chan error, 1)
	go func() { done <- barrierWithin(a, given, 5*time.Second) }()
	select {
	case err := <-done:
		t.Fatalf("the barrier returned %v while cc's durability wait was held", err)
	case <-time.After(300 * time.Millisecond):
	}
	release()
	if err := <-done; err != nil {
		t.Fatalf("the barrier after cc held the give: %v", err)
	}
	if n := m.count("hoardmq_group_gives_relayed_total", "held"); n != 1 {
		t.Fatalf("b relayed %v gives held, want 1", n)
	}
}

// longBound keeps b's relay bound past every barrier's context, so only the
// path under test can refuse the give in time.
func longBound(o *Options) { o.GiveTimeout = time.Minute }

// TestGroupGiveMemberLostRefusesThePublish: cc is lost after the give and
// before its durability wait ends; b relays the give refused and the
// publisher's barrier fails.
func TestGroupGiveMemberLostRefusesThePublish(t *testing.T) {
	a, b, cc, m := groupTrio(t, longBound)
	holdMember(t, cc)
	given := givenToCC(t, a, cc)
	b.tr.Isolate("cc", true)
	cc.tr.Isolate("b", true)
	err := barrierWithin(a, given, 5*time.Second)
	if !errors.Is(err, ErrRoutesUnavailable) || !strings.Contains(err.Error(), "did not hold it") {
		t.Fatalf("a publish whose member was lost before holding the give: %v, want the give's refusal", err)
	}
	if n := m.count("hoardmq_group_gives_relayed_total", "refused"); n != 1 {
		t.Fatalf("b relayed %v gives refused, want 1", n)
	}
}

// TestGroupGiveRelayBoundRefuses: cc never finishes its durability wait while
// every link stays up; b relays the give refused at its bound, before the
// publisher's own wait would have ended. The barrier's context is under cc's
// own five-second durability bound, whose failed acknowledgement b would
// otherwise relay in the bound's place.
func TestGroupGiveRelayBoundRefuses(t *testing.T) {
	a, _, cc, _ := groupTrio(t, func(o *Options) { o.GiveTimeout = 300 * time.Millisecond })
	holdMember(t, cc)
	given := givenToCC(t, a, cc)
	err := barrierWithin(a, given, 3*time.Second)
	if errors.Is(err, context.DeadlineExceeded) || !strings.Contains(fmt.Sprint(err), "did not hold it") {
		t.Fatalf("a publish whose member held the give past b's bound: %v, want the give's refusal", err)
	}
}

// TestGroupGiveRelayLostFailsClosed: a's data link to b resets while the
// give's outcome is outstanding, so the outcome sent on it can never arrive;
// the publisher's barrier fails rather than waiting out its context. b stays
// up, so the failure is the reset's and not the barrier's peer check.
func TestGroupGiveRelayLostFailsClosed(t *testing.T) {
	a, _, cc, _ := groupTrio(t, longBound)
	holdMember(t, cc)
	given := givenToCC(t, a, cc)
	a.tr.Reset("b", transport.RoleData, "test: the data link resets")
	err := barrierWithin(a, given, 5*time.Second)
	if err == nil || errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, ErrRoutesUnavailable) {
		t.Fatalf("a publish whose give outcome can no longer arrive: %v, want refused before its context ends", err)
	}
}

// TestGroupGiveBackToThePublisher: b gives the message back to a, the
// publisher's own node; a's apply loop holds the give while the publisher's
// barrier waits on the relay, and the barrier passes.
func TestGroupGiveBackToThePublisher(t *testing.T) {
	c := newCluster(t, []string{"a", "b"}, dispatch.RoundRobin)
	a, b := c.nodes["a"], c.nodes["b"]
	b.plane.Observe("default", "$share/g/t/#", 2)
	a.plane.Observe("default", "$share/g/t/#", 1)
	waitFor(t, "group routes", 5*time.Second, func() bool {
		_, groups := a.plane.Table().Match("default", "t/1")
		return len(groups) == 1 && len(groups[0].Members) == 2
	})
	for i := uint64(1); i <= 2; i++ {
		msg := &message.Message{Namespace: "default", Topic: "t/1", QoS: 1, Payload: []byte("g"), Publisher: message.Publisher{Namespace: "default", ClientID: "p", Node: "a"}, Seq: i, Arrived: time.Now()}
		if a.plane.Decide("default", "g/t/#", msg) {
			t.Fatal("precondition: a delivered locally without b's decision")
		}
		if err := barrierWithin(a, msg, 5*time.Second); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	if a.sink.groupCount() != 1 || b.sink.groupCount() != 1 {
		t.Fatalf("a holds %d and b %d group deliveries, want one each", a.sink.groupCount(), b.sink.groupCount())
	}
}

// TestOwnersGivingToEachOtherDoNotWait: b owns g1 and gives to cc; cc owns
// g2 and gives to b. b publishes to g2 and cc to g1, at once, so b's apply
// loop for cc's decides gives to cc while cc's apply loop for b's decides
// gives to b. Were either acknowledgement held for its give's outcome, each
// loop would wait on the other; every barrier passes because none is.
func TestOwnersGivingToEachOtherDoNotWait(t *testing.T) {
	m := newCountingMetrics()
	c := newClusterMetrics(t, []string{"b", "cc"}, dispatch.RoundRobin, m)
	b, cc := c.nodes["b"], c.nodes["cc"]
	b.plane.Observe("default", "$share/g1/t/#", 2)
	cc.plane.Observe("default", "$share/g1/t/#", 1)
	cc.plane.Observe("default", "$share/g2/u/#", 2)
	b.plane.Observe("default", "$share/g2/u/#", 1)
	waitFor(t, "group routes", 5*time.Second, func() bool {
		_, g1 := b.plane.Table().Match("default", "t/1")
		_, g2 := cc.plane.Table().Match("default", "u/1")
		_, g1cc := cc.plane.Table().Match("default", "t/1")
		_, g2b := b.plane.Table().Match("default", "u/1")
		return len(g1) == 1 && len(g1[0].Members) == 2 && len(g2) == 1 && len(g2[0].Members) == 2 &&
			len(g1cc) == 1 && len(g1cc[0].Members) == 2 && len(g2b) == 1 && len(g2b[0].Members) == 2
	})
	errs := make(chan error, 100)
	var wg sync.WaitGroup
	for _, side := range []struct {
		pub          *tnode
		group, topic string
	}{{b, "g2/u/#", "u/1"}, {cc, "g1/t/#", "t/1"}} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := uint64(1); i <= 50; i++ {
				msg := &message.Message{Namespace: "default", Topic: side.topic, QoS: 1, Payload: []byte("x"),
					Publisher: message.Publisher{Namespace: "default", ClientID: "p-" + side.pub.name, Node: side.pub.name}, Seq: i, Arrived: time.Now()}
				if side.pub.plane.Decide("default", side.group, msg) {
					errs <- fmt.Errorf("precondition: %s delivered a %s message without its owner's decision", side.pub.name, side.group)
					continue
				}
				errs <- barrierWithin(side.pub, msg, 5*time.Second)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("a publish to a group whose owners give to each other: %v", err)
		}
	}
	// Round robin at each owner alternates its own member and the other
	// owner's: 25 of each owner's 50 decides are given back to the publisher.
	if n := m.count("hoardmq_group_gives_relayed_total", "held"); n != 50 {
		t.Fatalf("%v gives relayed held, want 50", n)
	}
	if n := m.count("hoardmq_group_gives_relayed_total", "refused"); n != 0 {
		t.Fatalf("%v gives relayed refused, want none", n)
	}
}

// TestQoS0GroupGiveRelaysNothing: a QoS 0 message b gives to cc is not held
// at a and not relayed; cc still receives it.
func TestQoS0GroupGiveRelaysNothing(t *testing.T) {
	a, b, cc, m := groupTrio(t, nil)
	for i := uint64(1); i <= 2; i++ {
		msg := &message.Message{Namespace: "default", Topic: "t/1", Payload: []byte("q0"), Publisher: message.Publisher{Namespace: "default", ClientID: "p", Node: "a"}, Seq: i, Arrived: time.Now()}
		_, _ = a.plane.Forward(msg)
	}
	waitFor(t, "b and cc each deliver one", 5*time.Second, func() bool { return b.sink.groupCount() == 1 && cc.sink.groupCount() == 1 })
	waitFor(t, "a's decides acknowledged", 5*time.Second, func() bool {
		a.plane.mu.Lock()
		defer a.plane.mu.Unlock()
		o := a.plane.out["b"]
		return o.acked >= 2
	})
	a.plane.mu.Lock()
	pending := len(a.plane.out["b"].pending)
	a.plane.mu.Unlock()
	if pending != 0 {
		t.Fatalf("a holds %d QoS 0 decides pending", pending)
	}
	if n := m.count("hoardmq_group_gives_relayed_total", "held") + m.count("hoardmq_group_gives_relayed_total", "refused"); n != 0 {
		t.Fatalf("b relayed %v QoS 0 gives", n)
	}
}

// TestWatchedGivesAreBounded: a give past the watcher's bound is relayed
// refused at once, never left unanswered.
func TestWatchedGivesAreBounded(t *testing.T) {
	_, b, _, m := groupTrio(t, longBound)
	far := giveRef{peer: "cc", seq: 1 << 40} // never acknowledged
	b.plane.gives.mu.Lock()
	for i := 0; i < maxWatchedGives; i++ {
		b.plane.gives.entries = append(b.plane.gives.entries, watchedGive{decidePeer: "a", decideSeq: uint64(i + 1), give: far, deadline: time.Now().Add(time.Hour)})
	}
	b.plane.gives.count.Store(maxWatchedGives)
	b.plane.gives.mu.Unlock()
	b.plane.watchGives("a", []watchedGive{{decideSeq: maxWatchedGives + 1, give: far}})
	if n := m.count("hoardmq_group_gives_relayed_total", "refused"); n != 1 {
		t.Fatalf("a give past the bound: %v relayed refused, want 1", n)
	}
	b.plane.gives.mu.Lock()
	n := len(b.plane.gives.entries)
	b.plane.gives.entries = nil
	b.plane.gives.count.Store(0)
	b.plane.gives.mu.Unlock()
	if n != maxWatchedGives {
		t.Fatalf("%d gives watched, want the bound %d", n, maxWatchedGives)
	}
}

// TestLinkResetFailsPendingForwards: a reset of the link the outcomes come
// over refuses the forwards still pending, and leaves an acknowledged one
// that is not pending passed.
func TestLinkResetFailsPendingForwards(t *testing.T) {
	p := oneStream(2)
	p.acked("o", 2, false, false, []uint64{2})
	p.failInFlight("o")
	if v := verdictOf(p, "o", 2); v != forwardRefused {
		t.Fatalf("a pending forward after its link reset: verdict %d, want refused", v)
	}
	if v := verdictOf(p, "o", 1); v != forwardPass {
		t.Fatalf("an acknowledged forward not pending, after the reset: verdict %d, want pass", v)
	}
}

// TestOwnerAcknowledgesBeforeTheGivesOutcome: b acknowledges a's decide while
// its give to cc is still outstanding, so no apply loop waits on another
// peer. cc's durability is held for the whole test, so an acknowledgement
// held for the give's outcome could not arrive at all.
func TestOwnerAcknowledgesBeforeTheGivesOutcome(t *testing.T) {
	a, _, cc, _ := groupTrio(t, longBound)
	holdMember(t, cc)
	if err := barrierWithin(a, publish(a, "p", "t/1", "to-b", 1), 5*time.Second); err != nil {
		t.Fatalf("precondition: the publish b delivered itself was refused: %v", err)
	}
	m := publish(a, "p", "t/1", "to-cc", 2)
	seq := m.Forwards[0].Seq
	waitFor(t, "cc applied the give", 5*time.Second, func() bool { return cc.sink.groupCount() == 1 })
	deadline := time.Now().Add(3 * time.Second)
	for {
		a.plane.mu.Lock()
		acked := a.plane.out["b"].acked
		a.plane.mu.Unlock()
		if acked >= seq {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("b has not acknowledged a's decide %d while its give to cc is outstanding (acked %d)", seq, acked)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
