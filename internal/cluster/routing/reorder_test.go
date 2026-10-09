package routing

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/message"
)

// reorderLink is a transport whose delivery order is the test's to choose:
// every frame is held until released by index, so a pair of forwards can be
// delivered in the order the wire chose rather than the order they were sent.
// Frames it is not holding go straight to the peer's handler.
type reorderLink struct {
	stubTransport
	hmu     sync.Mutex
	held    []heldFrame
	hold    bool
	deliver func(from string, c transport.Class, payload []byte)
	from    string
}

type heldFrame struct {
	class     transport.Class
	payload   []byte
	delivered bool
}

func (l *reorderLink) Send(_ context.Context, _ string, c transport.Class, payload []byte) error {
	frame := heldFrame{class: c, payload: append([]byte(nil), payload...)}
	l.hmu.Lock()
	if l.hold {
		l.held = append(l.held, frame)
		l.hmu.Unlock()
		return nil
	}
	deliver, from := l.deliver, l.from
	l.hmu.Unlock()
	deliver(from, c, frame.payload)
	return nil
}

func (l *reorderLink) heldCount() int {
	l.hmu.Lock()
	defer l.hmu.Unlock()
	return len(l.held)
}

// release hands the held frame at idx to the peer, leaving the others where
// they are. Releasing out of index order is the point.
func (l *reorderLink) release(t *testing.T, idx int) {
	t.Helper()
	l.hmu.Lock()
	if idx >= len(l.held) {
		l.hmu.Unlock()
		t.Fatalf("no frame %d to release; %d held", idx, len(l.held))
	}
	if l.held[idx].delivered {
		l.hmu.Unlock()
		t.Fatalf("frame %d was already delivered", idx)
	}
	l.held[idx].delivered = true
	f := l.held[idx]
	deliver, from := l.deliver, l.from
	l.hmu.Unlock()
	deliver(from, f.class, f.payload)
}

// countingMetrics records counter increments by name and label, so a test can
// name the reason a refusal was counted under rather than infer it.
type countingMetrics struct {
	mu sync.Mutex
	n  map[string]float64
}

func newCountingMetrics() *countingMetrics { return &countingMetrics{n: map[string]float64{}} }

func (m *countingMetrics) Add(name string, labels map[string]string, delta float64) {
	m.mu.Lock()
	m.n[name+"|"+labels["reason"]+labels["outcome"]] += delta
	m.mu.Unlock()
}

func (m *countingMetrics) Set(string, map[string]string, float64) {}

func (m *countingMetrics) count(name, reason string) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.n[name+"|"+reason]
}

// reorderPair builds a sender and a receiver joined by two links. The
// sender's link holds its forwards for the test to release; the receiver's
// acknowledgements go straight back.
func reorderPair(t *testing.T) (send *Plane, recv *Plane, wire *reorderLink, sink *fakeSink, m *countingMetrics) {
	t.Helper()
	sendLink := &reorderLink{hold: true, from: "a"}
	recvLink := &reorderLink{from: "b"}
	for _, l := range []*reorderLink{sendLink, recvLink} {
		l.handlers = map[transport.Class]transport.Handler{}
	}
	sendLink.peers = []transport.PeerInfo{{Name: "b", State: transport.Up}}
	recvLink.peers = []transport.PeerInfo{{Name: "a", State: transport.Up}}

	m = newCountingMetrics()
	send = New(Options{Node: "a", Transport: sendLink, SyncInterval: time.Hour, Logger: quiet, Metrics: m})
	sink = &fakeSink{}
	recv = New(Options{Node: "b", Transport: recvLink, SyncInterval: time.Hour, Logger: quiet, Metrics: m})
	recv.SetSink(sink)

	sendLink.deliver = func(from string, c transport.Class, payload []byte) {
		recvLink.handlers[c](from, payload)
	}
	recvLink.deliver = func(from string, c transport.Class, payload []byte) {
		sendLink.handlers[c](from, payload)
	}

	send.Start()
	recv.Start()
	t.Cleanup(recv.Stop)
	t.Cleanup(send.Stop)
	return send, recv, sendLink, sink, m
}

// oneForward puts one QoS 1 publish on the sender's stream to "b" and returns
// the message, whose Forwards name the sequence its barrier will be judged on.
func oneForward(t *testing.T, p *Plane, topic string) *message.Message {
	t.Helper()
	msg := qos1(topic)
	p.send("b", fwdPublish, "", msg)
	if len(msg.Forwards) != 1 {
		t.Fatalf("%s: expected one forward ref, got %d", topic, len(msg.Forwards))
	}
	return msg
}

func waitDelivered(t *testing.T, sink *fakeSink, n int) {
	t.Helper()
	waitFor(t, "forwarded frames delivered", 5*time.Second, func() bool { return sink.directCount() >= n })
}

// warm sends one forward and delivers it in order, so the frames a test then
// reorders are in the middle of a stream: the frames below a stream's first
// arrival open no hole (inboundSeq), and a reordering there reads as the
// sender starting a new stream.
func warm(t *testing.T, p *Plane, wire *reorderLink, sink *fakeSink) {
	t.Helper()
	oneForward(t, p, "t/warm")
	wire.release(t, wire.heldCount()-1)
	waitDelivered(t, sink, 1)
}

// TestOutOfOrderForwardRefusesADeliveredPublish is WI 1708's deciding test:
// two forwards are sent in order, arrive out of order, and both are delivered
// — nothing is lost — yet the first publish's barrier refuses, because the
// receiver read the second frame's sequence as a hole. The ordering is made
// at the wire, below the sender, so the sender's fix does not remove it: this
// is the receiver's rule, which is correct for a hole it cannot tell from a
// loss.
func TestOutOfOrderForwardRefusesADeliveredPublish(t *testing.T) {
	send, _, wire, sink, m := reorderPair(t)
	warm(t, send, wire, sink)

	first := oneForward(t, send, "t/first")
	second := oneForward(t, send, "t/second")
	if first.Forwards[0].Seq != 2 || second.Forwards[0].Seq != 3 {
		t.Fatalf("expected sequences 2 and 3, got %d and %d", first.Forwards[0].Seq, second.Forwards[0].Seq)
	}
	if wire.heldCount() != 3 {
		t.Fatalf("expected the warm-up frame and both forwards held on the wire, got %d", wire.heldCount())
	}

	// The wire admits the second frame first. Neither frame is lost.
	wire.release(t, 2)
	waitDelivered(t, sink, 2)
	wire.release(t, 1)
	waitDelivered(t, sink, 3)

	sink.mu.Lock()
	order := []string{sink.direct[1].Topic, sink.direct[2].Topic}
	sink.mu.Unlock()
	if order[0] != "t/second" || order[1] != "t/first" {
		t.Fatalf("this test needs the frames delivered out of order; got %v", order)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := send.Barrier(ctx, first.Forwards); !errors.Is(err, ErrRoutesUnavailable) {
		t.Fatalf("the first publish was delivered and must be refused only if something was lost; barrier returned %v", err)
	}
	if got := m.count("hoardmq_forward_failures_total", "sequence-gap"); got != 1 {
		t.Fatalf("expected the refusal to be counted as one sequence gap, got %v", got)
	}
	if got := m.count("hoardmq_forward_failures_total", "send"); got != 0 {
		t.Fatalf("no frame failed to leave; got %v counted under send", got)
	}
	// The instrument that tells this case from a real loss in a load run:
	// the frame the gap reported missing arrived, below the expected sequence.
	if got := m.count("hoardmq_forward_late_frames_total", ""); got != 1 {
		t.Fatalf("expected the late frame to be counted once, got %v", got)
	}
}

// TestLateFrameRefusesOnlyTheOvertakenPublish: a late frame fills the hole it
// was reported for and leaves the expectation where it was, so the frame
// after it is in order. One reordering refuses the one publish that was
// overtaken; a late frame that rewound the expectation would make the next
// frame a second hole and refuse a second delivered publish.
func TestLateFrameRefusesOnlyTheOvertakenPublish(t *testing.T) {
	send, _, wire, sink, m := reorderPair(t)
	warm(t, send, wire, sink)

	first := oneForward(t, send, "t/first")
	second := oneForward(t, send, "t/second")
	third := oneForward(t, send, "t/third")

	wire.release(t, 2)
	waitDelivered(t, sink, 2)
	wire.release(t, 1)
	waitDelivered(t, sink, 3)
	wire.release(t, 3)
	waitDelivered(t, sink, 4)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := send.Barrier(ctx, first.Forwards); !errors.Is(err, ErrRoutesUnavailable) {
		t.Fatalf("the overtaken publish is refused by the gap; got %v", err)
	}
	if err := send.Barrier(ctx, second.Forwards); err != nil {
		t.Fatalf("the publish that overtook was delivered; barrier returned %v", err)
	}
	if err := send.Barrier(ctx, third.Forwards); err != nil {
		t.Fatalf("the publish after the late frame was delivered in order; barrier returned %v", err)
	}
	if got := m.count("hoardmq_forward_failures_total", "sequence-gap"); got != 1 {
		t.Fatalf("expected one sequence gap from one reordering, got %v", got)
	}
	if got := m.count("hoardmq_forward_late_frames_total", ""); got != 1 {
		t.Fatalf("expected one late frame, got %v", got)
	}
}

// TestInOrderForwardsAreNotRefused is the positive control for the two above:
// the same three publishes over the same harness, delivered in the order they
// were sent, report no gap and pass their barriers. A refusal here would mean
// the harness refuses rather than the ordering.
func TestInOrderForwardsAreNotRefused(t *testing.T) {
	send, _, wire, sink, m := reorderPair(t)
	warm(t, send, wire, sink)

	msgs := []*message.Message{
		oneForward(t, send, "t/first"),
		oneForward(t, send, "t/second"),
		oneForward(t, send, "t/third"),
	}
	for i := range msgs {
		wire.release(t, i+1)
		waitDelivered(t, sink, i+2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for i, msg := range msgs {
		if err := send.Barrier(ctx, msg.Forwards); err != nil {
			t.Fatalf("publish %d was delivered in order; barrier returned %v", i+1, err)
		}
	}
	if got := m.count("hoardmq_forward_failures_total", "sequence-gap"); got != 0 {
		t.Fatalf("in-order delivery must report no gap, got %v", got)
	}
	if got := m.count("hoardmq_forward_late_frames_total", ""); got != 0 {
		t.Fatalf("in-order delivery has no late frame, got %v", got)
	}
}

// inject hands the receiver a forwarded frame from peer "a" carrying seq, as
// a sender whose count started again would send it.
func inject(t *testing.T, recv *Plane, sink *fakeSink, seq uint64, topic string) {
	t.Helper()
	before := sink.directCount()
	recv.receiveForward("a", encodeForward(fwdPublish, "", qos1(topic)).frame(seq))
	waitDelivered(t, sink, before+1)
}

// TestRestartedSenderStreamStillReportsALoss: a frame below the expectation
// that no reported hole accounts for is the sender's new stream. The
// expectation follows it, so a frame the new stream loses is still reported
// — reading it as late instead would leave the new stream unchecked until
// its count passed the old one.
func TestRestartedSenderStreamStillReportsALoss(t *testing.T) {
	send, recv, wire, sink, m := reorderPair(t)
	warm(t, send, wire, sink)
	for i := 0; i < 4; i++ {
		oneForward(t, send, "t/old")
		wire.release(t, wire.heldCount()-1)
		waitDelivered(t, sink, i+2)
	}

	inject(t, recv, sink, 1, "t/new1")
	inject(t, recv, sink, 2, "t/new2")
	inject(t, recv, sink, 4, "t/new4") // 3 never arrives

	if got := m.count("hoardmq_forward_stream_restarts_total", ""); got != 1 {
		t.Fatalf("expected one new stream, got %v", got)
	}
	if got := m.count("hoardmq_forward_late_frames_total", ""); got != 0 {
		t.Fatalf("the new stream's frames are not late, got %v", got)
	}
	if got := m.count("hoardmq_forward_failures_total", "sequence-gap"); got != 1 {
		t.Fatalf("expected the new stream's lost frame reported as one gap, got %v", got)
	}
}

// TestRestartAfterAReceiverRestartStillReportsALoss is the rolling restart:
// the receiver starts partway into the sender's stream, so its first frame
// reports everything below it missing; then the sender restarts. The frames
// of its new stream fall below that first frame, and if the first-frame gap
// had been kept as a hole they would read as late and nothing lost among
// them would be reported.
func TestRestartAfterAReceiverRestartStillReportsALoss(t *testing.T) {
	_, recv, _, sink, m := reorderPair(t)

	inject(t, recv, sink, 50, "t/old50")
	inject(t, recv, sink, 51, "t/old51")
	inject(t, recv, sink, 1, "t/new1")
	inject(t, recv, sink, 2, "t/new2")
	inject(t, recv, sink, 4, "t/new4") // 3 never arrives

	if got := m.count("hoardmq_forward_failures_total", "sequence-gap"); got != 2 {
		t.Fatalf("expected the first frame's gap and the new stream's, got %v", got)
	}
	if got := m.count("hoardmq_forward_late_frames_total", ""); got != 0 {
		t.Fatalf("no frame here is late, got %v", got)
	}
	if got := m.count("hoardmq_forward_stream_restarts_total", ""); got != 1 {
		t.Fatalf("expected one new stream, got %v", got)
	}
}
