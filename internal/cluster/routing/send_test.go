package routing

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/dispatch"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/packet"
)

// admitOrderTransport records the sequence of each forwarded frame in the
// order its Send admitted it. The first Send holds its admission until a
// second Send has been admitted, or until hold passes: a second sender that
// can reach the transport while the first is still inside it is admitted
// first, which is the overtaking a receiver reads as a hole.
type admitOrderTransport struct {
	stubTransport
	hold     time.Duration
	amu      sync.Mutex
	calls    int
	admitted []uint64
	firstIn  chan struct{}
	secondIn chan struct{}
}

func (a *admitOrderTransport) Send(_ context.Context, _ string, _ transport.Class, payload []byte) error {
	var env forwardEnvelope
	if err := json.Unmarshal(payload[1:], &env); err != nil {
		return err
	}
	a.amu.Lock()
	a.calls++
	call := a.calls
	a.amu.Unlock()
	record := func() {
		a.amu.Lock()
		a.admitted = append(a.admitted, env.Seq)
		a.amu.Unlock()
	}
	switch call {
	case 1:
		close(a.firstIn)
		select {
		case <-a.secondIn:
		case <-time.After(a.hold):
		}
		record()
	case 2:
		record()
		close(a.secondIn)
	default:
		record()
	}
	return nil
}

// TestFramesAreAdmittedInSequenceOrder: a publisher that takes a sequence
// while another is still inside the transport with the one before it must
// wait for that admission. B starts only once A is inside Send, so if B can
// take sequence 2 and reach the transport, it is admitted before A's 1.
func TestFramesAreAdmittedInSequenceOrder(t *testing.T) {
	tr := &admitOrderTransport{hold: 200 * time.Millisecond, firstIn: make(chan struct{}), secondIn: make(chan struct{})}
	tr.handlers = map[transport.Class]transport.Handler{}
	p := New(Options{Node: "a", Transport: tr, SyncInterval: time.Hour, Logger: quiet})

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		p.send("b", fwdPublish, "", qos1("t/a"))
	}()
	<-tr.firstIn
	go func() {
		defer wg.Done()
		p.send("b", fwdPublish, "", qos1("t/b"))
	}()
	wg.Wait()

	tr.amu.Lock()
	defer tr.amu.Unlock()
	if len(tr.admitted) != 2 || tr.admitted[0] != 1 || tr.admitted[1] != 2 {
		t.Fatalf("frames admitted in sequence order %v; want [1 2]", tr.admitted)
	}
}

// TestConcurrentForwardsArriveWithoutAGap drives many publishers at one peer
// over the real transport. Every frame arrives and every barrier passes, and
// the receiver saw no hole, no late frame and no new stream: a frame that
// overtook the one before it would show as a gap and a late frame together.
func TestConcurrentForwardsArriveWithoutAGap(t *testing.T) {
	m := newCountingMetrics()
	c := newClusterMetrics(t, []string{"a", "b"}, dispatch.RoundRobin, m)
	a, b := c.nodes["a"], c.nodes["b"]
	b.plane.Observe("default", "t/#", 1)
	waitFor(t, "route", 5*time.Second, func() bool {
		nodes, _ := a.plane.Table().Match("default", "t/1")
		return len(nodes) == 1
	})

	const publishers, each = 32, 300
	msgs := make([][]*message.Message, publishers)
	var wg sync.WaitGroup
	for i := 0; i < publishers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < each; j++ {
				msgs[i] = append(msgs[i], publish(a, fmt.Sprintf("p%d", i), "t/1", "x", uint64(j+1)))
			}
		}(i)
	}
	wg.Wait()
	waitFor(t, "every forward delivered", 20*time.Second, func() bool { return b.sink.directCount() >= publishers*each })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i := range msgs {
		for _, msg := range msgs[i] {
			if err := a.plane.Barrier(ctx, msg.Forwards); err != nil {
				t.Fatalf("publisher %d: a forward was delivered and refused: %v", i, err)
			}
		}
	}
	if got := b.sink.directCount(); got != publishers*each {
		t.Fatalf("delivered %d, want %d", got, publishers*each)
	}
	for _, name := range []string{"hoardmq_forward_late_frames_total", "hoardmq_forward_stream_restarts_total"} {
		if got := m.count(name, ""); got != 0 {
			t.Errorf("%s = %v, want 0", name, got)
		}
	}
	if got := m.count("hoardmq_forward_failures_total", "sequence-gap"); got != 0 {
		t.Errorf("sequence gaps = %v, want 0", got)
	}
}

// TestForwardFrameIsTheEnvelope: the frame assembled around a message encoded
// ahead of its sequence is byte for byte what json.Marshal gives for the
// envelope, so the wire format is unchanged.
func TestForwardFrameIsTheEnvelope(t *testing.T) {
	ct, rt := "text/<plain>", "reply/&/\"x\""
	format := byte(1)
	rich := &message.Message{
		Namespace: "tenant-ü", Topic: `a/<b>/&c/"d"/\e/日本`, Payload: []byte("<&>\x00\xff"), QoS: 2, Retain: true,
		PayloadFormat: &format, ContentType: &ct, ResponseTopic: &rt, CorrelationData: []byte{1, 2, 3},
		UserProperties: []packet.UserProperty{{Name: "k<", Value: "v&"}},
		Expiry:         time.Date(2026, 9, 22, 1, 2, 3, 4, time.UTC),
		Publisher:      message.Publisher{Namespace: "n", ClientID: "c</script>"}, Seq: 7,
		Arrived: time.Date(2026, 9, 22, 1, 2, 3, 5, time.UTC), OriginCluster: "o", Hops: 3,
		Forwarded: true, Forwards: []message.ForwardRef{{Peer: "b", Seq: 1}},
	}
	cases := []struct {
		name  string
		kind  byte
		group string
		msg   *message.Message
		seq   uint64
	}{
		{"direct, plain", fwdPublish, "", qos1("t/1"), 1},
		{"direct, escaped", fwdPublish, "", rich, 1 << 40},
		{"group decide", fwdGroupDecide, "$share/g<&>/s/#", rich, 42},
		{"group give", fwdGroupGive, "$share/g/s/+", qos1("s/1"), 9},
	}
	for _, tc := range cases {
		body, err := json.Marshal(forwardEnvelope{Seq: tc.seq, Group: tc.group, Msg: tc.msg})
		if err != nil {
			t.Fatal(err)
		}
		want := append([]byte{tc.kind}, body...)
		if got := encodeForward(tc.kind, tc.group, tc.msg).frame(tc.seq); !bytes.Equal(got, want) {
			t.Errorf("%s:\n got %s\nwant %s", tc.name, got, want)
		}
	}
}

// TestFailedSendLeavesTheStreamUsable: a send the transport refuses marks its
// own sequence failed and releases the send lock, so the next forward takes
// the next sequence and is judged on its own.
func TestFailedSendLeavesTheStreamUsable(t *testing.T) {
	tr := &failingTransport{fail: map[int]bool{1: true}}
	tr.handlers = map[transport.Class]transport.Handler{}
	p := New(Options{Node: "a", Transport: tr, SyncInterval: time.Hour, Logger: quiet})
	first, second := qos1("t/1"), qos1("t/2")
	p.send("b", fwdPublish, "", first)
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.send("b", fwdPublish, "", second)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the send after a failed one did not return: the send lock was kept")
	}
	if first.Forwards[0].Seq != 1 || second.Forwards[0].Seq != 2 {
		t.Fatalf("sequences %d and %d, want 1 and 2", first.Forwards[0].Seq, second.Forwards[0].Seq)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := p.Barrier(ctx, first.Forwards); !errors.Is(err, ErrRoutesUnavailable) {
		t.Fatalf("the failed send's barrier: %v, want ErrRoutesUnavailable", err)
	}
}

// failingTransport refuses the sends whose call number is in fail.
type failingTransport struct {
	stubTransport
	fmu   sync.Mutex
	calls int
	fail  map[int]bool
}

func (f *failingTransport) Send(context.Context, string, transport.Class, []byte) error {
	f.fmu.Lock()
	defer f.fmu.Unlock()
	f.calls++
	if f.fail[f.calls] {
		return errors.New("refused by the test")
	}
	return nil
}
