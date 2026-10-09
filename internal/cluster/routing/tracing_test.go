package routing

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/dispatch"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/otel"
	"github.com/happydave/hoardmq/internal/otel/otlptest"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

const sampledParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

// baseSendAllocs is what one forward's send allocated before tracing existed
// (hoardmq 66fcb95, the same send at 1000 runs).
const baseSendAllocs = 8

// nopTransport admits every frame and keeps none.
type nopTransport struct{ stubTransport }

func (*nopTransport) Send(context.Context, string, transport.Class, []byte) error { return nil }

// captureTransport keeps every frame it is given.
type captureTransport struct {
	stubTransport
	cmu    sync.Mutex
	frames [][]byte
}

func (c *captureTransport) Send(_ context.Context, _ string, _ transport.Class, payload []byte) error {
	c.cmu.Lock()
	c.frames = append(c.frames, append([]byte(nil), payload...))
	c.cmu.Unlock()
	return nil
}

func sendAllocs(t *testing.T, tr *otel.Tracer, trace otel.SpanContext) float64 {
	t.Helper()
	tp := &nopTransport{}
	tp.handlers = map[transport.Class]transport.Handler{}
	p := New(Options{Node: "a", Transport: tp, SyncInterval: time.Hour, Logger: quiet, Tracer: tr})
	msg := qos1("t/x")
	msg.Trace = trace
	return testing.AllocsPerRun(1000, func() {
		msg.Forwards = msg.Forwards[:0]
		p.send("b", fwdPublish, "", msg)
	})
}

// TestForwardAllocationsWithTracingOff: a forward allocates what it did
// before tracing existed when tracing is off, whatever the message carries,
// and when the message is not sampled on a node that traces.
func TestForwardAllocationsWithTracingOff(t *testing.T) {
	sampled, _ := otel.ParseTraceparent(sampledParent)
	on := otel.New(otel.Config{Ratio: 1, Node: "a", Sink: func(otel.SpanData) {}})
	cases := []struct {
		name  string
		tr    *otel.Tracer
		trace otel.SpanContext
	}{
		{"tracing off", nil, otel.SpanContext{}},
		{"tracing off, a sampled message", nil, sampled},
		{"tracing on, an unsampled message", on, otel.SpanContext{}},
	}
	for _, tc := range cases {
		n := sendAllocs(t, tc.tr, tc.trace)
		t.Logf("%s: %v allocations a forward, %d before tracing", tc.name, n, baseSendAllocs)
		if n > baseSendAllocs && !raceEnabled {
			t.Errorf("%s: %v allocations a forward, %d before tracing existed", tc.name, n, baseSendAllocs)
		}
	}
	if n := sendAllocs(t, on, sampled); n <= baseSendAllocs {
		t.Fatalf("a sampled forward allocates %v: the measurement cannot see the span", n)
	}
}

// TestSpanContextCrossesTheForward: a sampled publish's forward is a span in
// its trace, the frame carries that span's context, and the receiving node's
// delivery is its child; an unsampled publish's frame carries nothing, and a
// node that does not trace applies a traced frame as any other.
func TestSpanContextCrossesTheForward(t *testing.T) {
	rec := &otlptest.Recorder{}
	c := newClusterOpts(t, []string{"a", "b"}, dispatch.RoundRobin, func(o *Options) {
		o.Tracer = otel.New(otel.Config{Ratio: 0, Node: o.Node, Sink: rec.Record})
	})
	a, b := c.nodes["a"], c.nodes["b"]
	b.plane.Observe("default", "t/#", 1)
	waitFor(t, "routes replicated", 5*time.Second, func() bool {
		nodes, _ := a.plane.Table().Match("default", "t/1")
		return len(nodes) == 1
	})
	pubSpan := a.plane.o.Tracer.StartPublish([]packet.UserProperty{{Name: "traceparent", Value: sampledParent}})
	m := &message.Message{Namespace: "default", Topic: "t/1", Payload: []byte("x"), QoS: 1,
		Publisher: message.Publisher{Namespace: "default", ClientID: "p", Node: "a"}, Seq: 1, Arrived: time.Now(),
		Trace: pubSpan.Context()}
	_, _ = a.plane.Forward(m)
	waitFor(t, "the deliver span", 5*time.Second, func() bool { return len(rec.Named("deliver")) == 1 })
	pubSpan.End()
	fwd, del := rec.Named("forward"), rec.Named("deliver")
	if len(fwd) != 1 {
		t.Fatalf("%d forward spans", len(fwd))
	}
	f, d := fwd[0], del[0]
	pc := pubSpan.Context()
	if f.TraceID != pc.TraceID || f.ParentSpanID != pc.SpanID || f.Kind != otel.KindProducer {
		t.Errorf("forward span %+v, publish %x/%x", f, pc.TraceID, pc.SpanID)
	}
	if d.TraceID != pc.TraceID || d.ParentSpanID != f.SpanID || d.Kind != otel.KindConsumer {
		t.Errorf("deliver span %+v, forward %x", d, f.SpanID)
	}
	for k, want := range map[string]string{"hoardmq.peer": "b", "hoardmq.forward.kind": "publish", "hoardmq.forward.seq": "1"} {
		if got, _ := f.Attr(k); got != want {
			t.Errorf("forward %s = %q, want %q", k, got, want)
		}
	}
	for k, want := range map[string]string{"hoardmq.peer": "a", "hoardmq.node": "b", "hoardmq.delivered": "1", "messaging.destination.name": "t/1"} {
		if got, _ := d.Attr(k); got != want {
			t.Errorf("deliver %s = %q, want %q", k, got, want)
		}
	}

	// The frames themselves.
	capture := &captureTransport{}
	capture.handlers = map[transport.Class]transport.Handler{}
	p := New(Options{Node: "a", Transport: capture, SyncInterval: time.Hour, Logger: quiet, Tracer: a.plane.o.Tracer})
	traced, plain := qos1("t/1"), qos1("t/1")
	traced.Trace = pc
	p.send("b", fwdPublish, "", traced)
	p.send("b", fwdPublish, "", plain)
	var envs [2]forwardEnvelope
	for i := range envs {
		if err := json.Unmarshal(capture.frames[i][1:], &envs[i]); err != nil {
			t.Fatal(err)
		}
	}
	sc, ok := otel.ParseTraceparent(envs[0].Trace)
	if !ok || sc.TraceID != pc.TraceID || !sc.Sampled() || sc.SpanID == pc.SpanID {
		t.Errorf("the traced frame's trace %q", envs[0].Trace)
	}
	if bytes.Contains(capture.frames[1], []byte(`"trace"`)) {
		t.Errorf("an unsampled publish's frame carries a trace: %s", capture.frames[1])
	}

	// A node that does not trace applies the traced frame as the plain one.
	off := newClusterOpts(t, []string{"x", "y"}, dispatch.RoundRobin, nil)
	y := off.nodes["y"]
	y.plane.Observe("default", "t/#", 1)
	y.plane.receiveForward("x", capture.frames[0])
	y.plane.receiveForward("x", capture.frames[1])
	waitFor(t, "both frames applied", 5*time.Second, func() bool { return y.sink.directCount() == 2 })
	y.sink.mu.Lock()
	first, second := *y.sink.direct[0], *y.sink.direct[1]
	y.sink.mu.Unlock()
	if first.Trace != (otel.SpanContext{}) {
		t.Errorf("a node that does not trace put a span context on the message: %+v", first.Trace)
	}
	first.Arrived, second.Arrived = time.Time{}, time.Time{}
	if !reflect.DeepEqual(first, second) {
		t.Errorf("the traced frame applied as %+v, the plain one as %+v", first, second)
	}
}

// TestForwardFrameCarriesTheTrace: the hand-built frame with a trace is the
// bytes json.Marshal gives for the envelope with it.
func TestForwardFrameCarriesTheTrace(t *testing.T) {
	tp := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b8-01"
	for _, group := range []string{"", "$share/g/s/#"} {
		kind := fwdPublish
		if group != "" {
			kind = fwdGroupDecide
		}
		msg := qos1("t/1")
		enc := encodeForward(kind, group, msg)
		enc.trace = []byte(tp)
		body, err := json.Marshal(forwardEnvelope{Seq: 3, Group: group, Trace: tp, Msg: msg})
		if err != nil {
			t.Fatal(err)
		}
		want := append([]byte{kind}, body...)
		if got := enc.frame(3); !bytes.Equal(got, want) {
			t.Errorf("group %q:\n got %s\nwant %s", group, got, want)
		}
	}
}

// TestGroupGiveIsAChildOfItsDelivery: a shared-group publish decided on the
// group's owner and given on to a member node is traced as one chain: the
// publisher's forward, the owner's delivery, the owner's give, and the
// member's delivery under it. A forward the receiver refuses ends its
// delivery span with an error naming the refusal.
func TestGroupGiveIsAChildOfItsDelivery(t *testing.T) {
	rec := &otlptest.Recorder{}
	c := newClusterOpts(t, []string{"a", "b", "cc"}, dispatch.RoundRobin, func(o *Options) {
		o.Tracer = otel.New(otel.Config{Ratio: 0, Node: o.Node, Sink: rec.Record})
	})
	a, b, cc := c.nodes["a"], c.nodes["b"], c.nodes["cc"]
	a.plane.Observe("default", "$share/g/t/#", 2)
	b.plane.Observe("default", "$share/g/t/#", 1)
	cc.plane.Observe("default", "$share/g/t/#", 1)
	waitFor(t, "group routes", 5*time.Second, func() bool {
		_, groups := b.plane.Table().Match("default", "t/1")
		return len(groups) == 1 && len(groups[0].Members) == 3
	})
	for i := 0; i < 3; i++ {
		sp := b.plane.o.Tracer.StartPublish([]packet.UserProperty{{Name: "traceparent", Value: sampledParent}})
		msg := &message.Message{Namespace: "default", Topic: "t/1", QoS: 1, Payload: []byte("g"), Publisher: message.Publisher{ClientID: "p", Node: "b"}, Seq: uint64(i + 1), Arrived: time.Now(), Trace: sp.Context()}
		b.plane.Decide("default", "g/t/#", msg)
		sp.End()
	}
	var give otel.SpanData
	waitFor(t, "a given frame delivered", 5*time.Second, func() bool {
		for _, f := range rec.Named("forward") {
			if k, _ := f.Attr("hoardmq.forward.kind"); k == "group-give" {
				for _, d := range rec.Named("deliver") {
					if d.ParentSpanID == f.SpanID {
						give = f
						return true
					}
				}
			}
		}
		return false
	})
	var ownerDelivery otel.SpanData
	for _, d := range rec.Named("deliver") {
		if d.SpanID == give.ParentSpanID {
			ownerDelivery = d
		}
	}
	if n, _ := ownerDelivery.Attr("hoardmq.node"); n != "a" || ownerDelivery.TraceID != give.TraceID {
		t.Fatalf("the give's parent is %+v, want a's delivery in the same trace", ownerDelivery)
	}
	decided := false
	for _, f := range rec.Named("forward") {
		if f.SpanID == ownerDelivery.ParentSpanID {
			k, _ := f.Attr("hoardmq.forward.kind")
			decided = k == "group-decide"
		}
	}
	if !decided {
		t.Error("a's delivery is not the child of b's group-decide forward")
	}

	// A forward that reaches no session is refused, and its delivery span
	// says so.
	capture := &captureTransport{}
	capture.handlers = map[transport.Class]transport.Handler{}
	p := New(Options{Node: "b", Transport: capture, SyncInterval: time.Hour, Logger: quiet, Tracer: b.plane.o.Tracer})
	p.SetSink(nobodySink{})
	sp := b.plane.o.Tracer.StartPublish([]packet.UserProperty{{Name: "traceparent", Value: sampledParent}})
	stray := qos1("nobody/here")
	stray.Trace = sp.Context()
	p.send("z", fwdPublish, "", stray)
	p.receiveForward("z", capture.frames[0])
	waitFor(t, "the refused delivery's span", 5*time.Second, func() bool {
		for _, d := range rec.Named("deliver") {
			if r, _ := d.Attr("hoardmq.refusal"); r == "no-local-subscriber" && d.Status == otel.StatusError {
				return true
			}
		}
		return false
	})
}

// nobodySink holds no session: every forwarded message reaches nobody.
type nobodySink struct{}

func (nobodySink) InjectForwarded(*message.Message) (int, []store.Scope, error) { return 0, nil, nil }
func (nobodySink) Durable(context.Context, []store.Scope) error                 { return nil }
func (nobodySink) InjectGroup(*message.Message, string) (store.Scope, bool, error) {
	return store.Scope{}, false, nil
}

// TestFailedSendEndsTheForwardSpanWithAnError: a forward the transport
// refuses is recorded as a failed span.
func TestFailedSendEndsTheForwardSpanWithAnError(t *testing.T) {
	rec := &otlptest.Recorder{}
	tr := &failingTransport{fail: map[int]bool{1: true}}
	tr.handlers = map[transport.Class]transport.Handler{}
	tracer := otel.New(otel.Config{Ratio: 0, Node: "a", Sink: rec.Record})
	p := New(Options{Node: "a", Transport: tr, SyncInterval: time.Hour, Logger: quiet, Tracer: tracer})
	sp := tracer.StartPublish([]packet.UserProperty{{Name: "traceparent", Value: sampledParent}})
	msg := qos1("t/1")
	msg.Trace = sp.Context()
	p.send("b", fwdPublish, "", msg)
	fwd := rec.Named("forward")
	if len(fwd) != 1 || fwd[0].Status != otel.StatusError || fwd[0].StatusMessage == "" {
		t.Fatalf("forward spans %+v, want one failed", fwd)
	}
}
