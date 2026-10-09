package otel

import (
	"sync"
	"testing"

	"github.com/happydave/hoardmq/internal/packet"
)

type sink struct {
	mu    sync.Mutex
	spans []SpanData
}

func (s *sink) record(d SpanData) {
	s.mu.Lock()
	s.spans = append(s.spans, d)
	s.mu.Unlock()
}

// TestTracingOffAllocatesNothing: every call the publish path makes, on a nil
// tracer, on a tracer that samples no root, and on a disabled tracer given a
// sampled parent, allocates nothing. The sampled case is measured beside them
// so the measurement is shown able to see an allocation.
func TestTracingOffAllocatesNothing(t *testing.T) {
	props := []packet.UserProperty{{Name: "a", Value: "1"}, {Name: "traceparent", Value: validTP}}
	unsampledProps := []packet.UserProperty{{Name: "traceparent", Value: "00-" + validTrace + "-" + validParent + "-00"}}
	parent, _ := ParseTraceparent(validTP)
	unsampledTP := "00-" + validTrace + "-" + validParent + "-00"
	path := func(tr *Tracer, props []packet.UserProperty, remoteTP string) func() {
		return func() {
			sp := tr.StartPublish(props)
			sp.String("messaging.destination.name", "t/x")
			sp.Int("hoardmq.qos", 1)
			sp.Bool("hoardmq.retain", false)
			ctx := sp.Context()
			child := tr.StartChild(ctx, "forward", KindProducer)
			if child.Recording() {
				_ = child.Context().AppendTraceparent(nil)
			}
			child.Error("refused")
			child.End()
			remote := tr.StartRemote(remoteTP, "deliver", KindConsumer)
			remote.String("hoardmq.peer", "n2")
			remote.End()
			sp.Error("refused")
			sp.End()
		}
	}
	zeroRatio := New(Config{Ratio: 0, Node: "n1", Sink: (&sink{}).record})
	disabled := New(Config{Ratio: 1, Node: "n1", Sink: (&sink{}).record})
	disabled.SetEnabled(false)
	cases := []struct {
		name     string
		tr       *Tracer
		props    []packet.UserProperty
		remoteTP string
	}{
		{"nil tracer, sampled traceparent", nil, props, validTP},
		{"nil tracer, no properties", nil, nil, ""},
		{"ratio 0, no parent", zeroRatio, nil, unsampledTP},
		{"ratio 0, unsampled parent", zeroRatio, unsampledProps, unsampledTP},
		{"disabled, sampled parent", disabled, props, validTP},
	}
	for _, tc := range cases {
		if n := testing.AllocsPerRun(1000, path(tc.tr, tc.props, tc.remoteTP)); n != 0 {
			t.Errorf("%s: %v allocations a publish, want 0", tc.name, n)
		}
	}
	var nilTracer *Tracer
	if n := testing.AllocsPerRun(1000, func() { _ = nilTracer.StartChild(parent, "forward", KindProducer) }); n != 0 {
		t.Errorf("nil tracer, child of a sampled parent: %v allocations", n)
	}
	on := New(Config{Ratio: 1, Node: "n1", Sink: func(SpanData) {}})
	if n := testing.AllocsPerRun(100, path(on, props, validTP)); n == 0 {
		t.Fatal("a sampled publish measured no allocation: the measurement cannot see one")
	}
}

// TestSampledSpanRecordsItsFields: a sampled publish span continues the
// inbound trace under the inbound span, and its child continues it under it.
func TestSampledSpanRecordsItsFields(t *testing.T) {
	s := &sink{}
	tr := New(Config{Ratio: 0, Node: "n1", Sink: s.record})
	parent, _ := ParseTraceparent(validTP)
	sp := tr.StartPublish([]packet.UserProperty{{Name: "traceparent", Value: validTP}, {Name: "tracestate", Value: "k=v"}})
	if !sp.Recording() {
		t.Fatal("a sampled parent was not followed at ratio 0")
	}
	sp.String("messaging.destination.name", "t/x")
	child := tr.StartChild(sp.Context(), "forward", KindProducer)
	child.Error("send failed")
	child.End()
	sp.End()
	if len(s.spans) != 2 {
		t.Fatalf("%d spans, want 2", len(s.spans))
	}
	fwd, pub := s.spans[0], s.spans[1]
	if pub.Name != "publish" || pub.Kind != KindServer || pub.TraceID != parent.TraceID || !pub.HasParent || pub.ParentSpanID != parent.SpanID || pub.TraceState != "k=v" {
		t.Errorf("publish span %+v", pub)
	}
	if v, _ := pub.Attr("messaging.destination.name"); v != "t/x" {
		t.Errorf("publish topic attribute %q", v)
	}
	if fwd.Name != "forward" || fwd.Kind != KindProducer || fwd.TraceID != parent.TraceID || fwd.ParentSpanID != pub.SpanID || fwd.Status != StatusError || fwd.StatusMessage != "send failed" {
		t.Errorf("forward span %+v", fwd)
	}
	if pub.SpanID == parent.SpanID || pub.SpanID == [8]byte{} || fwd.SpanID == pub.SpanID {
		t.Errorf("span ids not fresh: parent %x publish %x forward %x", parent.SpanID, pub.SpanID, fwd.SpanID)
	}
	if pub.End < pub.Start || pub.Start == 0 {
		t.Errorf("publish times %d..%d", pub.Start, pub.End)
	}
	root := tr.StartPublish(nil)
	if root.Recording() {
		t.Error("ratio 0 sampled a root")
	}
	if tr.StartRemote("00-"+validTrace+"-"+validParent+"-00", "deliver", KindConsumer).Recording() {
		t.Error("a child of an unsampled remote parent was recorded")
	}
	if tr.StartRemote("junk", "deliver", KindConsumer).Recording() {
		t.Error("a child of an unparsable remote parent was recorded")
	}
	remote := tr.StartRemote(validTP, "deliver", KindConsumer)
	if !remote.Recording() || remote.Context().TraceID != parent.TraceID {
		t.Error("a child of a sampled remote parent was not recorded in its trace")
	}
	all := New(Config{Ratio: 1, Node: "n1", Sink: s.record})
	r := all.StartPublish(nil)
	if !r.Recording() || r.Context().TraceID == [16]byte{} {
		t.Fatal("ratio 1 did not sample a root with a fresh trace id")
	}
	r.End()
	if got := s.spans[len(s.spans)-1]; got.HasParent {
		t.Error("a root span recorded a parent")
	}
}
