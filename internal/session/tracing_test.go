package session

import (
	"strings"
	"testing"

	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/otel"
	"github.com/happydave/hoardmq/internal/otel/otlptest"
	"github.com/happydave/hoardmq/internal/packet"
)

const sampledParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

// basePublishAllocs is what one of these publishes allocated before tracing
// existed (hoardmq 66fcb95, the same publish at 1000 runs, every arm).
const basePublishAllocs = 21

func measurePublishAllocs(t *testing.T, tr *otel.Tracer, qos byte, props []packet.UserProperty) float64 {
	t.Helper()
	m, _ := newManager(t, func(o *Options) { o.Tracer = tr })
	pub := newFake("pub", packet.V5, 100)
	connectV5(m, pub, true, 0, nil)
	for i := 0; i < 4; i++ {
		c := newFake("sub"+string(rune('a'+i)), packet.V5, 100)
		connectV5(m, c, true, 0, nil)
		subscribe(m, c, 0, "t/x")
	}
	var id uint16
	return testing.AllocsPerRun(1000, func() {
		id++
		m.Publish(pub, &packet.Publish{Topic: "t/x", QoS: qos, PacketID: id, Payload: []byte("x"), Properties: packet.Properties{UserProperties: props}})
	})
}

// TestPublishAllocationsWithTracingOff: with no tracer, and with a tracer
// that samples nothing, a publish allocates what it did before tracing
// existed, with or without a traceparent among its properties.
func TestPublishAllocationsWithTracingOff(t *testing.T) {
	props := []packet.UserProperty{{Name: "a", Value: "1"}, {Name: "traceparent", Value: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00"}}
	zero := otel.New(otel.Config{Ratio: 0, Node: "n1", Sink: func(otel.SpanData) {}})
	for _, qos := range []byte{0, 1} {
		for _, pp := range [][]packet.UserProperty{nil, props} {
			off := measurePublishAllocs(t, nil, qos, pp)
			unsampled := measurePublishAllocs(t, zero, qos, pp)
			t.Logf("QoS %d, %d properties: off %v, unsampled %v, before tracing %d", qos, len(pp), off, unsampled, basePublishAllocs)
			if off > basePublishAllocs && !raceEnabled {
				t.Errorf("QoS %d, %d properties: tracing off allocates %v a publish, %d before tracing existed", qos, len(pp), off, basePublishAllocs)
			}
			if unsampled != off {
				t.Errorf("QoS %d, %d properties: an unsampled publish allocates %v, tracing off %v", qos, len(pp), unsampled, off)
			}
		}
	}
	on := otel.New(otel.Config{Ratio: 1, Node: "n1", Sink: func(otel.SpanData) {}})
	if n := measurePublishAllocs(t, on, 1, nil); n <= basePublishAllocs {
		t.Fatalf("a sampled publish allocates %v, no more than off: the measurement cannot see the span", n)
	}
}

// TestSampledPublishRecordsAPublishSpan: a publish carrying a sampled
// traceparent is recorded as a server span in that trace, under that span,
// naming the topic, client and counts and holding no payload or property
// value; the message carries the span's context while it is routed.
func TestSampledPublishRecordsAPublishSpan(t *testing.T) {
	rec := &otlptest.Recorder{}
	var routed otel.SpanContext
	m, _ := newManager(t, func(o *Options) {
		o.Tracer = otel.New(otel.Config{Ratio: 0, Node: "n1", Sink: rec.Record})
		o.Node = "n1"
		o.Forwarder = traceForwarder{got: &routed}
	})
	pub := newFake("pub", packet.V5, 100)
	connectV5(m, pub, true, 0, nil)
	sub := newFake("sub", packet.V5, 100)
	connectV5(m, sub, true, 0, nil)
	subscribe(m, sub, 1, "t/x")
	props := []packet.UserProperty{{Name: "secret", Value: "hunter2"}, {Name: "traceparent", Value: sampledParent}}
	if code, _ := m.Publish(pub, &packet.Publish{Topic: "t/x", QoS: 1, PacketID: 1, Payload: []byte("the-payload"), Properties: packet.Properties{UserProperties: props}}); code.IsError() {
		t.Fatalf("publish: %v", code)
	}
	spans := rec.Named("publish")
	if len(spans) != 1 {
		t.Fatalf("%d publish spans", len(spans))
	}
	sp := spans[0]
	parent, _ := otel.ParseTraceparent(sampledParent)
	if sp.Kind != otel.KindServer || sp.TraceID != parent.TraceID || sp.ParentSpanID != parent.SpanID || !sp.HasParent {
		t.Errorf("span %+v", sp)
	}
	want := map[string]string{"messaging.system": "mqtt", "messaging.destination.name": "t/x", "messaging.client.id": "pub",
		"hoardmq.namespace": "default", "hoardmq.node": "n1", "hoardmq.qos": "1", "hoardmq.matched": "1"}
	for k, v := range want {
		if got, ok := sp.Attr(k); !ok || got != v {
			t.Errorf("attribute %s = %q (set %v), want %q", k, got, ok, v)
		}
	}
	for _, a := range sp.Attrs {
		if strings.Contains(a.Value(), "hunter2") || strings.Contains(a.Value(), "the-payload") {
			t.Errorf("attribute %s carries a payload or property value: %q", a.Key, a.Value())
		}
	}
	if routed.SpanID != sp.SpanID || routed.TraceID != sp.TraceID || !routed.Sampled() {
		t.Errorf("the routed message carried %+v, the span is %x/%x", routed, sp.TraceID, sp.SpanID)
	}
	if got := sub.lastProperties(); len(got) != 2 || got[0] != props[0] || got[1] != props[1] {
		t.Errorf("the subscriber received properties %v, want %v unaltered", got, props)
	}

	// Refused: the span records the reason as an error.
	m2, _ := newManager(t, func(o *Options) {
		o.Tracer = otel.New(otel.Config{Ratio: 1, Node: "n1", Sink: rec.Record})
	})
	stranger := newFake("stranger", packet.V5, 100)
	if code, _ := m2.Publish(stranger, &packet.Publish{Topic: "t/x", QoS: 1, PacketID: 1}); !code.IsError() {
		t.Fatalf("a publish from an unknown connection answered %v", code)
	}
	last := rec.Named("publish")
	if got := last[len(last)-1]; got.Status != otel.StatusError {
		t.Errorf("a refused publish's span has status %v", got.Status)
	} else if v, _ := got.Attr("mqtt.reason_code"); v != "128" {
		t.Errorf("reason code attribute %q", v)
	}
}

// traceForwarder stands for a routing plane: it reads the span context the
// message carries while it is routed.
type traceForwarder struct{ got *otel.SpanContext }

func (f traceForwarder) Forward(msg *message.Message) (int, error) {
	*f.got = msg.Trace
	return 0, nil
}

// lastProperties is the user properties of the last PUBLISH delivered.
func (f *fakeConn) lastProperties() []packet.UserProperty {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := len(f.sent) - 1; i >= 0; i-- {
		if p, ok := f.sent[i].(*packet.Publish); ok {
			return p.Properties.UserProperties
		}
	}
	return nil
}
