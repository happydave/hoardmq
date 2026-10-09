package routing

import (
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/otel"
)

// BenchmarkForwardTracing is one forward's send with tracing off, and with a
// sampled publish on a node that traces.
func BenchmarkForwardTracing(b *testing.B) {
	sampled, _ := otel.ParseTraceparent(sampledParent)
	for _, arm := range []struct {
		name  string
		tr    *otel.Tracer
		trace otel.SpanContext
	}{
		{"off", nil, otel.SpanContext{}},
		{"sampled", otel.New(otel.Config{Ratio: 1, Node: "a", Sink: func(otel.SpanData) {}}), sampled},
	} {
		b.Run(arm.name, func(b *testing.B) {
			tp := &nopTransport{}
			tp.handlers = map[transport.Class]transport.Handler{}
			p := New(Options{Node: "a", Transport: tp, SyncInterval: time.Hour, Logger: quiet, Tracer: arm.tr})
			msg := qos1("t/x")
			msg.Trace = arm.trace
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				msg.Forwards = msg.Forwards[:0]
				p.send("b", fwdPublish, "", msg)
			}
		})
	}
}
