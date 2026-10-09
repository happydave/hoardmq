package builtin

import (
	"testing"

	"github.com/happydave/hoardmq/internal/hook"
	"github.com/happydave/hoardmq/internal/module"
)

// BenchmarkCountPerEvent is what one per-event counter costs metrics.internal
// as the session manager calls it today (a label map built per call) and with
// a label map built once: the input to WI 1323's decision on pre-built maps.
var countSink hook.Metrics

func BenchmarkCountPerEvent(b *testing.B) {
	// Called through an interface value the compiler cannot see through, as
	// the session manager calls it, so the label map escapes as it does there.
	countSink = &metrics{series: map[string]*module.Metric{}}
	m := countSink
	ns, qos := "default", "1"
	b.Run("label-map-per-call", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			m.Add("hoardmq_publishes_total", map[string]string{"namespace": ns, "qos": qos}, 1)
		}
	})
	prebuilt := map[string]string{"namespace": ns, "qos": qos}
	b.Run("label-map-prebuilt", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			m.Add("hoardmq_publishes_total", prebuilt, 1)
		}
	})
}
