package sim

import (
	"testing"

	"github.com/happydave/hoardmq/internal/otel/otlptest"
)

// startTraceEndpoint starts the in-process OTLP endpoint a tracing run's
// nodes export to. At the cluster's end it reports what it received, and a
// request that broke a rule of OTLP's JSON fails the run.
func startTraceEndpoint(t testing.TB) *otlptest.Collector {
	t.Helper()
	c := otlptest.NewUnmanaged()
	t.Cleanup(func() {
		vs := c.Violations()
		t.Logf("trace endpoint: %d requests, %d spans, %d violations", c.Requests(), len(c.Spans()), len(vs))
		if len(vs) > 0 {
			t.Errorf("the nodes exported spans OTLP's JSON rules refuse, first: %v", vs[0])
		}
		c.Close()
	})
	return c
}
