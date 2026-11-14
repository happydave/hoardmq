package lease

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

// TestFirstAcquireMeasure times, on n2 of the three-node fixture, acquires of
// a new client each (a first acquire, whose grant leaves the lease n2's) and
// acquires of a lease n2 already owns, one at a time, by testing.Benchmark.
// Set HOARDMQ_FIRSTACQUIRE_MEASURE to run it.
func TestFirstAcquireMeasure(t *testing.T) {
	if os.Getenv("HOARDMQ_FIRSTACQUIRE_MEASURE") == "" {
		t.Skip("set HOARDMQ_FIRSTACQUIRE_MEASURE to run the first-acquire measurement")
	}
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	n2 := c.nodes["n2"]
	if _, err := acquire(t, n2, "owned", nil, 300, false); err != nil {
		t.Fatalf("the cluster does not serve an acquire before the measurement: %v", err)
	}
	var seq, failed, done atomic.Int64
	run := func(id func() string) testing.BenchmarkResult {
		return testing.Benchmark(func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_, err := n2.svc.Acquire(ctx, "default", id(), nil, 300, false)
				cancel()
				done.Add(1)
				if err != nil {
					failed.Add(1)
				}
			}
		})
	}
	first := run(func() string { return fmt.Sprintf("f%d", seq.Add(1)) })
	owned := run(func() string { return "owned" })
	if failed.Load() != 0 || first.N == 0 || owned.N == 0 {
		t.Fatalf("%d of %d acquires failed; first %d, owned %d iterations", failed.Load(), done.Load(), first.N, owned.N)
	}
	t.Logf("first-acquire ns/op %d n %d", first.NsPerOp(), first.N)
	t.Logf("owned-acquire ns/op %d n %d", owned.NsPerOp(), owned.N)
}
