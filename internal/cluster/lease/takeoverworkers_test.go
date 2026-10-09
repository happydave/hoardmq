package lease

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// TestTakeoverQueueServesItsWorkersWorthOfKeysAtOnce: takeover passes for
// different clients run at the same time, up to the queue's worker count.
// That is what the count buys — at 30,000 clients a pull spent 332 ms of its
// 350 ms waiting for a worker, against 4.2 ms for the pass itself (WI 1691) —
// and it is what a build with fewer workers cannot do.
func TestTakeoverQueueServesItsWorkersWorthOfKeysAtOnce(t *testing.T) {
	c := newCluster(t, []string{"n1", "n2"}, 2*time.Second, nil)
	n := c.nodes["n1"]
	release := make(chan struct{})
	var inFlight, peak atomic.Int64
	n.sessions.setOnTake(func(string, string) {
		raise(&peak, inFlight.Add(1))
		<-release
		inFlight.Add(-1)
	})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})

	// A literal count, not the constant under test: a test that scales with
	// it passes at any worker count and proves nothing (seen, here, by the
	// control that did not fail).
	const wantAtOnce, enqueued = 16, 24
	for i := 0; i < enqueued; i++ {
		id := fmt.Sprintf("c%d", i)
		n.svc.takeovers.enqueue(key{"default", id}, pendingTakeover{
			req:      takeoverRequest{Req: uint64(i + 1), Namespace: "default", ClientID: id, Epoch: 1},
			peer:     "n2",
			queuedAt: time.Now(),
		})
	}
	waitFor(t, fmt.Sprintf("%d takeover passes in flight at once", wantAtOnce), 5*time.Second,
		func() bool { return inFlight.Load() >= wantAtOnce })
	close(release)
	waitFor(t, "every pass to finish", 5*time.Second, func() bool { return n.svc.TakeoverStats().Passes == enqueued })
	if got := peak.Load(); got < wantAtOnce {
		t.Fatalf("at most %d takeover passes ever ran at once, want %d", got, wantAtOnce)
	}
}
