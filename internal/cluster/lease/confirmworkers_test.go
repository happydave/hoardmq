package lease

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// TestConfirmedQueueActsOnItsWorkersWorthAtOnce: confirmations for different
// clients are acted on at the same time, up to the queue's worker count. A
// pass that drops a sealed copy waits for the journal's group fsync, so the
// passes in flight at once are what one fsync releases; with four, a node
// loss's burst of confirmations queued for up to 1.6 s at 10,000 clients
// (WI 1834) and the moves they end refused meanwhile.
func TestConfirmedQueueActsOnItsWorkersWorthAtOnce(t *testing.T) {
	var ended atomic.Int64
	fixtureOptions = func(o *Options) {
		o.MoveEnded = func(string, string, string, uint64) { ended.Add(1) }
	}
	t.Cleanup(func() { fixtureOptions = nil })
	c := newCluster(t, []string{"n1", "n2"}, 2*time.Second, nil)
	n := c.nodes["n1"]
	release := make(chan struct{})
	var inFlight, peak atomic.Int64
	n.sessions.setOnDrop(func(string, string) {
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
	// it passes at any worker count.
	const wantAtOnce, enqueued = 64, 80
	for i := 0; i < enqueued; i++ {
		id := fmt.Sprintf("c%d", i)
		n.sessions.seal("default", id, 5)
		n.svc.confirmed.enqueue(key{"default", id}, confirmIn{token: 5, peer: "n2", fromSeq: 1, at: time.Now()})
	}
	waitFor(t, fmt.Sprintf("%d confirmations acted on at once", wantAtOnce), 5*time.Second,
		func() bool { return inFlight.Load() >= wantAtOnce })
	close(release)
	waitFor(t, "every seal to be dropped", 5*time.Second, func() bool { return n.sessions.sealedCount() == 0 })
	waitFor(t, "every move to be ended", 5*time.Second, func() bool { return ended.Load() >= enqueued })
	if got := peak.Load(); got < wantAtOnce {
		t.Fatalf("at most %d confirmations were ever acted on at once, want %d", got, wantAtOnce)
	}
	if got := ended.Load(); got != enqueued {
		t.Fatalf("MoveEnded called %d times for %d confirmations", got, enqueued)
	}
}

// TestConfirmationActedOnRecordsItsWaitSinceReceipt: a confirmation acted on
// is counted with its time since it was received, which covers its wait in
// the queue; one that finds nothing to act on is not.
func TestConfirmationActedOnRecordsItsWaitSinceReceipt(t *testing.T) {
	c := newCluster(t, []string{"n1", "n2"}, 2*time.Second, nil)
	n := c.nodes["n1"]
	n.sessions.seal("default", "x", 5)
	n.svc.confirmed.enqueue(key{"default", "x"}, confirmIn{token: 5, peer: "n2", fromSeq: 1, at: time.Now().Add(-500 * time.Millisecond)})
	waitFor(t, "the seal to be dropped", 5*time.Second, func() bool { return n.sessions.sealedCount() == 0 })
	waitFor(t, "the act to be counted", 5*time.Second, func() bool { return n.svc.LivenessStats().ConfirmsActed >= 1 })
	st := n.svc.LivenessStats()
	if st.ConfirmsActed != 1 || st.ConfirmActMsMax < 500 || st.ConfirmActMsTotal < 500 {
		t.Fatalf("after one confirmation received 500 ms before it was acted on: acted %d, max %.1f ms, total %.1f ms",
			st.ConfirmsActed, st.ConfirmActMsMax, st.ConfirmActMsTotal)
	}

	ignored := st.ConfirmsIgnored
	n.svc.confirmed.enqueue(key{"default", "nothing-here"}, confirmIn{token: 5, peer: "n2", fromSeq: 1, at: time.Now().Add(-time.Second)})
	waitFor(t, "the confirmation for nothing to be ignored", 5*time.Second,
		func() bool { return n.svc.LivenessStats().ConfirmsIgnored > ignored })
	if st := n.svc.LivenessStats(); st.ConfirmsActed != 1 || st.ConfirmActMsMax >= 1000 {
		t.Fatalf("a confirmation that acted on nothing was counted: acted %d, max %.1f ms", st.ConfirmsActed, st.ConfirmActMsMax)
	}
}
