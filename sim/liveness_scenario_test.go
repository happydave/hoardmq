package sim

import (
	"testing"
	"time"
)

// TestStreamRecoversAfterTwoQuickBreaks runs the shape WI 1491's defect
// needed: one stream broken, its resync refused by the cooldown, then broken
// again — after which nothing asked for a resync and the stream stayed holed,
// so every barrier for that peer failed and the node refused every QoS 1/2
// publish for the rest of the run.
//
// The schedule is explicit rather than seeded because no generated seed
// produces two breaks on one pair inside a cooldown; that is why the corpus
// never caught any of the four defects this property was written for.
//
// **This test is the vehicle for WI 1502's negative controls.** It passes on
// the current build, which says only that the build is sound. What gives it
// teeth is that it fails with each of the four fixes reverted, and that is
// recorded in the work item's `test.md` rather than here.
func TestStreamRecoversAfterTwoQuickBreaks(t *testing.T) {
	if testing.Short() {
		t.Skip("a fault run with a settle period")
	}
	// Two short partitions of the same node, close enough together that the
	// second stream break lands while the first resync is still inside its
	// cooldown (the cooldown is the acknowledgement timeout, 1 s here).
	//
	// **Partition, not drop.** A dropped frame is reported to the sender as a
	// successful write (`link.go`: `if fault.drops() { ...; return true }`),
	// so nothing fails, `broken()` is never called and no hole forms — the
	// peer simply never acknowledges. A hole needs a real send failure, which
	// is what tearing the link down gives. The first version of this test used
	// `drop` and could not reproduce the defect at all.
	// A third partition, healing as the workload stops so the stream goes quiet
	// immediately after a break, makes this run fail on the current build —
	// reproducibly, 3 of 3, with a stream holed and awaiting a resync that
	// never completes. That is a product defect and is filed as **WI 1506**,
	// with this schedule as its reproduction. It is deliberately not part of
	// the shipped test: a corpus test that fails on the current build is not a
	// test, it is a red gate.
	schedule := []Step{
		{At: 500 * time.Millisecond, Kind: "partition", Node: "n2", Dur: 300 * time.Millisecond},
		{At: 1000 * time.Millisecond, Kind: "partition", Node: "n2", Dur: 300 * time.Millisecond},
	}
	r := Run(t, RunOptions{
		Seed:     1502,
		Window:   6 * time.Second,
		Settle:   12 * time.Second,
		Schedule: schedule,
	})
	// The property is only evaluated when the cluster is whole; if it was
	// skipped, this test proves nothing and must say so rather than pass.
	if r.Stats["livenessProbes"] == 0 {
		t.Fatal("the liveness property was not evaluated: no probe was taken, so this run says nothing about recovery")
	}
	// Asked of the checker directly, so the failure names the stream even if
	// another property also fired.
	if v := livenessFindings(r.Liveness, r.BarrierFailures); len(v) != 0 {
		for _, one := range v {
			t.Errorf("a stream did not recover: %s", one.Detail)
		}
	}
	if t.Failed() {
		t.Logf("stats: %v", r.Stats)
	}
}
