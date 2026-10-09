package replication

import (
	"testing"
	"time"
)

// TestBatchTimingWaitsAfterApply: each frame's wait runs from its apply to the
// acknowledgement that covered it, so two settles in one batch give each part
// its own acknowledgement.
func TestBatchTimingWaitsAfterApply(t *testing.T) {
	began := time.Unix(100, 0)
	bt := newBatchTiming("n1", make([]frame, 4), began)
	at := func(ms int) time.Time { return began.Add(time.Duration(ms) * time.Millisecond) }
	bt.frameDone(at(10))
	bt.frameDone(at(20))
	bt.acknowledged(at(30)) // waits 20 and 10
	bt.frameDone(at(40))
	bt.frameDone(at(100))
	bt.acknowledged(at(110)) // waits 70 and 10
	bt.acknowledged(at(120)) // covers nothing new
	s := bt.sample(at(120))
	if s.Settles != 3 {
		t.Fatalf("settles %d, want 3", s.Settles)
	}
	if want := int64(110 * time.Millisecond / 4); s.AfterApplyMeanNs != want {
		t.Fatalf("mean wait after apply %v, want %v", time.Duration(s.AfterApplyMeanNs), time.Duration(want))
	}
	if want := int64(70 * time.Millisecond); s.AfterApplyMaxNs != want {
		t.Fatalf("longest wait after apply %v, want %v", time.Duration(s.AfterApplyMaxNs), time.Duration(want))
	}
	if want := int64(120 * time.Millisecond); s.TookNs != want {
		t.Fatalf("took %v, want %v", time.Duration(s.TookNs), time.Duration(want))
	}
}

// TestWorkerSamplesOnlyLongBatches: a batch of batchSampleMin frames is kept as
// a sample with its kinds and its one settle; a shorter one is not.
func TestWorkerSamplesOnlyLongBatches(t *testing.T) {
	h, _ := withCounters(t, []string{"n1", "n2"}, 2, 2)
	rep := h.nodes["n2"].rep
	w := newWorker(rep.follower, "n1")
	defer w.close()
	marks := func(from, n int) []frame {
		out := make([]frame, n)
		for i := range out {
			out[i] = frame{Kind: kindResyncMark, Seq: uint64(from + i), queuedAt: time.Now()}
		}
		return out
	}
	w.process(marks(1, batchSampleMin-1))
	if got := rep.BatchStats().Samples; len(got) != 0 {
		t.Fatalf("a batch of %d frames was sampled: %+v", batchSampleMin-1, got)
	}
	w.process(marks(batchSampleMin, batchSampleMin))
	got := rep.BatchStats().Samples
	if len(got) != 1 {
		t.Fatalf("samples %d, want 1", len(got))
	}
	s := got[0]
	if s.Peer != "n1" || s.Frames != batchSampleMin || s.ByKind["resync-mark"] != batchSampleMin || s.Settles != 1 {
		t.Fatalf("sample %+v", s)
	}
	if s.AfterApplyMaxNs < s.AfterApplyMeanNs || s.AfterApplyMeanNs <= 0 || s.TookNs < s.AfterApplyMaxNs {
		t.Fatalf("waits after apply: mean %d, max %d, took %d", s.AfterApplyMeanNs, s.AfterApplyMaxNs, s.TookNs)
	}
}

// TestLockHoldsKeepTheLongOnes: every hold is counted by its holder, and one
// of longLockHold or more is kept as a sample.
func TestLockHoldsKeepTheLongOnes(t *testing.T) {
	var c lockHoldCounters
	now := time.Now()
	c.held(holdFind, now)
	c.held(holdSnapshotRebalance, now.Add(-2*longLockHold))
	st := c.stats()
	if st.ByKind["find"].Count != 1 || st.ByKind["snapshot-rebalance"].Count != 1 || len(st.ByKind) != 2 {
		t.Fatalf("by kind %+v", st.ByKind)
	}
	if len(st.Long) != 1 || st.Long[0].Kind != "snapshot-rebalance" || st.Long[0].HeldNs < 2*longLockHold.Nanoseconds() {
		t.Fatalf("long holds %+v", st.Long)
	}
	if st.ByKind["snapshot-rebalance"].MaxNs != st.Long[0].HeldNs || st.ByKind["snapshot-rebalance"].MaxAtNs != now.Add(-2*longLockHold).UnixNano() {
		t.Fatalf("the longest snapshot hold %+v against %+v", st.ByKind["snapshot-rebalance"], st.Long[0])
	}
}
