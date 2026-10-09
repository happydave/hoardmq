package replication

import (
	"time"
)

// batchSampleMin is the batch length from which a worker's batch is timed part
// by part and kept as a sample. Below it a batch costs the worker nothing more
// than before.
const batchSampleMin = 1000

// BatchSample is one batch of batchSampleMin frames or more that a per-peer
// worker took: what it held, where its time went, and how long its frames
// waited for the acknowledgement after they were applied. Nothing reads it to
// decide.
type BatchSample struct {
	Peer   string         `json:"peer"`
	AtNs   int64          `json:"at_ns"`   // when the worker took the batch
	TookNs int64          `json:"took_ns"` // from then to its last acknowledgement
	Frames int            `json:"frames"`
	ByKind map[string]int `json:"by_kind"`
	// NsByKind is the time spent on each kind's frames, store syncs excluded.
	NsByKind map[string]int64 `json:"ns_by_kind"`
	// SnapshotRecords is how many records the batch's snapshots wrote.
	SnapshotRecords int `json:"snapshot_records,omitempty"`
	// Inside those: the lease checks, waiting for the follower's lock, the
	// replica store's appends, and materialising the records.
	StaleNs    int64 `json:"stale_ns"`
	LockWaitNs int64 `json:"lock_wait_ns"`
	AppendNs   int64 `json:"append_ns"`
	ApplyNs    int64 `json:"apply_ns"`
	// SyncNs is the store syncs the batch's acknowledgements waited for, and
	// Settles how many acknowledgements it sent.
	SyncNs  int64 `json:"sync_ns"`
	Settles int   `json:"settles"`
	// OldestQueuedNs is how long the batch's first frame had been queued when
	// the worker took it.
	OldestQueuedNs int64 `json:"oldest_queued_ns"`
	// From a sequenced frame's apply to the acknowledgement that covered it:
	// the mean over the batch's frames, and the longest.
	AfterApplyMeanNs int64 `json:"after_apply_mean_ns"`
	AfterApplyMaxNs  int64 `json:"after_apply_max_ns"`
}

// batchTiming accumulates one sampled batch on its worker's goroutine.
type batchTiming struct {
	s     BatchSample
	began time.Time
	// The frames applied since the last acknowledgement: how many, the sum of
	// their apply times since began, and the first of them.
	pending   int64
	doneSum   int64
	firstDone int64
	// What has been acknowledged: frames and the sum of their waits.
	ackedFrames int64
	waitSum     int64
}

func newBatchTiming(peer string, batch []frame, began time.Time) *batchTiming {
	t := &batchTiming{began: began, s: BatchSample{Peer: peer, AtNs: began.UnixNano(), Frames: len(batch),
		ByKind: map[string]int{}, NsByKind: map[string]int64{}}}
	for i := range batch {
		t.s.ByKind[kindLabel(kindIndex(batch[i].Kind))]++
		if q := batch[i].queuedAt; !q.IsZero() {
			if d := began.Sub(q).Nanoseconds(); d > t.s.OldestQueuedNs {
				t.s.OldestQueuedNs = d
			}
		}
	}
	return t
}

// frameDone records a sequenced frame handled, for its wait to the
// acknowledgement that covers it.
func (t *batchTiming) frameDone(now time.Time) {
	at := now.Sub(t.began).Nanoseconds()
	if t.pending == 0 {
		t.firstDone = at
	}
	t.pending++
	t.doneSum += at
}

// acknowledged records an acknowledgement covering every frame handled since
// the last one.
func (t *batchTiming) acknowledged(now time.Time) {
	t.s.Settles++
	if t.pending == 0 {
		return
	}
	at := now.Sub(t.began).Nanoseconds()
	t.waitSum += t.pending*at - t.doneSum
	t.ackedFrames += t.pending
	if d := at - t.firstDone; d > t.s.AfterApplyMaxNs {
		t.s.AfterApplyMaxNs = d
	}
	t.pending, t.doneSum = 0, 0
}

func (t *batchTiming) sample(end time.Time) BatchSample {
	t.s.TookNs = end.Sub(t.began).Nanoseconds()
	if t.ackedFrames > 0 {
		t.s.AfterApplyMeanNs = t.waitSum / t.ackedFrames
	}
	return t.s
}

// keepBatch adds a sampled batch to the node's samples: the first ones, and a
// ring of the latest.
func (c *batchCounters) keepBatch(s BatchSample) {
	c.mu.Lock()
	c.head, c.tail, c.next = keepSample(c.head, c.tail, c.next, s)
	c.mu.Unlock()
}
