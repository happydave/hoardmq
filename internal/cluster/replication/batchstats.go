package replication

import (
	"sync"
	"sync/atomic"
	"time"
)

// bigBatch is the batch length from which a per-peer worker's batch is
// counted by frame kind. A worker takes whatever its queue holds in one go, so
// a batch this long means the peer sent that much while one batch was being
// applied, and everything that peer waits on — an append's acknowledgement
// above all — waits behind it.
const bigBatch = 10000

// kindNames labels the frame kinds a batch is counted by; an index past the
// end counts as "other".
var kindNames = [...]string{"", "append", "snapshot", "fetch", "fetch-result", "retained-write", "retained-query", "retained-result", "retained-presence", "resync-mark", "retained-release"}

// batchCounters describe the big batches this node's per-peer workers took.
type batchCounters struct {
	batches, frames, nanos atomic.Int64
	byKind                 [len(kindNames) + 1]atomic.Int64

	// maxSeen mirrors maxLen so a batch that is neither big nor the largest
	// costs two comparisons and no lock.
	maxSeen  atomic.Int64
	mu       sync.Mutex // the largest batch of any length, replaced whole
	maxLen   int
	maxKinds [len(kindNames) + 1]int64
	maxNanos int64
	maxPeer  string
	// The batches timed part by part (batchtrace.go): the first ones, and a
	// ring of the latest.
	head, tail []BatchSample
	next       int
}

// BatchStats is the picture of those batches a storm artifact carries.
type BatchStats struct {
	// Batches of bigBatch frames or more, the frames in them, and the time
	// applying them took: no acknowledgement for the peer's stream leaves
	// while one is being applied.
	Batches int64            `json:"big_batches"`
	Frames  int64            `json:"big_batch_frames"`
	Ms      float64          `json:"big_batch_ms_total"`
	ByKind  map[string]int64 `json:"big_batch_frames_by_kind"`
	// The largest batch: its length, what it was made of, how long it took
	// and whose stream it was.
	MaxLen    int              `json:"max_batch_len"`
	MaxByKind map[string]int64 `json:"max_batch_by_kind"`
	MaxMs     float64          `json:"max_batch_ms"`
	MaxPeer   string           `json:"max_batch_peer"`
	// Samples are the batches of batchSampleMin frames or more, timed part by
	// part: the first ones, then the latest.
	Samples []BatchSample `json:"samples,omitempty"`
	// LockHolds is what held the follower's lock, which a worker applying a
	// record waits for.
	LockHolds *LockHolds `json:"lock_holds,omitempty"`
}

func kindIndex(k byte) int {
	if int(k) < len(kindNames) && kindNames[k] != "" {
		return int(k)
	}
	return len(kindNames)
}

func kindLabel(i int) string {
	if i < len(kindNames) {
		return kindNames[i]
	}
	return "other"
}

// note counts one batch a worker has applied: by kind when it is big, and as
// the largest seen whatever its length.
func (c *batchCounters) note(peer string, batch []frame, took time.Duration) {
	big := len(batch) >= bigBatch
	if !big && int64(len(batch)) <= c.maxSeen.Load() {
		return
	}
	var kinds [len(kindNames) + 1]int64
	for i := range batch {
		kinds[kindIndex(batch[i].Kind)]++
	}
	if big {
		c.batches.Add(1)
		c.frames.Add(int64(len(batch)))
		c.nanos.Add(int64(took))
		for i, n := range kinds {
			if n != 0 {
				c.byKind[i].Add(n)
			}
		}
	}
	c.mu.Lock()
	if len(batch) > c.maxLen {
		c.maxLen, c.maxKinds, c.maxNanos, c.maxPeer = len(batch), kinds, int64(took), peer
		c.maxSeen.Store(int64(len(batch)))
	}
	c.mu.Unlock()
}

func (c *batchCounters) stats() BatchStats {
	out := BatchStats{Batches: c.batches.Load(), Frames: c.frames.Load(), Ms: float64(c.nanos.Load()) / 1e6,
		ByKind: map[string]int64{}, MaxByKind: map[string]int64{}}
	for i := range c.byKind {
		if n := c.byKind[i].Load(); n != 0 {
			out.ByKind[kindLabel(i)] = n
		}
	}
	c.mu.Lock()
	out.MaxLen, out.MaxMs, out.MaxPeer = c.maxLen, float64(c.maxNanos)/1e6, c.maxPeer
	for i, n := range c.maxKinds {
		if n != 0 {
			out.MaxByKind[kindLabel(i)] = n
		}
	}
	out.Samples = append(append([]BatchSample(nil), c.head...), ringOrder(c.tail, c.next)...)
	c.mu.Unlock()
	return out
}

// BatchStats reports the big batches this node's per-peer workers have taken.
func (r *Replicator) BatchStats() BatchStats {
	st := r.batches.stats()
	h := r.follower.lockHolds.stats()
	st.LockHolds = &h
	return st
}
