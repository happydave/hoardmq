package replication

import (
	"math/bits"
	"sync"
	"sync/atomic"
	"time"
)

// What holds the follower's lock, and for how long, for the failover
// measurement: a worker applying a record waits for it, so a long hold is time
// no acknowledgement leaves. Nothing reads it to decide.

// Holders of the follower's lock.
const (
	holdApply             = iota // a worker applying a record
	holdApplyOther               // a drop or a promotion applying one outside a worker
	holdSnapshotRebalance        // the rebalance copying the replica state
	holdSnapshotOther            // any other copy of the replica state
	holdCheckpoint               // a replica-store checkpoint copying the state
	holdFind                     // a fetch or a check reading one session
	holdKinds
)

var holdNames = [holdKinds]string{"apply", "apply-other", "snapshot-rebalance", "snapshot-other", "checkpoint", "find"}

// longLockHold is the hold from which one is kept as a sample.
const longLockHold = 20 * time.Millisecond

// LockHolds is the picture of the follower's lock a storm artifact carries.
type LockHolds struct {
	ByKind map[string]LockHoldKind `json:"by_kind"`
	// Long are the holds of longLockHold or more: the first ones, then the
	// latest.
	Long []LockHold `json:"long,omitempty"`
}

// LockHoldKind sums one holder's holds.
type LockHoldKind struct {
	Count   int64 `json:"count"`
	TotalNs int64 `json:"total_ns"`
	MaxNs   int64 `json:"max_ns"`
	MaxAtNs int64 `json:"max_at_ns"`
	// HoldP99Ns and WaitP99Ns are the 99th percentiles of the holds and of
	// the waits for the lock, each the upper bound of a power-of-two bucket
	// of microseconds; WaitCount, WaitTotalNs and WaitMaxNs count, sum and
	// bound the waits.
	HoldP99Ns   int64 `json:"hold_p99_ns,omitempty"`
	WaitCount   int64 `json:"wait_count,omitempty"`
	WaitTotalNs int64 `json:"wait_total_ns,omitempty"`
	WaitMaxNs   int64 `json:"wait_max_ns,omitempty"`
	WaitP99Ns   int64 `json:"wait_p99_ns,omitempty"`
}

// histBuckets bounds the power-of-two buckets of microseconds a hold or a wait
// is counted in; the last takes everything longer.
const histBuckets = 32

// bucketOf is the bucket of a duration: 0 below a microsecond, then b for
// [2^(b-1), 2^b) microseconds.
func bucketOf(d int64) int {
	b := bits.Len64(uint64(max(d, 0) / 1000))
	return min(b, histBuckets-1)
}

// countOf is the samples in h.
func countOf(h *[histBuckets]atomic.Int64) int64 {
	var n int64
	for i := range h {
		n += h[i].Load()
	}
	return n
}

// p99Of is the upper bound, in nanoseconds, of the bucket holding the 99th
// percentile of h; 0 for an empty histogram.
func p99Of(h *[histBuckets]atomic.Int64) int64 {
	var total int64
	for i := range h {
		total += h[i].Load()
	}
	if total == 0 {
		return 0
	}
	want := (total*99 + 99) / 100
	var seen int64
	for i := range h {
		if seen += h[i].Load(); seen >= want {
			return int64(1) << i * 1000
		}
	}
	return int64(1) << (histBuckets - 1) * 1000
}

// LockHold is one long hold: who, when it began, and how long.
type LockHold struct {
	Kind   string `json:"kind"`
	AtNs   int64  `json:"at_ns"`
	HeldNs int64  `json:"held_ns"`
}

type lockHoldCounters struct {
	count, total, max, maxAt [holdKinds]atomic.Int64
	holds, waits             [holdKinds][histBuckets]atomic.Int64
	waitTotal, waitMax       [holdKinds]atomic.Int64

	mu         sync.Mutex
	head, tail []LockHold
	next       int
}

// held records one hold of kind that began at began and ends now.
func (c *lockHoldCounters) held(kind int, began time.Time) {
	d := time.Since(began).Nanoseconds()
	c.holds[kind][bucketOf(d)].Add(1)
	c.count[kind].Add(1)
	c.total[kind].Add(d)
	for {
		m := c.max[kind].Load()
		if d <= m {
			break
		}
		if c.max[kind].CompareAndSwap(m, d) {
			c.maxAt[kind].Store(began.UnixNano())
			break
		}
	}
	if d >= longLockHold.Nanoseconds() {
		c.mu.Lock()
		c.head, c.tail, c.next = keepSample(c.head, c.tail, c.next, LockHold{Kind: holdNames[kind], AtNs: began.UnixNano(), HeldNs: d})
		c.mu.Unlock()
	}
}

func (c *lockHoldCounters) stats() LockHolds {
	out := LockHolds{ByKind: map[string]LockHoldKind{}}
	for k := 0; k < holdKinds; k++ {
		if n := c.count[k].Load(); n != 0 {
			out.ByKind[holdNames[k]] = LockHoldKind{Count: n, TotalNs: c.total[k].Load(), MaxNs: c.max[k].Load(), MaxAtNs: c.maxAt[k].Load(),
				HoldP99Ns: p99Of(&c.holds[k]), WaitCount: countOf(&c.waits[k]), WaitTotalNs: c.waitTotal[k].Load(), WaitMaxNs: c.waitMax[k].Load(), WaitP99Ns: p99Of(&c.waits[k])}
		}
	}
	c.mu.Lock()
	out.Long = append(append([]LockHold(nil), c.head...), ringOrder(c.tail, c.next)...)
	c.mu.Unlock()
	return out
}

// waited records a wait of d for the lock by a holder of kind.
func (c *lockHoldCounters) waited(kind int, d time.Duration) {
	n := d.Nanoseconds()
	c.waits[kind][bucketOf(n)].Add(1)
	c.waitTotal[kind].Add(n)
	for {
		m := c.waitMax[kind].Load()
		if n <= m || c.waitMax[kind].CompareAndSwap(m, n) {
			return
		}
	}
}
