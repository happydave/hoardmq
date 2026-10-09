package replication

import (
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
}

// LockHold is one long hold: who, when it began, and how long.
type LockHold struct {
	Kind   string `json:"kind"`
	AtNs   int64  `json:"at_ns"`
	HeldNs int64  `json:"held_ns"`
}

type lockHoldCounters struct {
	count, total, max, maxAt [holdKinds]atomic.Int64

	mu         sync.Mutex
	head, tail []LockHold
	next       int
}

// held records one hold of kind that began at began and ends now.
func (c *lockHoldCounters) held(kind int, began time.Time) {
	d := time.Since(began).Nanoseconds()
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
			out.ByKind[holdNames[k]] = LockHoldKind{Count: n, TotalNs: c.total[k].Load(), MaxNs: c.max[k].Load(), MaxAtNs: c.maxAt[k].Load()}
		}
	}
	c.mu.Lock()
	out.Long = append(append([]LockHold(nil), c.head...), ringOrder(c.tail, c.next)...)
	c.mu.Unlock()
	return out
}
