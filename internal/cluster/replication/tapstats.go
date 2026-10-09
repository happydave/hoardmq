package replication

import (
	"sync/atomic"
	"time"

	"github.com/happydave/hoardmq/internal/store"
)

// TapStats is what Tap spent on session records. Tap runs inside the node's
// own store's lock, so every second here is a second no other record of this
// node's could be appended.
type TapStats struct {
	Calls    int64   `json:"session_records"`
	PushSec  float64 `json:"push_sec_total"`
	CountSec float64 `json:"count_sec_total"`
}

// DropStats is the replica-copy drop an owner's session upsert asks for.
// It runs after the store's lock is released (WI 1688), so these seconds are
// the appending goroutine's own and not a second of anyone else's wait.
type DropStats struct {
	// Checks are the session upserts this node wrote as owner, each of which
	// looks for a replica copy to drop; Dropped those that found one and
	// appended its delete to the replica store; Kept those whose copy was a
	// later owner's and was left alone.
	Checks  int64   `json:"checks"`
	Dropped int64   `json:"dropped"`
	Kept    int64   `json:"kept"`
	Sec     float64 `json:"sec_total"`
}

type tapCounters struct {
	calls, pushNs, countNs atomic.Int64
}

type dropCounters struct {
	checks, dropped, kept, ns atomic.Int64
}

func atomicSec(n *atomic.Int64) float64 { return time.Duration(n.Load()).Seconds() }

func (c *tapCounters) stats() TapStats {
	return TapStats{Calls: c.calls.Load(), PushSec: atomicSec(&c.pushNs), CountSec: atomicSec(&c.countNs)}
}

func (c *dropCounters) stats() DropStats {
	return DropStats{Checks: c.checks.Load(), Dropped: c.dropped.Load(), Kept: c.kept.Load(), Sec: atomicSec(&c.ns)}
}

// TapStats reports the tap's counters, lock-free.
func (r *Replicator) TapStats() TapStats { return r.taps.stats() }

// DropStats reports the owned-copy drop's counters, lock-free.
func (r *Replicator) DropStats() DropStats { return r.drops.stats() }

// ReplicaAppendStats reports the replica store's append counters.
func (r *Replicator) ReplicaAppendStats() store.AppendStats { return r.o.Replica.AppendStats() }
