package store

import (
	"sync/atomic"
	"time"

	extstore "github.com/happydave/hoardmq/ext/store"
)

// AppendStats is the extension API's append statistics.
type AppendStats = extstore.AppendStats

type appendCounters struct {
	appends, heldNs, tapNs, heldMaxNs atomic.Int64
}

// note records one append. Its caller holds the store's lock, so the longest
// hold has one writer.
func (c *appendCounters) note(held, tap time.Duration) {
	c.appends.Add(1)
	c.heldNs.Add(int64(held))
	c.tapNs.Add(int64(tap))
	if int64(held) > c.heldMaxNs.Load() {
		c.heldMaxNs.Store(int64(held))
	}
}

func (c *appendCounters) stats() AppendStats {
	return AppendStats{
		Appends:   c.appends.Load(),
		HeldSec:   time.Duration(c.heldNs.Load()).Seconds(),
		TapSec:    time.Duration(c.tapNs.Load()).Seconds(),
		HeldMaxMs: float64(c.heldMaxNs.Load()) / float64(time.Millisecond),
	}
}

// AppendStats reports the append counters, lock-free.
func (s *Store) AppendStats() AppendStats { return s.appends.stats() }
