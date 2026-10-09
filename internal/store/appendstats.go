package store

import (
	"sync/atomic"
	"time"
)

// AppendStats is what the store's lock was held for by appends, readable
// without that lock. Held covers the log write and the tap; the log write is
// Held less Tap.
type AppendStats struct {
	Appends   int64   `json:"appends"`
	HeldSec   float64 `json:"held_sec_total"`
	TapSec    float64 `json:"tap_sec_total"`
	HeldMaxMs float64 `json:"held_max_ms"`
}

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
