package lease

import (
	"sync/atomic"
	"time"
)

// TakeoverStats divides what a takeover pull waits for (WI 1691). The
// requester's figures and the previous owner's are recorded on the node that
// runs each stage, so no figure depends on two nodes' clocks agreeing: a
// requester's wait is its own, and the queue wait and pass are the owner's.
// What the two do not account for is the trip, which is read as the residue.
type TakeoverStats struct {
	// The requester's side: the pull the acquire makes inside a CONNECT.
	Pulls           int64   `json:"pulls"`
	PullMsTotal     float64 `json:"pull_ms_total"`
	PullMaxMs       float64 `json:"pull_max_ms"`
	PullsWithState  int64   `json:"pulls_with_state"`
	PullsEmpty      int64   `json:"pulls_answered_empty"`
	PullsSuperseded int64   `json:"pulls_superseded"`
	PullsUnanswered int64   `json:"pulls_unanswered"`
	// The previous owner's side. Requests is every takeover request served,
	// Passes the passes that served them: a pass answers every request
	// coalesced onto its key, so the two differ and a mean pass may not be
	// multiplied by the request count.
	Requests     int64   `json:"requests"`
	QueueMsTotal float64 `json:"queue_ms_total"`
	QueueMaxMs   float64 `json:"queue_max_ms"`
	Passes       int64   `json:"passes"`
	PassMsTotal  float64 `json:"pass_ms_total"`
	// The parts of a pass that take the session manager's locks.
	DetachMsTotal float64 `json:"detach_ms_total"`
	TakeMsTotal   float64 `json:"take_ms_total"`
}

type takeoverCounters struct {
	pulls, pullNs, pullMaxNs                 atomic.Int64
	withState, empty, superseded, unanswered atomic.Int64
	requests, queueNs, queueMaxNs            atomic.Int64
	passes, passNs, detachNs, takeNs         atomic.Int64
}

// notePull records one requester-side pull and how it ended.
func (c *takeoverCounters) notePull(d time.Duration, state, answered, superseded bool) {
	c.pulls.Add(1)
	c.pullNs.Add(int64(d))
	raise(&c.pullMaxNs, int64(d))
	switch {
	case superseded:
		c.superseded.Add(1)
	case !answered:
		c.unanswered.Add(1)
	case state:
		c.withState.Add(1)
	default:
		c.empty.Add(1)
	}
}

// noteQueued records how long one request waited for its pass.
func (c *takeoverCounters) noteQueued(d time.Duration) {
	c.requests.Add(1)
	c.queueNs.Add(int64(d))
	raise(&c.queueMaxNs, int64(d))
}

// notePass records one pass over a key and the two parts of it that take the
// session manager's locks.
func (c *takeoverCounters) notePass(total, detach, take time.Duration) {
	c.passes.Add(1)
	c.passNs.Add(int64(total))
	c.detachNs.Add(int64(detach))
	c.takeNs.Add(int64(take))
}

// raise sets n to v when v is larger. Several goroutines may call it, so the
// compare-and-swap is retried rather than assumed.
func raise(n *atomic.Int64, v int64) {
	for {
		old := n.Load()
		if v <= old || n.CompareAndSwap(old, v) {
			return
		}
	}
}

func (c *takeoverCounters) stats() TakeoverStats {
	ms := func(n *atomic.Int64) float64 { return float64(n.Load()) / 1e6 }
	return TakeoverStats{
		Pulls: c.pulls.Load(), PullMsTotal: ms(&c.pullNs), PullMaxMs: ms(&c.pullMaxNs),
		PullsWithState: c.withState.Load(), PullsEmpty: c.empty.Load(),
		PullsSuperseded: c.superseded.Load(), PullsUnanswered: c.unanswered.Load(),
		Requests: c.requests.Load(), QueueMsTotal: ms(&c.queueNs), QueueMaxMs: ms(&c.queueMaxNs),
		Passes: c.passes.Load(), PassMsTotal: ms(&c.passNs),
		DetachMsTotal: ms(&c.detachNs), TakeMsTotal: ms(&c.takeNs),
	}
}

// TakeoverStats reports the takeover counters; it takes no lock a storm
// contends for, so a harness can read it mid-storm.
func (s *Service) TakeoverStats() TakeoverStats { return s.takeoverTimes.stats() }
