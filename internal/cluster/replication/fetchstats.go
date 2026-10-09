package replication

import (
	"sync/atomic"
	"time"
)

// fetchCounters times a replica fetch stage by stage, each stage on the node
// that runs it. Atomics only: they are read during a reconnect storm, when
// any lock the fetch path shares may be held for seconds.
type fetchCounters struct {
	local, remote, keptCopy, none atomic.Int64
	localFinds, localFindNs       atomic.Int64

	requests, requestsEnded atomic.Int64
	requestNs, sendNs       atomic.Int64

	answered                     atomic.Int64
	queueNs, answerFindNs, reply atomic.Int64
	maxBatch, settlesBeside      atomic.Int64
	pooled, overflowed           atomic.Int64
	noneUnanswered               atomic.Int64
	noneSourceDown               atomic.Int64
	nonePeerDown                 atomic.Int64
	noneReturning                atomic.Int64
	noneKeptUnconfirmed          atomic.Int64

	finds                  atomic.Int64
	findLockNs, findHoldNs atomic.Int64
}

// FetchStats is one node's replica fetches as the node that asked and as the
// holder that answered. Totals are milliseconds. A remote fetch's transit and
// decode time is RequestMs less SendMs less the holder's QueueMs, FindMs and
// ReplyMs, read from the holder's own stats.
type FetchStats struct {
	Local    int64 `json:"local"`
	Remote   int64 `json:"remote"`
	KeptCopy int64 `json:"kept_copy"`
	None     int64 `json:"none"`
	// NoneUnanswered is the part of None in which a request ended without an
	// answer: nobody answering in time is not nobody having it.
	NoneUnanswered int64 `json:"none_unanswered"`
	// NoneSourceDown is the part of None in which a source was down and so
	// was not asked.
	NoneSourceDown int64 `json:"none_source_down"`
	// NonePeerDown is the part of None in which the map no longer held the
	// previous owner and a peer other than a source was down, so it was named
	// down for the caller to judge.
	NonePeerDown int64 `json:"none_peer_down"`
	// NoneReturning is the part of None in which a peer that could hold the
	// copy was back on its control connection and not Up, so it could not be
	// asked yet, and the fetch read unanswered.
	NoneReturning int64 `json:"none_returning"`
	// NoneKeptUnconfirmed is the part of None in which a former holder's kept
	// copy was found but a source did not answer.
	NoneKeptUnconfirmed int64 `json:"none_kept_copy_unconfirmed"`
	// LocalFinds are the fetches that looked in this node's own replicas,
	// found or not, and LocalFindMs the time that took.
	LocalFinds  int64   `json:"local_finds"`
	LocalFindMs float64 `json:"local_find_ms_total"`

	// Requests are fetch frames sent to a holder; RequestsEnded are those
	// that ended on their context or a failed send rather than an answer.
	Requests      int64   `json:"requests"`
	RequestsEnded int64   `json:"requests_ended"`
	RequestMs     float64 `json:"request_ms_total"`
	SendMs        float64 `json:"send_ms_total"`

	// Answered are fetch frames this node answered as a holder. QueueMs is
	// the time they waited to be answered.
	Answered int64   `json:"answered"`
	QueueMs  float64 `json:"queue_ms_total"`
	FindMs   float64 `json:"find_ms_total"`
	ReplyMs  float64 `json:"reply_ms_total"`
	// MaxBatch is the largest batch that held a fetch; SettlesBeside counts
	// store syncs run inside such batches.
	MaxBatch      int64 `json:"max_batch"`
	SettlesBeside int64 `json:"settles_beside"`
	// Pooled are the fetches the pool answered; Overflowed those that found
	// its queue full and went to the peer's worker.
	Pooled     int64 `json:"pooled"`
	Overflowed int64 `json:"overflowed"`

	// Every find, whoever called it: time waiting for the follower's lock and
	// time holding it.
	Finds      int64   `json:"finds"`
	FindLockMs float64 `json:"find_lock_ms_total"`
	FindHoldMs float64 `json:"find_hold_ms_total"`
}

func ms(ns int64) float64 { return float64(ns) / 1e6 }

// FetchStats reads the fetch counters without a lock.
func (r *Replicator) FetchStats() FetchStats {
	c := &r.fetches
	return FetchStats{
		Local: c.local.Load(), Remote: c.remote.Load(), KeptCopy: c.keptCopy.Load(), None: c.none.Load(),
		LocalFinds: c.localFinds.Load(), LocalFindMs: ms(c.localFindNs.Load()),
		Requests: c.requests.Load(), RequestsEnded: c.requestsEnded.Load(),
		RequestMs: ms(c.requestNs.Load()), SendMs: ms(c.sendNs.Load()),
		Answered: c.answered.Load(), QueueMs: ms(c.queueNs.Load()),
		FindMs: ms(c.answerFindNs.Load()), ReplyMs: ms(c.reply.Load()),
		MaxBatch: c.maxBatch.Load(), SettlesBeside: c.settlesBeside.Load(),
		Pooled: c.pooled.Load(), Overflowed: c.overflowed.Load(), NoneUnanswered: c.noneUnanswered.Load(), NoneSourceDown: c.noneSourceDown.Load(), NonePeerDown: c.nonePeerDown.Load(), NoneReturning: c.noneReturning.Load(), NoneKeptUnconfirmed: c.noneKeptUnconfirmed.Load(),
		Finds: c.finds.Load(), FindLockMs: ms(c.findLockNs.Load()), FindHoldMs: ms(c.findHoldNs.Load()),
	}
}

func storeMax(a *atomic.Int64, v int64) {
	for {
		cur := a.Load()
		if v <= cur || a.CompareAndSwap(cur, v) {
			return
		}
	}
}

func since(t time.Time) int64 { return int64(time.Since(t)) }
