// Package ratelimit is a token bucket whose state moves only on the times it
// is given, and the admission rule for a CONNECT against its listener's and
// its node's buckets.
package ratelimit

import (
	"math"
	"time"
)

// Bucket holds up to Burst tokens and gains Rate a second. A Rate of 0 or
// less is unlimited: every take succeeds and the state never moves. A bucket
// is a value; a caller that shares one guards it.
type Bucket struct {
	Rate   float64
	Burst  float64
	tokens float64
	last   time.Time
}

// New is a full bucket at now.
func New(rate, burst float64, now time.Time) Bucket {
	return Bucket{Rate: rate, Burst: burst, tokens: burst, last: now}
}

// Unlimited reports whether the bucket admits everything.
func (b Bucket) Unlimited() bool { return b.Rate <= 0 }

// refill is the bucket at now: the tokens gained since its last time, up to
// Burst. A time before the last adds nothing and does not move the last.
func (b Bucket) refill(now time.Time) Bucket {
	if b.Unlimited() {
		return b
	}
	if elapsed := now.Sub(b.last); elapsed > 0 {
		b.tokens = math.Min(b.Burst, b.tokens+elapsed.Seconds()*b.Rate)
		b.last = now
	}
	return b
}

// Take is the decision on n tokens at now: the bucket afterwards and whether
// the tokens were there. A refused take takes nothing.
func (b Bucket) Take(now time.Time, n float64) (Bucket, bool) {
	if b.Unlimited() {
		return b, true
	}
	b = b.refill(now)
	if b.tokens < n {
		return b, false
	}
	b.tokens -= n
	return b, true
}

// Charge takes n tokens at now whatever the bucket holds, so it may go into
// debt; Delay(now, 0) is then how long the debt takes to repay. A throttle
// charges what it has already let through rather than refusing it.
func (b Bucket) Charge(now time.Time, n float64) Bucket {
	if b.Unlimited() {
		return b
	}
	b = b.refill(now)
	b.tokens -= n
	return b
}

// Tokens is what the bucket holds at now.
func (b Bucket) Tokens(now time.Time) float64 {
	if b.Unlimited() {
		return math.Inf(1)
	}
	return b.refill(now).tokens
}

// Delay is how long from now until the bucket holds n tokens; 0 when it does
// already or is unlimited. A request above the burst is answered with the
// time the rate takes to cover it, as though the bucket could hold it.
func (b Bucket) Delay(now time.Time, n float64) time.Duration {
	if b.Unlimited() {
		return 0
	}
	short := n - b.refill(now).tokens
	if short <= 0 {
		return 0
	}
	return time.Duration(math.Ceil(short / b.Rate * float64(time.Second)))
}

// Verdict is the answer to a CONNECT.
type Verdict int

// Verdicts.
const (
	Admitted Verdict = iota
	RefusedListener
	RefusedNode
)

// Limit names the bucket that refused, for a metric label; "" when admitted.
func (v Verdict) Limit() string {
	switch v {
	case RefusedListener:
		return "listener"
	case RefusedNode:
		return "node"
	}
	return ""
}

// Admit is the decision on one CONNECT against its listener's bucket and the
// node's: both must hold a token, and only an admitted CONNECT takes from
// either, so a refusal by one costs the other nothing. It returns both
// buckets as they stand afterwards.
func Admit(listener, node Bucket, now time.Time) (Bucket, Bucket, Verdict) {
	listener, node = listener.refill(now), node.refill(now)
	if _, ok := listener.Take(now, 1); !ok {
		return listener, node, RefusedListener
	}
	if _, ok := node.Take(now, 1); !ok {
		return listener, node, RefusedNode
	}
	listener, _ = listener.Take(now, 1)
	node, _ = node.Take(now, 1)
	return listener, node, Admitted
}
