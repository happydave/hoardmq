package sim

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"time"
)

// onlineShape records what the failover measurement's headline figure hides.
//
// The headline is an endpoint: either "every client back online in T" or,
// when the window closes first, the count at that moment. Neither says what
// the run did in between, and the two are not measured over the same span —
// a round that reaches the finish line stops being watched, a round that
// misses it is watched for the rest of the window. Round 12 published
// "88,570 of 100,000" as its result; its peak had been 99,911 at 3m50s, and
// the difference decided a roadmap (WI 1373).
//
// Peak, trough and the time to 99% are the three figures that distinguish
// "recovered and could not close the tail" from "did not recover".
type onlineShape struct {
	target int
	from   time.Time

	peak       int
	peakAt     time.Duration
	trough     int
	troughAt   time.Duration
	ninetyNine time.Duration
	// nineNineNine is the same mark at 99.9%. At a hundred thousand clients
	// the last thousand are a different question from the last ten thousand,
	// and the headline is the whole population (WI 1618).
	nineNineNine time.Duration
	seen         bool
	// fell says the count has been seen below the 99% line, which is what
	// makes a later reading at or above it a return rather than a start.
	fell bool
	// fell999 is the same for the 99.9% line: a population that never went
	// below it never returned to it either.
	fell999 bool
}

func newOnlineShape(target int, from time.Time) *onlineShape {
	return &onlineShape{target: target, from: from}
}

// observe records one sample. It runs in the storm loop's 50 ms pass, so it
// is a few comparisons and no allocation.
//
// Peak and trough are of the online count, which every published batch
// reports, so the two remain comparable. The 99% and 99.9% marks are of the
// recovered count: they are the shape of the headline, and since WI 1618 the
// headline counts a client only when its subscriptions are granted too.
//
// The first sample is the pre-storm level: the loop starts at the crash and
// the clients have not necessarily noticed it yet. That is what peak is for
// and why it is seeded from that sample, and it is the reason the 99% mark
// cannot be.
func (s *onlineShape) observe(online, recovered int) {
	at := time.Since(s.from)
	if !s.seen {
		s.peak, s.trough, s.peakAt, s.troughAt, s.seen = online, online, at, at, true
	}
	if online > s.peak {
		s.peak, s.peakAt = online, at
	}
	if online < s.trough {
		s.trough, s.troughAt = online, at
	}
	// The mark is the moment the fleet came back to the line, so a reading at
	// or above it counts only once one below it has been seen. Without that,
	// a run published a time to 99% of 16 ms and a lowest count of 666 of
	// 1,000: at that load the storm is slower to appear than one 50 ms pass.
	// The remote driver's allback carries the same requirement.
	if s.target > 0 {
		switch line := 0.99 * float64(s.target); {
		case float64(recovered) < line:
			s.fell = true
		case s.fell && s.ninetyNine == 0:
			s.ninetyNine = at
		}
		switch line := 0.999 * float64(s.target); {
		case float64(recovered) < line:
			s.fell999 = true
		case s.fell999 && s.nineNineNine == 0:
			s.nineNineNine = at
		}
	}
}

func (s *onlineShape) peakLine() string {
	if !s.seen {
		return "not measured"
	}
	return fmt.Sprintf("%d of %d, at %s", s.peak, s.target, s.peakAt.Round(time.Second))
}

func (s *onlineShape) troughLine() string {
	if !s.seen {
		return "not measured"
	}
	return fmt.Sprintf("%d of %d, at %s", s.trough, s.target, s.troughAt.Round(time.Second))
}

func (s *onlineShape) toNinetyNine() string { return markLine(s.seen, s.ninetyNine) }

func (s *onlineShape) toNineNineNine() string { return markLine(s.seen, s.nineNineNine) }

func markLine(seen bool, at time.Duration) string {
	switch {
	case !seen:
		return "not measured"
	case at == 0:
		return "not reached"
	default:
		return at.Round(time.Millisecond).String()
	}
}

// counterValue formats a counter read from a node's metrics. A value that is
// not finite is a read that failed, and saying so is the point: reports from
// rounds 10b, 12 and 14 each carry a published "NaN" in the lease-operations
// row, which read as a quirk of the number rather than as a missing
// measurement.
func counterValue(v float64) string {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return "read failed"
	}
	return strconv.FormatFloat(v, 'f', 0, 64)
}

// runsCovered says how many runs the report rests on, so a single-run figure
// cannot be quoted as a property of a build without saying that it is one
// sample. Rounds 13, 14 and 15 differ by more than 3x on identical code.
func runsCovered() string {
	if n := os.Getenv("HOARDMQ_SIM_REPEAT_INDEX"); n != "" {
		if total := os.Getenv("HOARDMQ_SIM_REPEAT_TOTAL"); total != "" {
			return fmt.Sprintf("run %s of %s (see the repeat's summary for the distribution)", n, total)
		}
	}
	return "1 — a single run. This project has measured a 3x spread between " +
		"runs of identical code (rounds 13 to 15), so this figure is one " +
		"sample and not a property of the build"
}
