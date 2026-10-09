package sim

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// The failover measurement's definition of recovered, owner-decided
// 2026-09-21 (WI 1618).
//
// A client is recovered while it holds a live connection whose CONNACK was
// Success and every filter it holds was granted on that same connection
// (Client.Recovered). The cluster's recovery instant is the start of the
// earliest interval of length >= hold throughout which every still-trying
// client is continuously recovered.
//
// The hold is part of the measurement rather than a check run after it: a
// population that does not stay up moves the instant out to where it belongs,
// and no separate pass rule is needed. A measurement that stops at the first
// reading where everyone is online cannot see what the cluster still owes
// them — WI 1515's adoption queue closed connections after the number had
// been published.
const (
	// recoveryReached is the verdict's reason when an instant was settled.
	recoveryReached = ""
	// A reading below the target after the crash is what makes a later
	// reading at the target a return rather than a start. Without it a run
	// where nothing ever dropped reports its first pass as a recovery: the
	// remote driver has required this since WI 1480 and the in-process mode
	// did not, which is WI 1698.
	recoveryNeverFell    = "the recovered count was never seen below the target, so nothing recovered"
	recoveryNeverBack    = "the recovered count fell and never returned to the target"
	recoveryLate         = "the recovered count returned only after the window closed"
	recoveryHeldTooShort = "the hold did not complete before watching ended"
)

// holdWatch decides the recovery instant from the readings it is given,
// one at a time. It performs no sampling of its own and holds no clock, so a
// synthetic series exercises every branch (recovered_test.go).
type holdWatch struct {
	target   int
	hold     time.Duration
	deadline time.Time // the latest instant that may be reported

	fell    bool
	holding bool
	start   time.Time
	late    bool // an interval began, but after the deadline

	settled bool
	instant time.Time
}

// newHoldWatch watches for target clients recovered, held for hold, with
// the instant required to fall no later than deadline. The interval that
// confirms an instant may complete after the deadline: the instant is what
// the window bounds, not the confirmation of it.
func newHoldWatch(target int, hold time.Duration, deadline time.Time) *holdWatch {
	return &holdWatch{target: target, hold: hold, deadline: deadline}
}

// observe records one reading and reports whether the instant is now settled.
// Once settled it ignores further readings: the instant is the start of the
// first qualifying interval, and a later one cannot improve on it.
func (w *holdWatch) observe(at time.Time, recovered int) bool {
	if w.settled || w.target <= 0 {
		return w.settled
	}
	if recovered < w.target {
		w.fell, w.holding = true, false
		return false
	}
	if !w.fell {
		return false
	}
	if !w.holding {
		w.holding, w.start, w.late = true, at, at.After(w.deadline)
	}
	if !w.late && at.Sub(w.start) >= w.hold {
		w.settled, w.instant = true, w.start
	}
	return w.settled
}

// verdict is the instant and, when there is none, why. The two are exclusive:
// a zero instant always carries a reason.
func (w *holdWatch) verdict() (time.Time, string) {
	switch {
	case w.settled:
		return w.instant, recoveryReached
	case !w.fell:
		return time.Time{}, recoveryNeverFell
	case w.late:
		return time.Time{}, recoveryLate
	case w.holding:
		return time.Time{}, recoveryHeldTooShort
	default:
		return time.Time{}, recoveryNeverBack
	}
}

// holdStart is the open interval's start, or zero. The window loop reports it
// so a run that ended mid-hold says how far it had got.
func (w *holdWatch) holdStart() time.Time {
	if w.holding {
		return w.start
	}
	return time.Time{}
}

// holdDefault is H: the interval a recovered population must hold for before
// the instant counts (owner decision, 2026-09-21).
//
// It is two timings, not a round number. A lease lives 30 s
// (internal/cluster/lease/service.go) and the routing plane's full sync runs
// every 30 s (internal/cluster/routing/plane.go), so 60 s is the first value
// that covers a lease lapsing and the sync that reveals what the lapse
// changed. Shortening it below either timing lets the hold complete before
// the cluster has finished owing the clients anything.
const holdDefault = 60 * time.Second

// holdGate is the hold for the 1,000-client gate run. The interval rule is
// covered by holdWatch's own tests over synthetic series; making every
// `go test ./sim` wait a minute to exercise it again would buy nothing.
const holdGate = 2 * time.Second

// holdSeconds is H for this run: the default for its kind, overridden by
// HOARDMQ_SIM_HOLD_SEC. A zero or unreadable override is ignored rather than
// silently disabling the hold, which would publish the old definition's
// figure under the new definition's name.
func holdSeconds(recorded bool) time.Duration {
	hold := holdGate
	if recorded {
		hold = holdDefault
	}
	if sec, err := strconv.Atoi(os.Getenv("HOARDMQ_SIM_HOLD_SEC")); err == nil && sec > 0 {
		hold = time.Duration(sec) * time.Second
	}
	return hold
}

// connackLine is the old reading as the report prints it: the first return of
// the online count, with no hold and no subscriptions.
func connackLine(at time.Time, why string, crashAt time.Time) string {
	if why != recoveryReached {
		return "not reached — " + why
	}
	return at.Sub(crashAt).Round(time.Millisecond).String()
}

// holdMinLine is the lowest recovered count inside the confirming interval,
// or why there was none.
func holdMinLine(h holdRecord, why string) string {
	if why != recoveryReached {
		return "no interval completed — " + why
	}
	return fmt.Sprintf("%d, ending at %d recovered and %d online", h.MinRecovered, h.RecoveredAtEnd, h.OnlineAtEnd)
}

// holdRecord describes the interval that confirmed the recovery instant: H,
// the lowest recovered count seen inside it (-1 when no interval completed),
// and where the two counts ended.
type holdRecord struct {
	Seconds        int `json:"seconds"`
	MinRecovered   int `json:"min_recovered"`
	RecoveredAtEnd int `json:"recovered_at_end"`
	OnlineAtEnd    int `json:"online_at_end"`
}

func holdOrNil(h holdRecord) *holdRecord {
	if h.Seconds == 0 {
		return nil
	}
	return &h
}
