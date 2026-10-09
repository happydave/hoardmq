package sim

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
)

// stallWatch is the direct pause observer (WI 1352). It sleeps a fixed
// short period and measures how long the sleep actually took; the excess
// over the period is how long this process was not running. Its own
// starvation is the measurement, which is what makes it catch stalls the
// runtime's own metrics cannot explain — stop-the-world, scheduler
// starvation under a lock convoy, CPU steal on a shared host, swap.
//
// It reports what it measured and never a cause: an excess of six seconds
// is compatible with all four, and picking one in the artifact is how a
// measurement becomes a conclusion nobody checked.
type stallWatch struct {
	period    time.Duration
	threshold time.Duration
	origin    time.Time // what elapsed times are measured from

	mu      sync.Mutex
	worst   time.Duration // worst excess since the last takeWorst
	events  []StallEvent
	dropped int

	stop chan struct{}
	done chan struct{}
}

// StallEvent is one excess over the threshold: when it happened, measured
// from the run's origin, and how long this process was not running.
type StallEvent struct {
	AfterSec float64 `json:"after_sec"`
	StallSec float64 `json:"stall_sec"`
}

const (
	// stallPeriod is how often the watcher wakes. Twenty times a second is
	// far below any stall worth reporting and costs two integer operations
	// per wake.
	stallPeriod = 50 * time.Millisecond
	// stallThreshold is the excess worth recording as an event: five
	// periods, an order of magnitude above ordinary scheduling jitter and
	// two below the 5.8 s silence in WI 1340 round 8 that motivated this.
	stallThreshold = 250 * time.Millisecond
	// stallMaxEvents bounds the list; beyond it events are counted, not
	// kept, and the count is reported so a truncated list cannot read as a
	// quiet run.
	stallMaxEvents = 256
)

// newStallWatch starts a watcher measuring elapsed times from origin.
func newStallWatch(origin time.Time) *stallWatch {
	w := &stallWatch{period: stallPeriod, threshold: stallThreshold, origin: origin,
		stop: make(chan struct{}), done: make(chan struct{})}
	go w.loop()
	return w
}

func (w *stallWatch) loop() {
	defer close(w.done)
	for {
		select {
		case <-w.stop:
			return
		default:
		}
		before := time.Now()
		time.Sleep(w.period)
		excess := time.Since(before) - w.period
		if excess <= 0 {
			continue
		}
		w.mu.Lock()
		if excess > w.worst {
			w.worst = excess
		}
		if excess >= w.threshold {
			if len(w.events) < stallMaxEvents {
				w.events = append(w.events, StallEvent{
					AfterSec: before.Sub(w.origin).Seconds(),
					StallSec: excess.Seconds(),
				})
			} else {
				w.dropped++
			}
		}
		w.mu.Unlock()
	}
}

// takeWorst returns the worst excess since the last call and resets it, so
// each progress interval reports its own.
func (w *stallWatch) takeWorst() time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()
	worst := w.worst
	w.worst = 0
	return worst
}

// report returns the events kept and how many were dropped.
func (w *stallWatch) report() ([]StallEvent, int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]StallEvent, len(w.events))
	copy(out, w.events)
	return out, w.dropped
}

// close stops the watcher and waits for it.
func (w *stallWatch) close() {
	select {
	case <-w.stop:
	default:
		close(w.stop)
	}
	<-w.done
}

// Environment is what a starvation figure has to be read against: a
// scheduler-latency number means nothing without knowing how many threads
// the process was allowed and what else the host was doing.
type Environment struct {
	GOMAXPROCS int    `json:"gomaxprocs"`
	NumCPU     int    `json:"num_cpu"`
	LoadAvg    string `json:"load_avg,omitempty"`
}

// readEnvironment reads the process's parallelism and, where the host
// offers it, the load average. An unavailable load average is absent, never
// an error: this is context for a measurement, not part of it.
func readEnvironment() Environment {
	e := Environment{GOMAXPROCS: runtime.GOMAXPROCS(0), NumCPU: runtime.NumCPU()}
	if b, err := os.ReadFile("/proc/loadavg"); err == nil {
		if f := strings.Fields(string(b)); len(f) >= 3 {
			e.LoadAvg = strings.Join(f[:3], " ")
		}
	}
	return e
}

// intervalSample is one progress interval's own pause figures. Measured is
// false when the interval could not be subtracted, so a zero that means
// "nothing happened" is distinguishable from one that means "unknown".
// It lives here rather than in the test file that fills it because the
// helpers below consume it, and a non-test file cannot reference a type
// declared in a _test.go — which broke `go build ./...` in WI 1352 without
// any documented gate noticing (WI 1354).
type intervalSample struct {
	Phase       string  `json:"phase"`
	AfterSec    float64 `json:"after_sec"`
	GCPauseSec  float64 `json:"gc_pause_sec"`
	SchedMaxSec float64 `json:"sched_max_sec"`
	StallSec    float64 `json:"stall_sec"`
	Measured    bool    `json:"measured"`
	// Host is what the machine was doing over the same interval (WI 1375):
	// the pressure figures say how much of a stall was the machine waiting
	// on a resource, which the fields above cannot distinguish from the
	// process being descheduled.
	Host hostDelta `json:"host"`
}

// The report's four pause figures, kept beside the observer so the
// rendering and the measurement stay together.

func worstIntervalStall(in []intervalSample) string {
	var worst float64
	for _, s := range in {
		if s.StallSec > worst {
			worst = s.StallSec
		}
	}
	return fmt.Sprintf("%d ms", int64(worst*1000))
}

func worstIntervalSched(in []intervalSample) string {
	var worst float64
	measured := false
	for _, s := range in {
		if !s.Measured {
			continue
		}
		measured = true
		if s.SchedMaxSec > worst {
			worst = s.SchedMaxSec
		}
	}
	if !measured {
		return "not measured"
	}
	return fmt.Sprintf("%d ms", int64(worst*1000))
}

func gcTotal(in []intervalSample) string {
	var total float64
	for _, s := range in {
		total += s.GCPauseSec
	}
	return fmt.Sprintf("%d ms", int64(total*1000))
}

// stallSummary is the count and the worst of the recorded stalls, saying so
// when the list was truncated.
func stallSummary(w *stallWatch) string {
	events, dropped := w.report()
	if len(events) == 0 {
		return "none"
	}
	var worst float64
	for _, e := range events {
		if e.StallSec > worst {
			worst = e.StallSec
		}
	}
	out := fmt.Sprintf("%d (worst %.2f s)", len(events), worst)
	if dropped > 0 {
		out += fmt.Sprintf(", %d more not kept", dropped)
	}
	return out
}
