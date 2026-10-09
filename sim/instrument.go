package sim

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/metrics"
	"runtime/pprof"
	"time"
)

// Every sim binary runs with contention profiling on, so a run that goes
// wrong is diagnosable from its own artifact instead of a re-run with
// hand-added instrumentation. The rates are the ones WI 1329's research
// found negligible even under extreme contention; the worst case that does
// cost throughput is unbuffered channels at rate 1, which these are not.
// The product does not import this package, so nothing here reaches it.
func init() {
	runtime.SetMutexProfileFraction(100)
	runtime.SetBlockProfileRate(10000) // sample blocking events >= ~10 µs
}

// runtimeSnapshot is one read of the runtime metrics that catch a
// contention stall from outside: cumulative mutex wait and the scheduler
// latency histogram are the two families that move before anything
// user-visible does, and the goroutine count is the cheapest tripwire for a
// pile-up. Read in-process — a scrape path can block behind the very lock
// being diagnosed, which is how a metrics scrape once waited nine minutes.
type runtimeSnapshot struct {
	At           time.Time `json:"at"`
	MutexWaitSec float64   `json:"mutex_wait_sec"`
	Goroutines   uint64    `json:"goroutines"`
	GCPauseSec   float64   `json:"gc_pause_sec"`
	SchedP50Sec  float64   `json:"sched_p50_sec"`
	SchedP99Sec  float64   `json:"sched_p99_sec"`
	SchedMaxSec  float64   `json:"sched_max_sec"`
	// The histograms these figures were derived from, kept so an interval
	// can be subtracted out of two snapshots (WI 1352). The quantiles above
	// are cumulative since the process started, which is why round 8's
	// 21.5 s said "somewhere in this window" and never "at 22 s". Not
	// serialised: the per-interval series carries what a reader needs.
	sched *histogram
	gc    *histogram
}

// histogram is a copy of a runtime/metrics histogram. Counts is copied
// because Read reuses that storage ("all data must be deep-copied");
// Buckets is not, because the same documentation guarantees it does not
// change for a metric name until the program exits and that its values
// should only ever be read.
type histogram struct {
	counts  []uint64
	buckets []float64
}

func copyHistogram(h *metrics.Float64Histogram) *histogram {
	if h == nil {
		return nil
	}
	c := make([]uint64, len(h.Counts))
	copy(c, h.Counts)
	return &histogram{counts: c, buckets: h.Buckets}
}

// sub returns the counts this histogram gained over an earlier one: the
// interval's own distribution. A length mismatch cannot happen while the
// runtime keeps its promise about bucket boundaries, so it means a bug, and
// the honest answer is no interval rather than a mis-subtracted one.
func (h *histogram) sub(earlier *histogram) *histogram {
	if h == nil {
		return nil
	}
	if earlier == nil {
		return h
	}
	if len(earlier.counts) != len(h.counts) {
		return nil
	}
	d := &histogram{counts: make([]uint64, len(h.counts)), buckets: h.buckets}
	for i := range h.counts {
		if h.counts[i] >= earlier.counts[i] {
			d.counts[i] = h.counts[i] - earlier.counts[i]
		}
	}
	return d
}

func (h *histogram) float64Histogram() *metrics.Float64Histogram {
	if h == nil {
		return nil
	}
	return &metrics.Float64Histogram{Counts: h.counts, Buckets: h.buckets}
}

// intervalPauses is what one progress interval saw: the GC pause total
// within it and the worst scheduler latency within it, both from the
// histograms' difference rather than from a cumulative quantile. Zero
// values mean "measured, and nothing happened"; ok is false when the
// interval could not be measured at all.
func intervalPauses(prev, cur runtimeSnapshot) (gcSec, schedMaxSec float64, ok bool) {
	g, sc := cur.gc.sub(prev.gc), cur.sched.sub(prev.sched)
	if g == nil || sc == nil {
		return 0, 0, false
	}
	return histogramSum(g.float64Histogram()), histogramQuantile(sc.float64Histogram(), 1.00), true
}

var runtimeSamples = []string{
	"/sync/mutex/wait/total:seconds",
	"/sched/goroutines:goroutines",
	"/sched/pauses/total/gc:seconds",
	"/sched/latencies:seconds",
}

func readRuntime() runtimeSnapshot {
	samples := make([]metrics.Sample, len(runtimeSamples))
	for i, name := range runtimeSamples {
		samples[i].Name = name
	}
	metrics.Read(samples)
	s := runtimeSnapshot{At: time.Now()}
	for _, m := range samples {
		switch m.Name {
		case "/sync/mutex/wait/total:seconds":
			if m.Value.Kind() == metrics.KindFloat64 {
				s.MutexWaitSec = m.Value.Float64()
			}
		case "/sched/goroutines:goroutines":
			if m.Value.Kind() == metrics.KindUint64 {
				s.Goroutines = m.Value.Uint64()
			}
		case "/sched/pauses/total/gc:seconds":
			if m.Value.Kind() == metrics.KindFloat64Histogram {
				h := m.Value.Float64Histogram()
				s.GCPauseSec = histogramSum(h)
				s.gc = copyHistogram(h)
			}
		case "/sched/latencies:seconds":
			if m.Value.Kind() == metrics.KindFloat64Histogram {
				h := m.Value.Float64Histogram()
				s.SchedP50Sec = histogramQuantile(h, 0.50)
				s.SchedP99Sec = histogramQuantile(h, 0.99)
				s.SchedMaxSec = histogramQuantile(h, 1.00)
				s.sched = copyHistogram(h)
			}
		}
	}
	return s
}

// histogramQuantile approximates a quantile from a runtime/metrics
// histogram, taking each bucket's upper bound (the pessimistic read).
func histogramQuantile(h *metrics.Float64Histogram, q float64) float64 {
	var total uint64
	for _, c := range h.Counts {
		total += c
	}
	if total == 0 {
		return 0
	}
	target := uint64(float64(total) * q)
	var seen uint64
	for i, c := range h.Counts {
		seen += c
		if seen > target || (q >= 1 && seen == total && c > 0) {
			// Buckets has len(Counts)+1 boundaries; bucket i spans
			// Buckets[i]..Buckets[i+1]. The last boundary can be +Inf, in
			// which case the lower bound is the honest answer.
			hi := h.Buckets[i+1]
			if hi > 1e300 { // +Inf bucket
				return h.Buckets[i]
			}
			return hi
		}
	}
	return h.Buckets[len(h.Buckets)-1]
}

// histogramSum approximates the sum of a histogram's mass using bucket
// midpoints; for GC pause totals this is a rough figure, reported as such.
func histogramSum(h *metrics.Float64Histogram) float64 {
	var sum float64
	for i, c := range h.Counts {
		if c == 0 {
			continue
		}
		lo, hi := h.Buckets[i], h.Buckets[i+1]
		if hi > 1e300 {
			hi = lo
		}
		if lo < -1e300 {
			lo = hi
		}
		sum += float64(c) * (lo + hi) / 2
	}
	return sum
}

// runtimeDelta is what changed across a phase, plus the per-operation form
// of the contention figure. Per-op mutex wait over a fixed seed is the
// report-first tripwire from WI 1329's research: deterministic enough to
// compare across runs, and both of this project's contention incidents
// would have moved it by an order of magnitude.
type runtimeDelta struct {
	Start           runtimeSnapshot `json:"start"`
	End             runtimeSnapshot `json:"end"`
	WallSec         float64         `json:"wall_sec"`
	MutexWaitSec    float64         `json:"mutex_wait_delta_sec"`
	MutexWaitPerOp  float64         `json:"mutex_wait_per_op_us"` // microseconds per op
	Ops             int             `json:"ops"`
	GoroutinesDelta int64           `json:"goroutines_delta"`
}

func deltaBetween(a, b runtimeSnapshot, ops int) runtimeDelta {
	d := runtimeDelta{
		Start:           a,
		End:             b,
		WallSec:         b.At.Sub(a.At).Seconds(),
		MutexWaitSec:    b.MutexWaitSec - a.MutexWaitSec,
		Ops:             ops,
		GoroutinesDelta: int64(b.Goroutines) - int64(a.Goroutines),
	}
	if ops > 0 {
		d.MutexWaitPerOp = d.MutexWaitSec / float64(ops) * 1e6
	}
	return d
}

// progressLine renders one long-run progress line (WI 1285). The caller
// supplies every figure; this function reads nothing, which is what makes
// it unit-testable and keeps the reporting path free of locks by
// construction. The line goes to stderr because Go buffers a test's log
// output until the test ends — the exact failure mode that let a wedged
// forty-minute run go unnoticed.
func progressLine(phase string, elapsed time.Duration, done, total int, refusals, fails int64, goroutines uint64, mutexWaitRate float64) string {
	return fmt.Sprintf("[sim %s] %s %d/%d refusals=%d fails=%d goroutines=%d mutex-wait=%.2fs/s",
		phase, elapsed.Round(time.Second), done, total, refusals, fails, goroutines, mutexWaitRate)
}

// pauseSuffix renders one interval's pause figures for a progress line:
// how long this process was not running at worst, and the runtime's own
// account of why over the same interval. Two connections idle for six
// seconds beside a six-second stall is a whole-process pause; the same idle
// beside a quiet observer is not, and the reader should be able to tell
// them apart without opening a second file (WI 1352).
func pauseSuffix(worstStall time.Duration, gcSec, schedMaxSec float64) string {
	return fmt.Sprintf(" | stall=%dms gc=%dms sched-max=%dms",
		worstStall.Milliseconds(), int64(gcSec*1000), int64(schedMaxSec*1000))
}

// writeProfiles writes the goroutine, mutex and block profiles (debug=0: gzip
// protobuf, aggregated by stack signature, bounded pause since Go 1.19)
// into dir under the given tag. The duration is returned so a snapshot
// taken inside a measurement carries its own cost on the record. Never
// debug=2 here: that mode walks every goroutine unaggregated and belongs
// to HOARDMQ_SIM_DUMP, outside kept measurements.
func writeProfiles(dir, tag string) (time.Duration, error) {
	start := time.Now()
	for _, name := range []string{"goroutine", "mutex", "block"} {
		path := filepath.Join(dir, fmt.Sprintf("%s-%s.pb.gz", name, tag))
		f, err := os.Create(path) //nolint:gosec // artifact dir chosen by the operator
		if err != nil {
			return time.Since(start), err
		}
		werr := pprof.Lookup(name).WriteTo(f, 0)
		cerr := f.Close()
		if werr != nil {
			return time.Since(start), fmt.Errorf("write %s profile: %w", name, werr)
		}
		if cerr != nil {
			return time.Since(start), fmt.Errorf("close %s profile: %w", name, cerr)
		}
	}
	return time.Since(start), nil
}
