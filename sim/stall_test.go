package sim

import (
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"
)

// stopThisProcess has the OS stop this whole process for d, from a child
// that outlives the stop. This is the real article: SIGSTOP takes every
// thread off the CPU, which is what a descheduled process, CPU steal on a
// shared host, or a long stop-the-world looks like from inside. Testing the
// observer against anything less would test its arithmetic and not its
// claim.
func stopThisProcess(t *testing.T, d time.Duration) {
	t.Helper()
	script := fmt.Sprintf("kill -STOP %d; sleep %.2f; kill -CONT %d", os.Getpid(), d.Seconds(), os.Getpid())
	cmd := exec.Command("sh", "-c", script)
	if err := cmd.Start(); err != nil {
		t.Skipf("cannot induce a process stop here: %v", err)
	}
	// The stop lands somewhere in the next few milliseconds and this
	// goroutine is stopped with everything else; when it returns, the
	// process has been continued.
	if err := cmd.Wait(); err != nil {
		t.Fatalf("stop/continue helper: %v", err)
	}
}

// TestStallWatchSeesAnInducedPause: the observer's own starvation is the
// measurement. The assertion is against a known magnitude — "some drift was
// recorded" would pass on ordinary scheduling noise and prove nothing.
func TestStallWatchSeesAnInducedPause(t *testing.T) {
	origin := time.Now()
	w := newStallWatch(origin)
	defer w.close()

	const pause = 700 * time.Millisecond
	stopThisProcess(t, pause)

	deadline := time.Now().Add(5 * time.Second)
	var events []StallEvent
	for time.Now().Before(deadline) {
		events, _ = w.report()
		if len(events) > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(events) == 0 {
		t.Fatalf("a %s process stop was not reported", pause)
	}
	if got := events[0].StallSec; got < pause.Seconds()*0.8 {
		t.Fatalf("reported stall %.3fs is far below the %s induced", got, pause)
	}
	if events[0].AfterSec < 0 {
		t.Fatalf("event stamped before the origin: %+v", events[0])
	}
}

// TestStallWatchIsQuietWhenNothingStalls is the negative control: an
// observer that always fires is as useless as one that never does.
func TestStallWatchIsQuietWhenNothingStalls(t *testing.T) {
	w := newStallWatch(time.Now())
	time.Sleep(600 * time.Millisecond)
	w.close()
	events, dropped := w.report()
	if len(events) != 0 || dropped != 0 {
		t.Fatalf("an idle interval reported %d events (%d dropped): %+v", len(events), dropped, events)
	}
}

// TestStallWatchWorstIsPerInterval: takeWorst reports the interval's own
// worst and resets, so a quiet interval after a bad one reads as quiet.
func TestStallWatchWorstIsPerInterval(t *testing.T) {
	w := newStallWatch(time.Now())
	defer w.close()
	stopThisProcess(t, 400*time.Millisecond)
	time.Sleep(150 * time.Millisecond)
	if first := w.takeWorst(); first < 300*time.Millisecond {
		t.Fatalf("the interval with the stop reported %s", first)
	}
	time.Sleep(300 * time.Millisecond)
	if second := w.takeWorst(); second > 100*time.Millisecond {
		t.Fatalf("a quiet interval after a stalled one reported %s", second)
	}
}

// TestIntervalPausesAreTheIntervals is the figure round 8 needed and did
// not have: a bucket occupied before the interval must not appear in it.
func TestIntervalPausesAreTheIntervals(t *testing.T) {
	buckets := []float64{0, 0.001, 0.01, 0.1, 1, 10, 100}
	prev := runtimeSnapshot{
		sched: &histogram{counts: []uint64{5, 0, 0, 0, 1, 0}, buckets: buckets},
		gc:    &histogram{counts: []uint64{10, 2, 0, 0, 0, 0}, buckets: buckets},
	}
	cur := runtimeSnapshot{
		// One new sample in the 0.01–0.1 bucket; the 1–10 bucket is
		// unchanged and belongs to the past.
		sched: &histogram{counts: []uint64{5, 0, 1, 0, 1, 0}, buckets: buckets},
		gc:    &histogram{counts: []uint64{10, 3, 0, 0, 0, 0}, buckets: buckets},
	}
	gc, schedMax, ok := intervalPauses(prev, cur)
	if !ok {
		t.Fatal("the interval could not be measured")
	}
	if schedMax != 0.1 {
		t.Fatalf("worst scheduler latency in the interval = %v, want the 0.01-0.1 bucket's upper bound; "+
			"a cumulative read would have returned 10 from a bucket occupied before the interval", schedMax)
	}
	// One new GC pause in the 0.001-0.01 bucket: the midpoint sum.
	if want := (0.001 + 0.01) / 2; gc != want {
		t.Fatalf("GC pause total in the interval = %v, want %v", gc, want)
	}
}

// TestIntervalPausesRefuseAMismatch: the runtime guarantees bucket
// boundaries are stable, so a length mismatch is a bug — and the honest
// answer to a bug is no interval, not a mis-subtracted one.
func TestIntervalPausesRefuseAMismatch(t *testing.T) {
	prev := runtimeSnapshot{
		sched: &histogram{counts: []uint64{1, 2}, buckets: []float64{0, 1, 2}},
		gc:    &histogram{counts: []uint64{1, 2}, buckets: []float64{0, 1, 2}},
	}
	cur := runtimeSnapshot{
		sched: &histogram{counts: []uint64{1, 2, 3}, buckets: []float64{0, 1, 2, 3}},
		gc:    &histogram{counts: []uint64{1, 2, 3}, buckets: []float64{0, 1, 2, 3}},
	}
	if _, _, ok := intervalPauses(prev, cur); ok {
		t.Fatal("a mismatched histogram must not produce an interval")
	}
}

// TestRuntimeSnapshotKeepsItsOwnHistograms: readRuntime must deep-copy the
// counts, because runtime/metrics reuses that storage between reads. If it
// did not, every retained snapshot would alias the newest read and the
// whole per-interval series would silently read as zero.
func TestRuntimeSnapshotKeepsItsOwnHistograms(t *testing.T) {
	first := readRuntime()
	if first.sched == nil || first.gc == nil {
		t.Fatal("the snapshot kept no histograms")
	}
	before := append([]uint64(nil), first.sched.counts...)
	// Make the runtime do some work, then read again.
	for i := 0; i < 200; i++ {
		go func() {}()
	}
	time.Sleep(50 * time.Millisecond)
	_ = readRuntime()
	for i := range before {
		if first.sched.counts[i] != before[i] {
			t.Fatalf("the first snapshot's counts changed after a later read: bucket %d, %d -> %d",
				i, before[i], first.sched.counts[i])
		}
	}
}

func TestReadEnvironment(t *testing.T) {
	e := readEnvironment()
	if e.GOMAXPROCS < 1 || e.NumCPU < 1 {
		t.Fatalf("%+v", e)
	}
}

// TestPauseSuffixCarriesTheIntervalsFigures pins the progress line's new
// fields: the existing ones keep their names and order (progressLine is
// unchanged), and the suffix carries the three figures that classify a
// silence.
func TestPauseSuffixCarriesTheIntervalsFigures(t *testing.T) {
	got := progressLine("reconnect", 95*time.Second, 3855, 33402, 27105, 12, 51234, 1.75) +
		pauseSuffix(5800*time.Millisecond, 0.088, 21.47)
	want := "[sim reconnect] 1m35s 3855/33402 refusals=27105 fails=12 goroutines=51234 mutex-wait=1.75s/s" +
		" | stall=5800ms gc=88ms sched-max=21470ms"
	if got != want {
		t.Fatalf("progress line\n got %q\nwant %q", got, want)
	}
}
