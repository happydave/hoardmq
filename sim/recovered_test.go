package sim

import (
	"testing"
	"time"
)

// reading is one sample of the recovered count, at an offset from the crash.
type reading struct {
	at        time.Duration
	recovered int
}

// watchSeries folds a series into a watch and returns its verdict as an
// offset from the crash (negative when there is none) and the reason.
func watchSeries(target int, hold, window time.Duration, series []reading) (time.Duration, string) {
	crash := time.Unix(1_700_000_000, 0)
	w := newHoldWatch(target, hold, crash.Add(window))
	for _, r := range series {
		if w.observe(crash.Add(r.at), r.recovered) {
			break
		}
	}
	instant, reason := w.verdict()
	if instant.IsZero() {
		return -1, reason
	}
	return instant.Sub(crash), reason
}

// The interval rule, case by case.
func TestHoldWatchVerdicts(t *testing.T) {
	const window = 10 * time.Minute
	hold := 60 * time.Second
	cases := []struct {
		name   string
		series []reading
		want   time.Duration
		reason string
	}{
		{
			// C1: the instant is the start of the interval, not its end.
			name:   "returns and stays",
			series: []reading{{0, 100}, {5 * time.Second, 40}, {30 * time.Second, 100}, {95 * time.Second, 100}},
			want:   30 * time.Second,
		},
		{
			// C2 (WI 1515): a drop inside the hold restarts it, and the
			// instant moves to the interval that actually held.
			name: "drops inside the hold",
			series: []reading{{0, 100}, {5 * time.Second, 40}, {30 * time.Second, 100},
				{89 * time.Second, 99}, {100 * time.Second, 100}, {161 * time.Second, 100}},
			want: 100 * time.Second,
		},
		{
			// C3 (WI 1698): nothing ever dropped, so nothing recovered.
			name:   "never fell",
			series: []reading{{0, 100}, {30 * time.Second, 100}, {120 * time.Second, 100}},
			want:   -1,
			reason: recoveryNeverFell,
		},
		{
			name:   "fell and never returned",
			series: []reading{{0, 100}, {5 * time.Second, 40}, {600 * time.Second, 99}},
			want:   -1,
			reason: recoveryNeverBack,
		},
		{
			// C4, first half: the instant is inside the window and the hold
			// completes after it. The confirmation is not what the window
			// bounds.
			name: "hold completes past the deadline",
			series: []reading{{0, 100}, {5 * time.Second, 40}, {window - 10*time.Second, 100},
				{window + 51*time.Second, 100}},
			want: window - 10*time.Second,
		},
		{
			// C4, second half: an interval that begins after the window has
			// closed is not a recovery this run may report.
			name: "returns after the window closed",
			series: []reading{{0, 100}, {5 * time.Second, 40}, {window + time.Second, 100},
				{window + 120*time.Second, 100}},
			want:   -1,
			reason: recoveryLate,
		},
		{
			name:   "still holding when watching ended",
			series: []reading{{0, 100}, {5 * time.Second, 40}, {30 * time.Second, 100}, {59 * time.Second, 100}},
			want:   -1,
			reason: recoveryHeldTooShort,
		},
		{
			// C5's shape at this layer: one client short is not the target.
			name: "one short does not start the interval",
			series: []reading{{0, 100}, {5 * time.Second, 40}, {30 * time.Second, 99},
				{100 * time.Second, 99}, {110 * time.Second, 100}, {171 * time.Second, 100}},
			want: 110 * time.Second,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, reason := watchSeries(100, hold, window, c.series)
			if got != c.want || reason != c.reason {
				t.Fatalf("got %s / %q, want %s / %q", got, reason, c.want, c.reason)
			}
		})
	}
}

// A zero hold is the old reading — the first return to the target — and is
// how the run computes the CONNACK figure it publishes beside the new one.
// The fell-first requirement still applies, which the in-process mode lacked.
func TestHoldWatchZeroHoldIsTheFirstReturn(t *testing.T) {
	got, reason := watchSeries(100, 0, 10*time.Minute, []reading{
		{0, 100}, {5 * time.Second, 40}, {30 * time.Second, 100}, {95 * time.Second, 100}})
	if got != 30*time.Second || reason != recoveryReached {
		t.Fatalf("got %s / %q, want 30s reached", got, reason)
	}
	got, reason = watchSeries(100, 0, 10*time.Minute, []reading{{0, 100}, {30 * time.Second, 100}})
	if got != -1 || reason != recoveryNeverFell {
		t.Fatalf("a run where nothing fell: got %s / %q", got, reason)
	}
}

// The instant is the first qualifying interval's start and does not move
// afterwards, whatever the population does next.
func TestHoldWatchSettlesOnce(t *testing.T) {
	crash := time.Unix(1_700_000_000, 0)
	w := newHoldWatch(10, time.Minute, crash.Add(10*time.Minute))
	w.observe(crash.Add(time.Second), 4)
	w.observe(crash.Add(10*time.Second), 10)
	if !w.observe(crash.Add(70*time.Second), 10) {
		t.Fatal("the hold completed and the watch did not settle")
	}
	// A whole second interval after it settled: enough for a watch that kept
	// deciding to settle again, so the assertion below has something to catch.
	w.observe(crash.Add(80*time.Second), 0)
	w.observe(crash.Add(200*time.Second), 10)
	w.observe(crash.Add(280*time.Second), 10)
	instant, reason := w.verdict()
	if reason != recoveryReached || instant.Sub(crash) != 10*time.Second {
		t.Fatalf("instant moved: %s / %q", instant.Sub(crash), reason)
	}
}

// holdStart reports how far an unfinished hold had got, so a run that ends
// mid-interval says so rather than reporting nothing.
func TestHoldWatchReportsAnOpenInterval(t *testing.T) {
	crash := time.Unix(1_700_000_000, 0)
	w := newHoldWatch(10, time.Minute, crash.Add(10*time.Minute))
	w.observe(crash.Add(time.Second), 4)
	w.observe(crash.Add(20*time.Second), 10)
	if got := w.holdStart(); got.Sub(crash) != 20*time.Second {
		t.Fatalf("open interval start %s, want 20s", got.Sub(crash))
	}
	w.observe(crash.Add(30*time.Second), 9)
	if got := w.holdStart(); !got.IsZero() {
		t.Fatalf("a broken interval still reports a start: %s", got.Sub(crash))
	}
}

// A run with no clients cannot recover, and must not report that it did.
func TestHoldWatchNoTarget(t *testing.T) {
	got, reason := watchSeries(0, time.Minute, 10*time.Minute, []reading{{0, 0}, {time.Minute, 0}})
	if got != -1 || reason != recoveryNeverFell {
		t.Fatalf("got %s / %q", got, reason)
	}
}
