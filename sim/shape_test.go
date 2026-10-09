package sim

import (
	"errors"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestOnlineShapeRecordsPeakTroughAndNinetyNine(t *testing.T) {
	from := time.Now()
	s := newOnlineShape(100000, from)

	// Round 12's shape: a fall to zero when both survivors fenced, a climb
	// to 99,911, then a decay. Its published headline was 88,570.
	for _, n := range []int{67178, 33333, 0, 35578, 93278, 99911, 99137, 90570, 88570} {
		s.observe(n, n)
	}

	if got := s.peakLine(); !strings.HasPrefix(got, "99911 of 100000") {
		t.Errorf("peak = %q, want the 99,911 the headline never mentioned", got)
	}
	if got := s.troughLine(); !strings.HasPrefix(got, "0 of 100000") {
		t.Errorf("trough = %q, want 0", got)
	}
	if got := s.toNinetyNine(); got == "not reached" || got == "not measured" {
		t.Errorf("time to 99%% = %q; 99,911 of 100,000 is past 99%%", got)
	}
}

func TestOnlineShapeReportsNinetyNineNotReached(t *testing.T) {
	s := newOnlineShape(100000, time.Now())
	// Round 15's shape: never got close.
	for _, n := range []int{67178, 0, 21808, 12933} {
		s.observe(n, n)
	}
	if got := s.toNinetyNine(); got != "not reached" {
		t.Errorf("time to 99%% = %q, want %q", got, "not reached")
	}
	if got := s.peakLine(); !strings.HasPrefix(got, "67178 of 100000") {
		t.Errorf("peak = %q", got)
	}
}

// TestOnlineShapeMarksTheReturnNotThePreStormReading replays the shape of
// the run that produced WI 1498: the window loop's first samples are taken
// before the clients notice the crash, so the count starts at target, falls,
// and comes back. The mark belongs to the return.
//
// The published figures are r1 of the 2026-09-15 client-host batch
// (sim/results/gtr-clients-2026-09-15/r1-remote-1k): a time to 99% of 16 ms
// against a lowest count of 666 of 1,000.
func TestOnlineShapeMarksTheReturnNotThePreStormReading(t *testing.T) {
	base := time.Now()
	s := newOnlineShape(1000, base)
	for _, sample := range []struct {
		at     time.Duration
		online int
	}{
		{0, 1000},                     // the crash has happened; nobody has noticed
		{16 * time.Millisecond, 1000}, // the sample that used to set the mark
		{time.Second, 666},            // the storm
		{10 * time.Second, 800},
		{24 * time.Second, 989}, // one short of the line
		{25 * time.Second, 992}, // the return
		{26 * time.Second, 1000},
	} {
		s.from = base.Add(-sample.at)
		s.observe(sample.online, sample.online)
	}

	if got := s.toNinetyNine(); got != "25s" {
		t.Errorf("time to 99%% = %q, want %q: the mark belongs to the return, not to a reading from before the storm", got, "25s")
	}
	if got := s.ninetyNineSeconds(); got < 24.9 || got > 25.1 {
		t.Errorf("ninetyNineSeconds() = %v, want about 25", got)
	}
	if !strings.HasPrefix(s.troughLine(), "666 of 1000") {
		t.Errorf("trough = %q, want the 666 the 16 ms figure contradicted", s.troughLine())
	}
	if !strings.HasPrefix(s.peakLine(), "1000 of 1000") {
		t.Errorf("peak = %q: the first sample is the pre-storm level and peak is meant to keep it", s.peakLine())
	}
}

// TestOnlineShapeReportsNotReachedWhenTheCountNeverFell: a run whose count
// never fell below the line never returned to it, and has no time to report.
// That is a real outcome at small client counts and must not read as an
// instant recovery.
func TestOnlineShapeReportsNotReachedWhenTheCountNeverFell(t *testing.T) {
	base := time.Now()
	s := newOnlineShape(1000, base)
	for i, online := range []int{1000, 1000, 995, 1000, 991, 1000} {
		s.from = base.Add(-time.Duration(i) * time.Second)
		s.observe(online, online)
	}
	if got := s.toNinetyNine(); got != "not reached" {
		t.Errorf("time to 99%% = %q, want %q", got, "not reached")
	}
	if got := s.ninetyNineSeconds(); got != -1 {
		t.Errorf("ninetyNineSeconds() = %v, want -1 so an unreached line cannot average as zero", got)
	}
}

func TestOnlineShapeWithNoSamples(t *testing.T) {
	s := newOnlineShape(100000, time.Now())
	for _, got := range []string{s.peakLine(), s.troughLine(), s.toNinetyNine()} {
		if got != "not measured" {
			t.Errorf("with no samples: %q, want %q", got, "not measured")
		}
	}
}

// TestCounterValueSaysAReadFailed is the row that has carried a published
// "NaN" in three of the five most recent rounds (10b, 12 and 14), where it
// read as a quirk of the number rather than as a missing measurement.
func TestCounterValueSaysAReadFailed(t *testing.T) {
	for _, bad := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		got := counterValue(bad)
		if got != "read failed" {
			t.Errorf("counterValue(%v) = %q, want %q", bad, got, "read failed")
		}
		if strings.Contains(got, "NaN") || strings.Contains(got, "Inf") {
			t.Errorf("counterValue(%v) = %q: a failed read must not look like a number", bad, got)
		}
	}
	if got := counterValue(341394); got != "341394" {
		t.Errorf("counterValue(341394) = %q", got)
	}
}

// TestRunsCoveredSaysItIsOneSample: a single-run report must not be quotable
// as a property of a build without saying that is what it is.
func TestRunsCoveredSaysItIsOneSample(t *testing.T) {
	got := runsCovered()
	if !strings.Contains(got, "single run") {
		t.Errorf("runsCovered() = %q; a single-run report must say so", got)
	}
	t.Setenv("HOARDMQ_SIM_REPEAT_INDEX", "3")
	t.Setenv("HOARDMQ_SIM_REPEAT_TOTAL", "5")
	if got := runsCovered(); !strings.Contains(got, "run 3 of 5") {
		t.Errorf("runsCovered() under a repeat = %q", got)
	}
}

// TestShapeAgreesWithThePublishedRounds is WI 1373's fourth acceptance
// criterion: the new figures, derived by this code from the four published
// rounds' own progress logs, must agree with what those runs did. It is the
// test that makes the other four trustworthy, and it is the easiest to skip
// because the data is already in hand and "obviously" right.
func TestShapeAgreesWithThePublishedRounds(t *testing.T) {
	if _, err := os.Stat("results"); errors.Is(err, fs.ErrNotExist) {
		t.Skip("sim/results/ is not in this checkout: the public repository omits the published results, which the private repository keeps")
	}
	for _, want := range []struct {
		round            string
		peak, trough     int
		peakAt, troughAt string
	}{
		{"round10b", 99965, 33333, "3m5s", "1m5s"},
		{"round11", 99809, 33845, "5m48s", "1m3s"},
		{"round12", 99911, 0, "3m50s", "1m5s"},
		{"round13", 99709, 0, "1m33s", "1m3s"},
	} {
		t.Run(want.round, func(t *testing.T) {
			logs, err := filepath.Glob("results/" + want.round + "-artifacts/*/progress.log")
			if err != nil {
				t.Fatal(err)
			}
			if len(logs) == 0 {
				t.Fatalf("no published progress log for %s under results/", want.round)
			}
			body, err := os.ReadFile(logs[0]) //nolint:gosec // a published artifact in this repo
			if err != nil {
				t.Fatal(err)
			}
			// The log's timestamps are relative to the crash, as the shape's
			// are; replaying them recovers the same instants.
			re := regexp.MustCompile(`\]\s+((?:\d+m)?\d+s)\s+(\d+)/\d+`)
			base := time.Now()
			s := newOnlineShape(100000, base)
			var samples int
			for _, m := range re.FindAllStringSubmatch(string(body), -1) {
				d, err := time.ParseDuration(strings.Replace(m[1], "m", "m0s", 1))
				if err != nil {
					d, err = time.ParseDuration(m[1])
				}
				if err != nil {
					continue
				}
				n, _ := strconv.Atoi(m[2])
				s.from = base.Add(-d) // replay the sample at its logged offset
				s.observe(n, n)
				samples++
			}
			if samples < 5 {
				t.Fatalf("%s: only %d samples parsed; the log format has moved", want.round, samples)
			}
			if s.peak != want.peak || s.trough != want.trough {
				t.Errorf("%s: peak/trough = %d/%d, want %d/%d", want.round, s.peak, s.trough, want.peak, want.trough)
			}
			if got := s.peakAt.Round(time.Second).String(); got != want.peakAt {
				t.Errorf("%s: peak at %s, want %s", want.round, got, want.peakAt)
			}
			if got := s.troughAt.Round(time.Second).String(); got != want.troughAt {
				t.Errorf("%s: trough at %s, want %s", want.round, got, want.troughAt)
			}
		})
	}
}

// The 99% and 99.9% marks are of the recovered count, not the online one: a
// population that is online with its subscriptions refused has not returned
// to the line, and since WI 1618 the headline says so.
func TestOnlineShapeMarksFollowTheRecoveredCount(t *testing.T) {
	base := time.Now()
	s := newOnlineShape(1000, base)
	for _, sample := range []struct {
		at                time.Duration
		online, recovered int
	}{
		{0, 1000, 1000},
		{time.Second, 400, 400},
		{10 * time.Second, 1000, 400}, // back online, nothing granted
		{20 * time.Second, 1000, 995}, // past the 99% line, short of 99.9%
		{30 * time.Second, 1000, 1000},
	} {
		s.from = base.Add(-sample.at)
		s.observe(sample.online, sample.recovered)
	}
	if got := s.ninetyNineSeconds(); got < 19.9 || got > 20.1 {
		t.Errorf("time to 99%% = %v, want about 20: at 10 s every client was online and 400 were recovered", got)
	}
	if got := s.nineNineNineSeconds(); got < 29.9 || got > 30.1 {
		t.Errorf("time to 99.9%% = %v, want about 30: 995 of 1000 is short of the line", got)
	}
}
