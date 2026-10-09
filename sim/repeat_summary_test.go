package sim

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestSummariseKeepsNotReachedOutOfTheDistribution is the defect this driver
// exists to avoid producing. Two of the four runs in the 2026-09-07 batch
// never got every client back; a summary that folded them in as a duration —
// zero, or a negative — would report a median better than any run actually
// achieved.
func TestSummariseKeepsNotReachedOutOfTheDistribution(t *testing.T) {
	s := summarise([]repeatRun{
		{Index: 1, Recovery: "1m39.804s", RecoverySec: 99.804},
		{Index: 2, Recovery: "5m36.225s", RecoverySec: 336.225},
		{Index: 3, Recovery: "not reached in 10m: 14396 of 100000 online", RecoverySec: -1},
		{Index: 4, Recovery: "not reached in 10m: 17507 of 100000 online", RecoverySec: -1},
	})

	if s.Runs != 4 || s.Reached != 2 || s.NotReached != 2 || s.FailedRuns != 0 {
		t.Fatalf("populations: runs=%d reached=%d notReached=%d failed=%d", s.Runs, s.Reached, s.NotReached, s.FailedRuns)
	}
	if s.MinSec != 99.804 || s.MaxSec != 336.225 {
		t.Errorf("min/max = %v/%v, want the two runs that reached", s.MinSec, s.MaxSec)
	}
	if want := (99.804 + 336.225) / 2; s.MedianSec != want {
		t.Errorf("median = %v, want %v — the median of those that reached", s.MedianSec, want)
	}
	if s.MinSec < 0 || s.MedianSec < 0 {
		t.Errorf("a run that did not reach was folded into the distribution: min=%v median=%v", s.MinSec, s.MedianSec)
	}
}

// TestSummaryStringAlwaysSaysHowManyDidNotReach: a median without that count
// is the single-number claim this whole work item exists to stop.
func TestSummaryStringAlwaysSaysHowManyDidNotReach(t *testing.T) {
	got := summarise([]repeatRun{
		{Index: 1, RecoverySec: 99.804},
		{Index: 2, RecoverySec: -1},
	}).String()
	for _, want := range []string{"n=2", "1 reached", "1 did not", "median"} {
		if !strings.Contains(got, want) {
			t.Errorf("summary %q is missing %q", got, want)
		}
	}
}

func TestSummariseWithNothingThatReached(t *testing.T) {
	s := summarise([]repeatRun{{Index: 1, RecoverySec: -1}, {Index: 2, Failed: true, Why: "no marker"}})
	if s.MedianSec != 0 || s.MinSec != 0 || s.MaxSec != 0 {
		t.Errorf("a batch where nothing reached must report no distribution: %+v", s)
	}
	if !strings.Contains(s.String(), "1 failed to run") {
		t.Errorf("summary %q does not report the failed run", s.String())
	}
}

func TestRecoverySecondsDistinguishesNotReached(t *testing.T) {
	crash := time.Now()
	if got := recoverySeconds(time.Time{}, crash); got >= 0 {
		t.Errorf("a run that did not reach gave %v; it must be negative, never zero", got)
	}
	if got := recoverySeconds(crash.Add(90*time.Second), crash); got < 89 || got > 91 {
		t.Errorf("recoverySeconds = %v, want about 90", got)
	}
}

func TestRunArtifactDirRefusesToGuess(t *testing.T) {
	base := t.TempDir()
	if _, err := runArtifactDir(base); err == nil {
		t.Error("an empty base must be an error, not an empty result")
	}
	if err := os.MkdirAll(filepath.Join(base, "TestFailover-1000-120000"), 0o750); err != nil {
		t.Fatal(err)
	}
	if got, err := runArtifactDir(base); err != nil || filepath.Base(got) != "TestFailover-1000-120000" {
		t.Errorf("one directory: got %q, %v", got, err)
	}
	if err := os.MkdirAll(filepath.Join(base, "TestFailover-1000-130000"), 0o750); err != nil {
		t.Fatal(err)
	}
	_, err := runArtifactDir(base)
	if err == nil || !strings.Contains(err.Error(), "must not be reused") {
		t.Errorf("two directories must be an error rather than a guess at the newest; got %v", err)
	}
}

// TestReadRunRequiresTheMarker: the marker is the signal, and its absence is
// a failed run rather than a run read from a half-written artifact.
func TestReadRunRequiresTheMarker(t *testing.T) {
	base := t.TempDir()
	inner := filepath.Join(base, "TestFailover-1000-120000")
	if err := os.MkdirAll(inner, 0o750); err != nil {
		t.Fatal(err)
	}
	art := map[string]any{
		"fill_time": "22.68s", "recovery": "1m39.804s", "recovery_sec": 99.804,
		"peak_online": 99709, "trough_online": 0, "online_at_end": 100000,
		"victim_policy": "before-leader", "leader_before_crash": "n1", "leader_after": "n1", "storm_target": "n1",
	}
	raw, err := json.Marshal(art)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inner, "run.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}

	if r := readRun(1, base); !r.Failed || !strings.Contains(r.Why, "completion marker") {
		t.Fatalf("a complete-looking run.json with no marker must fail: %+v", r)
	}

	if err := writeComplete(inner, 1000, "1m39.804s"); err != nil {
		t.Fatal(err)
	}
	r := readRun(1, base)
	if r.Failed {
		t.Fatalf("with the marker present the run must read: %s", r.Why)
	}
	if r.RecoverySec != 99.804 || r.Recovery != "1m39.804s" || r.PeakOnline != 99709 {
		t.Errorf("figures came out wrong: %+v", r)
	}
	if r.VictimPolicy != "before-leader" || r.LeaderBeforeCrash != "n1" || r.LeaderAfter != "n1" || r.StormTarget != "n1" {
		t.Errorf("leader placement came out wrong: %+v", r)
	}
}

func TestReadRunFailsOnUndecodableArtifact(t *testing.T) {
	base := t.TempDir()
	inner := filepath.Join(base, "TestFailover-1000-120000")
	if err := os.MkdirAll(inner, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inner, "run.json"), []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := writeComplete(inner, 1000, "1m39.804s"); err != nil {
		t.Fatal(err)
	}
	if r := readRun(1, base); !r.Failed || !strings.Contains(r.Why, "undecodable") {
		t.Fatalf("an unreadable artifact must be a failed run, not a zero one: %+v", r)
	}
}

// TestSettleLimitIsTheMeasurementsOwnRule: the driver must wait for exactly
// the condition the measurement will test, not a second opinion about it.
func TestSettleLimitIsTheMeasurementsOwnRule(t *testing.T) {
	if got := settleLimit(100000); got != 10000 {
		t.Errorf("settleLimit(100000) = %d, want 10000", got)
	}
	if got := settleLimit(1000); got != 5000 {
		t.Errorf("settleLimit(1000) = %d, want the 5000 floor", got)
	}
}

// TestValidateStoreRootRefusesAnythingOutsideTheTempRoot is the guard on a
// driver that removes a path taken from a JSON file. One malformed artifact
// must not be able to make it delete something else.
//
// It asserts on validateStoreRoot, not removeStore, and that is the whole
// design. validateStoreRoot only inspects the path, so these live paths are
// never handed to os.RemoveAll — which means the obvious way to check this
// test actually works, breaking the guard and expecting a failure, costs a red
// test and nothing else. A version of this test that called the remover with
// these same arguments would be safe only while the guard it was testing still
// held, and would delete a home directory the moment anyone checked (WI 1377).
func TestValidateStoreRootRefusesAnythingOutsideTheTempRoot(t *testing.T) {
	bad := []string{"/", "/etc", os.TempDir() + "/..", filepath.Dir(os.TempDir())}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		bad = append(bad, home)
	}
	for _, dir := range bad {
		if err := validateStoreRoot(dir); err == nil {
			t.Errorf("validateStoreRoot(%q) was allowed; it must refuse anything outside %q", dir, os.TempDir())
		}
	}
}

func TestValidateStoreRootAcceptsAPathUnderTheTempRoot(t *testing.T) {
	under := filepath.Join(os.TempDir(), "TestFailoverStores-accepted")
	if err := validateStoreRoot(under); err != nil {
		t.Errorf("a path under %q must be accepted: %v", os.TempDir(), err)
	}
}

func TestRemoveStoreRemovesARunsStores(t *testing.T) {
	// Under the real temp root, because that is what the guard checks, and
	// created here so the test only ever deletes what it made itself.
	dir, err := os.MkdirTemp("", "TestFailoverStores")
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"n1", "n2", "n3"} {
		if err := os.MkdirAll(filepath.Join(dir, n, "data"), 0o750); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "n1", "data", "wal"), []byte("records"), 0o600); err != nil {
		t.Fatal(err)
	}

	removed, err := removeStore(dir)
	if err != nil || removed != dir {
		t.Fatalf("removeStore: %q, %v", removed, err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("the stores are still there: %v", err)
	}
	// Removing it twice is not an error: a run may have cleaned up itself.
	if _, err := removeStore(dir); err != nil {
		t.Errorf("removing an absent store is not an error: %v", err)
	}
}

func TestRemoveStoreWithNoStoreRootIsNotAnError(t *testing.T) {
	if removed, err := removeStore(""); err != nil || removed != "" {
		t.Errorf("an artifact with no store root is not an error: %q, %v", removed, err)
	}
}

// writeFinishedArtifact builds the artifact a finished measurement leaves: one
// run directory under base, a run.json carrying the figures and the store
// root it was given, and — unless withMarker is false — the completion
// marker.
func writeFinishedArtifact(t *testing.T, base, storeDir string, withMarker bool) {
	t.Helper()
	inner := filepath.Join(base, "TestFailover-1000-120000")
	if err := os.MkdirAll(inner, 0o750); err != nil {
		t.Fatal(err)
	}
	art := map[string]any{
		"fill_time": "22.68s", "recovery": "1m39.804s", "recovery_sec": 99.804,
		"peak_online": 99709, "trough_online": 0, "online_at_end": 100000,
		"store_dir": storeDir,
	}
	raw, err := json.Marshal(art)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inner, "run.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if withMarker {
		if err := writeComplete(inner, 1000, "1m39.804s"); err != nil {
			t.Fatal(err)
		}
	}
}

// TestFinishRunRemovesThePublishedStore is the assertion the end-to-end run
// cannot make on a fast host: that the driver's step, given a finished
// artifact, reads the figures and then removes the store root the artifact
// named. The store is created here, under the real temporary root, so the
// step only ever removes what this test made.
func TestFinishRunRemovesThePublishedStore(t *testing.T) {
	store, err := os.MkdirTemp("", "TestFailoverStores")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(store) })
	if err := os.WriteFile(filepath.Join(store, "wal"), []byte("records"), 0o600); err != nil {
		t.Fatal(err)
	}
	base := t.TempDir()
	writeFinishedArtifact(t, base, store, true)

	r, note := finishRun(1, base)
	if r.Failed {
		t.Fatalf("the run must read: %s", r.Why)
	}
	if r.RecoverySec != 99.804 || r.StoreDir != store {
		t.Errorf("figures came out wrong: %+v", r)
	}
	if _, err := os.Stat(store); !os.IsNotExist(err) {
		t.Errorf("the store is still there after the step: %v", err)
	}
	if !strings.Contains(note, "removed") || !strings.Contains(note, store) {
		t.Errorf("the note must say the store was removed and name it; got %q", note)
	}
}

// TestFinishRunLeavesARefusedStoreAndStillReads: a store root outside the
// guard's temporary root is refused, said so, and the figures are read
// regardless. The refused directory is one this test created — the
// temporary root is moved out from under it with t.Setenv rather than the
// path being invented — so a neutered guard could remove nothing but that.
func TestFinishRunLeavesARefusedStoreAndStillReads(t *testing.T) {
	store := t.TempDir() // under the real temporary root, for now
	base := t.TempDir()
	writeFinishedArtifact(t, base, store, true)
	moved, err := os.MkdirTemp("", "TestFailoverMovedRoot")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(moved) })
	t.Setenv("TMPDIR", moved) // from here the guard's root is moved; store is outside it

	r, note := finishRun(1, base)
	if r.Failed {
		t.Fatalf("a refused store must not fail the run: %s", r.Why)
	}
	if r.RecoverySec != 99.804 {
		t.Errorf("figures came out wrong: %+v", r)
	}
	if _, err := os.Stat(store); err != nil {
		t.Errorf("a refused store must be left alone: %v", err)
	}
	if !strings.Contains(note, "leaked") || !strings.Contains(note, store) {
		t.Errorf("the note must say the store was leaked and name it; got %q", note)
	}
}

// TestFinishRunOnAFailedRunTouchesNothing: no marker means no figures and
// no store to speak of.
func TestFinishRunOnAFailedRunTouchesNothing(t *testing.T) {
	store := t.TempDir()
	base := t.TempDir()
	writeFinishedArtifact(t, base, store, false)

	r, note := finishRun(1, base)
	if !r.Failed || !strings.Contains(r.Why, "completion marker") {
		t.Fatalf("a run with no marker must fail to read: %+v", r)
	}
	if note != "" {
		t.Errorf("a failed run has no store note; got %q", note)
	}
	if _, err := os.Stat(store); err != nil {
		t.Errorf("a failed run's store must be left alone: %v", err)
	}
}

// TestTreeReportPath: a recorded run with an artifact directory leaves the
// source tree alone (WI 1483).
func TestTreeReportPath(t *testing.T) {
	now := time.Date(2026, 9, 15, 14, 0, 0, 0, time.UTC)
	if got := treeReportPath(true, "/tmp/art", now); got != "" {
		t.Fatalf("a recorded run with an artifact directory writes %q into the tree", got)
	}
	if got := treeReportPath(false, "", now); got != "" {
		t.Fatalf("an unrecorded run writes %q into the tree", got)
	}
	if got, want := treeReportPath(true, "", now), filepath.Join("results", "failover-2026-09-15.md"); got != want {
		t.Fatalf("a recorded run with no artifact directory writes %q, want %q", got, want)
	}
}
