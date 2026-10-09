package sim

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// This file holds the parts of the repeat driver that are worth testing
// without spawning anything: what a batch's figures mean, and how a summary
// is computed from them. The driver itself is TestFailoverRepeat.
//
// A repeat exists because the measurement cannot characterise a build from
// one run. Four runs of identical code on 2026-09-07 gave 1m39.804s,
// 5m36.225s, and twice not-reached — a spread covering every outcome this
// project had been reading as a difference between builds (WI 1374).

// repeatRun is one measurement's outcome within a batch.
type repeatRun struct {
	Index int    `json:"index"`
	Dir   string `json:"dir,omitempty"`
	// Failed runs carry Why and no figures. A run that did not complete is
	// never silently dropped from the distribution.
	Failed bool   `json:"failed,omitempty"`
	Why    string `json:"why,omitempty"`

	StoreDir     string  `json:"store_dir,omitempty"`
	TimingSet    string  `json:"timing_set,omitempty"`
	Recovery     string  `json:"recovery,omitempty"`
	RecoverySec  float64 `json:"recovery_sec,omitempty"`
	FillTime     string  `json:"fill_time,omitempty"`
	PeakOnline   int     `json:"peak_online,omitempty"`
	TroughOnline int     `json:"trough_online,omitempty"`
	OnlineAtEnd  int     `json:"online_at_end,omitempty"`
	// Leader placement (WI 1493), so runs can be compared within a shape.
	VictimPolicy      string `json:"victim_policy,omitempty"`
	LeaderBeforeCrash string `json:"leader_before_crash,omitempty"`
	LeaderAfter       string `json:"leader_after,omitempty"`
	StormTarget       string `json:"storm_target,omitempty"`
	// Subscribe refusals (WI 1509). Kept per run, not only in the batch total,
	// because a batch total cannot say whether one run carried all of them.
	SubRefusals       int64 `json:"sub_refusals,omitempty"`
	SubRefusedClients int   `json:"sub_refused_clients,omitempty"`
	// Bounds is the run judged against the owner's pass bounds (WI 1734),
	// from the run.json figures in in.
	Bounds *boundsVerdict `json:"bounds,omitempty"`
	in     boundsInput
}

// Reached reports whether every client came back within the window. A run
// that did not reach has a negative RecoverySec, never zero, so it cannot be
// averaged into a distribution as though it were instant.
func (r repeatRun) reached() bool { return !r.Failed && r.RecoverySec >= 0 }

// repeatSummary is what a batch says. It separates the three populations a
// single number would merge: runs that reached, runs that ran and did not,
// and runs that failed to produce a measurement at all.
type repeatSummary struct {
	Runs       int         `json:"runs"`
	Reached    int         `json:"reached"`
	NotReached int         `json:"not_reached"`
	FailedRuns int         `json:"failed"`
	MedianSec  float64     `json:"median_sec,omitempty"`
	MinSec     float64     `json:"min_sec,omitempty"`
	MaxSec     float64     `json:"max_sec,omitempty"`
	Detail     []repeatRun `json:"detail"`
	// TimingSets are the distinct timing labels the runs used; a batch
	// that mixed them is visible as more than one (WI 1475).
	TimingSets []string `json:"timing_sets,omitempty"`
	// SubRefusals is the batch total and SubRefusedRuns how many runs saw any.
	// A recovery figure counts a client as back when it reconnects, and a
	// client whose resubscribe was refused is back and subscribed to nothing;
	// these two say whether that happened in the batch being read (WI 1509).
	SubRefusals    int64 `json:"sub_refusals"`
	SubRefusedRuns int   `json:"sub_refused_runs"`
	// Bounds judges the runs that produced a measurement against the pass
	// bounds, each run's verdict beside the batch's (WI 1734).
	Bounds *batchVerdict `json:"bounds,omitempty"`
	// Provenance is the driver's own: how the batch was invoked and from
	// which tree. Each run's run.json carries its child's (WI 1379).
	Provenance *Provenance `json:"provenance,omitempty"`
}

// summarise computes the distribution over the runs that reached. Runs that
// did not reach are counted, never folded into the median: a batch of two
// fast runs and two that never recovered is not a batch with a middling
// median, and reporting it as one would be the same mistake the single-run
// headline made.
func summarise(runs []repeatRun) repeatSummary {
	s := repeatSummary{Runs: len(runs), Detail: runs}
	var secs []float64
	var judged []namedRun
	seenSet := map[string]bool{}
	for _, r := range runs {
		if !r.Failed {
			judged = append(judged, namedRun{Name: fmt.Sprintf("run %d", r.Index), In: r.in})
		}
		if r.TimingSet != "" && !seenSet[r.TimingSet] {
			seenSet[r.TimingSet] = true
			s.TimingSets = append(s.TimingSets, r.TimingSet)
		}
		s.SubRefusals += r.SubRefusals
		if r.SubRefusals > 0 {
			s.SubRefusedRuns++
		}
		switch {
		case r.Failed:
			s.FailedRuns++
		case r.reached():
			s.Reached++
			secs = append(secs, r.RecoverySec)
		default:
			s.NotReached++
		}
	}
	if len(judged) > 0 {
		b := judgeBatch(judged)
		s.Bounds = &b
	}
	if len(secs) == 0 {
		return s
	}
	sort.Float64s(secs)
	s.MinSec, s.MaxSec = secs[0], secs[len(secs)-1]
	if n := len(secs); n%2 == 1 {
		s.MedianSec = secs[n/2]
	} else {
		s.MedianSec = (secs[n/2-1] + secs[n/2]) / 2
	}
	return s
}

// String is the line an operator reads. It never reports a median without
// saying how many runs did not reach, because the median of the runs that
// finished is meaningless on its own.
func (s repeatSummary) String() string {
	out := fmt.Sprintf("n=%d: %d reached, %d did not, %d failed to run", s.Runs, s.Reached, s.NotReached, s.FailedRuns)
	if s.Reached > 0 {
		out += fmt.Sprintf("; of those that reached, median %s, min %s, max %s",
			dur(s.MedianSec), dur(s.MinSec), dur(s.MaxSec))
	}
	if len(s.TimingSets) > 0 {
		out += "; timings: " + strings.Join(s.TimingSets, " | ")
	}
	if s.SubRefusals > 0 {
		out += fmt.Sprintf("; %d subscribe refusals in %d runs", s.SubRefusals, s.SubRefusedRuns)
	}
	if s.Bounds != nil {
		out += "; bounds: " + strings.ReplaceAll(boundsVerdictLine(s.Bounds.boundsVerdict), "**", "")
	}
	return out
}

func dur(sec float64) string {
	return time.Duration(sec * float64(time.Second)).Round(time.Millisecond).String()
}

// readRun reads one run's outcome from its artifact directory. The figures
// come from run.json and never from report.md: a report is prose for a
// human, and four of its rows were reworded the day before this was written.
func readRun(index int, dir string) repeatRun {
	r := repeatRun{Index: index, Dir: dir}
	inner, err := runArtifactDir(dir)
	if err != nil {
		return failedRun(r, err.Error())
	}
	if _, err := os.Stat(filepath.Join(inner, completeFile)); err != nil {
		return failedRun(r, "no completion marker: "+err.Error())
	}
	raw, err := os.ReadFile(filepath.Join(inner, "run.json")) //nolint:gosec // operator-chosen dir
	if err != nil {
		return failedRun(r, "run.json unreadable: "+err.Error())
	}
	var art struct {
		StoreDir          string  `json:"store_dir"`
		FillTime          string  `json:"fill_time"`
		Recovery          string  `json:"recovery"`
		RecoverySec       float64 `json:"recovery_sec"`
		PeakOnline        int     `json:"peak_online"`
		TroughOnline      int     `json:"trough_online"`
		OnlineAtEnd       int     `json:"online_at_end"`
		VictimPolicy      string  `json:"victim_policy"`
		LeaderBeforeCrash string  `json:"leader_before_crash"`
		LeaderAfter       string  `json:"leader_after"`
		StormTarget       string  `json:"storm_target"`
		SubRefusals       int64   `json:"sub_refusals"`
		SubRefusedClients int     `json:"sub_refused_clients"`
		Provenance        struct {
			Effective struct {
				TimingSet string `json:"timing_set"`
			} `json:"effective"`
		} `json:"provenance"`
	}
	if err := json.Unmarshal(raw, &art); err != nil {
		return failedRun(r, "run.json undecodable: "+err.Error())
	}
	if err := json.Unmarshal(raw, &r.in); err != nil {
		return failedRun(r, "run.json undecodable: "+err.Error())
	}
	v := judgeRun(r.in)
	r.Bounds = &v
	r.Dir, r.FillTime, r.Recovery, r.RecoverySec = inner, art.FillTime, art.Recovery, art.RecoverySec
	r.StoreDir, r.TimingSet = art.StoreDir, art.Provenance.Effective.TimingSet
	r.PeakOnline, r.TroughOnline, r.OnlineAtEnd = art.PeakOnline, art.TroughOnline, art.OnlineAtEnd
	r.VictimPolicy, r.LeaderBeforeCrash, r.LeaderAfter, r.StormTarget = art.VictimPolicy, art.LeaderBeforeCrash, art.LeaderAfter, art.StormTarget
	r.SubRefusals, r.SubRefusedClients = art.SubRefusals, art.SubRefusedClients
	return r
}

// validateStoreRoot reports whether dir is a path removeStore is permitted to
// delete: one strictly under the temporary root this process would itself have
// used.
//
// It is pure. It inspects the path and touches nothing, so a test may assert
// on live paths — "/", a home directory — without risk, and breaking the guard
// to confirm the test catches it costs a red test and nothing else. Keeping
// the decision separable from the deletion is the point: a guard whose only
// test drives the destructive path is safe only while the guard works (WI 1377).
func validateStoreRoot(dir string) error {
	root := os.TempDir()
	rel, err := filepath.Rel(root, dir)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("refusing to remove %q: it is not under %q", dir, root)
	}
	return nil
}

// removeStore removes a run's node stores. Ending a run once its marker
// appears skips t.TempDir's cleanup, so the driver that chose to end it early
// owns the cleanup that skipped: 11 GB accumulated in one evening's batches,
// and in a tmpfs data directory it is leaked RAM rather than disk.
//
// It removes only a path the measurement published and only one
// validateStoreRoot accepts. A driver that removes a path out of a JSON file
// is one malformed artifact away from removing something else, and no
// measurement is worth that.
func removeStore(dir string) (string, error) {
	if dir == "" {
		return "", nil // an artifact from before the store root was published
	}
	if err := validateStoreRoot(dir); err != nil {
		return "", err
	}
	if err := os.RemoveAll(dir); err != nil {
		return "", err
	}
	return dir, nil
}

// finishRun is the driver's step after a measurement has ended: read the
// run's figures, then remove the node stores it published. It is one function
// so that the sequence — figures first, then the store the artifact named —
// can be asserted on a synthetic artifact. Inline in the driver, the only
// evidence it ran was a log line at a scale the dev host cannot reach: with
// the call deleted, runs at 300 and 20,000 clients still left nothing behind,
// because the child cleaned up itself before the driver ended it (WI 1465).
//
// The note is what the driver logs about the store: where it was removed,
// where it was leaked and why, or nothing when no root was published or the
// run failed to read. A leaked store never fails the run; the measurement is
// still valid (WI 1377).
func finishRun(index int, dir string) (repeatRun, string) {
	r := readRun(index, dir)
	if r.Failed {
		return r, ""
	}
	removed, err := removeStore(r.StoreDir)
	switch {
	case err != nil:
		return r, fmt.Sprintf("stores leaked at %q: %v", r.StoreDir, err)
	case removed != "":
		return r, "removed the run's stores at " + removed
	}
	return r, ""
}

func failedRun(r repeatRun, why string) repeatRun {
	r.Failed, r.Why = true, why
	return r
}

// runArtifactDir finds the single per-run directory the measurement creates
// beneath the base it was given. More than one means a base was reused,
// which would make the figures ambiguous, so it is an error rather than a
// guess at the newest — guessing at the newest is what the throwaway driver
// did.
func runArtifactDir(base string) (string, error) {
	entries, err := os.ReadDir(base)
	if err != nil {
		return "", err
	}
	var dirs []string
	for _, e := range entries {
		if e.IsDir() {
			dirs = append(dirs, filepath.Join(base, e.Name()))
		}
	}
	switch len(dirs) {
	case 1:
		return dirs[0], nil
	case 0:
		return "", fmt.Errorf("no run directory under %s: the measurement wrote no artifact", base)
	default:
		return "", fmt.Errorf("%d run directories under %s: a run's base must not be reused", len(dirs), base)
	}
}
