package sim

import (
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/happydave/hoardmq/internal/module"
)

// The rolling-restart arm (WI 2208): every node drained, stopped and
// restarted in turn, as a StatefulSet rolls, with the publish probe running.
// Since WI 1788 a draining node leaves the shard map before it stops, so each
// step moves the copies of the sessions that targeted it and moves them back
// when it rejoins; the record says what that cost per step.

const (
	rollEnv            = "HOARDMQ_SIM_ROLL"
	rollDrainLimitEnv  = "HOARDMQ_SIM_ROLL_DRAIN_LIMIT_SEC"
	rollDrainLimitDflt = 1800 * time.Second
	// rollCutEnv cuts a drain still running that many seconds after it
	// began, as the kubelet does at a pod's termination grace (WI 2251).
	rollCutEnv = "HOARDMQ_SIM_ROLL_CUT_SEC"
	// rollKillAfter is how long a cut node's shutdown may run: the kubelet
	// sends SIGTERM when the grace ends and SIGKILL two seconds later.
	rollKillAfter   = 2 * time.Second
	rollReadyLimit  = 5 * time.Minute
	rollGateClients = 300
	rollGateMinStep = 3 * time.Second
	// rollChartGraceSec is the chart's termination grace at its defaults:
	// cluster.drain timeoutSeconds 30 + confirmTimeoutSeconds 30 +
	// graceMarginSeconds 15 (deploy/helm/hoardmq/values.yaml). The harness has
	// no kubelet, so a drain past it is reported, and cut only at rollCutEnv.
	rollChartGraceSec = 75.0
	// rollReferenceRate is the most the chart's rolling-restart runbook
	// refused after WI 1788: 23 of 600 (hoardmq sim/results/leaving-2026-10-05).
	rollReferenceRate = 23.0 / 600.0
)

// rollDrainLimit is how long a step waits for its drain before stopping the
// node anyway.
func rollDrainLimit(getenv func(string) string) time.Duration {
	if v := getenv(rollDrainLimitEnv); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	return rollDrainLimitDflt
}

// rollCutAfter is how long after its start a running drain is cut; zero for
// never.
func rollCutAfter(getenv func(string) string) time.Duration {
	if v := getenv(rollCutEnv); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	return 0
}

// beyondChartGrace says a drain outlasted the chart's termination grace,
// past which the kubelet stops the pod.
func beyondChartGrace(drainSec float64) bool { return drainSec > rollChartGraceSec }

// rollOrder is the order a StatefulSet rolls its pods: highest name first.
func rollOrder(names []string) []string {
	out := append([]string(nil), names...)
	sort.Sort(sort.Reverse(sort.StringSlice(out)))
	return out
}

// rollRecord is roll.json.
type rollRecord struct {
	Clients     int      `json:"clients"`
	Failed      int      `json:"failed"`
	FillTime    string   `json:"fill_time"`
	TimingSet   string   `json:"timing_set"`
	Order       []string `json:"order"`
	HoldSec     int      `json:"hold_sec"`
	SampleMs    int      `json:"sample_ms"`
	DrainLimitS int      `json:"drain_limit_sec"`
	// CutS is the cut's time after a drain's start (0 for none) and DrainRate
	// the clients a drain redirected a second.
	CutS         int            `json:"cut_sec,omitempty"`
	DrainRate    int            `json:"drain_rate"`
	ChartGraceS  float64        `json:"chart_grace_sec"`
	RollSec      float64        `json:"roll_sec"`
	Steps        []rollStep     `json:"steps"`
	Population   rollPopulation `json:"population"`
	Probe        *probeRecord   `json:"probe,omitempty"`
	ProbeMissing string         `json:"probe_missing,omitempty"`
	// Retained is the retained probe, when it ran.
	Retained        *retainedProbeRecord `json:"retained,omitempty"`
	RetainedMissing string               `json:"retained_missing,omitempty"`
	// Refusals is the probe over the whole roll, from the first drain to the
	// last node ready, against the chart runbook's worst rate.
	Refusals      windowCounts `json:"roll_refusals"`
	ReferenceRate float64      `json:"reference_rate"`
	Findings      []string     `json:"findings"`
	StoreDir      string       `json:"store_dir"`
	Provenance    Provenance   `json:"provenance"`
}

// rollStep is one node's drain, stop and restart. Times are seconds after
// the roll began (the first drain's start).
type rollStep struct {
	Node             string             `json:"node"`
	DrainStartSec    float64            `json:"drain_start_sec"`
	DrainEndSec      float64            `json:"drain_end_sec"`
	DrainSec         float64            `json:"drain_sec"`
	DrainCompleted   bool               `json:"drain_completed"`
	Phases           []phaseMark        `json:"phases"`
	Status           module.DrainStatus `json:"status"`
	DrainCounters    map[string]float64 `json:"drain_counters"`
	BeyondChartGrace bool               `json:"beyond_chart_grace"`
	StopSec          float64            `json:"stop_sec"`
	RestartSec       float64            `json:"restart_sec"`
	RestartErr       string             `json:"restart_error,omitempty"`
	Ready            bool               `json:"ready"`
	ReadySec         float64            `json:"ready_sec"`
	// Maps is every (version, basis) a running node applied during the step,
	// first seen when.
	Maps []mapSeen `json:"maps"`
	// KeptMovedPeak is the most moved copies the running nodes kept at once
	// over the step, and Changes each map change with the moved copies kept
	// until the next one.
	KeptMovedPeak int                     `json:"kept_moved_peak"`
	Changes       []changeLag             `json:"changes"`
	Confirms      map[string]confirmDelta `json:"confirms"`
	Probe         windowCounts            `json:"probe"`
	// Failures is the rise of the forward and barrier failure counters over
	// the step, summed over the nodes running at both ends.
	Failures map[string]float64 `json:"failures"`
	// Copies is the rise of the replica copies dropped and kept, by reason,
	// and of the checks of an owner's targets, by outcome, summed as Failures
	// is.
	Copies map[string]float64 `json:"copies"`
	// Cut says the drain was still running at the cut, and CutStatus is its
	// status then; the node was then shut down within rollKillAfter. The
	// status counts no leases while clients are redirected, so CutLeases is
	// the leases the node's own table said it still owned at the cut.
	Cut       bool                `json:"cut,omitempty"`
	CutStatus *module.DrainStatus `json:"cut_status,omitempty"`
	CutLeases int                 `json:"cut_leases,omitempty"`
	// Empty is the rise of the sessions the nodes served empty, by reason,
	// summed as Failures is, and the restarted node's own from its restart
	// to its readiness.
	Empty map[string]float64 `json:"served_empty"`
}

type phaseMark struct {
	Phase string  `json:"phase"`
	AtSec float64 `json:"at_sec"`
}

type mapSeen struct {
	Node    string   `json:"node"`
	Version uint64   `json:"version"`
	Basis   []string `json:"basis"`
	AtSec   float64  `json:"at_sec"`
}

// confirmDelta is a node's confirmations over a step. As an owner: every
// confirmation it sent, those to old targets of a moved session, and the jobs
// it gave up because the session was not held here in time, because its
// durability wait kept failing, or because a send failed. As a recipient:
// those it received, those it acted on (a sealed or offline copy ended) with
// the time from receipt to act, and those that ended nothing here; a moved
// copy's confirmation is among the last.
type confirmDelta struct {
	SentMoved  int64   `json:"sent_moved"`
	Acted      int64   `json:"acted"`
	ActMsTotal float64 `json:"act_ms_total"`
	ActMsMax   float64 `json:"act_ms_max_since_start"`
	Sent       int64   `json:"sent"`
	Received   int64   `json:"received"`
	Ignored    int64   `json:"ignored"`
	Unheld     int64   `json:"unheld"`
	Undurable  int64   `json:"undurable"`
	Unsent     int64   `json:"unsent"`
}

type rollPopulation struct {
	OnlineAtEnd        int   `json:"online_at_end"`
	Target             int   `json:"target"`
	SessionLost        int64 `json:"session_lost"`
	SessionLostClients int   `json:"session_lost_clients"`
	SubRefusals        int64 `json:"sub_refusals"`
	SubDowngraded      int64 `json:"sub_downgraded"`
	Drops              int64 `json:"drops"`
	Refusals           int64 `json:"connect_refusals"`
}

// windowCounts is the probe's outcomes in one window, pairs and groups apart.
type windowCounts struct {
	Attempts      int64            `json:"attempts"`
	Refused       map[string]int64 `json:"refused"`
	Failed        int64            `json:"failed"`
	Groups        int64            `json:"group_attempts"`
	GroupsRefused map[string]int64 `json:"group_refused"`
	GroupsFailed  int64            `json:"group_failed"`
	Rate          float64          `json:"rate"`
}

func newWindowCounts() windowCounts {
	return windowCounts{Refused: map[string]int64{}, GroupsRefused: map[string]int64{}}
}

func (w *windowCounts) add(o probeOutcome) {
	if o.group {
		w.Groups++
		switch {
		case o.code != "":
			w.GroupsRefused[o.code]++
		case o.failed:
			w.GroupsFailed++
		}
		return
	}
	w.Attempts++
	switch {
	case o.code != "":
		w.Refused[o.code]++
	case o.failed:
		w.Failed++
	}
}

// refused is every refusal and failure in the window, pairs and groups.
func (w windowCounts) refused() int64 {
	return sumCounts(w.Refused) + w.Failed + sumCounts(w.GroupsRefused) + w.GroupsFailed
}

func (w *windowCounts) finish() {
	if n := w.Attempts + w.Groups; n > 0 {
		w.Rate = float64(w.refused()) / float64(n)
	}
}

// stepWindows counts outcomes by step: step i runs from its drain's start to
// the next one's, and the last to end. An outcome before the first start or
// at or after end is in none; the whole roll is returned beside the steps.
func stepWindows(starts []time.Time, end time.Time, outcomes []probeOutcome) ([]windowCounts, windowCounts) {
	steps := make([]windowCounts, len(starts))
	for i := range steps {
		steps[i] = newWindowCounts()
	}
	roll := newWindowCounts()
	for _, o := range outcomes {
		if len(starts) == 0 || o.at.Before(starts[0]) || !o.at.Before(end) {
			continue
		}
		i := sort.Search(len(starts), func(i int) bool { return starts[i].After(o.at) }) - 1
		steps[i].add(o)
		roll.add(o)
	}
	for i := range steps {
		steps[i].finish()
	}
	roll.finish()
	return steps, roll
}

// rollFindings names what a run reports as wrong or past its reference. It
// reads the record alone.
func rollFindings(r rollRecord) []string {
	out := []string{}
	for _, s := range r.Steps {
		switch {
		case s.Cut && s.CutStatus != nil:
			c := s.CutStatus
			out = append(out, fmt.Sprintf("%s: the drain was cut at %d s in phase %s, with %d clients and %d leases left and %d sessions awaiting confirmation",
				s.Node, r.CutS, c.Phase, c.ClientsRemaining, max(c.LeasesRemaining, s.CutLeases), c.SessionsAwaiting))
		case !s.DrainCompleted:
			out = append(out, fmt.Sprintf("%s: the drain did not complete within %d s", s.Node, r.DrainLimitS))
		}
		if n := sumLost(s.Empty); n > 0 {
			out = append(out, fmt.Sprintf("%s: %.0f sessions served empty during the step", s.Node, n))
		}
		if s.Status.SessionsUnconfirmed > 0 {
			out = append(out, fmt.Sprintf("%s: the drain ended with %d sessions unconfirmed", s.Node, s.Status.SessionsUnconfirmed))
		}
		for key, v := range s.DrainCounters {
			if strings.HasPrefix(key, "hoardmq_drain_placement_total{") && !strings.Contains(key, `"left"`) && v > 0 {
				out = append(out, fmt.Sprintf("%s: drain placement %s", s.Node, key))
			}
		}
		if s.BeyondChartGrace {
			out = append(out, fmt.Sprintf("%s: the drain took %.1f s, beyond the chart's %.0f s termination grace", s.Node, s.DrainSec, r.ChartGraceS))
		}
		if s.RestartErr != "" || !s.Ready {
			out = append(out, fmt.Sprintf("%s: not ready again after its restart %s", s.Node, s.RestartErr))
		}
	}
	if r.Probe == nil {
		out = append(out, "no probe: "+r.ProbeMissing)
	} else {
		lost := r.Probe.AckedNotDelivered
		if r.Probe.Groups != nil {
			lost += r.Probe.Groups.AckedNotDelivered + r.Probe.Groups.QoS2Duplicates
		}
		if lost > 0 {
			out = append(out, fmt.Sprintf("probe: %d acknowledged messages lost or QoS 2 group messages duplicated", lost))
		}
	}
	if r.Population.SessionLost > 0 {
		out = append(out, fmt.Sprintf("population: %d sessions rebuilt empty across %d clients", r.Population.SessionLost, r.Population.SessionLostClients))
	}
	if r.Population.OnlineAtEnd < r.Population.Target {
		out = append(out, fmt.Sprintf("population: %d of %d online at the end", r.Population.OnlineAtEnd, r.Population.Target))
	}
	if r.Refusals.Rate > r.ReferenceRate {
		out = append(out, fmt.Sprintf("probe: %.2f%% refused across the roll, above the chart runbook's %.2f%%", 100*r.Refusals.Rate, 100*r.ReferenceRate))
	}
	return out
}

// changeLag is one map change of a step and the moved copies the running
// nodes kept after it: their peak, and the time from the change to the last
// sample before the next change that read any (-1 when none did). Cleared
// says the segment's last sample read none.
type changeLag struct {
	Version  uint64   `json:"version"`
	AtSec    float64  `json:"at_sec"`
	Basis    []string `json:"basis"`
	KeptPeak int      `json:"kept_peak"`
	ClearSec float64  `json:"clear_sec"`
	Cleared  bool     `json:"cleared"`
}

// mapChangeLags lists a step's map changes, each dated by the first node to
// apply it, with what was kept from it until the next change. The version
// in force when the step began is the oldest the step saw, and is no change.
func mapChangeLags(maps []mapSeen, kept []keptSample) (peak int, lags []changeLag) {
	for _, k := range kept {
		peak = max(peak, k.kept)
	}
	if len(maps) == 0 {
		return peak, nil
	}
	oldest := maps[0].Version
	for _, m := range maps {
		oldest = min(oldest, m.Version)
	}
	first := map[uint64]mapSeen{}
	for _, m := range maps {
		if f, ok := first[m.Version]; m.Version > oldest && (!ok || m.AtSec < f.AtSec) {
			first[m.Version] = m
		}
	}
	for v, m := range first {
		lags = append(lags, changeLag{Version: v, AtSec: m.AtSec, Basis: m.Basis, ClearSec: -1})
	}
	sort.Slice(lags, func(i, j int) bool { return lags[i].Version < lags[j].Version })
	for i := range lags {
		end := math.Inf(1)
		if i+1 < len(lags) {
			end = lags[i+1].AtSec
		}
		last := -1
		for _, k := range kept {
			if k.atSec < lags[i].AtSec || k.atSec >= end {
				continue
			}
			lags[i].KeptPeak = max(lags[i].KeptPeak, k.kept)
			if k.kept > 0 {
				lags[i].ClearSec = k.atSec - lags[i].AtSec
			}
			last = k.kept
		}
		lags[i].Cleared = last == 0
	}
	return peak, lags
}

type keptSample struct {
	atSec float64
	kept  int
}

// rollDrainMetric picks the families a step reads from a node's metrics.
func rollDrainMetric(name string) bool {
	return strings.HasPrefix(name, "hoardmq_drain_") || rollFailureMetric(name) || rollCopyMetric(name) || rollEmptyMetric(name)
}

// rollFailureMetric is a family a step's failures sum.
func rollFailureMetric(name string) bool {
	return name == "hoardmq_forward_failures_total" || name == "hoardmq_replication_barrier_failures_total"
}

// rollEmptyMetric is the family a step's served_empty sums.
func rollEmptyMetric(name string) bool { return name == "hoardmq_sessions_rebuilt_empty_total" }

// sumLost sums a step's served-empty counts but those of sessions that had
// no expiry.
func sumLost(empty map[string]float64) float64 {
	n := 0.0
	for k, v := range empty {
		if !strings.Contains(k, `reason="`+noExpiryReason+`"`) {
			n += v
		}
	}
	return n
}

// rollCopyMetric is a family a step's copies sum: replica copies dropped and
// kept by reason, and the checks of an owner's targets by outcome.
func rollCopyMetric(name string) bool {
	return name == "hoardmq_replicas_dropped_total" || name == "hoardmq_replicas_kept_total" ||
		name == "hoardmq_replicas_unwritten_checks_total"
}

func pickRollMetrics(snapshot []module.Metric) map[string]float64 {
	out := map[string]float64{}
	for _, m := range snapshot {
		if rollDrainMetric(m.Name) && !math.IsNaN(m.Value) && !math.IsInf(m.Value, 0) {
			out[metricKey(m)] += m.Value
		}
	}
	return out
}

// risen is what rose from before to after, by series, for series whose name
// passes keep.
func risen(before, after map[string]float64, keep func(string) bool) map[string]float64 {
	out := map[string]float64{}
	for k, v := range after {
		name, _, _ := strings.Cut(k, "{")
		if !keep(name) {
			continue
		}
		if d := v - before[k]; d > 0 {
			out[k] = d
		}
	}
	return out
}

// rollReport is report.md.
func rollReport(r rollRecord) string {
	var b strings.Builder
	p := func(format string, a ...any) { fmt.Fprintf(&b, format, a...) }
	p("# Rolling restart — %d clients\n\n", r.Clients)
	cut := fmt.Sprintf("Drains are not cut at the chart's %.0f s grace", r.ChartGraceS)
	if r.CutS > 0 {
		cut = fmt.Sprintf("A drain still running %d s after its start is cut", r.CutS)
	}
	p("%s timings; order %s; drain rate %d a second; roll %.1f s; hold %d s after it. %s.\n\n",
		r.TimingSet, strings.Join(r.Order, ", "), r.DrainRate, r.RollSec, r.HoldSec, cut)
	p("| Step | Drain s | Phases (s from the drain's start) | Sessions confirmed / unconfirmed | Map changes: version at s (moved copies peak, last kept s after) | Ready s after restart | Probe refused / attempts (groups) |\n")
	p("|---|---|---|---|---|---|---|\n")
	for _, s := range r.Steps {
		var phases []string
		for _, ph := range s.Phases {
			phases = append(phases, fmt.Sprintf("%s %.1f", ph.Phase, ph.AtSec-s.DrainStartSec))
		}
		var changes []string
		for _, c := range s.Changes {
			changes = append(changes, fmt.Sprintf("v%d at %.1f (%d, %.1f)", c.Version, c.AtSec-s.DrainStartSec, c.KeptPeak, c.ClearSec))
		}
		p("| %s | %.1f | %s | %d / %d | %s | %.1f | %d / %d (%d / %d) |\n", s.Node, s.DrainSec,
			strings.Join(phases, "; "), s.Status.SessionsConfirmed, s.Status.SessionsUnconfirmed, strings.Join(changes, "; "),
			s.ReadySec-s.RestartSec,
			sumCounts(s.Probe.Refused)+s.Probe.Failed, s.Probe.Attempts, sumCounts(s.Probe.GroupsRefused)+s.Probe.GroupsFailed, s.Probe.Groups)
	}
	p("\nAcross the roll: %.2f%% of probe publishes refused or failed (the chart runbook's worst: %.2f%%).\n\n", 100*r.Refusals.Rate, 100*r.ReferenceRate)
	p("Population at the end: %d of %d online; sessions rebuilt empty %d.\n\n", r.Population.OnlineAtEnd, r.Population.Target, r.Population.SessionLost)
	p("Probe: %s\n\n", probeLine(r.Probe))
	if r.Retained != nil {
		p("%s\n\n", retainedLine(r.Retained))
	}
	if len(r.Findings) == 0 {
		p("Findings: none.\n")
	} else {
		p("Findings:\n\n")
		for _, f := range r.Findings {
			p("- %s\n", f)
		}
	}
	return b.String()
}

// rollClients is the measured run's client count, or the gate's.
func rollClients() (int, bool, error) {
	v := os.Getenv(rollEnv)
	if v == "" {
		return rollGateClients, false, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return 0, false, fmt.Errorf("%s=%q: want a positive client count", rollEnv, v)
	}
	return n, true, nil
}
