package sim

import (
	"fmt"
	"sort"
	"strings"
)

// The failover measurement's pass bounds, ratified by the owner on 2026-09-22
// from WI 1618's batch (project.md Decisions) and applied by WI 1734.
//
// A run is judged by judgeRun and a set of runs by judgeBatch. Both read a
// boundsInput, which is decoded from run.json with the keys the artifact
// writes, so a run, its repeat summary and a later reading of a result folder
// judge the same figures the same way.
const (
	// boundRecoverySec is the time bound: the worst run of the first batch
	// under the definition (35.65 s) plus about half again. Setting it is the
	// owner's.
	boundRecoverySec = 55.0

	// The configuration the time bound was measured on. A run outside it
	// reports the figure without judging it.
	boundsClients    = 100_000
	boundsHost       = "ai2"
	boundsTimingSet  = "product"
	boundsClientMode = "in-process"
	boundsHoldSec    = 60

	// boundsBalanceNeed is how many runs of each placement a set needs before
	// it can pass the time bound: the bound came from a batch of three and
	// four, and it is the balancing driver's default NEED.
	boundsBalanceNeed = 3
)

// A row's status.
const (
	boundPass      = "pass"
	boundFail      = "fail"
	boundNotJudged = "not judged"
	boundRecorded  = "recorded"
)

// The rows, in the order a report prints them.
const (
	rowRecovery    = "time to every client recovered"
	rowDowngraded  = "subscriptions granted below the QoS asked for"
	rowUndelivered = "probe messages acknowledged and never delivered"
	rowSessionLost = "sessions rebuilt empty on a reconnect"
	rowRefusals    = "QoS 1 refusals after the recovery instant"
	rowNinetyNine  = "time to 99% recovered"
	rowNineNine9   = "time to 99.9% recovered"
)

// boundsInput is the part of run.json the bounds read. Its keys are the
// artifact's own, and TestBoundsInputDecodesFromTheArtifact holds them to it.
type boundsInput struct {
	Clients            int          `json:"clients"`
	RecoverySec        float64      `json:"recovery_sec"`
	RecoveryReason     string       `json:"recovery_reason"`
	NinetyNineSec      float64      `json:"ninety_nine_sec"`
	NineNineNineSec    float64      `json:"ninety_nine_nine_sec"`
	SubDowngraded      int64        `json:"sub_downgraded"`
	SessionLost        int64        `json:"session_lost"`
	SessionLostClients int          `json:"session_lost_clients"`
	Probe              *probeRecord `json:"probe"`
	Hold               *holdRecord  `json:"hold"`
	ClientMode         string       `json:"client_mode"`
	StormTarget        string       `json:"storm_target"`
	LeaderAfter        string       `json:"leader_after"`
	Provenance         struct {
		Host      string           `json:"host"`
		Effective *boundsEffective `json:"effective"`
	} `json:"provenance"`
}

// boundsEffective is the part of the effective timings the bounds read.
// TakeoverTimeoutMs is nil in a record written before WI 1805.
type boundsEffective struct {
	TimingSet         string `json:"timing_set"`
	TakeoverTimeoutMs *int   `json:"takeover_timeout_ms"`
}

// preProductTimeouts says a run is labelled product but was recorded before
// WI 1805, when every run's takeover and ack timeouts were the harness's
// 1 s whatever the label. It is a note; it decides nothing.
func (in boundsInput) preProductTimeouts() bool {
	e := in.Provenance.Effective
	return e != nil && e.TimingSet == boundsTimingSet && e.TakeoverTimeoutMs == nil
}

// preProductTimeoutsNote is the note such a run carries.
const preProductTimeoutsNote = "recorded before WI 1805: its takeover and ack timeouts were the harness's 1 s, not the product's 5 s and 2 s"

// boundRow is one figure against its bound.
type boundRow struct {
	Name   string `json:"name"`
	Bound  string `json:"bound"`
	Figure string `json:"figure"`
	Status string `json:"status"`
	Note   string `json:"note,omitempty"`
}

// boundsVerdict is a run's or a set's judgement: fail when any gating row
// failed, and each failure named with its bound and its figure.
type boundsVerdict struct {
	Verdict  string     `json:"verdict"`
	Failures []string   `json:"failures,omitempty"`
	Rows     []boundRow `json:"rows"`
}

// placement is where the crashed node's clients land first relative to the
// new leader, as sim/results/analysis/fetcharms.py computes it: "target led"
// when the node they retry on first won the election, "other led" when the
// other survivor did, "" when either name is missing.
func (in boundsInput) placement() string {
	switch {
	case in.StormTarget == "" || in.LeaderAfter == "":
		return ""
	case in.StormTarget == in.LeaderAfter:
		return "target led"
	default:
		return "other led"
	}
}

// outOfConfiguration names every way the run differs from the configuration
// the time bound was measured on; empty means the bound judges it.
func (in boundsInput) outOfConfiguration() []string {
	var why []string
	if in.Clients != boundsClients {
		why = append(why, fmt.Sprintf("%d clients, not %d", in.Clients, boundsClients))
	}
	if in.Provenance.Host != boundsHost {
		why = append(why, fmt.Sprintf("host %q, not %s", in.Provenance.Host, boundsHost))
	}
	timings := ""
	if in.Provenance.Effective != nil {
		timings = in.Provenance.Effective.TimingSet
	}
	if timings != boundsTimingSet {
		why = append(why, fmt.Sprintf("timings %q, not %s", timings, boundsTimingSet))
	}
	if in.ClientMode != boundsClientMode {
		why = append(why, fmt.Sprintf("clients %q, not %s", in.ClientMode, boundsClientMode))
	}
	switch {
	case in.Hold == nil:
		why = append(why, "no hold recorded")
	case in.Hold.Seconds != boundsHoldSec:
		why = append(why, fmt.Sprintf("hold %d s, not %d s", in.Hold.Seconds, boundsHoldSec))
	}
	if in.placement() == "" {
		why = append(why, "placement unknown")
	}
	return why
}

// judgeRun judges one run from its figures alone.
func judgeRun(in boundsInput) boundsVerdict {
	rows := []boundRow{
		recoveryRow(in),
		zeroRow(rowDowngraded, in.SubDowngraded, fmt.Sprintf("%d", in.SubDowngraded)),
		undeliveredRow(in.Probe),
		zeroRow(rowSessionLost, in.SessionLost, fmt.Sprintf("%d across %d %s", in.SessionLost, in.SessionLostClients, clientsWord(in.SessionLostClients))),
		refusalsRow(in.Probe),
		recordedRow(rowNinetyNine, in.NinetyNineSec),
		recordedRow(rowNineNine9, in.NineNineNineSec),
	}
	return verdictOf(rows)
}

func verdictOf(rows []boundRow) boundsVerdict {
	v := boundsVerdict{Verdict: boundPass, Rows: rows}
	for _, r := range rows {
		if r.Status == boundFail {
			v.Verdict = boundFail
			v.Failures = append(v.Failures, fmt.Sprintf("%s: %s, bound %s", r.Name, r.Figure, r.Bound))
		}
	}
	return v
}

func recoveryRow(in boundsInput) boundRow {
	r := boundRow{Name: rowRecovery, Bound: fmt.Sprintf("≤ %.0f s", boundRecoverySec)}
	reached := in.RecoverySec >= 0
	if reached {
		r.Figure = fmt.Sprintf("%.3f s", in.RecoverySec)
	} else {
		r.Figure = "not reached"
		if in.RecoveryReason != "" {
			r.Figure += ": " + in.RecoveryReason
		}
	}
	if why := in.outOfConfiguration(); len(why) > 0 {
		r.Status = boundNotJudged
		r.Note = "the bound holds only for " + boundsConfiguration() + "; this run: " + strings.Join(why, ", ")
		return r
	}
	r.Note = in.placement()
	if in.preProductTimeouts() {
		r.Note += "; " + preProductTimeoutsNote
	}
	if reached && in.RecoverySec <= boundRecoverySec {
		r.Status = boundPass
	} else {
		r.Status = boundFail
	}
	return r
}

// boundsConfiguration is the configuration as a report states it.
func boundsConfiguration() string {
	return fmt.Sprintf("%d clients on %s, %s timings, %s clients, a %d s hold, placement known",
		boundsClients, boundsHost, boundsTimingSet, boundsClientMode, boundsHoldSec)
}

func zeroRow(name string, n int64, figure string) boundRow {
	r := boundRow{Name: name, Bound: "0", Figure: figure, Status: boundPass}
	if n != 0 {
		r.Status = boundFail
	}
	return r
}

// undeliveredRow gates on the probe: a probe that did not run, or published
// nothing, has no figure, and a gating row with no figure does not pass.
func undeliveredRow(p *probeRecord) boundRow {
	r := boundRow{Name: rowUndelivered, Bound: "0"}
	switch {
	case p == nil:
		r.Figure, r.Status, r.Note = "no figure", boundFail, "the probe did not run"
	case p.Attempts == 0:
		r.Figure, r.Status, r.Note = "no figure", boundFail, "the probe published nothing"
	default:
		n, of := int64(p.AckedNotDelivered), p.Acked
		figure := ""
		if g := p.Groups; g != nil {
			n += int64(g.AckedNotDelivered)
			of += g.Acked
			figure = fmt.Sprintf(" (pairs %d of %d, groups %d of %d)", p.AckedNotDelivered, p.Acked, g.AckedNotDelivered, g.Acked)
		}
		r = zeroRow(rowUndelivered, n, fmt.Sprintf("%d of %d acknowledged", n, of)+figure)
	}
	return r
}

// refusalsRow gates on the probe's refusals after the instant: the owner
// decided the row at 0 on 2026-09-22, gating once WI 1706 closed. As for the
// acknowledged row, a probe that did not run or published nothing has no
// figure, and a gating row with no figure does not pass.
func refusalsRow(p *probeRecord) boundRow {
	r := boundRow{Name: rowRefusals, Bound: "0"}
	switch {
	case p == nil:
		r.Figure, r.Status, r.Note = "no figure", boundFail, "the probe did not run"
		return r
	case p.Attempts == 0:
		r.Figure, r.Status, r.Note = "no figure", boundFail, "the probe published nothing"
		return r
	}
	pairs, errs := sumCounts(p.RefusedAfterRecovery), p.ErrorsAfterRecovery
	n, figure := pairs, ""
	if g := p.Groups; g != nil {
		groups := sumCounts(g.RefusedAfterRecovery)
		n += groups
		errs += g.ErrorsAfterRecovery
		figure = fmt.Sprintf(" (pairs %d, groups %d)", pairs, groups)
	}
	r = zeroRow(rowRefusals, n, fmt.Sprintf("%d", n)+figure)
	if errs > 0 {
		r.Figure += fmt.Sprintf(" (and %d with no acknowledgement)", errs)
	}
	return r
}

func sumCounts(m map[string]int64) int64 {
	var n int64
	for _, v := range m {
		n += v
	}
	return n
}

func recordedRow(name string, sec float64) boundRow {
	r := boundRow{Name: name, Bound: "none", Status: boundRecorded, Note: "recorded, never gating"}
	if sec < 0 {
		r.Figure = "not reached"
	} else {
		r.Figure = fmt.Sprintf("%.3f s", sec)
	}
	return r
}

// namedRun is one run of a set, with the name a report calls it by.
type namedRun struct {
	Name string
	In   boundsInput
}

// batchVerdict is a set's judgement, with each run's beside it.
type batchVerdict struct {
	boundsVerdict
	Runs []namedVerdict `json:"runs"`
	// Placement counts the runs the time row judged, by placement.
	Placement map[string]int `json:"placement"`
}

type namedVerdict struct {
	Name string `json:"name"`
	boundsVerdict
}

// judgeBatch judges a set of runs. A gating row fails when any run fails it,
// the failing runs named. The time row passes only when every run it judged
// passed and those runs are balanced over placement: a run beyond the bound
// fails it whatever the mix, but a pass read from one placement is a claim
// about a distribution the bound was not measured on.
func judgeBatch(runs []namedRun) batchVerdict {
	b := batchVerdict{Placement: map[string]int{}}
	per := make([]boundsVerdict, len(runs))
	for i, r := range runs {
		per[i] = judgeRun(r.In)
		b.Runs = append(b.Runs, namedVerdict{Name: r.Name, boundsVerdict: per[i]})
	}
	rows := []boundRow{
		batchRecoveryRow(runs, per, b.Placement),
		batchGatingRow(rowDowngraded, runs, per),
		batchGatingRow(rowUndelivered, runs, per),
		batchGatingRow(rowSessionLost, runs, per),
		batchGatingRow(rowRefusals, runs, per),
		batchRangeRow(rowNinetyNine, runs, per),
		batchRangeRow(rowNineNine9, runs, per),
	}
	b.boundsVerdict = verdictOf(rows)
	return b
}

// rowOf finds a run's row by name.
func rowOf(v boundsVerdict, name string) boundRow {
	for _, r := range v.Rows {
		if r.Name == name {
			return r
		}
	}
	return boundRow{Name: name}
}

func batchRecoveryRow(runs []namedRun, per []boundsVerdict, placement map[string]int) boundRow {
	r := boundRow{Name: rowRecovery, Bound: fmt.Sprintf("≤ %.0f s", boundRecoverySec)}
	var failed, passed []string
	lo, hi := 0.0, 0.0
	notJudged := 0
	for i, run := range runs {
		row := rowOf(per[i], rowRecovery)
		switch row.Status {
		case boundNotJudged:
			notJudged++
			continue
		case boundFail:
			failed = append(failed, fmt.Sprintf("%s %s", run.Name, row.Figure))
		default:
			passed = append(passed, run.Name)
			if len(passed) == 1 || run.In.RecoverySec < lo {
				lo = run.In.RecoverySec
			}
			if run.In.RecoverySec > hi {
				hi = run.In.RecoverySec
			}
		}
		placement[run.In.placement()]++
	}
	judged := len(failed) + len(passed)
	outside := ""
	if notJudged > 0 {
		outside = fmt.Sprintf("; %d of %d runs outside the configuration, not judged", notJudged, len(runs))
	}
	early := 0
	for _, run := range runs {
		if run.In.preProductTimeouts() {
			early++
		}
	}
	if early > 0 {
		outside += fmt.Sprintf("; %d of %d runs %s", early, len(runs), preProductTimeoutsNote)
	}
	switch {
	case len(failed) > 0:
		r.Status, r.Figure = boundFail, strings.Join(failed, ", ")
		r.Note = fmt.Sprintf("%d of %d judged runs over the bound%s", len(failed), judged, outside)
	case judged == 0:
		r.Status, r.Figure = boundNotJudged, "—"
		r.Note = "no run in the configuration the bound holds for (" + boundsConfiguration() + ")"
	default:
		r.Figure = fmt.Sprintf("%.2f–%.2f s over %d runs", lo, hi, judged)
		if placement["target led"] < boundsBalanceNeed || placement["other led"] < boundsBalanceNeed {
			r.Status = boundNotJudged
			r.Note = fmt.Sprintf("placement not balanced: target led %d, other led %d; a pass needs %d of each%s",
				placement["target led"], placement["other led"], boundsBalanceNeed, outside)
		} else {
			r.Status = boundPass
			r.Note = fmt.Sprintf("target led %d, other led %d%s", placement["target led"], placement["other led"], outside)
		}
	}
	return r
}

func batchGatingRow(name string, runs []namedRun, per []boundsVerdict) boundRow {
	r := boundRow{Name: name, Bound: "0", Status: boundPass}
	var failed []string
	for i, run := range runs {
		row := rowOf(per[i], name)
		if row.Status == boundFail {
			failed = append(failed, fmt.Sprintf("%s %s", run.Name, row.Figure))
		}
	}
	if len(failed) > 0 {
		r.Status = boundFail
		r.Figure = strings.Join(failed, "; ")
		r.Note = fmt.Sprintf("%d of %d runs fail", len(failed), len(runs))
		return r
	}
	r.Figure = fmt.Sprintf("0 in all %d runs", len(runs))
	return r
}

// batchRangeRow gives the figures of a row that decides nothing, run by run,
// with the status and note the runs carry.
func batchRangeRow(name string, runs []namedRun, per []boundsVerdict) boundRow {
	r := boundRow{Name: name}
	var figs []string
	for i, run := range runs {
		row := rowOf(per[i], name)
		r.Bound, r.Status, r.Note = row.Bound, row.Status, row.Note
		figs = append(figs, run.Name+" "+row.Figure)
	}
	r.Figure = strings.Join(figs, ", ")
	return r
}

// boundsTable renders a verdict as the report's Markdown table.
func boundsTable(v boundsVerdict) string {
	var b strings.Builder
	b.WriteString(boundsVerdictLine(v) + "\n\n| Figure | Bound | This run | Verdict | Note |\n|---|---|---|---|---|\n")
	for _, r := range v.Rows {
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %s |\n", r.Name, r.Bound, r.Figure, r.Status, r.Note)
	}
	return b.String()
}

// boundsSection is the report's section on the bounds.
func boundsSection(v boundsVerdict) string {
	return "\n## Pass bounds\n\nRatified by the owner on 2026-09-22 and applied by WI 1734. A bound judges a run only " +
		"where the table says so, and a recorded row never gates.\n\n" +
		boundsTable(v)
}

// boundsVerdictLine is the one-line verdict: the failures, each with its bound
// and figure, or a pass that says which rows it did not judge.
func boundsVerdictLine(v boundsVerdict) string {
	if v.Verdict == boundFail {
		return "**FAIL** — " + strings.Join(v.Failures, "; ")
	}
	var unjudged []string
	for _, r := range v.Rows {
		if r.Status == boundNotJudged {
			unjudged = append(unjudged, r.Name+" not judged ("+r.Note+")")
		}
	}
	if len(unjudged) == 0 {
		return "**PASS** on every gating bound"
	}
	return "**PASS** on every gating bound it judged; " + strings.Join(unjudged, "; ")
}

// sortedNames keeps a map's rendering stable.
func sortedNames[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func clientsWord(n int) string {
	if n == 1 {
		return "client"
	}
	return "clients"
}
