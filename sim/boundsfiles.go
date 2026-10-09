package sim

import (
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// JudgeRunFiles judges a set of failover runs from their run.json files
// against the pass bounds and writes the per-run verdicts, the set's verdict
// and the WI 1625 rider as Markdown to w. Each argument is a run.json or a
// directory searched for them. It returns 0 when the set passes, 1 when it
// fails and 2 when a file cannot be read or no run was found: an empty set is
// never a pass. sim/cmd/failoverbounds is its command line.
func JudgeRunFiles(w io.Writer, args []string) int {
	runs, err := readRunFiles(args)
	if err != nil {
		_, _ = fmt.Fprintln(w, "failoverbounds:", err)
		return 2
	}
	named := make([]namedRun, len(runs))
	for i, r := range runs {
		named[i] = namedRun{Name: r.name, In: r.in}
	}
	b := judgeBatch(named)
	writeBatch(w, b, runs)
	if b.Verdict == boundFail {
		return 1
	}
	return 0
}

// runFile is one run.json as the command reads it: the bounds' figures and
// what the rider needs.
type runFile struct {
	name  string
	in    boundsInput
	rider riderInput
}

type riderInput struct {
	Victim           string                        `json:"victim"`
	MetricsBefore    map[string]map[string]float64 `json:"metrics_before"`
	MetricsAfter     map[string]map[string]float64 `json:"metrics_after"`
	MetricsAtInstant map[string]map[string]float64 `json:"metrics_at_instant"`
	Timing           *instantRecord                `json:"metrics_at_instant_timing"`
	DropTimeline     *dropTimeline                 `json:"drop_timeline"`
}

func readRunFiles(args []string) ([]runFile, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("give run.json files or directories holding them")
	}
	var out []runFile
	for _, arg := range args {
		paths, base, err := runJSONPaths(arg)
		if err != nil {
			return nil, err
		}
		for _, p := range paths {
			raw, err := os.ReadFile(p) //nolint:gosec // operator-chosen path
			if err != nil {
				return nil, err
			}
			r := runFile{name: runName(base, p)}
			if err := json.Unmarshal(raw, &r.in); err != nil {
				return nil, fmt.Errorf("%s: %w", p, err)
			}
			if err := json.Unmarshal(raw, &r.rider); err != nil {
				return nil, fmt.Errorf("%s: %w", p, err)
			}
			out = append(out, r)
		}
	}
	return out, nil
}

// runJSONPaths is the run.json files an argument names, sorted, and the
// directory their names are given relative to. A directory with none is an
// error.
func runJSONPaths(arg string) ([]string, string, error) {
	info, err := os.Stat(arg)
	if err != nil {
		return nil, "", err
	}
	if !info.IsDir() {
		return []string{arg}, filepath.Dir(filepath.Dir(arg)), nil
	}
	var paths []string
	err = filepath.WalkDir(arg, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && d.Name() == "run.json" {
			paths = append(paths, p)
		}
		return nil
	})
	if err != nil {
		return nil, "", err
	}
	if len(paths) == 0 {
		return nil, "", fmt.Errorf("%s holds no run.json", arg)
	}
	sort.Strings(paths)
	return paths, arg, nil
}

// runName is the run's directory relative to the argument it was found under:
// "r1" for recovered-2026-09-22/r1/run.json.
func runName(base, path string) string {
	rel, err := filepath.Rel(base, filepath.Dir(path))
	if err != nil || rel == "." {
		return filepath.Base(filepath.Dir(path))
	}
	return filepath.ToSlash(rel)
}

func writeBatch(w io.Writer, b batchVerdict, runs []runFile) {
	p := func(format string, a ...any) { _, _ = fmt.Fprintf(w, format, a...) }
	p("# Pass bounds over %d runs\n\n", len(runs))
	p("Bounds ratified by the owner on 2026-09-22, applied by WI 1734. The time bound judges only %s.\n\n", boundsConfiguration())
	p("## The set\n\n%s\n\n| Figure | Bound | The set | Verdict | Note |\n|---|---|---|---|---|\n", boundsVerdictLine(b.boundsVerdict))
	for _, r := range b.Rows {
		p("| %s | %s | %s | %s | %s |\n", r.Name, r.Bound, r.Figure, r.Status, r.Note)
	}
	p("\n## Each run\n\n| Run | Verdict | %s | %s | %s | %s | %s | Placement |\n|---|---|---|---|---|---|---|---|\n",
		rowRecovery, rowSessionLost, rowDowngraded, rowUndelivered, rowRefusals)
	for i, v := range b.Runs {
		cell := func(name string) string {
			r := rowOf(v.boundsVerdict, name)
			if r.Status == boundFail || r.Status == boundNotJudged {
				return r.Figure + " — **" + r.Status + "**"
			}
			return r.Figure
		}
		placement := runs[i].in.placement()
		if placement == "" {
			placement = "unknown"
		}
		p("| %s | %s | %s | %s | %s | %s | %s | %s |\n", v.Name, strings.ToUpper(v.Verdict), cell(rowRecovery),
			cell(rowSessionLost), cell(rowDowngraded), cell(rowUndelivered), cell(rowRefusals), placement)
	}
	for _, v := range b.Runs {
		for _, f := range v.Failures {
			p("\n- %s fails %s", v.Name, f)
		}
	}
	p("\n\n## WI 1625's forget against the recovery instant\n\n")
	p("From `hoardmq_route_owners_forgotten_total` in the snapshot taken when the confirming hold began and at window end. " +
		"The counter is read at the end of the snapshot, so *before* means no later than the instant plus the time the " +
		"snapshot's read ended after it.\n\n| Run | Survivor | Forget | Read began after the instant | Read took |\n|---|---|---|---|---|\n")
	for _, r := range runs {
		if r.rider.Timing == nil || r.rider.Timing.Absent != "" || r.rider.MetricsAtInstant == nil {
			why := "no snapshot at the instant in this run.json"
			if r.rider.Timing != nil && r.rider.Timing.Absent != "" {
				why = r.rider.Timing.Absent
			}
			p("| %s | — | not read — %s | — | — |\n", r.name, why)
			continue
		}
		for _, node := range sortedNames(r.rider.MetricsAfter) {
			inst, haveInst := r.rider.MetricsAtInstant[node]
			order := forgetOrder(inst, r.rider.MetricsAfter[node], haveInst, true)
			t := r.rider.Timing.Nodes[node]
			p("| %s | %s | %s | %.3f s | %.3f s |\n", r.name, node, order, t.BeganAfterInstantSec, t.ReadSec)
		}
	}
	writeDropSection(w, runs)
	writeServedSection(w, runs)
	writeRefusalSourceSection(w, runs)
	writeGroupsSection(w, runs)
}

// writeGroupsSection prints what the probe's shared groups saw in each run
// (WI 1962), and what the survivors' routing planes did with group messages:
// metrics_after less metrics_before, which is read before the probe starts,
// summed over the survivors. Report-only: the groups' losses and refusals are
// already in the gating rows.
func writeGroupsSection(w io.Writer, runs []runFile) {
	p := func(format string, a ...any) { _, _ = fmt.Fprintf(w, format, a...) }
	p("\n\n## Shared groups\n\n")
	p("The probe's shared groups: their publishes, what was delivered to no member, QoS 2 messages received as more than " +
		"one exchange, QoS 1 repeats, deliveries by member node, and the survivors' group forwards and relayed gives from " +
		"just before the probe started to window end. Report-only.\n\n")
	p("| Run | Groups | Attempts | Acknowledged | Delivered to no member | Refused (after the instant) | QoS 2 duplicates | QoS 1 repeats (second member) | Delivered by node | Survivors: decides, gives, relays held / refused |\n")
	p("|---|---|---|---|---|---|---|---|---|---|\n")
	for _, r := range runs {
		var g *groupsRecord
		if r.in.Probe != nil {
			g = r.in.Probe.Groups
		}
		if g == nil {
			p("| %s | no groups in this run | — | — | — | — | — | — | — | — |\n", r.name)
			continue
		}
		q1, q2 := 0, 0
		for _, s := range g.Start {
			if s.QoS == 2 {
				q2++
			} else {
				q1++
			}
		}
		storm := "—"
		if r.rider.MetricsBefore != nil && r.rider.MetricsAfter != nil {
			rose := func(key string) float64 {
				var n float64
				for node, after := range r.rider.MetricsAfter {
					if d := after[key] - r.rider.MetricsBefore[node][key]; d > 0 {
						n += d
					}
				}
				return n
			}
			storm = fmt.Sprintf("%.0f, %.0f, %.0f / %.0f",
				rose(`hoardmq_forwards_total{kind="group-decide"}`), rose(`hoardmq_forwards_total{kind="group-give"}`),
				rose(`hoardmq_group_gives_relayed_total{outcome="held"}`), rose(`hoardmq_group_gives_relayed_total{outcome="refused"}`))
		}
		nodes := make([]string, 0, len(g.DeliveredByNode))
		for n := range g.DeliveredByNode {
			nodes = append(nodes, n)
		}
		sort.Strings(nodes)
		byNode := ""
		for i, n := range nodes {
			if i > 0 {
				byNode += ", "
			}
			byNode += fmt.Sprintf("%s %d", n, g.DeliveredByNode[n])
		}
		p("| %s | %d (QoS 1: %d, QoS 2: %d) | %d | %d | %d | %d (%d) | %d | %d (%d) | %s | %s |\n",
			r.name, g.Groups, q1, q2, g.Attempts, g.Acked, g.AckedNotDelivered, sumCounts(g.Refused),
			sumCounts(g.RefusedAfterRecovery), g.QoS2Duplicates, g.QoS1Repeated, g.QoS1SecondMember, orDash(byNode), storm)
	}
	for _, r := range runs {
		if r.in.Probe == nil || r.in.Probe.Groups == nil {
			continue
		}
		g := r.in.Probe.Groups
		for _, e := range g.Examples {
			p("\n- %s: delivered to no member: %s", r.name, e)
		}
		for _, e := range g.DuplicateExamples {
			p("\n- %s: QoS 2 duplicate: %s", r.name, e)
		}
	}
	p("\n")
}
