package sim

import (
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/happydave/hoardmq/internal/broker"
	"github.com/happydave/hoardmq/internal/cluster/replication"
	"github.com/happydave/hoardmq/internal/cluster/routing"
)

// dropTimeline is each step from a node crash to the shard map dropping it,
// and each survivor's forget of its routes, in seconds after the harness's
// crash mark. Every node runs in this process, so its stamps share the
// mark's clock. A step that could not be read is named in Missing and left
// out, never written as zero.
type dropTimeline struct {
	Victim string `json:"victim"`
	Leader string `json:"leader,omitempty"`
	// Survivors are the nodes whose stats were read.
	Survivors []string `json:"survivors"`
	// PeerDownSec is when each survivor's transport lost the victim.
	PeerDownSec map[string]float64 `json:"peer_down_sec,omitempty"`
	// LastRenewalSec is when the post-crash leader last applied an alive
	// record from the victim: negative, the renewal that the ttl runs from.
	LastRenewalSec         *float64           `json:"last_renewal_sec,omitempty"`
	DeclarationProposedSec *float64           `json:"declaration_proposed_sec,omitempty"`
	DeclarationAppliedSec  map[string]float64 `json:"declaration_applied_sec,omitempty"`
	// MapVersion is the first map after the crash whose basis leaves the
	// victim out; MapCause why the proposer's driver checked, and
	// MapDecidedSec when it computed the map.
	MapVersion       uint64             `json:"map_version,omitempty"`
	MapCause         string             `json:"map_cause,omitempty"`
	MapDecidedSec    *float64           `json:"map_decided_sec,omitempty"`
	MapAppliedSec    map[string]float64 `json:"map_applied_sec,omitempty"`
	MapReconciledSec map[string]float64 `json:"map_reconciled_sec,omitempty"`
	// AdoptedSec is, per survivor, the first served check that found its
	// own adoptions up to the victim's emptied index finished; ProposedSec
	// when its served fact covering that index was committed (WI 1633).
	AdoptedSec  map[string]float64 `json:"adopted_sec,omitempty"`
	ProposedSec map[string]float64 `json:"proposed_sec,omitempty"`
	// CoverSec is, per survivor that took sessions in transit, when its
	// cover was last committed before the forget. A survivor that
	// took none has no cover, and is not missing.
	CoverSec  map[string]float64 `json:"cover_sec,omitempty"`
	ForgetSec map[string]float64 `json:"forget_sec,omitempty"`
	// ForgetLastHeld is, per survivor, the reason that kept the victim's
	// routes last before they were forgotten.
	ForgetLastHeld map[string]string `json:"forget_last_held,omitempty"`
	InstantSec     *float64          `json:"instant_sec,omitempty"`
	// DegradedSec is, per survivor, the seconds between the crash mark and
	// the read in which it lacked an owner's routes (WI 1571).
	DegradedSec map[string]float64 `json:"degraded_sec,omitempty"`
	Missing     []string           `json:"missing,omitempty"`
}

// newDropTimeline reads the timeline from the survivors' stats, read at
// read. leader is the post-crash leader; instant counts only when reached.
func newDropTimeline(crash, read time.Time, victim, leader string, stats map[string]broker.StormStats, instant time.Time, reached bool) dropTimeline {
	d := dropTimeline{Victim: victim, Leader: leader}
	since := func(ns int64) float64 { return time.Unix(0, ns).Sub(crash).Seconds() }
	after := func(ns int64) bool { return ns != 0 && !time.Unix(0, ns).Before(crash) }
	missing := func(format string, a ...any) { d.Missing = append(d.Missing, fmt.Sprintf(format, a...)) }
	put := func(m *map[string]float64, node string, v float64) {
		if *m == nil {
			*m = map[string]float64{}
		}
		(*m)[node] = v
	}
	survivors := sortedNames(stats)
	d.Survivors = survivors

	for _, n := range survivors {
		found := false
		for _, p := range stats[n].Peers {
			if p.Name == victim && p.DownForMs > 0 {
				put(&d.PeerDownSec, n, read.Add(-time.Duration(p.DownForMs*float64(time.Millisecond))).Sub(crash).Seconds())
				found = true
			}
		}
		if !found {
			missing("%s: no down link to %s", n, victim)
		}
	}

	if ls, ok := stats[leader]; leader == "" || !ok || ls.Lease == nil {
		missing("the post-crash leader's lease timeline")
	} else {
		tl := ls.Lease.Timeline[victim]
		if tl.LastAliveAppliedNs != 0 {
			v := since(tl.LastAliveAppliedNs)
			d.LastRenewalSec = &v
		} else {
			missing("%s: no alive record from %s", leader, victim)
		}
		if after(tl.DeclarationProposedNs) {
			v := since(tl.DeclarationProposedNs)
			d.DeclarationProposedSec = &v
		} else {
			missing("%s: no declaration of %s proposed after the crash", leader, victim)
		}
	}
	for _, n := range survivors {
		if ls := stats[n].Lease; ls != nil && after(ls.Timeline[victim].DeclarationAppliedNs) {
			put(&d.DeclarationAppliedSec, n, since(ls.Timeline[victim].DeclarationAppliedNs))
		} else {
			missing("%s: no declaration of %s applied after the crash", n, victim)
		}
	}

	for _, n := range survivors {
		if mt := stats[n].ReplicationMaps; mt != nil {
			for _, e := range mt.Events {
				if after(e.AppliedNs) && len(e.Basis) > 0 && !containsName(e.Basis, victim) && (d.MapVersion == 0 || e.Version < d.MapVersion) {
					d.MapVersion = e.Version
				}
			}
		}
	}
	if d.MapVersion == 0 {
		missing("no map leaving %s out applied after the crash", victim)
	} else {
		for _, n := range survivors {
			var e replication.MapEvent
			if mt := stats[n].ReplicationMaps; mt != nil {
				for _, x := range mt.Events {
					if x.Version == d.MapVersion {
						e = x
					}
				}
			}
			if e.AppliedNs == 0 {
				missing("%s: map %d not applied", n, d.MapVersion)
				continue
			}
			put(&d.MapAppliedSec, n, since(e.AppliedNs))
			if e.ReconciledNs != 0 {
				put(&d.MapReconciledSec, n, since(e.ReconciledNs))
			} else {
				missing("%s: map %d not reconciled", n, d.MapVersion)
			}
			if e.Cause != "" && e.DecidedNs != 0 {
				d.MapCause = e.Cause
				v := since(e.DecidedNs)
				d.MapDecidedSec = &v
			}
		}
		if d.MapDecidedSec == nil {
			missing("no survivor recorded proposing map %d", d.MapVersion)
		}
	}

	for _, n := range survivors {
		var adopted, proposed int64
		for _, e := range stats[n].RoutingForgets {
			if e.Owner == victim && after(e.ServedHereNs) {
				adopted, proposed = e.ServedHereNs, e.ProposedNs
			}
		}
		if adopted == 0 {
			missing("%s: its adoptions not seen finished for %s", n, victim)
			continue
		}
		put(&d.AdoptedSec, n, since(adopted))
		for _, e := range stats[n].RoutingForgets {
			if e.Owner == victim && after(e.CoveredNs) {
				put(&d.CoverSec, n, since(e.CoveredNs))
			}
		}
		if after(proposed) {
			put(&d.ProposedSec, n, since(proposed))
		} else {
			missing("%s: no served fact committed for %s", n, victim)
		}
	}

	for _, n := range survivors {
		var forgot int64
		var held string
		for _, e := range stats[n].RoutingForgets {
			if e.Owner != victim || !after(e.ForgottenNs) {
				continue
			}
			forgot = e.ForgottenNs
			var last int64
			for why, at := range e.LastHeldNs {
				if at > last || (at == last && why < held) {
					last, held = at, why
				}
			}
		}
		if forgot == 0 {
			missing("%s: %s's routes not forgotten", n, victim)
			continue
		}
		put(&d.ForgetSec, n, since(forgot))
		if held != "" {
			if d.ForgetLastHeld == nil {
				d.ForgetLastHeld = map[string]string{}
			}
			d.ForgetLastHeld[n] = held
		}
	}

	for _, n := range survivors {
		episodes := stats[n].OwnerKnowledge
		if episodes == nil {
			missing("%s: no owner-knowledge record", n)
			continue
		}
		sec, cut := unknownWithin(episodes, crash, read)
		put(&d.DegradedSec, n, sec)
		if cut {
			missing("%s: owner-knowledge spans before %d s dropped by the bound", n, int(episodes[0].Start.Sub(crash).Seconds()))
		}
	}

	if reached {
		v := instant.Sub(crash).Seconds()
		d.InstantSec = &v
	} else {
		missing("no recovery instant")
	}
	return d
}

// unknownWithin is the seconds of [from, to] the episodes cover, an open one
// running to to; cut reports that the plane's bound may have dropped an
// episode inside the span.
func unknownWithin(episodes []routing.Episode, from, to time.Time) (sec float64, cut bool) {
	for _, e := range episodes {
		start, end := e.Start, e.End
		if end.IsZero() || end.After(to) {
			end = to
		}
		if start.Before(from) {
			start = from
		}
		if end.After(start) {
			sec += end.Sub(start).Seconds()
		}
	}
	return sec, len(episodes) >= routing.MaxEpisodes && episodes[0].Start.After(from)
}

func containsName(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// lastOf is the latest of a per-survivor figure, and whether there was one
// for every name given.
func lastOf(m map[string]float64, names []string) (float64, bool) {
	var last float64
	for i, n := range names {
		v, ok := m[n]
		if !ok {
			return 0, false
		}
		if i == 0 || v > last {
			last = v
		}
	}
	return last, len(names) > 0
}

func secs(v *float64) string {
	if v == nil {
		return "—"
	}
	return fmt.Sprintf("%.2f", *v)
}

func perNode(m map[string]float64, extra map[string]string) string {
	if len(m) == 0 {
		return "—"
	}
	var parts []string
	for _, n := range sortedNames(m) {
		s := fmt.Sprintf("%s %.2f", n, m[n])
		if extra[n] != "" {
			s += " (" + extra[n] + ")"
		}
		parts = append(parts, s)
	}
	return strings.Join(parts, ", ")
}

// dropLine is the report's row: when the map dropped the crashed node against
// the recovery instant, and the steps before it.
func dropLine(d *dropTimeline) string {
	if d == nil {
		return "not recorded"
	}
	var parts []string
	parts = append(parts, "last renewal "+secs(d.LastRenewalSec)+" s", "declaration proposed "+secs(d.DeclarationProposedSec)+" s",
		"applied "+perNode(d.DeclarationAppliedSec, nil))
	if d.MapVersion != 0 {
		parts = append(parts, fmt.Sprintf("map %d decided %s s on a %s check, applied %s", d.MapVersion, secs(d.MapDecidedSec),
			orDash(d.MapCause), perNode(d.MapAppliedSec, nil)))
	}
	parts = append(parts, "forgotten "+perNode(d.ForgetSec, d.ForgetLastHeld), "instant "+secs(d.InstantSec)+" s", dropOrder(d))
	if len(d.Missing) > 0 {
		parts = append(parts, "not read: "+strings.Join(d.Missing, "; "))
	}
	return strings.Join(parts, "; ")
}

func orDash(s string) string {
	if s == "" {
		return "—"
	}
	return s
}

// dropOrder says whether the map without the crashed node had applied on
// every survivor at the instant.
func dropOrder(d *dropTimeline) string {
	last, ok := lastOf(d.MapAppliedSec, d.Survivors)
	switch {
	case d.MapVersion == 0 || !ok:
		return "not dropped on every survivor in the window"
	case d.InstantSec == nil:
		return fmt.Sprintf("dropped on every survivor by %.2f s; no instant", last)
	case last <= *d.InstantSec:
		return fmt.Sprintf("dropped on every survivor %.2f s before the instant", *d.InstantSec-last)
	default:
		return fmt.Sprintf("dropped on every survivor %.2f s **after** the instant", last-*d.InstantSec)
	}
}

// writeDropSection is failoverbounds' report of each run's drop and forget
// against the instant. It judges nothing.
func writeDropSection(w io.Writer, runs []runFile) {
	p := func(format string, a ...any) { _, _ = fmt.Fprintf(w, format, a...) }
	p("\n\n## The crashed node's drop from the shard map, and the forget, against the recovery instant\n\n")
	p("Seconds after the harness's crash mark, from `drop_timeline` (WI 1791). Report-only: no verdict reads it.\n\n")
	p("| Run | Leader | Last renewal | Declaration proposed | Declaration applied | Map decided (check) | Map applied | Forgotten (last held) | Instant | Drop against the instant |\n")
	p("|---|---|---|---|---|---|---|---|---|---|\n")
	for _, r := range runs {
		d := r.rider.DropTimeline
		if d == nil {
			p("| %s | — | — | — | — | — | — | — | — | not recorded in this run.json |\n", r.name)
			continue
		}
		decided := "—"
		if d.MapVersion != 0 {
			decided = fmt.Sprintf("%s (%s)", secs(d.MapDecidedSec), orDash(d.MapCause))
		}
		order := dropOrder(d)
		if len(d.Missing) > 0 {
			order += "; not read: " + strings.Join(d.Missing, "; ")
		}
		p("| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n", r.name, orDash(d.Leader), secs(d.LastRenewalSec), secs(d.DeclarationProposedSec),
			perNode(d.DeclarationAppliedSec, nil), decided, perNode(d.MapAppliedSec, nil), perNode(d.ForgetSec, d.ForgetLastHeld),
			secs(d.InstantSec), order)
	}
}

// servedLine is one run's reading of what bounded the forget: when the last
// survivor's adoptions finished and when the last survivor forgot, against
// the instant.
func servedLine(d *dropTimeline) string {
	adopted, okA := lastOf(d.AdoptedSec, d.Survivors)
	forgot, okF := lastOf(d.ForgetSec, d.Survivors)
	switch {
	case !okA || !okF:
		return "not read on every survivor"
	case d.InstantSec == nil:
		return fmt.Sprintf("adoptions finished by %.2f s, forgotten by %.2f s; no instant", adopted, forgot)
	}
	rel := func(v float64) string {
		if v <= *d.InstantSec {
			return fmt.Sprintf("%.2f s before the instant", *d.InstantSec-v)
		}
		return fmt.Sprintf("%.2f s **after** the instant", v-*d.InstantSec)
	}
	return fmt.Sprintf("last adoptions finished %s; last forget %s, %.2f s after them", rel(adopted), rel(forgot), forgot-adopted)
}

// servedProposals is, per survivor, the served facts its lease service
// proposed over the run, attempts included.
func servedProposals(after map[string]map[string]float64) map[string]float64 {
	out := map[string]float64{}
	for node, series := range after {
		for key, v := range series {
			if name, labels := seriesOf(key); name == "hoardmq_lease_proposals_total" && labels["op"] == "served" {
				out[node] += v
			}
		}
	}
	return out
}

// writeServedSection is failoverbounds' report of WI 1633's two points per
// run: when each survivor's own adoptions up to the crashed node's emptied
// index finished, and when it forgot the crashed node's routes, against the
// instant; and the served facts proposed. It judges nothing.
func writeServedSection(w io.Writer, runs []runFile) {
	p := func(format string, a ...any) { _, _ = fmt.Fprintf(w, format, a...) }
	p("\n\n## Adoptions finished, served facts, and the forget, against the recovery instant\n\n")
	p("Seconds after the harness's crash mark, from `drop_timeline` (WI 1633). *Adoptions finished* is each survivor's " +
		"first served check that found its own adoptions up to the crashed node's emptied index done; *fact* when its " +
		"served fact was committed; *proposals* the served facts its lease service proposed over the run, attempts " +
		"included. Report-only.\n\n")
	p("| Run | Adoptions finished | Fact committed | Forgotten (last held) | Instant | Served proposals | Reading |\n")
	p("|---|---|---|---|---|---|---|\n")
	for _, r := range runs {
		d := r.rider.DropTimeline
		if d == nil {
			p("| %s | — | — | — | — | — | not recorded in this run.json |\n", r.name)
			continue
		}
		proposals := "—"
		if r.rider.MetricsAfter != nil {
			per := servedProposals(r.rider.MetricsAfter)
			var parts []string
			total := 0.0
			for _, n := range sortedNames(r.rider.MetricsAfter) {
				parts = append(parts, fmt.Sprintf("%s %.0f", n, per[n]))
				total += per[n]
			}
			proposals = fmt.Sprintf("%.0f (%s)", total, strings.Join(parts, ", "))
		}
		p("| %s | %s | %s | %s | %s | %s | %s |\n", r.name, perNode(d.AdoptedSec, nil), perNode(d.ProposedSec, nil),
			perNode(d.ForgetSec, d.ForgetLastHeld), secs(d.InstantSec), proposals, servedLine(d))
	}
}

var labelRE = regexp.MustCompile(`(\w+)="([^"]*)"`)

// seriesOf splits a metric key into its name and labels.
func seriesOf(key string) (string, map[string]string) {
	name, rest, ok := strings.Cut(key, "{")
	labels := map[string]string{}
	if ok {
		for _, m := range labelRE.FindAllStringSubmatch(rest, -1) {
			labels[m[1]] = m[2]
		}
	}
	return name, labels
}

// refusalSources is what rose after the instant across the survivors: the
// replication barrier's failures on the crashed node's stream and on a
// survivor's, and forward failures, each by reason. A series that fell (a
// counter reset) adds nothing.
func refusalSources(victim string, atInstant, atEnd map[string]map[string]float64) (crashed, survivor, forwards map[string]float64) {
	crashed, survivor, forwards = map[string]float64{}, map[string]float64{}, map[string]float64{}
	for _, node := range sortedNames(atEnd) {
		inst, ok := atInstant[node]
		if !ok {
			continue
		}
		for key, end := range atEnd[node] {
			d := end - inst[key]
			if d <= 0 {
				continue
			}
			name, labels := seriesOf(key)
			switch name {
			case "hoardmq_replication_barrier_failures_total":
				if labels["peer"] == victim {
					crashed[labels["reason"]] += d
				} else {
					survivor[labels["reason"]] += d
				}
			case "hoardmq_forward_failures_total":
				forwards[labels["reason"]] += d
			}
		}
	}
	return crashed, survivor, forwards
}

func byReason(m map[string]float64) string {
	if len(m) == 0 {
		return "0"
	}
	var total float64
	keys := make([]string, 0, len(m))
	for k, v := range m {
		keys = append(keys, k)
		total += v
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s %.0f", k, m[k])
	}
	return fmt.Sprintf("%.0f (%s)", total, strings.Join(parts, ", "))
}

// writeRefusalSourceSection is failoverbounds' report of where each run's
// refusals after the instant came from, from the counters split at the
// instant. It judges nothing.
func writeRefusalSourceSection(w io.Writer, runs []runFile) {
	p := func(format string, a ...any) { _, _ = fmt.Fprintf(w, format, a...) }
	p("\n\n## Refusals after the recovery instant, by source\n\n")
	p("Increases from the snapshot taken when the confirming hold began to window end, summed over the survivors. " +
		"Barrier failures count every durability wait, not only the probe's publishes, so they are sources, not a " +
		"second count of the probe's refusals. Report-only.\n\n")
	p("| Run | Probe refusals after the instant | Barrier failures, the crashed node's stream | Barrier failures, a survivor's stream | Forward failures |\n")
	p("|---|---|---|---|---|\n")
	for _, r := range runs {
		probe := "—"
		if r.in.Probe != nil {
			n := sumCounts(r.in.Probe.RefusedAfterRecovery)
			if g := r.in.Probe.Groups; g != nil {
				n += sumCounts(g.RefusedAfterRecovery)
			}
			probe = fmt.Sprint(n)
		}
		if r.rider.MetricsAtInstant == nil || r.rider.Victim == "" {
			p("| %s | %s | not split — no snapshot at the instant or no victim in this run.json | — | — |\n", r.name, probe)
			continue
		}
		crashed, survivor, forwards := refusalSources(r.rider.Victim, r.rider.MetricsAtInstant, r.rider.MetricsAfter)
		p("| %s | %s | %s | %s | %s |\n", r.name, probe, byReason(crashed), byReason(survivor), byReason(forwards))
	}
}
