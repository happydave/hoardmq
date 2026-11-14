package sim

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

// The counters a scale run reads beside the lock histograms at each point:
// replica fetches by outcome, the route reconciler's work, and each node's
// sessions against its subscriptions.

// eachSample calls f for every sample line of a /metrics body with its name,
// its label text (without braces) and its value.
func eachSample(body string, f func(name, labels string, v float64)) {
	for _, line := range strings.Split(body, "\n") {
		if line == "" || line[0] == '#' {
			continue
		}
		sp := strings.LastIndexByte(line, ' ')
		if sp < 0 {
			continue
		}
		v, err := strconv.ParseFloat(line[sp+1:], 64)
		if err != nil {
			continue
		}
		name, labels, _ := strings.Cut(line[:sp], "{")
		f(name, strings.TrimSuffix(labels, "}"), v)
	}
}

// labelValue is one label's value from a sample's label text, or "".
func labelValue(labels, key string) string {
	for _, pair := range strings.Split(labels, ",") {
		if k, v, ok := strings.Cut(pair, "="); ok && k == key {
			return strings.Trim(v, `"`)
		}
	}
	return ""
}

// parseFetches reads hoardmq_replication_fetches_total by outcome. An outcome
// the node never reached has no series, so it is absent, not 0.
func parseFetches(body string) map[string]float64 {
	out := map[string]float64{}
	eachSample(body, func(name, labels string, v float64) {
		if name == "hoardmq_replication_fetches_total" {
			out[labelValue(labels, "outcome")] += v
		}
	})
	return out
}

// reconcileFigures is one node's route reconciler at one reading: passes
// made, differences seen once and awaiting their second sighting, and the
// corrections, refusals and skips counted so far, each keyed by its labels.
type reconcileFigures struct {
	Passes      float64            `json:"passes"`
	Suspects    float64            `json:"suspects"`
	Corrections map[string]float64 `json:"corrections"`
	Refused     map[string]float64 `json:"refused"`
	Skipped     map[string]float64 `json:"skipped"`
}

// parseReconcile reads the reconciler's series, or nil from a node that has
// no reconciler (no passes series).
func parseReconcile(body string) *reconcileFigures {
	f := &reconcileFigures{Corrections: map[string]float64{}, Refused: map[string]float64{}, Skipped: map[string]float64{}}
	seen := false
	eachSample(body, func(name, labels string, v float64) {
		switch name {
		case "hoardmq_route_reconcile_passes_total":
			f.Passes, seen = v, true
		case "hoardmq_route_reconcile_suspects":
			f.Suspects = v
		case "hoardmq_route_reconcile_corrections_total":
			f.Corrections[labelValue(labels, "direction")+" "+labelValue(labels, "kind")] += v
		case "hoardmq_route_reconcile_refused_total":
			f.Refused[labelValue(labels, "kind")+" "+labelValue(labels, "reason")] += v
		case "hoardmq_route_reconcile_skipped_total":
			f.Skipped[labelValue(labels, "reason")] += v
		}
	})
	if !seen {
		return nil
	}
	return f
}

// total sums a figure map.
func total(m map[string]float64) float64 {
	s := 0.0
	for _, v := range m {
		s += v
	}
	return s
}

// reconcileFindings names every node whose reconciler corrected anything by
// the last of the readings at point: a correction is a difference the live
// paths left, so a finding, while the run itself goes on. It is pure.
func reconcileFindings(readings []lockReading, point string) []string {
	last := map[string]*reconcileFigures{}
	for _, rd := range readings {
		if rd.Point == point && rd.Reconcile != nil {
			last[rd.Node] = rd.Reconcile
		}
	}
	var out []string
	for _, n := range sortedNames(last) {
		f := last[n]
		if c := total(f.Corrections); c > 0 {
			out = append(out, fmt.Sprintf("%s's route reconciler made %.0f corrections (%s)", n, c, figureList(f.Corrections)))
		}
	}
	return out
}

// figureList writes a figure map as "key n, key n" in key order.
func figureList(m map[string]float64) string {
	parts := make([]string, 0, len(m))
	for _, k := range sortedNames(m) {
		parts = append(parts, fmt.Sprintf("%s %.0f", k, m[k]))
	}
	return strings.Join(parts, ", ")
}

// sessionCheck is the whole-session check: each node's sessions and
// subscriptions, and the sessions holding none beyond those the run made so.
// Every client of the run holds one filter, so without a hollow session the
// sessions exceed the subscriptions by exactly the exempt ones: the probe's
// publishers and the delivery check's negative controls.
type sessionCheck struct {
	PerNode       map[string][2]float64 `json:"per_node"`
	Sessions      float64               `json:"sessions"`
	Subscriptions float64               `json:"subscriptions"`
	Exempt        int                   `json:"exempt"`
	ExemptWhy     string                `json:"exempt_why"`
	// Hollow is sessions minus subscriptions minus exempt: sessions holding
	// no subscription though their client subscribed one.
	Hollow float64 `json:"hollow"`
	// Inexact says why the figure may be off, when it may.
	Inexact string `json:"inexact,omitempty"`
}

// checkSessions decides the whole-session check over one reading of every
// node. ok is false when a node gave no figures, so the check is not
// decided. It is pure.
func checkSessions(readings []lockReading, exempt int, why string) (sessionCheck, bool) {
	c := sessionCheck{PerNode: map[string][2]float64{}, Exempt: exempt, ExemptWhy: why}
	if len(readings) == 0 {
		return c, false
	}
	for _, rd := range readings {
		if rd.Routes == nil {
			return c, false
		}
		c.PerNode[rd.Node] = [2]float64{rd.Routes.Sessions, rd.Routes.Subscriptions}
		c.Sessions += rd.Routes.Sessions
		c.Subscriptions += rd.Routes.Subscriptions
	}
	c.Hollow = c.Sessions - c.Subscriptions - float64(exempt)
	return c, true
}

// longHoldMin is the bound above which a lock hold or wait counts as long:
// the bucket edge 2^19 µs, about a quarter of a replica fetch's 2 s budget.
const longHoldMin = 0.524288

// holdInterval is one node's lock and fetch figures between two consecutive
// readings in the storm window, dated against the crash.
type holdInterval struct {
	Node    string  `json:"node"`
	FromSec float64 `json:"from_sec"`
	ToSec   float64 `json:"to_sec"`
	// LongHolds is, by kind, the follower's lock holds over longHoldMin, and
	// LongestBucketSec the bound of the highest hold bucket that rose, -1
	// past the last bound.
	LongHolds        map[string]float64 `json:"long_holds,omitempty"`
	LongestBucketSec float64            `json:"longest_hold_bucket_sec,omitempty"`
	// LongFindWaits is fetches that waited over longHoldMin for the lock.
	LongFindWaits float64 `json:"long_find_waits,omitempty"`
	EmptyServes   float64 `json:"empty_serves,omitempty"`
	// Fetches is the replica fetches by outcome.
	Fetches map[string]float64 `json:"fetches,omitempty"`
}

// notable says whether the report lists the interval: a long hold or wait,
// a session served empty, or a fetch that found nothing.
func (h holdInterval) notable() bool {
	if len(h.LongHolds) > 0 || h.LongFindWaits > 0 || h.EmptyServes > 0 {
		return true
	}
	for o := range h.Fetches {
		if strings.HasPrefix(o, "none") {
			return true
		}
	}
	return false
}

const (
	holdFamily = "hoardmq_replication_lock_hold_seconds/"
	findWaits  = "hoardmq_replication_lock_wait_seconds/find"
)

// overBound is how many of a histogram's samples between two readings lie
// above bound, and the bound of the highest bucket that rose (-1 past the
// last bound); ok is false when the readings cannot be compared (a restart
// between them).
func overBound(before, after *histSeries, bound float64) (n, highest float64, ok bool) {
	if after == nil {
		return 0, 0, before == nil
	}
	b := &histSeries{Le: after.Le, Counts: make([]float64, len(after.Counts))}
	if before != nil {
		b = before
	}
	if len(b.Counts) != len(after.Counts) || after.Count < b.Count {
		return 0, 0, false
	}
	below := 0.0
	prev := 0.0
	for i, le := range after.Le {
		d := after.Counts[i] - b.Counts[i]
		edge := leBound(le)
		if edge <= bound {
			below = d
		}
		if d > prev {
			highest = edge
			if math.IsInf(edge, 1) {
				highest = -1
			}
		}
		prev = d
	}
	return after.Count - b.Count - below, highest, true
}

// holdIntervals splits the storm window into each node's intervals between
// consecutive readings at the storm's points, in time order. It is pure.
func holdIntervals(readings []lockReading, crashMs int64) []holdInterval {
	byNode := map[string][]lockReading{}
	for _, rd := range readings {
		switch rd.Point {
		case "storm-start", "storm-tick", "storm-end":
		default:
			continue
		}
		if rd.Error != "" {
			continue
		}
		byNode[rd.Node] = append(byNode[rd.Node], rd)
	}
	out := make([]holdInterval, 0)
	for _, n := range sortedNames(byNode) {
		rs := byNode[n]
		sort.SliceStable(rs, func(i, j int) bool { return rs[i].AtMs < rs[j].AtMs })
		for i := 1; i < len(rs); i++ {
			a, b := rs[i-1], rs[i]
			h := holdInterval{Node: n, FromSec: float64(a.AtMs-crashMs) / 1000, ToSec: float64(b.AtMs-crashMs) / 1000,
				EmptyServes: math.Max(0, b.EmptyServes-a.EmptyServes)}
			for key, after := range b.Hist {
				kind, ok := strings.CutPrefix(key, holdFamily)
				if !ok {
					continue
				}
				if c, hi, ok := overBound(a.Hist[key], after, longHoldMin); ok && c > 0 {
					if h.LongHolds == nil {
						h.LongHolds = map[string]float64{}
					}
					h.LongHolds[kind] = c
					if hi < 0 || (h.LongestBucketSec >= 0 && hi > h.LongestBucketSec) {
						h.LongestBucketSec = hi
					}
				}
			}
			if c, _, ok := overBound(a.Hist[findWaits], b.Hist[findWaits], longHoldMin); ok {
				h.LongFindWaits = c
			}
			for o, v := range b.Fetches {
				if d := v - a.Fetches[o]; d > 0 {
					if h.Fetches == nil {
						h.Fetches = map[string]float64{}
					}
					h.Fetches[o] = d
				}
			}
			out = append(out, h)
		}
	}
	return out
}

// lastAt is each node's last reading at a point.
func lastAt(readings []lockReading, point string) map[string]lockReading {
	out := map[string]lockReading{}
	for _, rd := range readings {
		if rd.Point == point && rd.Error == "" {
			out[rd.Node] = rd
		}
	}
	return out
}

// wholeSessionReport is the report's line for the whole-session check.
func wholeSessionReport(c *sessionCheck) string {
	if c == nil {
		return "- Whole sessions: not decided (a node gave no figures).\n"
	}
	var b strings.Builder
	label := "Whole sessions"
	if c.Hollow != 0 {
		label = "**Finding: hollow sessions**"
	}
	fmt.Fprintf(&b, "- %s: %.0f sessions, %.0f subscriptions, %d exempt (%s): %.0f holding no subscription their client subscribed.", label,
		c.Sessions, c.Subscriptions, c.Exempt, c.ExemptWhy, c.Hollow)
	for _, n := range sortedNames(c.PerNode) {
		fmt.Fprintf(&b, " %s %.0f / %.0f.", n, c.PerNode[n][0], c.PerNode[n][1])
	}
	if c.Inexact != "" {
		fmt.Fprintf(&b, " Inexact: %s.", c.Inexact)
	}
	b.WriteString("\n")
	return b.String()
}

// reconcileReport is the report's table of each node's route reconciler at
// the census and at the run's end.
func reconcileReport(readings []lockReading) string {
	var b strings.Builder
	for _, point := range []string{"census", "end"} {
		at := lastAt(readings, point)
		for _, n := range sortedNames(at) {
			f := at[n].Reconcile
			if f == nil {
				continue
			}
			if b.Len() == 0 {
				b.WriteString("\n## Route reconciler\n\nEach node's counters at the census and at the end; a correction is a finding.\n\n| Point | Node | Passes | Suspects | Corrections | Refused | Skipped |\n|---|---|---|---|---|---|---|\n")
			}
			fmt.Fprintf(&b, "| %s | %s | %.0f | %.0f | %.0f %s | %.0f %s | %.0f %s |\n", point, n, f.Passes, f.Suspects,
				total(f.Corrections), figureList(f.Corrections), total(f.Refused), figureList(f.Refused), total(f.Skipped), figureList(f.Skipped))
		}
	}
	return b.String()
}

// fetchReport is the report's table of each node's replica fetches by
// outcome, at the storm's start and end and at the run's end.
func fetchReport(readings []lockReading) string {
	var b strings.Builder
	for _, point := range []string{"storm-start", "storm-end", "end"} {
		at := lastAt(readings, point)
		for _, n := range sortedNames(at) {
			if b.Len() == 0 {
				b.WriteString("\n## Replica fetches by outcome\n\nEach node's `hoardmq_replication_fetches_total` since its start; an outcome it never reached is absent.\n\n| Point | Node | Outcomes |\n|---|---|---|\n")
			}
			fmt.Fprintf(&b, "| %s | %s | %s |\n", point, n, figureList(at[n].Fetches))
		}
	}
	return b.String()
}

// intervalReport is the report's table of the storm window's notable
// intervals on each survivor.
func intervalReport(hs []holdInterval) string {
	if len(hs) == 0 {
		return ""
	}
	var b strings.Builder
	notable := 0
	for _, h := range hs {
		if h.notable() {
			notable++
		}
	}
	fmt.Fprintf(&b, "\n## Long holds through the storm\n\n%d intervals between readings about %s apart; %d with a lock hold or wait over %.2f s, a session served empty, or a fetch that found nothing:\n\n",
		len(hs), stormTickEvery, notable, longHoldMin)
	if notable == 0 {
		return b.String()
	}
	b.WriteString("| Node | From s | To s | Long holds | Longest bucket s | Long find waits | Served empty | Fetches |\n|---|---|---|---|---|---|---|---|\n")
	for _, h := range hs {
		if !h.notable() {
			continue
		}
		fmt.Fprintf(&b, "| %s | %+.1f | %+.1f | %s | %.3f | %.0f | %.0f | %s |\n", h.Node, h.FromSec, h.ToSec, figureList(h.LongHolds),
			h.LongestBucketSec, h.LongFindWaits, h.EmptyServes, figureList(h.Fetches))
	}
	return b.String()
}
