package sim

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// The route census: on every node, the subscriptions its sessions hold against
// the own routes it advertises. The run's filters are one per client, so the
// two are equal on a node whose every subscription is reachable from the
// others; a node holding more than it advertises has subscribers a publish
// made elsewhere cannot find.

// routeFigures are one node's census counters from a /metrics body.
type routeFigures struct {
	Subscriptions float64 `json:"subscriptions"`
	OwnRoutes     float64 `json:"own_routes"`
	// Sessions is the sessions the node owns, for the whole-session check.
	Sessions float64 `json:"sessions"`
}

// censusGap is a node the census could not pass: its figures differ, or were
// not read.
type censusGap struct {
	Node          string  `json:"node"`
	Subscriptions float64 `json:"subscriptions"`
	OwnRoutes     float64 `json:"own_routes"`
	Reason        string  `json:"reason"`
}

// scaleCensus is the census's outcome in run.json: the gaps of its last
// reading, how many readings it took, and when that one was made.
type scaleCensus struct {
	Readings int         `json:"readings"`
	AtMs     int64       `json:"at_ms"`
	Gaps     []censusGap `json:"gaps"`
	// Sessions is the whole-session check over the same reading; nil when a
	// node gave no figures. Its hollow sessions are a finding, not a failure.
	Sessions *sessionCheck `json:"whole_sessions,omitempty"`
	// Reconciler names each node whose route reconciler corrected anything:
	// findings, not failures.
	Reconciler []string `json:"reconciler_findings,omitempty"`
}

// censusWait bounds how long differing figures may take to agree: a move's
// held route withdrawal lasts the takeover timeout, 5 s by default.
// censusPause is the time between readings. Tests shorten both.
var (
	censusWait  = 30 * time.Second
	censusPause = 5 * time.Second
)

// parseRouteFigures reads the census counters from a /metrics body:
// hoardmq_subscriptions_active and hoardmq_sessions_active summed over
// namespaces, and hoardmq_routes_active{scope="local"}. ok is false when
// either of the first and last is absent.
func parseRouteFigures(body string) (routeFigures, bool) {
	var f routeFigures
	subs, routes := false, false
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
		switch {
		case name == "hoardmq_subscriptions_active":
			f.Subscriptions += v
			subs = true
		case name == mSessions:
			f.Sessions += v
		case name == "hoardmq_routes_active" && strings.Contains(labels, `scope="local"`):
			f.OwnRoutes = v
			routes = true
		}
	}
	return f, subs && routes
}

// censusGaps decides the census over one reading of every node: a node whose
// figures differ, or that gave none, is a gap. No reading at all is a gap
// too, so a census that read nothing does not pass. It is pure.
func censusGaps(readings []lockReading) []censusGap {
	gaps := make([]censusGap, 0)
	if len(readings) == 0 {
		return append(gaps, censusGap{Reason: "no node read"})
	}
	for _, rd := range readings {
		switch {
		case rd.Error != "":
			gaps = append(gaps, censusGap{Node: rd.Node, Reason: "not read: " + rd.Error})
		case rd.Routes == nil:
			gaps = append(gaps, censusGap{Node: rd.Node, Reason: "no census counters"})
		case rd.Routes.Subscriptions != rd.Routes.OwnRoutes:
			gaps = append(gaps, censusGap{Node: rd.Node, Subscriptions: rd.Routes.Subscriptions, OwnRoutes: rd.Routes.OwnRoutes,
				Reason: "subscriptions and own routes differ"})
		}
	}
	return gaps
}

// census reads every node at the scrape point "census" until the figures
// agree or censusWait passes, records the last reading's gaps, and fails the
// run on any.
func (r *scaleRun) census(ctx context.Context) string {
	deadline := time.Now().Add(censusWait)
	res := &scaleCensus{}
	r.rec.Census = res
	exempt, why := r.sessionExempt()
	for {
		from := len(r.rec.LockFetch)
		r.readLocks(ctx, "census")
		res.Readings++
		res.AtMs = time.Now().UnixMilli()
		res.Gaps = censusGaps(r.rec.LockFetch[from:])
		res.Sessions = nil
		if c, ok := checkSessions(r.rec.LockFetch[from:], exempt, why); ok {
			if r.rec.Fill.Failed > 0 {
				c.Inexact = fmt.Sprintf("%d clients failed the fill, each holding a session without its subscription or none", r.rec.Fill.Failed)
			}
			res.Sessions = &c
		}
		whole := res.Sessions != nil && res.Sessions.Hollow == 0
		if (len(res.Gaps) == 0 && whole) || !time.Now().Before(deadline) || ctx.Err() != nil {
			break
		}
		sleepCtx(ctx, censusPause)
	}
	res.Reconciler = reconcileFindings(r.rec.LockFetch, "census")
	if len(res.Gaps) == 0 {
		return fmt.Sprintf("every node advertises what it holds (%d readings)", res.Readings)
	}
	parts := make([]string, 0, len(res.Gaps))
	for _, g := range res.Gaps {
		parts = append(parts, fmt.Sprintf("%s: %s (%.0f subscriptions, %.0f own routes)", g.Node, g.Reason, g.Subscriptions, g.OwnRoutes))
	}
	r.fail("route census: %s", strings.Join(parts, "; "))
	return "gaps: " + strings.Join(parts, "; ")
}

// sessionExempt is how many sessions the run itself left holding no
// subscription when the census reads: the probe's publishers, and the
// delivery check's negative controls.
func (r *scaleRun) sessionExempt() (int, string) {
	pubs := 0
	if r.pr != nil {
		pubs = len(r.pr.pubs)
	}
	controls := 0
	if r.rec.Delivery != nil {
		controls = r.rec.Delivery.ControlsRemoved
	}
	return pubs + controls, fmt.Sprintf("%d probe publishers, %d negative controls", pubs, controls)
}
