package sim

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// The follower's lock and fetch histograms a scale run reads from each node's
// /metrics: what the in-process failover harness reads from the broker's own
// trace, which a run across hosts cannot reach.
var lockFamilies = []string{
	"hoardmq_replication_lock_hold_seconds",
	"hoardmq_replication_lock_wait_seconds",
	"hoardmq_replication_fetch_request_seconds",
}

// histSeries is one exported histogram: its bounds in increasing order
// ("+Inf" last), the cumulative count at each, the sum in seconds and the
// count.
type histSeries struct {
	Le     []string  `json:"le"`
	Counts []float64 `json:"counts"`
	Sum    float64   `json:"sum"`
	Count  float64   `json:"count"`
}

// lockReading is one node's histograms at one point of the run, keyed by
// family and, after a slash, the kind label where there is one.
type lockReading struct {
	Point string                 `json:"point"`
	Node  string                 `json:"node"`
	AtMs  int64                  `json:"at_ms"`
	Error string                 `json:"error,omitempty"`
	Hist  map[string]*histSeries `json:"hist,omitempty"`
	// Routes is the route census's counters from the same body.
	Routes *routeFigures `json:"routes,omitempty"`
	// EmptyServes is the node's sessions served empty, from the same body.
	EmptyServes float64 `json:"empty_serves"`
	// Fetches is the node's replica fetches by outcome, and Reconcile its
	// route reconciler's counters, from the same body.
	Fetches   map[string]float64 `json:"fetches,omitempty"`
	Reconcile *reconcileFigures  `json:"reconcile,omitempty"`
	// TookMs is how long the scrape took: at a million sessions it walks
	// every session record, so it is part of the load it measures.
	TookMs int64 `json:"took_ms"`
}

// parseHistograms reads lockFamilies' histograms from a /metrics body, each
// one's buckets sorted by bound, since /metrics writes them in label order. A
// body without them, a broker from before they were exported, yields none.
func parseHistograms(body string) map[string]*histSeries {
	out := map[string]*histSeries{}
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
		name, labelText, _ := strings.Cut(line[:sp], "{")
		labels := map[string]string{}
		for _, pair := range strings.Split(strings.TrimSuffix(labelText, "}"), ",") {
			if k, val, ok := strings.Cut(pair, "="); ok {
				labels[k] = strings.Trim(val, `"`)
			}
		}
		for _, f := range lockFamilies {
			suffix, ok := strings.CutPrefix(name, f+"_")
			if !ok {
				continue
			}
			key := f
			if k := labels["kind"]; k != "" {
				key += "/" + k
			}
			h := out[key]
			if h == nil {
				h = &histSeries{}
				out[key] = h
			}
			switch suffix {
			case "bucket":
				h.Le = append(h.Le, labels["le"])
				h.Counts = append(h.Counts, v)
			case "sum":
				h.Sum = v
			case "count":
				h.Count = v
			}
		}
	}
	for _, h := range out {
		idx := make([]int, len(h.Le))
		for i := range idx {
			idx[i] = i
		}
		sort.SliceStable(idx, func(a, b int) bool { return leBound(h.Le[idx[a]]) < leBound(h.Le[idx[b]]) })
		le, counts := make([]string, len(idx)), make([]float64, len(idx))
		for i, j := range idx {
			le[i], counts[i] = h.Le[j], h.Counts[j]
		}
		h.Le, h.Counts = le, counts
	}
	return out
}

// leBound is a bucket's bound; +Inf, or one that does not parse, is last.
func leBound(le string) float64 {
	v, err := strconv.ParseFloat(le, 64)
	if err != nil {
		return math.Inf(1)
	}
	return v
}

// histWindow is a histogram's samples between two readings of one process:
// their count and the bound of the bucket holding the 99th percentile's
// sample, in seconds (+Inf past the last bound). ok is false when the later
// reading counts fewer, a restart between them.
func histWindow(before, after *histSeries) (count float64, p99 float64, ok bool) {
	if after == nil {
		return 0, 0, false
	}
	b := &histSeries{Counts: make([]float64, len(after.Counts))}
	if before != nil {
		b = before
	}
	if len(b.Counts) != len(after.Counts) || after.Count < b.Count {
		return 0, 0, false
	}
	count = after.Count - b.Count
	if count == 0 {
		return 0, 0, true
	}
	want := math.Ceil(count * 0.99)
	for i := range after.Counts {
		if after.Counts[i]-b.Counts[i] >= want {
			if after.Le[i] == "+Inf" {
				return count, math.Inf(1), true
			}
			le, _ := strconv.ParseFloat(after.Le[i], 64)
			return count, le, true
		}
	}
	return count, math.Inf(1), true
}

// readLocks records every node's lock and fetch histograms, and the route
// census's counters, at a point of the run. A node that does not answer is
// recorded with its error.
func (r *scaleRun) readLocks(ctx context.Context, point string) {
	r.rec.LockFetch = append(r.rec.LockFetch, readLocksOf(ctx, point, r.cfg.Layout.Nodes)...)
}

// readLocksOf reads the given nodes at a point and returns their readings.
func readLocksOf(ctx context.Context, point string, nodes []scaleNode) []lockReading {
	out := make([]lockReading, 0, len(nodes))
	for _, n := range nodes {
		start := time.Now()
		rd := lockReading{Point: point, Node: n.Name, AtMs: start.UnixMilli()}
		body, err := scrapeBody(ctx, n)
		rd.TookMs = time.Since(start).Milliseconds()
		if err != nil {
			rd.Error = err.Error()
		} else {
			rd.Hist = parseHistograms(body)
			if f, ok := parseRouteFigures(body); ok {
				rd.Routes = &f
			}
			rd.EmptyServes = parseEmptyServes(body)
			rd.Fetches = parseFetches(body)
			rd.Reconcile = parseReconcile(body)
		}
		out = append(out, rd)
	}
	return out
}

func scrapeBody(ctx context.Context, n scaleNode) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+n.obsAddr()+"/metrics", nil)
	if err != nil {
		return "", err
	}
	resp, err := scaleHTTP.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	return string(body), err
}

// lockReport is the report's table of the storm window, on each node read
// both before the kill and after the verdict.
func lockReport(readings []lockReading) string {
	at := map[string]map[string]lockReading{}
	for _, rd := range readings {
		if at[rd.Point] == nil {
			at[rd.Point] = map[string]lockReading{}
		}
		at[rd.Point][rd.Node] = rd
	}
	var nodes []string
	for n, rd := range at["storm-end"] {
		if before, ok := at["storm-start"][n]; ok && rd.Error == "" && before.Error == "" {
			nodes = append(nodes, n)
		}
	}
	if len(nodes) == 0 {
		return ""
	}
	sort.Strings(nodes)
	cols := []struct{ title, key string }{
		{"find waits", "hoardmq_replication_lock_wait_seconds/find"},
		{"apply waits", "hoardmq_replication_lock_wait_seconds/apply"},
		{"apply holds", "hoardmq_replication_lock_hold_seconds/apply"},
		{"fetch requests", "hoardmq_replication_fetch_request_seconds"},
	}
	var b strings.Builder
	b.WriteString("\n## The follower's lock and fetches, storm window\n\nFrom before the kill to the storm's verdict, on each node read at both: samples, and the 99th percentile's bucket bound in ms.\n\n| Node |")
	for _, c := range cols {
		fmt.Fprintf(&b, " %s | p99 ms |", c.title)
	}
	b.WriteString("\n|---|")
	for range cols {
		b.WriteString("---|---|")
	}
	b.WriteString("\n")
	for _, n := range nodes {
		fmt.Fprintf(&b, "| %s |", n)
		for _, c := range cols {
			count, p99, ok := histWindow(at["storm-start"][n].Hist[c.key], at["storm-end"][n].Hist[c.key])
			switch {
			case !ok:
				b.WriteString(" — | — |")
			case count == 0:
				b.WriteString(" 0 | — |")
			default:
				fmt.Fprintf(&b, " %.0f | %.3f |", count, p99*1000)
			}
		}
		b.WriteString("\n")
	}
	return b.String()
}
