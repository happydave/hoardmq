package broker

import (
	"context"
	"encoding/json"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/replication"
	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

// scrapedHistogram is one exported histogram, its buckets in increasing
// bound.
type scrapedHistogram struct {
	les    []float64
	counts []float64
	sum    float64
	count  float64
	hasSum bool
}

// scrapeHistograms reads every histogram series of the named families from a
// /metrics body, keyed by family and the kind label ("" without one). The
// buckets are sorted by bound: /metrics writes them in label order.
func scrapeHistograms(body string, families ...string) map[string]map[string]*scrapedHistogram {
	out := map[string]map[string]*scrapedHistogram{}
	for _, f := range families {
		out[f] = map[string]*scrapedHistogram{}
	}
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "#") {
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
		series, labelText, _ := strings.Cut(line[:sp], "{")
		labels := map[string]string{}
		for _, pair := range strings.Split(strings.TrimSuffix(labelText, "}"), ",") {
			if k, val, ok := strings.Cut(pair, "="); ok {
				labels[k] = strings.Trim(val, `"`)
			}
		}
		for _, f := range families {
			suffix, ok := strings.CutPrefix(series, f+"_")
			if !ok {
				continue
			}
			h := out[f][labels["kind"]]
			if h == nil {
				h = &scrapedHistogram{}
				out[f][labels["kind"]] = h
			}
			switch suffix {
			case "bucket":
				le := math.Inf(1)
				if labels["le"] != "+Inf" {
					le, _ = strconv.ParseFloat(labels["le"], 64)
				}
				h.les = append(h.les, le)
				h.counts = append(h.counts, v)
			case "sum":
				h.sum, h.hasSum = v, true
			case "count":
				h.count = v
			}
		}
	}
	for _, byKind := range out {
		for _, h := range byKind {
			idx := make([]int, len(h.les))
			for i := range idx {
				idx[i] = i
			}
			sort.Slice(idx, func(a, b int) bool { return h.les[idx[a]] < h.les[idx[b]] })
			les, counts := make([]float64, len(idx)), make([]float64, len(idx))
			for i, j := range idx {
				les[i], counts[i] = h.les[j], h.counts[j]
			}
			h.les, h.counts = les, counts
		}
	}
	return out
}

// p99Ns is the bound, in nanoseconds, of the first bucket whose cumulative
// count reaches the 99th percentile's sample: the trace's rule, applied to the
// exported buckets.
func (h *scrapedHistogram) p99Ns() int64 {
	if h.count == 0 {
		return 0
	}
	want := (int64(h.count)*99 + 99) / 100
	for i, c := range h.counts {
		if int64(c) >= want {
			return int64(math.Round(h.les[i] * 1e9))
		}
	}
	return -1
}

// lockTrace is the part of a node's in-process trace the export carries.
type lockTrace struct {
	byKind       map[string]replication.LockHoldKind
	requests     int64
	requestMs    float64
	requestP99Ns int64
}

func readLockTrace(f *fixture) lockTrace {
	rep := f.srv.Replication()
	bs := rep.BatchStats()
	fs := rep.FetchStats()
	return lockTrace{byKind: bs.LockHolds.ByKind, requests: fs.Requests, requestMs: fs.RequestMs, requestP99Ns: fs.RequestP99Ns}
}

const (
	famHold  = "hoardmq_replication_lock_hold_seconds"
	famWait  = "hoardmq_replication_lock_wait_seconds"
	famFetch = "hoardmq_replication_fetch_request_seconds"
)

// TestSetHistogramWritesThePrometheusForm: cumulative counts at 1 µs to
// 2^22 µs, a sample past the last bound under +Inf only, the sum in seconds.
func TestSetHistogramWritesThePrometheusForm(t *testing.T) {
	var h replication.Histogram
	h.Buckets[0], h.Buckets[3], h.Buckets[22], h.Buckets[23] = 2, 1, 1, 1
	h.Count, h.SumNs = 5, 7_500_000_000
	got := map[string]float64{}
	setHistogram(func(name string, labels map[string]string, v float64) {
		got[name+"/"+labels["kind"]+"/"+labels["le"]] = v
	}, "h", map[string]string{"kind": "find"}, h)
	want := map[string]float64{"h_bucket/find/1e-06": 2, "h_bucket/find/2e-06": 2, "h_bucket/find/8e-06": 3,
		"h_bucket/find/2.097152": 3, "h_bucket/find/4.194304": 4, "h_bucket/find/+Inf": 5, "h_sum/find/": 7.5, "h_count/find/": 5}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %v; want %v", k, got[k], v)
		}
	}
	if len(got) != 26 {
		t.Errorf("%d series; want 23 bounds, +Inf, the sum and the count", len(got))
	}
}

// TestLockAndFetchHistogramsAgreeWithTheTrace: what /metrics exports of the
// follower's lock and of fetch requests is the in-process trace's: each
// histogram's count and sum are the trace's, its buckets are cumulative up to
// +Inf at the count, and the 99th percentile read off the exported buckets is
// the trace's own.
func TestLockAndFetchHistogramsAgreeWithTheTrace(t *testing.T) {
	nodes := startTrio(t, func(_ string, d *config.Document) {
		d.Cluster.Shards = 8
		d.Cluster.Replicas = 2
		d.Observability.Address = "127.0.0.1:0"
		d.Modules["metrics.prometheus"] = json.RawMessage(`{}`)
	})
	m := waitMap(t, nodes, 3)
	id := "traced"
	targets := m.Targets(shardmap.ShardOf("default", id, 8), "n1")
	if len(targets) != 1 {
		t.Fatalf("targets %v; want one", targets)
	}
	target := targets[0]
	exp := uint32(600)
	x, ack := nodes["n1"].connect("tcp", packet.V5, &packet.Connect{ClientID: id, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("connect: %s", ack.ReasonCode)
	}
	defer func() { _ = x.c.Close() }()
	waitUntil(t, target+" to hold the copy", 5*time.Second, func() bool {
		return nodes[target].srv.Replication().ReplicaState().Find("default", id) != nil
	})
	var askers []string
	for name := range nodes {
		if name != target {
			askers = append(askers, name)
		}
	}
	for _, name := range askers {
		for range 3 {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			st, answered, _ := nodes[name].srv.Replication().Fetch(ctx, "default", id, "n1")
			cancel()
			if st == nil || !answered {
				t.Fatalf("%s's fetch: copy %v, answered %v", name, st != nil, answered)
			}
		}
	}

	for name, n := range nodes {
		var tr lockTrace
		var body string
		settled := false
		for range 20 {
			before := readLockTrace(n)
			body = mustScrape(t, n)
			tr = readLockTrace(n)
			if reflect.DeepEqual(before, tr) {
				settled = true
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if !settled {
			t.Fatalf("%s: the trace moved during each of 20 scrapes", name)
		}
		got := scrapeHistograms(body, famHold, famWait, famFetch)
		check := func(what string, h *scrapedHistogram, count int64, sumSec float64, p99 int64) {
			t.Helper()
			if h == nil {
				t.Errorf("%s: %s not exported", name, what)
				return
			}
			if int64(h.count) != count {
				t.Errorf("%s: %s count %v; the trace's %d", name, what, h.count, count)
			}
			if !h.hasSum || math.Abs(h.sum-sumSec) > 1e-9*math.Max(1, sumSec) {
				t.Errorf("%s: %s sum %v s (present %v); the trace's %v s", name, what, h.sum, h.hasSum, sumSec)
			}
			if len(h.les) != 24 || !math.IsInf(h.les[len(h.les)-1], 1) {
				t.Errorf("%s: %s bounds %v; want 1 µs to 2^22 µs and +Inf", name, what, h.les)
				return
			}
			for i := range h.les {
				if want := float64(int64(1)<<i) / 1e6; i < 23 && h.les[i] != want {
					t.Errorf("%s: %s bound %d is %v; want %v", name, what, i, h.les[i], want)
				}
				if i > 0 && (h.les[i] <= h.les[i-1] || h.counts[i] < h.counts[i-1]) {
					t.Errorf("%s: %s not cumulative in increasing bounds at %d: %v %v", name, what, i, h.les, h.counts)
				}
			}
			if last := h.counts[len(h.counts)-1]; last != h.count {
				t.Errorf("%s: %s +Inf bucket %v; count %v", name, what, last, h.count)
			}
			if got := h.p99Ns(); got != p99 {
				t.Errorf("%s: %s p99 from the exported buckets %d ns; the trace's %d ns", name, what, got, p99)
			}
		}
		for _, kind := range []string{"apply", "apply-other", "snapshot-rebalance", "snapshot-other", "checkpoint", "find"} {
			k := tr.byKind[kind]
			check("hold "+kind, got[famHold][kind], k.Count, float64(k.TotalNs)/1e9, k.HoldP99Ns)
		}
		for _, kind := range []string{"apply", "apply-other", "find"} {
			k := tr.byKind[kind]
			check("wait "+kind, got[famWait][kind], k.WaitCount, float64(k.WaitTotalNs)/1e9, k.WaitP99Ns)
		}
		if len(got[famWait]) != 3 {
			t.Errorf("%s: wait histograms for %d kinds; want apply, apply-other and find only", name, len(got[famWait]))
		}
		check("fetch requests", got[famFetch][""], tr.requests, tr.requestMs/1e3, tr.requestP99Ns)

		// Non-vacuity: what the session's replication and the fetches did is
		// in what was compared.
		if name == target {
			if k := tr.byKind["apply"]; k.Count < 1 || k.WaitCount < 1 {
				t.Errorf("%s: apply holds %d, waits %d; want each at least 1", name, k.Count, k.WaitCount)
			}
			if tr.byKind["find"].WaitCount < 1 {
				t.Errorf("%s: no find waited on the holder", name)
			}
		} else if tr.requests < 1 {
			t.Errorf("%s: no fetch request", name)
		}
	}
}
