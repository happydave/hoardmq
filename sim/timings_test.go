package sim

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/config"
)

func env(vars map[string]string) func(string) string {
	return func(k string) string { return vars[k] }
}

// TestRecordedFailoverDefaultsToProductTimings is WI 1475's default: a
// recorded run measures the timings the broker ships.
func TestRecordedFailoverDefaultsToProductTimings(t *testing.T) {
	cases := []struct {
		name   string
		vars   map[string]string
		want   TimingSet
		failed bool
	}{
		{"no variable", nil, TimingsProduct, false},
		{"product named", map[string]string{timingsEnv: "product"}, TimingsProduct, false},
		{"harness named", map[string]string{timingsEnv: "harness"}, TimingsHarness, false},
		{"unknown set", map[string]string{timingsEnv: "fast"}, "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := failoverTimings(true, env(tc.vars))
			if (err != nil) != tc.failed || got != tc.want {
				t.Fatalf("got %q, %v; want %q (error %v)", got, err, tc.want, tc.failed)
			}
			if tc.failed && !strings.Contains(err.Error(), "product") {
				t.Errorf("the error should name the allowed sets: %v", err)
			}
		})
	}
}

// TestGateKeepsHarnessTimings: the gate's duration is an invariant, so it
// runs the harness's timings whatever the variable says.
func TestGateKeepsHarnessTimings(t *testing.T) {
	for _, v := range []string{"", "product", "harness", "fast"} {
		got, err := failoverTimings(false, env(map[string]string{timingsEnv: v}))
		if err != nil || got != TimingsHarness {
			t.Errorf("%s=%q on the gate: got %q, %v; want harness", timingsEnv, v, got, err)
		}
	}
}

// TestProductTimingsComeFromTheProductDefaults: the product set is what
// config.Default ships, every timing of it, and the harness set is unchanged.
func TestProductTimingsComeFromTheProductDefaults(t *testing.T) {
	d := config.Default()
	want := clusterTimings{
		HeartbeatMs: d.Cluster.HeartbeatMs, HeartbeatMisses: d.Cluster.HeartbeatMisses, RaftTickMs: d.Cluster.Raft.TickMs,
		TTL:               time.Duration(d.Cluster.Leases.TTLMs) * time.Millisecond,
		TakeoverTimeoutMs: d.Cluster.Leases.TakeoverTimeoutMs, AckTimeoutMs: d.Cluster.Replication.AckTimeoutMs,
		JoinTimeoutMs: d.Cluster.Raft.JoinTimeoutMs, ConnectTimeoutSec: d.Limits.ConnectTimeout,
		ReadinessStableMs: d.Cluster.Readiness.StableMs, DrainRatePerSecond: d.Cluster.Drain.RatePerSecond,
		DrainTimeoutMs: d.Cluster.Drain.TimeoutMs, DrainConfirmTimeoutMs: d.Cluster.Drain.ConfirmTimeoutMs,
	}
	if got := timingDefaults(TimingsProduct); got != want {
		t.Errorf("product set %+v, want config.Default's %+v", got, want)
	}
	if want.TakeoverTimeoutMs != 5000 || want.AckTimeoutMs != 2000 {
		t.Errorf("config.Default's takeover and ack timeouts are %d and %d ms, not the 5000 and 2000 this test was written against: re-read what the product set now runs", want.TakeoverTimeoutMs, want.AckTimeoutMs)
	}
	harness := clusterTimings{
		HeartbeatMs: 100, HeartbeatMisses: 3, RaftTickMs: 10, TTL: 2 * time.Second,
		TakeoverTimeoutMs: 1000, AckTimeoutMs: 1000, JoinTimeoutMs: 1000, ConnectTimeoutSec: 5,
		ReadinessStableMs: 0, DrainRatePerSecond: 1000, DrainTimeoutMs: 3000, DrainConfirmTimeoutMs: 2000,
	}
	for _, set := range []TimingSet{TimingsHarness, ""} {
		if got := timingDefaults(set); got != harness {
			t.Errorf("%q: %+v, want the harness's own %+v", set, got, harness)
		}
	}
}

// intFields lists every integer field of a configuration document by its
// JSON path, so a timing added to the document later is compared too.
func intFields(v reflect.Value, path string, out map[string]int64) {
	switch v.Kind() {
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			f := v.Type().Field(i)
			name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
			if name == "" || name == "-" {
				name = f.Name
			}
			intFields(v.Field(i), path+"/"+name, out)
		}
	case reflect.Pointer:
		if !v.IsNil() {
			intFields(v.Elem(), path, out)
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		out[path] = v.Int()
	}
}

// harnessCluster builds a cluster record at a set without starting it, with
// one node whose configuration can be read.
func harnessCluster(t *testing.T, set TimingSet) (*Cluster, *Node) {
	dir := t.TempDir()
	secret := filepath.Join(dir, "secret")
	if err := os.WriteFile(secret, []byte("simulation-cluster-shared-secret-bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := &Cluster{t: t, o: Options{Nodes: 1, TTL: timingDefaults(set).TTL, Replicas: 2, Shards: 16, Timings: set}, secret: secret}
	n := &Node{Name: "n1", Dir: filepath.Join(dir, "n1"), TCP: "127.0.0.1:1883", Cluster: "127.0.0.1:7946", Obs: "127.0.0.1:9090", c: c}
	c.Nodes = []*Node{n}
	return c, n
}

// notTimings are the integer fields a product cluster's configuration may
// differ from config.Default in, each with its reason.
var notTimings = map[string]string{
	"/cluster/shards": "the harness runs 16 shards; not a timing (WI 1475)",
}

// TestProductClusterRunsEveryDefaultTiming (WI 1805): a product cluster's
// node is configured with config.Default's value for every integer field of
// the document but the named non-timings, so no timing the broker reads can
// run at a harness value under the product's label.
func TestProductClusterRunsEveryDefaultTiming(t *testing.T) {
	c, n := harnessCluster(t, TimingsProduct)
	got, want := map[string]int64{}, map[string]int64{}
	intFields(reflect.ValueOf(c.config(n)), "", got)
	intFields(reflect.ValueOf(config.Default()), "", want)
	if len(want) < 30 {
		t.Fatalf("the walk found %d integer fields in config.Default; it is not reading the document", len(want))
	}
	for path, w := range want {
		if _, skip := notTimings[path]; skip {
			continue
		}
		if got[path] != w {
			t.Errorf("%s = %d in a product cluster's node, config.Default has %d", path, got[path], w)
		}
	}
}

// TestHarnessClusterKeepsItsTimings: the gate and the corpus run the
// harness's values, so their duration does not change (WI 1805).
func TestHarnessClusterKeepsItsTimings(t *testing.T) {
	c, n := harnessCluster(t, TimingsHarness)
	doc := c.config(n)
	got := map[string]int{
		"heartbeat": doc.Cluster.HeartbeatMs, "misses": doc.Cluster.HeartbeatMisses, "tick": doc.Cluster.Raft.TickMs,
		"ttl": doc.Cluster.Leases.TTLMs, "takeover": doc.Cluster.Leases.TakeoverTimeoutMs, "ack": doc.Cluster.Replication.AckTimeoutMs,
		"join": doc.Cluster.Raft.JoinTimeoutMs, "connect": doc.Limits.ConnectTimeout, "stable": doc.Cluster.Readiness.StableMs,
		"drain rate": doc.Cluster.Drain.RatePerSecond, "drain timeout": doc.Cluster.Drain.TimeoutMs, "drain confirm": doc.Cluster.Drain.ConfirmTimeoutMs,
	}
	want := map[string]int{
		"heartbeat": 100, "misses": 3, "tick": 10, "ttl": 2000, "takeover": 1000, "ack": 1000,
		"join": 1000, "connect": 5, "stable": 0, "drain rate": 1000, "drain timeout": 3000, "drain confirm": 2000,
	}
	for k, w := range want {
		if got[k] != w {
			t.Errorf("harness %s = %d, want %d", k, got[k], w)
		}
	}
	if c.o.DrainConfirmTimeout = 7 * time.Second; c.config(n).Cluster.Drain.ConfirmTimeoutMs != 7000 {
		t.Errorf("a scenario's drain confirmation must still win")
	}
}

// TestTimingLabelNamesOverrides: a run is labelled with its set, and a knob
// moved away from the set says so, so no run claims a set it did not run.
func TestTimingLabelNamesOverrides(t *testing.T) {
	product := effectiveOf(timingDefaults(TimingsProduct))
	harness := effectiveOf(timingDefaults(TimingsHarness))
	moved := func(e Effective, f func(*Effective)) Effective { f(&e); return e }
	harnessTTL30 := effectiveOf(setTimings(TimingsHarness, 30*time.Second))
	cases := []struct {
		set  TimingSet
		e    Effective
		want string
	}{
		{TimingsProduct, product, "product"},
		{TimingsHarness, harness, "harness (stress)"},
		{"", harness, "harness (stress)"},
		{TimingsHarness, harnessTTL30, "harness (stress), overridden: ttl 30000 ms"},
		{TimingsProduct, moved(product, func(e *Effective) { e.HeartbeatMs = 500 }), "product, overridden: heartbeat 500 ms"},
		{TimingsProduct, moved(product, func(e *Effective) { e.TakeoverTimeoutMs, e.AckTimeoutMs = 1000, 1000 }), "product, overridden: takeover 1000 ms, ack 1000 ms"},
		{TimingsProduct, moved(product, func(e *Effective) { e.ConnectTimeoutSec, e.JoinTimeoutMs, e.ReadinessStableMs = 5, 1000, 0 }), "product, overridden: join 1000 ms, connect 5 s, readiness stable 0 ms"},
		{TimingsProduct, moved(product, func(e *Effective) { e.DrainRatePerSecond, e.DrainTimeoutMs, e.DrainConfirmTimeoutMs = 1000, 3000, 2000 }), "product, overridden: drain rate 1000/s, drain timeout 3000 ms, drain confirm 2000 ms"},
		{TimingsHarness, moved(harness, func(e *Effective) { e.AckTimeoutMs = 500 }), "harness (stress), overridden: ack 500 ms"},
	}
	for _, tc := range cases {
		if got := timingLabel(tc.set, tc.e); got != tc.want {
			t.Errorf("timingLabel(%q, %+v) = %q, want %q", tc.set, tc.e, got, tc.want)
		}
	}
}

// effectiveOf is the record a set's timings give.
func effectiveOf(tm clusterTimings) Effective {
	return Effective{
		HeartbeatMs: tm.HeartbeatMs, HeartbeatMisses: tm.HeartbeatMisses, RaftTickMs: tm.RaftTickMs,
		LeaseTTLMs: int(tm.TTL / time.Millisecond), TakeoverTimeoutMs: tm.TakeoverTimeoutMs, AckTimeoutMs: tm.AckTimeoutMs,
		JoinTimeoutMs: tm.JoinTimeoutMs, ConnectTimeoutSec: tm.ConnectTimeoutSec, ReadinessStableMs: tm.ReadinessStableMs,
		DrainRatePerSecond: tm.DrainRatePerSecond, DrainTimeoutMs: tm.DrainTimeoutMs, DrainConfirmTimeoutMs: tm.DrainConfirmTimeoutMs,
	}
}

// TestEffectiveCarriesTheLabelAndTheSetsValues: a product-timing cluster's
// record says product and holds the product's values, through the same
// resolution its nodes use, and the takeover and ack variables reach both.
func TestEffectiveCarriesTheLabelAndTheSetsValues(t *testing.T) {
	c, n := harnessCluster(t, TimingsProduct)
	e := c.effective(100)
	if e.TimingSet != "product" || e.HeartbeatMs != 1000 || e.RaftTickMs != 100 ||
		e.TakeoverTimeoutMs != 5000 || e.AckTimeoutMs != 2000 || e.JoinTimeoutMs != 30000 || e.ConnectTimeoutSec != 10 ||
		e.ReadinessStableMs != 5000 || e.DrainRatePerSecond != 100 || e.DrainTimeoutMs != 30000 || e.DrainConfirmTimeoutMs != 30000 ||
		e.Shards != 16 {
		t.Fatalf("product cluster: %+v", e)
	}
	t.Setenv("HOARDMQ_SIM_HEARTBEAT_MS", "500")
	if e := c.effective(100); e.TimingSet != "product, overridden: heartbeat 500 ms" {
		t.Fatalf("with a heartbeat override: %q", e.TimingSet)
	}
	t.Setenv("HOARDMQ_SIM_HEARTBEAT_MS", "")
	t.Setenv("HOARDMQ_SIM_TAKEOVER_TIMEOUT_MS", "1000")
	t.Setenv("HOARDMQ_SIM_ACK_TIMEOUT_MS", "1000")
	e = c.effective(100)
	if e.TimingSet != "product, overridden: takeover 1000 ms, ack 1000 ms" {
		t.Fatalf("with the harness's takeover and ack: %q", e.TimingSet)
	}
	doc := c.config(n)
	if doc.Cluster.Leases.TakeoverTimeoutMs != e.TakeoverTimeoutMs || doc.Cluster.Replication.AckTimeoutMs != e.AckTimeoutMs {
		t.Fatalf("the record says %d and %d ms, the node was given %d and %d", e.TakeoverTimeoutMs, e.AckTimeoutMs,
			doc.Cluster.Leases.TakeoverTimeoutMs, doc.Cluster.Replication.AckTimeoutMs)
	}
}

func TestRepeatSummaryReportsTheTimingSets(t *testing.T) {
	runs := []repeatRun{
		{Index: 1, RecoverySec: 60, TimingSet: "product"},
		{Index: 2, RecoverySec: -1, TimingSet: "product"},
	}
	s := summarise(runs)
	if len(s.TimingSets) != 1 || s.TimingSets[0] != "product" || !strings.Contains(s.String(), "timings: product") {
		t.Fatalf("one set: %+v, %q", s.TimingSets, s.String())
	}
	runs = append(runs, repeatRun{Index: 3, RecoverySec: 90, TimingSet: "harness (stress)"})
	if s := summarise(runs); len(s.TimingSets) != 2 {
		t.Fatalf("a mixed batch must show both sets: %+v", s.TimingSets)
	}
}

// TestSettleWaitsOutAColdStart: at the product's timings a fresh cluster's
// designated node bootstraps only after the join window, and readiness then
// wants the topology stable, so New's settle bound must cover both or every
// product-timing run fails before its fill.
func TestSettleWaitsOutAColdStart(t *testing.T) {
	for _, set := range []TimingSet{TimingsProduct, TimingsHarness} {
		tm := timingDefaults(set)
		floor := time.Duration(tm.JoinTimeoutMs+tm.ReadinessStableMs)*time.Millisecond + 20*time.Second
		if got := tm.settleBudget(); got < floor {
			t.Errorf("%s: settle bound %s, but the join window and readiness alone take %s", set, got, floor-20*time.Second)
		}
	}
	if got := timingDefaults(TimingsHarness).settleBudget(); got > 32*time.Second {
		t.Errorf("the harness's settle bound grew to %s; the gate's start should not wait longer than before", got)
	}
}
