package sim

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// reconcileBody is a node's route reconciler series as /metrics writes them.
const reconcileBody = `# TYPE hoardmq_route_reconcile_passes_total counter
hoardmq_route_reconcile_passes_total 14
hoardmq_route_reconcile_suspects 2
hoardmq_route_reconcile_corrections_total{direction="withdraw",kind="dispatched-unheld"} 3
hoardmq_route_reconcile_corrections_total{direction="announce",kind="unadvertised"} 1
hoardmq_route_reconcile_refused_total{kind="dispatched-unheld",reason="connected"} 2
hoardmq_route_reconcile_skipped_total{reason="moving"} 5
hoardmq_route_reconcile_skipped_total{reason="held"} 1
hoardmq_route_reconcile_step_seconds_count 900
`

func TestParseReconcile(t *testing.T) {
	f := parseReconcile(reconcileBody)
	want := &reconcileFigures{Passes: 14, Suspects: 2,
		Corrections: map[string]float64{"withdraw dispatched-unheld": 3, "announce unadvertised": 1},
		Refused:     map[string]float64{"dispatched-unheld connected": 2},
		Skipped:     map[string]float64{"moving": 5, "held": 1}}
	if !reflect.DeepEqual(f, want) {
		t.Fatalf("read %+v, want %+v", f, want)
	}
	if f := parseReconcile("hoardmq_sessions_active 3\n"); f != nil {
		t.Fatalf("a node without the reconciler read %+v", f)
	}
}

func TestParseFetches(t *testing.T) {
	body := "hoardmq_replication_fetches_total{outcome=\"local\"} 900\nhoardmq_replication_fetches_total{outcome=\"none-budget-spent\"} 2\nhoardmq_replication_fetch_request_seconds_count 5\n"
	if got, want := parseFetches(body), map[string]float64{"local": 900, "none-budget-spent": 2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("read %v, want %v", got, want)
	}
}

// TestCheckSessions: a million-client run's survivors, 1,000,200 sessions and
// 999,964 subscriptions with 100 probe publishers and 10 controls, hold 126
// hollow sessions; a node without figures leaves the check undecided.
func TestCheckSessions(t *testing.T) {
	rs := []lockReading{
		{Node: "n1", Routes: &routeFigures{Sessions: 524_405, Subscriptions: 524_287}},
		{Node: "n2", Routes: &routeFigures{}},
		{Node: "n3", Routes: &routeFigures{Sessions: 475_795, Subscriptions: 475_677}},
	}
	c, ok := checkSessions(rs, 110, "100 probe publishers, 10 negative controls")
	if !ok || c.Sessions != 1_000_200 || c.Subscriptions != 999_964 || c.Hollow != 126 || c.PerNode["n3"] != [2]float64{475_795, 475_677} {
		t.Fatalf("check %+v ok %v: want 126 hollow of 1,000,200 sessions and 999,964 subscriptions", c, ok)
	}
	if _, ok := checkSessions(append(rs, lockReading{Node: "n4"}), 110, ""); ok {
		t.Fatal("a node without figures decided the check")
	}
	if _, ok := checkSessions(nil, 0, ""); ok {
		t.Fatal("no reading decided the check")
	}
}

// TestReconcileFindings: a node whose reconciler corrected anything by the
// last reading at the point is named with its corrections; one that made
// none, or was read only elsewhere, is not.
func TestReconcileFindings(t *testing.T) {
	f := parseReconcile(reconcileBody)
	none := &reconcileFigures{Passes: 3, Corrections: map[string]float64{}}
	rs := []lockReading{
		{Point: "census", Node: "n1", Reconcile: none},
		{Point: "census", Node: "n1", Reconcile: f},
		{Point: "census", Node: "n3", Reconcile: none},
		{Point: "end", Node: "n2", Reconcile: f},
	}
	got := reconcileFindings(rs, "census")
	if want := []string{"n1's route reconciler made 4 corrections (announce unadvertised 1, withdraw dispatched-unheld 3)"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("findings %q, want %q", got, want)
	}
}

// hist is a lock histogram with cumulative counts at the bounds 0.262144,
// 0.524288, 1.048576, 2.097152 and +Inf.
func hist(counts ...float64) *histSeries {
	return &histSeries{Le: []string{"0.262144", "0.524288", "1.048576", "2.097152", "+Inf"}, Counts: counts, Count: counts[len(counts)-1]}
}

// TestHoldIntervals: four readings of a survivor make three intervals; the
// one holding a checkpoint hold of 1.05–2.1 s, a long fetch wait, a session
// served empty and a fetch whose budget was spent says so, the others do
// not; a reading after a restart is not compared with the one before it.
func TestHoldIntervals(t *testing.T) {
	const crash = 1_000_000
	at := func(point string, sec float64, cp, find *histSeries, empty, spent float64) lockReading {
		return lockReading{Point: point, Node: "n3", AtMs: crash + int64(sec*1000), EmptyServes: empty,
			Hist:    map[string]*histSeries{holdFamily + "checkpoint": cp, findWaits: find},
			Fetches: map[string]float64{"local": 10 * sec, "none-budget-spent": spent}}
	}
	rs := []lockReading{
		at("storm-end", 40, hist(2, 3, 3, 5, 5), hist(80, 80, 81, 83, 85), 1, 1),
		at("storm-start", -5, hist(2, 3, 3, 3, 3), hist(50, 50, 50, 50, 50), 0, 0),
		at("storm-tick", 10, hist(2, 3, 3, 3, 3), hist(60, 60, 60, 60, 60), 0, 0),
		{Point: "storm-tick", Node: "n3", AtMs: crash + 15000, Error: "refused"},
		at("storm-tick", 20, hist(2, 3, 3, 5, 5), hist(70, 70, 71, 73, 73), 1, 1),
		at("census", 300, hist(9, 9, 9, 9, 9), hist(1, 1, 1, 1, 1), 1, 1),
	}
	got := holdIntervals(rs, crash)
	if len(got) != 3 {
		t.Fatalf("%d intervals %+v, want 3", len(got), got)
	}
	first, second, third := got[0], got[1], got[2]
	if first.FromSec != -5 || first.ToSec != 10 || first.notable() {
		t.Errorf("the first interval %+v: want -5 to 10 s and nothing notable", first)
	}
	want := holdInterval{Node: "n3", FromSec: 10, ToSec: 20, LongHolds: map[string]float64{"checkpoint": 2}, LongestBucketSec: 2.097152,
		LongFindWaits: 3, EmptyServes: 1, Fetches: map[string]float64{"local": 100, "none-budget-spent": 1}}
	if !reflect.DeepEqual(second, want) || !second.notable() {
		t.Errorf("the second interval %+v, want %+v", second, want)
	}
	if third.EmptyServes != 0 || third.Fetches["none-budget-spent"] != 0 || third.LongFindWaits != 2 {
		t.Errorf("the third interval %+v: want only its own two long waits", third)
	}
	// After the restart the node counts one long hold and one long wait of
	// its own, fewer samples than before it in all.
	restart := []lockReading{at("storm-start", 0, hist(3, 3, 3, 3, 3), hist(9, 9, 9, 9, 9), 0, 0), at("storm-end", 9, hist(0, 0, 0, 1, 1), hist(0, 0, 0, 1, 1), 0, 0)}
	if got := holdIntervals(restart, crash); len(got) != 1 || len(got[0].LongHolds) != 0 || got[0].LongFindWaits != 0 {
		t.Errorf("across a restart: %+v, want no hold compared", got)
	}
}

// TestStormTicks reads two fake nodes' counters through a short storm window
// while the victim is never read, and stop, called twice, records them once.
func TestStormTicks(t *testing.T) {
	every := stormTickEvery
	stormTickEvery = 30 * time.Millisecond
	t.Cleanup(func() { stormTickEvery = every })
	body := func(int) string { return censusBody(9, 9) + "hoardmq_replication_fetches_total{outcome=\"local\"} 4\n" }
	r := censusRun(t, censusNode(t, "n1", body), censusNode(t, "n2", body), censusNode(t, "n3", body))
	r.cfg.Victim = "n2"
	stop := r.stormTicks(context.Background())
	time.Sleep(200 * time.Millisecond)
	stop()
	stop()
	n := len(r.rec.LockFetch)
	nodes := map[string]int{}
	for _, rd := range r.rec.LockFetch {
		if rd.Point != "storm-tick" || rd.Fetches["local"] != 4 {
			t.Fatalf("a reading %+v: want a storm tick with the node's fetches", rd)
		}
		nodes[rd.Node]++
	}
	if n < 4 || nodes["n2"] != 0 || nodes["n1"] < 2 || nodes["n3"] < 2 {
		t.Fatalf("%d readings by node %v: want at least two each of n1 and n3 and none of the victim n2", n, nodes)
	}
	time.Sleep(100 * time.Millisecond)
	if len(r.rec.LockFetch) != n {
		t.Fatalf("readings went on after stop: %d, then %d", n, len(r.rec.LockFetch))
	}
}

// TestReadLocksOfRecordsCounters: a reading carries the fetch outcomes, the
// reconciler's counters, the sessions and the scrape's duration.
func TestReadLocksOfRecordsCounters(t *testing.T) {
	n := censusNode(t, "n1", func(int) string {
		return censusBody(9, 9) + reconcileBody + "hoardmq_replication_fetches_total{outcome=\"none\"} 1\n"
	})
	rs := readLocksOf(context.Background(), "census", []scaleNode{n})
	if len(rs) != 1 {
		t.Fatalf("%d readings", len(rs))
	}
	rd := rs[0]
	if rd.Error != "" || rd.Fetches["none"] != 1 || rd.Reconcile == nil || rd.Reconcile.Passes != 14 || rd.Routes == nil || rd.Routes.Sessions != 9 || rd.TookMs < 0 {
		t.Fatalf("reading %+v (reconcile %+v, routes %+v): want the node's fetches, reconciler, sessions and the scrape's time", rd, rd.Reconcile, rd.Routes)
	}
}

// TestScaleCensusWholeSessions: a node holding sessions without their
// subscription is a finding the census records, not a failure; one whose
// figures clear within the bound reads whole.
func TestScaleCensusWholeSessions(t *testing.T) {
	hollow := func(int) string { return censusBody(9, 9) + "hoardmq_sessions_active{namespace=\"default\"} 126\n" }
	r := censusRun(t, censusNode(t, "n1", hollow), censusNode(t, "n3", func(int) string { return censusBody(5, 5) }))
	r.census(context.Background())
	c := r.rec.Census
	if len(r.failed) != 0 || c.Sessions == nil || c.Sessions.Hollow != 126 || c.Readings < 2 {
		t.Fatalf("a lasting hollow count: failed %q, census %+v, sessions %+v; want 126 recorded over several readings, the run not failed", r.failed, c, c.Sessions)
	}
	if out := scaleReport(r.rec); !strings.Contains(out, "**Finding: hollow sessions**: 140 sessions, 14 subscriptions") {
		t.Errorf("the report lacks the finding:\n%s", out)
	}

	r = censusRun(t, censusNode(t, "n1", func(n int) string {
		if n == 1 {
			return hollow(n)
		}
		return censusBody(9, 9)
	}))
	r.census(context.Background())
	if c := r.rec.Census; c.Sessions == nil || c.Sessions.Hollow != 0 || c.Readings != 2 {
		t.Fatalf("a count that cleared: census %+v, sessions %+v; want whole at the second reading", c, c.Sessions)
	}
}

// TestSessionExempt: the probe's publishers and the delivery check's
// removed controls hold sessions without a subscription.
func TestSessionExempt(t *testing.T) {
	r := &scaleRun{rec: &scaleRecord{Delivery: &deliveryResult{ControlsRemoved: 10}}, pr: &probe{pubs: make([]*Client, 100)}}
	if n, why := r.sessionExempt(); n != 110 || why != "100 probe publishers, 10 negative controls" {
		t.Fatalf("exempt %d (%s), want 110", n, why)
	}
	if n, _ := (&scaleRun{rec: &scaleRecord{}}).sessionExempt(); n != 0 {
		t.Fatalf("no probe and no check: exempt %d", n)
	}
}

// TestScaleReportNewSections: the report carries the reconciler's table and
// finding, the fetch outcomes, the storm's notable intervals and the named
// clients' CONNACKs.
func TestScaleReportNewSections(t *testing.T) {
	f := parseReconcile(reconcileBody)
	sp := 44.25
	rec := &scaleRecord{
		LockFetch: []lockReading{
			{Point: "storm-start", Node: "n3", Fetches: map[string]float64{"local": 5}},
			{Point: "census", Node: "n1", Reconcile: f},
			{Point: "end", Node: "n1", Reconcile: f, Fetches: map[string]float64{"none-budget-spent": 2}},
		},
		Census: &scaleCensus{Readings: 1, Sessions: &sessionCheck{Sessions: 10, Subscriptions: 10}, Reconciler: reconcileFindings([]lockReading{{Point: "census", Node: "n1", Reconcile: f}}, "census")},
		Storm:  &scaleStorm{Intervals: []holdInterval{{Node: "n3", FromSec: 40, ToSec: 50, LongHolds: map[string]float64{"checkpoint": 1}, LongestBucketSec: 2.097152, EmptyServes: 1}, {Node: "n3", FromSec: 50, ToSec: 60}}},
		Connacks: &scaleConnacks{Asked: 1, LogLines: map[string]int{"n3": 1}, Clients: []clientConnacks{{ID: "f222667", Found: true, Seen: 2, Why: []string{"n3 served it empty (takeover)"},
			Last: []connackEntry{{NodeName: "n3", Code: "Success", SessionPresent: true, TookMs: 12, SinceCrashSec: &sp}}}}},
	}
	out := scaleReport(rec)
	for _, want := range []string{
		"## Route reconciler", "| census | n1 | 14 | 2 | 4 announce unadvertised 1, withdraw dispatched-unheld 3 | 2 dispatched-unheld connected 2 | 6 held 1, moving 5 |",
		"**Finding:** n1's route reconciler made 4 corrections",
		"Whole sessions: 10 sessions, 10 subscriptions",
		"## Replica fetches by outcome", "| end | n1 | none-budget-spent 2 |", "| storm-start | n3 | local 5 |",
		"## Long holds through the storm", "2 intervals", "1 with a lock hold", "| n3 | +40.0 | +50.0 | checkpoint 1 | 2.097 | 0 | 1 |",
		"## Named clients' CONNACKs", "f222667 (n3 served it empty (takeover)): 2 CONNACKs: +44.250 s n3 Success, Session Present true, 12 ms",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("the report lacks %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "+50.0 | +60.0") {
		t.Errorf("the report lists an interval with nothing notable:\n%s", out)
	}
}

// TestReadConnacks runs the teardown's read against an in-process cluster's
// clients through the driver: a client a node's log names and one the
// delivery check lost are each read from the driver, named by node and dated
// against the crash, with why the run named them.
func TestReadConnacks(t *testing.T) {
	const clients = 30
	c := New(t, Options{})
	d := startPipedDriver(t, c, clients, testDriverOptions())
	t.Cleanup(d.pop.close)
	if _, err := d.pop.hello(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var completed atomic.Int64
	if failed, err := d.pop.fill(ctx, &completed); err != nil || failed != 0 {
		t.Fatalf("fill: %d failed, %v", failed, err)
	}
	if err := d.pop.mark(); err != nil {
		t.Fatal(err)
	}
	crashAt := time.Now()
	if err := d.pop.crashed(); err != nil {
		t.Fatal(err)
	}
	if _, err := d.pop.finish(); err != nil {
		t.Fatal(err)
	}

	r := &scaleRun{pop: d.pop, procs: map[string]*nodeProc{}, logf: t.Logf,
		rec: &scaleRecord{Storm: &scaleStorm{CrashMs: crashAt.UnixMilli(), Summary: &d.pop.summary},
			Delivery: &deliveryResult{Lost: []deliveryClient{{ID: "f000002", Class: classLost, NodeName: "n3"}}}}}
	for i, addr := range c.Addresses() {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			t.Fatal(err)
		}
		p, _ := strconv.Atoi(port)
		n := scaleNode{Name: "n" + strconv.Itoa(i+1), Address: host, ClientPort: p}
		r.cfg.Layout.Nodes = append(r.cfg.Layout.Nodes, n)
		dir := t.TempDir()
		log := ""
		if n.Name == "n3" {
			log = strings.ReplaceAll(logLineServedEmpty, "f222667", "f000001") + "\n"
		}
		if err := os.WriteFile(filepath.Join(dir, "node.log"), []byte(log), 0o600); err != nil {
			t.Fatal(err)
		}
		r.procs[n.Name] = &nodeProc{node: n, sh: localShell{}, dir: dir}
	}
	r.readConnacks(ctx)
	got := r.rec.Connacks
	if got == nil || got.Asked != 2 || len(got.Clients) != 2 || len(got.Errors) != 0 || got.LogLines["n3"] != 1 || got.LogLines["n1"] != 0 {
		t.Fatalf("records %+v: want the two named clients and n3's one line", got)
	}
	for i, want := range []struct{ id, why string }{{"f000002", "delivery check: lost session"}, {"f000001", "n3 served it empty (takeover)"}} {
		cl := got.Clients[i]
		if cl.ID != want.id || !cl.Found || cl.Seen < 1 || len(cl.Last) < 1 || len(cl.Why) != 1 || !strings.Contains(cl.Why[0], want.why) {
			t.Fatalf("client %d %+v: want %s found, with its CONNACKs and why %q", i, cl, want.id, want.why)
		}
		e := cl.Last[len(cl.Last)-1]
		if e.NodeName == "" || e.SinceCrashSec == nil || *e.SinceCrashSec > 0 || e.Code != "Success" {
			t.Fatalf("%s's last CONNACK %+v: want a named node, accepted before the crash", cl.ID, e)
		}
	}
}
