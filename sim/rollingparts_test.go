package sim

import (
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/module"
)

// R1: outcomes are counted in the step whose drain began last before them;
// before the first drain and from the roll's end on they are in no step.
func TestStepWindowsCountByStep(t *testing.T) {
	t0 := time.Now()
	at := func(s float64) time.Time { return t0.Add(time.Duration(s * float64(time.Second))) }
	starts := []time.Time{at(10), at(20), at(30)}
	end := at(40)
	outcomes := []probeOutcome{
		{at: at(5)},                // before the roll
		{at: at(10)},               // step 0, at its start
		{at: at(15), code: "0x83"}, // step 0
		{at: at(19.9), group: true, failed: true}, // step 0
		{at: at(20), group: true},                 // step 1
		{at: at(25), group: true, code: "0x83"},   // step 1
		{at: at(39.9), failed: true},              // step 2
		{at: at(40), code: "0x83"},                // after the roll
		{at: at(45), group: true, code: "0x83"},   // after the roll
	}
	steps, roll := stepWindows(starts, end, outcomes)
	if len(steps) != 3 {
		t.Fatalf("%d steps", len(steps))
	}
	if s := steps[0]; s.Attempts != 2 || s.Refused["0x83"] != 1 || s.Groups != 1 || s.GroupsFailed != 1 {
		t.Errorf("step 0: %+v", s)
	}
	if s := steps[1]; s.Attempts != 0 || s.Groups != 2 || s.GroupsRefused["0x83"] != 1 || s.Rate != 0.5 {
		t.Errorf("step 1: %+v", s)
	}
	if s := steps[2]; s.Attempts != 1 || s.Failed != 1 || s.Groups != 0 || s.Rate != 1 {
		t.Errorf("step 2: %+v", s)
	}
	if roll.Attempts+roll.Groups != 6 || roll.refused() != 4 {
		t.Errorf("roll: %d attempts, %d refused or failed; want 6 and 4", roll.Attempts+roll.Groups, roll.refused())
	}
	if _, roll := stepWindows(nil, end, outcomes); roll.Attempts+roll.Groups != 0 {
		t.Errorf("no step, and the roll counted %+v", roll)
	}
}

// cleanRoll is a record with nothing to report.
func cleanRoll() rollRecord {
	step := func(node string) rollStep {
		return rollStep{Node: node, DrainCompleted: true, Ready: true, DrainSec: 30,
			Status:        module.DrainStatus{Phase: "complete", Complete: true, SessionsConfirmed: 10},
			DrainCounters: map[string]float64{`hoardmq_drain_placement_total{outcome="left"}`: 1}}
	}
	return rollRecord{DrainLimitS: 1800, ChartGraceS: rollChartGraceSec, ReferenceRate: rollReferenceRate,
		Steps:      []rollStep{step("n3"), step("n2"), step("n1")},
		Probe:      &probeRecord{Attempts: 100, Acked: 99, Groups: &groupsRecord{}},
		Population: rollPopulation{OnlineAtEnd: 300, Target: 300},
		Refusals:   windowCounts{Rate: 0.01}}
}

// R2: each finding appears for a record that has it and not for one that
// does not.
func TestRollFindings(t *testing.T) {
	if f := rollFindings(cleanRoll()); len(f) != 0 {
		t.Fatalf("a clean roll reports %v", f)
	}
	cases := []struct {
		name   string
		break_ func(*rollRecord)
		want   string
	}{
		{"drain not completed", func(r *rollRecord) { r.Steps[0].DrainCompleted = false }, "n3: the drain did not complete within 1800 s"},
		{"unconfirmed", func(r *rollRecord) { r.Steps[1].Status.SessionsUnconfirmed = 4 }, "n2: the drain ended with 4 sessions unconfirmed"},
		{"placement", func(r *rollRecord) {
			r.Steps[2].DrainCounters[`hoardmq_drain_placement_total{outcome="copies-expired"}`] = 1
		}, `n1: drain placement hoardmq_drain_placement_total{outcome="copies-expired"}`},
		{"beyond grace", func(r *rollRecord) { r.Steps[0].BeyondChartGrace, r.Steps[0].DrainSec = true, 333.3 }, "n3: the drain took 333.3 s, beyond the chart's 75 s termination grace"},
		{"not ready", func(r *rollRecord) { r.Steps[1].Ready = false }, "n2: not ready again after its restart"},
		{"restart error", func(r *rollRecord) { r.Steps[1].RestartErr = "bind: address in use" }, "n2: not ready again after its restart bind: address in use"},
		{"no probe", func(r *rollRecord) { r.Probe, r.ProbeMissing = nil, "build failed" }, "no probe: build failed"},
		{"pair loss", func(r *rollRecord) { r.Probe.AckedNotDelivered = 1 }, "probe: 1 acknowledged messages lost or QoS 2 group messages duplicated"},
		{"group loss", func(r *rollRecord) { r.Probe.Groups.AckedNotDelivered = 2 }, "probe: 2 acknowledged messages lost"},
		{"group duplicate", func(r *rollRecord) { r.Probe.Groups.QoS2Duplicates = 1 }, "probe: 1 acknowledged messages lost or QoS 2 group messages duplicated"},
		{"session lost", func(r *rollRecord) { r.Population.SessionLost, r.Population.SessionLostClients = 3, 2 }, "population: 3 sessions rebuilt empty across 2 clients"},
		{"not online", func(r *rollRecord) { r.Population.OnlineAtEnd = 299 }, "population: 299 of 300 online at the end"},
		{"rate", func(r *rollRecord) { r.Refusals.Rate = 0.05 }, "probe: 5.00% refused across the roll, above the chart runbook's 3.83%"},
		{"cut", func(r *rollRecord) {
			r.CutS = 75
			r.Steps[2].DrainCompleted, r.Steps[2].Cut = false, true
			r.Steps[2].CutStatus = &module.DrainStatus{Phase: "redirecting-clients", ClientsRemaining: 40, LeasesRemaining: 7, SessionsAwaiting: 2}
		}, "n1: the drain was cut at 75 s in phase redirecting-clients, with 40 clients and 7 leases left and 2 sessions awaiting confirmation"},
		{"cut while redirecting", func(r *rollRecord) {
			r.CutS = 75
			r.Steps[2].DrainCompleted, r.Steps[2].Cut, r.Steps[2].CutLeases = false, true, 120
			r.Steps[2].CutStatus = &module.DrainStatus{Phase: "redirecting-clients", ClientsRemaining: 40}
		}, "n1: the drain was cut at 75 s in phase redirecting-clients, with 40 clients and 120 leases left and 0 sessions awaiting confirmation"},
		{"served empty", func(r *rollRecord) {
			r.Steps[1].Empty = map[string]float64{`hoardmq_sessions_rebuilt_empty_total{reason="adoption"}`: 2, `hoardmq_sessions_rebuilt_empty_total{reason="takeover"}`: 1}
		}, "n2: 3 sessions served empty during the step"},
		// A session with no expiry ended with its connection: recorded, not a
		// finding, and not added to one.
		{"served empty beside no expiry", func(r *rollRecord) {
			r.Steps[1].Empty = map[string]float64{`hoardmq_sessions_rebuilt_empty_total{reason="no-expiry"}`: 4, `hoardmq_sessions_rebuilt_empty_total{reason="raced"}`: 1}
		}, "n2: 1 sessions served empty during the step"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := cleanRoll()
			tc.break_(&r)
			got := rollFindings(r)
			if len(got) != 1 || !strings.HasPrefix(got[0], tc.want) {
				t.Fatalf("findings %q, want one starting %q", got, tc.want)
			}
		})
	}
	r := cleanRoll()
	r.Steps[1].Empty = map[string]float64{`hoardmq_sessions_rebuilt_empty_total{reason="no-expiry"}`: 2}
	if f := rollFindings(r); len(f) != 0 {
		t.Fatalf("sessions with no expiry served empty are reported: %v", f)
	}
	r = cleanRoll()
	r.Refusals.Rate = rollReferenceRate
	if f := rollFindings(r); len(f) != 0 {
		t.Fatalf("a rate at the reference is not above it: %v", f)
	}
}

// R3: the grace, the order, the drain bound's variable, the moved copies per
// map change and what rose.
func TestRollParts(t *testing.T) {
	if beyondChartGrace(75) || !beyondChartGrace(75.1) {
		t.Errorf("the chart's grace is 75 s: 75 beyond %v, 75.1 beyond %v", beyondChartGrace(75), beyondChartGrace(75.1))
	}
	if got := strings.Join(rollOrder([]string{"n1", "n2", "n3"}), ","); got != "n3,n2,n1" {
		t.Errorf("order %s", got)
	}
	env := func(v string) func(string) string { return func(string) string { return v } }
	for v, want := range map[string]time.Duration{"": rollDrainLimitDflt, "60": time.Minute, "x": rollDrainLimitDflt, "0": rollDrainLimitDflt} {
		if got := rollDrainLimit(env(v)); got != want {
			t.Errorf("drain limit %q: %s, want %s", v, got, want)
		}
	}
	for v, want := range map[string]time.Duration{"": 0, "75": 75 * time.Second, "x": 0, "0": 0, "-5": 0} {
		if got := rollCutAfter(env(v)); got != want {
			t.Errorf("cut %q: %s, want %s", v, got, want)
		}
	}
	maps := []mapSeen{{Node: "n1", Version: 4, AtSec: 0.1}, {Node: "n2", Version: 4, AtSec: 0.1},
		{Node: "n2", Version: 5, AtSec: 4.6, Basis: []string{"n1", "n2", "n3"}}, {Node: "n1", Version: 5, AtSec: 4.5, Basis: []string{"n1", "n2", "n3"}},
		{Node: "n1", Version: 6, AtSec: 80, Basis: []string{"n2", "n3"}}}
	kept := []keptSample{{0.1, 7}, {4.5, 0}, {5, 30}, {9, 2}, {9.5, 0}, {79.9, 0}, {80.5, 40}, {84, 1}}
	peak, lags := mapChangeLags(maps, kept)
	if peak != 40 || len(lags) != 2 {
		t.Fatalf("peak %d, %d changes; want 40 and 2: %+v", peak, len(lags), lags)
	}
	if l := lags[0]; l.Version != 5 || l.AtSec != 4.5 || l.KeptPeak != 30 || l.ClearSec != 4.5 || !l.Cleared {
		t.Errorf("v5: %+v, want at 4.5, peak 30, last kept 4.5 s after, cleared", l)
	}
	if l := lags[1]; l.Version != 6 || l.KeptPeak != 40 || l.ClearSec != 4 || l.Cleared || len(l.Basis) != 2 {
		t.Errorf("v6: %+v, want peak 40, last kept 4 s after, not cleared, basis n2 n3", l)
	}
	if _, lags := mapChangeLags(maps[:2], kept); len(lags) != 0 {
		t.Errorf("no change, and %+v", lags)
	}
	got := risen(map[string]float64{"a{x=\"1\"}": 2, "b": 5}, map[string]float64{"a{x=\"1\"}": 5, "b": 4, "c": 1},
		func(name string) bool { return name != "c" })
	if len(got) != 1 || got[`a{x="1"}`] != 3 {
		t.Errorf("risen %v, want only a +3", got)
	}
	snap := func(d float64) []module.Metric {
		return []module.Metric{
			{Name: "hoardmq_forward_failures_total", Labels: map[string]string{"reason": "r"}, Value: 1 + d},
			{Name: "hoardmq_replicas_dropped_total", Labels: map[string]string{"reason": "not-a-target-confirmed"}, Value: 2 + d},
			{Name: "hoardmq_replicas_kept_total", Labels: map[string]string{"reason": "moved-target"}, Value: 3 + d},
			{Name: "hoardmq_replicas_unwritten_checks_total", Labels: map[string]string{"outcome": "held"}, Value: 4 + d},
			{Name: "hoardmq_drain_seconds_total", Value: 5 + d},
			{Name: "hoardmq_sessions_rebuilt_empty_total", Labels: map[string]string{"reason": "adoption"}, Value: 7 + d},
			{Name: "hoardmq_publishes_total", Value: 6 + d},
		}
	}
	before := map[string]map[string]float64{"n1": pickRollMetrics(snap(0)), "n2": pickRollMetrics(snap(0))}
	after := map[string]map[string]float64{"n1": pickRollMetrics(snap(1))}
	drained := map[string]map[string]float64{"n2": pickRollMetrics(snap(2))}
	if _, ok := before["n1"]["hoardmq_publishes_total"]; ok {
		t.Errorf("picked a family the step does not read: %v", before["n1"])
	}
	fails := stepRise(before, after, drained, "n2", rollFailureMetric)
	copies := stepRise(before, after, drained, "n2", rollCopyMetric)
	if len(fails) != 1 || fails[`hoardmq_forward_failures_total{reason="r"}`] != 3 {
		t.Errorf("failures %v, want the forward failures only, 1 on n1 and 2 on the drained n2", fails)
	}
	if empty := stepRise(before, after, drained, "n2", rollEmptyMetric); len(empty) != 1 || empty[`hoardmq_sessions_rebuilt_empty_total{reason="adoption"}`] != 3 {
		t.Errorf("served empty %v, want the empty sessions only, 3", empty)
	}
	if len(copies) != 3 || copies[`hoardmq_replicas_dropped_total{reason="not-a-target-confirmed"}`] != 3 ||
		copies[`hoardmq_replicas_kept_total{reason="moved-target"}`] != 3 || copies[`hoardmq_replicas_unwritten_checks_total{outcome="held"}`] != 3 {
		t.Errorf("copies %v, want the three replica families, 3 each", copies)
	}
}
