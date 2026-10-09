package sim

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/broker"
	"github.com/happydave/hoardmq/internal/module"
)

// TestRollingRestart drains, stops and restarts each node in turn, highest
// name first, with the publish probe running (WI 2208). The gate runs 300
// clients at the harness's timings and checks the arm's structure;
// HOARDMQ_SIM_ROLL=<clients> runs the measurement at the product's timings
// and writes roll.json and report.md to HOARDMQ_SIM_ARTIFACT's directory. A
// measurement records and judges nothing: what went wrong is in findings.
func TestRollingRestart(t *testing.T) {
	clients, record, err := rollClients()
	if err != nil {
		t.Fatal(err)
	}
	if os.Getenv("HOARDMQ_SIM_REDUCED") != "" && !record {
		t.Skip("reduced mode: the rolling-restart gate runs without -race")
	}
	if used, ok := tcpSockets(); ok {
		limit := settleLimit(clients + probeClients(probePairs(), probeGroupCount(), 3))
		if used > limit {
			t.Fatalf("machine is not settled: %d TCP sockets in use or in TIME-WAIT (limit %d for %d clients)", used, limit, clients)
		}
	}
	artifactDir := ""
	if base := os.Getenv(artifactEnv); base != "" {
		artifactDir = filepath.Join(base, fmt.Sprintf("TestRollingRestart-%d-%s", clients, time.Now().Format("150405")))
		if err := os.MkdirAll(artifactDir, 0o750); err != nil {
			t.Fatalf("artifact: %v", err)
		}
	}
	progress := func(format string, a ...any) {
		line := fmt.Sprintf("[roll] "+format, a...)
		fmt.Fprintln(os.Stderr, line)
		if artifactDir != "" {
			if f, err := os.OpenFile(filepath.Join(artifactDir, "progress.log"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o640); err == nil { //nolint:gosec // operator-chosen dir
				_, _ = fmt.Fprintln(f, line)
				_ = f.Close()
			}
		}
	}
	set, err := failoverTimings(record, os.Getenv)
	if err != nil {
		t.Fatal(err)
	}
	opts := Options{TTL: timingDefaults(set).TTL, Timings: set, FileUsers: clients}
	if artifactDir != "" && os.Getenv("HOARDMQ_SIM_NODELOG") != "" {
		if f, err := os.Create(filepath.Join(artifactDir, "nodes.log")); err != nil { //nolint:gosec // operator-chosen dir
			t.Logf("artifact: %v", err)
		} else {
			opts.Log = f
			defer func() { _ = f.Close() }()
		}
	}
	c := New(t, opts)
	prov := readProvenance(os.Args, os.Environ(), ".")
	eff := c.effective(clients)
	sources := []string{"127.0.0.1"}
	if clients > 20000 {
		sources = nil
		for i := 1; i <= 8; i++ {
			sources = append(sources, fmt.Sprintf("127.0.0.%d", i))
		}
	}
	eff.ClientSources, eff.ClientDial = clientDial(sources)
	prov.Effective = &eff
	dialSources := sources
	if len(dialSources) <= 1 {
		dialSources = nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Hour)
	defer cancel()
	pop := newLocalPopulation(clients, c.Addresses(), dialSources, nil)
	var completed atomic.Int64
	start := time.Now()
	failed, err := pop.fill(ctx, &completed)
	if err != nil {
		t.Fatal(err)
	}
	defer pop.close()
	rec := rollRecord{Clients: clients, Failed: failed, FillTime: time.Since(start).Round(time.Millisecond).String(),
		TimingSet: string(set), HoldSec: int(holdSeconds(record) / time.Second), SampleMs: 100,
		DrainLimitS: int(rollDrainLimit(os.Getenv) / time.Second), ChartGraceS: rollChartGraceSec,
		CutS: int(rollCutAfter(os.Getenv) / time.Second), DrainRate: c.drainRate(),
		ReferenceRate: rollReferenceRate, StoreDir: c.Dir()}
	progress("%d clients connected and subscribed in %s (%d failed)", clients-failed, rec.FillTime, failed)

	var pr *probe
	if n := probePairs(); n > 0 {
		built, err := newProbe(ctx, n, probeGroupCount(), probeDefaultRate, c)
		if err != nil {
			rec.ProbeMissing = err.Error()
			built.close()
		} else {
			pr = built
			pr.keepOutcomes = true
			defer pr.close()
			pr.start(ctx)
		}
	} else {
		rec.ProbeMissing = "HOARDMQ_SIM_PROBE=0"
	}
	var rp *retainedProbe
	if n := rollRetainedTopics(os.Getenv, record); n > 0 {
		built, err := newRetainedProbe(ctx, n, probeDefaultRate, c)
		if err != nil {
			rec.RetainedMissing = err.Error()
			built.close()
		} else {
			rp = built
			defer rp.close()
			rp.start(ctx)
		}
	}
	if err := pop.mark(); err != nil {
		t.Fatal(err)
	}

	names := make([]string, len(c.Nodes))
	for i, n := range c.Nodes {
		names[i] = n.Name
	}
	rec.Order = rollOrder(names)
	rollStart := time.Now()
	sec := func(at time.Time) float64 { return at.Sub(rollStart).Seconds() }
	var starts []time.Time
	for _, name := range rec.Order {
		step, began := rollStepRun(t, c, c.Node(name), rec.DrainLimitS, rec.CutS, sec, progress)
		if !record {
			// At the harness's timings a step takes under a second, less
			// than the probe's period, so the gate holds each for a few.
			if d := rollGateMinStep - time.Since(began); d > 0 {
				time.Sleep(d)
			}
		}
		starts = append(starts, began)
		rec.Steps = append(rec.Steps, step)
		if !step.Ready {
			break
		}
	}
	rollEnd := time.Now()
	rec.RollSec = sec(rollEnd)
	progress("roll done in %.1f s; holding %d s", rec.RollSec, rec.HoldSec)
	time.Sleep(time.Duration(rec.HoldSec) * time.Second)
	if pr != nil {
		pr.mu.Lock()
		outcomes := append([]probeOutcome(nil), pr.outcomes...)
		pr.mu.Unlock()
		var steps []windowCounts
		steps, rec.Refusals = stepWindows(starts, rollEnd, outcomes)
		for i := range steps {
			rec.Steps[i].Probe = steps[i]
		}
		probeRec := pr.result(rollStart, rollEnd)
		rec.Probe = &probeRec
	}
	if rp != nil {
		r := rp.result(ctx, c, starts, rollEnd)
		rec.Retained = &r
	}
	s, err := pop.sample()
	if err != nil {
		t.Fatal(err)
	}
	sum, err := pop.finish()
	if err != nil {
		t.Fatal(err)
	}
	rec.Population = rollPopulation{OnlineAtEnd: s.Online, Target: clients - failed, SessionLost: sum.SessionLost,
		SessionLostClients: sum.SessionLostClients, SubRefusals: sum.SubRefusals, SubDowngraded: sum.SubDowngraded,
		Drops: sum.Drops, Refusals: sum.Refusals}
	rec.Findings = rollFindings(rec)
	prov.TreeAtEnd = checkTree(".", prov.Tree)
	rec.Provenance = prov
	report := rollReport(rec)
	t.Log(report)
	if artifactDir != "" {
		raw, err := json.MarshalIndent(rec, "", " ")
		if err != nil {
			t.Fatal(err)
		}
		for name, data := range map[string][]byte{"roll.json": raw, "report.md": []byte(report)} {
			if err := os.WriteFile(filepath.Join(artifactDir, name), data, 0o640); err != nil { //nolint:gosec // operator-chosen dir
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(filepath.Join(artifactDir, "complete"), nil, 0o640); err != nil { //nolint:gosec // operator-chosen dir
			t.Fatal(err)
		}
	}
	if !record {
		checkRollGate(t, rec, names)
	}
}

// checkRollGate is what the gate asserts of the arm: its own structure, not
// the product's figures.
func checkRollGate(t *testing.T, rec rollRecord, names []string) {
	t.Helper()
	if strings.Join(rec.Order, ",") != strings.Join(rollOrder(names), ",") || len(rec.Steps) != len(names) {
		t.Fatalf("order %v and %d steps, want %v and %d", rec.Order, len(rec.Steps), rollOrder(names), len(names))
	}
	for _, s := range rec.Steps {
		if !s.DrainCompleted || s.Status.Phase != "complete" || !s.Ready {
			t.Errorf("%s: drain completed %v phase %q, ready %v %s", s.Node, s.DrainCompleted, s.Status.Phase, s.Ready, s.RestartErr)
		}
		left := false
		for _, m := range s.Maps {
			if !containsName(m.Basis, s.Node) && len(m.Basis) > 0 {
				left = true
			}
		}
		if !left {
			t.Errorf("%s: no map seen during its step left it out: %+v", s.Node, s.Maps)
		}
		if s.Probe.Attempts == 0 {
			t.Errorf("%s: the probe published nothing during the step", s.Node)
		}
	}
	if rec.Population.OnlineAtEnd != rec.Population.Target {
		t.Errorf("%d of %d online at the end", rec.Population.OnlineAtEnd, rec.Population.Target)
	}
	// The retained probe is not vacuous: it published and was acknowledged,
	// and the end check read every topic it wrote.
	if r := rec.Retained; r == nil {
		t.Errorf("the retained probe did not run: %s", rec.RetainedMissing)
	} else if r.Acked == 0 || r.Served < r.AckedTopics {
		t.Errorf("retained probe: %d acknowledged, %d of the %d acknowledged topics served at the end", r.Acked, r.Served, r.AckedTopics)
	}
	raw, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	var back rollRecord
	if err := json.Unmarshal(raw, &back); err != nil || len(back.Steps) != len(rec.Steps) || back.Steps[0].Node != rec.Steps[0].Node {
		t.Fatalf("roll.json does not decode back: %v", err)
	}
}

// rollStepRun drains, stops and restarts one node and records the step. It
// returns when the node is ready again, or its wait ended.
func rollStepRun(t *testing.T, c *Cluster, n *Node, drainLimitS, cutS int, sec func(time.Time) float64,
	progress func(string, ...any)) (rollStep, time.Time) {
	t.Helper()
	step := rollStep{Node: n.Name, Confirms: map[string]confirmDelta{}}
	srv := n.Server()
	beforeAll := rollSnapshot(c)
	confirmBefore := rollConfirms(c)
	began := time.Now()
	step.DrainStartSec = sec(began)
	progress("%s: drain at %.1f s", n.Name, step.DrainStartSec)

	// The sampler: the draining node's phase, and every running node's map
	// and moved copies, until the node is ready again.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	var kept []keptSample
	seenMap := map[string]string{}
	lastPhase := ""
	wg.Add(1)
	go func() {
		defer wg.Done()
		tick := time.NewTicker(50 * time.Millisecond)
		defer tick.Stop()
		odd := false
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
			}
			now := time.Now()
			if st := srv.DrainStatus(); st.Phase != "" {
				mu.Lock()
				if st.Phase != lastPhase {
					step.Phases = append(step.Phases, phaseMark{Phase: st.Phase, AtSec: sec(now)})
					lastPhase = st.Phase
				}
				mu.Unlock()
			}
			if odd = !odd; odd {
				continue
			}
			total := 0
			for _, rn := range c.Running() {
				s := rn.Server()
				if s == nil || s.Replication() == nil {
					continue
				}
				m := s.Replication().Map().Current()
				basis := append([]string(nil), m.Basis...)
				sort.Strings(basis)
				key := fmt.Sprintf("%d %v", m.Version, basis)
				mu.Lock()
				if seenMap[rn.Name] != key {
					seenMap[rn.Name] = key
					step.Maps = append(step.Maps, mapSeen{Node: rn.Name, Version: m.Version, Basis: basis, AtSec: sec(now)})
				}
				mu.Unlock()
				total += s.Replication().KeptMoved()
			}
			mu.Lock()
			kept = append(kept, keptSample{atSec: sec(now), kept: total})
			mu.Unlock()
		}
	}()

	srv.Drain()
	var cut <-chan time.Time
	if cutS > 0 {
		cut = time.After(time.Duration(cutS) * time.Second)
	}
	select {
	case <-srv.Drained():
		step.DrainCompleted = true
	case <-cut:
		st := srv.DrainStatus()
		step.Cut, step.CutStatus = true, &st
		if l := srv.Leases(); l != nil {
			step.CutLeases = len(l.Table().OwnedBy(n.Name))
		}
	case <-time.After(time.Duration(drainLimitS) * time.Second):
	}
	end := time.Now()
	step.DrainEndSec, step.DrainSec = sec(end), end.Sub(began).Seconds()
	step.BeyondChartGrace = beyondChartGrace(step.DrainSec)
	step.Status = srv.DrainStatus()
	drainedAfter := rollSnapshot(c)
	confirmDrained := rollConfirms(c)
	step.DrainCounters = risen(beforeAll[n.Name], drainedAfter[n.Name], func(name string) bool {
		return strings.HasPrefix(name, "hoardmq_drain_")
	})
	progress("%s: drain %s after %.1f s: %+v", n.Name, map[bool]string{true: "complete", false: "NOT complete"}[step.DrainCompleted], step.DrainSec, step.Status)
	if step.Cut {
		c.StopWithin(n, rollKillAfter)
	} else {
		c.Stop(n)
	}
	step.StopSec = sec(time.Now())
	restarted := time.Now()
	step.RestartSec = sec(restarted)
	if err := c.start(n); err != nil {
		step.RestartErr = err.Error()
	} else {
		deadline := restarted.Add(rollReadyLimit)
		for time.Now().Before(deadline) {
			if s := n.Server(); s != nil {
				if ready, _ := s.Readiness(); ready {
					step.Ready = true
					break
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	step.ReadySec = sec(time.Now())
	progress("%s: restarted, ready %v after %.1f s", n.Name, step.Ready, step.ReadySec-step.RestartSec)
	close(stop)
	wg.Wait()

	after := rollSnapshot(c)
	step.Failures = stepRise(beforeAll, after, drainedAfter, n.Name, rollFailureMetric)
	step.Copies = stepRise(beforeAll, after, drainedAfter, n.Name, rollCopyMetric)
	step.Empty = stepRise(beforeAll, after, drainedAfter, n.Name, rollEmptyMetric)
	// The restarted node's counters start from zero, so all of them rose in
	// the step.
	for k, v := range risen(nil, after[n.Name], rollEmptyMetric) {
		step.Empty[k] += v
	}
	confirmAfter := rollConfirms(c)
	// A restarted node's counters start again, so the drained node is read
	// as it stopped.
	confirmAfter[n.Name] = confirmDrained[n.Name]
	for node, a := range confirmAfter {
		b := confirmBefore[node]
		step.Confirms[node] = confirmDelta{SentMoved: a.SentMoved - b.SentMoved, Acted: a.Acted - b.Acted,
			ActMsTotal: a.ActMsTotal - b.ActMsTotal, ActMsMax: a.ActMsMax, Sent: a.Sent - b.Sent,
			Received: a.Received - b.Received, Ignored: a.Ignored - b.Ignored, Unheld: a.Unheld - b.Unheld,
			Undurable: a.Undurable - b.Undurable, Unsent: a.Unsent - b.Unsent}
	}
	step.KeptMovedPeak, step.Changes = mapChangeLags(step.Maps, kept)
	return step, began
}

// stepRise sums what rose over a step for the families keep picks: on every
// node running at both ends but the drained one, and on the drained node until
// its drain ended, since its restart starts its counters again.
func stepRise(before, after, drainedAfter map[string]map[string]float64, drained string, keep func(string) bool) map[string]float64 {
	out := map[string]float64{}
	for node, a := range after {
		b, ok := before[node]
		if !ok || node == drained {
			continue
		}
		for k, v := range risen(b, a, keep) {
			out[k] += v
		}
	}
	for k, v := range risen(before[drained], drainedAfter[drained], keep) {
		out[k] += v
	}
	return out
}

// rollSnapshot reads each running node's drain, forward, barrier and replica
// copy counters.
func rollSnapshot(c *Cluster) map[string]map[string]float64 {
	out := map[string]map[string]float64{}
	for _, n := range c.Running() {
		srv := n.Server()
		if srv == nil {
			continue
		}
		snap, ok := srv.Registry().Metrics().(interface{ Snapshot() []module.Metric })
		if !ok {
			continue
		}
		got := make(chan []module.Metric, 1)
		go func() { got <- snap.Snapshot() }()
		select {
		case m := <-got:
			out[n.Name] = pickRollMetrics(m)
		case <-time.After(30 * time.Second):
		}
	}
	return out
}

// rollConfirms reads each running node's moved-copy confirmation counters.
func rollConfirms(c *Cluster) map[string]confirmDelta {
	out := map[string]confirmDelta{}
	for _, n := range c.Running() {
		srv := n.Server()
		if srv == nil {
			continue
		}
		got := make(chan broker.StormStats, 1)
		go func() { got <- srv.StormStats() }()
		select {
		case st := <-got:
			if l := st.Lease; l != nil {
				out[n.Name] = confirmDelta{SentMoved: l.ConfirmsSentMoved, Acted: l.ConfirmsActed,
					ActMsTotal: l.ConfirmActMsTotal, ActMsMax: l.ConfirmActMsMax, Sent: l.ConfirmsSent,
					Received: l.ConfirmsReceived, Ignored: l.ConfirmsIgnored, Unheld: l.ConfirmsUnheld,
					Undurable: l.ConfirmsUndurable, Unsent: l.ConfirmsUnsent}
			}
		case <-time.After(30 * time.Second):
		}
	}
	return out
}
