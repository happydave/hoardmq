package sim

import (
	"fmt"
	"strings"
	"time"

	"github.com/happydave/hoardmq/internal/module"
)

// The failure counters split at the recovery instant (WI 1734, from WI 1709's
// recommendation 2).
//
// metrics_before and metrics_after bracket the whole run, so a refusal counted
// between them cannot be dated against the instant. One metrics snapshot per
// survivor, taken when the hold that confirms the instant begins, splits them:
// what rose after it is metrics_after minus that snapshot. A snapshot walks
// the session table under its lock, so it is taken once per hold start, off
// the sampling loop, and never as a series; one taken at the hold's start
// comes after the storm.

// counterSnapshot is one snapshot of every survivor's storm metrics, taken for
// the hold that began at For.
type counterSnapshot struct {
	For      time.Time
	Metrics  map[string]map[string]float64
	Began    map[string]time.Time
	Took     map[string]time.Duration
	TimedOut []string
}

// instantSnapshots takes a counterSnapshot each time a hold begins, one at a
// time and off the caller's goroutine, and keeps them until the instant is
// known. take is the snapshot itself; tests give it a fake.
type instantSnapshots struct {
	take     func(forStart time.Time) counterSnapshot
	done     chan counterSnapshot
	inFlight bool
	lastFor  time.Time
	begun    int
	taken    []counterSnapshot
}

func newInstantSnapshots(take func(time.Time) counterSnapshot) *instantSnapshots {
	return &instantSnapshots{take: take, done: make(chan counterSnapshot, 1)}
}

// observe is called once per pass of the window loop with the open hold's
// start (zero when none is open). It collects a finished snapshot and, when
// none is in flight and a hold has begun that has no snapshot yet, starts one.
// A hold that restarts while a snapshot is in flight is taken once that one
// returns.
func (s *instantSnapshots) observe(holdStart time.Time) {
	select {
	case snap := <-s.done:
		s.taken = append(s.taken, snap)
		s.inFlight = false
	default:
	}
	if s.inFlight || holdStart.IsZero() || holdStart.Equal(s.lastFor) {
		return
	}
	s.inFlight, s.lastFor = true, holdStart
	s.begun++
	go func() { s.done <- s.take(holdStart) }()
}

// finish waits for a snapshot in flight and returns the one taken for the
// hold that began at instant. When none was (the loop ended before one could
// start), it takes one now and reports it as late. A zero instant has no
// snapshot.
func (s *instantSnapshots) finish(instant time.Time) (snap counterSnapshot, ok, late bool) {
	if s.inFlight {
		s.taken = append(s.taken, <-s.done)
		s.inFlight = false
	}
	if instant.IsZero() {
		return counterSnapshot{}, false, false
	}
	for i := len(s.taken) - 1; i >= 0; i-- {
		if s.taken[i].For.Equal(instant) {
			return s.taken[i], true, false
		}
	}
	s.begun++
	return s.take(instant), true, true
}

// snapshotSurvivors reads every survivor's storm metrics, each bounded as
// leaseCounters bounds it, and records when each read began and how long it
// took.
func snapshotSurvivors(c *Cluster, victim *Node, forStart time.Time) counterSnapshot {
	out := counterSnapshot{For: forStart, Metrics: map[string]map[string]float64{},
		Began: map[string]time.Time{}, Took: map[string]time.Duration{}}
	for _, n := range c.Nodes {
		if n == victim {
			continue
		}
		srv := n.Server()
		if srv == nil {
			continue
		}
		snap, ok := srv.Registry().Metrics().(interface{ Snapshot() []module.Metric })
		if !ok {
			continue
		}
		began := time.Now()
		got := make(chan []module.Metric, 1)
		go func() { got <- snap.Snapshot() }()
		select {
		case metrics := <-got:
			out.Metrics[n.Name] = pickStormMetrics(metrics)
		case <-time.After(30 * time.Second):
			out.TimedOut = append(out.TimedOut, n.Name)
		}
		out.Began[n.Name], out.Took[n.Name] = began, time.Since(began)
	}
	return out
}

// instantRecord is run.json's account of the snapshot at the instant: when
// each survivor's read began after the instant and how long it took, how
// many snapshots the run began (one per hold start), and whether this one was
// taken only after watching ended. Absent says why there is none.
type instantRecord struct {
	Nodes     map[string]instantTiming `json:"nodes,omitempty"`
	Snapshots int                      `json:"snapshots"`
	Late      bool                     `json:"late,omitempty"`
	TimedOut  []string                 `json:"timed_out,omitempty"`
	Absent    string                   `json:"absent,omitempty"`
}

type instantTiming struct {
	BeganAfterInstantSec float64 `json:"began_after_instant_sec"`
	ReadSec              float64 `json:"read_sec"`
}

func newInstantRecord(snap counterSnapshot, instant time.Time, begun int, late bool) instantRecord {
	r := instantRecord{Nodes: map[string]instantTiming{}, Snapshots: begun, Late: late, TimedOut: snap.TimedOut}
	for name, began := range snap.Began {
		r.Nodes[name] = instantTiming{BeganAfterInstantSec: began.Sub(instant).Seconds(), ReadSec: snap.Took[name].Seconds()}
	}
	return r
}

// The counter families the report splits at the instant: why a forward did not
// complete, and why the replication barrier refused.
var splitFamilies = []string{"hoardmq_forward_failures_total", "hoardmq_replication_barrier_failures_total"}

// forgottenFamily is WI 1625's forget: a lost owner's routes dropped because
// its sessions are served elsewhere.
const forgottenFamily = "hoardmq_route_owners_forgotten_total"

// familySum adds every series of a family in one survivor's snapshot.
func familySum(m map[string]float64, family string) (float64, bool) {
	var sum float64
	seen := false
	for k, v := range m {
		if k == family || strings.HasPrefix(k, family+"{") {
			sum += v
			seen = true
		}
	}
	return sum, seen
}

// roseAfterInstant lists, per survivor, every series of the split families
// whose value at window end is above its value at the instant.
func roseAfterInstant(atInstant, atEnd map[string]map[string]float64) []string {
	var out []string
	for _, node := range sortedNames(atEnd) {
		inst, ok := atInstant[node]
		if !ok {
			out = append(out, node+" not read at the instant")
			continue
		}
		for _, key := range sortedNames(atEnd[node]) {
			for _, fam := range splitFamilies {
				if key != fam && !strings.HasPrefix(key, fam+"{") {
					continue
				}
				if d := atEnd[node][key] - inst[key]; d > 0 {
					out = append(out, fmt.Sprintf("%s %s +%.0f", node, key, d))
				}
			}
		}
	}
	return out
}

// Where a survivor's forget fell against the instant.
const (
	forgetBefore  = "before"
	forgetAfter   = "after"
	forgetNone    = "none"
	forgetNotRead = "not read"
)

// forgetOrder dates a survivor's forget against the instant from the
// forgotten counter at the instant and at window end. The counter is read at
// the end of the at-instant snapshot, so "before" means no later than the
// instant plus that snapshot's read time.
func forgetOrder(atInstant, atEnd map[string]float64, haveInstant, haveEnd bool) string {
	if !haveInstant || !haveEnd {
		return forgetNotRead
	}
	end, _ := familySum(atEnd, forgottenFamily)
	inst, _ := familySum(atInstant, forgottenFamily)
	switch {
	case end == 0:
		return forgetNone
	case inst >= end:
		return forgetBefore
	default:
		return forgetAfter
	}
}

// instantSplitLine is the report's row: per survivor, the forget against the
// instant, and every failure counter that rose after it.
func instantSplitLine(atInstant, atEnd map[string]map[string]float64, rec *instantRecord) string {
	if rec == nil || rec.Absent != "" {
		why := "no instant"
		if rec != nil {
			why = rec.Absent
		}
		return "not split — " + why
	}
	var parts []string
	for _, node := range sortedNames(atEnd) {
		inst, haveInst := atInstant[node]
		order := forgetOrder(inst, atEnd[node], haveInst, true)
		t := rec.Nodes[node]
		parts = append(parts, fmt.Sprintf("%s: lost owner's routes forgotten %s the instant (snapshot began %.3f s after it, read in %.3f s)",
			node, order, t.BeganAfterInstantSec, t.ReadSec))
	}
	rose := roseAfterInstant(atInstant, atEnd)
	if len(rose) == 0 {
		parts = append(parts, "no forward or barrier failure counted after the instant")
	} else {
		parts = append(parts, "counted after the instant: "+strings.Join(rose, ", "))
	}
	if rec.Late {
		parts = append(parts, "the snapshot was taken after watching ended")
	}
	return strings.Join(parts, "; ")
}
