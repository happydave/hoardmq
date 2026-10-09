package sim

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/happydave/hoardmq/internal/module"
)

// NodeRecovery is how long one node went on refusing QoS 1 publishes after
// every fault was healed and the cluster settled, and how much it refused.
// It is the magnitude beside replication-recovers' verdict: a node wedged for
// twenty seconds that clears before the final probe passes the property and
// shows here.
type NodeRecovery struct {
	Node      string `json:"node"`
	Recovered bool   `json:"recovered"`
	// AfterMs runs from the healed instant to the first accepted probe. For a
	// node that did not recover it is how long the node was watched, which is
	// a lower bound and never a duration to average.
	AfterMs float64 `json:"after_ms"`
	Probes  int     `json:"probes"`
	Refused int     `json:"refused"`
	// ByCode counts the refusals by reason code; a probe that got no answer
	// is counted under "no answer", with the last such error kept.
	ByCode  map[string]int `json:"refused_by_code,omitempty"`
	LastErr string         `json:"last_error,omitempty"`
	// Barrier is the node's failures at the replication and forward barriers
	// between the healed instant and the end of the watch, so a refusal is
	// attributed to what produced it during recovery rather than during the
	// faults.
	Barrier map[string]int `json:"barrier_failures_since_heal,omitempty"`
}

// Wedged reports whether the node refused anything after the heal. A run in
// which no node did says nothing about recovery time.
func (r NodeRecovery) Wedged() bool { return r.Refused > 0 }

func accepted(p Probe) bool { return p.Err == nil && !p.Code.IsError() }

// recoveryOf reads one node's probe sequence. The sequence is in the order
// the probes were taken; watchedUntil is when the watch ended.
func recoveryOf(node string, healed time.Time, probes []Probe, watchedUntil time.Time) NodeRecovery {
	r := NodeRecovery{Node: node, Probes: len(probes)}
	for _, p := range probes {
		if accepted(p) {
			r.Recovered = true
			r.AfterMs = ms(p.At.Sub(healed))
			return r
		}
		r.Refused++
		if r.ByCode == nil {
			r.ByCode = map[string]int{}
		}
		if p.Err != nil && p.Code == 0 {
			r.ByCode["no answer"]++
			r.LastErr = p.Err.Error()
		} else {
			r.ByCode[p.Code.String()]++
		}
	}
	r.AfterMs = ms(watchedUntil.Sub(healed))
	return r
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// barrierDelta is what after holds beyond before, without the rows that did
// not move.
func barrierDelta(before, after map[string]int) map[string]int {
	var out map[string]int
	for k, v := range after {
		if d := v - before[k]; d > 0 {
			if out == nil {
				out = map[string]int{}
			}
			out[k] = d
		}
	}
	return out
}

// recoveryWatch probes every node from the healed instant, each on its own
// goroutine so one node's refusals do not delay the first question put to
// the next. It uses its own client identifiers: sharing livenessProbes' would
// have each probe take over the other's session and read as a refusal.
type recoveryWatch struct {
	healed   time.Time
	topic    string
	deadline atomic.Int64 // unix nanoseconds; 0 until the run sets it
	wg       sync.WaitGroup

	mu     sync.Mutex
	probes map[string][]Probe
	ended  map[string]time.Time
	before map[string]map[string]int
}

// watchHardLimit ends a watch whose run never set a deadline.
const watchHardLimit = 5 * time.Minute

// watchRecovery starts the watch, or returns nil when a node is down: with a
// replica missing the survivors refuse correctly, as livenessProbes explains.
func watchRecovery(c *Cluster, topic string) *recoveryWatch {
	for _, n := range c.Nodes {
		if !n.Running() {
			return nil
		}
	}
	w := &recoveryWatch{healed: time.Now(), topic: topic, probes: map[string][]Probe{}, ended: map[string]time.Time{}, before: barrierCounts(c)}
	for _, n := range c.Nodes {
		w.wg.Add(1)
		go w.watch(n.Name, n.TCP)
	}
	return w
}

func (w *recoveryWatch) watch(node, addr string) {
	defer w.wg.Done()
	hard := w.healed.Add(watchHardLimit)
	for {
		code, err := ProbePublish(addr, "recovery-"+node, w.topic)
		p := Probe{At: time.Now(), Node: node, Code: code, Err: err}
		w.mu.Lock()
		w.probes[node] = append(w.probes[node], p)
		w.ended[node] = p.At
		w.mu.Unlock()
		if accepted(p) {
			return
		}
		if d := w.deadline.Load(); (d != 0 && p.At.UnixNano() > d) || p.At.After(hard) {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// giveUntil gives the nodes still refusing until the bound from now. The run
// calls it where the final probes begin, so both judge the same deadline.
func (w *recoveryWatch) giveUntil(bound time.Duration) {
	if w != nil {
		w.deadline.Store(time.Now().Add(bound).UnixNano())
	}
}

// finish waits for every watcher and returns the records in node order, or
// nil when the cluster was not whole and nothing was watched.
func (w *recoveryWatch) finish(c *Cluster) []NodeRecovery {
	if w == nil {
		return nil
	}
	w.wg.Wait()
	after := barrierCounts(c)
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]NodeRecovery, 0, len(c.Nodes))
	for _, n := range c.Nodes {
		r := recoveryOf(n.Name, w.healed, w.probes[n.Name], w.ended[n.Name])
		r.Barrier = barrierDelta(w.before[n.Name], after[n.Name])
		out = append(out, r)
	}
	return out
}

// barrierCounts reads each running node's failures at both barriers a QoS 1
// acknowledgement waits on, as "replication:peer/reason" and "forward:reason"
// counts. The wait is bounded for the reason refusalsByReason gives: a node
// that does not answer is skipped, never hung on.
func barrierCounts(c *Cluster) map[string]map[string]int {
	out := map[string]map[string]int{}
	for _, n := range c.Running() {
		metrics, ok := snapshotMetrics(n)
		if !ok {
			continue
		}
		counts := map[string]int{}
		for _, m := range metrics {
			if m.Value <= 0 {
				continue
			}
			// A QoS 1 acknowledgement waits on two barriers, and either
			// can refuse it: the replication streams, counted by peer, and
			// the routing plane's forwards, counted by reason alone.
			switch m.Name {
			case "hoardmq_replication_barrier_failures_total":
				counts["replication:"+m.Labels["peer"]+"/"+m.Labels["reason"]] += int(m.Value)
			case "hoardmq_forward_failures_total":
				counts["forward:"+m.Labels["reason"]] += int(m.Value)
			}
		}
		out[n.Name] = counts
	}
	return out
}

// snapshotMetrics reads a running node's counters, bounded: a node that does
// not answer is skipped, never hung on.
func snapshotMetrics(n *Node) ([]module.Metric, bool) {
	srv := n.Server()
	if srv == nil {
		return nil, false
	}
	snap, ok := srv.Registry().Metrics().(interface{ Snapshot() []module.Metric })
	if !ok {
		return nil, false
	}
	got := make(chan []module.Metric, 1)
	go func() { got <- snap.Snapshot() }()
	select {
	case metrics := <-got:
		return metrics, true
	case <-time.After(5 * time.Second):
		return nil, false
	}
}

// unappliedCounts sums, over the running nodes, the replicated records that
// changed nothing on a follower, as "reason" and "reason.type", and the
// owners' repairs of a refused key as "repairs.outcome". A node's counters
// restart with it, so what a node counted before a crash in the schedule is
// not here.
func unappliedCounts(c *Cluster) map[string]int {
	out := map[string]int{}
	for _, n := range c.Running() {
		metrics, ok := snapshotMetrics(n)
		if !ok {
			continue
		}
		for _, m := range metrics {
			if m.Value <= 0 {
				continue
			}
			switch m.Name {
			case "hoardmq_replication_unapplied_total":
				out[m.Labels["reason"]] += int(m.Value)
				out[m.Labels["reason"]+"."+m.Labels["type"]] += int(m.Value)
			case "hoardmq_replication_repairs_total":
				out["repairs."+m.Labels["outcome"]] += int(m.Value)
			}
		}
	}
	return out
}

// recoveryStats are the integer summaries that fit the run's stats map. The
// longest duration is over nodes that were wedged and recovered: a node that
// never recovered has no duration, and is counted instead.
func recoveryStats(rec []NodeRecovery) map[string]int {
	st := map[string]int{}
	if rec == nil {
		return st
	}
	st["recovery.watched"] = len(rec)
	for _, r := range rec {
		st["recovery.refusedProbes"] += r.Refused
		if !r.Wedged() {
			continue
		}
		st["recovery.wedgedNodes"]++
		if !r.Recovered {
			st["recovery.notRecovered"]++
			continue
		}
		if v := int(r.AfterMs); v > st["recovery.longestMs"] {
			st["recovery.longestMs"] = v
		}
	}
	return st
}

// describeRecovery renders the records for a report or a violation. A run
// where nothing refused says so, because its silence is not a fast recovery.
func describeRecovery(rec []NodeRecovery) string {
	if rec == nil {
		return "recovery: not watched (a node was down)"
	}
	var parts []string
	for _, r := range rec {
		if r.Wedged() {
			parts = append(parts, r.String())
		}
	}
	if len(parts) == 0 {
		return "recovery: no node refused a publish after the heal"
	}
	return "recovery: " + strings.Join(parts, "; ")
}

func (r NodeRecovery) String() string {
	var b strings.Builder
	if r.Recovered {
		fmt.Fprintf(&b, "%s refused %d probes and accepted %.0f ms after the heal", r.Node, r.Refused, r.AfterMs)
	} else {
		fmt.Fprintf(&b, "%s refused %d probes and had NOT recovered when the watch ended, at least %.0f ms after the heal", r.Node, r.Refused, r.AfterMs)
	}
	b.WriteString(countsNote(" by code", r.ByCode))
	b.WriteString(countsNote(" barrier failures since the heal", r.Barrier))
	return b.String()
}

func countsNote(label string, m map[string]int) string {
	if len(m) == 0 {
		return ""
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", k, m[k]))
	}
	return ";" + label + " " + strings.Join(parts, " ")
}

// recoveryNote is the node's record, for a violation that names the node.
func recoveryNote(rec []NodeRecovery, node string) string {
	for _, r := range rec {
		if r.Node == node {
			return "; " + r.String()
		}
	}
	return ""
}
