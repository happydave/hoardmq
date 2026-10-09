package sim

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/module"
)

// RunOptions shape one seeded run.
type RunOptions struct {
	Seed     int64
	Window   time.Duration // the fault window; default 8 s
	Settle   time.Duration // wait after the window for the cluster and clients; default 12 s
	Events   int           // faults in the window; default 4
	NoFaults bool          // workload only
	Cluster  Options
	Workload WorkloadOptions
	// Events, when set, replaces the generated schedule (defect injection tests).
	Schedule []Step
}

// Report is what a run produced.
type Report struct {
	Cluster    *Cluster // the cluster the run used; its nodes stop at test end
	Seed       int64
	Schedule   []Step
	Violations []Violation
	Stats      map[string]int
	// Liveness and BarrierFailures are the end-of-run probes and the counters
	// that explain them, for a test that asks about recovery directly.
	Liveness        []Probe
	BarrierFailures map[string]string
	// Recovery is each node's refusing time after the heal, from HealedAt; nil
	// when a node was down and nothing was measured.
	Recovery []NodeRecovery
	// RefusedKeys are the keys peers refused records for, watched from the
	// heal; nil when nothing was watched.
	RefusedKeys []KeyBlock
	HealedAt    time.Time
	Duration    time.Duration
}

// Describe renders the report for logs and failures.
func (r Report) Describe() string {
	var b strings.Builder
	fmt.Fprintf(&b, "seed %d (%s)\n", r.Seed, r.Duration.Round(time.Millisecond))
	for _, e := range r.Schedule {
		fmt.Fprintf(&b, "  %s\n", e)
	}
	keys := make([]string, 0, len(r.Stats))
	for k := range r.Stats {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(&b, "  %s=%d", k, r.Stats[k])
	}
	b.WriteString("\n")
	fmt.Fprintf(&b, "  %s\n", describeRecovery(r.Recovery))
	if r.RefusedKeys != nil {
		fmt.Fprintf(&b, "  %s\n", describeRefusedKeys(r.RefusedKeys))
	}
	for _, v := range r.Violations {
		fmt.Fprintf(&b, "  VIOLATION %s\n", v)
	}
	return b.String()
}

// Run executes one seeded scenario: a cluster, a workload, the schedule,
// a settle period, then the property checks. It never fails the test
// itself; callers judge the report.
func Run(t testing.TB, o RunOptions) Report {
	t.Helper()
	start := time.Now()
	if o.Window == 0 {
		o.Window = 8 * time.Second
	}
	if o.Settle == 0 {
		o.Settle = 12 * time.Second
	}
	if o.Events == 0 {
		o.Events = 4
	}
	rtBefore := readRuntime()
	rng := rand.New(rand.NewSource(o.Seed)) //nolint:gosec // seeded simulation
	c := New(t, o.Cluster)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	w, err := NewWorkload(ctx, c, rng.Int63(), o.Workload)
	if err != nil {
		t.Fatalf("seed %d: workload: %v", o.Seed, err)
	}
	var names []string
	for _, n := range c.Nodes {
		names = append(names, n.Name)
	}
	schedule := o.Schedule
	if schedule == nil && !o.NoFaults {
		schedule = Generate(rng, names, o.Window, o.Events, c.o.TTL)
	}
	sampler := newSampler(c)
	// Every node's view of the workload's topics, so an acknowledged loss
	// carries the routing state that produced it (WI 1354).
	routes := newRouteSampler(c, w.Topics)
	sampler.start()
	w.Start()
	faultsAt := time.Now()
	var probes []Probe
	var closed []FailClosed
	windowDone := time.After(o.Window)
	if len(schedule) > 0 {
		probes, closed = Execute(c, schedule, c.o.TTL)
	}
	<-windowDone
	w.Stop()
	c.HealAll()
	for _, n := range c.Nodes {
		if !n.Running() {
			c.Start(n)
		}
	}
	c.WaitSettled(o.Settle)
	// The healed instant: from here a refusal has no fault left to explain
	// it, so each node is watched until it first accepts a publish.
	watch := watchRecovery(c, "liveness/recovery")
	var refused *refusedWatch
	if watch != nil {
		refused = watchRefusedKeys(c, watch.healed)
	}
	// Every client back online, then a moment for redeliveries.
	deadline := time.Now().Add(o.Settle)
	for time.Now().Before(deadline) {
		all := true
		for _, cl := range append(append([]*Client(nil), w.Subs...), w.Pubs...) {
			if !cl.Online() {
				all = false
			}
		}
		if all {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(2 * time.Second)
	sampler.stop()
	w.Close()
	// The replica-resend exception's bounds derive from the cluster's
	// timing. A partitioned owner keeps serving its clients — delivering and
	// taking their PUBACKs — for up to the lease ttl before it fences, and
	// none of those completion records reach its followers; so a replica can
	// lag the client's acknowledgements by the ttl plus the acknowledgement
	// timeout (twice, for a retried send). A rebuilt queue drains within the
	// takeover that produced it plus one acknowledgement round.
	routes.stop()
	// Every fault is healed, every stopped node restarted and the cluster
	// settled: a node that cannot accept a publish now has no excuse left
	// (WI 1502). The topic has no subscriber, so the healthy answer is No
	// Matching Subscribers.
	watch.giveUntil(livenessBound(c))
	liveness := livenessProbes(c, "liveness/probe")
	recovery := watch.finish(c)
	refusedKeys := refused.finish()
	e := Evidence{Ledger: w.Ledger, Owners: sampler.samples(), Probes: probes, FailClosed: closed,
		Liveness: liveness, BarrierFailures: barrierFailuresByNode(c), Recovery: recovery, Unapplied: unappliedCounts(c), RefusedKeys: refusedKeys, FaultFree: len(schedule) == 0,
		ResendLag: c.o.TTL + 2*c.AckTimeout, ResendWindow: 2*c.TakeoverTimeout + c.AckTimeout,
		Routes: routes.taken(), ServedEmpty: servedEmptyByNode(c)}
	violations := Check(e)
	if v := sampler.vacuity(); v != nil {
		violations = append(violations, *v)
	}
	rep := Report{Cluster: c, Seed: o.Seed, Schedule: schedule, Violations: violations, Stats: Stats(e), Duration: time.Since(start),
		Liveness: e.Liveness, BarrierFailures: e.BarrierFailures, Recovery: e.Recovery, RefusedKeys: e.RefusedKeys}
	if watch != nil {
		rep.HealedAt = watch.healed
	}
	// Why a build refused, from the brokers' own counters rather than the
	// clients' error strings: a change in the refusal rate is then visible
	// in every corpus run instead of only on a cluster (WI 1351).
	for reason, n := range refusalsByReason(c) {
		rep.Stats["refused."+reason] = n
	}
	// How often a late forward met a session installed holding its message
	// (WI 1521): the corpus reaches the path when this is above zero.
	rep.Stats["deliveries.duplicateSkipped"] = duplicatesSkipped(c)
	// Evidence capture only: with the env var unset this is a no-op, and a
	// failure inside it never changes the verdict.
	ops := rep.Stats["publications"] + rep.Stats["deliveries"] + rep.Stats["event.connack"]
	writeRunArtifact(t, rep, e, faultsAt, deltaBetween(rtBefore, readRuntime(), ops))
	return rep
}

// duplicatesSkipped totals the running nodes' routed deliveries skipped
// because the session was installed holding the message.
func duplicatesSkipped(c *Cluster) int {
	n := 0
	for _, nd := range c.Running() {
		if srv := nd.Server(); srv != nil {
			n += int(srv.Sessions().DuplicatesSkipped())
		}
	}
	return n
}

// refusalsByReason totals every running node's forward failures by the
// reason the routing plane recorded, so "the publish was refused" carries
// which rule refused it.
//
// Two limits, because the number is easy to over-read: it covers the nodes
// still running at the end of the run, each counting from its last start —
// so a node crashed and not restarted contributes nothing, and a restarted
// node contributes only what it refused since it came back. Snapshotting a
// registry walks the session table under its lock, which the failover
// measurement has seen held for longer than a measurement, so the wait is
// bounded and a node that does not answer is skipped rather than hung on.
func refusalsByReason(c *Cluster) map[string]int {
	out := map[string]int{}
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
		case metrics := <-got:
			for _, m := range metrics {
				if m.Name == "hoardmq_forward_failures_total" {
					out[m.Labels["reason"]] += int(m.Value)
				}
			}
		case <-time.After(5 * time.Second):
		}
	}
	return out
}

// servedEmptyByNode reads each running node's
// hoardmq_sessions_rebuilt_empty_total by reason, with refusalsByReason's
// bounded wait and its limits: a crashed node contributes nothing, and a
// restarted one only what it counted since it came back.
func servedEmptyByNode(c *Cluster) map[string]map[string]int {
	out := map[string]map[string]int{}
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
		case metrics := <-got:
			byReason := map[string]int{}
			for _, m := range metrics {
				if m.Name == "hoardmq_sessions_rebuilt_empty_total" && m.Value > 0 {
					byReason[m.Labels["reason"]] += int(m.Value)
				}
			}
			out[n.Name] = byReason
		case <-time.After(5 * time.Second):
		}
	}
	return out
}

// barrierFailuresByNode reads each running node's replication barrier
// failures, rendered as "peer/reason=n" pairs, so a liveness violation names
// the stream that could not make progress. The same bounded-wait shape as
// refusalsByReason, and for the same reason: a node that does not answer is
// skipped, never hung on.
func barrierFailuresByNode(c *Cluster) map[string]string {
	out := map[string]string{}
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
		case metrics := <-got:
			var parts []string
			for _, m := range metrics {
				if m.Name == "hoardmq_replication_barrier_failures_total" && m.Value > 0 {
					parts = append(parts, fmt.Sprintf("%s/%s=%d", m.Labels["peer"], m.Labels["reason"], int(m.Value)))
				}
				// WI 1510 investigation: barriers that passed with a refused
				// sequence inside their window. Not a failure — the opposite,
				// and the reason it is reported beside them.
				if m.Name == "hoardmq_replication_barrier_escapes_total" && m.Value > 0 {
					parts = append(parts, fmt.Sprintf("%s/ESCAPED=%d", m.Labels["peer"], int(m.Value)))
				}
				// A refusal that could not be scoped blocks the whole stream,
				// which is the behaviour WI 1510 removed; its rate says how
				// much of the scoping holds.
				if m.Name == "hoardmq_replication_refusal_fallback_total" && m.Value > 0 {
					parts = append(parts, fmt.Sprintf("%s/FALLBACK-%s=%d", m.Labels["peer"], m.Labels["kind"], int(m.Value)))
				}
			}
			sort.Strings(parts)
			out[n.Name] = strings.Join(parts, " ")
		case <-time.After(5 * time.Second):
		}
	}
	return out
}

// sampler watches for a client identifier online on two nodes at once.
type sampler struct {
	c     *Cluster
	mu    sync.Mutex
	found []OwnerSample
	// ticks and sawOnline are what keep "no client was seen on two nodes"
	// from being true of a sampler that never looked, or looked at nothing.
	ticks     int
	sawOnline int
	stopped   chan struct{}
	done      chan struct{}
}

func newSampler(c *Cluster) *sampler {
	return &sampler{c: c, stopped: make(chan struct{}), done: make(chan struct{})}
}

func (s *sampler) start() { go s.loop() }

func (s *sampler) stop() {
	close(s.stopped)
	<-s.done
}

// vacuity reports why the sampler's silence would mean nothing, if it would.
func (s *sampler) vacuity() *Violation {
	s.mu.Lock()
	defer s.mu.Unlock()
	return ownerSamplingVacuous(s.ticks, s.sawOnline)
}

// ownerSamplingVacuous is the non-vacuity of one-live-owner: the property
// passes when no client was seen online on two nodes, which is also what a
// sampler that never ran, or never saw a client online at all, reports.
func ownerSamplingVacuous(ticks, mostOnline int) *Violation {
	switch {
	case ticks == 0:
		return &Violation{Property: PropOneOwner, Detail: "the owner sampler never sampled, so it could not have seen two live owners"}
	case mostOnline == 0:
		return &Violation{Property: PropOneOwner, Detail: fmt.Sprintf("the owner sampler saw no client online in %d samples, so it could not have seen two live owners", ticks)}
	}
	return nil
}

func (s *sampler) samples() []OwnerSample {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]OwnerSample(nil), s.found...)
}

func (s *sampler) loop() {
	defer close(s.done)
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-s.stopped:
			return
		case <-tick.C:
		}
		online := map[string][]string{}
		for _, n := range s.c.Nodes {
			srv := n.Server()
			if srv == nil {
				continue
			}
			for _, rec := range srv.Sessions().Records() {
				if rec.Online {
					online[rec.ClientID] = append(online[rec.ClientID], n.Name)
				}
			}
		}
		now := time.Now()
		s.mu.Lock()
		s.ticks++
		if len(online) > s.sawOnline {
			s.sawOnline = len(online)
		}
		s.mu.Unlock()
		for id, nodes := range online {
			if len(nodes) > 1 {
				sort.Strings(nodes)
				s.mu.Lock()
				s.found = append(s.found, OwnerSample{At: now, Client: id, Nodes: nodes})
				s.mu.Unlock()
			}
		}
	}
}
