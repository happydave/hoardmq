package sim

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/broker"
	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/ldap/ldaptest"
	"github.com/happydave/hoardmq/internal/oidc/oidctest"
	"github.com/happydave/hoardmq/internal/otel/otlptest"
	"github.com/happydave/hoardmq/internal/packet"
)

// Options shape a harness cluster.
type Options struct {
	Nodes    int
	Replicas int
	Shards   int
	TTL      time.Duration // lease ttl; default from Timings
	// SnapshotEntries, when set, lowers the raft snapshot threshold so a
	// crashed node can need a snapshot to rejoin within a short scenario.
	SnapshotEntries int
	Defects         broker.Defects
	// AdoptionDelay holds every node's adoption worker back, so a scenario's
	// client can reach the adopting node before it.
	AdoptionDelay time.Duration
	// DrainConfirmTimeout is how long a drain waits after its hand-off for
	// its sessions to be confirmed held by their receiver; zero is the ttl,
	// as the product's default is its ttl's.
	DrainConfirmTimeout time.Duration
	// DrainGate is every node's broker.Hooks.DrainGate.
	DrainGate func()
	// LoadGate and StartGate are every node's broker.Hooks.LoadGate and
	// StartGate.
	LoadGate  func(node string)
	StartGate func(node string)
	// ForwardApplyGate is every node's broker.Hooks.ForwardApplyGate.
	ForwardApplyGate func(node, peer string)
	// ImportGate and AcquireGate, when set, are every node's broker.Hooks of
	// the same names, told the node.
	ImportGate  func(node, ns, id string)
	AcquireGate func(node, ns, id string, granted bool)
	// TakeoverDelay, when set, gives a node's broker.Hooks.TakeoverDelay
	// each time it starts.
	TakeoverDelay func(node string) time.Duration
	// Log, when set, receives every node's debug log prefixed by node.
	Log io.Writer
	// Timings is the set every node timing comes from (clusterTimings);
	// empty is TimingsHarness.
	Timings TimingSet
	// ListenAddrs, when set, are the nodes' client listener addresses in node
	// order, each also its advertised address: the remote-client mode's
	// addresses on the broker host (WI 1480). Empty draws loopback ports.
	ListenAddrs []string
	// FileUsers is how many of the population's clients the users file holds
	// when they authenticate by one (HOARDMQ_SIM_AUTH=file).
	FileUsers int
}

// TimingSet names the cluster timings a harness cluster runs at.
type TimingSet string

// The timing sets. Harness timings are ten times tighter than the product's
// so short scenarios are fast; product timings are what the broker ships.
const (
	TimingsHarness TimingSet = "harness"
	TimingsProduct TimingSet = "product"
)

// clusterTimings is every timing a node's configuration carries. The harness
// writes each of these, so each belongs to the set: a knob left outside it
// runs the harness's value under the product's label (WI 1805).
type clusterTimings struct {
	HeartbeatMs, HeartbeatMisses, RaftTickMs int
	TTL                                      time.Duration
	TakeoverTimeoutMs, AckTimeoutMs          int
	JoinTimeoutMs                            int
	ConnectTimeoutSec                        int
	ReadinessStableMs                        int
	DrainRatePerSecond, DrainTimeoutMs       int
	DrainConfirmTimeoutMs                    int
}

// timingDefaults returns a set's timings at its own lease ttl.
func timingDefaults(set TimingSet) clusterTimings {
	if set == TimingsProduct {
		return setTimings(set, time.Duration(config.Default().Cluster.Leases.TTLMs)*time.Millisecond)
	}
	return setTimings(set, 2*time.Second)
}

// setTimings returns a set's timings at a given lease ttl. The product's come
// from config.Default, so a change to what ships moves the measurement with
// it. The harness's drain confirmation follows its ttl.
func setTimings(set TimingSet, ttl time.Duration) clusterTimings {
	if set == TimingsProduct {
		d := config.Default()
		return clusterTimings{
			HeartbeatMs: d.Cluster.HeartbeatMs, HeartbeatMisses: d.Cluster.HeartbeatMisses, RaftTickMs: d.Cluster.Raft.TickMs,
			TTL:               ttl,
			TakeoverTimeoutMs: d.Cluster.Leases.TakeoverTimeoutMs, AckTimeoutMs: d.Cluster.Replication.AckTimeoutMs,
			JoinTimeoutMs:      d.Cluster.Raft.JoinTimeoutMs,
			ConnectTimeoutSec:  d.Limits.ConnectTimeout,
			ReadinessStableMs:  d.Cluster.Readiness.StableMs,
			DrainRatePerSecond: d.Cluster.Drain.RatePerSecond, DrainTimeoutMs: d.Cluster.Drain.TimeoutMs,
			DrainConfirmTimeoutMs: d.Cluster.Drain.ConfirmTimeoutMs,
		}
	}
	return clusterTimings{
		HeartbeatMs: 100, HeartbeatMisses: 3, RaftTickMs: 10,
		TTL:               ttl,
		TakeoverTimeoutMs: 1000, AckTimeoutMs: 1000,
		JoinTimeoutMs:      1000,
		ConnectTimeoutSec:  5,
		ReadinessStableMs:  0,
		DrainRatePerSecond: 1000, DrainTimeoutMs: 3000,
		DrainConfirmTimeoutMs: max(1000, int(ttl/time.Millisecond)),
	}
}

// Node is one in-process broker with its seams.
type Node struct {
	Name    string
	Dir     string
	TCP     string // client address (stable across restarts)
	Cluster string // transport address (stable across restarts)
	Obs     string

	c       *Cluster
	srv     *broker.Server
	skew    atomic.Int64 // nanoseconds added to the node's clock
	running atomic.Bool
	mu      sync.Mutex
}

// Cluster is a set of nodes with the fault seams the schedule drives.
type Cluster struct {
	t      testing.TB
	o      Options
	dir    string // the root every node's store is under
	secret string
	Nodes  []*Node
	byName map[string]*Node
	byAddr map[string]*Node

	// jwt is the provider the nodes verify client tokens against, nil when
	// the clients are anonymous (HOARDMQ_SIM_AUTH).
	jwt *oidctest.Provider
	// directory is the one the nodes bind to when the clients authenticate
	// by LDAP, with the service account's password file.
	directory         *ldaptest.Server
	directoryPassword string
	// users is the users file the nodes verify passwords against when the
	// clients authenticate by one.
	users string
	// knobs are the product settings the run's environment moves away from
	// their defaults (HOARDMQ_SIM_CONNECT_RATE, HOARDMQ_SIM_AUTH_CACHE_TTL).
	knobs productKnobs
	// traces is the OTLP endpoint the nodes export to when the run traces
	// (HOARDMQ_SIM_TRACE_RATIO).
	traces *otlptest.Collector

	// The timing the nodes are configured with, for properties whose bounds
	// derive from it (the replica-resend exception).
	AckTimeout      time.Duration
	TakeoverTimeout time.Duration
}

// Dir is the root every node's store is under. It is published in the
// failover artifact so a driver that ends a run early — before t.TempDir's
// cleanup can run — knows what to remove (WI 1377).
func (c *Cluster) Dir() string { return c.dir }

// New starts a cluster and waits for it to settle.
func New(t testing.TB, o Options) *Cluster {
	t.Helper()
	if o.Nodes == 0 {
		o.Nodes = 3
	}
	if o.Replicas == 0 {
		o.Replicas = 2
	}
	if o.Shards == 0 {
		o.Shards = 16
	}
	if o.TTL == 0 {
		o.TTL = timingDefaults(o.Timings).TTL
	}
	dir := t.TempDir()
	secret := filepath.Join(dir, "secret")
	if err := os.WriteFile(secret, []byte("simulation-cluster-shared-secret-bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	if len(o.ListenAddrs) > 0 && len(o.ListenAddrs) != o.Nodes {
		t.Fatalf("%d listener addresses for %d nodes", len(o.ListenAddrs), o.Nodes)
	}
	c := &Cluster{t: t, o: o, dir: dir, secret: secret, byName: map[string]*Node{}, byAddr: map[string]*Node{}}
	mode, err := simAuthMode()
	if err != nil {
		t.Fatal(err)
	}
	switch mode {
	case "jwt":
		c.jwt = startTokenSource(t)
	case "ldap":
		c.directory, c.directoryPassword = startDirectory(t, dir)
	case "file":
		c.users = startUsersFile(t, dir, o.FileUsers)
	}
	if c.knobs, err = readProductKnobs(os.Getenv); err != nil {
		t.Fatal(err)
	}
	if c.knobs.traceSet {
		c.traces = startTraceEndpoint(t)
		c.knobs.traceEndpoint = c.traces.Endpoint()
	}
	tm := c.timings()
	c.AckTimeout = time.Duration(tm.AckTimeoutMs) * time.Millisecond
	c.TakeoverTimeout = time.Duration(tm.TakeoverTimeoutMs) * time.Millisecond
	for i := 0; i < o.Nodes; i++ {
		n := &Node{Name: fmt.Sprintf("n%d", i+1), Dir: filepath.Join(dir, fmt.Sprintf("n%d", i+1)), c: c}
		c.Nodes = append(c.Nodes, n)
		c.byName[n.Name] = n
	}
	c.drawAddresses()
	// A port drawn free can be taken before its node binds it. A node cannot
	// re-draw alone, because the nodes started before it already list its
	// address as a peer, so the whole cluster is stopped and started again on
	// new addresses, in a fresh directory rather than over the stores the
	// failed attempt created (WI 1488).
	for attempt := 1; ; attempt++ {
		err := c.startAll()
		if err == nil {
			break
		}
		if !errors.Is(err, syscall.EADDRINUSE) || attempt == clusterStartAttempts {
			t.Fatal(err)
		}
		t.Logf("cluster start attempt %d: %v; starting again on new addresses", attempt, err)
		for _, n := range c.Nodes {
			n.Dir = filepath.Join(dir, fmt.Sprintf("retry-%d", attempt+1), n.Name)
		}
		c.drawAddresses()
	}
	t.Cleanup(func() {
		for _, n := range c.Nodes {
			if n.running.Load() {
				n.mu.Lock()
				srv := n.srv
				n.mu.Unlock()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_ = srv.Shutdown(ctx)
				cancel()
				n.running.Store(false)
			}
		}
	})
	c.WaitServing(tm.settleBudget())
	c.WaitAccepting(tm.settleBudget())
	return c
}

// settleBudget bounds each of New's waits. A cold start waits out the join
// window before the designated node bootstraps, and readiness then wants the
// topology stable: 30 s and 5 s at the product's values.
func (t clusterTimings) settleBudget() time.Duration {
	return 30*time.Second + time.Duration(t.JoinTimeoutMs+t.ReadinessStableMs)*time.Millisecond
}

// Addresses lists the client addresses in node order.
// envInt reads a positive integer knob from the environment, or the default.
func envInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

func (c *Cluster) Addresses() []string {
	out := make([]string, 0, len(c.Nodes))
	for _, n := range c.Nodes {
		out = append(out, n.TCP)
	}
	return out
}

// Node returns a node by name.
func (c *Cluster) Node(name string) *Node { return c.byName[name] }

// NodeAt returns the node serving a client address, or nil.
func (c *Cluster) NodeAt(addr string) *Node { return c.byAddr[addr] }

// Server exposes a running node's broker, or nil.
func (n *Node) Server() *broker.Server {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.running.Load() {
		return nil
	}
	return n.srv
}

// Running reports whether the node is up.
func (n *Node) Running() bool { return n.running.Load() }

// timings resolves what every node of the cluster is configured with: the
// set's values at the cluster's ttl, then the per-knob variables and the
// scenario's drain confirmation. It is the only place they are resolved: a
// measurement's provenance record reads them through it too, so the record
// cannot disagree with the nodes.
func (c *Cluster) timings() clusterTimings {
	t := setTimings(c.o.Timings, c.o.TTL)
	// Per-knob variables override the set, so a measurement can vary one
	// timing (WI 1342: how much of a storm's election churn is the harness's
	// own aggressiveness).
	t.HeartbeatMs = envInt("HOARDMQ_SIM_HEARTBEAT_MS", t.HeartbeatMs)
	t.HeartbeatMisses = envInt("HOARDMQ_SIM_HEARTBEAT_MISSES", t.HeartbeatMisses)
	t.RaftTickMs = envInt("HOARDMQ_SIM_RAFT_TICK_MS", t.RaftTickMs)
	t.TakeoverTimeoutMs = envInt("HOARDMQ_SIM_TAKEOVER_TIMEOUT_MS", t.TakeoverTimeoutMs)
	t.AckTimeoutMs = envInt("HOARDMQ_SIM_ACK_TIMEOUT_MS", t.AckTimeoutMs)
	if c.o.DrainConfirmTimeout > 0 {
		t.DrainConfirmTimeoutMs = int(c.o.DrainConfirmTimeout / time.Millisecond)
	}
	return t
}

// applyTimings writes the cluster's timings into a node's configuration.
func (c *Cluster) applyTimings(doc *config.Document) {
	// HOARDMQ_SIM_WAL_GROUP_DELAY_MS sets the journal's group-commit hold
	// for a measurement (WI 1292 round 4 compares 0 and 1 ms at 100k); the
	// harness builds its configuration directly, so the broker's env
	// overrides do not reach it.
	if v := os.Getenv("HOARDMQ_SIM_WAL_GROUP_DELAY_MS"); v != "" {
		if ms, err := strconv.Atoi(v); err == nil {
			doc.Node.WALGroupMaxDelayMs = ms
		}
	}
	t := c.timings()
	doc.Cluster.HeartbeatMs = t.HeartbeatMs
	doc.Cluster.HeartbeatMisses = t.HeartbeatMisses
	doc.Cluster.Raft.TickMs = t.RaftTickMs
	doc.Cluster.Leases.TTLMs = int(t.TTL / time.Millisecond)
	doc.Cluster.Leases.TakeoverTimeoutMs = t.TakeoverTimeoutMs
	doc.Cluster.Replication.AckTimeoutMs = t.AckTimeoutMs
	doc.Cluster.Raft.JoinTimeoutMs = t.JoinTimeoutMs
	doc.Limits.ConnectTimeout = t.ConnectTimeoutSec
	doc.Cluster.Readiness.StableMs = t.ReadinessStableMs
	doc.Cluster.Drain.RatePerSecond = t.DrainRatePerSecond
	doc.Cluster.Drain.TimeoutMs = t.DrainTimeoutMs
	doc.Cluster.Drain.ConfirmTimeoutMs = t.DrainConfirmTimeoutMs
}

func (c *Cluster) config(n *Node) config.Document {
	doc := config.Default()
	doc.Node.Name = n.Name
	doc.Node.DataDir = filepath.Join(n.Dir, "data")
	doc.Auth.Anonymous = true
	c.applyTimings(&doc)
	doc.Listeners = []config.Listener{{Name: "tcp", Type: "tcp", Address: n.TCP}}
	acl := filepath.Join(n.Dir, "acl.json")
	_ = os.MkdirAll(n.Dir, 0o750)
	_ = os.WriteFile(acl, []byte(`{"rules":[{"topic":"#","allow":["publish","subscribe","unsubscribe"]}]}`), 0o600)
	doc.Modules = map[string]json.RawMessage{"authz.file": json.RawMessage(`{"path":"` + acl + `"}`)}
	if c.jwt != nil {
		doc.Auth.Anonymous = false
		doc.Modules["auth.jwt"] = jwtModule(c.jwt)
	}
	if c.directory != nil {
		doc.Auth.Anonymous = false
		doc.Modules["auth.ldap"] = ldapModule(c.directory, c.directoryPassword)
	}
	if c.users != "" {
		// Anonymous stays on: the probes and every client outside the users
		// file connect without credentials beside the population.
		doc.Modules["auth.file"] = json.RawMessage(`{"path":"` + c.users + `"}`)
	}
	c.knobs.apply(&doc)
	doc.Cluster.Listen = n.Cluster
	doc.Cluster.Auth = config.ClusterAuth{Mode: "secret", SecretFile: c.secret}
	if c.o.SnapshotEntries > 0 {
		doc.Cluster.Raft.SnapshotEntries = c.o.SnapshotEntries
		doc.Cluster.Raft.SnapshotTrailing = snapshotTrailing(c.o.SnapshotEntries)
	}
	doc.Cluster.Replicas = c.o.Replicas
	doc.Cluster.Shards = c.o.Shards
	doc.Cluster.Advertise = n.TCP
	doc.Observability.Address = n.Obs
	for _, other := range c.Nodes {
		if other != n {
			doc.Cluster.Peers = append(doc.Cluster.Peers, config.Peer{Name: other.Name, Address: other.Cluster})
		}
	}
	if p := doc.Validate(); len(p) != 0 {
		c.t.Fatalf("harness configuration: %v", p)
	}
	return doc
}

// clusterStartAttempts bounds how many times New starts a cluster whose
// drawn addresses were taken before the nodes bound them.
const clusterStartAttempts = 3

// drawAddresses gives every node new drawn addresses; given listener
// addresses are kept.
func (c *Cluster) drawAddresses() {
	// Before any draw, and on every redraw: a restart must not draw a
	// colliding port either.
	reserveListenPorts(c.o.ListenAddrs)
	c.byAddr = map[string]*Node{}
	for i, n := range c.Nodes {
		n.TCP, n.Cluster, n.Obs = drawPort(c.t), drawPort(c.t), drawPort(c.t)
		if len(c.o.ListenAddrs) > 0 {
			n.TCP = c.o.ListenAddrs[i]
		}
		c.byAddr[n.TCP] = n
	}
}

// startAll starts every node in order. On the first failure it stops the
// nodes it started and returns the error, naming the node.
func (c *Cluster) startAll() error {
	for i, n := range c.Nodes {
		if err := c.start(n); err != nil {
			for _, started := range c.Nodes[:i] {
				c.Stop(started)
			}
			return fmt.Errorf("%s start: %w", n.Name, err)
		}
	}
	return nil
}

// Start runs a node (fresh, or over the data its last run left).
func (c *Cluster) Start(n *Node) {
	c.t.Helper()
	if err := c.start(n); err != nil {
		c.t.Fatalf("%s start: %v", n.Name, err)
	}
}

// start is Start returning the error.
func (c *Cluster) start(n *Node) error {
	if n.running.Load() {
		return nil
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if c.o.Log != nil {
		logger = slog.New(slog.NewTextHandler(c.o.Log, &slog.HandlerOptions{Level: slog.LevelDebug})).With("node", n.Name)
	}
	hooks := broker.Hooks{
		Clock:            func() time.Time { return time.Now().Add(time.Duration(n.skew.Load())) },
		Steady:           time.Now,
		Defects:          c.o.Defects,
		AdoptionDelay:    c.o.AdoptionDelay,
		DrainGate:        c.o.DrainGate,
		LoadGate:         c.o.LoadGate,
		StartGate:        c.o.StartGate,
		ForwardApplyGate: c.o.ForwardApplyGate,
	}
	if g := c.o.ImportGate; g != nil {
		hooks.ImportGate = func(ns, id string) { g(n.Name, ns, id) }
	}
	if g := c.o.AcquireGate; g != nil {
		hooks.AcquireGate = func(ns, id string, granted bool) { g(n.Name, ns, id, granted) }
	}
	if c.o.TakeoverDelay != nil {
		hooks.TakeoverDelay = c.o.TakeoverDelay(n.Name)
	}
	var srv *broker.Server
	var err error
	nodeLabels(n.Name, func() {
		srv, err = broker.NewWithHooks(c.config(n), logger, hooks)
		if err == nil {
			err = srv.Start()
		}
	})
	if err != nil {
		return err
	}
	n.mu.Lock()
	n.srv = srv
	n.mu.Unlock()
	n.running.Store(true)
	return nil
}

// TryStart is Start for a node expected to refuse (a corrupted log); it
// returns the start error instead of failing the test.
func (c *Cluster) TryStart(n *Node) error {
	if n.running.Load() {
		return nil
	}
	hooks := broker.Hooks{Clock: func() time.Time { return time.Now().Add(time.Duration(n.skew.Load())) }, Steady: time.Now, Defects: c.o.Defects, AdoptionDelay: c.o.AdoptionDelay, DrainGate: c.o.DrainGate, ForwardApplyGate: c.o.ForwardApplyGate}
	var srv *broker.Server
	var err error
	nodeLabels(n.Name, func() {
		srv, err = broker.NewWithHooks(c.config(n), slog.New(slog.NewTextHandler(io.Discard, nil)), hooks)
		if err == nil {
			err = srv.Start()
		}
	})
	if err != nil {
		return err
	}
	n.mu.Lock()
	n.srv = srv
	n.mu.Unlock()
	n.running.Store(true)
	return nil
}

// Crash stops a node as a power loss would.
func (c *Cluster) Crash(n *Node) {
	if !n.running.Load() {
		return
	}
	n.mu.Lock()
	srv := n.srv
	n.mu.Unlock()
	srv.Crash()
	n.running.Store(false)
}

// Stop shuts a node down gracefully (no drain).
func (c *Cluster) Stop(n *Node) { c.StopWithin(n, 10*time.Second) }

// StopWithin shuts a node down, waiting at most d for its connections and
// workers to end.
func (c *Cluster) StopWithin(n *Node, d time.Duration) {
	if !n.running.Load() {
		return
	}
	n.mu.Lock()
	srv := n.srv
	n.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	_ = srv.Shutdown(ctx)
	n.running.Store(false)
}

// Drain runs a node's graceful leave and waits for it, then stops it.
func (c *Cluster) Drain(n *Node) {
	srv := n.Server()
	if srv == nil {
		return
	}
	began := time.Now()
	srv.Drain()
	select {
	case <-srv.Drained():
		st := srv.DrainStatus()
		c.t.Logf("%s: drained in %v (sessions confirmed %d, unconfirmed %d)", n.Name, time.Since(began).Round(time.Millisecond), st.SessionsConfirmed, st.SessionsUnconfirmed)
	case <-time.After(30 * time.Second):
		c.t.Errorf("%s: drain did not complete", n.Name)
	}
	c.Stop(n)
}

// Partition cuts the transport between two nodes in both directions.
func (c *Cluster) Partition(a, b *Node) {
	if s := a.Server(); s != nil && s.Cluster() != nil {
		s.Cluster().Isolate(b.Name, true)
	}
	if s := b.Server(); s != nil && s.Cluster() != nil {
		s.Cluster().Isolate(a.Name, true)
	}
}

// Heal restores the transport between two nodes.
func (c *Cluster) Heal(a, b *Node) {
	if s := a.Server(); s != nil && s.Cluster() != nil {
		s.Cluster().Isolate(b.Name, false)
	}
	if s := b.Server(); s != nil && s.Cluster() != nil {
		s.Cluster().Isolate(a.Name, false)
	}
}

// HealAll clears every partition and fault.
func (c *Cluster) HealAll() {
	for _, a := range c.Nodes {
		for _, b := range c.Nodes {
			if a != b {
				if s := a.Server(); s != nil && s.Cluster() != nil {
					s.Cluster().Isolate(b.Name, false)
					s.Cluster().SetFault(b.Name, nil)
				}
			}
		}
		a.skew.Store(0)
	}
}

// Fault installs a frame fault from a to b.
func (c *Cluster) Fault(a, b *Node, f *transport.Fault) {
	if s := a.Server(); s != nil && s.Cluster() != nil {
		s.Cluster().SetFault(b.Name, f)
	}
}

// Skew offsets a node's lease and replication clock.
func (c *Cluster) Skew(n *Node, d time.Duration) { n.skew.Store(int64(d)) }

// Log names for disk faults.
const (
	StateLog   = "state"
	ReplicaLog = "replica"
	RaftLog    = "raft"
)

func (n *Node) logDir(name string) string {
	switch name {
	case StateLog:
		return filepath.Join(n.Dir, "data", "logs", "state")
	case ReplicaLog:
		return filepath.Join(n.Dir, "data", "replica", "state")
	default:
		return filepath.Join(n.Dir, "data", "logs", "raft")
	}
}

func lastSegment(dir string) (string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}
	var segs []string
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".seg" {
			segs = append(segs, filepath.Join(dir, e.Name()))
		}
	}
	if len(segs) == 0 {
		return "", fmt.Errorf("no segments in %s", dir)
	}
	sort.Strings(segs)
	return segs[len(segs)-1], nil
}

// TruncateTail cuts the last n bytes off a stopped node's newest log
// segment: a torn write.
func (c *Cluster) TruncateTail(node *Node, log string, n int64) error {
	seg, err := lastSegment(node.logDir(log))
	if err != nil {
		return err
	}
	info, err := os.Stat(seg)
	if err != nil {
		return err
	}
	size := max(info.Size()-n, 0)
	return os.Truncate(seg, size)
}

// Segment layout of the write-ahead log, as the harness must know it to
// corrupt a record by hand: a 32-byte file header, then records of a 4-byte
// length, a 1-byte type and a 4-byte checksum, followed by the payload.
const (
	segHeaderBytes    = 32
	recordHeaderBytes = 4 + 1 + 4
	recordCRCOffset   = 5
)

// CorruptRecord flips a bit in the stored checksum of the last record in a
// stopped node's newest log segment. The payload is left intact, so the
// checksum is the only thing that can catch the record: the sound build
// refuses to start and a build that skips the checksum starts over it, which
// is what the fail-closed property and its negative control are about.
func (c *Cluster) CorruptRecord(node *Node, log string) error {
	seg, err := lastSegment(node.logDir(log))
	if err != nil {
		return err
	}
	data, err := os.ReadFile(seg) //nolint:gosec // a path this harness built
	if err != nil {
		return err
	}
	last := int64(-1)
	for off := int64(segHeaderBytes); off+recordHeaderBytes <= int64(len(data)); {
		length := int64(binary.BigEndian.Uint32(data[off : off+4]))
		end := off + recordHeaderBytes + length
		if length < 0 || end > int64(len(data)) {
			break // a torn tail: not a whole record
		}
		last = off
		off = end
	}
	if last < 0 {
		return fmt.Errorf("no whole record in %s", seg)
	}
	f, err := os.OpenFile(seg, os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	at := last + recordCRCOffset
	b := data[at] ^ 0x01
	_, err = f.WriteAt([]byte{b}, at)
	return err
}

// Running lists the running nodes in name order.
func (c *Cluster) Running() []*Node {
	var out []*Node
	for _, n := range c.Nodes {
		if n.running.Load() {
			out = append(out, n)
		}
	}
	return out
}

// WaitSettled waits until every running node is ready with the map computed
// over the running set and every running peer connected.
func (c *Cluster) WaitSettled(d time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(d)
	var last string
	for time.Now().Before(deadline) {
		if last = c.unsettled(); last == "" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatalf("cluster did not settle: %s", last)
}

// WaitAccepting waits until every running node answers a QoS 1 publish
// without an error. It is the one thing a test that publishes needs, and the
// one thing no predicate over the cluster's own state supplies: a node that
// has just started can have every replication frame it has sent to a peer
// fail to send, and while its failures are as many as its sends the barrier
// refuses every QoS 1 publish — with each peer up, every owner's routes known
// and no holed stream, so WaitServing is already satisfied.
//
// Measured on this: two gate tests that publish immediately after New were
// refused 6 times in 100 runs under the load of a whole-module gate, always
// "a frame was not delivered" to one peer (WI 1661).
func (c *Cluster) WaitAccepting(d time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(d)
	for _, n := range c.Running() {
		var code packet.ReasonCode
		var err error
		for attempt := 0; ; attempt++ {
			id := fmt.Sprintf("accepting-%s-%d", n.Name, attempt)
			if code, err = ProbePublish(n.TCP, id, "liveness/accepting"); err == nil && !code.IsError() {
				break
			}
			if !time.Now().Before(deadline) {
				c.t.Fatalf("%s did not accept a publish within %s: %s %v", n.Name, d, code, err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// Settled reports whether the cluster is settled now.
func (c *Cluster) Settled() bool { return c.unsettled() == "" }

// WaitServing waits for a settled cluster that will also take a publish: one
// where every node knows every owner's routes and no replication stream
// carries a hole. Both are refusals a publisher meets — WI 1351's, and a
// barrier over a stream awaiting its resync — and a scenario that publishes
// at the return of a wait would otherwise race them (WI 1570).
//
// It is not what WaitSettled means, and deliberately so: a run that heals a
// fault measures how long a node takes to accept a publish *after* the heal
// (WI 1519), and that measurement needs the healed instant, not the instant
// the refusals stopped.
func (c *Cluster) WaitServing(d time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(d)
	var last string
	for time.Now().Before(deadline) {
		if last = c.notServing(); last == "" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatalf("cluster did not reach a state where a publish is taken: %s", last)
}

func (c *Cluster) notServing() string {
	if s := c.unsettled(); s != "" {
		return s
	}
	for _, n := range c.Running() {
		s := n.Server()
		if s == nil {
			continue
		}
		if r := s.Routes(); r != nil && !r.OwnersKnown() {
			return n.Name + " does not know every owner's routes"
		}
		if holed := s.Replication().HoledStreams(); len(holed) > 0 {
			return fmt.Sprintf("%s has a holed stream to %v", n.Name, holed)
		}
	}
	return ""
}

func (c *Cluster) unsettled() string {
	running := c.Running()
	if len(running) == 0 {
		return "no node running"
	}
	names := map[string]bool{}
	for _, n := range running {
		names[n.Name] = true
	}
	for _, n := range running {
		s := n.Server()
		if s == nil {
			return n.Name + " stopped"
		}
		if ready, reason := s.Readiness(); !ready {
			return n.Name + " not ready: " + reason
		}
		for _, p := range s.Cluster().Peers() {
			if names[p.Name] && p.State != transport.Up {
				return n.Name + " sees " + p.Name + " " + p.State.String()
			}
		}
		m := s.Replication().Map().Current()
		if m.Version == 0 || len(m.Basis) != len(running) {
			return fmt.Sprintf("%s map basis %v", n.Name, m.Basis)
		}
		for _, b := range m.Basis {
			if !names[b] {
				return n.Name + " map names stopped node " + b
			}
		}
	}
	return ""
}

var portRNG = rand.New(rand.NewSource(time.Now().UnixNano())) //nolint:gosec // port choice
var portMu sync.Mutex

// portWindow is the range harness node addresses are drawn from: outside
// the kernel's ephemeral range, so a node's address stays free across its
// restarts and can never be taken by a client's source port in between.
// The ephemeral range is read from the kernel rather than assumed — it
// differs between machines (32768–60999 here, 10000–65535 on a host tuned
// for load testing), and a node port inside it is a bind failure waiting
// for the next restart.
func portWindow() (lo, hi int) {
	elo, ehi := 32768, 60999
	if data, err := os.ReadFile("/proc/sys/net/ipv4/ip_local_port_range"); err == nil {
		var a, b int
		if n, _ := fmt.Sscan(string(data), &a, &b); n == 2 && a > 0 && b >= a {
			elo, ehi = a, b
		}
	}
	switch {
	case elo > 2048:
		return 1024, elo - 1
	case ehi < 64511:
		return ehi + 1, 65535
	default:
		return 0, 0
	}
}

// drawPort is where New and restarts-on-new-addresses get a node address;
// tests replace it to force a collision.
var drawPort = freePort

// portWindowOf is the window freePort draws from; tests narrow it.
var portWindowOf = portWindow

// handedPorts holds every port freePort has returned in this process, so two
// draws never return the same port before either is bound. Guarded by
// portMu.
var handedPorts = map[int]bool{}

// reserveListenPorts keeps the given listener addresses' ports out of the
// draw. freePort checks a port by binding 127.0.0.1, but a listener address
// given here is on another interface — in the remote-client mode every node
// listens on 10.231.14.x:8001 — so that bind succeeds and the port is handed
// out as a cluster address anyway.
//
// The consequence was not a clash on the wire: the cluster transport bound
// 127.0.0.1:8001 quite happily beside the client listeners. It was that the
// evidence sampler filters `ss` by the cluster's ports, so one node's cluster
// port being 8001 made that filter match every client connection to every
// node. At 250,000 clients one run's artifact reached 937 MB and each sample
// took 286 ms to read (WI 1511).
func reserveListenPorts(addrs []string) {
	portMu.Lock()
	defer portMu.Unlock()
	for _, a := range addrs {
		_, p, err := net.SplitHostPort(a)
		if err != nil {
			continue
		}
		if n, err := strconv.Atoi(p); err == nil {
			handedPorts[n] = true
		}
	}
}

// freePort reserves an address outside the kernel's ephemeral range.
func freePort(t testing.TB) string {
	t.Helper()
	lo, hi := portWindowOf()
	if hi-lo < 100 {
		t.Fatalf("no port range outside the kernel's ephemeral ports (%d-%d) for the harness to bind nodes in", lo, hi)
	}
	for i := 0; i < 500; i++ {
		portMu.Lock()
		port := lo + portRNG.Intn(hi-lo)
		if handedPorts[port] {
			portMu.Unlock()
			continue
		}
		// Claimed before the bind check, so a concurrent draw cannot pass the
		// same check for the same port.
		handedPorts[port] = true
		portMu.Unlock()
		addr := fmt.Sprintf("127.0.0.1:%d", port)
		l, err := net.Listen("tcp", addr)
		if err != nil {
			portMu.Lock()
			delete(handedPorts, port)
			portMu.Unlock()
			continue
		}
		_ = l.Close()
		return addr
	}
	t.Fatal("no free port in range")
	return ""
}
