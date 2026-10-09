// Package broker assembles a node from its configuration: listeners, the
// authentication and authorization chains, the connection options, and the
// session manager that owns sessions and routing.
package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/happydave/hoardmq/ext"
	extstore "github.com/happydave/hoardmq/ext/store"
	"github.com/happydave/hoardmq/internal/admin"
	"github.com/happydave/hoardmq/internal/audit"
	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/cluster/consensus"
	"github.com/happydave/hoardmq/internal/cluster/consensus/etcdraft"
	"github.com/happydave/hoardmq/internal/cluster/lease"
	"github.com/happydave/hoardmq/internal/cluster/records"
	"github.com/happydave/hoardmq/internal/cluster/replication"
	"github.com/happydave/hoardmq/internal/cluster/routing"
	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/conn"
	"github.com/happydave/hoardmq/internal/dispatch"
	"github.com/happydave/hoardmq/internal/hook"
	"github.com/happydave/hoardmq/internal/listener"
	"github.com/happydave/hoardmq/internal/module"
	_ "github.com/happydave/hoardmq/internal/module/builtin" // registers the default modules
	"github.com/happydave/hoardmq/internal/otel"
	"github.com/happydave/hoardmq/internal/session"
	"github.com/happydave/hoardmq/internal/store"
	"github.com/happydave/hoardmq/internal/trace"
)

// Server is one broker node.
type Server struct {
	// doc is the configuration in force; a reload replaces it whole.
	doc      atomic.Pointer[config.Document]
	log      *slog.Logger
	opts     conn.Options
	core     conn.Core
	sessions *session.Manager
	store    module.StoreProvider
	registry *module.Registry
	router   *dispatch.Router
	checkpts chan struct{}
	// audit is installed after the cluster transport is already running —
	// the transport must bind before anything is created on disk — so the
	// transport's own goroutines can read it while Start writes it.
	audit     atomic.Pointer[audit.Log]
	metrics   hook.Metrics
	cluster   *transport.Transport
	consensus *etcdraft.Core
	// coreReady and leasesReady are consensus and leases as the routing
	// plane's callbacks read them; see members.
	coreReady   atomic.Pointer[etcdraft.Core]
	leasesReady atomic.Pointer[lease.Service]
	// records is the replicated records store, set once the core is open.
	records  atomic.Pointer[records.Store]
	leases   *lease.Service
	routes   *routing.Plane
	replica  *store.Store
	repl     *replication.Replicator
	drainer  *drainer
	topology *topology
	hooks    Hooks
	ready    atomic.Bool

	// passwords is the node's cache of password verifications; admit is the
	// connection's gate (the connect-rate limits in force, and overload);
	// overload the stress controller; policy the ban list, the administered
	// limits and flapping detection.
	passwords *auth.VerifyCache
	admit     *admission
	overload  *overload
	policy    *policy

	listeners []*listener.Listener
	// The admin API: built in New when admin.address is set, bound in
	// Start's bind phase, served once Start has finished.
	adminAPI  *admin.Server
	adminLn   net.Listener
	adminHTTP *http.Server
	// admins forwards admin requests for clients other nodes hold, and serves
	// theirs; nil without a cluster transport. searching lets one client
	// search walk the lease table at a time.
	admins    *adminForwarder
	searching chan struct{}
	mu        sync.Mutex
	conns     map[*conn.Conn]struct{}
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	started   bool

	// reloadMu orders reloads, and their start and end: serving is true from
	// the end of Start to the start of Shutdown, when a reload may run.
	reloadMu sync.Mutex
	source   *ConfigSource
	serving  bool
}

// New builds a server from a validated configuration: the module registry
// is provisioned (which reads the users and ACL files a module names) but
// nothing is bound, opened for writing or started.
func New(cfg config.Document, log *slog.Logger) (*Server, error) {
	return NewWithHooks(cfg, log, Hooks{})
}

// NewWithHooks is New with the simulation harness's seams attached.
func NewWithHooks(cfg config.Document, log *slog.Logger, seams Hooks) (*Server, error) {
	if log == nil {
		log = slog.Default()
	}
	s := &Server{log: log, conns: map[*conn.Conn]struct{}{}, hooks: seams, searching: make(chan struct{}, 1)}
	s.doc.Store(&cfg)
	s.drainer = newDrainer(s)
	s.passwords = auth.NewVerifyCache(time.Duration(cfg.Auth.Cache.TTLSeconds)*time.Second, cfg.Auth.Cache.MaxEntries)
	auth.SetArgon2MemoryBytes(cfg.Auth.Argon2MemoryBytes)
	reg, problems := module.New(cfg, moduleContext(cfg, log, services{s: s}, seams, s.passwords), hookPolicy(cfg))
	if len(problems) > 0 {
		msgs := make([]string, 0, len(problems))
		for _, p := range problems {
			msgs = append(msgs, p.String())
		}
		return nil, errors.New(strings.Join(msgs, "; "))
	}
	s.registry = reg
	var hooks hook.Interceptor
	if h := reg.Hooks(); !h.Empty() {
		hooks = h
	}
	if m := reg.Metrics(); m != nil {
		s.metrics = m
		reg.ShareMetrics(m)
	}
	s.opts = conn.Options{
		Capabilities:              cfg.MQTT.Capabilities(),
		MaxPacketSize:             uint32(cfg.Limits.MaxPacketSize),
		ReceiveMaximum:            uint16(cfg.Limits.ReceiveMaximum),
		MaxKeepAlive:              uint16(cfg.Limits.MaxKeepAlive),
		ConnectTimeout:            time.Duration(cfg.Limits.ConnectTimeout) * time.Second,
		Anonymous:                 cfg.Auth.Anonymous,
		Authenticators:            reg.Authenticators(),
		Authorizers:               reg.Authorizers(),
		CertificateAuthenticators: reg.CertificateAuthenticators(),
		Mechanisms:                reg.Mechanism,
		DecisionTTL:               time.Duration(cfg.Auth.DecisionTTLSeconds) * time.Second,
		Namespace:                 "default",
		Logger:                    log,
		Hooks:                     hooks,
		Metrics:                   s.metrics,
		TraceEvents:               cfg.Observability.TraceEvents,
	}
	s.admit = &admission{}
	s.overload = newOverload(s, cfg.Overload, seams)
	s.admit.overload = s.overload
	s.policy = newPolicy(s, seams)
	s.opts.ConnectGate = s.admit
	s.opts.Banned = s.policy.Banned
	s.opts.ClientLimits = s.policy.ClientLimits
	s.opts.OnEnded = s.policy.flap.ended
	s.opts.OnWritable = func(c *conn.Conn) {
		if m := s.sessions; m != nil {
			m.Resume(c)
		}
	}
	s.router = dispatch.New(dispatch.Strategy(cfg.MQTT.SharedSubscriptionStrategy))
	s.checkpts = make(chan struct{})
	tr, err := newTransport(cfg, log, s.metrics, func(action, target, outcome string, detail map[string]string) {
		s.record("cluster", action, target, outcome, detail)
	}, seams.Defects.ReorderForwards, framingVersion(seams, cfg.Node.Name))
	if err != nil {
		return nil, err
	}
	s.cluster = tr
	if tr != nil {
		s.admins = newAdminForwarder(s)
		// The routing plane rides the transport; it learns the local
		// subscriptions from the dispatcher and forwards for the sessions.
		s.routes = routing.New(routing.Options{
			Node:              cfg.Node.Name,
			Transport:         tr,
			Strategy:          dispatch.Strategy(cfg.MQTT.SharedSubscriptionStrategy),
			SyncInterval:      time.Duration(cfg.Cluster.Routing.SyncMs) * time.Millisecond,
			MoveGrace:         time.Duration(cfg.Cluster.Leases.TakeoverTimeoutMs) * time.Millisecond,
			Members:           s.members,
			OwnedSessions:     s.ownedSessions,
			Serving:           s.forwardsServed,
			AwaitLoad:         !seams.Defects.SyncBeforeLoad,
			AdoptionState:     s.adoptionState,
			LeaseIndex:        s.leaseIndex,
			EmptiedAt:         s.emptiedAt,
			ServedFact:        s.servedFact,
			ProposeServed:     s.proposeServed,
			AdoptedThrough:    s.adoptedThrough,
			Cover:             s.cover,
			CoverDue:          s.coverDue,
			ProposeCover:      s.proposeCover,
			Logger:            log,
			Metrics:           s.metrics,
			Tracer:            reg.Tracer(),
			IgnoreForwardGaps: seams.Defects.IgnoreForwardGaps,
			ApplyGate:         seams.ForwardApplyGate,
			GiveTimeout:       time.Duration(cfg.Cluster.Replication.AckTimeoutMs) * time.Millisecond,
		})
		s.router.SetObserver(s.routes.Observe)
		s.router.SetGroupDecider(s.routes.Decide)
	}
	if cfg.Admin.Address != "" {
		api, err := admin.New(cfg.Admin, adminBackend{s: s}, log)
		if err != nil {
			return nil, err
		}
		if d := seams.AdminWriteTimeout; d > 0 {
			api.SetWriteTimeout(d)
		}
		s.adminAPI = api
	}
	return s, nil
}

// moduleContext is what modules receive at provision. For a read-only build
// (hoardmq check) svc and passwords are nil: modules get no services, and a
// password verifier that caches nothing.
func moduleContext(cfg config.Document, log *slog.Logger, svc module.Services, hooks Hooks, passwords *auth.VerifyCache) *module.Context {
	return &module.Context{
		Logger:    log,
		Node:      cfg.Node.Name,
		Namespace: "default",
		DataDir:   cfg.Node.DataDir,
		Store: extstore.Options{
			SyncPolicy:     cfg.Node.WALSync,
			SyncInterval:   time.Duration(cfg.Node.WALSyncIntervalMs) * time.Millisecond,
			GroupMaxDelay:  time.Duration(cfg.Node.WALGroupMaxDelayMs) * time.Millisecond,
			GroupMaxSize:   cfg.Node.WALGroupMaxSize,
			SegmentBytes:   cfg.Node.WALSegmentBytes,
			Sync:           hooks.Sync,
			IgnoreChecksum: hooks.Defects.IgnoreChecksum,
		},
		Services:      svc,
		Passwords:     passwords,
		Observability: module.Observability{Address: cfg.Observability.Address, MetricsPath: cfg.Observability.MetricsPath, AdminOwnsDrain: cfg.Admin.Address != ""},
	}
}

func hookPolicy(cfg config.Document) hook.Policy {
	p := hook.Policy{Timeout: time.Duration(cfg.Hooks.TimeoutMs) * time.Millisecond}
	if cfg.Hooks.Fallback == "disconnect" {
		p.Fallback = hook.Disconnect
	}
	return p
}

// ValidateModules provisions the configured modules read-only and returns
// every problem, for hoardmq check.
func ValidateModules(cfg config.Document, log *slog.Logger) []module.Problem {
	_, problems := module.New(cfg, moduleContext(cfg, log, nil, Hooks{}, nil), hookPolicy(cfg))
	return problems
}

// members and ownedSessions answer the routing plane, which calls them from
// transport goroutines that Start brings up before it opens the consensus
// core. They read the core and the lease service through atomics for that
// reason; every other reader runs on a goroutine Start launches afterwards.
func (s *Server) members() []string {
	rc := s.coreReady.Load()
	if rc == nil {
		return nil
	}
	var names []string
	for _, m := range rc.Members() {
		names = append(names, m.Name)
	}
	return names
}

// forwardsAdmitted decides whether this node takes forwarded QoS 1 and 2
// deliveries into its sessions: only once replication is attached, so a
// delivery is acknowledged on the replicas and not on the local journal
// alone, and only once the consensus core has recovered, so the lease table
// says which of the sessions the store loaded are still this node's.
func forwardsAdmitted(attached, recovering bool) bool { return attached && !recovering }

// forwardsServed is the routing plane's Serving.
func (s *Server) forwardsServed() bool {
	rc := s.coreReady.Load()
	return s.hooks.Defects.ForwardsBeforeServing || forwardsAdmitted(rc != nil, rc != nil && rc.Recovering())
}

// ownedSessions bounds the unknown-owner refusal: an owner the replicated
// lease state says holds nothing needs no routes. Recovering means the state
// has not been replayed yet, so nothing about it can be proved.
func (s *Server) ownedSessions(node string) (int, bool) {
	rc, svc := s.coreReady.Load(), s.leasesReady.Load()
	if svc == nil || rc == nil || rc.Recovering() {
		return 0, false
	}
	return svc.Table().CountOwnedBy(node), true
}

// adoptionState is the routing plane's view of this node's adoptions; not
// known until the lease service exists and the core has recovered.
func (s *Server) adoptionState() (uint64, int, bool) {
	rc, svc := s.coreReady.Load(), s.leasesReady.Load()
	if svc == nil || rc == nil || rc.Recovering() {
		return 0, 0, false
	}
	index, unfinished := svc.AdoptionState()
	return index, unfinished, true
}

func (s *Server) emptiedAt(node string) uint64 {
	if svc := s.leasesReady.Load(); svc != nil {
		return svc.LostAt(node)
	}
	return 0
}

func (s *Server) servedFact(node string) (uint64, uint64) {
	if svc := s.leasesReady.Load(); svc != nil {
		return svc.ServedFact(node)
	}
	return 0, 0
}

func (s *Server) adoptedThrough() uint64 {
	if svc := s.leasesReady.Load(); svc != nil {
		return svc.AdoptedThrough()
	}
	return 0
}

func (s *Server) proposeServed(ctx context.Context, index, routes uint64) error {
	svc := s.leasesReady.Load()
	if svc == nil {
		return errors.New("the lease service is not running")
	}
	return svc.ProposeServed(ctx, index, routes)
}

func (s *Server) cover(node string) (uint64, uint64) {
	if svc := s.leasesReady.Load(); svc != nil {
		return svc.Cover(node)
	}
	return 0, 0
}

func (s *Server) coverDue() (uint64, bool) {
	if svc := s.leasesReady.Load(); svc != nil {
		return svc.CoverDue()
	}
	return 0, false
}

func (s *Server) proposeCover(ctx context.Context, grant, routes uint64) error {
	svc := s.leasesReady.Load()
	if svc == nil {
		return errors.New("the lease service is not running")
	}
	return svc.ProposeCover(ctx, grant, routes)
}

func (s *Server) leaseIndex() uint64 {
	if svc := s.leasesReady.Load(); svc != nil {
		return svc.AppliedIndex()
	}
	return 0
}

// Registry exposes the module registry, for tests and the admin surface.
func (s *Server) Registry() *module.Registry { return s.registry }

// StormStats is the admission and lease-work picture of this node, read
// without any lock a reconnect storm contends for: the listeners' handshake
// slots and pause time, and the lease service's work queues. A metrics
// snapshot walks the session table under its lock and has been seen to wait
// minutes under a storm (WI 1292); a harness reads this instead.
type StormStats struct {
	Listeners []listener.Stats  `json:"listeners"`
	LeaseWork []lease.WorkStats `json:"lease_work,omitempty"`
	// Sync is the journal's group-commit picture (WI 1292 round 4).
	Sync *store.SyncStats `json:"sync,omitempty"`
	// Lease is the lease service's liveness picture.
	Lease *lease.LivenessStats `json:"lease,omitempty"`
	// Takeover divides what a takeover pull waits for, as the requester and
	// as the previous owner (WI 1691).
	Takeover *lease.TakeoverStats `json:"takeover,omitempty"`
	// ReplicationFetch is this node's replica fetches, stage by stage, as
	// the node that asked and as the holder that answered.
	ReplicationFetch *replication.FetchStats `json:"replication_fetch,omitempty"`
	// ReplicationBatches is the big batches this node's per-peer replication
	// workers took, by frame kind.
	ReplicationBatches *replication.BatchStats `json:"replication_batches,omitempty"`
	// StoreAppend and ReplicaAppend are what each store's lock was held for
	// by appends; ReplicationTap is the tap's share of the first, by part.
	// ReplicationDrop is the owned-copy drop, which an upsert asks for inside
	// that lock and which runs once it is released (WI 1688).
	StoreAppend     *store.AppendStats     `json:"store_append,omitempty"`
	ReplicaAppend   *store.AppendStats     `json:"replica_append,omitempty"`
	ReplicationTap  *replication.TapStats  `json:"replication_tap,omitempty"`
	ReplicationDrop *replication.DropStats `json:"replication_drop,omitempty"`
	// ReplicationMaps is what this node saw of each recent shard-map version
	// and why its driver checked the basis; RoutingForgets what its served
	// checks saw of each lost owner (WI 1791).
	ReplicationMaps *replication.MapTimeline `json:"replication_maps,omitempty"`
	RoutingForgets  []routing.ForgetEvent    `json:"routing_forgets,omitempty"`
	// ReplicationStreams is what each replication stream to and from a peer
	// was sent, refused and failed, over time (WI 1802).
	ReplicationStreams []replication.StreamTrace `json:"replication_streams,omitempty"`
	// RoutingRefusals samples the forward refusals this node made or was told
	// of, with the moving filters it knew of then (WI 1803).
	RoutingRefusals *routing.RefusalTrace `json:"routing_refusals,omitempty"`
	// OwnerKnowledge is the last spans in which this node lacked an owner's
	// routes and refused QoS 1 and 2 publishes for it (WI 1571).
	OwnerKnowledge []routing.Episode `json:"owner_knowledge,omitempty"`
	// Displaced is connections closed because a second CONNECT arrived for
	// the same client identifier on this node — a SessionTakenOver that no
	// lease movement explains (WI 1495).
	Displaced int64 `json:"displaced"`
	// Connections this node closed, by the call that closed them. These plus
	// Displaced account for every SessionTakenOver a client here can see.
	ClosedTake   int64 `json:"closed_take"`
	ClosedEvict  int64 `json:"closed_evict"`
	ClosedDetach int64 `json:"closed_detach"`
	// The subset of ClosedEvict that came from importing an adopted session.
	ClosedEvictImport int64 `json:"closed_evict_import"`
	// Imports and ImportMs are the sessions imported and the time that took,
	// journal sync included.
	Imports  int64   `json:"imports"`
	ImportMs float64 `json:"import_ms_total"`
	// SealWaitsImport and ImportWaitsImport count Seals and imports that
	// waited for an import of the same client to finish.
	SealWaitsImport   int64 `json:"seal_waits_import"`
	ImportWaitsImport int64 `json:"import_waits_import"`
	// Consensus is the core's view — role, term, leader, commit/applied —
	// and Peers the transport's, so a storm's timeline shows whether a
	// leader exists and whether the links are up (WI 1337 round 5c: every
	// renewal failed for want of a leader and nothing said why).
	Consensus *consensus.Status    `json:"consensus,omitempty"`
	Peers     []transport.PeerInfo `json:"peers,omitempty"`
	// PasswordCache is what the password-verification cache has done, and
	// ConnectRateRefused the CONNECTs the connect-rate limits refused, by
	// "<listener>/<limit>" (WI 1283).
	PasswordCache      auth.VerifyStats `json:"password_cache"`
	ConnectRateRefused map[string]int64 `json:"connect_rate_refused,omitempty"`
	// Overload is what the stress controller saw; OverloadRefused the CONNECTs
	// refused while the cluster was at level 3; BansMet the CONNECTs a ban
	// refused; FlappingTrips and FlappingBans the detector's trips and the
	// bans it wrote (WI 1314).
	Overload        overloadStats `json:"overload"`
	OverloadRefused int64         `json:"overload_refused"`
	BansMet         int64         `json:"bans_met"`
	FlappingTrips   int64         `json:"flapping_trips"`
	FlappingBans    int64         `json:"flapping_bans"`
	// Tracing is the OpenTelemetry exporter's counters, when traces.otlp is
	// configured (WI 1313).
	Tracing *otel.Stats `json:"tracing,omitempty"`
	// ReadMs is how long each part of this read took. Most parts are atomic
	// reads, but the lease liveness read takes the lease table's lock, the
	// consensus status the core's mutex, and the peers the transport's and
	// each peer's; a storm can hold any of them for seconds (WI 1474).
	ReadMs map[string]float64 `json:"read_ms,omitempty"`
}

func (s *Server) StormStats() StormStats {
	st := StormStats{ReadMs: map[string]float64{}, PasswordCache: s.passwords.Stats(), ConnectRateRefused: s.rateGate().Refused(),
		Overload: s.overload.Stats(), OverloadRefused: s.admit.refused.Load(), BansMet: s.policy.banned.Load(),
		FlappingTrips: s.policy.flap.trips.Load(), FlappingBans: s.policy.flap.written.Load()}
	if tr := s.registry.Tracer(); tr != nil {
		ts := tr.Stats()
		st.Tracing = &ts
	}
	timed := func(part string, read func()) {
		began := time.Now()
		read()
		st.ReadMs[part] = float64(time.Since(began)) / 1e6
	}
	timed("listeners", func() {
		for _, l := range s.listeners {
			st.Listeners = append(st.Listeners, l.Stats())
		}
	})
	if s.sessions != nil {
		st.Displaced = s.sessions.Displaced()
		st.ClosedTake, st.ClosedEvict, st.ClosedDetach = s.sessions.CloseCounts()
		st.ClosedEvictImport = s.sessions.EvictsFromImport()
		n, total := s.sessions.ImportCost()
		st.Imports, st.ImportMs = n, float64(total)/1e6
		st.SealWaitsImport, st.ImportWaitsImport = s.sessions.ImportWaits()
	}
	if s.leases != nil {
		timed("lease_work", func() { st.LeaseWork = s.leases.WorkStats() })
		ts := s.leases.TakeoverStats()
		st.Takeover = &ts
		timed("lease_liveness", func() {
			ls := s.leases.LivenessStats()
			st.Lease = &ls
		})
	}
	if s.repl != nil {
		fs := s.repl.FetchStats()
		st.ReplicationFetch = &fs
		bs := s.repl.BatchStats()
		st.ReplicationBatches = &bs
		ts, ra := s.repl.TapStats(), s.repl.ReplicaAppendStats()
		st.ReplicationTap, st.ReplicaAppend = &ts, &ra
		ds := s.repl.DropStats()
		st.ReplicationDrop = &ds
		mt := s.repl.MapTimeline()
		st.ReplicationMaps = &mt
		timed("replication_streams", func() { st.ReplicationStreams = s.repl.StreamTraces() })
	}
	if s.routes != nil {
		st.RoutingForgets = s.routes.ForgetTimeline()
		rt := s.routes.RefusalTrace()
		st.RoutingRefusals = &rt
		st.OwnerKnowledge, _ = s.routes.OwnerKnowledge()
	}
	if p, ok := s.store.(ext.AppendStatsProvider); ok {
		as := p.AppendStats()
		st.StoreAppend = &as
	}
	if p, ok := s.store.(ext.SyncStatsProvider); ok {
		timed("sync", func() {
			ss := p.SyncStats()
			st.Sync = &ss
		})
	}
	if s.consensus != nil {
		timed("consensus", func() {
			cs := s.consensus.Status()
			st.Consensus = &cs
		})
	}
	if s.cluster != nil {
		timed("peers", func() { st.Peers = s.cluster.Peers() })
	}
	return st
}

// LeaseFences is how many times this node's lease service has fenced, read
// from one atomic and so safe to poll during a storm; 0 with no lease
// service.
func (s *Server) LeaseFences() int64 {
	if s.leases == nil {
		return 0
	}
	return s.leases.Fences()
}

// checkpointInterval is how often the broker checks whether the store wants
// a checkpoint.
const checkpointInterval = 30 * time.Second

// openStore starts the modules (the store provider opens its log in Start),
// recovers the persisted state into a new session manager, and writes a
// fresh checkpoint so the next start replays little.
func (s *Server) openStore() error {
	if err := s.registry.Start(context.Background()); err != nil {
		return err
	}
	provider := s.registry.Store()
	if provider == nil {
		_ = s.registry.Stop(context.Background())
		return errors.New("no store provider configured")
	}
	state, err := provider.Recover()
	if err != nil {
		_ = s.registry.Stop(context.Background())
		return err
	}
	s.store = provider
	var observer hook.Observer
	if h := s.registry.Hooks(); !h.Empty() {
		observer = h
	}
	var metrics hook.Metrics
	if m := s.registry.Metrics(); m != nil {
		metrics = m
	}
	sessOpts := session.Options{
		Router:           s.router,
		Namespace:        "default",
		Node:             s.config().Node.Name,
		MaxQueued:        s.config().Limits.MaxQueuedMessages,
		DropNewest:       s.config().Limits.QueueDrop == "newest",
		MaxSessionExpiry: uint32(s.config().Limits.MaxSessionExpiry),
		Shards:           s.config().Node.SessionShards,
		StoreAdmission:   s.config().Node.StoreAdmission,
		Authorizers:      s.opts.Authorizers,
		Logger:           s.log,
		Journal:          provider.Journal(),
		Observer:         observer,
		Metrics:          metrics,
		Tracer:           s.registry.Tracer(),
		Ended: func(ns, id string, epoch uint64) {
			if s.leases != nil {
				s.leases.Release(ns, id, epoch)
			}
		},
	}
	if s.routes != nil {
		sessOpts.Forwarder = s.routes
		sessOpts.Moving = s.routes.MovingClient
	}
	sessOpts.ForgetInboundQoS2 = s.hooks.Defects.ForgetInbound
	sessOpts.PubrelUnreplicated = s.hooks.Defects.PubrelUnreplicated
	s.sessions = session.New(sessOpts)
	if s.routes != nil {
		s.routes.SetSink(s.sessions)
	}
	s.core = core{m: s.sessions}
	s.sessions.Load(state)
	if s.routes != nil {
		s.routes.Loaded()
	}
	s.log.Info("state recovered", "sessions", len(state.Sessions), "retained", len(state.Retained), "modules", s.registry.Modules())
	if err := provider.Checkpoint(s.sessions.CheckpointState); err != nil {
		return fmt.Errorf("checkpoint: %w", err)
	}
	return nil
}

func (s *Server) checkpointLoop() {
	defer s.wg.Done()
	t := time.NewTicker(checkpointInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			if s.store.NeedsCheckpoint() {
				if err := s.store.Checkpoint(s.sessions.CheckpointState); err != nil {
					s.log.Error("checkpoint failed", "err", err)
				}
			}
			if s.replica != nil && s.replica.NeedsCheckpoint() {
				if err := s.replica.Checkpoint(s.repl.CheckpointState); err != nil {
					s.log.Error("replica checkpoint failed", "err", err)
				}
			}
		case <-s.checkpts:
			return
		}
	}
}

// Options returns the connection options, for tests.
func (s *Server) Options() conn.Options { return s.opts }

// Sessions returns the session manager.
func (s *Server) Sessions() *session.Manager { return s.sessions }

// Start binds every listener, then creates the data directory, then begins
// accepting. Binding is all-or-nothing: a listener that cannot bind closes
// the others and nothing is created.
func (s *Server) Start() error {
	if s.started {
		return errors.New("broker already started")
	}
	for _, lc := range s.config().Listeners {
		l, err := listener.Open(lc, s.config().Limits.MaxPacketSize)
		if err != nil {
			for _, o := range s.listeners {
				_ = o.Close()
			}
			s.listeners = nil
			return err
		}
		s.listeners = append(s.listeners, l)
	}
	// The admin and cluster listeners bind in the same phase as the client
	// listeners: nothing is created on disk until every bind has succeeded.
	unbind := func() {
		for _, o := range s.listeners {
			_ = o.Close()
		}
		s.listeners = nil
		s.stopAdmin()
		if s.cluster != nil {
			s.cluster.Stop()
		}
	}
	if err := s.bindAdmin(); err != nil {
		unbind()
		return err
	}
	if s.cluster != nil {
		if err := s.cluster.Start(); err != nil {
			unbind()
			return err
		}
		s.routes.Start()
	}
	if err := os.MkdirAll(s.config().Node.DataDir, 0o750); err != nil {
		unbind()
		return fmt.Errorf("data directory: %w", err)
	}
	if s.adminAPI != nil {
		// Read only: the keys it holds go into the cluster's collection once
		// the core is open.
		if err := s.adminAPI.ReadKeyFile(filepath.Join(s.config().Node.DataDir, "admin")); err != nil {
			unbind()
			return fmt.Errorf("admin API keys: %w", err)
		}
	}
	if s.config().Observability.Audit.Enabled {
		a, err := audit.Open(filepath.Join(s.config().Node.DataDir, "audit"), audit.Options{MaxBytes: s.config().Observability.Audit.MaxBytes, MaxFiles: s.config().Observability.Audit.MaxFiles, Queue: s.config().Observability.Audit.Queue, Logger: s.log})
		if err != nil {
			unbind()
			return err
		}
		s.audit.Store(a)
		s.opts.Audit = auditor{a}
	}
	if s.hooks.LoadGate != nil {
		s.hooks.LoadGate(s.config().Node.Name)
	}
	if err := s.openStore(); err != nil {
		unbind()
		if a := s.audit.Load(); a != nil {
			_ = a.Close()
		}
		return err
	}
	if s.hooks.StartGate != nil {
		s.hooks.StartGate(s.config().Node.Name)
	}
	if err := s.openConsensus(); err != nil {
		unbind()
		_ = s.registry.Stop(context.Background())
		if a := s.audit.Load(); a != nil {
			_ = a.Close()
		}
		return fmt.Errorf("consensus: %w", err)
	}
	s.registerCollector()
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.started = true
	stable := time.Duration(s.config().Cluster.Readiness.StableMs) * time.Millisecond
	s.topology = newTopology(s.clusterFingerprint, stable, func() {
		s.count("hoardmq_topology_changes_total", nil, 1)
	})
	s.topology.start()
	s.ready.Store(true)
	if st := s.records.Load(); st != nil {
		s.policy.start(st)
		if s.adminAPI != nil {
			s.wg.Add(1)
			go s.importKeys()
		}
	}
	s.overload.start()
	s.reloadMu.Lock()
	s.serving = true
	s.reloadMu.Unlock()
	s.record("server", "server.start", s.config().Node.Name, "ok", nil)
	s.wg.Add(1)
	go s.checkpointLoop()
	for _, l := range s.listeners {
		s.log.Info("listening", "listener", l.Config.Name, "type", l.Config.Type, "address", l.Addr().String())
		s.wg.Add(1)
		go s.acceptLoop(l)
	}
	if s.cluster != nil {
		s.log.Info("cluster transport listening", "address", s.cluster.Addr().String(), "peers", len(s.config().Cluster.Peers), "auth", s.config().Cluster.Auth.Mode)
	}
	s.serveAdmin()
	s.log.Info("consensus core started", "node", s.consensus.Status().Role)
	return nil
}

func (s *Server) acceptLoop(l *listener.Listener) {
	defer s.wg.Done()
	for {
		nc, err := l.Accept()
		if err != nil {
			if !errors.Is(err, listener.ErrClosed) {
				s.log.Warn("accept failed", "listener", l.Config.Name, "err", err)
			}
			return
		}
		c := conn.New(nc, l.Config.Name, s.opts, s.core)
		s.mu.Lock()
		s.conns[c] = struct{}{}
		s.mu.Unlock()
		if s.metrics != nil {
			s.metrics.Add("hoardmq_connections_total", map[string]string{"listener": l.Config.Name}, 1)
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			c.Serve(s.ctx)
			s.mu.Lock()
			delete(s.conns, c)
			s.mu.Unlock()
		}()
	}
}

// Addr returns the bound address of a listener by name, or nil.
func (s *Server) Addr(name string) net.Addr {
	for _, l := range s.listeners {
		if l.Config.Name == name {
			return l.Addr()
		}
	}
	return nil
}

// Connections is the number of connections currently served.
func (s *Server) Connections() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}

// Shutdown stops accepting, tells level-5 clients the server is shutting
// down, and waits for connections to end until ctx expires.
func (s *Server) Shutdown(ctx context.Context) error {
	if !s.started {
		return nil
	}
	s.started = false
	s.reloadMu.Lock()
	s.serving = false
	s.reloadMu.Unlock()
	s.ready.Store(false)
	s.stopAdmin()
	s.record("server", "server.stop", s.config().Node.Name, "ok", nil)
	if s.topology != nil {
		s.topology.close()
	}
	s.overload.close()
	s.policy.close()
	for _, l := range s.listeners {
		_ = l.Close()
	}
	if s.repl != nil {
		s.repl.Stop()
	}
	if s.leases != nil {
		s.leases.Stop()
	}
	if s.consensus != nil {
		s.consensus.Stop()
	}
	if s.routes != nil {
		s.routes.Stop()
	}
	if s.cluster != nil {
		s.cluster.Stop()
	}
	s.sessions.Close()
	close(s.checkpts)
	s.cancel()
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	var err error
	select {
	case <-done:
	case <-ctx.Done():
		err = ctx.Err()
	}
	if cerr := s.store.Checkpoint(s.sessions.CheckpointState); cerr != nil {
		s.log.Error("final checkpoint failed", "err", cerr)
	}
	if s.replica != nil {
		if cerr := s.replica.Checkpoint(s.repl.CheckpointState); cerr != nil {
			s.log.Error("final replica checkpoint failed", "err", cerr)
		}
		if cerr := s.replica.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	if cerr := s.registry.Stop(context.Background()); cerr != nil && err == nil {
		err = cerr
	}
	if a := s.audit.Load(); a != nil {
		if cerr := a.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	return err
}

// auditor adapts the audit log to the hook interface.
type auditor struct{ l *audit.Log }

// Audit queues the event. auth.failure is the one event produced once per
// connection attempt, so under a reconnect storm it is dropped and counted
// rather than queued: the connection path never waits on the audit log.
// Every other event is kept; a new per-connection event must choose its
// class here deliberately.
func (a auditor) Audit(actor, action, target, outcome string, detail map[string]string) {
	e := audit.Event{Actor: actor, Action: action, Target: target, Outcome: outcome, Detail: detail}
	if action == "auth.failure" {
		_ = a.l.RecordLossy(e)
		return
	}
	_ = a.l.Record(e)
}

// count adds to a metric when a metrics provider is configured.
func (s *Server) count(name string, labels map[string]string, delta float64) {
	if s.metrics != nil {
		s.metrics.Add(name, labels, delta)
	}
}

// record queues an audit event when auditing is on. The server's and the
// cluster's own events are lossless; see auditor.Audit for the one exception.
func (s *Server) record(actor, action, target, outcome string, detail map[string]string) {
	if a := s.audit.Load(); a != nil {
		_ = a.Record(audit.Event{Actor: actor, Action: action, Target: target, Outcome: outcome, Detail: detail})
	}
}

// registerCollector sets the at-scrape gauges from the live state.
func (s *Server) registerCollector() {
	c, ok := s.registry.Metrics().(module.Collector)
	if !ok {
		return
	}
	c.AddCollector(func(set func(string, map[string]string, float64)) {
		set("hoardmq_connections_active", nil, float64(s.Connections()))
		degraded, degradedFor := s.degraded(time.Now())
		set("hoardmq_node_degraded", map[string]string{"reason": degradedRoutesUnknown}, degraded)
		set("hoardmq_node_degraded_seconds", nil, degradedFor)
		ov := s.overload.Stats()
		set("hoardmq_overload_level", map[string]string{"scope": "node"}, float64(ov.NodeLevel))
		set("hoardmq_overload_level", map[string]string{"scope": "cluster"}, float64(ov.ClusterLevel))
		set("hoardmq_overload_indicator", map[string]string{"name": "queued_messages"}, float64(ov.Indicators.Queued))
		set("hoardmq_overload_indicator", map[string]string{"name": "replication_lag"}, float64(ov.Indicators.Lag))
		set("hoardmq_overload_indicator", map[string]string{"name": "heap_bytes"}, float64(ov.Indicators.Heap))
		set("hoardmq_overload_shed_connections", nil, float64(ov.Shed))
		set("hoardmq_overload_refused_total", nil, float64(s.admit.refused.Load()))
		for how := conn.EndedGraceful; how <= conn.EndedAbnormal; how++ {
			set("hoardmq_connections_ended_total", map[string]string{"how": how.String()}, float64(s.policy.flap.ends[how].Load()))
		}
		if ix := s.policy.index.Load(); ix != nil {
			set("hoardmq_bans", nil, float64(ix.Size()))
			set("hoardmq_bans_invalid", nil, float64(ix.Invalid()))
		}
		pw := s.passwords.Stats()
		set("hoardmq_auth_password_derivations_total", nil, float64(pw.Derivations))
		set("hoardmq_auth_password_cache_hits_total", nil, float64(pw.Hits))
		set("hoardmq_auth_password_cache_entries", nil, float64(pw.Entries))
		byNS := map[string]struct{ sessions, subs int }{}
		for _, r := range s.sessions.Records() {
			e := byNS[r.Namespace]
			e.sessions++
			e.subs += r.Subscriptions
			byNS[r.Namespace] = e
		}
		if len(byNS) == 0 {
			byNS["default"] = struct{ sessions, subs int }{}
		}
		for ns, e := range byNS {
			set("hoardmq_sessions_active", map[string]string{"namespace": ns}, float64(e.sessions))
			set("hoardmq_subscriptions_active", map[string]string{"namespace": ns}, float64(e.subs))
		}
		retained := s.router.RetainedCounts()
		if _, ok := retained["default"]; !ok {
			retained["default"] = 0
		}
		for ns, n := range retained {
			set("hoardmq_retained_active", map[string]string{"namespace": ns}, float64(n))
		}
		if p, ok := s.store.(ext.JournalStatsProvider); ok {
			js := p.JournalStats()
			set("hoardmq_log_index", nil, float64(js.LastIndex))
			setJournal(set, "node", js)
		}
		if s.replica != nil {
			setJournal(set, "replica", s.replica.JournalStats())
		}
		if tr := s.registry.Tracer(); tr != nil {
			setTracing(set, tr.Stats())
		}
		if st := s.records.Load(); st != nil {
			counts := st.Counts()
			if _, ok := counts[auth.CredentialsCollection]; !ok {
				counts[auth.CredentialsCollection] = 0
			}
			for c, n := range counts {
				set("hoardmq_records", map[string]string{"collection": c}, float64(n))
			}
		}
		if s.consensus != nil {
			st := s.consensus.Status()
			set("hoardmq_raft_term", nil, float64(st.Term))
			set("hoardmq_raft_commit_index", nil, float64(st.Commit))
			set("hoardmq_raft_applied_index", nil, float64(st.Applied))
			set("hoardmq_raft_snapshot_index", nil, float64(st.SnapshotIndex))
			leader := 0.0
			if st.Role == "leader" {
				leader = 1
			}
			set("hoardmq_raft_leader", nil, leader)
			byRole := map[consensus.Role]int{consensus.Voter: 0, consensus.Learner: 0}
			for _, m := range s.consensus.Members() {
				byRole[m.Role]++
			}
			for role, n := range byRole {
				set("hoardmq_raft_members", map[string]string{"role": string(role)}, float64(n))
			}
		}
		if s.leases != nil {
			cluster, build, min := s.leases.Levels()
			set("hoardmq_cluster_feature_level", nil, float64(cluster))
			set("hoardmq_cluster_feature_level_build", nil, float64(build))
			set("hoardmq_cluster_feature_level_min_advertised", nil, float64(min))
		}
		if s.sessions != nil {
			take, evict, detach := s.sessions.CloseCounts()
			fromImport := s.sessions.EvictsFromImport()
			// The four causes the storm harness already reads, on /metrics:
			// what closed a deployment's connections, and whether the imports
			// of WI 1515 still evict any (WI 1523). "evict" excludes the
			// import's share so the two do not double-count.
			set("hoardmq_connections_closed_total", map[string]string{"cause": "take"}, float64(take))
			set("hoardmq_connections_closed_total", map[string]string{"cause": "evict"}, float64(evict-fromImport))
			set("hoardmq_connections_closed_total", map[string]string{"cause": "evict-import"}, float64(fromImport))
			set("hoardmq_connections_closed_total", map[string]string{"cause": "detach"}, float64(detach))
		}
		if rep := s.Replication(); rep != nil {
			for _, st := range rep.StreamStates() {
				// A blocked stream is why a QoS 1 or 2 publish that needs
				// that peer is refused; the age is what says it is happening
				// now rather than having happened (WI 1519's question, asked
				// of the product).
				reason, blocked := "none", 0.0
				if st.Reason != "" {
					reason, blocked = st.Reason, 1
				}
				age := st.Blocked.Seconds()
				set("hoardmq_replication_stream_blocked", map[string]string{"peer": st.Peer, "reason": reason}, blocked)
				set("hoardmq_replication_stream_blocked_seconds", map[string]string{"peer": st.Peer}, age)
			}
			for peer, n := range rep.BacklogByPeer() {
				set("hoardmq_replication_backlog_frames", map[string]string{"peer": peer}, float64(n))
			}
		}
		shortSessions, shortLed := s.shortCopies()
		set("hoardmq_replication_sessions_short", nil, float64(shortSessions))
		set("hoardmq_replication_retained_shards_short", nil, float64(shortLed))
		for id, n := range s.registry.Hooks().Failures() {
			set("hoardmq_hook_failures_total", map[string]string{"module": string(id)}, float64(n))
		}
		if a := s.audit.Load(); a != nil {
			st := a.Stats()
			set("hoardmq_audit_records_total", nil, float64(st.Written))
			set("hoardmq_audit_dropped_total", nil, float64(st.Dropped))
			set("hoardmq_audit_write_failures_total", nil, float64(st.WriteFailures))
			set("hoardmq_audit_batches_total", nil, float64(st.Batches))
			set("hoardmq_audit_queue_depth", nil, float64(st.Depth))
		}
	})
}

// setJournal sets one store's journal series, labelled by which store it is.
func setJournal(set func(string, map[string]string, float64), which string, js store.JournalStats) {
	l := map[string]string{"store": which}
	set("hoardmq_journal_index", l, float64(js.LastIndex))
	set("hoardmq_journal_checkpoint_index", l, float64(js.CheckpointIndex))
	set("hoardmq_journal_checkpoint_age_seconds", l, time.Since(js.CheckpointAt).Seconds())
	set("hoardmq_journal_checkpoints_total", l, float64(js.Sync.Checkpoints))
	set("hoardmq_journal_fsyncs_total", l, float64(js.Fsyncs))
	set("hoardmq_journal_fsync_seconds_total", l, js.FsyncSec)
	set("hoardmq_journal_fsync_failures_total", l, float64(js.Sync.Failures))
	set("hoardmq_journal_repairs_total", l, float64(js.Sync.Repairs))
	set("hoardmq_journal_repair_failures_total", l, float64(js.Sync.RepairFailures))
	set("hoardmq_journal_sync_callers_total", l, float64(js.Sync.Callers))
	set("hoardmq_journal_sync_wait_seconds_total", l, js.Sync.WaitSec)
}

// setTracing sets the OpenTelemetry exporter's series.
func setTracing(set func(string, map[string]string, float64), st otel.Stats) {
	set("hoardmq_otel_spans_started_total", nil, float64(st.Started))
	set("hoardmq_otel_spans_exported_total", nil, float64(st.Exported))
	for reason, n := range st.Dropped {
		set("hoardmq_otel_spans_dropped_total", map[string]string{"reason": reason}, float64(n))
	}
	for outcome, n := range st.Exports {
		set("hoardmq_otel_exports_total", map[string]string{"outcome": outcome}, float64(n))
	}
	set("hoardmq_otel_queue_depth", nil, float64(st.Queued))
}

// SetModuleEnabled toggles a module and audits the action.
func (s *Server) SetModuleEnabled(id module.ID, enabled bool) bool {
	ok := s.registry.SetEnabled(id, enabled)
	action, outcome := "module.enable", "ok"
	if !enabled {
		action = "module.disable"
	}
	if !ok {
		outcome = "not toggleable or unknown"
	}
	s.record("admin", action, string(id), outcome, nil)
	return ok
}

// StartTrace begins recording a connected client's packets.
func (s *Server) StartTrace(clientID string, limit int) error {
	c := s.connected(clientID)
	if c == nil {
		return fmt.Errorf("client %q is not connected", clientID)
	}
	c.StartTrace(limit)
	s.record("admin", "trace.start", clientID, "ok", nil)
	return nil
}

// StopTrace ends a recording and returns it.
func (s *Server) StopTrace(clientID string) ([]trace.Event, error) {
	c := s.connected(clientID)
	if c == nil {
		return nil, fmt.Errorf("client %q is not connected", clientID)
	}
	events := c.StopTrace()
	s.record("admin", "trace.stop", clientID, "ok", map[string]string{"events": strconv.Itoa(len(events))})
	return events, nil
}

// Trace returns a recording in progress.
func (s *Server) Trace(clientID string) []trace.Event {
	c := s.connected(clientID)
	if c == nil {
		return nil
	}
	return c.Trace()
}

// AuditDir is the audit log directory, or "" when auditing is off.
func (s *Server) AuditDir() string {
	if s.audit.Load() == nil {
		return ""
	}
	return s.audit.Load().Dir()
}

// ObservabilityAddr is the bound observability listener, or nil.
func (s *Server) ObservabilityAddr() net.Addr {
	if p, ok := s.registry.Metrics().(ext.ListenerProvider); ok {
		return p.Addr()
	}
	return nil
}

// config is the configuration in force.
func (s *Server) config() *config.Document { return s.doc.Load() }
