package session

import (
	"context"
	"errors"
	"hash/fnv"
	"log/slog"
	"math/bits"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/conn"
	"github.com/happydave/hoardmq/internal/hook"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/otel"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

// Options configure the manager.
type Options struct {
	Namespace string
	Node      string
	MaxQueued int
	// Shards is the session map's shard count, rounded up to a power of two;
	// zero uses defaultShards. Exposed so a measurement can sweep it.
	Shards int
	// StoreAdmission bounds how many connects and disconnects are inside the
	// store's write path at once; zero means unbounded. The single map lock
	// used to bound it at one as a side effect (WI 1292 round 5).
	StoreAdmission   int
	DropNewest       bool
	MaxSessionExpiry uint32 // 0 means no cap
	Authorizers      auth.AuthorizerChain
	Logger           *slog.Logger
	Clock            Clock
	Router           Router
	// Journal is the store provider; nil means no persistence.
	Journal Journal
	// Observer receives delivery, retained and Will events; nil means none.
	Observer hook.Observer
	// Metrics receives counters; nil means none.
	Metrics hook.Metrics
	// Tracer records spans for the publishes this node takes in; nil is
	// tracing off.
	Tracer *otel.Tracer
	// Ended is told when a session's state is gone for good (expired, or
	// replaced by a Clean Start), with the epoch it carried; nil means none.
	// Evictions for a takeover do not count: the state moved.
	Ended func(ns, id string, epoch uint64)
	// Forwarder sends a message published on this node to the other nodes
	// with matching subscribers; nil means a single node. It is called on
	// the publishing connection's goroutine, after local routing, for
	// messages that were not themselves forwarded.
	Forwarder Forwarder
	// Moving, when set, is told a session's filters just before its state
	// leaves this node for another (a lease takeover or hand-off), so the
	// routing plane can hold their removal until the new owner's routes are
	// known; id names the client, so the plane can apply the new owner's
	// confirmation to them. nil means a single node.
	Moving func(ns, id string, filters []string)
	// ForgetInboundQoS2 is the simulation harness's injected defect: a
	// repeated QoS 2 PUBLISH is routed again instead of being recognised.
	// Never set from configuration.
	ForgetInboundQoS2 bool
	// PubrelUnreplicated is the simulation harness's injected defect for
	// exactly-once across an owner loss: the PUBREL goes out without waiting
	// for the replicas to hold the PUBREC state, so a client can complete an
	// exchange the cluster cannot recover. Never set from configuration.
	PubrelUnreplicated bool
}

// Forwarder is the routing plane's entry point.
type Forwarder interface {
	// Forward sends the message to the other nodes with matching
	// subscribers and returns how many forwards it sent. It returns an error
	// when a matching subscriber's node cannot receive it, so a QoS 1 or 2
	// publish is not acknowledged as delivered to them.
	Forward(msg *message.Message) (int, error)
}

// Replication is the data-plane replication layer's entry points; attached
// with SetReplication once it exists, nil for a single node.
type Replication interface {
	// Barrier returns once every record written so far is acknowledged by
	// the followers and each of the publish's own forwards by its receiver;
	// it is called before a QoS 1 or 2 acknowledgement.
	//
	// keys name the records this acknowledgement is claiming durability for —
	// the publisher's session and every session the publish was delivered
	// into. A peer that refused a record for any of them cannot be said to
	// hold it, so the barrier fails. A call that names nothing fails too: it
	// would otherwise acknowledge records it never examined.
	//
	// forwards are the frames the routing plane sent for this publish
	// (message.Message.Forwards); a publish that forwarded nothing waits on no
	// peer's forwards.
	Barrier(ctx context.Context, keys []store.Scope, forwards []message.ForwardRef) error
	// Durable returns once every record written so far is acknowledged by
	// the followers, without waiting on forwards: what a node needs before
	// it acknowledges a forwarded publish to its sender (waiting on forwards
	// there would make two nodes wait on each other).
	//
	// keys carry the same meaning as Barrier's.
	Durable(ctx context.Context, keys []store.Scope) error
	// StoreRetained is asked before a retained message is stored. It
	// returns true when this node holds the topic's shard and the manager
	// stores locally; false when the write was handed to the shard leader.
	StoreRetained(msg *message.Message) (local bool)
	// QueryRetained returns the live retained messages a filter matches
	// across every shard.
	QueryRetained(ns, filter string, now time.Time) []*message.Message
}

// Manager owns every session on the node.
type Manager struct {
	opts   Options
	log    *slog.Logger
	clock  Clock
	router Router

	// Every site that closes a client's connection, counted where the close
	// happens rather than at its callers (WI 1495). A SessionTakenOver a
	// client reports comes from exactly one of these, and attributing an
	// excess drop to a lease move without them is a guess: at 1,000 clients
	// every excess drop was a SessionTakenOver while both lease paths
	// reported detaching nothing live.
	//
	//   displaced    — a second CONNECT for the same identifier, no lease moved
	//   closedTake   — Take, the new owner pulling the session away
	//   closedEvict  — Evict, the state discarded here
	//   closedDetach — Detach, the connection closed with the state left behind
	displaced    atomic.Int64
	closedTake   atomic.Int64
	closedEvict  atomic.Int64
	closedDetach atomic.Int64
	// closedEvictImport is the subset of closedEvict that came from Import —
	// a lease being adopted and its state pulled in, which evicts whatever
	// this node held for the key first. WI 1513 attributed the evictions to
	// the lease service's deferred reclaim and fixed that, and the fix was
	// inert; this says which caller is actually responsible.
	closedEvictImport atomic.Int64
	// A CONNECT refused because the session is sealed for a newer grant, and
	// sealed sessions a CONNECT at this node's own later grant resumed.
	connectsRefusedSealed atomic.Int64
	sealsResumed          atomic.Int64
	// imports and importNs are Import's count and total time (ImportCost).
	imports  atomic.Int64
	importNs atomic.Int64
	// Seals and imports that waited for an import of the same client to
	// finish (ImportWaits).
	sealWaits   atomic.Int64
	importWaits atomic.Int64
	// duplicates counts routed deliveries skipped because the session was
	// installed holding the message (DuplicatesSkipped).
	duplicates atomic.Int64
	// queued is the messages queued across this manager's sessions.
	queued atomic.Int64

	replication atomic.Pointer[replicationBox]

	// limits are the queue and expiry limits in force: Options' at New, and
	// SetLimits' after it.
	limits atomic.Pointer[Limits]

	// shards hold the session maps. One mutex for all of them made every
	// connect, disconnect and inbound packet in the process queue behind
	// every other: WI 1352 round 9 put 80.3% of a storm's mutex delay in the
	// accept loop and 58.8% of it inside Disconnect. A session and the
	// connection attached to it always land in the same shard, chosen from
	// the session key, so no operation on one session touches two shards.
	//
	// The lock order is unchanged and is what keeps this deadlock-free: a
	// shard's mutex is taken before a session's own mutex, never after; an
	// operation spanning shards takes them in index order; and the admission
	// gate below is taken before any shard lock.
	shards []*shard
	// gate bounds how many connects and disconnects are inside the store's
	// write path at once. The single map lock used to do this as a side
	// effect — see the comment in Connect — and sharding removes that, so
	// the bound is explicit here instead (WI 1292 round 5, I6).
	gate chan struct{}
	seq  atomic.Uint64
	// lifecycle guards the manager's own timer, which is not session state.
	lifecycle  sync.Mutex
	sweepTimer Timer
	closed     bool
	// journaled counts successful journal writes; synced is the highest count
	// a Sync that returned nil had read before it started, so every write it
	// counted is covered (see syncIfDirty).
	journaled atomic.Uint64
	synced    atomic.Uint64
	// importPause, set only by tests, runs inside an admitted import before
	// it changes anything.
	importPause func()
	// checkpointPause, set only by tests, runs in CheckpointState after the
	// sessions are listed and before any is read.
	checkpointPause func()
}

// shard is one slice of the session map with its own mutex.
type shard struct {
	mu       sync.Mutex
	sessions map[key]*Session
	byConn   map[Connection]*Session
	// sealed holds sessions handed to a takeover and not yet given up: out of
	// the session map, so nothing routes to or resumes them, and their journal
	// records not yet deleted. A key is in sessions or in sealed, never both.
	sealed map[key]*sealedCopy
	// importing marks the clients an admitted import is installing; the
	// channel closes when it returns (ImportIf).
	importing map[key]chan struct{}
}

// shardFor returns the shard a session key belongs to. It is the only place
// that maps a key to a shard; no call site computes it.
func (m *Manager) shardFor(k key) *shard {
	h := fnv.New64a()
	_, _ = h.Write([]byte(k.ns))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(k.id))
	return m.shards[h.Sum64()&uint64(len(m.shards)-1)]
}

// shardForConn returns the shard holding a connection's session. A
// connection and its session share a key, so this is the same shard the
// session map uses and lookup takes exactly one lock.
func (m *Manager) shardForConn(c Connection) *shard {
	return m.shardFor(key{m.opts.Namespace, c.ClientID()})
}

// lockAll takes every shard in index order, for the operations that need a
// consistent view of all sessions: recovery, the record listing, the
// checkpoint and the count. Callers unlock with unlockAll.
func (m *Manager) lockAll() {
	for _, sh := range m.shards {
		sh.mu.Lock()
	}
}

func (m *Manager) unlockAll() {
	for i := len(m.shards) - 1; i >= 0; i-- {
		m.shards[i].mu.Unlock()
	}
}

// admit takes a slot in the store admission gate, and returns the release.
// It must be called before any shard lock: waiting for the gate under a
// shard lock would rebuild the convoy this sharding removes, one shard at a
// time.
func (m *Manager) admit() func() {
	if m.gate == nil {
		return func() {}
	}
	m.gate <- struct{}{}
	// Idempotent: callers release it as soon as they drop the shard lock, so
	// the slot covers the journal write and nothing after it, and keep a
	// deferred release for the early returns. Releasing twice would hand out
	// a slot nobody holds.
	var once sync.Once
	return func() { once.Do(func() { <-m.gate }) }
}

// New creates a manager.
func New(opts Options) *Manager {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Clock == nil {
		opts.Clock = realClock{}
	}
	if opts.Router == nil {
		opts.Router = newExactRouter()
	}
	if opts.Namespace == "" {
		opts.Namespace = "default"
	}
	m := &Manager{opts: opts, log: opts.Logger, clock: opts.Clock, router: opts.Router}
	m.limits.Store(&Limits{MaxQueued: opts.MaxQueued, DropNewest: opts.DropNewest, MaxSessionExpiry: opts.MaxSessionExpiry})
	n := opts.Shards
	if n <= 0 {
		n = defaultShards
	}
	n = 1 << bits.Len(uint(n-1)) // a power of two, so the hash masks
	m.shards = make([]*shard, n)
	for i := range m.shards {
		m.shards[i] = &shard{sessions: map[key]*Session{}, byConn: map[Connection]*Session{}, sealed: map[key]*sealedCopy{}, importing: map[key]chan struct{}{}}
	}
	if g := opts.StoreAdmission; g > 0 {
		m.gate = make(chan struct{}, g)
	}
	m.armSweep()
	return m
}

// Limits are the per-session limits a running manager can change: the queue
// bound and what a full queue drops, from the next message queued, and the
// cap on a session's expiry interval, from the next one a client sets.
type Limits struct {
	MaxQueued        int
	DropNewest       bool
	MaxSessionExpiry uint32 // 0 means no cap
}

// SetLimits puts new limits into force. A queue already longer than a
// lowered bound is not trimmed, and a session keeps the expiry it has.
func (m *Manager) SetLimits(l Limits) { m.limits.Store(&l) }

// CurrentLimits are the limits in force.
func (m *Manager) CurrentLimits() Limits { return *m.limits.Load() }

// defaultShards is the session map's shard count. A power of two so the
// hash masks rather than divides; large enough that a storm's connects and
// disconnects spread, small enough that the whole-map operations (recovery,
// the record listing, the checkpoint) stay cheap.
const defaultShards = 64

// sweepInterval is how often expired retained messages are dropped.
const sweepInterval = time.Minute

func (m *Manager) armSweep() {
	m.lifecycle.Lock()
	defer m.lifecycle.Unlock()
	if m.closed {
		return
	}
	m.sweepTimer = m.clock.AfterFunc(sweepInterval, func() {
		m.router.Sweep(m.clock.Now())
		m.armSweep()
	})
}

// Close stops the manager's own timers. Sessions are left as they are.
func (m *Manager) Close() {
	m.lifecycle.Lock()
	defer m.lifecycle.Unlock()
	m.closed = true
	if m.sweepTimer != nil {
		m.sweepTimer.Stop()
	}
}

type replicationBox struct{ r Replication }

// SetReplication attaches or clears the replication layer.
func (m *Manager) SetReplication(r Replication) {
	if r == nil {
		m.replication.Store(nil)
		return
	}
	m.replication.Store(&replicationBox{r: r})
}

// Replication returns the attached replication layer, or nil.
func (m *Manager) Replication() Replication {
	if b := m.replication.Load(); b != nil {
		return b.r
	}
	return nil
}

// InjectRetained stores a retained message on this node as the shard leader
// and makes it durable: the replication layer installs messages it received
// from other nodes through it. It reports whether the record was appended and
// synced; one that was not may not be acknowledged.
func (m *Manager) InjectRetained(msg *message.Message) (durable bool) {
	if msg.Namespace == "" {
		msg.Namespace = m.opts.Namespace
	}
	m.router.Retain(msg)
	w := m.ackWriter()
	w.retain(msg)
	synced := m.syncIfDirty()
	return !w.unjournaled() && synced
}

// retainedFor returns the retained messages a filter matches, across the
// cluster when replication is attached.
func (m *Manager) retainedFor(ns, filter string, now time.Time) []*message.Message {
	if r := m.Replication(); r != nil {
		return r.QueryRetained(ns, filter, now)
	}
	return m.router.RetainedFor(ns, filter, now)
}

// Inject publishes a message originating from the node itself (a module).
func (m *Manager) Inject(msg *message.Message) {
	if msg.Namespace == "" {
		msg.Namespace = m.opts.Namespace
	}
	msg.Seq = m.seq.Add(1)
	msg.Arrived = m.clock.Now()
	m.dispatch(msg, nil, nil, false)
}

// Records lists every session's view, sorted by client identifier.
func (m *Manager) Records() []Record {
	m.lockAll()
	var sessions []*Session
	for _, sh := range m.shards {
		for _, s := range sh.sessions {
			sessions = append(sessions, s)
		}
	}
	m.unlockAll()
	out := make([]Record, 0, len(sessions))
	for _, s := range sessions {
		s.mu.Lock()
		out = append(out, s.record())
		s.mu.Unlock()
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ClientID < out[j].ClientID })
	return out
}

// Router returns the router in use, for the subscription work item's tests.
func (m *Manager) Router() Router { return m.router }

// Record returns the view of a session, and whether it exists.
func (m *Manager) Record(clientID string) (Record, bool) {
	k := key{m.opts.Namespace, clientID}
	sh := m.shardFor(k)
	sh.mu.Lock()
	s, ok := sh.sessions[k]
	sh.mu.Unlock()
	if !ok {
		return Record{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.record(), true
}

// Count is the number of sessions, online and offline.
func (m *Manager) Count() int {
	m.lockAll()
	defer m.unlockAll()
	n := 0
	for _, sh := range m.shards {
		n += len(sh.sessions)
	}
	return n
}

// expiryFor derives the Session Expiry Interval a CONNECT asks for, capped,
// and whether the cap applied.
func (m *Manager) expiryFor(pkt *packet.Connect) (uint32, bool) {
	var interval uint32
	if pkt.ProtocolLevel == packet.V5 {
		if pkt.Properties.SessionExpiryInterval != nil {
			interval = *pkt.Properties.SessionExpiryInterval
		}
	} else if !pkt.CleanStart {
		interval = NeverExpires
	}
	if cap := m.limits.Load().MaxSessionExpiry; cap != 0 && interval > cap {
		return cap, true
	}
	return interval, false
}

// Connect implements the core's connect step for an authenticated
// connection: takeover, resume or create, then the Will and the CONNACK.
// Connect establishes or resumes a session. leaseEpoch is the generation the
// lease service just granted for this client, or 0 when there is no lease
// service (a single node). It is applied before the session is journalled:
// the record carries the epoch it was written under, and the replication tap
// stamps that on the frame, so the session's copy has to be authoritative by
// the time the first write happens rather than one call later.
func (m *Manager) Connect(c Connection, pkt *packet.Connect, leaseEpoch uint64) conn.ConnectResult {
	return m.connect(c, pkt, leaseEpoch, false)
}

// ConnectReplacing is Connect for a client whose earlier session may have
// been written to the replicas (lease.AcquireResult.MayHaveCopies). A
// Clean Start with no session held here journals the session's delete at
// leaseEpoch before the new session's first record, persistent or not, so
// no replica keeps the previous session under the new one.
func (m *Manager) ConnectReplacing(c Connection, pkt *packet.Connect, leaseEpoch uint64) conn.ConnectResult {
	return m.connect(c, pkt, leaseEpoch, true)
}

func (m *Manager) connect(c Connection, pkt *packet.Connect, leaseEpoch uint64, replacing bool) conn.ConnectResult {
	k := key{m.opts.Namespace, c.ClientID()}
	// The gate before the shard lock, never under it (WI 1292 round 5, I6):
	// this path journals, and the single map lock used to bound how many
	// connects reached the store at once as a side effect. Sharding removes
	// that side effect, so the bound is taken explicitly here.
	release := m.admit()
	defer release()
	sh := m.shardFor(k)
	sh.mu.Lock()
	s, existed := sh.sessions[k]
	var resumed *Session
	if c := sh.sealed[k]; !existed && c != nil {
		switch sealedConnect(c.token, leaseEpoch, pkt.CleanStart) {
		case sealedRefuse:
			sh.mu.Unlock()
			m.connectsRefusedSealed.Add(1)
			return conn.ConnectResult{Reason: packet.ServerBusy}
		case sealedResume:
			delete(sh.sealed, k)
			rs, lapsed := m.rebuild(c.st, m.clock.Now())
			if lapsed {
				m.journalSealedDelete(c)
				break
			}
			sh.sessions[k] = rs
			rs.mu.Lock()
			rs.countQueue()
			rs.mu.Unlock()
			s, existed, resumed = rs, true, rs
			m.sealsResumed.Add(1)
		case sealedDiscard:
			delete(sh.sealed, k)
			m.journalSealedDelete(c)
		}
	}
	var old Connection
	var redistribute []redistributable
	var discarded *Session
	heldHere := existed
	if existed && pkt.CleanStart {
		discarded = s
	}
	if existed {
		s.mu.Lock()
		if s.conn != nil {
			old = s.conn
			delete(sh.byConn, old)
			s.detach()
			s.epoch++
		}
		if pkt.CleanStart {
			redistribute = m.discardLocked(sh, s)
			epoch := s.epoch
			s.mu.Unlock()
			s = newSession(k, m.log)
			s.mgr = m
			s.epoch = epoch + 1
			sh.sessions[k] = s
			s.mu.Lock()
			existed = false
		}
	} else {
		s = newSession(k, m.log)
		s.mgr = m
		sh.sessions[k] = s
		s.mu.Lock()
	}
	s.stopTimers()
	s.willArmed = false
	s.conn = c
	s.online.Store(true)
	s.version = c.Version()
	s.identity = *c.Identity()
	sh.byConn[c] = s
	interval, capped := m.expiryFor(pkt)
	s.expiryInterval = interval
	s.expiresAt = time.Time{}
	if !existed {
		s.persistent = interval != 0
	}
	s.will = nil
	if w := pkt.Will; w != nil {
		pub := &packet.Publish{Topic: w.Topic, Payload: w.Payload, QoS: w.QoS, Retain: w.Retain, Properties: w.Properties}
		wm := message.FromPublish(pub, m.publisher(s), 0, m.clock.Now())
		delay := uint32(0)
		if w.Properties.WillDelayInterval != nil {
			delay = *w.Properties.WillDelayInterval
		}
		s.will = &Will{Message: wm, Delay: delay}
	}
	// The lease epoch is applied before anything is journalled: the record
	// carries the epoch it was written under and the replication tap stamps
	// that on the frame, so the session's copy has to be authoritative here
	// rather than a call later.
	if leaseEpoch != 0 {
		s.epoch = leaseEpoch
	}
	if replacing && pkt.CleanStart && !heldHere {
		m.writer().deleteCopies(s)
	}
	// The journal write stays inside the locks, and inside the admission gate
	// taken at the top of this function. Taking it out of the manager's lock
	// looked free at benchmark concurrency (8-32 goroutines) and was
	// catastrophic at storm concurrency: that lock was metering how many
	// connects reached the store's lock at once, and without it ~50,000
	// concurrent connects piled onto the store directly — fill went from
	// 4m25s to not completing in 45 minutes at 100,000 clients. Sharding the
	// map removes that side effect, so `StoreAdmission` is now the bound, set
	// deliberately rather than inherited from an unrelated mutex. See WI 1292
	// rounds 2 and 5.
	m.writer().upsert(s)
	s.mu.Unlock()
	sh.mu.Unlock()
	release()

	if old != nil {
		// A second connection for the same client identifier displaces the
		// first here, on this node, with no lease moving anywhere. It is a
		// third producer of SessionTakenOver besides the lease service's two,
		// and at 1,000 clients it was the only one that fired — so a
		// measurement that attributes every such disconnect to a lease move
		// is attributing it to the wrong thing (WI 1495).
		m.displaced.Add(1)
		old.Close(packet.SessionTakenOver)
	}
	if resumed != nil {
		// Routed once the rebuilt session is whole and installed, as Load does.
		resumed.mu.Lock()
		subs := make([]Subscription, 0, len(resumed.subs))
		for _, sb := range resumed.subs {
			subs = append(subs, sb)
		}
		resumed.mu.Unlock()
		for _, sb := range subs {
			m.router.Add(resumed, sb)
		}
	}
	m.redistribute(discarded, redistribute)
	m.ended(discarded)
	if !m.syncIfDirty() {
		// The CONNACK would claim records the journal may not hold: the
		// connection leaves the session as an abnormal end without a Will
		// would, and the client is refused.
		m.Disconnect(c, false, packet.ImplementationSpecificError, false)
		return conn.ConnectResult{Reason: packet.ImplementationSpecificError}
	}
	res := conn.ConnectResult{SessionPresent: existed, Reason: packet.Success}
	if capped && c.Version() == packet.V5 {
		v := interval
		res.Properties.SessionExpiryInterval = &v
	}
	return res
}

// Resume replays in-flight messages and drains the queue on the connection
// that just received its CONNACK. The connection layer calls it after the
// CONNACK is queued so the client never sees a PUBLISH first.
func (m *Manager) Resume(c Connection) {
	s := m.lookup(c)
	if s == nil {
		return
	}
	s.mu.Lock()
	s.drain(m.clock.Now(), m.writer())
	s.mu.Unlock()
	m.syncIfDirty()
}

func (m *Manager) publisher(s *Session) message.Publisher {
	return message.Publisher{Namespace: s.key.ns, ClientID: s.key.id, Username: s.identity.Username, Node: m.opts.Node}
}

// discardLocked removes a session's state and returns the shared-group QoS 1
// messages the caller must redistribute once the locks are released. The
// caller holds the session's shard lock and the session's own mutex, and
// passes the shard rather than letting this re-derive one it already holds:
// a caller with the wrong shard should be a compile error, not a silent
// cross-shard write.
func (m *Manager) discardLocked(sh *shard, s *Session) []redistributable {
	m.writer().delete(s)
	return m.removeLocked(sh, s)
}

// removeLocked is discardLocked without the journal's delete: the session
// leaves memory and its records stay. The caller holds the same locks.
func (m *Manager) removeLocked(sh *shard, s *Session) []redistributable {
	s.stopTimers()
	filters := make([]string, 0, len(s.subs))
	for f := range s.subs {
		filters = append(filters, f)
	}
	m.router.RemoveAll(s, filters)
	shared := s.sharedQoS1()
	s.persistent = false
	s.subs = map[string]Subscription{}
	s.inboundQoS2 = map[uint16]*inboundAttempt{}
	s.inflight = nil
	s.inflightByID = map[uint16]*inflight{}
	s.queue = nil
	s.countQueue()
	if s.conn != nil {
		delete(sh.byConn, s.conn)
		s.conn = nil
		s.online.Store(false)
	}
	delete(sh.sessions, s.key)
	return shared
}

// redistribute re-offers QoS 1 messages a departed member held to its
// shared groups. No locks are held on entry.
func (m *Manager) redistribute(from *Session, items []redistributable) {
	if from == nil || len(items) == 0 {
		return
	}
	now := m.clock.Now()
	for _, it := range items {
		match, ok := m.router.MatchGroup(from.key.ns, it.group, it.msg, from)
		if !ok {
			continue
		}
		m.deliverMatch(it.msg, match, now, m.writer())
	}
}

// deliverMatch delivers a message to one match, applying the QoS cap.
func (m *Manager) deliverMatch(msg *message.Message, match Match, now time.Time, w *journalWriter) {
	qos := msg.QoS
	if match.QoS < qos {
		qos = match.QoS
	}
	t := target{qos: qos, ids: match.Identifiers, retain: msg.Retain && match.RetainAsPublished, group: match.Group}
	s := match.Session
	s.mu.Lock()
	if heldAtInstall(s.installed, msg) {
		s.mu.Unlock()
		m.duplicates.Add(1)
		m.count("hoardmq_deliveries_duplicate_total", map[string]string{"namespace": s.key.ns}, 1)
		return
	}
	l := m.limits.Load()
	s.deliver(msg, t, now, l.MaxQueued, l.DropNewest, w)
	s.mu.Unlock()
}

// heldAtInstall decides whether a routed message is one a session was
// installed holding, so delivering it would give the session a second copy
// under a new identifier. It is pure, and it skips a delivery only on an
// exact identity: a message with none, or held nowhere, is delivered.
func heldAtInstall(installed map[message.ID]struct{}, msg *message.Message) bool {
	id, ok := msg.Identity()
	if !ok || installed == nil {
		return false
	}
	_, held := installed[id]
	return held
}

// installedIdentities lists the messages a session's state holds, and those
// its previous owner sent it recently and handed over as delivered.
func installedIdentities(ss *store.SessionState) map[message.ID]struct{} {
	var out map[message.ID]struct{}
	add := func(id message.ID) {
		if out == nil {
			out = make(map[message.ID]struct{}, len(ss.Entries)+len(ss.Delivered))
		}
		out[id] = struct{}{}
	}
	for _, e := range ss.Entries {
		if e.Message == nil {
			continue
		}
		if id, ok := e.Message.Identity(); ok {
			add(id)
		}
	}
	for _, id := range ss.Delivered {
		add(id)
	}
	return out
}

// DuplicatesSkipped counts routed deliveries skipped because the session was
// installed from a hand-over already holding the message.
func (m *Manager) DuplicatesSkipped() int64 { return m.duplicates.Load() }

// dispatch stores a retained message and routes it to every match. It
// returns the number of local matches plus the forwards sent to other nodes,
// so a publish whose only subscriber is elsewhere is not answered as having
// none (WI 1487). When into is non-nil, every record this publish caused that
// a barrier must cover is appended to it: each local session delivered into,
// and the retained topic when the message is retained. When acked, the
// publish's acknowledgement waits on what it writes: failed reports a record
// that did not append, and a queued copy that failed is withdrawn.
func (m *Manager) dispatch(msg *message.Message, publisher *Session, into *[]store.Scope, acked bool) (n int, failed bool) {
	w := m.writer()
	if acked {
		w = m.ackWriter()
	}
	if msg.Retain {
		local := true
		if r := m.Replication(); r != nil {
			local = r.StoreRetained(msg)
		}
		if local {
			m.router.Retain(msg)
			w.retain(msg)
		}
		if m.opts.Observer != nil {
			m.opts.Observer.OnRetained(context.Background(), msg)
		}
		// A retained write is replicated whether this node stored it or handed
		// it to the shard leader, and it is keyed by topic, not by session. An
		// acknowledgement that named only sessions would pass over a retained
		// record a follower refused.
		if into != nil {
			*into = append(*into, store.Scope{Namespace: msg.Namespace, Topic: msg.Topic})
		}
	}
	m.count("hoardmq_publishes_total", map[string]string{"namespace": msg.Namespace, "qos": qosLabel(msg.QoS)}, 1)
	n = m.route(msg, publisher, into, w)
	if m.opts.Forwarder != nil && !msg.Forwarded {
		sent, err := m.opts.Forwarder.Forward(msg)
		n += sent
		if err != nil && publisher != nil {
			publisher.mu.Lock()
			publisher.forwardErr = true
			publisher.mu.Unlock()
		}
	}
	return n, w.unjournaled()
}

// Durable returns once everything journaled so far is synced and, with
// replication attached, held by the live followers. The routing plane
// acknowledges forwarded publishes after it.
func (m *Manager) Durable(ctx context.Context, keys []store.Scope) error {
	if !m.syncIfDirty() {
		return errors.New("session: journal sync failed")
	}
	if r := m.Replication(); r != nil {
		return r.Durable(ctx, keys)
	}
	return nil
}

// ErrNotJournaled is returned by InjectForwarded and InjectGroup when a record
// the delivery wrote failed to append: the forward must not be acknowledged.
var ErrNotJournaled = errors.New("session: a delivery's record failed to append")

// InjectForwarded delivers a message received from another node to this
// node's direct subscribers; shared groups are decided by the routing plane.
// At QoS 1 and 2 a record that failed to append is reported as
// ErrNotJournaled and its queued copy withdrawn, since the sender's publish
// is refused for it.
func (m *Manager) InjectForwarded(msg *message.Message) (int, []store.Scope, error) {
	msg.Forwarded = true
	if msg.Namespace == "" {
		msg.Namespace = m.opts.Namespace
	}
	m.count("hoardmq_publishes_total", map[string]string{"namespace": msg.Namespace, "qos": qosLabel(msg.QoS)}, 1)
	var into []store.Scope
	w := m.forwardWriter(msg)
	n := m.route(msg, nil, &into, w)
	if w.unjournaled() {
		return n, into, ErrNotJournaled
	}
	return n, into, nil
}

// InjectGroup delivers a message the group's dispatch owner chose this node
// for: one local member of the shared group receives it. A failed append is
// reported as InjectForwarded reports it.
func (m *Manager) InjectGroup(msg *message.Message, group string) (store.Scope, bool, error) {
	msg.Forwarded = true
	match, ok := m.router.MatchGroup(msg.Namespace, group, msg, nil)
	if !ok {
		return store.Scope{}, false, nil
	}
	w := m.forwardWriter(msg)
	m.deliverMatch(msg, match, m.clock.Now(), w)
	if w.unjournaled() {
		return match.Session.scope(), true, ErrNotJournaled
	}
	return match.Session.scope(), true, nil
}

// forwardWriter is the writer for a forwarded message's deliveries: one that
// reports a failed append at QoS 1 and 2, whose forward is acknowledged, and
// the plain writer at QoS 0, whose is not.
func (m *Manager) forwardWriter(msg *message.Message) *journalWriter {
	if msg.QoS == 0 {
		return m.writer()
	}
	return m.ackWriter()
}

func qosLabel(q byte) string { return string(rune('0' + q)) }

// count adds to a metric when a provider is configured.
func (m *Manager) count(name string, labels map[string]string, delta float64) {
	if m.opts.Metrics != nil {
		m.opts.Metrics.Add(name, labels, delta)
	}
}

// event reports a delivery event when an observer is configured.
func (m *Manager) event(ev hook.DeliveryEvent, s *Session, msg *message.Message, qos byte) {
	switch ev {
	case hook.Dropped:
		m.count("hoardmq_drops_total", map[string]string{"namespace": s.key.ns, "reason": "queue_full"}, 1)
	case hook.Expired:
		m.count("hoardmq_drops_total", map[string]string{"namespace": s.key.ns, "reason": "expired"}, 1)
	default:
		m.count("hoardmq_deliveries_total", map[string]string{"namespace": s.key.ns, "event": ev.String()}, 1)
	}
	if m.opts.Observer != nil {
		m.opts.Observer.OnDelivery(context.Background(), ev, s.key.ns, s.key.id, msg, qos)
	}
}

// route delivers a message to every matching session. When into is non-nil
// every session delivered into is appended to it: a publish's acknowledgement
// claims durability for those records, so the barrier has to name them. It is
// nil wherever nothing is acknowledged — a QoS 0 publish allocates nothing.
func (m *Manager) route(msg *message.Message, publisher *Session, into *[]store.Scope, w *journalWriter) int {
	matches := m.router.Match(msg, publisher)
	now := m.clock.Now()
	for _, match := range matches {
		m.deliverMatch(msg, match, now, w)
		if into != nil {
			*into = append(*into, match.Session.scope())
		}
	}
	return len(matches)
}

// publishWill authorizes and routes the Will; the caller holds no locks.
func (m *Manager) publishWill(w *Will, s *Session) {
	s.mu.Lock()
	id := s.identity
	s.mu.Unlock()
	if m.opts.Authorizers.Authorize(context.Background(), auth.Publish, w.Message.Topic, &id) != auth.Allow {
		s.log.Debug("will not published: not authorized", "topic", w.Message.Topic)
		return
	}
	w.Message.Seq = m.seq.Add(1)
	w.Message.Arrived = m.clock.Now()
	if m.opts.Observer != nil {
		m.opts.Observer.OnWill(context.Background(), s.key.ns, s.key.id, w.Message)
	}
	m.count("hoardmq_wills_total", map[string]string{"namespace": s.key.ns}, 1)
	m.dispatch(w.Message, s, nil, false)
}

// Queued is the number of messages queued across this node's sessions, read
// without a lock.
func (m *Manager) Queued() int64 { return m.queued.Load() }
