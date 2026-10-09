// Package replication is the data plane's durability across nodes: a
// session's owner fans every journal record out to its shard's followers, a
// retained write goes through its shard's leader, a QoS 1 or 2
// acknowledgement waits for the live followers, and a lost owner's sessions
// are recovered from a follower.
package replication

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
	"github.com/happydave/hoardmq/internal/cluster/lease"
	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/hook"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

// Transport is what the replicator needs from the cluster transport.
type Transport interface {
	Send(ctx context.Context, peer string, c transport.Class, payload []byte) error
	Handle(c transport.Class, h transport.Handler)
	Observe(o transport.Observer)
	ObserveData(o transport.Observer)
	Peers() []transport.PeerInfo
}

// Leases is the lease table's read side.
type Leases interface {
	Lookup(ns, id string) (lease.Lease, bool)
	Node(name string) (lease.NodeInfo, bool)
	// Live is the log's verdict on a node: it has an alive record and the
	// leader has not declared it dead since.
	Live(node string) bool
	// Leaving is whether a drain's hand-off has marked the node leaving since
	// its last alive record.
	Leaving(node string) bool
}

// Tokens is the node's record of the highest grant token seen per client
// (lease.Tokens). The lease table's view lags a grant; this does not have to.
type Tokens interface {
	Note(ns, id string, token uint64)
	Seen(ns, id string) uint64
}

// Sessions is what the replicator needs from the session manager.
type Sessions interface {
	// Snapshot lists this node's persistent sessions and retained messages.
	Snapshot() *store.State
	// Export is one persistent session as this node holds it, for a repair.
	Export(ns, id string) (*store.SessionState, bool)
	// InjectRetained stores a retained message locally as the shard leader,
	// reporting whether its record is durable here.
	InjectRetained(msg *message.Message) (durable bool)
}

// Local is the node's retained store (the dispatcher).
type Local interface {
	RetainedFor(ns, filter string, now time.Time) []*message.Message
	AllRetained() []*message.Message
}

// Options configure the replicator.
type Options struct {
	Node      string
	Transport Transport // nil for a node without peers
	Map       *shardmap.Table
	Core      consensus.Core
	Leases    Leases
	// Tokens, when set, is what a record's epoch is judged against beside the
	// lease table, and is told every epoch a record is accepted at.
	Tokens   Tokens
	Sessions Sessions
	Local    Local
	// Replica is the follower-side store, separate from the node's own.
	Replica *store.Store
	// ReplicaDir is the replica store's directory; the last reconciled
	// shard-map version is persisted there so a restart never drops copies
	// against a map older than the one this node last acted on.
	ReplicaDir string
	Replicas   int           // configured copy count
	AckTimeout time.Duration // default 2 s
	// TTL is the lease ttl: a member whose alive record is older is not in
	// the shard map's basis.
	TTL time.Duration
	// Interval is how often the leader checks the basis; default TTL/3.
	Interval time.Duration
	Logger   *slog.Logger
	Metrics  hook.Metrics
	Audit    func(action, target, outcome string, detail map[string]string)
	Clock    func() time.Time
	// Injected defects for the simulation harness's negative controls;
	// never set from configuration. SkipEpochCheck makes followers apply
	// stale-epoch records; NoBarrier acknowledges after the local sync only.
	SkipEpochCheck bool
	// IgnoreGaps makes followers apply a frame that arrives past a gap in
	// the peer's sequence without reporting it: the defect the gap check
	// exists to remove.
	IgnoreGaps bool
	// DeleteWithoutHandoverAck makes a node that stops holding a copy delete
	// it after pushing it to the new holders without waiting for their
	// acknowledgement: the defect the acknowledged hand-over removes.
	DeleteWithoutHandoverAck bool
	// AcknowledgeUnapplied makes a follower acknowledge a record it could not
	// apply instead of refusing it — the silent divergence this work item
	// removed; Defects.
	AcknowledgeUnapplied bool
	// HandOverOwnedCopies makes an owner keep and hand over a replica copy
	// of a session it owns — the stale copy from the session's previous
	// owner, sent as if it were the owner's state; Defects.
	HandOverOwnedCopies bool
	// Level is the cluster's feature level as this node reads it, stamped on
	// the frames it sends; BuildLevel is the highest this build understands,
	// and a frame stamped above it is refused. Nil reads as level 1.
	Level      func() uint64
	BuildLevel func() uint64
	// KeepUnfedCopies makes a node keep, and accept, a replica copy of a
	// session it is not a target for — a copy the owner replicates nothing
	// to, which a takeover can then read. It is the whole of this rule: a
	// node that still holds the shard keeps every copy it has, including one
	// of a session it owns, as it did before; Defects.
	KeepUnfedCopies bool
	NoBarrier       bool
	// FetchAnswerers and FetchQueue size the pool that answers replica
	// fetches; zero takes the default. FetchOnWorker answers a fetch on the
	// asking peer's worker, in line with that peer's appends; Defects.
	FetchAnswerers int
	FetchQueue     int
	FetchOnWorker  bool
	// UnwrittenCheck is how long a copy of another grant's session, on a node
	// its owner does not replicate to, is kept before this node asks the
	// owner's targets whether one holds the session, and how often it asks
	// again; default twice TTL.
	UnwrittenCheck time.Duration
	// DropMovedRetained makes a node that stops holding a retained shard drop
	// its copies at its rebalance, before the shard's new holders hold them:
	// the defect the kept retained copies remove; Defects.
	DropMovedRetained bool
	// KeepRetainedNotHandedOn keeps a kept retained shard whose releaser is
	// gone without handing it to the shard's leader; Defects.
	KeepRetainedNotHandedOn bool
	// IgnoreOwedHandOver lets a release drop a node's copies of a shard while
	// the node still owes the sender a hand-over of them; Defects.
	IgnoreOwedHandOver bool
	// NoHandOverRetry keeps no note of a follower's unacknowledged hand-over
	// for a dead or absent leader, so it is never made again; Defects.
	NoHandOverRetry bool
	// NoEarlyReleaseDrop leaves a release kept for an owed hand-over unacted
	// on once nothing is owed; Defects.
	NoEarlyReleaseDrop bool
	// NoHandOverAtLeaderChange hands a shard's copies to its new leader only
	// when that leader held nothing of the shard, as before every old holder
	// handed over at a leader change; Defects.
	NoHandOverAtLeaderChange bool
	// StaleKeptJudgement acts on a kept shard's note from the snapshot and the
	// map a judging pass began with, even if a release or a newer map has
	// changed them since; Defects.
	StaleKeptJudgement bool
	// DeleteOwnOnHandOver makes an old leader that still holds a retained
	// shard delete its own copies once its hand-over is acknowledged, before
	// it holds the new leader's push; Defects.
	DeleteOwnOnHandOver bool
	// HandOverNoteOverwritten lets a later keep of a retained shard replace a
	// note that still owes a hand-over, so the owing is forgotten; Defects.
	HandOverNoteOverwritten bool
	// AnswerHandOverAnywhere answers a retained hand-over from the own store
	// on a node that does not lead the shard; Defects.
	AnswerHandOverAnywhere bool
	// PreRetainedMoves gives the node the retained behaviour of a build before
	// retained moves: it drops at its rebalance, sends no release, refuses no
	// write on a moving shard, does not know the release's kind and ignores
	// the hand-over mark. For the tests of a mixed pair; never configured.
	PreRetainedMoves bool
	// UnorderedRetainedPush reads a shard for a new follower's push without
	// the lock that orders it against the shard's writes; a test control.
	UnorderedRetainedPush bool
	// ConfirmMoved is told, after a rebalance has pushed a session this node
	// serves whole to the targets it gained, the nodes that stopped being its
	// targets: each keeps its copy until this node confirms the session
	// durable at its targets now. Nil confirms nothing.
	ConfirmMoved func(ns, id string, to []string)
}

// A fetch reads a copy and takes microseconds. The pool keeps it from waiting
// behind a storm's appends, so it is small.
const (
	defaultFetchAnswerers = 4
	defaultFetchQueue     = 4096
)

// fetchJob is one fetch waiting for an answerer.
type fetchJob struct {
	peer string
	fr   frame
}

// Frame kinds within the replication-append class.
const (
	kindAppend         byte = 1 // one journal record
	kindSnapshot       byte = 2 // a whole session, replacing the replica
	kindFetch          byte = 3 // a new owner asks a holder for a replica
	kindFetchResult    byte = 4
	kindRetainedWrite  byte = 5 // to the shard leader: store this retained message
	kindRetainedQuery  byte = 6 // to a shard leader: retained messages matching a filter
	kindRetainedResult byte = 7
	// kindRetainedPresence tells a peer which shards the sender holds
	// retained messages for and leads, so the peer can answer a subscribe
	// that cannot match any of them without asking. See presence.go.
	kindRetainedPresence byte = 8
	// kindResyncMark ends every resync. A break marks every frame sent so
	// far as failed, and a barrier fails while its target is at or below that
	// mark; when the resync has nothing to send, only a frame after the mark
	// lets a later barrier pass, once the peer acknowledges it (WI 1494). It
	// changes nothing on the peer, so a peer that ignores it is safe.
	kindResyncMark byte = 9
)

// frame is the wire form of every replication-append message.
type frame struct {
	Kind      byte                `json:"k"`
	Seq       uint64              `json:"seq,omitempty"`
	Req       uint64              `json:"req,omitempty"`
	Type      byte                `json:"t,omitempty"`
	Namespace string              `json:"ns,omitempty"`
	ClientID  string              `json:"id,omitempty"`
	Topic     string              `json:"topic,omitempty"`
	Epoch     uint64              `json:"epoch,omitempty"` // lease epoch, or map version for retained records
	Payload   json.RawMessage     `json:"p,omitempty"`
	State     *store.SessionState `json:"state,omitempty"`
	Filter    string              `json:"filter,omitempty"`
	Msg       *message.Message    `json:"msg,omitempty"`
	Msgs      []*message.Message  `json:"msgs,omitempty"`
	Found     bool                `json:"found,omitempty"`
	// Level is the cluster feature level the sender read when it built the
	// frame, absent at level 1. A frame is applied off the log, so the
	// receiver's own reading of the level may differ from the sender's; the
	// frame's semantics are the sender's.
	Level uint64 `json:"lvl,omitempty"`
	// Shards carries a retained-presence statement. An absent list is the
	// empty set — "I hold none" — which is a statement, not silence; the
	// kind is what distinguishes the two.
	Shards []int `json:"rs,omitempty"`
	// Handover marks a retained write as a node handing the shard's copies to
	// its leader, not a client's write: the leader takes it while the shard
	// moves, and installs it only for a topic it holds nothing for. Ignored by
	// an older leader, which installs it as any write.
	Handover bool `json:"ho,omitempty"`
	// FailedBelow, on a resync mark, is the first sequence of the stream that
	// has not failed at the sender: no barrier passes over a frame below it,
	// whatever the peer acknowledges. A peer whose writes may not be on its
	// disk acknowledges again once this is above them. Absent from an older
	// sender.
	FailedBelow uint64 `json:"fb,omitempty"`

	// queuedAt is when the frame arrived, for timing its wait. Not sent.
	queuedAt time.Time
}

// ack is the wire form of the replication-ack class: every sequence up to
// Seq from the sender was handled and what it wrote is synced. Rejected names
// one that was refused and acknowledges nothing: the follower sends it before
// it syncs the frames applied ahead of it in the same batch.
type ack struct {
	Seq      uint64 `json:"seq"`
	Rejected bool   `json:"rejected,omitempty"`
	Reason   string `json:"reason,omitempty"`
	// A rejection names the key the refused frame carried, so the sender can
	// scope it without having kept the sequence. Absent from an older
	// follower, and ignored by an older sender.
	Namespace string `json:"ns,omitempty"`
	ClientID  string `json:"id,omitempty"`
	Topic     string `json:"topic,omitempty"`
	Epoch     uint64 `json:"epoch,omitempty"`
}

// rejection is a follower's refusal of one frame, naming what it carried.
func rejection(fr frame, reason string) ack {
	return ack{Seq: fr.Seq, Rejected: true, Reason: reason, Namespace: fr.Namespace, ClientID: fr.ClientID, Topic: fr.Topic, Epoch: fr.Epoch}
}

// named is the key a rejection names, scoped as the sender scoped the frame
// when it pushed it; zero when it names none.
func (a ack) named() store.Scope {
	return store.Key{Namespace: a.Namespace, ClientID: a.ClientID, Topic: a.Topic, Epoch: a.Epoch}.Scope()
}

// Replicator is one node's replication layer.
type Replicator struct {
	o   Options
	log *slog.Logger

	mu    sync.Mutex
	peers map[string]*outbound // by peer name
	// stopped is set by Stop inside the same critical section that takes its
	// copy of peers, so no stream is created after the ones it will close have
	// been chosen. Read only under mu: that is what orders every surviving
	// wg.Add before Stop's wg.Wait. Not the stop channel — that closes before
	// the drive loop has finished, and a rebalance still running there may
	// open a stream this Stop then closes.
	stopped bool

	follower *follower
	pres     *presence

	reqs    atomic.Uint64
	fetches fetchCounters
	batches batchCounters
	taps    tapCounters
	drops   dropCounters
	fetchq  chan fetchJob
	waiters sync.Map // req → chan frame
	started atomic.Bool

	// Reconciliation runs on one worker against the latest map, from the
	// map it last finished with, so two map changes never act concurrently
	// and a superseded map is never acted on.
	recMu      sync.Mutex
	latest     shardmap.Map
	pending    bool
	reconciled shardmap.Map
	persisted  uint64 // last reconciled version from a previous incarnation
	// persistedKnown is false when the reconciled file could not be read: the
	// floor is then unknown, and no copy is dropped until a map has been
	// reconciled and written by this incarnation.
	persistedKnown bool
	wake           chan struct{}
	// basisWake asks the shard-map driver to check the basis now; maps is
	// what this node saw of each map version, for the operator.
	basisWake chan struct{}
	maps      mapTimeline
	// unwritten holds the copies kept until their owner writes the session.
	unwritten *unwrittenCopies
	// checkSlots bounds the checks of owners' targets in flight at once.
	checkSlots chan struct{}
	// fanned is the version of the map whose whole writes to gained targets
	// this node's rebalance has queued. A barrier on a newer map also waits
	// where the records written before it went.
	fanned atomic.Uint64
	// fanOutGate, set by a test before Start, runs at the start of each
	// rebalance, before its whole writes are queued.
	fanOutGate func()
	// rmoves is this node's share of the retained moves (retainedmove.go).
	// pushMu orders a new follower's whole push of a shard against the taps
	// of the shard's writes: a tap holds it for reading.
	rmoves *retainedMoves
	pushMu sync.RWMutex
	// judgeGate, set by a test, runs before a judging pass acts on each kept
	// shard.
	judgeGate func(shard int)
	// pushGate, set by a test, runs inside the push lock between the push's
	// read of the shard and its enqueue.
	pushGate func()

	ctx    context.Context
	cancel context.CancelFunc
	stop   chan struct{}
	done   chan struct{}
	wg     sync.WaitGroup
}

// New prepares the replicator: it registers the shard map with the machine
// and its transport handlers. Call Start after the core starts.
func New(o Options, machine *consensus.Machine) (*Replicator, error) {
	if o.Map == nil || o.Core == nil || o.Leases == nil || o.Sessions == nil || o.Local == nil || o.Replica == nil {
		return nil, errors.New("replication: map, core, leases, sessions, local and replica stores are required")
	}
	if o.AckTimeout == 0 {
		o.AckTimeout = 2 * time.Second
	}
	if o.TTL == 0 {
		o.TTL = 30 * time.Second
	}
	if o.Interval == 0 {
		o.Interval = o.TTL / 3
	}
	if o.Replicas < 1 {
		o.Replicas = 1
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.Clock == nil {
		o.Clock = time.Now
	}
	r := &Replicator{o: o, log: o.Logger.With("component", "replication"), peers: map[string]*outbound{}, pres: newPresence(), stop: make(chan struct{}), done: make(chan struct{}), wake: make(chan struct{}, 1), basisWake: make(chan struct{}, 1), unwritten: newUnwrittenCopies(), checkSlots: make(chan struct{}, unwrittenChecksAtOnce), rmoves: newRetainedMoves()}
	r.ctx, r.cancel = context.WithCancel(context.Background())
	r.persisted, r.persistedKnown = r.readReconciled()
	r.rmoves.kept = r.readKeptRetained()
	f, err := newFollower(r)
	if err != nil {
		return nil, err
	}
	r.follower = f
	if machine != nil {
		machine.Register(shardmap.Kind, o.Map)
	}
	o.Map.Observe(r.mapChanged)
	r.wg.Add(1)
	go r.reconcileLoop()
	if o.Transport != nil && !o.FetchOnWorker {
		answerers, queue := o.FetchAnswerers, o.FetchQueue
		if answerers <= 0 {
			answerers = defaultFetchAnswerers
		}
		if queue <= 0 {
			queue = defaultFetchQueue
		}
		r.fetchq = make(chan fetchJob, queue)
		for i := 0; i < answerers; i++ {
			r.wg.Add(1)
			go r.answerFetches()
		}
	}
	if o.Transport != nil {
		o.Transport.Handle(transport.ReplicationAppend, r.receive)
		o.Transport.Handle(transport.ReplicationAck, r.receiveAck)
		o.Transport.Observe(r.peerChanged)
		// Replication rides the data connection (WI 1340): its resets break
		// the streams and its return is when a peer is resynced.
		o.Transport.ObserveData(r.peerChanged)
	}
	return r, nil
}

// Start runs the shard-map driver.
func (r *Replicator) Start() {
	if r.started.Swap(true) {
		return
	}
	go r.driveLoop()
}

// Stop ends the loops and the follower's ack loop; the replica store is
// left open for the caller to close.
func (r *Replicator) Stop() {
	select {
	case <-r.stop:
		return
	default:
	}
	close(r.stop)
	r.cancel()
	if r.started.Load() {
		<-r.done
	}
	r.mu.Lock()
	r.stopped = true
	peers := make([]*outbound, 0, len(r.peers))
	for _, p := range r.peers {
		peers = append(peers, p)
	}
	r.mu.Unlock()
	for _, p := range peers {
		p.close()
	}
	r.follower.close()
	r.wg.Wait()
}

// Map exposes the shard map table.
func (r *Replicator) Map() *shardmap.Table { return r.o.Map }

// ReplicaState returns a copy of the follower-side state, for tests and the
// admin surface.
func (r *Replicator) ReplicaState() *store.State { return r.follower.snapshot(holdSnapshotOther) }

// CheckpointState is the replica store's checkpoint snapshot (store.Snapshot).
func (r *Replicator) CheckpointState(appended func() uint64) *store.State {
	return r.follower.checkpointState(appended)
}

// Tap is installed on the node's own store: every record this node writes
// is fanned out to the session's or the topic's replica targets.
func (r *Replicator) Tap(rec store.Record) func() {
	m := r.o.Map.Current()
	if m.Version == 0 || r.o.Transport == nil {
		return nil
	}
	var targets []string
	var epoch uint64
	var after func()
	if store.IsRetained(rec.Type) {
		shard := r.o.Map.ShardOf(rec.Key.Namespace, rec.Key.Topic)
		if m.Shards[shard].Leader != r.o.Node {
			return nil // not this node's shard to replicate
		}
		r.notePresence(shard, store.IsRetainedDelete(rec.Type))
		targets = recordTargets(m, shard, r.o.Node, true)
		epoch = m.Version
		// A new follower's whole push of the shard holds this for writing,
		// so this record lands either in what the push read or behind it.
		r.pushMu.RLock()
		defer r.pushMu.RUnlock()
	} else {
		r.taps.calls.Add(1)
		if (store.IsSessionUpsert(rec.Type) || store.IsSessionDelete(rec.Type)) && !r.o.HandOverOwnedCopies {
			// This node is writing or deleting the session as its owner, so a copy
			// of it in the replica store is a previous owner's and must go. The
			// drop appends to the replica store and takes the follower's
			// mutex, and it is answerable to nothing this append does, so it
			// runs once the store's lock is released rather than behind it.
			ns, id, at := rec.Key.Namespace, rec.Key.ClientID, rec.Key.Epoch
			reason := "owned-upsert"
			if store.IsSessionDelete(rec.Type) {
				reason = "owned-delete"
			}
			after = func() { r.dropOwnedCopyAfterWrite(ns, id, at, reason) }
		}
		shard := r.o.Map.ShardOf(rec.Key.Namespace, rec.Key.ClientID)
		targets = recordTargets(m, shard, r.o.Node, false)
		// The record carries the epoch this node held when it wrote it. It is
		// not looked up here: the lease table's read lock is starved by its
		// own writer during a takeover storm, and this tap runs inside the
		// store's lock, so a lookup made every connect wait on the churn the
		// storm was already producing. It is also the more accurate value —
		// a lookup at this point can return a newer epoch belonging to a
		// different owner and stamp it on this node's record.
		epoch = rec.Key.Epoch
		if epoch == 0 {
			// A session record with no epoch was written before its owner
			// held a generation for the client. A follower whose table holds
			// one refuses it as stale, which marks the key not held and fails
			// the barrier over it — a refused publish, diagnosable only from
			// the refusal until now. The lookup is here and not above because
			// it runs for this record alone (WI 1570).
			r.noteUnstamped(rec)
		}
	}
	if len(targets) == 0 {
		return after
	}
	f := frame{Kind: kindAppend, Type: rec.Type, Namespace: rec.Key.Namespace, ClientID: rec.Key.ClientID, Topic: rec.Key.Topic, Epoch: epoch, Payload: rec.Payload}
	began := time.Now()
	for _, t := range targets {
		r.outboundFor(t).push(f)
	}
	pushed := time.Now()
	r.count("hoardmq_replication_records_total", map[string]string{"direction": "out"}, float64(len(targets)))
	if !store.IsRetained(rec.Type) {
		r.taps.pushNs.Add(int64(pushed.Sub(began)))
		r.taps.countNs.Add(int64(time.Since(pushed)))
	}
	return after
}

// dropOwnedCopyAfterWrite drops this node's replica copy of a session it has
// just written or deleted as its owner, unless the copy is provably a later
// owner's: the lease can move away between the write and this call, and the
// node that took it over replicates the session back here at its own, higher
// epoch.
func (r *Replicator) dropOwnedCopyAfterWrite(ns, id string, wroteAt uint64, reason string) {
	began := time.Now()
	copyEpoch, held := r.follower.copyEpoch(ns, id)
	switch {
	case dropStaleOwnedCopy(held, copyEpoch, wroteAt):
		if r.follower.dropCopy(ns, id, reason) {
			r.drops.dropped.Add(1)
		}
	case held:
		r.drops.kept.Add(1)
		r.count("hoardmq_replicas_kept_total", map[string]string{"reason": "owned-later-epoch"}, 1)
		r.log.Debug("replica copy kept: later than this node's own write", "client", id, "copyEpoch", copyEpoch, "wroteAt", wroteAt)
	}
	r.drops.checks.Add(1)
	r.drops.ns.Add(int64(time.Since(began)))
}

// dropStaleOwnedCopy decides whether a replica copy this node holds for a
// session it has just written as owner is the previous owner's. Only a copy
// at a higher epoch than the record survives, and only when the record
// carries an epoch at all: a record written before its owner held a
// generation for the client (WI 1570) still clears what came before it.
func dropStaleOwnedCopy(held bool, copyEpoch, recordEpoch uint64) bool {
	if !held {
		return false
	}
	return recordEpoch == 0 || copyEpoch <= recordEpoch
}

// noteUnstamped reports a session record written without a lease epoch while
// this node's lease table holds one for the client.
func (r *Replicator) noteUnstamped(rec store.Record) {
	l, ok := r.o.Leases.Lookup(rec.Key.Namespace, rec.Key.ClientID)
	if !ok || l.Epoch == 0 {
		return
	}
	r.count("hoardmq_replication_unstamped_total", map[string]string{"type": store.RecordName(rec.Type)}, 1)
	r.log.Warn("session record written without a lease epoch", "client", rec.Key.ClientID, "type", store.RecordName(rec.Type), "leaseEpoch", l.Epoch, "leaseOwner", l.Owner)
}

// StreamState is what an operator is shown about one peer's stream: whether
// no barrier to it may pass, why, and for how long (WI 1523). The age is
// measured here, against the clock the block was recorded with, so a reader
// need not hold that clock to read it.
type StreamState struct {
	Peer    string
	Reason  string // "holed", "refused", or "" while the stream carries barriers
	Blocked time.Duration
}

// StreamStates reports every peer this node replicates to, blocked or not. A
// blocked stream is why a QoS 1 or 2 publish that needs that peer is refused,
// and the age of the state is what says whether it is happening now or
// happened once (WI 1519's question, asked of the product rather than the
// harness).
func (r *Replicator) StreamStates() []StreamState {
	r.mu.Lock()
	peers := make([]*outbound, 0, len(r.peers))
	for _, p := range r.peers {
		peers = append(peers, p)
	}
	r.mu.Unlock()
	now := r.o.Clock()
	named := r.namedByMap()
	out := make([]StreamState, 0, len(peers))
	for _, p := range peers {
		reason, since := p.blocked()
		if !named(p.name) {
			// No barrier waits on it, so it blocks none.
			reason = ""
		}
		var held time.Duration
		if reason != "" && !since.IsZero() {
			held = now.Sub(since)
		}
		out = append(out, StreamState{Peer: p.name, Reason: reason, Blocked: held})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Peer < out[j].Peer })
	return out
}

// Backlog is the frames pushed to peers and not yet acknowledged, summed over
// the peers: how far the followers lag this node. A broken stream's frames
// are counted until its resync, as the peer holds none of them.
func (r *Replicator) Backlog() int64 {
	r.mu.Lock()
	peers := make([]*outbound, 0, len(r.peers))
	for _, p := range r.peers {
		peers = append(peers, p)
	}
	r.mu.Unlock()
	var n int64
	for _, p := range peers {
		p.mu.Lock()
		if p.sent > p.ackSeq {
			n += int64(p.sent - p.ackSeq)
		}
		p.mu.Unlock()
	}
	return n
}

// BacklogByPeer is Backlog's terms: for each peer, the frames pushed and not
// yet acknowledged. Streams are per peer, so this is the followers' lag.
func (r *Replicator) BacklogByPeer() map[string]int64 {
	r.mu.Lock()
	peers := make([]*outbound, 0, len(r.peers))
	for _, p := range r.peers {
		peers = append(peers, p)
	}
	r.mu.Unlock()
	out := make(map[string]int64, len(peers))
	for _, p := range peers {
		p.mu.Lock()
		var n int64
		if p.sent > p.ackSeq {
			n = int64(p.sent - p.ackSeq)
		}
		p.mu.Unlock()
		out[p.name] = n
	}
	return out
}

// HoledStreams names the peers whose stream carries a hole — frames were
// dropped and no resync has repaired it — so no barrier to them may pass. A
// publish that needs one of those peers is refused until the resync lands,
// which is why the simulation harness waits for this before it calls a
// cluster ready to take a publish (WI 1570).
func (r *Replicator) HoledStreams() []string {
	r.mu.Lock()
	peers := make([]*outbound, 0, len(r.peers))
	for _, p := range r.peers {
		peers = append(peers, p)
	}
	r.mu.Unlock()
	named := r.namedByMap()
	var out []string
	for _, p := range peers {
		if named(p.name) && p.holed() {
			out = append(out, p.name)
		}
	}
	sort.Strings(out)
	return out
}

// RefusedKeys reports, per peer, the keys the peer has refused a record for
// and not since been given a whole copy of, with the class of each refusal.
// For the harness and diagnostics; it takes each peer's lock once.
func (r *Replicator) RefusedKeys() map[string]map[store.Scope]string {
	r.mu.Lock()
	peers := make([]*outbound, 0, len(r.peers))
	for _, p := range r.peers {
		peers = append(peers, p)
	}
	r.mu.Unlock()
	out := map[string]map[store.Scope]string{}
	for _, p := range peers {
		if keys := p.refusedKeys(); keys != nil {
			out[p.name] = keys
		}
	}
	return out
}

// Barrier implements session.Replication: it returns once every peer the
// named keys' records were sent to under the current map has acknowledged
// everything pushed to it so far, and a peer that is down fails it. No other
// peer is waited on, so a node that is down but still named fails only the
// publishes whose records it holds. A key refused by any peer the map names
// still fails it: only a whole rewrite of the key or a resync ends a refusal,
// never a map change.
func (r *Replicator) Barrier(ctx context.Context, keys []store.Scope) error {
	return r.barrier(ctx, keys, false)
}

// barrier is Barrier; handOver waits for a hand-over of retained copies, which
// a moving shard takes.
func (r *Replicator) barrier(ctx context.Context, keys []store.Scope, handOver bool) error {
	if r.o.Transport == nil || r.o.NoBarrier {
		return nil
	}
	m := r.o.Map.Current()
	if !handOver {
		for _, k := range keys {
			if k.Topic == "" {
				continue
			}
			if shard := r.o.Map.ShardOf(k.Namespace, k.Topic); r.retainedMoving(m, shard) {
				// Its old holders keep copies taken before the move, which a
				// lost leader's successor is handed: a write acknowledged now
				// would be missing from them.
				r.count("hoardmq_replication_barrier_failures_total", map[string]string{"peer": "-", "reason": "moving"}, 1)
				return &barrierError{"-", "moving", fmt.Errorf("%w: retained shard %d is moving", ErrReplicaUnavailable, shard)}
			}
		}
	}
	if k, seen, ok := r.supersededKey(keys); ok {
		r.count("hoardmq_replication_barrier_failures_total", map[string]string{"peer": "-", "reason": "superseded"}, 1)
		r.log.Debug("barrier refused: a later grant was seen for a session it names", "client", k.ClientID, "epoch", k.Epoch, "seen", seen)
		return &barrierError{"-", "superseded", fmt.Errorf("%w: %s's copy at epoch %d is behind grant %d", ErrReplicaUnavailable, k.ClientID, k.Epoch, seen)}
	}
	// A wait that names nothing has nothing to check, and the records it was
	// meant to cover would be acknowledged unexamined. Blocking is the only
	// safe reading; every caller that gates a write names what it wrote.
	targets, named := barrierTargets(m, r.o.Map.ShardOf, r.o.Node, keys)
	if !named {
		r.count("hoardmq_replication_barrier_failures_total", map[string]string{"peer": "-", "reason": "unnamed"}, 1)
		return fmt.Errorf("%w: a durability wait named no records", ErrReplicaUnavailable)
	}
	// Until the rebalance has queued its whole writes for m, records written
	// under the map before it sit only at that map's targets, which keep
	// their copies until the owner's confirmation: wait on those that are up.
	var before []string
	if r.fanned.Load() < m.Version {
		r.recMu.Lock()
		prev := r.reconciled
		r.recMu.Unlock()
		before = spanTargets(prev, m, r.o.Map.ShardOf, r.o.Node, keys, targets, r.up)
	}
	select {
	case <-r.stop:
		return fmt.Errorf("%w: replication stopped", ErrReplicaUnavailable)
	default:
	}
	start := r.o.Clock()
	ctx, cancel := context.WithTimeout(ctx, r.o.AckTimeout)
	defer cancel()
	r.mu.Lock()
	peers := make([]*outbound, 0, len(r.peers))
	for _, p := range r.peers {
		peers = append(peers, p)
	}
	r.mu.Unlock()
	sort.Slice(peers, func(i, j int) bool { return peers[i].name < peers[j].name })
	for _, p := range peers {
		spanned := contains(before, p.name)
		if !contains(m.Basis, p.name) && !spanned {
			continue
		}
		var err error
		if spanned || contains(targets, p.name) {
			err = p.wait(ctx, r.up, keys)
		} else {
			err = p.refusal(keys)
		}
		if err != nil {
			reason := barrierReason(err)
			r.count("hoardmq_replication_barrier_failures_total", map[string]string{"peer": p.name, "reason": reason}, 1)
			p.trace.failed(reason)
			return err
		}
	}
	r.count("hoardmq_replication_barrier_seconds_total", nil, r.o.Clock().Sub(start).Seconds())
	return nil
}

// supersededKey finds a named session key whose copy here is behind a grant
// this node has seen: another node owns the session now, and a write into
// this copy reaches no copy that node reads, whatever a target's lease table
// says yet.
func (r *Replicator) supersededKey(keys []store.Scope) (store.Scope, uint64, bool) {
	if r.o.Tokens == nil {
		return store.Scope{}, 0, false
	}
	for _, k := range keys {
		if k.ClientID == "" {
			continue
		}
		if seen := r.o.Tokens.Seen(k.Namespace, k.ClientID); keySuperseded(k.Epoch, seen) {
			return k, seen, true
		}
	}
	return store.Scope{}, 0, false
}

// keySuperseded decides whether a write at epoch is into a copy a later grant
// replaced. A record with no epoch is judged elsewhere, by its sender's
// ownership, as a follower judges it.
func keySuperseded(epoch, seen uint64) bool { return epoch != 0 && seen > epoch }

// recordTargets lists the peers this node sends the records of one key on a
// shard to under m: a session's targets with this node as its owner; a
// retained topic's followers when this node leads the shard, else the leader,
// which a retained write is forwarded to.
func recordTargets(m shardmap.Map, shard int, self string, retained bool) []string {
	if shard < 0 || shard >= len(m.Shards) {
		return nil
	}
	if !retained {
		return m.Targets(shard, self)
	}
	s := m.Shards[shard]
	switch s.Leader {
	case self:
		return s.Followers
	case "":
		return nil
	}
	return []string{s.Leader}
}

// barrierTargets is the union of the named keys' record targets under m. It
// reports false when the list is empty or a key names neither a client nor a
// topic: such a key has no targets, and waiting on nothing for it would pass it
// unexamined.
func barrierTargets(m shardmap.Map, shardOf func(ns, name string) int, self string, keys []store.Scope) ([]string, bool) {
	if len(keys) == 0 {
		return nil, false
	}
	var out []string
	for _, k := range keys {
		var peers []string
		switch {
		case k.ClientID != "":
			peers = recordTargets(m, shardOf(k.Namespace, k.ClientID), self, false)
		case k.Topic != "":
			peers = recordTargets(m, shardOf(k.Namespace, k.Topic), self, true)
		default:
			return nil, false
		}
		for _, p := range peers {
			if !contains(out, p) {
				out = append(out, p)
			}
		}
	}
	return out, true
}

// spanTargets lists the named keys' record targets under prev, a map older
// than cur, that are up and are not already targets under cur. It is empty
// when prev has no version or is not older.
func spanTargets(prev, cur shardmap.Map, shardOf func(ns, name string) int, self string, keys []store.Scope, targets []string, up func(string) bool) []string {
	if prev.Version == 0 || prev.Version >= cur.Version {
		return nil
	}
	old, _ := barrierTargets(prev, shardOf, self, keys)
	var out []string
	for _, p := range old {
		if !contains(targets, p) && up(p) {
			out = append(out, p)
		}
	}
	return out
}

// namedByMap reports, for the map as it stands now, whether a peer is one of
// the nodes it was computed from. It is read when asked, never remembered: a
// node the map names again is waited on again, with its stream as it is.
func (r *Replicator) namedByMap() func(peer string) bool {
	basis := r.o.Map.Current().Basis
	return func(peer string) bool { return contains(basis, peer) }
}

// up reports whether the transport has the peer connected.
func (r *Replicator) up(name string) bool {
	for _, p := range r.o.Transport.Peers() {
		if p.Name == name {
			return p.State == transport.Up
		}
	}
	return false
}

func (r *Replicator) outboundFor(name string) *outbound {
	r.mu.Lock()
	defer r.mu.Unlock()
	p := r.peers[name]
	if p == nil {
		if r.stopped {
			// A peer whose link changed while the replicator was
			// stopping: the hazard follower.enqueue refuses a frame
			// for. The caller gets a closed stream rather than nil, so
			// its push is dropped and its wait refuses, as they do for
			// a stream Stop closed a moment earlier. It is not recorded
			// in peers, which stays the set of streams this node had.
			return stoppedOutbound(r, name)
		}
		p = newOutbound(r, name)
		r.peers[name] = p
	}
	return p
}

// receiveAck runs on the transport reader for the acknowledging peer.
func (r *Replicator) receiveAck(peer string, payload []byte) {
	var a ack
	if err := json.Unmarshal(payload, &a); err != nil {
		return
	}
	r.mu.Lock()
	p := r.peers[peer]
	r.mu.Unlock()
	if p == nil {
		return
	}
	if a.Rejected {
		r.count("hoardmq_replication_acks_total", map[string]string{"outcome": "rejected"}, 1)
		if strings.HasPrefix(a.Reason, unappliedReason) {
			// The follower's copy of the key has diverged: a record for it
			// changed nothing there. The key is refused, as for any rejected
			// record, and repaired now — the session sent whole from what
			// this node serves — before the refusal wakes the barrier, so
			// the repair's push has cleared the key: a barrier over the
			// refused record waits for the repair's acknowledgement rather
			// than failing.
			scope, scoped := p.scopeOf(a.Seq, a.named())
			r.log.Warn("a follower's replica has diverged; repairing the key", "follower", peer, "seq", a.Seq, "reason", a.Reason)
			if p.failedAt(a.Seq, a.Reason, a.named()) || !scoped {
				r.resync(peer)
			} else {
				r.repair(peer, p, scope)
			}
			p.refusalArrived()
			return
		}
		if strings.HasPrefix(a.Reason, gapReason) {
			// Frames were lost with the link up, or the follower's replica
			// sync failed: either way its copy lacks frames it may have been
			// sent, as after a failed send, and the resync is what repairs it.
			if strings.HasPrefix(a.Reason, syncFailedReason) {
				r.log.Warn("a follower's replica sync failed; resyncing", "follower", peer, "seq", a.Seq, "reason", a.Reason)
			} else {
				r.log.Warn("replication frames lost on the way to a follower; resyncing", "follower", peer, "seq", a.Seq, "reason", a.Reason)
			}
			p.trace.rejected(rejGap, nil)
			p.broken()
			r.resync(peer)
			p.acked(a.Seq)
			return
		}
		// Usual cause: an old owner's clean-up record after a takeover. A
		// rejected record is not held by the follower, so the barriers
		// covering it fail as they do for a failed send.
		r.log.Debug("replication record rejected by follower", "follower", peer, "seq", a.Seq, "reason", a.Reason)
		if p.failedAt(a.Seq, a.Reason, a.named()) {
			// The refusal could not be scoped, so the stream is blocked whole
			// until a resync replaces the peer's copy. Ask for it here: no
			// state change is coming to trigger one.
			r.resync(peer)
		}
		// Not acked: the frames ahead of this one in the follower's batch
		// are unsynced until its acknowledgement says otherwise.
		p.refusalArrived()
		return
	}
	r.count("hoardmq_replication_acks_total", map[string]string{"outcome": "ok"}, 1)
	p.acked(a.Seq)
}

// receive runs on the transport reader for the sending peer: frames are
// queued to the follower's worker for that peer so a slow apply never holds
// the link.
func (r *Replicator) receive(peer string, payload []byte) {
	var f frame
	if err := json.Unmarshal(payload, &f); err != nil {
		r.log.Warn("replication frame undecodable", "peer", peer, "err", err)
		return
	}
	switch f.Kind {
	case kindFetchResult, kindRetainedResult:
		if ch, ok := r.waiters.Load(f.Req); ok {
			select {
			case ch.(chan frame) <- f:
			default:
			}
		}
	case kindFetch:
		// A fetch reads a copy and orders against nothing this peer has sent:
		// the peer's appends are for sessions it owns, and it asks for one it
		// holds no state for. So it is answered apart from the peer's worker,
		// whose queue after a node loss is seconds deep in the storm's appends
		// while a CONNECT holds a handshake slot for the answer. A full queue
		// falls back to the worker: the reader never blocks and a fetch is
		// never dropped.
		f.queuedAt = time.Now()
		if r.fetchq != nil {
			select {
			case r.fetchq <- fetchJob{peer: peer, fr: f}:
				return
			default:
				r.fetches.overflowed.Add(1)
			}
		}
		r.follower.enqueue(peer, f)
	default:
		f.queuedAt = time.Now()
		r.follower.enqueue(peer, f)
	}
}

// answerFetches is one of the pool's goroutines.
func (r *Replicator) answerFetches() {
	defer r.wg.Done()
	for {
		select {
		case <-r.stop:
			return
		case job := <-r.fetchq:
			r.fetches.pooled.Add(1)
			r.follower.answerFetch(job.peer, job.fr)
			r.count("hoardmq_replication_records_total", map[string]string{"direction": "in"}, 1)
		}
	}
}

// request sends a frame and waits for the answer with the same request id.
func (r *Replicator) request(ctx context.Context, peer string, f frame) (frame, bool) {
	f.Req = r.reqs.Add(1)
	ch := make(chan frame, 1)
	r.waiters.Store(f.Req, ch)
	defer r.waiters.Delete(f.Req)
	timed := f.Kind == kindFetch
	began := time.Now()
	if timed {
		r.fetches.requests.Add(1)
		defer func() { r.fetches.requestNs.Add(since(began)) }()
	}
	body, _ := json.Marshal(f)
	sending := time.Now()
	err := r.o.Transport.Send(ctx, peer, transport.ReplicationAppend, body)
	if timed {
		r.fetches.sendNs.Add(since(sending))
	}
	if err != nil {
		if timed {
			r.fetches.requestsEnded.Add(1)
		}
		return frame{}, false
	}
	select {
	case ans := <-ch:
		return ans, true
	case <-ctx.Done():
	case <-r.stop:
	}
	if timed {
		r.fetches.requestsEnded.Add(1)
	}
	return frame{}, false
}

// peerChanged resyncs a peer that comes back: everything it should hold
// from this node is sent whole.
func (r *Replicator) peerChanged(peer string, s transport.State) {
	if s != transport.Up {
		// The peer may gain or lose retained messages while the link is not
		// this one.
		r.forgetPresence(peer)
		// Frames admitted to a link that then closed are gone: the barriers
		// covering them must fail, and the peer is resynced whole on return.
		r.mu.Lock()
		p := r.peers[peer]
		r.mu.Unlock()
		if p != nil {
			p.broken()
		}
		return
	}
	// The peer has not been told anything over this link. What it said is
	// kept: the link that went away cleared it, so a statement held now
	// arrived over this one, ahead of this notification.
	r.forgetTold(peer)
	r.announcePresence()
	r.resync(peer)
}

// resync replaces a peer's copies whole, in the background, and closes the
// hole a dropped batch left in its stream. Only one runs per peer.
func (r *Replicator) resync(peer string) {
	p := r.outboundFor(peer)
	gen, ok, wait := p.startResync(r.o.Clock(), r.o.AckTimeout)
	if !ok {
		if wait > 0 {
			r.resyncAfter(peer, p, wait)
		}
		return
	}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		for {
			if r.o.Map.Current().Version != 0 {
				r.resyncPeer(peer)
			}
			p.push(frame{Kind: kindResyncMark})
			// With no map there is nothing the peer should hold, so the
			// stream is whole by vacuity.
			if p.whole(gen, r.o.Clock()) || !r.up(peer) {
				// A peer that is down will be resynced by its next Up.
				return
			}
			// It broke again while this one ran. Retrying at once turns the
			// repair into a lock convoy on the session manager, so wait out
			// the same interval an external trigger would have to.
			select {
			case <-time.After(r.o.AckTimeout):
			case <-r.stop:
				return
			}
			if gen, ok, wait = p.startResync(r.o.Clock(), r.o.AckTimeout); !ok {
				if wait > 0 {
					r.resyncAfter(peer, p, wait)
				}
				return
			}
		}
	}()
}

// resyncAfter arranges one resync of a peer whose resync the cooldown
// refused, for when the cooldown ends. Without it a stream that broke within
// the cooldown stayed holed, and every barrier for the peer failed, until some
// later failure or state change happened to ask again (WI 1491). At most one
// is pending per peer.
func (r *Replicator) resyncAfter(peer string, p *outbound, wait time.Duration) {
	p.mu.Lock()
	if p.retryPending || p.closed {
		p.mu.Unlock()
		return
	}
	p.retryPending = true
	p.mu.Unlock()
	p.retries.Add(1)
	r.count("hoardmq_replication_resync_deferred_total", nil, 1)
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		t := time.NewTimer(wait)
		defer t.Stop()
		select {
		case <-t.C:
		case <-r.stop:
		}
		p.mu.Lock()
		p.retryPending = false
		p.mu.Unlock()
		select {
		case <-r.stop:
			return
		default:
		}
		r.resync(peer)
	}()
}

// resyncPeer sends the sessions and retained messages the peer replicates
// for this node under the current map.
func (r *Replicator) resyncPeer(peer string) {
	m := r.o.Map.Current()
	if m.Version == 0 {
		return
	}
	st := r.o.Sessions.Snapshot()
	n := 0
	bulk := newBulk("resync")
	for _, ss := range st.Sessions {
		shard := r.o.Map.ShardOf(ss.Namespace, ss.ClientID)
		if contains(m.Targets(shard, r.o.Node), peer) {
			bulk.note(r.outboundFor(peer))
			r.sendSnapshot(peer, ss)
			n++
		}
	}
	bulk.done()
	for _, msg := range st.Retained {
		shard := r.o.Map.ShardOf(msg.Namespace, msg.Topic)
		if m.Shards[shard].Leader == r.o.Node && contains(m.Shards[shard].Followers, peer) {
			rec := store.RetainRecord(msg)
			r.outboundFor(peer).push(frame{Kind: kindAppend, Type: rec.Type, Namespace: msg.Namespace, Topic: msg.Topic, Epoch: m.Version, Payload: rec.Payload})
			n++
		}
	}
	if n > 0 {
		r.log.Info("replicas resynced to peer", "peer", peer, "items", n)
	}
}

// unappliedReason begins the rejection reason a follower gives for a record
// it could not apply on a whole stream; the owner repairs the key on it.
const unappliedReason = "unapplied"

// repair sends a peer the session whole, from what this node serves, after
// the peer refused a record for it as unappliable; a session this node no
// longer serves is deleted there instead. One repair per key per
// acknowledgement timeout: a refusal inside that interval is counted, not
// answered, since the repair already on the stream answers it.
func (r *Replicator) repair(peer string, p *outbound, scope store.Scope) {
	bare := scope.Bare()
	if !p.repairDue(bare, r.o.Clock(), r.o.AckTimeout) {
		r.count("hoardmq_replication_repairs_total", map[string]string{"outcome": "repeat"}, 1)
		return
	}
	if ss, ok := r.o.Sessions.Export(scope.Namespace, scope.ClientID); ok {
		r.sendSnapshot(peer, ss)
		r.count("hoardmq_replication_repairs_total", map[string]string{"outcome": "snapshot"}, 1)
		r.audit("replication.repair", scope.Namespace+"/"+scope.ClientID, "snapshot", map[string]string{"to": peer})
		return
	}
	var epoch uint64
	if l, ok := r.o.Leases.Lookup(scope.Namespace, scope.ClientID); ok {
		epoch = l.Epoch
	}
	del := store.DeleteRecord(scope.Namespace, scope.ClientID)
	p.push(frame{Kind: kindAppend, Type: del.Type, Namespace: scope.Namespace, ClientID: scope.ClientID, Epoch: epoch, Payload: del.Payload})
	r.count("hoardmq_replication_repairs_total", map[string]string{"outcome": "delete"}, 1)
	r.audit("replication.repair", scope.Namespace+"/"+scope.ClientID, "delete", map[string]string{"to": peer})
}

// sendHandOver pushes a copy this node holds for another owner, at the
// copy's own epoch: the receiver accepts it only while that is the table's
// epoch, so a copy older than the lease is refused rather than taken for
// the owner's current state.
func (r *Replicator) sendHandOver(peer string, ss *store.SessionState) {
	r.outboundFor(peer).push(frame{Kind: kindSnapshot, Namespace: ss.Namespace, ClientID: ss.ClientID, Epoch: ss.Epoch, State: ss})
	r.count("hoardmq_replication_records_total", map[string]string{"direction": "out"}, 1)
}

func (r *Replicator) sendSnapshot(peer string, ss *store.SessionState) {
	var epoch uint64
	if l, ok := r.o.Leases.Lookup(ss.Namespace, ss.ClientID); ok {
		epoch = l.Epoch
	}
	r.outboundFor(peer).push(frame{Kind: kindSnapshot, Namespace: ss.Namespace, ClientID: ss.ClientID, Epoch: epoch, State: ss})
	r.count("hoardmq_replication_records_total", map[string]string{"direction": "out"}, 1)
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

func (r *Replicator) count(name string, labels map[string]string, delta float64) {
	if r.o.Metrics != nil {
		r.o.Metrics.Add(name, labels, delta)
	}
}

func (r *Replicator) gauge(name string, v float64) {
	if r.o.Metrics != nil {
		r.o.Metrics.Set(name, nil, v)
	}
}

func (r *Replicator) audit(action, target, outcome string, detail map[string]string) {
	if r.o.Audit != nil {
		r.o.Audit(action, target, outcome, detail)
	}
}

// sortedKeys is a small helper for deterministic iteration in logs.
func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// level is the cluster feature level this node stamps on its frames.
func (r *Replicator) level() uint64 {
	if r.o.Level == nil {
		return 1
	}
	return r.o.Level()
}

// buildLevel is the highest feature level this build understands.
func (r *Replicator) buildLevel() uint64 {
	if r.o.BuildLevel == nil {
		return 1
	}
	return r.o.BuildLevel()
}
