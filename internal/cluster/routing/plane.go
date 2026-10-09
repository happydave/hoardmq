package routing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/dispatch"
	"github.com/happydave/hoardmq/internal/hook"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/otel"
	"github.com/happydave/hoardmq/internal/store"
	"github.com/happydave/hoardmq/internal/topic"
)

// Sink is where forwarded messages are delivered on the receiving node.
type Sink interface {
	// InjectForwarded delivers to this node's direct subscribers and
	// returns how many; none means the sender's route was stale. The scopes
	// name the sessions it delivered into, which the durability wait that
	// acknowledges this batch has to cover. An error means a record the
	// delivery wrote was not journaled, so the frame is refused.
	InjectForwarded(msg *message.Message) (int, []store.Scope, error)
	// InjectGroup delivers to one local member of a shared group and
	// reports whether one existed, with the session it chose, and an error
	// as InjectForwarded does.
	InjectGroup(msg *message.Message, group string) (store.Scope, bool, error)
	// Durable returns once everything injected so far is durable on this
	// node and its replicas; a forward is acknowledged after it. keys name
	// the sessions injected, so a peer's refusal of one of their records
	// fails the wait rather than being acknowledged to the forwarding node.
	Durable(ctx context.Context, keys []store.Scope) error
}

// ErrRoutesUnavailable is returned by Forward when a subscriber's node is
// down and its sessions have not been re-homed: the publish cannot be
// acknowledged as delivered to them.
var ErrRoutesUnavailable = errors.New("routing: a matching subscriber's node is unreachable")

// ErrNotServing is a forwarded QoS 1 or 2 frame refused because this node
// does not take deliveries into its sessions yet (Options.Serving).
var ErrNotServing = errors.New("routing: this node is not serving its sessions yet")

// Transport is what the plane needs from the cluster transport.
type Transport interface {
	Send(ctx context.Context, peer string, c transport.Class, payload []byte) error
	Handle(c transport.Class, h transport.Handler)
	Observe(o transport.Observer)
	ObserveData(o transport.Observer)
	Peers() []transport.PeerInfo
}

// Options configure the plane.
type Options struct {
	Node         string
	Transport    Transport
	Strategy     dispatch.Strategy
	SyncInterval time.Duration // default 30 s
	// IgnoreForwardGaps, when set, makes the receiver apply a forwarded
	// frame that arrives past a gap in the peer's sequence without reporting
	// the gap: the defect the check exists to remove.
	IgnoreForwardGaps bool
	// ApplyGate, when set, is called by a peer's apply loop before it applies
	// each batch of that peer's forwarded frames, so a test can hold them.
	ApplyGate func(node, peer string)
	// MoveGrace is how long the removal of a moving session's routes is held
	// back (see Moving); default 5 s.
	MoveGrace time.Duration
	// Members, when set, lists the cluster's member names; owners not in it
	// are forgotten at each sync.
	Members func() []string
	// OwnedSessions, when set, reports how many sessions the cluster's
	// replicated lease state says an owner still holds, and whether that
	// state can be read yet. It is what bounds the unknown-owner refusal
	// (WI 1351): an owner proved to hold nothing needs no routes. A node
	// that cannot read the state yet cannot prove anything, so "not known"
	// counts as "holds something".
	OwnedSessions func(node string) (count int, known bool)
	// Serving, when set, reports whether this node takes forwarded QoS 1
	// and 2 deliveries into its sessions yet; a frame that would deliver one
	// while it does not is refused to its sender. Nil is always.
	Serving func() bool
	// AwaitLoad, when set, holds every route update and full set this node
	// would send until Loaded is called.
	AwaitLoad bool
	// AdoptionState, when set, reports the lease log index this node has
	// applied with its change hooks run, how many adoptions it has not
	// finished, and whether that state can be read yet: this node's own part
	// in forgetting a lost owner's routes (served.go).
	AdoptionState func() (index uint64, unfinished int, known bool)
	// ServedFact reads a node's served fact from the lease log: the index up
	// to which its adoptions are finished and its route mark, zeros when it
	// has said nothing. ProposeServed puts this node's in the log and returns
	// once it is committed or ctx ends.
	ServedFact    func(node string) (index, routes uint64)
	ProposeServed func(ctx context.Context, index, routes uint64) error
	// AdoptedThrough, when set, is the highest grant of a session in transit
	// this node took and now holds: its served fact must reach it, so a
	// fact already at the emptied index is proposed again.
	AdoptedThrough func() uint64
	// Cover, when set, reads a node's cover from the lease log: the grant
	// through which the sessions it took in transit are held, and its route
	// mark then. CoverDue says whether this node's own is due, and through
	// which grant; ProposeCover puts it in the log.
	Cover        func(node string) (grant, routes uint64)
	CoverDue     func() (grant uint64, due bool)
	ProposeCover func(ctx context.Context, grant, routes uint64) error
	// EmptiedAt, when set, is the lease log index of the last command that
	// took a lease from the node: for a node that owns nothing, where it
	// was emptied. It comes from the log, so every member holds the same
	// number, and a member that reports hooks run to it has said enough for
	// all of them. Zero when unknown; LeaseIndex — this node's applied index,
	// read after OwnedSessions, so at or past whatever emptied the owner — is
	// the fallback, correct but private to this node, so a peer's report
	// made just before it costs a wait for that peer's next sync.
	EmptiedAt  func(node string) uint64
	LeaseIndex func() uint64
	// ServedInterval is how often the hidden owners, and the unknown ones,
	// are judged; default 500 ms. With none of either and no fact owed, a
	// check reads no lease count.
	ServedInterval time.Duration
	Logger         *slog.Logger
	Metrics        hook.Metrics
	// Tracer records a span for each forward of a sampled publish and for
	// each forwarded frame received with a sampled trace; nil is tracing off.
	Tracer *otel.Tracer
	// GiveTimeout bounds how long this node, as a group's dispatch owner,
	// waits for a member to hold a message it gave on for another node's
	// publish before relaying it refused: the publisher's barrier timeout,
	// past which no barrier is waiting. Default 2 s.
	GiveTimeout time.Duration
}

// Envelope kinds within the forwarded-publish class and the route-update
// class.
const (
	fwdPublish     byte = 1 // deliver to direct subscribers
	fwdGroupDecide byte = 2 // to the group's dispatch owner: choose a member node
	fwdGroupGive   byte = 3 // to the chosen member node: deliver to one local member
	fwdAck         byte = 4 // every forwarded frame up to Seq is durable on the receiver
	fwdGiven       byte = 5 // the owner's outcome of a give for the decide at Seq: held, or Failed

	routeDelta byte = 1
	routeFull  byte = 2
)

type forwardEnvelope struct {
	Seq    uint64 `json:"seq,omitempty"`
	Failed bool   `json:"failed,omitempty"` // ack only: the frames up to Seq were not made durable
	// One, on a failed ack, narrows it to the frame at Seq: the receiver
	// refused that frame on its own account, and says nothing of the frames
	// before it, which its next ack covers. An older sender ignores it and
	// fails them all.
	One bool `json:"one,omitempty"`
	// Given, on an ack, lists the decide frames it covers that the group's
	// dispatch owner gave to another node: each is held until the owner
	// relays the give's outcome (fwdGiven). An older sender ignores it.
	Given []uint64 `json:"given,omitempty"`
	Group string   `json:"group,omitempty"`
	// Trace, on a forward of a sampled publish, is the forward span's
	// context as a W3C traceparent; the receiver's span is its child. An
	// older receiver ignores it.
	Trace string           `json:"trace,omitempty"`
	Msg   *message.Message `json:"msg"`
}

// outbound is this node's forward sequence to one peer and the peer's
// acknowledgement, for the barrier.
type outbound struct {
	// sendMu is held from taking a sequence until the transport has admitted
	// the frame, so frames reach the wire in sequence order: a receiver reads
	// a frame overtaken by a later one as a hole (WI 1708). Taken before p.mu.
	sendMu     sync.Mutex
	sent       uint64
	acked      uint64
	lastFailed uint64 // highest sequence whose send failed
	// refused holds the frames the receiver refused one at a time, oldest
	// first in refusedOrder, bounded by maxRefusedForwards.
	refused      map[uint64]struct{}
	refusedOrder []uint64
	// giveRefused names the refusals that were a give's, for the barrier's
	// reason; it holds only sequences also in refused.
	giveRefused map[uint64]struct{}
	// pending holds the forwards the receiver acknowledged but gave on to
	// another node, until the receiver relays the give's outcome; oldest
	// first in pendingOrder, which may hold sequences already settled.
	pending      map[uint64]struct{}
	pendingOrder []uint64
	waiters      []chan struct{}
}

// maxRefusedForwards bounds the frames a peer's refusals are remembered for.
// A refusal pushed out raises the failure mark to it, so forgetting one fails
// more forwards, never fewer.
const maxRefusedForwards = 4096

// refuseOne records that the receiver refused the frame at seq. Callers hold
// p.mu.
func (o *outbound) refuseOne(seq uint64) {
	if o.refused == nil {
		o.refused = make(map[uint64]struct{})
	}
	if _, ok := o.refused[seq]; ok {
		return
	}
	o.refused[seq] = struct{}{}
	o.refusedOrder = append(o.refusedOrder, seq)
	if len(o.refusedOrder) > maxRefusedForwards {
		old := o.refusedOrder[0]
		o.refusedOrder = o.refusedOrder[1:]
		delete(o.refused, old)
		delete(o.giveRefused, old)
		if old > o.lastFailed {
			o.lastFailed = old
		}
	}
}

// refuseGive records that the give made for the forward at seq was not held:
// a one-frame refusal, labelled as a give's. Callers hold p.mu.
func (o *outbound) refuseGive(seq uint64) {
	delete(o.pending, seq)
	o.refuseOne(seq)
	if _, ok := o.refused[seq]; ok {
		if o.giveRefused == nil {
			o.giveRefused = make(map[uint64]struct{})
		}
		o.giveRefused[seq] = struct{}{}
	}
}

// maxPendingGives bounds the forwards held for a give's outcome per peer. One
// pushed out is refused, never passed.
const maxPendingGives = 4096

// hold marks the forward at seq as awaiting its give's outcome. Callers hold
// p.mu.
func (o *outbound) hold(seq uint64) {
	if o.pending == nil {
		o.pending = make(map[uint64]struct{})
	}
	if _, ok := o.pending[seq]; ok {
		return
	}
	o.pending[seq] = struct{}{}
	o.pendingOrder = append(o.pendingOrder, seq)
	for len(o.pending) > maxPendingGives && len(o.pendingOrder) > 0 {
		old := o.pendingOrder[0]
		o.pendingOrder = o.pendingOrder[1:]
		if _, ok := o.pending[old]; ok {
			o.refuseGive(old)
		}
	}
	if len(o.pendingOrder) > 2*maxPendingGives {
		o.pendingOrder = o.livePending()
	}
}

// livePending is pendingOrder without the sequences already settled.
func (o *outbound) livePending() []uint64 {
	live := make([]uint64, 0, len(o.pending))
	for _, s := range o.pendingOrder {
		if _, ok := o.pending[s]; ok {
			live = append(live, s)
		}
	}
	return live
}

// Whether a forward's barrier fails, passes, or waits.
const (
	forwardWait = iota
	forwardPass
	forwardFailed
	forwardRefused
)

// forwardVerdict decides a forward's barrier from its sequence and the
// stream's marks. A frame the receiver refused fails even once a later
// acknowledgement covers it, since a batch the receiver delivered nowhere is
// acknowledged without a durability wait; every failure not placed on one frame
// fails everything at or below it. A pending frame was given on by the
// receiver to another node, so its acknowledgement does not cover the delivery
// until the give's outcome arrives.
func forwardVerdict(seq, lastFailed, acked uint64, refused, pending bool) int {
	switch {
	case lastFailed >= seq:
		return forwardFailed
	case refused:
		return forwardRefused
	case pending:
		return forwardWait
	case acked >= seq:
		return forwardPass
	}
	return forwardWait
}

// inbound is one peer's forwarded frames awaiting apply and acknowledgement,
// applied in order by a worker so the transport reader is never held.
type inbound struct {
	mu     sync.Mutex
	cond   *sync.Cond
	queue  []forwardFrame
	closed bool
	// seq is touched only by the peer's apply loop.
	seq inboundSeq
}

type forwardFrame struct {
	kind byte
	env  forwardEnvelope
}

// Plane is one node's routing plane.
type Plane struct {
	o       Options
	log     *slog.Logger
	table   *Table
	version atomic.Uint64
	// unknownOwners is set while at least one owner's route set is unknown
	// to this node, which makes every QoS 1 or 2 publish unacknowledgeable
	// (WI 1351). Read on the publish path, so it is an atomic rather than a
	// walk of the peer list; recomputed on peer, sync and membership events.
	unknownOwners atomic.Bool
	// unloaded holds every route send while the node's store has not been
	// loaded (Options.AwaitLoad): an empty set sent then would withdraw at
	// its peers the routes of sessions its journal still holds.
	unloaded atomic.Bool
	// knowledgeLogged makes the first evaluation report whichever state it
	// finds, so both ends of the window are in the log and its length can
	// be read off rather than inferred from the node's start time.
	knowledgeLogged atomic.Bool
	// knowMu orders the changes of unknownOwners with the episodes that
	// record them, the last MaxEpisodes of them.
	knowMu   sync.Mutex
	episodes []Episode
	// refreshMu makes one evaluation of the owners' routes at a time, so an
	// evaluation that read older facts cannot store its verdict after a
	// newer one. Taken after servedMu, never with mu held. unknownWhy is,
	// per unknown owner, the reason it was last held; refreshMu guards it.
	refreshMu  sync.Mutex
	unknownWhy map[string]string
	// afterJudge, set only by tests, runs in refreshOwnerKnowledge between
	// the judgement and its store.
	afterJudge func()

	mu       sync.Mutex
	sink     Sink
	rr       map[string]int    // round-robin position per group key
	sticky   map[string]string // group key + publisher → node
	rng      *rand.Rand
	out      map[string]*outbound
	in       map[string]*inbound
	lastSync map[string]time.Time // peer → when its route sync last arrived
	// settled is, per lost owner this node has had no full sync from, the
	// emptied index at which it found the owner's sessions served elsewhere
	// with their routes held here: at that index it is not judged again.
	// zeroAt is, per hidden owner, the lease index that owner was emptied
	// at. servedProposed is the highest index this node's served fact was
	// committed at, proposing whether a proposal is in flight, and
	// awaitRoutes, per member, the route mark this node is waiting to hold
	// before it can forget. See served.go.
	settled        map[string]uint64
	zeroAt         map[string]uint64
	servedProposed uint64
	proposing      bool
	// coverProposed is the highest grant this node's cover was committed at,
	// and coverProposing whether a cover proposal is in flight.
	coverProposed  uint64
	coverProposing bool
	awaitRoutes    map[string]uint64
	servedWake     chan struct{}
	// servedMu makes one served check at a time: a woken loop and a direct
	// caller must not both forget the same owner.
	servedMu sync.Mutex
	// batches is the number of the last delta batch composed, seeded like
	// the entry versions; proofs is, per peer, how far its route updates are
	// held without a gap. See routeproof.go.
	batches uint64
	proofs  map[string]*routeProof
	// forgets is what the served checks saw of each hidden owner, for the
	// operator.
	forgets forgetTimeline
	// refusals samples the forward refusals this node made or was told of.
	refusals refusalTrace
	gives    giveWatcher
	// pending is the route changes waiting to go out, coalesced by entry so
	// a filter that changed ten times while a send was in flight costs one
	// entry. fullTo names peers owed a complete set. Both are the flusher's,
	// and emit hands to them rather than sending: emit runs under a session
	// mutex (session.Manager.Subscribe holds it across router.Add) and the
	// route-update class is not urgent, so its enqueue blocks — which is how
	// 64.7% of a storm's mutex delay ended up in link.admit (WI 1356).
	pending  map[entryKey]Entry
	fullTo   map[string]bool
	flushing bool
	moving   map[routeID]time.Time   // filters whose session is moving, until when
	held     map[routeID]*time.Timer // removals waiting out a move
	ending   map[routeID]*time.Timer // moves of filters other sessions still hold, waiting out their grace
	// moves are the clients moving each filter, and byClient the filters each
	// client is moving; see moveproof.go.
	moves    map[routeID]map[string]*clientMove
	byClient map[clientKey]map[routeID]bool
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
	// stopped is set by Stop inside the critical section that closes the
	// inbounds, and read only under mu, so no apply loop is started after
	// the ones Stop will close have been chosen: it would add to the wait
	// group Stop is waiting on and block in cond.Wait forever. Not the stop
	// channel, which closes before the sync loop and the flusher have
	// exited, while a known peer's frames are still being applied.
	stopped bool

	stop      chan struct{}
	done      chan struct{}
	wake      chan struct{}
	flushDone chan struct{}

	// afterSend, set only by tests, runs in Forward between the sends and the
	// checks that follow them.
	afterSend func()
}

// routeID names one of this node's route entries.
type routeID struct{ ns, filter string }

// New prepares the plane and registers its transport handlers. The sink is
// attached with SetSink once the session manager exists.
func New(o Options) *Plane {
	if o.SyncInterval == 0 {
		o.SyncInterval = 30 * time.Second
	}
	if o.MoveGrace == 0 {
		o.MoveGrace = 5 * time.Second
	}
	if o.ServedInterval == 0 {
		o.ServedInterval = 500 * time.Millisecond
	}
	if o.GiveTimeout == 0 {
		o.GiveTimeout = 2 * time.Second
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.Strategy == "" {
		o.Strategy = dispatch.RoundRobin
	}
	p := &Plane{o: o, log: o.Logger.With("component", "routing"), table: NewTable(o.Node), rr: map[string]int{}, sticky: map[string]string{},
		rng: rand.New(rand.NewSource(time.Now().UnixNano())), stop: make(chan struct{}), done: make(chan struct{}), //nolint:gosec // distribution, not security
		out: map[string]*outbound{}, in: map[string]*inbound{}, lastSync: map[string]time.Time{}, moving: map[routeID]time.Time{}, held: map[routeID]*time.Timer{}, ending: map[routeID]*time.Timer{},
		moves: map[routeID]map[string]*clientMove{}, byClient: map[clientKey]map[routeID]bool{},
		pending: map[entryKey]Entry{}, fullTo: map[string]bool{}, wake: make(chan struct{}, 1),
		zeroAt: map[string]uint64{}, settled: map[string]uint64{}, unknownWhy: map[string]string{}, awaitRoutes: map[string]uint64{}, servedWake: make(chan struct{}, 1),
		batches: uint64(time.Now().UnixNano()), proofs: map[string]*routeProof{}}
	p.gives.wake = make(chan struct{}, 1)
	p.ctx, p.cancel = context.WithCancel(context.Background())
	// Until the first evaluation this node has proved nothing about its
	// peers; the safe direction is to refuse.
	p.unknownOwners.Store(true)
	p.unloaded.Store(o.AwaitLoad)
	p.episodes = []Episode{{Start: time.Now()}}
	// Versions start above anything a previous incarnation of this node
	// published, so a restart cannot lose to its own stale tombstones.
	p.version.Store(uint64(time.Now().UnixNano()))
	if o.Transport != nil {
		o.Transport.Handle(transport.RouteUpdate, p.receiveRoutes)
		o.Transport.Handle(transport.ForwardedPublish, p.receiveForward)
		o.Transport.Observe(p.peerChanged)
		o.Transport.ObserveData(p.dataChanged)
	}
	return p
}

// Loaded releases the route sends AwaitLoad held: this node's own entries
// now describe the sessions it holds, so a full set may be sent.
func (p *Plane) Loaded() {
	if p.unloaded.Swap(false) {
		p.queueFull("")
	}
}

// SetSink attaches the delivery sink.
func (p *Plane) SetSink(s Sink) {
	p.mu.Lock()
	p.sink = s
	p.mu.Unlock()
}

// Table exposes the route table for reads.
func (p *Plane) Table() *Table { return p.table }

// Episode is a span during which at least one owner's routes were unknown
// to this node, so it refused every QoS 1 or 2 publish that needed one. End
// is zero while the span lasts.
type Episode struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end,omitzero"`
}

// MaxEpisodes bounds the episodes a plane keeps.
const MaxEpisodes = 16

// OwnerKnowledge returns the last episodes of unknown owners, oldest first,
// and whether one is open now.
func (p *Plane) OwnerKnowledge() (episodes []Episode, open bool) {
	p.knowMu.Lock()
	defer p.knowMu.Unlock()
	episodes = append([]Episode(nil), p.episodes...)
	return episodes, len(episodes) > 0 && episodes[len(episodes)-1].End.IsZero()
}

// OwnersKnown reports whether this node knows every owner's routes. While it
// does not, a QoS 1 or 2 publish that would need one is refused rather than
// acknowledged (WI 1351), so a caller that expects a publish to be taken —
// the simulation harness's idea of a settled cluster — waits for this.
func (p *Plane) OwnersKnown() bool { return !p.unknownOwners.Load() }

// Start runs the periodic full sync, after one evaluation of what this node
// knows: the safe default set in New must not outlive startup.
func (p *Plane) Start() {
	p.refreshOwnerKnowledge()
	p.flushDone = make(chan struct{})
	go p.flushLoop()
	go p.syncLoop()
	p.wg.Add(1)
	go p.servedLoop()
	p.wg.Add(1)
	go p.giveLoop()
}

// Stop ends the sync loop.
func (p *Plane) Stop() {
	select {
	case <-p.stop:
		return
	default:
	}
	close(p.stop)
	p.cancel()
	<-p.done
	if p.flushDone != nil {
		<-p.flushDone
	}
	p.mu.Lock()
	p.stopped = true
	for id, t := range p.held {
		t.Stop()
		delete(p.held, id)
	}
	for id, t := range p.ending {
		t.Stop()
		delete(p.ending, id)
	}
	for _, in := range p.in {
		in.mu.Lock()
		in.closed = true
		in.mu.Unlock()
		in.cond.Broadcast()
	}
	for _, o := range p.out {
		for _, w := range o.waiters {
			close(w)
		}
		o.waiters = nil
	}
	p.mu.Unlock()
	p.wg.Wait()
}

// Observe is the dispatcher's filter observer: a local filter gained its
// first subscriber (or a group changed its member count) or lost its last.
// The removal of a filter whose session is moving to another node is held
// back for the move grace (see Moving).
func (p *Plane) Observe(ns, filter string, count int) {
	id := routeID{ns, filter}
	p.mu.Lock()
	if t := p.held[id]; t != nil {
		t.Stop()
		delete(p.held, id)
	}
	until, moving := p.moving[id]
	inGrace := moving && until.After(time.Now())
	if count == 0 && inGrace {
		p.stopEndingLocked(id)
		p.held[id] = time.AfterFunc(time.Until(until), func() { p.emit(ns, filter, 0) })
		p.mu.Unlock()
		return
	}
	if inGrace && count < p.table.OwnCount(ns, filter) {
		// The moving session's own subscription going, with others still on
		// the filter. The peers are told the new count; the move is not over.
		// Ending it here left a publish answered for by the subscriber that
		// stayed, while the one in transit was imported elsewhere without it.
		if p.ending[id] == nil {
			p.ending[id] = time.AfterFunc(time.Until(until), func() { p.endMove(id, until) })
		}
		p.mu.Unlock()
		p.emitCount(ns, filter, count, true)
		return
	}
	delete(p.moving, id)
	p.clearMovesLocked(id)
	p.stopEndingLocked(id)
	p.mu.Unlock()
	p.emit(ns, filter, count)
}

// stopEndingLocked cancels a filter's end-of-move timer. Callers hold p.mu.
func (p *Plane) stopEndingLocked(id routeID) {
	if t := p.ending[id]; t != nil {
		t.Stop()
		delete(p.ending, id)
	}
}

// endMove ends a move whose filter other sessions kept alive: nothing else
// will, since no removal is being held for it. A second session leaving the
// same filter meanwhile has set a later deadline, and the timer follows it.
func (p *Plane) endMove(id routeID, until time.Time) {
	p.mu.Lock()
	delete(p.ending, id)
	cur, ok := p.moving[id]
	switch {
	case !ok:
		p.mu.Unlock()
		return
	case !cur.Equal(until):
		p.ending[id] = time.AfterFunc(time.Until(cur), func() { p.endMove(id, cur) })
		p.mu.Unlock()
		return
	}
	delete(p.moving, id)
	p.clearMovesLocked(id)
	p.mu.Unlock()
	p.table.SetMoving(id.ns, id.filter, false)
}

// Moving records that these filters belong to a session leaving this node
// for another: their removal is not told to the other nodes until the grace
// elapses. Until then the other nodes keep forwarding here, the message
// finds no session, and the publish is refused — rather than matching
// nothing anywhere and being acknowledged with nowhere to go while the new
// owner's route is still in flight.
func (p *Plane) Moving(ns string, filters []string) { p.recordMove(ns, "", filters) }

// recordMove is Moving, recording the client's move of each filter too when
// the client is named.
func (p *Plane) recordMove(ns, client string, filters []string) {
	until := time.Now().Add(p.o.MoveGrace)
	p.mu.Lock()
	for _, f := range filters {
		id := routeID{ns, f}
		p.moving[id] = until
		if client == "" {
			continue
		}
		if p.moves[id] == nil {
			p.moves[id] = map[string]*clientMove{}
		}
		p.moves[id][client] = &clientMove{until: until}
		k := clientKey{ns, client}
		if p.byClient[k] == nil {
			p.byClient[k] = map[routeID]bool{}
		}
		p.byClient[k][id] = true
	}
	p.mu.Unlock()
	// The other nodes keep forwarding here because the route is still
	// advertised; this node cannot see that route at all (Match skips self,
	// HiddenMatch reports only hidden peers), so it needs its own index to
	// answer for the same window (WI 1353).
	for _, f := range filters {
		p.table.SetMoving(ns, f, true)
	}
}

// emit merges one of this node's route changes and tells the peers. It ends
// any move of the filter.
func (p *Plane) emit(ns, filter string, count int) { p.emitCount(ns, filter, count, false) }

// emitCount is emit, optionally leaving a move of the filter in progress.
func (p *Plane) emitCount(ns, filter string, count int, stillMoving bool) {
	id := routeID{ns, filter}
	p.mu.Lock()
	delete(p.held, id)
	if !stillMoving {
		delete(p.moving, id)
		p.clearMovesLocked(id)
		p.stopEndingLocked(id)
	}
	p.mu.Unlock()
	e := Entry{Namespace: ns, Filter: filter, Owner: p.o.Node, Version: p.version.Add(1), Count: count, Tombstone: count == 0}
	// The local merge stays synchronous. Match, HiddenMatch and MovingMatch
	// are all consulted on this node's publish path, and deferring this
	// would reopen the acknowledged-loss window WI 1353 closed.
	p.table.Merge([]Entry{e})
	// Cleared after the merge, not before: between the two there must be no
	// instant where neither the moving index nor a live route answers for
	// this topic. Clearing first would leave exactly that gap, which is the
	// defect this rule exists to close, one microsecond wide.
	if !stillMoving {
		p.table.SetMoving(ns, filter, false)
	}
	p.count("hoardmq_route_updates_total", map[string]string{"direction": "out"}, 1)
	p.queue(e)
	p.gauge()
}

// queue hands a change to the flusher, coalescing by entry so a filter that
// changed repeatedly while a send was in flight costs one entry, and keeping
// the highest version so a peer is never sent an older view after a newer
// one. It never blocks: that is the whole point.
func (p *Plane) queue(e Entry) {
	k := entryKey{e.Owner, e.Namespace, e.Filter}
	p.mu.Lock()
	if prev, ok := p.pending[k]; !ok || e.Version >= prev.Version {
		p.pending[k] = e
	}
	p.mu.Unlock()
	p.wakeFlusher()
}

// queueFull marks a peer (or, for the empty string, every peer) as owed a
// complete set. It runs on the transport's observer goroutine, which
// notifyAll walks synchronously for every observer, so it must not send.
func (p *Plane) queueFull(peer string) {
	p.mu.Lock()
	p.fullTo[peer] = true
	p.mu.Unlock()
	p.wakeFlusher()
}

func (p *Plane) wakeFlusher() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// flushLoop sends what has accumulated, then takes whatever arrived while it
// was sending. No timer and no configured hold: idle, it sends at once;
// loaded, it batches everything that piled up behind the send in flight.
// The same shape as store.groupSyncer and lease.Service.flushLoop, both of
// which default their hold to zero for the same reason.
func (p *Plane) flushLoop() {
	defer close(p.flushDone)
	for {
		select {
		case <-p.stop:
			p.drainPending()
			return
		case <-p.wake:
		}
		for p.flushOnce() {
		}
	}
}

// flushOnce sends one batch and reports whether more was waiting. The
// plane's mutex is never held across a send.
func (p *Plane) flushOnce() bool {
	if p.unloaded.Load() {
		return false
	}
	p.mu.Lock()
	if len(p.pending) == 0 && len(p.fullTo) == 0 {
		p.flushing = false
		p.mu.Unlock()
		return false
	}
	batch := make([]Entry, 0, len(p.pending))
	for _, e := range p.pending {
		batch = append(batch, e)
	}
	clear(p.pending)
	var number uint64
	if len(batch) > 0 {
		p.batches++
		number = p.batches
	}
	cover := p.batches
	fulls := make([]string, 0, len(p.fullTo))
	for peer := range p.fullTo {
		fulls = append(fulls, peer)
	}
	clear(p.fullTo)
	p.flushing = true
	p.mu.Unlock()

	// A full set supersedes the deltas it contains, so it goes first and the
	// deltas that follow carry higher versions or are ignored by Merge.
	for _, peer := range fulls {
		p.sendFull(peer, cover)
	}
	if len(batch) > 0 {
		sort.Slice(batch, func(i, j int) bool { return batch[i].Version < batch[j].Version })
		batch[0].Batch = number
		p.broadcast(routeDelta, batch, "")
	}
	return true
}

// drainPending makes one best-effort flush at Stop. What it cannot send is
// discarded rather than waited on: the periodic full sync repairs any gap,
// and a stop that blocks on a peer that is not reading is worse than a
// route update that arrives one sync interval late.
func (p *Plane) drainPending() {
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.flushOnce()
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
	}
}

// fullSync is the body of a routeFull message: the sender's complete set of
// own entries, and the version below which its silence is authoritative.
type fullSync struct {
	Snapshot uint64  `json:"snapshot"`
	Entries  []Entry `json:"entries"`
	// Batch is the number of the last delta batch the sender had composed
	// when it composed this set, which holds every entry of every batch up
	// to it (routeproof.go). Absent from an older sender.
	Batch uint64 `json:"batch,omitempty"`
}

// broadcastFull sends this node's complete own-entry set to every peer that
// is up, or to one peer. Unlike a delta it is sent even when the set is
// empty: "I hold nothing" is the message that retires the entries a previous
// incarnation of this node published, and it is the only way to send it.
func (p *Plane) broadcastFull(only string) { p.queueFull(only) }

// sendFull composes and sends the set. It runs on the flusher, never on the
// transport's observer goroutine: notifyAll walks the observers in turn, so
// a send that blocks there delays every other component's notice of the same
// peer event, and at a hundred thousand routes this payload is not small.
func (p *Plane) sendFull(only string, cover uint64) {
	body, _ := json.Marshal(fullSync{Snapshot: p.version.Add(1), Entries: p.table.Own(), Batch: cover})
	p.sendPayload(append([]byte{routeFull}, body...), only)
}

// broadcast sends entries to every peer that is up, or to one peer.
func (p *Plane) broadcast(kind byte, entries []Entry, only string) {
	if len(entries) == 0 {
		return
	}
	body, _ := json.Marshal(entries)
	p.sendPayload(append([]byte{kind}, body...), only)
}

func (p *Plane) sendPayload(payload []byte, only string) {
	if p.o.Transport == nil {
		return
	}
	for _, peer := range p.o.Transport.Peers() {
		if peer.State != transport.Up || (only != "" && peer.Name != only) {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		if err := p.o.Transport.Send(ctx, peer.Name, transport.RouteUpdate, payload); err != nil {
			p.log.Debug("route update not sent", "peer", peer.Name, "err", err)
		}
		cancel()
	}
}

// receiveRoutes merges a peer's entries.
func (p *Plane) receiveRoutes(peer string, payload []byte) {
	if len(payload) < 1 {
		return
	}
	var entries []Entry
	var snapshot, number uint64
	if payload[0] == routeFull {
		var fs fullSync
		if err := json.Unmarshal(payload[1:], &fs); err != nil {
			p.log.Warn("bad route sync", "peer", peer, "err", err)
			return
		}
		entries, snapshot, number = fs.Entries, fs.Snapshot, fs.Batch
	} else if err := json.Unmarshal(payload[1:], &entries); err != nil {
		p.log.Warn("bad route update", "peer", peer, "err", err)
		return
	}
	if payload[0] != routeFull && len(entries) > 0 {
		number, entries[0].Batch = entries[0].Batch, 0
	}
	for i := range entries {
		if entries[i].Owner == "" {
			entries[i].Owner = peer
		}
	}
	if payload[0] == routeFull {
		_, withdrawn := p.table.MergeFull(peer, entries, snapshot)
		p.count("hoardmq_route_syncs_total", map[string]string{"direction": "in"}, 1)
		if n := len(withdrawn); n > 0 {
			p.count("hoardmq_routes_withdrawn_total", map[string]string{"reason": "absent-from-sync"}, float64(n))
			p.log.Info("routes withdrawn by a peer's sync", "peer", peer, "count", n)
		}
		// Recorded after the merge: the proof vouches for the entries.
		p.noteRoutes(peer, true, number)
		p.markSync(peer)
	} else {
		p.table.Merge(entries)
		p.noteRoutes(peer, false, number)
	}
	p.count("hoardmq_route_updates_total", map[string]string{"direction": "in"}, 1)
	p.gauge()
}

// markSync records when a peer's full sync last arrived — syncs only, not
// deltas, so the age means what its name says. Every peer that is up sends
// one every SyncInterval whether or not it holds anything, so silence from a
// peer is meaningful rather than merely the normal state of an idle node.
func (p *Plane) markSync(peer string) {
	p.mu.Lock()
	_, had := p.lastSync[peer]
	p.lastSync[peer] = time.Now()
	p.mu.Unlock()
	if !had {
		// The first sync from this peer is what makes its routes known.
		p.refreshOwnerKnowledge()
	}
}

// syncedFrom reports whether a full route sync from an owner has arrived
// since this node started. The plane is constructed fresh at start and
// lastSync is written only by markSync, so the map means exactly that.
func (p *Plane) syncedFrom(owner string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.lastSync[owner]
	return ok
}

// candidates are the owners whose routes this node needs in order to answer
// for a publish: every configured peer and every cluster member, less this
// node. The union is deliberate — a peer membership has dropped is cleared
// by its lease count instead of by a special case, and a member this node
// has no configured link to still owns sessions that matter.
func (p *Plane) candidates() []string {
	seen := map[string]bool{p.o.Node: true}
	out := make([]string, 0, 4)
	add := func(name string) {
		if name == "" || seen[name] {
			return
		}
		seen[name] = true
		out = append(out, name)
	}
	if p.o.Transport != nil {
		for _, peer := range p.o.Transport.Peers() {
			add(peer.Name)
		}
	}
	if p.o.Members != nil {
		for _, name := range p.o.Members() {
			add(name)
		}
	}
	return out
}

// refreshOwnerKnowledge recomputes whether any owner's route set is unknown
// to this node: no full sync has arrived from it since this node started and
// ownerRoutesKnown cannot show its routes do not matter. That is the state a
// node is in after restarting beside a peer it cannot reach, where the route
// table holds nothing for that peer and HiddenMatch has nothing to match
// against.
func (p *Plane) refreshOwnerKnowledge() {
	p.refreshMu.Lock()
	defer p.refreshMu.Unlock()
	unknown := 0
	unserved := map[string]int{}
	var self *servedSelf
	for _, owner := range p.candidates() {
		if p.syncedFrom(owner) {
			delete(p.unknownWhy, owner)
			continue
		}
		known, why, emptied := p.judgeUnsynced(owner, &self)
		if known {
			delete(p.unknownWhy, owner)
			continue
		}
		unknown++
		if emptied != 0 {
			unserved[why]++
		}
		if p.unknownWhy[owner] != why {
			p.unknownWhy[owner] = why
			p.log.Debug("an owner's routes are unknown here", "owner", owner, "reason", why, "emptiedAt", emptied)
		}
	}
	if p.afterJudge != nil {
		p.afterJudge()
	}
	now := time.Now()
	p.knowMu.Lock()
	was := p.unknownOwners.Swap(unknown > 0)
	switch {
	case !was && unknown > 0:
		p.episodes = append(p.episodes, Episode{Start: now})
		if len(p.episodes) > MaxEpisodes {
			p.episodes = append(p.episodes[:0:0], p.episodes[len(p.episodes)-MaxEpisodes:]...)
		}
	case was && unknown == 0 && len(p.episodes) > 0:
		p.episodes[len(p.episodes)-1].End = now
	}
	p.knowMu.Unlock()
	if p.o.Metrics != nil {
		p.o.Metrics.Set("hoardmq_route_owners_unknown", nil, float64(unknown))
		for _, why := range []string{keepUnsynced, keepAdopting, keepUnreachable} {
			p.o.Metrics.Set("hoardmq_route_owners_unserved", map[string]string{"reason": why}, float64(unserved[why]))
		}
	}
	if first := !p.knowledgeLogged.Swap(true); first || was != (unknown > 0) {
		if unknown > 0 {
			p.log.Warn("routes unknown for at least one owner; QoS 1 and 2 publishes are refused until they are known", "owners", unknown)
		} else {
			p.log.Info("every owner's routes are known; QoS 1 and 2 publishes are acknowledgeable again")
		}
	}
}

// judgeUnsynced is ownerRoutesKnown for an owner this node has had no full
// sync from, with its inputs read: the members only once nothing nearer
// decides, since each costs a count over the lease table. emptied is the
// index the owner was emptied at when it holds nothing, zero otherwise. self
// is this node's own adoption state, read once per evaluation.
func (p *Plane) judgeUnsynced(owner string, self **servedSelf) (known bool, why string, emptied uint64) {
	if p.o.OwnedSessions == nil {
		return false, keepUnknown, 0
	}
	owns, readable := p.o.OwnedSessions(owner)
	if readable && owns == 0 && p.o.EmptiedAt != nil {
		emptied = p.o.EmptiedAt(owner)
	}
	if *self == nil {
		index, adopting := p.adoptionState()
		*self = &servedSelf{index: index, adopting: adopting}
	}
	p.mu.Lock()
	settled := p.settled[owner]
	p.mu.Unlock()
	known, why = ownerRoutesKnown(owns, readable, emptied, settled, **self, nil)
	if !known || emptied == 0 || settled >= emptied {
		return known, why, emptied
	}
	known, why = ownerRoutesKnown(owns, readable, emptied, settled, **self, p.servedMembers(owner, emptied))
	if known {
		p.mu.Lock()
		p.settled[owner] = max(p.settled[owner], emptied)
		p.mu.Unlock()
	}
	return known, why, emptied
}

// SyncAges reports, per peer, how long it has been since a route sync
// arrived. A peer that is up and absent from this map has never sent one.
func (p *Plane) SyncAges() map[string]time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[string]time.Duration, len(p.lastSync))
	for peer, at := range p.lastSync {
		out[peer] = time.Since(at)
	}
	return out
}

// peerChanged hides a down peer's entries and re-synchronises with a peer
// that came up.
// peerChanged hides a down peer's entries and re-synchronises with a peer
// that came up; the peer's state is its control connection's (WI 1340).
func (p *Plane) peerChanged(peer string, s transport.State) {
	defer p.refreshOwnerKnowledge()
	switch s {
	case transport.Up:
		p.table.Hide(peer, false)
		p.broadcastFull(peer)
		p.count("hoardmq_route_syncs_total", map[string]string{"direction": "out"}, 1)
	case transport.Down:
		p.table.Hide(peer, true)
		p.failInFlight(peer)
	}
}

// dataChanged: forwards and route updates ride the data connection, so its
// reset loses what was in flight (the barriers covering it fail) and its
// return re-synchronises the routes; the peer's entries stay visible, since
// the peer is not down (WI 1340).
func (p *Plane) dataChanged(peer string, s transport.State) {
	switch s {
	case transport.Up:
		p.broadcastFull(peer)
		p.count("hoardmq_route_syncs_total", map[string]string{"direction": "out"}, 1)
	case transport.Down:
		p.failInFlight(peer)
	}
}

// failInFlight fails every barrier over frames admitted to a link that
// closed: they are gone, and a later acknowledgement must not cover them. A
// forward awaiting its give's outcome fails too, since the outcome would have
// come over that link.
func (p *Plane) failInFlight(peer string) {
	p.mu.Lock()
	if o := p.out[peer]; o != nil && (o.sent > o.acked || len(o.pending) > 0) {
		if o.sent > o.acked {
			o.lastFailed = o.sent
			o.acked = o.sent
		}
		for _, s := range o.livePending() {
			o.refuseGive(s)
		}
		o.pendingOrder = nil
		waiters := o.waiters
		o.waiters = nil
		p.mu.Unlock()
		for _, w := range waiters {
			close(w)
		}
		p.wakeGives()
		p.count("hoardmq_forward_failures_total", map[string]string{"reason": "link-reset"}, 1)
	} else {
		p.mu.Unlock()
	}
}

// syncLoop sends the full own-entry set to every peer periodically and
// forgets owners the membership no longer lists.
func (p *Plane) syncLoop() {
	defer close(p.done)
	ticker := time.NewTicker(p.o.SyncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-p.stop:
			return
		case <-ticker.C:
		}
		p.broadcastFull("")
		p.count("hoardmq_route_syncs_total", map[string]string{"direction": "out"}, 1)
		if p.o.Members != nil {
			p.forgetRemoved(p.o.Members())
		}
		// Membership and lease ownership change without an event this plane
		// sees, so the tick is what picks those up.
		p.refreshOwnerKnowledge()
		p.sweepMoving()
	}
}

// sweepMoving clears hand-overs whose grace has long passed. Every ordinary
// path ends a move through emit, which clears both the deadline map and the
// index; this covers the one way that can fail — a filter marked moving
// whose removal never reaches Observe, because a session's recorded
// subscriptions and the dispatcher's disagreed. Without it such an entry
// would refuse its topic on this node for as long as the process lives,
// which is a worse failure than the one the rule prevents.
func (p *Plane) sweepMoving() {
	cutoff := time.Now().Add(-p.o.MoveGrace)
	var stale []routeID
	p.mu.Lock()
	for id, until := range p.moving {
		if until.Before(cutoff) && p.held[id] == nil {
			stale = append(stale, id)
			delete(p.moving, id)
			p.clearMovesLocked(id)
		}
	}
	p.mu.Unlock()
	for _, id := range stale {
		p.table.SetMoving(id.ns, id.filter, false)
		p.log.Warn("a hand-over was marked moving and never resolved; clearing", "filter", id.filter)
	}
}

// forgetRemoved drops the entries of owners that are neither members nor
// this node. Peers that are configured but not yet members are kept: the
// membership is authoritative only once it lists someone.
func (p *Plane) forgetRemoved(members []string) {
	if len(members) == 0 {
		return
	}
	keep := map[string]bool{p.o.Node: true}
	for _, m := range members {
		keep[m] = true
	}
	for _, peer := range p.o.Transport.Peers() {
		if !keep[peer.Name] && peer.State != transport.Up {
			p.table.Forget(peer.Name)
			// Forget the sync record too, so "synced" keeps meaning
			// "synced since this node started" if the owner returns.
			p.mu.Lock()
			delete(p.lastSync, peer.Name)
			p.mu.Unlock()
		}
	}
}

// Forward is the session manager's forwarder: it sends a message published
// on this node to every remote node with a matching direct subscription,
// and handles shared groups this node has no local member of (the groups
// with local members are decided in Decide).
func (p *Plane) Forward(msg *message.Message) (int, error) {
	if p.o.Transport == nil || msg.Forwarded {
		return 0, nil
	}
	// Read before the match: a forward sent before the confirmation was
	// acted on may have reached the new owner before it held the session.
	var moves []moveView
	if msg.QoS > 0 && p.table.MovingMatch(msg.Namespace, msg.Topic) {
		moves = p.moveViews(msg.Namespace, msg.Topic)
	}
	nodes, groups := p.table.Match(msg.Namespace, msg.Topic)
	sent := 0
	for _, n := range nodes {
		p.send(n, fwdPublish, "", msg)
		sent++
	}
	for _, g := range groups {
		if g.Members[p.o.Node] > 0 {
			continue // decided through the local dispatcher
		}
		if !p.dispatchGroup(g, msg) {
			sent++
		}
	}
	if p.afterSend != nil {
		p.afterSend()
	}
	if msg.QoS > 0 && p.table.HiddenMatch(msg.Namespace, msg.Topic) {
		p.count("hoardmq_forward_failures_total", map[string]string{"reason": "subscriber-node-down"}, 1)
		p.noteRefusal("local", "subscriber-node-down", "", msg.Namespace, msg.Topic, 0, 0)
		return sent, ErrRoutesUnavailable
	}
	// An owner whose routes this node has never received could hold a
	// subscriber for any topic, so there is no match to test: the publish
	// cannot be acknowledged as delivered (WI 1351). The forwards above
	// still went to the owners this node does know about.
	if msg.QoS > 0 && p.unknownOwners.Load() {
		p.count("hoardmq_forward_failures_total", map[string]string{"reason": "routes-unknown"}, 1)
		return sent, ErrRoutesUnavailable
	}
	// A session that has left this node for another takes its subscriptions
	// with it, but its route stays advertised for the move grace so the
	// other nodes keep forwarding here and are refused. Until now this node
	// answered its own publishes as though the route did not exist — the one
	// node still advertising it was the one node blind to it (WI 1353).
	if msg.QoS > 0 && p.table.MovingMatch(msg.Namespace, msg.Topic) {
		if p.proven(msg.Namespace, moves, publishPath{targets: nodes}) {
			p.count("hoardmq_move_refusals_lifted_total", map[string]string{"path": "local"}, 1)
			return sent, nil
		}
		p.count("hoardmq_forward_failures_total", map[string]string{"reason": "session-moving"}, 1)
		p.noteRefusal("local", "session-moving", "", msg.Namespace, msg.Topic, 0, sent)
		return sent, ErrRoutesUnavailable
	}
	return sent, nil
}

// Barrier returns once the receiver of each of the publish's forwards has
// acknowledged it; a forward that failed, a peer that went down with one
// outstanding, or the context fails it. A publish is judged on the frames it
// sent and no others: the failure mark is a high-water sequence that nothing
// lowers, so judging a publish on the peer's whole stream refuses every later
// publish until some forward moves the stream past the mark.
//
// The failure is tested before the acknowledgement because a failed send and
// a closed link both advance acked along with the mark.
func (p *Plane) Barrier(ctx context.Context, forwards []message.ForwardRef) error {
	if p.o.Transport == nil || len(forwards) == 0 {
		return nil
	}
	select {
	case <-p.stop:
		return fmt.Errorf("routing: plane stopped: %w", ErrRoutesUnavailable)
	default:
	}
	for _, f := range forwards {
		for {
			p.mu.Lock()
			o := p.out[f.Peer]
			verdict := forwardFailed
			giveRefused := false
			if o != nil {
				_, refused := o.refused[f.Seq]
				_, pending := o.pending[f.Seq]
				_, giveRefused = o.giveRefused[f.Seq]
				verdict = forwardVerdict(f.Seq, o.lastFailed, o.acked, refused, pending)
			}
			if verdict == forwardFailed || verdict == forwardRefused {
				reason := "send-failed"
				if verdict == forwardRefused {
					reason = "receiver-refused"
					if giveRefused {
						reason = "give-refused"
					}
				}
				s := RefusalSample{Where: "barrier", Reason: reason, Peer: f.Peer, Seq: f.Seq}
				if o != nil {
					s.LastFailed, s.Acked = o.lastFailed, o.acked
				}
				p.mu.Unlock()
				p.refusals.add(s)
				p.count("hoardmq_forward_failures_total", map[string]string{"reason": reason}, 1)
				if giveRefused {
					return fmt.Errorf("routing: %s gave the forward to a group member that did not hold it: %w", f.Peer, ErrRoutesUnavailable)
				}
				if verdict == forwardRefused {
					return fmt.Errorf("routing: the receiver refused the forward to %s: %w", f.Peer, ErrRoutesUnavailable)
				}
				return fmt.Errorf("routing: a forward to %s was not delivered: %w", f.Peer, ErrRoutesUnavailable)
			}
			if verdict == forwardPass {
				p.mu.Unlock()
				break
			}
			w := make(chan struct{})
			o.waiters = append(o.waiters, w)
			p.mu.Unlock()
			if !p.up(f.Peer) {
				p.count("hoardmq_forward_failures_total", map[string]string{"reason": "peer-down-before-ack"}, 1)
				return fmt.Errorf("routing: forwards to %s not acknowledged: %w", f.Peer, ErrRoutesUnavailable)
			}
			select {
			case <-w:
			case <-ctx.Done():
				p.count("hoardmq_forward_failures_total", map[string]string{"reason": "ack-timeout"}, 1)
				return fmt.Errorf("routing: forwards to %s not acknowledged: %w", f.Peer, ctx.Err())
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	return nil
}

func (p *Plane) up(name string) bool {
	for _, pi := range p.o.Transport.Peers() {
		if pi.Name == name {
			return pi.State == transport.Up
		}
	}
	return false
}

func (p *Plane) acked(peer string, seq uint64, failed, one bool, given []uint64) {
	p.mu.Lock()
	o := p.out[peer]
	if o == nil {
		p.mu.Unlock()
		return
	}
	if failed && one {
		// The receiver refused this frame alone. The frames before it are
		// neither failed nor acknowledged by this: the receiver's next ack,
		// after its durability wait, covers them.
		p.refusals.add(RefusalSample{Where: "sender", Reason: "receiver-refused", Peer: peer, Seq: seq, LastFailed: o.lastFailed, Acked: o.acked})
		o.refuseOne(seq)
		waiters := o.waiters
		o.waiters = nil
		p.mu.Unlock()
		for _, w := range waiters {
			close(w)
		}
		p.wakeGives()
		return
	}
	if failed && seq > o.lastFailed {
		p.refusals.add(RefusalSample{Where: "sender", Reason: "receiver-not-durable", Peer: peer, Seq: seq, LastFailed: o.lastFailed, Acked: o.acked})
		o.lastFailed = seq
		p.count("hoardmq_forward_failures_total", map[string]string{"reason": "receiver-not-durable"}, 1)
	}
	if !failed {
		// Held before the acknowledged mark moves, in the same section, so
		// no barrier sees a given forward acknowledged and not pending.
		for _, s := range given {
			if s > o.lastFailed && s <= seq {
				o.hold(s)
			}
		}
	}
	if seq > o.acked {
		o.acked = seq
	}
	waiters := o.waiters
	o.waiters = nil
	p.mu.Unlock()
	for _, w := range waiters {
		close(w)
	}
	p.wakeGives()
}

// giveOutcome settles a forward the peer gave on: held clears its pending
// mark, and does nothing to a forward not pending; failed refuses it whether
// or not it is pending, since a refusal is the safe direction.
func (p *Plane) giveOutcome(peer string, seq uint64, failed bool) {
	p.mu.Lock()
	o := p.out[peer]
	if o == nil {
		p.mu.Unlock()
		return
	}
	if failed {
		o.refuseGive(seq)
		p.refusals.add(RefusalSample{Where: "sender", Reason: "give-refused", Peer: peer, Seq: seq, LastFailed: o.lastFailed, Acked: o.acked})
	} else {
		delete(o.pending, seq)
	}
	waiters := o.waiters
	o.waiters = nil
	p.mu.Unlock()
	for _, w := range waiters {
		close(w)
	}
}

// The dispatcher names a group "{name}/{filter}"; the table keeps the full
// filter "$share/{name}/{filter}". These convert between the two.
func tableKey(dispatcherKey string) string { return topic.SharePrefix + dispatcherKey }
func dispatcherKey(tableKey string) string {
	return strings.TrimPrefix(tableKey, topic.SharePrefix)
}

// Decide is the dispatcher's group decider for a message published on this
// node that matched a shared group with local members: deliver locally only
// when this node owns the group's dispatch and its strategy chose itself;
// otherwise forward for the decision or to the chosen node. groupKey is in
// the dispatcher's form.
func (p *Plane) Decide(ns, groupKey string, msg *message.Message) bool {
	if msg.Forwarded || p.o.Transport == nil {
		return !msg.Forwarded
	}
	want := tableKey(groupKey)
	_, groups := p.table.Match(ns, msg.Topic)
	for _, g := range groups {
		if g.Key != want {
			continue
		}
		if len(g.Members) == 1 && g.Members[p.o.Node] > 0 {
			return true // only local members
		}
		return p.dispatchGroup(g, msg)
	}
	return true // the group is not known remotely: local only
}

// dispatchGroup routes one shared-group delivery: a non-owner forwards to
// the owner; the owner chooses a member node by strategy. It returns true
// when this node itself must deliver locally.
func (p *Plane) dispatchGroup(g Group, msg *message.Message) bool {
	owner := g.Owner()
	if owner != p.o.Node {
		p.send(owner, fwdGroupDecide, g.Key, msg)
		return false
	}
	node := p.chooseNode(g, msg)
	if node == p.o.Node {
		return true
	}
	p.send(node, fwdGroupGive, g.Key, msg)
	return false
}

// chooseNode applies the strategy over the group's member nodes.
func (p *Plane) chooseNode(g Group, msg *message.Message) string {
	nodes := g.Nodes()
	if len(nodes) == 1 {
		return nodes[0]
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	switch p.o.Strategy {
	case dispatch.Random:
		return nodes[p.rng.Intn(len(nodes))]
	case dispatch.Sticky:
		k := g.Key + "|" + msg.Publisher.Namespace + "/" + msg.Publisher.ClientID
		if n, ok := p.sticky[k]; ok && g.Members[n] > 0 {
			return n
		}
		p.sticky[k] = nodes[0]
		return nodes[0]
	case dispatch.Hash:
		h := fnv.New32a()
		_, _ = h.Write([]byte(msg.Publisher.ClientID))
		return nodes[int(h.Sum32()%uint32(len(nodes)))]
	case dispatch.LocalFirst:
		if g.Members[p.o.Node] > 0 {
			return p.o.Node
		}
		fallthrough
	default:
		i := p.rr[g.Key]
		p.rr[g.Key] = i + 1
		return nodes[i%len(nodes)]
	}
}

// encodedForward is a forward's group and message, encoded ahead of the
// sequence so that the send lock covers only the envelope around them.
type encodedForward struct {
	kind  byte
	group []byte // JSON string, or nil when there is no group
	trace []byte // a traceparent, or nil when the publish is not sampled
	msg   []byte // JSON object
}

func encodeForward(kind byte, group string, msg *message.Message) encodedForward {
	e := encodedForward{kind: kind}
	if group != "" {
		e.group, _ = json.Marshal(group)
	}
	e.msg, _ = json.Marshal(msg)
	return e
}

// frame is the kind byte followed by the bytes json.Marshal gives for
// forwardEnvelope{Seq: seq, Group: group, Msg: msg}; the envelope's field
// order is the wire order. seq is never zero, so it is always present.
func (e encodedForward) frame(seq uint64) []byte {
	b := make([]byte, 0, len(e.msg)+len(e.group)+40)
	b = append(b, e.kind)
	b = append(b, `{"seq":`...)
	b = strconv.AppendUint(b, seq, 10)
	if e.group != nil {
		b = append(b, `,"group":`...)
		b = append(b, e.group...)
	}
	if e.trace != nil {
		// Hex digits and dashes only: nothing to escape.
		b = append(b, `,"trace":"`...)
		b = append(b, e.trace...)
		b = append(b, '"')
	}
	b = append(b, `,"msg":`...)
	b = append(b, e.msg...)
	return append(b, '}')
}

// send forwards one frame to a peer with the next sequence for that peer;
// the receiver acknowledges it once durable. A send that fails is
// recorded so the barrier reports it instead of waiting. It returns the
// sequence taken and whether the frame was admitted.
func (p *Plane) send(peer string, kind byte, group string, msg *message.Message) (uint64, bool) {
	enc := encodeForward(kind, group, msg)
	sp := p.o.Tracer.StartChild(msg.Trace, "forward", otel.KindProducer)
	if sp.Recording() {
		enc.trace = sp.Context().AppendTraceparent(nil)
		defer sp.End()
	}
	// Created before the send lock: a caller queued behind a full link
	// spends its own second waiting, rather than each taking a fresh one.
	ctx, cancel := context.WithTimeout(p.ctx, time.Second)
	defer cancel()
	p.mu.Lock()
	o := p.out[peer]
	if o == nil {
		o = &outbound{}
		p.out[peer] = o
	}
	p.mu.Unlock()
	o.sendMu.Lock()
	p.mu.Lock()
	o.sent++
	seq := o.sent
	p.mu.Unlock()
	if msg.QoS > 0 && !msg.Forwarded {
		// Named even when the send below fails: the failure is recorded
		// against this sequence, and it is this publish's barrier that must
		// see it.
		msg.Forwards = append(msg.Forwards, message.ForwardRef{Peer: peer, Seq: seq})
	}
	err := p.o.Transport.Send(ctx, peer, transport.ForwardedPublish, enc.frame(seq))
	o.sendMu.Unlock()
	kindLabel := map[byte]string{fwdPublish: "publish", fwdGroupDecide: "group-decide", fwdGroupGive: "group-give"}[kind]
	if sp.Recording() {
		sp.String("hoardmq.peer", peer)
		sp.String("hoardmq.forward.kind", kindLabel)
		sp.Int("hoardmq.forward.seq", int64(seq))
		if err != nil {
			sp.Error(err.Error())
		}
	}
	if err != nil {
		p.count("hoardmq_forward_failures_total", map[string]string{"reason": "send"}, 1)
		p.log.Debug("forward not sent", "peer", peer, "kind", kindLabel, "err", err)
		p.mu.Lock()
		if seq > o.lastFailed {
			o.lastFailed = seq
		}
		if seq > o.acked {
			o.acked = seq
		}
		waiters := o.waiters
		o.waiters = nil
		p.mu.Unlock()
		for _, w := range waiters {
			close(w)
		}
		p.wakeGives()
		return seq, false
	}
	p.count("hoardmq_forwards_total", map[string]string{"kind": kindLabel}, 1)
	return seq, true
}

// sendAck tells a peer every forwarded frame up to seq is durable here, or
// with failed that they are not (the sender's barrier fails for them).
func (p *Plane) sendAck(peer string, seq uint64, failed bool) {
	p.sendControl(peer, fwdAck, forwardEnvelope{Seq: seq, Failed: failed})
}

// refuseFrame tells a peer the frame at seq alone was refused here.
func (p *Plane) refuseFrame(peer string, seq uint64) {
	p.sendControl(peer, fwdAck, forwardEnvelope{Seq: seq, Failed: true, One: true})
}

// sendControl sends a frame that takes no forward sequence: an ack, or a
// give's outcome.
func (p *Plane) sendControl(peer string, kind byte, env forwardEnvelope) {
	body, _ := json.Marshal(env)
	ctx, cancel := context.WithTimeout(p.ctx, time.Second)
	defer cancel()
	if err := p.o.Transport.Send(ctx, peer, transport.ForwardedPublish, append([]byte{kind}, body...)); err != nil {
		p.log.Debug("forward control frame not sent", "peer", peer, "kind", kind, "err", err)
	}
}

// receiveForward delivers a forwarded message: to direct subscribers, to
// the group's owner decision, or to one local group member.
func (p *Plane) receiveForward(peer string, payload []byte) {
	if len(payload) < 1 {
		return
	}
	var env forwardEnvelope
	if err := json.Unmarshal(payload[1:], &env); err != nil {
		p.log.Warn("bad forwarded frame", "peer", peer, "err", err)
		return
	}
	switch payload[0] {
	case fwdAck:
		p.acked(peer, env.Seq, env.Failed, env.One, env.Given)
		return
	case fwdGiven:
		p.giveOutcome(peer, env.Seq, env.Failed)
		return
	}
	if env.Msg == nil {
		return
	}
	p.mu.Lock()
	in := p.in[peer]
	if in == nil {
		if p.stopped {
			// Dropped unacknowledged, as a closed inbound drops it.
			p.mu.Unlock()
			return
		}
		in = &inbound{seq: newInboundSeq()}
		in.cond = sync.NewCond(&in.mu)
		p.in[peer] = in
		p.wg.Add(1)
		go p.applyLoop(peer, in)
	}
	p.mu.Unlock()
	in.mu.Lock()
	if !in.closed {
		in.queue = append(in.queue, forwardFrame{kind: payload[0], env: env})
	}
	in.mu.Unlock()
	in.cond.Signal()
}

// applyLoop applies one peer's forwarded frames in order and acknowledges
// each batch once the sink reports it durable.
func (p *Plane) applyLoop(peer string, in *inbound) {
	defer p.wg.Done()
	for {
		in.mu.Lock()
		for len(in.queue) == 0 && !in.closed {
			in.cond.Wait()
		}
		if in.closed {
			in.mu.Unlock()
			return
		}
		batch := in.queue
		in.queue = nil
		in.mu.Unlock()
		if p.o.ApplyGate != nil {
			p.o.ApplyGate(p.o.Node, peer)
		}
		// The sessions this batch delivered into, named to the durability
		// wait that acknowledges it to the forwarding node.
		var covers []store.Scope
		var highest uint64
		// The decides this batch gave to another node: named on its
		// acknowledgement, then watched for their outcome.
		var given []uint64
		var gives []watchedGive
		for _, f := range batch {
			// Acknowledgements are cumulative, so a gap in the sequence must
			// be reported before this frame's own acknowledgement covers it:
			// the frames that never arrived reached no session.
			switch v, from, to := in.seq.observe(f.env.Seq, p.o.IgnoreForwardGaps); v {
			case seqGap:
				p.count("hoardmq_forward_failures_total", map[string]string{"reason": "sequence-gap"}, 1)
				p.refusals.add(RefusalSample{Where: "receiver", Reason: "sequence-gap", Peer: peer, Seq: to})
				p.log.Warn("forwarded frames missing; reported to the sender", "peer", peer, "expected", from, "got", to+1)
				p.sendAck(peer, to, true)
			case seqLate:
				// The hole it was reported for never existed. Counted apart
				// from the gap so a run can tell a phantom hole from a lost
				// frame (WI 1708).
				p.count("hoardmq_forward_late_frames_total", nil, 1)
			case seqRestart:
				p.count("hoardmq_forward_stream_restarts_total", nil, 1)
				p.log.Info("forward sequence restarted", "peer", peer, "got", f.env.Seq)
			}
			sp := p.o.Tracer.StartRemote(f.env.Trace, "deliver", otel.KindConsumer)
			if sp.Recording() && f.env.Msg != nil {
				f.env.Msg.Trace = sp.Context()
			}
			ok, into, give, err := p.apply(peer, f.kind, f.env)
			covers = append(covers, into...)
			if give.peer != "" {
				given = append(given, f.env.Seq)
				gives = append(gives, watchedGive{decideSeq: f.env.Seq, give: give})
			}
			refusal := ""
			if m := f.env.Msg; m != nil && m.QoS > 0 {
				moving := f.kind == fwdPublish && p.table.MovingMatch(m.Namespace, m.Topic)
				switch {
				case errors.Is(err, ErrNotServing):
					p.count("hoardmq_forward_failures_total", map[string]string{"reason": "not-serving"}, 1)
					p.noteRefusal("receiver", "not-serving", peer, m.Namespace, m.Topic, f.env.Seq, 0)
					p.refuseFrame(peer, f.env.Seq)
					refusal = "not-serving"
				case err != nil:
					// A record the delivery wrote is not in the journal (a
					// queued copy was withdrawn): the publish is refused for
					// it, whatever else the frame reached.
					p.count("hoardmq_forward_failures_total", map[string]string{"reason": "not-journaled"}, 1)
					p.noteRefusal("receiver", "not-journaled", peer, m.Namespace, m.Topic, f.env.Seq, len(into))
					p.refuseFrame(peer, f.env.Seq)
					refusal = "not-journaled"
				case moving && p.proven(m.Namespace, p.moveViews(m.Namespace, m.Topic), publishPath{forwarded: true, peer: peer, seq: m.Seq}):
					// The new owner routed it after it held the moved session,
					// so it reached that session there, whether or not it
					// reached anyone here.
					p.count("hoardmq_move_refusals_lifted_total", map[string]string{"path": "forward"}, 1)
				case !ok:
					// The route the sender used is stale here: the message
					// reached no session, so its publish must not be
					// acknowledged as delivered.
					p.count("hoardmq_forward_failures_total", map[string]string{"reason": "no-local-subscriber"}, 1)
					p.noteRefusal("receiver", "no-local-subscriber", peer, m.Namespace, m.Topic, f.env.Seq, 0)
					p.refuseFrame(peer, f.env.Seq)
					refusal = "no-local-subscriber"
				case moving:
					// It reached whoever is still here. A session on the same
					// filter is moving to another node and was exported without
					// it, so the sender is answered as Forward answers a
					// publish made here. A group delivery is not checked: the
					// member that took it is all a group message needs.
					p.count("hoardmq_forward_failures_total", map[string]string{"reason": "session-moving"}, 1)
					p.noteRefusal("receiver", "session-moving", peer, m.Namespace, m.Topic, f.env.Seq, len(into))
					p.refuseFrame(peer, f.env.Seq)
					refusal = "session-moving"
				}
			}
			if sp.Recording() {
				p.endDeliverSpan(sp, peer, f, len(into), refusal)
			}
			if f.env.Seq > highest {
				highest = f.env.Seq
			}
		}
		p.mu.Lock()
		sink := p.sink
		p.mu.Unlock()
		// An empty covers means the batch reached no local session, so there
		// is nothing to make durable — apply has already reported any QoS 1/2
		// frame that landed nowhere. It is not an unnamed wait: nothing was
		// written, rather than written and left unnamed.
		if sink != nil && len(covers) > 0 {
			ctx, cancel := context.WithTimeout(p.ctx, 5*time.Second)
			err := sink.Durable(ctx, covers)
			cancel()
			if err != nil {
				p.log.Warn("forwarded frames not durable; reported to the sender", "peer", peer, "err", err)
				p.refusals.add(RefusalSample{Where: "receiver", Reason: "not-durable", Peer: peer, Seq: highest})
				if highest > 0 {
					p.sendAck(peer, highest, true)
				}
				continue
			}
		}
		if highest > 0 {
			p.sendControl(peer, fwdAck, forwardEnvelope{Seq: highest, Given: given})
		}
		// Only after the acknowledgement naming them, so the sender holds
		// each forward pending before its outcome can arrive.
		p.watchGives(peer, gives)
	}
}

// apply delivers one forwarded frame and reports whether it reached a session
// (an expired message counts as handled), with the sessions it delivered into:
// the wait that acknowledges this batch to the forwarding node must name them,
// or it would acknowledge records a peer may have refused. give names a QoS
// 1/2 decide this node gave to another node, whose outcome the sender waits
// for. An error is the sink's: a record the delivery wrote was not journaled.
func (p *Plane) apply(peer string, kind byte, env forwardEnvelope) (ok bool, into []store.Scope, give giveRef, err error) {
	msg := env.Msg
	msg.Forwarded = true
	if msg.Expired(time.Now()) {
		return true, nil, giveRef{}, nil
	}
	p.mu.Lock()
	sink := p.sink
	p.mu.Unlock()
	if sink == nil {
		return false, nil, giveRef{}, nil
	}
	if msg.QoS > 0 && p.o.Serving != nil && !p.o.Serving() {
		return false, nil, giveRef{}, ErrNotServing
	}
	_ = peer
	payload := []byte{kind}
	local := dispatcherKey(env.Group)
	switch payload[0] {
	case fwdPublish:
		n, into, err := sink.InjectForwarded(msg)
		return n > 0, into, giveRef{}, err
	case fwdGroupDecide:
		_, groups := p.table.Match(msg.Namespace, msg.Topic)
		for _, g := range groups {
			if g.Key != env.Group {
				continue
			}
			if g.Owner() != p.o.Node {
				// The tables disagree during churn; deliver locally rather
				// than bounce the message between nodes.
				ok, into, err := groupInto(sink.InjectGroup(msg, local))
				return ok, into, giveRef{}, err
			}
			node := p.chooseNode(g, msg)
			if node == p.o.Node {
				ok, into, err := groupInto(sink.InjectGroup(msg, local))
				return ok, into, giveRef{}, err
			}
			seq, _ := p.send(node, fwdGroupGive, env.Group, msg)
			if msg.QoS > 0 {
				give = giveRef{peer: node, seq: seq}
			}
			return true, nil, give, nil
		}
		ok, into, err := groupInto(sink.InjectGroup(msg, local))
		return ok, into, giveRef{}, err
	case fwdGroupGive:
		ok, into, err := groupInto(sink.InjectGroup(msg, local))
		return ok, into, giveRef{}, err
	}
	return true, nil, giveRef{}, nil
}

// endDeliverSpan names what a received forward reached and ends its span.
func (p *Plane) endDeliverSpan(sp otel.Span, peer string, f forwardFrame, delivered int, refusal string) {
	sp.String("hoardmq.peer", peer)
	sp.Int("hoardmq.forward.seq", int64(f.env.Seq))
	sp.String("hoardmq.node", p.o.Node)
	if m := f.env.Msg; m != nil {
		sp.String("messaging.destination.name", m.Topic)
		sp.String("hoardmq.namespace", m.Namespace)
	}
	sp.Int("hoardmq.delivered", int64(delivered))
	if refusal != "" {
		sp.String("hoardmq.refusal", refusal)
		sp.Error(refusal)
	}
	sp.End()
}

// groupInto adapts a shared-group delivery to apply's shape.
func groupInto(s store.Scope, ok bool, err error) (bool, []store.Scope, error) {
	if !ok {
		return false, nil, err
	}
	return true, []store.Scope{s}, err
}

func (p *Plane) count(name string, labels map[string]string, delta float64) {
	if p.o.Metrics != nil {
		p.o.Metrics.Add(name, labels, delta)
	}
}

func (p *Plane) gauge() {
	if p.o.Metrics == nil {
		return
	}
	local, remote := p.table.Count()
	p.o.Metrics.Set("hoardmq_routes_active", map[string]string{"scope": "local"}, float64(local))
	p.o.Metrics.Set("hoardmq_routes_active", map[string]string{"scope": "remote"}, float64(remote))
}
