// Package etcdraft is the shell around go.etcd.io/raft/v3: the node loop
// with persist-before-send enforced by the store's durability token, the
// transport adapter, the join protocol, snapshots in the project's format
// and membership with learners promoted once caught up. It is the only
// package that imports the library.
package etcdraft

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
	"github.com/happydave/hoardmq/internal/cluster/consensus/raftlog"
	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/hook"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

// Transport is what the shell needs from the cluster transport. A nil
// transport means a node with nobody to talk to.
type Transport interface {
	Send(ctx context.Context, peer string, c transport.Class, payload []byte) error
	Handle(c transport.Class, h transport.Handler)
	SetPeers(peers []config.Peer)
	Peers() []transport.PeerInfo
}

// Options configure a core. Zero durations and counts take the defaults.
type Options struct {
	Node             string
	Peers            []config.Peer // configured peers, for joining and for the transport
	Store            *raftlog.Store
	Transport        Transport
	Machine          *consensus.Machine
	Tick             time.Duration // default 100 ms
	ElectionTicks    int           // default 10
	HeartbeatTicks   int           // default 1
	SnapshotEntries  uint64        // default 10,000
	SnapshotTrailing uint64        // default 1,000
	PromoteLag       uint64        // default 100
	JoinTimeout      time.Duration // default 30 s
	JoinWarnEvery    time.Duration // default 30 s: how often the wait past the join window is warned of
	Bootstrap        *bool         // nil: the lowest name among this node and its peers bootstraps
	Logger           *slog.Logger
	Metrics          hook.Metrics
	Audit            func(action, target, outcome string, detail map[string]string)
	// HoldAdmissions, while it returns true, keeps this node as leader from
	// admitting a joiner: the joiner is answered with the leader's name and
	// a refusal. For tests; nil in the product.
	HoldAdmissions func() bool
	// SendBeforeSave is the simulation harness's injected defect: messages
	// are offered to the transport before the entries they depend on are
	// durable. Never set from configuration.
	SendBeforeSave bool
}

// Envelope kinds inside the transport's consensus class.
const (
	envRaft         byte = 0
	envJoin         byte = 1
	envJoinResponse byte = 2
)

type joinResponse struct {
	Accepted bool   `json:"accepted"`
	Leader   string `json:"leader,omitempty"`
	Reason   string `json:"reason,omitempty"`
}

// Core implements consensus.Core over the library.
type Core struct {
	o       Options
	id      uint64
	log     *slog.Logger
	machine *consensus.Machine
	members *consensus.Membership

	mu        sync.Mutex
	node      raft.Node
	lead      uint64
	state     raft.StateType
	term      uint64
	commit    uint64
	removed   bool
	wasMember bool
	stopped   bool
	bootstrap bool
	pendingCC bool
	waiters   map[uint64]chan result  // proposal id → completion
	ccWaiters map[uint64][]chan error // node id → completions of configuration changes
	confState *pb.ConfState
	// campaignedAt is when a restarted sole voter last campaigned early
	// (see campaignIfSoleVoterLocked).
	campaignedAt atomic.Int64
	idNames      map[uint64]string         // raft id → peer name, from configuration and from received frames
	readmit      map[uint64]consensus.Meta // members being removed so a wiped node can come back as a learner
	readded      map[uint64]bool           // re-added members not yet seen replicating
	deferred     []consensus.Meta          // re-adds to propose once the removal has been advanced past
	nextID       atomic.Uint64

	inbox     chan *pb.Message
	joins     chan joinEvent
	proposals chan proposal
	started   chan struct{} // closed once the raft node exists
	stop      chan struct{}
	done      chan struct{}
	dropped   atomic.Uint64

	// Raft counts its election and check-quorum timeouts in ticks, so the
	// loop's clock is only as good as the ticks it delivers. These say what
	// it delivered against what wall time owed it; see advanceTicks.
	ticks        atomic.Uint64 // delivered
	ticksOwed    atomic.Uint64 // whole tick periods that elapsed
	ticksDropped atomic.Uint64 // owed beyond one wake's burst bound
	ticksMaxRun  atomic.Uint64 // largest single catch-up

	// recoverTo is non-zero while a restart's recovery is in progress;
	// leaderCommit is the highest commit index the leader has announced to
	// this node (unclamped by this node's own log), which is what recovery
	// must reach before components act again.
	recoverTo    atomic.Uint64
	leaderCommit atomic.Uint64 // from MsgApp: the leader's commit, unclamped
	hbCommit     atomic.Uint64 // from MsgHeartbeat: clamped to this node's match
	heartbeats   atomic.Uint64 // heartbeats received since start

	// sendBeforeSave is the test seam for the persist-before-send control:
	// when set, the loop offers messages before saving, which the ordering
	// test must detect.
	sendBeforeSave bool
	onReady        func(entries, messages int, mustSync bool)
	sent           atomic.Uint64 // frames handed to the transport
	unroutable     atomic.Uint64 // messages with no known peer
}

type result struct {
	value any
	err   error
}

type proposal struct {
	id   uint64
	data []byte
	cc   *pb.ConfChangeV2
	done chan error
}

type joinEvent struct {
	from string
	meta consensus.Meta
	resp *joinResponse
}

// New validates the options. Nothing runs until Start.
func New(o Options) (*Core, error) {
	if o.Node == "" || o.Store == nil || o.Machine == nil {
		return nil, errors.New("etcdraft: node name, store and machine are required")
	}
	if o.Tick == 0 {
		o.Tick = 100 * time.Millisecond
	}
	if o.ElectionTicks == 0 {
		o.ElectionTicks = 10
	}
	if o.HeartbeatTicks == 0 {
		o.HeartbeatTicks = 1
	}
	if o.SnapshotEntries == 0 {
		o.SnapshotEntries = 10000
	}
	if o.SnapshotTrailing == 0 {
		o.SnapshotTrailing = 1000
	}
	if o.PromoteLag == 0 {
		o.PromoteLag = 100
	}
	if o.JoinTimeout == 0 {
		o.JoinTimeout = 30 * time.Second
	}
	if o.JoinWarnEvery == 0 {
		o.JoinWarnEvery = 30 * time.Second
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	c := &Core{o: o, id: consensus.NodeID(o.Node), log: o.Logger.With("component", "consensus"), machine: o.Machine, members: o.Machine.Membership(),
		waiters: map[uint64]chan result{}, ccWaiters: map[uint64][]chan error{}, idNames: map[uint64]string{}, readmit: map[uint64]consensus.Meta{}, readded: map[uint64]bool{},
		inbox: make(chan *pb.Message, 4096), joins: make(chan joinEvent, 64), proposals: make(chan proposal, 256),
		started: make(chan struct{}), stop: make(chan struct{}), done: make(chan struct{})}
	for _, p := range o.Peers {
		c.idNames[consensus.NodeID(p.Name)] = p.Name
	}
	if o.Transport != nil {
		o.Transport.Handle(transport.Consensus, c.receive)
		// Snapshots are bulk and travel on the data connection (WI 1340);
		// the envelope is the same.
		o.Transport.Handle(transport.ConsensusSnapshot, c.receive)
	}
	c.sendBeforeSave = o.SendBeforeSave
	return c, nil
}

// maxAppendBytes bounds one raft append message and maxInflightAppends the
// number a follower may have outstanding. Appends ride the control
// connection with the heartbeats and lease renewals (WI 1340), so one must
// never be bulk; a storm's lease batches are megabytes and are split. One
// named value, not a configuration setting, by owner decision (2026-09-06):
// it becomes a setting with this default only if it proves to need
// frequent change.
const (
	maxAppendBytes     = 256 << 10
	maxInflightAppends = 256
)

// ID is this node's Raft id.
func (c *Core) ID() uint64 { return c.id }

func (c *Core) raftConfig() *raft.Config {
	return &raft.Config{
		ID:              c.id,
		ElectionTick:    c.o.ElectionTicks,
		HeartbeatTick:   c.o.HeartbeatTicks,
		Storage:         c.o.Store,
		Applied:         c.machine.Applied(),
		MaxSizePerMsg:   maxAppendBytes,
		MaxInflightMsgs: maxInflightAppends,
		CheckQuorum:     true,
		PreVote:         true,
		Logger:          &raftLogger{c.log},
	}
}

// Start decides how this node enters the group and starts the loop: a log
// with state restarts; no peers bootstraps a one-member group; otherwise the
// node joins, bootstrapping only as the designated node after the join
// window passes with no answer.
func (c *Core) Start() error {
	if c.o.Transport != nil {
		c.o.Transport.SetPeers(c.o.Peers)
	}
	switch {
	case c.o.Store.HasState():
		// A restart replays the committed history above the local snapshot
		// through the machine. Components apply it silently and reconcile
		// once the applied index reaches the commit index the log holds:
		// acting on each replayed change would act on states this node
		// already acted on before it stopped.
		hs, _, err := c.o.Store.InitialState()
		if err != nil {
			return err
		}
		// recoverTo marks that a recovery is in progress; it ends once a
		// leader is known and the applied index reaches the leader's commit.
		// The local snapshot's restore is part of it.
		c.recoverTo.Store(max(hs.GetCommit(), 1))
		c.machine.BeginRecovery()
		c.count("hoardmq_raft_recoveries_total", map[string]string{"kind": "restart"}, 1)
		if err := c.restoreLatest(); err != nil {
			return err
		}
		c.setNode(raft.RestartNode(c.raftConfig()))
		c.log.Info("consensus restarted from the log", "applied", c.machine.Applied(), "recoverTo", c.recoverTo.Load())
		c.mu.Lock()
		c.campaignIfSoleVoterLocked() // a snapshot-only restart has no entries to replay
		c.mu.Unlock()
	case len(c.o.Peers) == 0:
		c.bootstrapGroup()
	default:
		go c.joinLoop()
	}
	go c.run()
	return nil
}

// restoreLatest applies the stored snapshot to the machine before the node
// restarts, so the applied index passed to the library is right.
func (c *Core) restoreLatest() error {
	snap, err := c.o.Store.Snapshot()
	if err != nil {
		return err
	}
	if raft.IsEmptySnap(snap) {
		return nil
	}
	return c.installSnapshot(snap)
}

// soleVoter reports whether the stored membership names this node as its
// only voter, so no election can produce another leader.
func soleVoter(cs *pb.ConfState, id uint64) bool {
	if cs == nil || len(cs.Voters) != 1 || len(cs.VotersOutgoing) != 0 {
		return false
	}
	return cs.Voters[0] == id
}

func (c *Core) bootstrapGroup() {
	meta := consensus.EncodeMeta(consensus.Meta{Name: c.o.Node, ID: c.id, Address: c.ownAddress()})
	c.setNode(raft.StartNode(c.raftConfig(), []raft.Peer{{ID: c.id, Context: meta}}))
	c.mu.Lock()
	c.bootstrap = true
	c.mu.Unlock()
	c.audit("cluster.bootstrap", c.o.Node, "ok", nil)
	c.log.Info("consensus bootstrapped a new group", "id", c.id)
}

func (c *Core) setNode(n raft.Node) {
	c.mu.Lock()
	c.node = n
	c.mu.Unlock()
	close(c.started)
}

// ownAddress is what other nodes should dial for this node: the transport
// peers do not include self, so it is learned from the join or left empty.
func (c *Core) ownAddress() string { return "" }

// designated reports whether this node bootstraps when nobody answers.
func (c *Core) designated() bool {
	if c.o.Bootstrap != nil {
		return *c.o.Bootstrap
	}
	names := []string{c.o.Node}
	for _, p := range c.o.Peers {
		names = append(names, p.Name)
	}
	sort.Strings(names)
	return names[0] == c.o.Node
}

// joinLoop asks the peers to admit this node until one does, then starts
// the node with an empty log to be filled by the leader. After the join
// window the designated node bootstraps instead. The time waited is the
// gauge hoardmq_raft_join_wait_seconds, zero again once the loop returns.
func (c *Core) joinLoop() {
	began := time.Now()
	deadline := began.Add(c.o.JoinTimeout)
	c.set("hoardmq_raft_join_wait_seconds", nil, 0)
	defer c.set("hoardmq_raft_join_wait_seconds", nil, 0)
	// groupKnown records that an answer named a leader, which only a group
	// that exists can do. The fallback below is for the case where no group
	// exists at all, and without this it fires on a slow answer instead: a
	// wiped node whose joins went unanswered for the window bootstrapped a
	// second group and was then admitted to the real one, 1 run in 40 under
	// a loaded gate (WI 1504). A node that has not joined or bootstrapped
	// answers nothing — its loop waits on c.started — so a cold start
	// produces no answers and this stays false.
	groupKnown := false
	waited := false
	var warned time.Time
	meta := consensus.EncodeMeta(consensus.Meta{Name: c.o.Node, ID: c.id})
	interval := max(c.o.Tick*20, 100*time.Millisecond)
	ask := func() {
		for _, p := range c.o.Transport.Peers() {
			if p.State != transport.Up {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), c.o.Tick*5)
			_ = c.o.Transport.Send(ctx, p.Name, transport.Consensus, append([]byte{envJoin}, meta...))
			cancel()
			c.count("hoardmq_raft_join_requests_total", nil, 1)
		}
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	ask()
	for {
		select {
		case <-c.stop:
			return
		case ev := <-c.joins:
			if ev.resp != nil && ev.resp.Accepted {
				c.audit("cluster.join", c.o.Node, "ok", map[string]string{"via": ev.from})
				c.log.Info("admitted to the cluster", "via", ev.from)
				// Messages addressed to an earlier incarnation of this node
				// may have queued while joining; the leader's view of this
				// node was reset before it answered, so they are stale.
				c.drainInbox()
				c.setNode(raft.RestartNode(c.raftConfig()))
				return
			}
			if ev.resp != nil && ev.resp.Leader != "" {
				groupKnown = true
				c.log.Debug("join redirected", "leader", ev.resp.Leader)
			}
		case <-ticker.C:
			now := time.Now()
			c.set("hoardmq_raft_join_wait_seconds", nil, now.Sub(began).Seconds())
			switch {
			case !now.After(deadline):
			case groupKnown:
				// A group exists and has not admitted this node yet. Waiting
				// is the only safe answer: bootstrapping here is a second
				// group, and this node would then be in two.
				if !waited {
					waited = true
					c.count("hoardmq_raft_join_window_passed_total", nil, 1)
				}
				if warned.IsZero() || now.Sub(warned) >= c.o.JoinWarnEvery {
					warned = now
					c.log.Warn("the join window passed and a group exists; waiting to be admitted rather than bootstrapping",
						"window", c.o.JoinTimeout, "waited", now.Sub(began).Round(time.Second))
				}
			case c.designated():
				c.count("hoardmq_raft_bootstrapped_after_window_total", nil, 1)
				c.log.Warn("no peer answered within the join window and no answer named a leader; bootstrapping as the designated node")
				c.bootstrapGroup()
				return
			}
			ask()
		}
	}
}

func (c *Core) drainInbox() {
	for {
		select {
		case <-c.inbox:
		default:
			return
		}
	}
}

// inboxWaitFloor keeps the wait from collapsing to an instant drop when the
// tick is unset, as it is for a Core built without options. It is not the
// bound: the bound is a small multiple of the raft tick, which is what
// makes it scale with the deployment.
//
// The bound must stay well under the transport's silence limit, which is
// the heartbeat times the permitted misses -- 3 s at product settings and
// 300 ms in the simulation harness. This handler runs on the link's
// per-class drainer, and a drainer that stops eventually fills the class
// queue and stops the reader, which is how a healthy node gets judged
// silent by its peer. An absolute floor is the wrong shape for that
// constraint: 100 ms is a sixth of the product limit and a third of the
// harness's, so a value chosen against the product would be reintroducing
// the stall in the configuration the corpus actually runs.
const inboxWaitFloor = 10 * time.Millisecond

// inboxWait is how long an ordinary raft message waits for space: five raft
// ticks, so it scales with the deployment rather than against it. 500 ms at
// a 100 ms product tick against a 3 s silence limit; 50 ms at the harness's
// 10 ms tick against 300 ms. One sixth in both cases, by construction.
func (c *Core) inboxWait() time.Duration {
	return max(c.o.Tick*5, inboxWaitFloor)
}

// offer hands a raft message to the loop's inbox.
//
// A full inbox means the loop is behind — applying, or writing to disk. The
// message is not discarded immediately: it waits, because raft's retries are
// cheap only in the sense that they cost a round trip, and under a storm a
// systematic drop is what turns a busy node into an unreachable one.
//
// The wait is bounded for every message type except a snapshot. Every other
// type has a retry behind it: the leader resends an append, a candidate
// re-campaigns after a lost vote. A snapshot does not. The leader holds the
// follower in StateSnapshot until it is told the snapshot arrived or
// failed, and a discarded snapshot tells it neither, so a snapshot waits
// until there is space or the core stops.
//
// The design amendment's receiver-dispatch rule for this class reads "the
// reader blocks ... a raft message must not be dropped silently by the
// transport". Blocking without a bound is what that rule asks for and it is
// not safe here for the reason given on inboxWaitFloor; the deliberate
// exception, and this bound, are recorded in the amendment.
func (c *Core) offer(m *pb.Message) {
	select {
	case c.inbox <- m:
		return
	default:
	}
	if m.GetType() == pb.MsgSnap {
		select {
		case c.inbox <- m:
		case <-c.stop:
		}
		return
	}
	t := time.NewTimer(c.inboxWait())
	defer t.Stop()
	select {
	case c.inbox <- m:
	case <-c.stop:
	case <-t.C:
		c.dropped.Add(1)
		c.count("hoardmq_raft_dropped_frames_total", map[string]string{"type": m.GetType().String()}, 1)
		c.log.Warn("raft message dropped: the consensus loop is behind", "type", m.GetType().String(), "from", m.GetFrom())
	}
}

// receive is the transport handler for the consensus class. Raft messages
// go to the inbox for the loop; join traffic to the join channel. A full
// inbox waits rather than dropping at once; see offer.
func (c *Core) receive(peer string, payload []byte) {
	if len(payload) == 0 {
		return
	}
	switch payload[0] {
	case envRaft:
		m := &pb.Message{}
		if err := protoUnmarshal(payload[1:], m); err != nil {
			c.log.Warn("bad raft message", "peer", peer, "err", err)
			return
		}
		// The transport authenticated the peer; remember which id it is so
		// replies can be addressed before the membership is applied.
		c.mu.Lock()
		c.idNames[m.GetFrom()] = peer
		c.mu.Unlock()
		c.offer(m)
	case envJoin:
		meta, ok := consensus.DecodeMeta(payload[1:])
		if !ok {
			return
		}
		select {
		case c.joins <- joinEvent{from: peer, meta: meta}:
		default:
		}
	case envJoinResponse:
		var r joinResponse
		if json.Unmarshal(payload[1:], &r) != nil {
			return
		}
		select {
		case c.joins <- joinEvent{from: peer, resp: &r}:
		default:
		}
	}
}

// Propose implements consensus.Core.
func (c *Core) Propose(ctx context.Context, kind consensus.Kind, payload []byte) (any, error) {
	if err := c.leaderCheck(); err != nil {
		return nil, err
	}
	began := time.Now()
	id := c.nextID.Add(1)
	data := make([]byte, 8, 9+len(payload))
	putUint64(data, id)
	data = append(data, consensus.Encode(kind, payload)...)
	done := make(chan result, 1)
	c.mu.Lock()
	c.waiters[id] = done
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.waiters, id)
		c.mu.Unlock()
	}()
	p := proposal{id: id, data: data, done: make(chan error, 1)}
	select {
	case c.proposals <- p:
	case <-ctx.Done():
		c.proposed("timeout", began)
		return nil, ctx.Err()
	case <-c.done:
		return nil, consensus.ErrStopped
	}
	select {
	case err := <-p.done:
		if err != nil {
			c.proposed("rejected", began)
			return nil, err
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		return nil, consensus.ErrStopped
	}
	select {
	case r := <-done:
		outcome := "applied"
		if r.err != nil {
			outcome = "failed"
		}
		c.proposed(outcome, began)
		return r.value, r.err
	case <-ctx.Done():
		c.proposed("timeout", began)
		return nil, ctx.Err()
	case <-c.done:
		return nil, consensus.ErrStopped
	}
}

// proposed counts a proposal's outcome and the time from Propose to it.
func (c *Core) proposed(outcome string, began time.Time) {
	l := map[string]string{"outcome": outcome}
	c.count("hoardmq_raft_proposals_total", l, 1)
	c.count("hoardmq_raft_proposal_seconds_total", l, time.Since(began).Seconds())
}

func (c *Core) leaderCheck() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return consensus.ErrStopped
	}
	if c.removed {
		return consensus.ErrRemoved
	}
	if c.node == nil || c.state != raft.StateLeader {
		return &consensus.NotLeaderError{Leader: c.leaderNameLocked()}
	}
	return nil
}

func (c *Core) leaderNameLocked() string {
	if c.lead == 0 {
		return ""
	}
	if m, ok := c.members.Lookup(c.lead); ok && m.Name != "" {
		return m.Name
	}
	if name, ok := c.idNames[c.lead]; ok {
		return name
	}
	return fmt.Sprintf("%x", c.lead)
}

// peerName resolves a raft id to a transport peer: the membership first,
// then the names learned from configuration and received frames.
func (c *Core) peerName(id uint64) (string, bool) {
	if m, ok := c.members.Lookup(id); ok && m.Name != "" {
		return m.Name, true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	name, ok := c.idNames[id]
	return name, ok
}

// AddMember implements consensus.Core: the node joins as a learner.
func (c *Core) AddMember(ctx context.Context, name, address string) error {
	id := consensus.NodeID(name)
	if m, ok := c.members.Lookup(id); ok && m.Name != name {
		return fmt.Errorf("%w: %q and %q", consensus.ErrIDCollision, m.Name, name)
	}
	if m, ok := c.members.Lookup(id); ok && m.Name == name {
		return nil
	}
	meta := consensus.EncodeMeta(consensus.Meta{Name: name, ID: id, Address: address})
	cc := &pb.ConfChangeV2{Changes: []*pb.ConfChangeSingle{{Type: pb.ConfChangeAddLearnerNode.Enum(), NodeId: &id}}, Context: meta}
	return c.confChange(ctx, id, cc, "cluster.member.added", name)
}

// PromoteMember implements consensus.Core.
func (c *Core) PromoteMember(ctx context.Context, name string) error {
	m, ok := c.members.ByName(name)
	if !ok {
		return consensus.ErrNotMember
	}
	if m.Role == consensus.Voter {
		return consensus.ErrAlreadyVoter
	}
	id := m.ID
	cc := &pb.ConfChangeV2{Changes: []*pb.ConfChangeSingle{{Type: pb.ConfChangeAddNode.Enum(), NodeId: &id}}, Context: consensus.EncodeMeta(consensus.Meta{Name: name, ID: id, Address: m.Address})}
	return c.confChange(ctx, id, cc, "cluster.member.promoted", name)
}

// RemoveMember implements consensus.Core.
func (c *Core) RemoveMember(ctx context.Context, name string) error {
	m, ok := c.members.ByName(name)
	if !ok {
		return consensus.ErrNotMember
	}
	id := m.ID
	cc := &pb.ConfChangeV2{Changes: []*pb.ConfChangeSingle{{Type: pb.ConfChangeRemoveNode.Enum(), NodeId: &id}}, Context: consensus.EncodeMeta(consensus.Meta{Name: name, ID: id})}
	return c.confChange(ctx, id, cc, "cluster.member.removed", name)
}

// confChange proposes a configuration change and waits until it applies.
func (c *Core) confChange(ctx context.Context, id uint64, cc *pb.ConfChangeV2, action, target string) error {
	if err := c.leaderCheck(); err != nil {
		return err
	}
	done := make(chan error, 1)
	c.mu.Lock()
	c.ccWaiters[id] = append(c.ccWaiters[id], done)
	c.mu.Unlock()
	p := proposal{cc: cc, done: make(chan error, 1)}
	select {
	case c.proposals <- p:
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return consensus.ErrStopped
	}
	select {
	case err := <-p.done:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return consensus.ErrStopped
	}
	select {
	case err := <-done:
		if err == nil {
			c.audit(action, target, "ok", nil)
		}
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return consensus.ErrStopped
	}
}

// Leader implements consensus.Core.
func (c *Core) Leader() (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	name := c.leaderNameLocked()
	return name, name != ""
}

// Members implements consensus.Core.
func (c *Core) Members() []consensus.Member { return c.members.List() }

// Status implements consensus.Core.
func (c *Core) Status() consensus.Status {
	c.mu.Lock()
	defer c.mu.Unlock()
	st := consensus.Status{Term: c.term, Commit: c.commit, Applied: c.machine.Applied(), SnapshotIndex: c.o.Store.SnapshotIndex(), Leader: c.leaderNameLocked(), Bootstrapped: c.bootstrap, Joined: c.readyLocked()}
	st.Ticks, st.TicksOwed, st.TicksDropped, st.TicksMaxRun = c.TickStats()
	switch {
	case c.stopped:
		st.Role = "stopped"
	case c.removed:
		st.Role = "removed"
	case c.node == nil:
		st.Role = "joining"
	case c.state == raft.StateLeader:
		st.Role = "leader"
	case c.state == raft.StateCandidate || c.state == raft.StatePreCandidate:
		st.Role = "candidate"
	default:
		if m, ok := c.members.Lookup(c.id); ok && m.Role == consensus.Learner {
			st.Role = "learner"
		} else {
			st.Role = "follower"
		}
	}
	return st
}

// Ready implements consensus.Core.
func (c *Core) Ready() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.readyLocked()
}

func (c *Core) readyLocked() bool {
	if c.node == nil || c.removed || c.stopped || c.lead == 0 {
		return false
	}
	if !c.members.Has(c.id) {
		return false
	}
	return c.machine.Applied()+c.o.PromoteLag >= c.commit
}

// Stop implements consensus.Core.
func (c *Core) Stop() {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return
	}
	c.stopped = true
	c.mu.Unlock()
	close(c.stop)
	<-c.done
	_ = c.o.Store.Close()
}

func (c *Core) audit(action, target, outcome string, detail map[string]string) {
	if c.o.Audit != nil {
		c.o.Audit(action, target, outcome, detail)
	}
}

func (c *Core) count(name string, labels map[string]string, delta float64) {
	if c.o.Metrics != nil {
		c.o.Metrics.Add(name, labels, delta)
	}
}

func (c *Core) set(name string, labels map[string]string, value float64) {
	if c.o.Metrics != nil {
		c.o.Metrics.Set(name, labels, value)
	}
}

// raftLogger adapts slog to the library's logger.
type raftLogger struct{ l *slog.Logger }

func (r *raftLogger) Debug(v ...any)              { r.l.Debug(fmt.Sprint(v...)) }
func (r *raftLogger) Debugf(f string, v ...any)   { r.l.Debug(fmt.Sprintf(f, v...)) }
func (r *raftLogger) Error(v ...any)              { r.l.Error(fmt.Sprint(v...)) }
func (r *raftLogger) Errorf(f string, v ...any)   { r.l.Error(fmt.Sprintf(f, v...)) }
func (r *raftLogger) Info(v ...any)               { r.l.Info(fmt.Sprint(v...)) }
func (r *raftLogger) Infof(f string, v ...any)    { r.l.Info(fmt.Sprintf(f, v...)) }
func (r *raftLogger) Warning(v ...any)            { r.l.Warn(fmt.Sprint(v...)) }
func (r *raftLogger) Warningf(f string, v ...any) { r.l.Warn(fmt.Sprintf(f, v...)) }
func (r *raftLogger) Fatal(v ...any)              { r.l.Error("raft fatal: " + fmt.Sprint(v...)) }
func (r *raftLogger) Fatalf(f string, v ...any)   { r.l.Error("raft fatal: " + fmt.Sprintf(f, v...)) }
func (r *raftLogger) Panic(v ...any)              { panic(fmt.Sprint(v...)) }
func (r *raftLogger) Panicf(f string, v ...any)   { panic(fmt.Sprintf(f, v...)) }

// checkRecovered ends a restart's recovery once a leader is known and the
// applied index has reached the commit index the leader reports. The commit
// index the log held at start is not enough: it is stale by definition
// after a crash, and a node that acted on it would act on an old picture
// until the leader's entries or snapshot arrived — which is the class of
// defect recovery exists to prevent.
func (c *Core) checkRecovered() {
	if c.recoverTo.Load() == 0 || !c.machine.Recovering() {
		return
	}
	c.mu.Lock()
	lead := c.lead
	c.mu.Unlock()
	if lead == 0 {
		return
	}
	// The node's own commit index is clamped to its log, so a stale log
	// reads as caught up the moment its few entries are applied. The
	// leader's appends carry its real commit index and are the target; a
	// heartbeat's commit is clamped to what the leader knows this node has,
	// so it only settles the case where the leader has nothing to send —
	// after several heartbeats with nothing appended, the log is current.
	// A node that leads holds the truth itself.
	applied := c.machine.Applied()
	target := c.leaderCommit.Load()
	switch {
	case lead == c.id:
	case target > 0 && applied >= target:
	case target == 0 && c.heartbeats.Load() >= 3 && applied >= c.hbCommit.Load() && c.hbCommit.Load() > 0:
	default:
		return
	}
	c.recoverTo.Store(0)
	c.machine.EndRecovery()
	c.log.Info("consensus recovered", "applied", applied, "leaderCommit", target, "heartbeats", c.heartbeats.Load())
}

// noteLeaderCommit records the commit index the leader announces in its
// appends, heartbeats and snapshots.
func (c *Core) noteLeaderCommit(m *pb.Message) {
	raise := func(a *atomic.Uint64, v uint64) {
		for {
			cur := a.Load()
			if v <= cur || a.CompareAndSwap(cur, v) {
				return
			}
		}
	}
	switch m.GetType() {
	case pb.MsgApp:
		raise(&c.leaderCommit, m.GetCommit())
	case pb.MsgHeartbeat:
		c.heartbeats.Add(1)
		raise(&c.hbCommit, m.GetCommit())
	}
}

// Recovering implements consensus.Core.
func (c *Core) Recovering() bool { return c.machine.Recovering() }
