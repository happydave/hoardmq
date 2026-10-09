package etcdraft

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
	"github.com/happydave/hoardmq/internal/cluster/consensus/raftlog"
	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/config"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
	"google.golang.org/protobuf/proto"
)

func putUint64(b []byte, v uint64) { binary.BigEndian.PutUint64(b, v) }

func protoUnmarshal(b []byte, m proto.Message) error { return proto.Unmarshal(b, m) }

// snapshot envelope: the project's format around the machine bytes.
var snapshotMagic = []byte("HQRS")

const snapshotFormat = 1

func wrapSnapshot(index, term uint64, machine []byte) []byte {
	out := append([]byte(nil), snapshotMagic...)
	out = append(out, snapshotFormat)
	out = binary.BigEndian.AppendUint64(out, index)
	out = binary.BigEndian.AppendUint64(out, term)
	return append(out, machine...)
}

func unwrapSnapshot(data []byte) (index, term uint64, machine []byte, err error) {
	if len(data) < 4+1+16 || !bytes.Equal(data[:4], snapshotMagic) {
		return 0, 0, nil, errors.New("snapshot: not a HoardMQ snapshot")
	}
	if data[4] != snapshotFormat {
		return 0, 0, nil, fmt.Errorf("snapshot: format %d not supported", data[4])
	}
	index = binary.BigEndian.Uint64(data[5:13])
	term = binary.BigEndian.Uint64(data[13:21])
	return index, term, data[21:], nil
}

// run is the node loop. Nothing else touches the library's node.
func (c *Core) run() {
	defer close(c.done)
	select {
	case <-c.started:
	case <-c.stop:
		return
	}
	c.mu.Lock()
	n := c.node
	c.mu.Unlock()
	ticker := time.NewTicker(c.o.Tick)
	defer ticker.Stop()
	defer n.Stop()
	last := time.Now()
	for {
		select {
		case <-c.stop:
			return
		case now := <-ticker.C:
			if !c.advanceTicks(n, now.Sub(last)) {
				return
			}
			last = now
			c.maybePromote(n)
		case rd := <-n.Ready():
			if err := c.handleReady(n, rd); err != nil {
				c.log.Error("consensus loop stopping", "err", err)
				return
			}
			if c.isRemoved() {
				return
			}
		case m := <-c.inbox:
			c.noteLeaderCommit(m)
			if m.GetTo() != c.id {
				continue
			}
			if err := n.Step(context.Background(), m); err != nil {
				c.log.Debug("step rejected", "type", m.GetType().String(), "err", err)
			}
		case p := <-c.proposals:
			c.propose(n, p)
		case ev := <-c.joins:
			c.handleJoin(n, ev)
		}
	}
}

// maxCatchUpWindow bounds one wake's burst, in the units the bound is
// actually about: how much of a gap is worth reconstructing. It is not
// there for the clock's sake — the clock wants every tick it is owed — but
// as a backstop against a gap that is not a stall at all, such as a
// suspended machine or a laptop lid. A minute is far past any stall a
// running node has (the worst measured is 7.18 s) and short of a burst
// worth waiting through.
//
// It is a duration, not a tick count, because a count would mean something
// different at the product's 100 ms tick and the harness's 10 ms one — and
// a count small enough to be safe at 10 ms would silently reintroduce the
// defect at 100 ms, which is the configuration that matters.
const maxCatchUpWindow = time.Minute

// advanceTicks gives raft the ticks wall time owed it, rather than the one
// tick a ticker receive represents.
//
// time.NewTicker is documented to "drop ticks to make up for slow
// receivers", and its channel holds one, so a stall costs raft — which
// counts its election and check-quorum timeouts in ticks — very nearly the
// whole gap. Measured on this loop at a 10 ms tick: a 1 s stall over 110
// wall periods delivered 11 ticks without this, and 109 with it; a 200 ms
// stall over 30 periods delivered 10 against 29. The deficit is permanent,
// not repaid later.
//
// Against the 7.18 s whole-process stall round 12 recorded at the 100 ms
// product tick, that is on the order of seventy ticks — seven election
// timeouts — that the node never counted, which is why it can go far longer
// than its stated timeout without campaigning.
//
// The catch-up is delivered in full up to the bound, and a burst cannot
// disrupt a healthy leader: raftConfig sets PreVote, so a node that
// campaigns must first win a pre-vote, and one whose leader is alive and
// reachable loses it without burning a term. That dependency is why this
// comment names the setting rather than describing the burst as safe.
//
// Ticks past the bound are dropped rather than carried: carrying them would
// leave the loop permanently behind after one long stall, which is the
// defect being fixed. It returns false when the core stopped mid-burst, so
// a stop is never waited out by a catch-up.
func (c *Core) advanceTicks(n raft.Node, elapsed time.Duration) bool {
	if c.o.Tick <= 0 {
		// New defaults Tick, so this is only reachable by a Core built
		// without options; tick once and count it, so the totals still add
		// up rather than silently under-reporting.
		n.Tick()
		c.ticks.Add(1)
		c.ticksOwed.Add(1)
		c.count("hoardmq_raft_ticks_total", map[string]string{"outcome": "delivered"}, 1)
		return true
	}
	owed := int(elapsed / c.o.Tick)
	if owed < 1 {
		owed = 1
	}
	c.ticksOwed.Add(uint64(owed))
	run := owed
	if bound := max(int(maxCatchUpWindow/c.o.Tick), 1); run > bound {
		c.ticksDropped.Add(uint64(owed - bound))
		c.count("hoardmq_raft_ticks_total", map[string]string{"outcome": "dropped"}, float64(owed-bound))
		run = bound
	}
	for {
		was := c.ticksMaxRun.Load()
		if uint64(run) <= was || c.ticksMaxRun.CompareAndSwap(was, uint64(run)) {
			break
		}
	}
	for i := 0; i < run; i++ {
		select {
		case <-c.stop:
			return false
		default:
		}
		n.Tick()
		c.ticks.Add(1)
	}
	// Counted per advance rather than per tick: the interesting quantity is
	// the catch-up, and a metric call inside the burst would be paid for a
	// hundred times during exactly the stall this exists to survive.
	c.count("hoardmq_raft_ticks_total", map[string]string{"outcome": "delivered"}, float64(run))
	return true
}

// TickStats reports what the loop's clock did: ticks delivered, whole tick
// periods wall time owed, ticks dropped past the burst bound, and the
// largest single catch-up — the one that says whether a run was starved.
//
// The same figures reach a deployment as hoardmq_raft_ticks_total{outcome};
// this is the in-process reading, for the harness and for tests.
func (c *Core) TickStats() (delivered, owed, dropped, maxRun uint64) {
	return c.ticks.Load(), c.ticksOwed.Load(), c.ticksDropped.Load(), c.ticksMaxRun.Load()
}

func (c *Core) isRemoved() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.removed
}

func (c *Core) propose(n raft.Node, p proposal) {
	ctx, cancel := context.WithTimeout(context.Background(), c.o.Tick*5)
	defer cancel()
	var err error
	if p.cc != nil {
		c.mu.Lock()
		pending := c.pendingCC
		c.mu.Unlock()
		if pending {
			err = errors.New("consensus: a configuration change is already in flight")
		} else {
			err = n.ProposeConfChange(ctx, p.cc)
			if err == nil {
				c.mu.Lock()
				c.pendingCC = true
				c.mu.Unlock()
			}
		}
	} else {
		err = n.Propose(ctx, p.data)
	}
	if err != nil {
		err = fmt.Errorf("%w: %v", consensus.ErrDropped, err)
	}
	p.done <- err
}

// handleReady performs one Ready batch in the order the library requires:
// save, send (with the token the save returned), apply, advance.
func (c *Core) handleReady(n raft.Node, rd raft.Ready) error {
	if rd.SoftState != nil {
		c.mu.Lock()
		c.lead, c.state = rd.Lead, rd.RaftState
		c.mu.Unlock()
	}
	if rd.HardState != nil && !raft.IsEmptyHardState(rd.HardState) {
		c.mu.Lock()
		c.term, c.commit = rd.GetTerm(), rd.GetCommit()
		c.mu.Unlock()
	}
	if c.onReady != nil {
		c.onReady(len(rd.Entries), len(rd.Messages), rd.MustSync)
	}
	if c.sendBeforeSave {
		// Test seam: the wrong order, using a stale token.
		c.send(n, c.o.Store.LastDurable(), rd.Messages)
	}
	tok, err := c.o.Store.Save(rd.HardState, rd.Entries, rd.Snapshot, rd.MustSync)
	if err != nil {
		return fmt.Errorf("raft log save: %w", err)
	}
	if !c.sendBeforeSave {
		c.send(n, tok, rd.Messages)
	}
	if rd.Snapshot != nil && !raft.IsEmptySnap(rd.Snapshot) {
		if err := c.installSnapshot(rd.Snapshot); err != nil {
			return err
		}
		c.count("hoardmq_raft_snapshots_total", map[string]string{"direction": "in"}, 1)
	}
	for _, e := range rd.CommittedEntries {
		c.apply(n, e)
	}
	c.checkRecovered()
	n.Advance()
	c.proposeDeferred(n)
	// After Advance for the same reason as proposeDeferred: a campaign is
	// refused while a configuration change this Ready applied is still
	// pending in the library.
	c.mu.Lock()
	c.campaignIfSoleVoterLocked()
	c.mu.Unlock()
	return c.maybeSnapshot()
}

// proposeDeferred re-adds members whose removal has just applied. It runs
// after Advance because the library refuses a configuration change while
// the previous one is, from its point of view, still unapplied.
func (c *Core) proposeDeferred(n raft.Node) {
	c.mu.Lock()
	readd := c.deferred
	c.deferred = nil
	pending := c.pendingCC
	c.mu.Unlock()
	for _, meta := range readd {
		if pending {
			c.mu.Lock()
			c.deferred = append(c.deferred, meta)
			c.mu.Unlock()
			continue
		}
		id := meta.ID
		cc := &pb.ConfChangeV2{Changes: []*pb.ConfChangeSingle{{Type: pb.ConfChangeAddLearnerNode.Enum(), NodeId: &id}}, Context: consensus.EncodeMeta(meta)}
		ctx, cancel := context.WithTimeout(context.Background(), c.o.Tick*5)
		err := n.ProposeConfChange(ctx, cc)
		cancel()
		if err == nil {
			pending = true
			c.mu.Lock()
			c.pendingCC = true
			c.readded[id] = true
			c.mu.Unlock()
			c.audit("cluster.member.readded", meta.Name, "proposed", nil)
		} else {
			c.mu.Lock()
			c.deferred = append(c.deferred, meta)
			c.mu.Unlock()
		}
	}
}

// send hands messages to the transport. The token is required by type: it
// exists only after the store has persisted the batch it belongs to.
func (c *Core) send(n raft.Node, _ raftlog.Durable, msgs []*pb.Message) {
	if c.o.Transport == nil {
		return
	}
	for _, m := range msgs {
		if m.GetTo() == c.id {
			continue
		}
		to, ok := c.peerName(m.GetTo())
		if !ok {
			c.unroutable.Add(1)
			n.ReportUnreachable(m.GetTo())
			continue
		}
		c.sent.Add(1)
		b, err := proto.Marshal(m)
		if err != nil {
			c.log.Error("marshal raft message", "err", err)
			continue
		}
		// Raft messages ride the control connection, which is never shared
		// with data (WI 1340), so a send may wait up to an election timeout
		// for its queue rather than the one tick that, on a shared queue
		// under a storm, turned every raft message into a drop. A snapshot
		// is bulk and goes on the data connection; a version-2 peer has one
		// connection and takes it on the consensus class.
		class := transport.Consensus
		if m.GetType() == pb.MsgSnap {
			class = transport.ConsensusSnapshot
		}
		ctx, cancel := context.WithTimeout(context.Background(), c.o.Tick*time.Duration(c.o.ElectionTicks))
		err = c.o.Transport.Send(ctx, to, class, append([]byte{envRaft}, b...))
		if errors.Is(err, transport.ErrUnsupportedClass) {
			err = c.o.Transport.Send(ctx, to, transport.Consensus, append([]byte{envRaft}, b...))
		}
		cancel()
		if m.GetType() == pb.MsgSnap {
			status := raft.SnapshotFinish
			if err != nil {
				status = raft.SnapshotFailure
			}
			n.ReportSnapshot(m.GetTo(), status)
		}
		if err != nil {
			n.ReportUnreachable(m.GetTo())
		}
	}
}

// installSnapshot restores the machine and the membership from a snapshot.
// The restore runs silently and the components reconcile once against the
// restored state, so nothing acts on the intermediate picture. A snapshot at
// or below the applied index is counted, not refused: refusing one was tried
// and reverted in WI 1273 because it starved a node that needed the snapshot
// to catch up (WI 1291 has the record).
func (c *Core) installSnapshot(snap *pb.Snapshot) error {
	index, _, machine, err := unwrapSnapshot(snap.Data)
	if err != nil {
		return err
	}
	if applied := c.machine.Applied(); applied > 0 && snap.Metadata.GetIndex() <= applied {
		c.count("hoardmq_raft_snapshots_total", map[string]string{"direction": "in-stale"}, 1)
		c.log.Warn("snapshot at or below the applied index installed", "snapshot", snap.Metadata.GetIndex(), "applied", applied)
	}
	live := !c.machine.Recovering()
	if live {
		c.machine.BeginRecovery()
		c.count("hoardmq_raft_recoveries_total", map[string]string{"kind": "snapshot"}, 1)
	}
	if err := c.machine.Restore(machine); err != nil {
		return err
	}
	c.machine.SetApplied(snap.Metadata.GetIndex())
	_ = index
	c.applyConfState(snap.Metadata.GetConfState())
	if live {
		c.machine.EndRecovery()
	} else {
		c.checkRecovered()
	}
	return nil
}

// apply applies one committed entry.
func (c *Core) apply(n raft.Node, e *pb.Entry) {
	switch e.GetType() {
	case pb.EntryNormal:
		data := e.GetData()
		if len(data) < 9 {
			c.machine.SetApplied(e.GetIndex())
			return
		}
		id := binary.BigEndian.Uint64(data[:8])
		value, err := c.machine.Apply(e.GetIndex(), data[8:])
		c.mu.Lock()
		w := c.waiters[id]
		c.mu.Unlock()
		if w != nil {
			w <- result{value: value, err: err}
		}
	case pb.EntryConfChange, pb.EntryConfChangeV2:
		var cc pb.ConfChangeI
		if e.GetType() == pb.EntryConfChange {
			v1 := &pb.ConfChange{}
			if err := proto.Unmarshal(e.GetData(), v1); err != nil {
				c.log.Error("bad conf change", "err", err)
				return
			}
			cc = v1
		} else {
			v2 := &pb.ConfChangeV2{}
			if err := proto.Unmarshal(e.GetData(), v2); err != nil {
				c.log.Error("bad conf change", "err", err)
				return
			}
			cc = v2
		}
		v2 := cc.AsV2()
		if meta, ok := consensus.DecodeMeta(v2.GetContext()); ok {
			c.members.Upsert(meta)
		}
		cs := n.ApplyConfChange(cc)
		c.machine.SetApplied(e.GetIndex())
		c.applyConfState(cs)
		c.mu.Lock()
		c.pendingCC = false
		var ids []uint64
		var readd []consensus.Meta
		for _, ch := range v2.GetChanges() {
			ids = append(ids, ch.GetNodeId())
			if ch.GetType() == pb.ConfChangeRemoveNode {
				if meta, ok := c.readmit[ch.GetNodeId()]; ok {
					delete(c.readmit, ch.GetNodeId())
					readd = append(readd, meta)
				}
			}
		}
		var toResolve []chan error
		for _, id := range ids {
			toResolve = append(toResolve, c.ccWaiters[id]...)
			delete(c.ccWaiters, id)
		}
		if c.state == raft.StateLeader {
			c.deferred = append(c.deferred, readd...)
		}
		c.mu.Unlock()
		for _, w := range toResolve {
			w <- nil
		}
	}
}

// campaignIfSoleVoterLocked starts the one early election a restarted sole
// voter makes. A restarted node learns its membership from the snapshot and
// the log it replays; a sole voter learns nobody else can be elected, and
// recovery ends only once a leader is known, so waiting out an election
// timeout would keep a single-node broker refusing clients for nothing. It
// waits until the local log is replayed — the library refuses to campaign
// while a committed configuration change is still unapplied — and repeats,
// no oftener than every few ticks, until a leader is known: a campaign the
// library refused returns no error. Callers hold c.mu. Off this goroutine:
// the library's step blocks until its loop takes the message, and that loop
// may be waiting on the caller.
func (c *Core) campaignIfSoleVoterLocked() {
	n := c.node
	if n == nil || c.lead != 0 || !c.machine.Recovering() || c.machine.Applied() < c.recoverTo.Load() || !soleVoter(c.confState, c.id) {
		return
	}
	if last := c.campaignedAt.Load(); last != 0 && time.Since(time.Unix(0, last)) < 2*c.o.Tick {
		return
	}
	c.campaignedAt.Store(time.Now().UnixNano())
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), c.o.Tick*time.Duration(c.o.ElectionTicks))
		defer cancel()
		if err := n.Campaign(ctx); err != nil {
			c.log.Warn("sole voter could not campaign at once", "err", err)
		}
	}()
}

// applyConfState updates the membership roles from the library's view and
// tells the transport about members it may not have as peers.
func (c *Core) applyConfState(cs *pb.ConfState) {
	if cs == nil {
		return
	}
	voters := append(append([]uint64(nil), cs.GetVoters()...), cs.GetVotersOutgoing()...)
	learners := append(append([]uint64(nil), cs.GetLearners()...), cs.GetLearnersNext()...)
	c.members.SetRoles(voters, learners)
	inConf := c.members.Has(c.id)
	c.mu.Lock()
	c.confState = proto.Clone(cs).(*pb.ConfState)
	// A joiner applies the group's history before it appears in the
	// configuration; only a node that was a member and no longer is has
	// been removed.
	if inConf {
		c.wasMember = true
	} else if c.wasMember && c.machine.Applied() >= c.commit {
		// Not in the configuration at the head of the log: removed. (A
		// node replaying history it was once removed from is not.)
		c.removed = true
	}
	removed := c.removed
	c.mu.Unlock()
	if removed {
		c.audit("cluster.member.removed", c.o.Node, "self", nil)
		c.log.Warn("this node was removed from the cluster")
	}
	c.syncPeers()
}

// syncPeers gives the transport the union of the configured peers and the
// members with addresses.
func (c *Core) syncPeers() {
	if c.o.Transport == nil {
		return
	}
	seen := map[string]bool{}
	var peers []config.Peer
	for _, p := range c.o.Peers {
		seen[p.Name] = true
		peers = append(peers, p)
	}
	for _, m := range c.members.List() {
		if m.Name == "" || m.Name == c.o.Node || seen[m.Name] || m.Address == "" {
			continue
		}
		peers = append(peers, config.Peer{Name: m.Name, Address: m.Address})
	}
	c.o.Transport.SetPeers(peers)
}

// joinDisposition is what the leader decides to do with a join request.
type joinDisposition int

const (
	// joinAccept admits the joiner as it stands: either it is already being
	// re-admitted, or it is a known member with nothing to reset.
	joinAccept joinDisposition = iota
	// joinIDCollision refuses: the id belongs to a member under another name.
	joinIDCollision
	// joinReadmit refuses for now and resets the member: it holds an id the
	// configuration carries but arrives with an empty log, so it lost its
	// data and must be removed and re-added as a learner.
	joinReadmit
	// joinAddNew adds an id the configuration does not carry, as a learner.
	joinAddNew
)

func (d joinDisposition) String() string {
	switch d {
	case joinAccept:
		return "accept"
	case joinIDCollision:
		return "id-collision"
	case joinReadmit:
		return "readmit"
	case joinAddNew:
		return "add-new"
	}
	return "unknown"
}

// decideJoin is the leader's disposition of a join request, as a pure
// function of what the leader knows about the joiner. Every joiner arrives
// with an empty log, so the question is only ever whether the member's
// progress needs resetting before it is filled again.
//
// matchZero is the leader's progress for the member reading zero. It is not
// evidence that the member never held a log: a leader resets every
// follower's progress when it is elected, so immediately after an election
// every member's match index is zero until the first append reaches it. The
// durable evidence is the role. A member becomes a voter only by promotion,
// and maybePromote promotes a learner only once its match index is above
// zero and within the promotion lag of the commit index, so a voter has
// held a log by construction — and the role, unlike the progress, is
// carried in the configuration state and survives an election.
func decideJoin(known bool, m consensus.Member, joiningName string, readded, matchZero bool) joinDisposition {
	if !known {
		return joinAddNew
	}
	if m.Name != joiningName {
		return joinIDCollision
	}
	if readded {
		// Already removed and re-added for this joiner; accepting is what
		// terminates its retry loop.
		return joinAccept
	}
	if m.Role != consensus.Voter && matchZero {
		// A learner the leader has never replicated to: nothing to reset.
		return joinAccept
	}
	return joinReadmit
}

// handleJoin answers a join request when this node is the leader: the
// joiner is added as a learner. A follower answers with the leader hint.
func (c *Core) handleJoin(n raft.Node, ev joinEvent) {
	if ev.resp != nil {
		return // a late response to our own join, after we started
	}
	resp := joinResponse{}
	c.mu.Lock()
	leader := c.state == raft.StateLeader
	leaderName := c.leaderNameLocked()
	c.mu.Unlock()
	switch {
	case !leader:
		resp.Leader = leaderName
		resp.Reason = "not the leader"
	case c.o.HoldAdmissions != nil && c.o.HoldAdmissions():
		resp.Leader = c.o.Node
		resp.Reason = "admissions held"
	default:
		// The leader names itself, so every answer from a group that exists
		// names a leader — including a refusal. That is what the joiner reads
		// to know there is something to join (WI 1504).
		resp.Leader = c.o.Node
		m, ok := c.members.Lookup(ev.meta.ID)
		c.mu.Lock()
		readded := c.readded[ev.meta.ID]
		pending := c.pendingCC
		c.mu.Unlock()
		matchZero := false
		if ok {
			if pr, has := n.Status().Progress[ev.meta.ID]; has && pr.Match == 0 {
				matchZero = true
			}
		}
		switch decideJoin(ok, m, ev.meta.Name, readded, matchZero) {
		case joinIDCollision:
			resp.Reason = consensus.ErrIDCollision.Error()
			c.audit("cluster.join", ev.meta.Name, "refused", map[string]string{"reason": resp.Reason})
		case joinAccept:
			resp.Accepted = true
		case joinReadmit:
			// A member with an empty log: it lost its data. Its progress
			// must be reset, so it is removed and re-added as a learner;
			// the joiner is answered on a later request.
			if pending {
				resp.Reason = "a configuration change is in flight; retry"
			} else {
				meta := ev.meta
				if meta.Address == "" {
					meta.Address = m.Address
				}
				id := meta.ID
				cc := &pb.ConfChangeV2{Changes: []*pb.ConfChangeSingle{{Type: pb.ConfChangeRemoveNode.Enum(), NodeId: &id}}, Context: consensus.EncodeMeta(meta)}
				ctx, cancel := context.WithTimeout(context.Background(), c.o.Tick*5)
				err := n.ProposeConfChange(ctx, cc)
				cancel()
				if err != nil {
					resp.Reason = err.Error()
				} else {
					c.mu.Lock()
					c.pendingCC = true
					c.readmit[id] = meta
					c.mu.Unlock()
					resp.Reason = "member lost its log; being re-admitted, retry"
					c.audit("cluster.member.readmit", meta.Name, "proposed", nil)
				}
			}
		case joinAddNew:
			if pending {
				resp.Reason = "a configuration change is in flight; retry"
			} else {
				meta := ev.meta
				if meta.Address == "" {
					// The transport knows where the joiner is.
					for _, p := range c.o.Transport.Peers() {
						if p.Name == ev.from {
							meta.Address = p.Address
						}
					}
				}
				id := meta.ID
				cc := &pb.ConfChangeV2{Changes: []*pb.ConfChangeSingle{{Type: pb.ConfChangeAddLearnerNode.Enum(), NodeId: &id}}, Context: consensus.EncodeMeta(meta)}
				ctx, cancel := context.WithTimeout(context.Background(), c.o.Tick*5)
				err := n.ProposeConfChange(ctx, cc)
				cancel()
				if err != nil {
					resp.Reason = err.Error()
				} else {
					c.mu.Lock()
					c.pendingCC = true
					c.mu.Unlock()
					resp.Accepted = true
					c.audit("cluster.member.added", meta.Name, "ok", map[string]string{"via": "join"})
				}
			}
		}
	}
	b, _ := json.Marshal(resp)
	ctx, cancel := context.WithTimeout(context.Background(), c.o.Tick*5)
	_ = c.o.Transport.Send(ctx, ev.from, transport.Consensus, append([]byte{envJoinResponse}, b...))
	cancel()
}

// maybePromote, on the leader, promotes learners whose match index is
// within the promotion lag of the commit index, one change at a time.
func (c *Core) maybePromote(n raft.Node) {
	c.mu.Lock()
	leader := c.state == raft.StateLeader
	pending := c.pendingCC
	commit := c.commit
	c.mu.Unlock()
	if !leader {
		return
	}
	st := n.Status()
	for id, pr := range st.Progress {
		// Caught up means the follower is replicating (not probing or
		// receiving a snapshot) and its matched index is within the lag.
		caught := pr.Match > 0 && pr.State == tracker.StateReplicate && pr.Match+c.o.PromoteLag >= commit
		c.members.SetCaughtUp(id, caught)
		if pr.Match > 0 {
			c.mu.Lock()
			delete(c.readded, id)
			c.mu.Unlock()
		}
		if pr.IsLearner && caught && !pending && commit > 0 {
			m, ok := c.members.Lookup(id)
			if !ok {
				continue
			}
			cc := &pb.ConfChangeV2{Changes: []*pb.ConfChangeSingle{{Type: pb.ConfChangeAddNode.Enum(), NodeId: proto.Uint64(id)}}, Context: consensus.EncodeMeta(consensus.Meta{Name: m.Name, ID: id, Address: m.Address})}
			ctx, cancel := context.WithTimeout(context.Background(), c.o.Tick*5)
			err := n.ProposeConfChange(ctx, cc)
			cancel()
			if err == nil {
				c.mu.Lock()
				c.pendingCC = true
				c.mu.Unlock()
				pending = true
				c.audit("cluster.member.promoted", m.Name, "proposed", nil)
			}
		}
	}
}

// maybeSnapshot takes a snapshot when enough entries have been applied since
// the last one, then compacts the log behind a trailing window.
func (c *Core) maybeSnapshot() error {
	applied := c.machine.Applied()
	last := c.o.Store.SnapshotIndex()
	if applied < last+c.o.SnapshotEntries {
		return nil
	}
	data, err := c.machine.Snapshot()
	if err != nil {
		return err
	}
	c.mu.Lock()
	cs := c.confState
	term := c.term
	c.mu.Unlock()
	if cs == nil {
		cs = &pb.ConfState{}
	}
	if _, err := c.o.Store.CreateSnapshot(applied, cs, wrapSnapshot(applied, term, data)); err != nil {
		return fmt.Errorf("snapshot: %w", err)
	}
	c.count("hoardmq_raft_snapshots_total", map[string]string{"direction": "out"}, 1)
	if applied > c.o.SnapshotTrailing {
		if err := c.o.Store.Compact(applied - c.o.SnapshotTrailing); err != nil && !errors.Is(err, raft.ErrCompacted) {
			return fmt.Errorf("compact: %w", err)
		}
	}
	return nil
}
