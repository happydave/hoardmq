package broker

import (
	"context"
	"os"
	"time"

	"github.com/happydave/hoardmq/internal/conn"
)

// Hooks are the simulation harness's seams into a node: an injected clock
// for the lease and replication layers, an injected durability call for
// every log, and injected defects that disable one protective mechanism
// each so the harness can prove its properties catch the failure. None is
// reachable from configuration; New passes an empty Hooks.
type Hooks struct {
	Clock func() time.Time
	// Steady is the lease service's own elapsed time (lease.Options.Steady):
	// a harness that skews Clock leaves this alone, as a wrong wall clock
	// leaves a machine's monotonic one.
	Steady func() time.Time
	Sync   func(*os.File) error
	// AdoptionDelay holds the lease service's adoption worker back, so a
	// scenario can have a client reach the adopting node before it.
	AdoptionDelay time.Duration
	// TakeoverDelay holds the lease service's takeover passes back, so a test
	// can make this node a previous owner whose answer comes too late.
	TakeoverDelay time.Duration
	// BuildLevel stands the node in for a build that understands only this
	// cluster feature level; zero is the build's own.
	BuildLevel uint64
	// ImportGate is called between a session's fetch and its import, by the
	// lease service's adoption worker and by a CONNECT installing a takeover's
	// state, so a test can hold an import there.
	ImportGate func(ns, id string)
	// AcquireGate is the lease service's: called by a CONNECT's acquire
	// after its lookup and before it proposes, and again once granted.
	AcquireGate func(ns, id string, granted bool)
	// DrainGate is called by a draining node after it has handed its leases
	// off and before it waits for their confirmations, so a test can hold the
	// drain there.
	DrainGate func()
	// LoadGate is called by a starting node once its cluster transport and
	// routing plane run and before it loads its store, so a test can hold a
	// restart there.
	LoadGate func(node string)
	// StartGate is called by a starting node once its store's state is
	// loaded and before its consensus core, lease service and replication
	// open, so a test can hold a restart there.
	StartGate func(node string)
	// DrainHandoff is called by a draining node just before each attempt at
	// its hand-off, numbered from 1.
	DrainHandoff func(attempt int)
	// DrainHandedOff is called by a draining node once its hand-off has
	// applied and before it waits to leave the shard map.
	DrainHandedOff func()
	// ForwardApplyGate is called by the routing plane before it applies a
	// batch of forwarded frames from peer, so a test can hold them there.
	ForwardApplyGate func(node, peer string)
	// HoldAdmissions, while it returns true, keeps this node as consensus
	// leader from admitting a joiner (etcdraft.Options.HoldAdmissions).
	HoldAdmissions func() bool
	// Stress, when set, gives a node's stress level, by name, in place of the one its
	// indicators give; OverloadInterval and BanSweepInterval replace the
	// controller's one second and the ban sweeper's ten.
	Stress           func(node string) uint8
	OverloadInterval time.Duration
	BanSweepInterval time.Duration
	// FramingVersion, when set and non-zero for a node, is the highest
	// transport framing version it advertises, standing it in for an older
	// build.
	FramingVersion func(node string) uint8
	// AdminForwardGate is called on the owner before it runs a forwarded
	// admin request, so a test can hold it there; AdminForwardTimeout
	// replaces the caller's wait for an answer.
	AdminForwardGate    func(node string)
	AdminForwardTimeout time.Duration
	// AdminAnswerLimit, when set, replaces the frame limit an owner checks a
	// forwarded request's answer against.
	AdminAnswerLimit int
	// AdminSearchGate is called by a client search once it may walk the
	// lease table, before it does.
	AdminSearchGate func()
	// AdminAppliedGate is called on a node asked whether it has applied an
	// index, before it answers, so a test can hold the answer;
	// AdminWriteTimeout replaces the admin API's write timeout.
	AdminAppliedGate  func(node string)
	AdminWriteTimeout time.Duration
	Defects           Defects
}

// framingVersion is the framing version a node advertises: the hook's,
// or zero for the build's.
func framingVersion(h Hooks, node string) uint8 {
	if h.FramingVersion == nil {
		return 0
	}
	return h.FramingVersion(node)
}

// Defects are the harness's injected defects: each switch builds a broker
// with a known fault in it, so a property can be shown to catch it. Each
// names the mechanism it disables.
type Defects struct {
	SkipEpochCheck           bool // followers apply stale-epoch replication records
	SendBeforeSave           bool // the raft shell sends before its entries are durable
	NoBarrier                bool // QoS 1 and 2 acknowledged after the local sync only
	MinorityServes           bool // a fenced node keeps granting leases
	ReorderForwards          bool // the transport swaps adjacent frames
	IgnoreChecksum           bool // a log record with a bad checksum is read as sound
	ForgetInbound            bool // a repeated QoS 2 PUBLISH is routed twice
	PubrelUnreplicated       bool // the PUBREL goes out before the replicas hold the PUBREC state
	AdmitDuringAdoption      bool // a CONNECT is admitted while its lease's adoption is unfinished
	AdmitUnansweredFetch     bool // a session fetch that went unanswered is taken as "held nowhere"
	DiscardLateState         bool // an answer carrying a session that arrives after its pull gave up is dropped
	SyncBeforeLoad           bool // a starting node sends its route set before its store is loaded
	ForwardsBeforeServing    bool // a restarted node takes forwarded deliveries before its replication is attached and its core has recovered
	RemoveOnHandOver         bool // a takeover pass removes the session and journals its delete at once instead of sealing it
	IgnoreReplicationGaps    bool // a follower applies frames past a gap in the owner's sequence without reporting it
	DeleteWithoutHandoverAck bool // a node deletes a replica it hands over without the new holder's acknowledgement
	IgnoreForwardGaps        bool // a receiver applies forwarded frames past a gap in the sender's sequence without reporting it
	AcknowledgeUnapplied     bool // a follower acknowledges a replicated record it could not apply instead of refusing it
	HandOverOwnedCopies      bool // an owner keeps, and hands over as its own, a stale replica copy of a session it owns
	KeepUnfedCopies          bool // a node keeps, and accepts, a replica copy of a session it is not a target for
	DropMovedRetained        bool // a node that stops holding a retained shard drops its copies before the new holders hold them
	KeepRetainedNotHandedOn  bool // a kept retained shard whose releaser is gone is not handed to the shard's leader
	IgnoreOwedHandOver       bool // a release drops a node's retained copies while it still owes the sender a hand-over of them
	NoHandOverRetry          bool // a follower's unacknowledged retained hand-over for a dead leader is not made again
	NoEarlyReleaseDrop       bool // a release kept for an owed hand-over is never acted on
	NoHandOverAtLeaderChange bool // a shard's copies go to a new leader only when it held nothing of the shard
	ImportPastLaterGrant     bool // a fetched session is installed although a later grant for its client has been seen
	DrainSkipsConfirmations  bool // a drain completes when its hand-off commits, without waiting for its sessions to be confirmed held by their receiver
}

// Crash stops the node the way a power loss would: listeners and client
// connections dropped without a DISCONNECT, every component stopped, no
// final checkpoint of any store. What the logs hold is what a restart
// recovers.
func (s *Server) Crash() {
	if !s.started {
		return
	}
	s.started = false
	s.ready.Store(false)
	s.record("server", "server.crash", s.config().Node.Name, "injected", nil)
	for _, l := range s.listeners {
		_ = l.Close()
	}
	s.mu.Lock()
	conns := make([]*conn.Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()
	for _, c := range conns {
		c.Abort()
	}
	// The network goes first, as it does when the power does: nothing this
	// node says after this point reaches anyone, so no acknowledgement can
	// leave for a copy that is about to die with the process.
	if s.cluster != nil {
		s.cluster.Stop()
	}
	if s.topology != nil {
		s.topology.close()
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
	s.sessions.Close()
	close(s.checkpts)
	s.cancel()
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
	}
	if s.replica != nil {
		_ = s.replica.Close()
	}
	_ = s.registry.Stop(context.Background())
	if a := s.audit.Load(); a != nil {
		_ = a.Close()
	}
}
