// Package consensus is the in-house consensus interface: commands applied in
// log order by a replicated state machine made of registered components,
// snapshots in a format the project owns, membership operations and a
// leader hint. Nothing here depends on the Raft library; the shell that
// wires it in lives in the etcdraft subpackage.
package consensus

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
)

// Kind identifies a command and the component that applies it.
type Kind byte

// Kinds registered by this package. Later work items add their own above
// KindReserved.
const (
	KindConfig Kind = 1
	KindEpoch  Kind = 2

	// KindReserved is the first kind available to other components.
	KindReserved Kind = 16
)

// Component is one section of the replicated state machine: it applies the
// commands of its kind and serialises its state for snapshots.
type Component interface {
	// Apply applies one committed command at the given log index and
	// returns the result handed back to the proposer.
	Apply(index uint64, payload []byte) (any, error)
	// Snapshot serialises the component's state.
	Snapshot() ([]byte, error)
	// Restore replaces the component's state from a snapshot section.
	Restore(data []byte) error
}

// Recoverable is implemented by a component whose applied changes have
// outward effects (observers, hooks). While the machine is recovering — a
// restart replaying committed history, or a snapshot rebuilding the state —
// the component applies silently; Recovered is called once when recovery
// ends so it can reconcile against the final state. An observer that acted
// on every replayed change would act on states the node had already acted
// on before it stopped, and undo work it had already done.
type Recoverable interface {
	SetRecovering(bool)
	Recovered()
}

// Role of a member.
type Role string

// Member roles.
const (
	Voter   Role = "voter"
	Learner Role = "learner"
)

// Member is one node of the cluster as the state machine knows it.
type Member struct {
	Name     string
	ID       uint64
	Address  string
	Role     Role
	CaughtUp bool // leader's view: match index within the promotion lag of the commit
}

// Status is a snapshot of the core.
type Status struct {
	Term          uint64
	Commit        uint64
	Applied       uint64
	SnapshotIndex uint64
	Leader        string
	Role          string // leader, follower, candidate, learner, joining, removed, stopped
	Bootstrapped  bool   // this node created the group
	Joined        bool   // this node is a member and caught up
	// Raft counts its election and check-quorum timeouts in ticks, so a
	// driver that loses ticks under load has a timeout that no longer means
	// what it says. TicksOwed against Ticks is how much wall time the clock
	// actually followed; TicksMaxRun is the largest gap it had to make up,
	// which is the figure that says whether a run was starved.
	Ticks        uint64
	TicksOwed    uint64
	TicksDropped uint64
	TicksMaxRun  uint64
}

// Core is what the cluster layers build on.
type Core interface {
	// Propose replicates a command and returns its component's result once
	// applied on this node. On a follower it fails with a NotLeaderError.
	Propose(ctx context.Context, kind Kind, payload []byte) (any, error)
	// AddMember adds a node as a learner; PromoteMember makes a caught-up
	// learner a voter; RemoveMember removes a member.
	AddMember(ctx context.Context, name, address string) error
	PromoteMember(ctx context.Context, name string) error
	RemoveMember(ctx context.Context, name string) error
	// Leader is the current leader's name, if known.
	Leader() (string, bool)
	// Members lists the membership.
	Members() []Member
	// Status snapshots the core.
	Status() Status
	// Ready reports whether this node is a member that has caught up with
	// the leader.
	Ready() bool
	// Recovering reports whether the machine is replaying committed history
	// or rebuilding from a snapshot; components act on nothing until it ends.
	Recovering() bool
	// Stop ends the node loop and closes the store.
	Stop()
}

// NotLeaderError is returned by a proposal on a node that is not the
// leader; Leader is the hint, empty when unknown.
type NotLeaderError struct {
	Leader string
}

func (e *NotLeaderError) Error() string {
	if e.Leader == "" {
		return "consensus: not the leader, and no leader is known"
	}
	return fmt.Sprintf("consensus: not the leader; the leader is %s", e.Leader)
}

// Errors from the core.
var (
	ErrStopped      = errors.New("consensus: core stopped")
	ErrRemoved      = errors.New("consensus: this node was removed from the cluster")
	ErrUnknownKind  = errors.New("consensus: no component for this command kind")
	ErrDropped      = errors.New("consensus: proposal dropped")
	ErrIDCollision  = errors.New("consensus: node id collides with an existing member")
	ErrNotMember    = errors.New("consensus: no such member")
	ErrAlreadyVoter = errors.New("consensus: member is already a voter")
)

// NodeID derives a node's Raft id from its name.
func NodeID(name string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(name))
	id := h.Sum64()
	if id == 0 {
		// Zero is the library's "no node".
		return 1
	}
	return id
}
