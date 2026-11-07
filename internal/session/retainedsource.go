package session

import "github.com/happydave/hoardmq/internal/message"

// The sources of a retained write the manager names to its Replication. They
// match the replication package's: only a client's QoS 1 or 2 publish has a
// barrier waiting on a forward to the shard's leader.
const (
	sourceClient     = "client"
	sourceClientQoS0 = "client-qos0"
	sourceWill       = "will"
	sourceModule     = "module"
)

// sourcedRetained is a Replication told each retained write's source, so it
// can hold a forward no barrier waits on until it is answered.
type sourcedRetained interface {
	StoreRetainedFrom(msg *message.Message, source string) (local bool)
}

// retainedLeader is a Replication that names the map version under which this
// node leads a retained topic's shard, for the expiry sweep's tombstones.
type retainedLeader interface {
	LeadsRetained(ns, topic string) (epoch uint64, ok bool)
}

// tombstoneDropper is a Replication that can drop, at once, a namespace's
// tombstones that may go, for a write the retained-topic limit refused.
type tombstoneDropper interface {
	DropEligibleTombstones(ns string) int
}

// versionedRetained is a Replication that also names the map version under
// which this node leads the write's shard, so the write is stamped with it.
type versionedRetained interface {
	StoreRetainedAt(msg *message.Message, source string) (local bool, epoch uint64)
}

// FullReplication is a Replication with every optional method the manager
// type-asserts its replication value for. A value without one is still
// accepted, and the manager quietly does without it, so an adapter between
// the replicator and the manager is checked against this.
type FullReplication interface {
	Replication
	sourcedRetained
	retainedLeader
	tombstoneDropper
	versionedRetained
}

// storeRetainedAt asks r about a retained write: whether to store it here,
// and the epoch to stamp it with (0 for an unversioned store: no map, or a
// Replication that does not name one).
func storeRetainedAt(r Replication, msg *message.Message, source string) (bool, uint64) {
	if v, ok := r.(versionedRetained); ok {
		return v.StoreRetainedAt(msg, source)
	}
	return storeRetained(r, msg, source), 0
}

// storeRetained asks r about a retained write, naming its source when r takes
// one.
func storeRetained(r Replication, msg *message.Message, source string) bool {
	if s, ok := r.(sourcedRetained); ok {
		return s.StoreRetainedFrom(msg, source)
	}
	return r.StoreRetained(msg)
}
