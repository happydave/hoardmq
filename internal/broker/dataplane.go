package broker

import (
	"context"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/replication"
	"github.com/happydave/hoardmq/internal/cluster/routing"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

// dataPlane is what the session manager sees as its replication layer: the
// replicator's barrier and retained store, plus the routing plane's forward
// barrier, so a QoS 1 or 2 acknowledgement also waits for the nodes the
// message was forwarded to.
type dataPlane struct {
	repl    *replication.Replicator
	routes  *routing.Plane
	timeout time.Duration
}

func (d dataPlane) Barrier(ctx context.Context, keys []store.Scope, forwards []message.ForwardRef) error {
	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()
	if err := d.repl.Barrier(ctx, keys); err != nil {
		return err
	}
	if d.routes != nil {
		return d.routes.Barrier(ctx, forwards)
	}
	return nil
}

func (d dataPlane) Durable(ctx context.Context, keys []store.Scope) error {
	return d.repl.Barrier(ctx, keys)
}

func (d dataPlane) StoreRetained(msg *message.Message) bool { return d.repl.StoreRetained(msg) }

// StoreRetainedFrom names the write's source, so a forward no barrier waits
// on is held for a misrouted refusal.
func (d dataPlane) StoreRetainedFrom(msg *message.Message, source string) bool {
	return d.repl.StoreRetainedFrom(msg, source)
}

// StoreRetainedAt names the map version a write stored here is stamped with:
// without it every client write and admin delete would be stored unversioned.
func (d dataPlane) StoreRetainedAt(msg *message.Message, source string) (bool, uint64) {
	return d.repl.StoreRetainedAt(msg, source)
}

// LeadsRetained lets the expiry sweep leave a stamped tombstone at the
// shard's leader.
func (d dataPlane) LeadsRetained(ns, topic string) (uint64, bool) {
	return d.repl.LeadsRetained(ns, topic)
}

// DropEligibleTombstones lets the retained-topic limit make room for a clear.
func (d dataPlane) DropEligibleTombstones(ns string) int { return d.repl.DropEligibleTombstones(ns) }

func (d dataPlane) QueryRetained(ns, filter string, now time.Time) []*message.Message {
	return d.repl.QueryRetained(ns, filter, now)
}
