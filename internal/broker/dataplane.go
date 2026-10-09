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

func (d dataPlane) QueryRetained(ns, filter string, now time.Time) []*message.Message {
	return d.repl.QueryRetained(ns, filter, now)
}
