package replication

import (
	"sync"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

// retainedFailures are the retained installs and deletes this node's journal
// did not take: a promotion's install, kept as a replica copy until made
// durable, and a former leader's delete of its own copy, made again. Both are
// in memory; after a restart the reconcile from no map runs the promotion
// again over the kept replica copy.
type retainedFailures struct {
	mu       sync.Mutex
	promoted map[failedKey]int
	deletes  map[failedKey]int
}

type failedKey struct{ ns, topic string }

// notePromotionFailed records a promoted copy whose install was not durable.
func (r *Replicator) notePromotionFailed(shard int, m *message.Message) {
	f := &r.rfailed
	f.mu.Lock()
	if f.promoted == nil {
		f.promoted = map[failedKey]int{}
	}
	f.promoted[failedKey{m.Namespace, m.Topic}] = shard
	f.mu.Unlock()
}

// promotionKept reports whether a promoted copy of the shard is kept for its
// install. The shard then refuses client writes as a moving shard does, so no
// write is acknowledged that a retry's install could replace.
func (r *Replicator) promotionKept(shard int) bool {
	f := &r.rfailed
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, s := range f.promoted {
		if s == shard {
			return true
		}
	}
	return false
}

// noteDeleteFailed records a delete of an own copy whose record was not
// durable, so a restart would bring the copy back.
func (r *Replicator) noteDeleteFailed(shard int, m *message.Message) {
	f := &r.rfailed
	f.mu.Lock()
	if f.deletes == nil {
		f.deletes = map[failedKey]int{}
	}
	f.deletes[failedKey{m.Namespace, m.Topic}] = shard
	f.mu.Unlock()
	r.count("hoardmq_retained_drop_failures_total", nil, 1)
	r.log.Warn("own retained copy's delete not durable; it is made again", "shard", shard, "topic", m.Topic)
}

// promotedCopyGoes decides a kept promoted copy at a retry: forgotten as a
// promotion when this node no longer leads the shard, the map's own rules
// then deciding the replica copy; otherwise merged again, and dropped from
// the replica store only once what the store holds for the topic, the copy or
// an entry that outranks it, is durable. While it is kept the shard refuses
// client writes (promotionKept). It is pure.
func promotedCopyGoes(leadsNow, durable bool) (drop, forget bool) {
	switch {
	case !leadsNow:
		return false, true
	case !durable:
		return false, false
	}
	return true, true
}

// retryOwnDelete decides whether a failed delete of an own copy is made
// again: only while this node does not lead the shard and holds no message
// for the topic, as a write since makes it the node's again. It is pure.
func retryOwnDelete(leadsNow, heldAny bool) bool { return !leadsNow && !heldAny }

// retryRetainedFailures makes the noted installs and deletes again, on the
// reconcile worker.
func (r *Replicator) retryRetainedFailures(cur shardmap.Map) {
	f := &r.rfailed
	f.mu.Lock()
	promoted := make(map[failedKey]int, len(f.promoted))
	for k, s := range f.promoted {
		promoted[k] = s
	}
	deletes := make(map[failedKey]int, len(f.deletes))
	for k, s := range f.deletes {
		deletes[k] = s
	}
	f.mu.Unlock()
	if len(promoted)+len(deletes) == 0 {
		return
	}
	leads := func(shard int) bool { return shard < len(cur.Shards) && cur.Shards[shard].Leader == r.o.Node }
	for k, shard := range promoted {
		var replica *store.RetainedEntry
		for _, e := range r.follower.retainedIn(shard) {
			if e.Msg.Namespace == k.ns && e.Msg.Topic == k.topic {
				replica = &e
			}
		}
		if replica == nil {
			r.forgetFailure(f.promoted, k)
			continue
		}
		durable := false
		if leads(shard) {
			// Merge, not replace: the held entry may outrank the copy; the
			// answer covers what is held, made durable first.
			var merged bool
			var heldRV store.Version
			merged, durable, heldRV = r.o.Sessions.InjectRetainedAt(replica.Msg, replica.RV, false)
			r.countMerge("retry", replica.Msg.Topic, merged, heldRV, replica.RV)
		}
		drop, forget := promotedCopyGoes(leads(shard), durable)
		if drop {
			if _, err := r.follower.apply(store.RetainRecord(&message.Message{Namespace: k.ns, Topic: k.topic})); err != nil {
				r.log.Error("replica retained drop failed", "err", err)
				continue
			}
			r.log.Debug("promoted retained copy installed; its replica copy dropped", "shard", shard, "topic", k.topic)
		}
		if forget {
			r.forgetFailure(f.promoted, k)
		}
	}
	for k, shard := range deletes {
		held := r.o.Local.RetainedFor(k.ns, k.topic, r.o.Clock())
		if !retryOwnDelete(leads(shard), len(held) > 0) {
			r.forgetFailure(f.deletes, k)
			continue
		}
		if durable, _ := r.o.Sessions.InjectRetained(&message.Message{Namespace: k.ns, Topic: k.topic}); durable {
			r.forgetFailure(f.deletes, k)
		}
	}
}

func (r *Replicator) forgetFailure(m map[failedKey]int, k failedKey) {
	r.rfailed.mu.Lock()
	delete(m, k)
	r.rfailed.mu.Unlock()
}
