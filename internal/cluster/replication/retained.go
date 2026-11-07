package replication

import (
	"context"
	"sync"
	"time"

	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/topic"
)

// StoreRetained implements session.Replication: a retained write is stored
// locally when this node leads the topic's shard (the tap then replicates
// it to the followers) and otherwise handed to the leader, whose
// acknowledgement the barrier includes. A leader the transport reports down
// is counted and sent nothing, and nothing is stored here: the barrier
// refuses the write, and a copy left on a node outside the shard would be
// served, over newer writes, once that node led it.
func (r *Replicator) StoreRetained(msg *message.Message) bool {
	return r.StoreRetainedFrom(msg, SourceClient)
}

// StoreRetainedFrom is StoreRetained for a write whose source the session
// manager names. A forward from a source no barrier waits on (SourceWill,
// SourceModule, SourceClientQoS0) is held until it is answered, so a
// misrouted refusal can be forwarded again (misrouted.go).
func (r *Replicator) StoreRetainedFrom(msg *message.Message, source string) bool {
	local, _ := r.StoreRetainedAt(msg, source)
	return local
}

// LeadsRetained reports the map version under which this node leads a
// retained topic's shard, for the expiry sweep's tombstones: ok is false when
// it does not lead it, or there is no map.
func (r *Replicator) LeadsRetained(ns, topic string) (uint64, bool) {
	m := r.o.Map.Current()
	if m.Version == 0 || r.o.Transport == nil {
		return 0, false
	}
	shard := r.o.Map.ShardOf(ns, topic)
	if shard < 0 || shard >= len(m.Shards) || m.Shards[shard].Leader != r.o.Node {
		return 0, false
	}
	return m.Version, true
}

// StoreRetainedAt is StoreRetainedFrom naming, for a write stored here, the
// version of the map snapshot that decided this node leads the shard: the
// epoch the write is stamped with. It is 0 under no map, or no leader, when
// the write is stored unversioned.
func (r *Replicator) StoreRetainedAt(msg *message.Message, source string) (local bool, epoch uint64) {
	m := r.o.Map.Current()
	if m.Version == 0 || r.o.Transport == nil {
		return true, 0
	}
	shard := r.o.Map.ShardOf(msg.Namespace, msg.Topic)
	leader := m.Shards[shard].Leader
	switch leader {
	case r.o.Node:
		return true, m.Version
	case "":
		return true, 0
	}
	if !r.up(leader) {
		r.count("hoardmq_retained_forwards_total", map[string]string{"outcome": "leader-down"}, 1)
		return false, 0
	}
	seq := r.outboundFor(leader).push(frame{Kind: kindRetainedWrite, Namespace: msg.Namespace, Topic: msg.Topic, Epoch: m.Version, Msg: msg})
	r.count("hoardmq_retained_forwards_total", map[string]string{"outcome": "forwarded"}, 1)
	if source != SourceClient {
		r.holdForward(leader, seq, msg, source, false)
	}
	return false, 0
}

// QueryRetained implements session.Replication: the union of every shard
// leader's matches. A leader that is down or late is skipped and counted;
// the subscriber gets what the rest hold. A leader whose retained-presence
// statement rules the filter out is not asked at all (see presence.go).
func (r *Replicator) QueryRetained(ns, filter string, now time.Time) []*message.Message {
	m := r.o.Map.Current()
	if m.Version == 0 || r.o.Transport == nil {
		return r.o.Local.RetainedFor(ns, filter, now)
	}
	var leaders []string
	if topic.HasWildcard(filter) {
		leaders = m.Leaders()
	} else {
		leaders = []string{m.Shards[r.o.Map.ShardOf(ns, filter)].Leader}
	}
	out := r.localRetained(ns, filter, now)
	var mu sync.Mutex
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(r.ctx, r.o.AckTimeout)
	defer cancel()
	for _, l := range leaders {
		if l == r.o.Node || l == "" {
			continue
		}
		if !r.mayHold(l, ns, filter) {
			// Tested before up(), which walks the transport's peer list: at
			// a hundred thousand simultaneous subscribes that walk is itself
			// worth not doing.
			r.count("hoardmq_retained_queries_total", map[string]string{"direction": "out", "outcome": "skipped"}, 1)
			continue
		}
		if !r.up(l) {
			r.count("hoardmq_retained_queries_total", map[string]string{"direction": "out", "outcome": "leader-down"}, 1)
			continue
		}
		wg.Add(1)
		go func(l string) {
			defer wg.Done()
			ans, ok := r.request(ctx, l, frame{Kind: kindRetainedQuery, Namespace: ns, Filter: filter})
			outcome := "ok"
			if !ok {
				outcome = "timeout"
			}
			r.count("hoardmq_retained_queries_total", map[string]string{"direction": "out", "outcome": outcome}, 1)
			if ok {
				mu.Lock()
				out = append(out, ans.Msgs...)
				mu.Unlock()
			}
		}(l)
	}
	wg.Wait()
	return out
}

// localRetained returns this node's retained matches restricted to the
// shards it leads; everything before the first map.
func (r *Replicator) localRetained(ns, filter string, now time.Time) []*message.Message {
	all := r.o.Local.RetainedFor(ns, filter, now)
	m := r.o.Map.Current()
	if m.Version == 0 {
		return all
	}
	out := make([]*message.Message, 0, len(all))
	for _, msg := range all {
		if m.Shards[r.o.Map.ShardOf(msg.Namespace, msg.Topic)].Leader == r.o.Node {
			out = append(out, msg)
		}
	}
	return out
}
