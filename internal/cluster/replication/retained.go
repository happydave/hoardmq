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
// falls back to a local store, counted, so the write is not lost.
func (r *Replicator) StoreRetained(msg *message.Message) bool {
	m := r.o.Map.Current()
	if m.Version == 0 || r.o.Transport == nil {
		return true
	}
	shard := r.o.Map.ShardOf(msg.Namespace, msg.Topic)
	leader := m.Shards[shard].Leader
	if leader == r.o.Node || leader == "" {
		return true
	}
	if !r.up(leader) {
		r.count("hoardmq_retained_forwards_total", map[string]string{"outcome": "leader-down"}, 1)
		return true
	}
	r.outboundFor(leader).push(frame{Kind: kindRetainedWrite, Namespace: msg.Namespace, Topic: msg.Topic, Epoch: m.Version, Msg: msg})
	r.count("hoardmq_retained_forwards_total", map[string]string{"outcome": "forwarded"}, 1)
	return false
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
