package replication

import "github.com/happydave/hoardmq/internal/cluster/shardmap"

// Short reports where this node's work has fewer copies than the map's
// replica count asks for under the current map: the shards on which a
// session it owns has fewer than replicas-1 targets, and how many of the
// shards it leads have fewer than replicas-1 followers for their retained
// messages. Both are empty before the first map.
func (r *Replicator) Short() (sessions map[int]bool, retainedLed int) {
	return shortShards(r.o.Map.Current(), r.o.Node)
}

// shortShards is Short's decision over one map.
func shortShards(m shardmap.Map, self string) (sessions map[int]bool, retainedLed int) {
	want := m.Replicas - 1
	if m.Version == 0 || want <= 0 {
		return nil, 0
	}
	for i, s := range m.Shards {
		if len(m.Targets(i, self)) < want {
			if sessions == nil {
				sessions = map[int]bool{}
			}
			sessions[i] = true
		}
		if s.Leader == self && len(s.Followers) < want {
			retainedLed++
		}
	}
	return sessions, retainedLed
}
