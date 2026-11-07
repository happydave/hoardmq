package broker

import (
	"sort"
	"time"

	"github.com/happydave/hoardmq/ext"
)

// memberPoll is how often a node reads its view of the consensus membership
// for member.joined and member.left: the latency bound of those events.
const memberPoll = time.Second

// clusterEvent hands ev to the cluster listeners without waiting; with none
// configured it does nothing.
func (s *Server) clusterEvent(ev ext.ClusterEvent) {
	if s.registry == nil {
		return
	}
	h := s.registry.Hooks()
	if !h.Clustering() {
		return
	}
	if ev.Time.IsZero() {
		ev.Time = time.Now()
	}
	h.Cluster(ev)
}

// watchMembers raises member.joined and member.left from this node's view of
// the consensus membership, read every memberPoll. The first view that names
// anyone is the baseline and raises nothing.
func (s *Server) watchMembers() {
	defer s.wg.Done()
	t := time.NewTicker(memberPoll)
	defer t.Stop()
	var known map[string]bool
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
		}
		now := memberSet(s.members())
		if known == nil {
			if len(now) > 0 {
				known = now
			}
			continue
		}
		for _, ev := range memberChanges(known, now) {
			s.clusterEvent(ev)
		}
		known = now
	}
}

// memberSet is a view of the membership by name. A member whose name this
// node has not learned yet is left out, so it is reported once, by name.
func memberSet(names []string) map[string]bool {
	out := make(map[string]bool, len(names))
	for _, n := range names {
		if n != "" {
			out[n] = true
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// memberChanges lists the joins, then the departures, from before to after,
// each in name order.
func memberChanges(before, after map[string]bool) []ext.ClusterEvent {
	var joined, left []string
	for n := range after {
		if !before[n] {
			joined = append(joined, n)
		}
	}
	for n := range before {
		if !after[n] {
			left = append(left, n)
		}
	}
	sort.Strings(joined)
	sort.Strings(left)
	out := make([]ext.ClusterEvent, 0, len(joined)+len(left))
	for _, n := range joined {
		out = append(out, ext.ClusterEvent{Kind: ext.MemberJoined, Member: n})
	}
	for _, n := range left {
		out = append(out, ext.ClusterEvent{Kind: ext.MemberLeft, Member: n})
	}
	return out
}
