package replication

import (
	"sort"
	"sync"

	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/topic"
)

// presence is what lets a subscribe answer "does any peer hold a retained
// message matching this filter?" without asking.
//
// Each node states which shards it holds retained messages for and leads;
// a node that has heard such a statement over a peer's current link may
// skip that peer's query when the filter's shard is not in it. The rule is
// sound in one direction only: a shard's absence from a current statement
// means the peer holds nothing there, while no statement at all, or one
// made over a link that has since changed state, means ask.
//
// The set only ever grows from the tap, and shrinks only on a recompute
// from the node's own retained store. A shard that has lost its last
// retained message therefore lingers in the set, which costs a query and
// never a message.
type presence struct {
	mu  sync.Mutex
	own map[int]bool
	// gen changes whenever own changes, so a peer told generation g has
	// been told that exact set.
	gen   uint64
	peers map[string]map[int]bool // by peer; a missing key is "unknown"
	told  map[string]uint64       // by peer, on the peer's current link
	// removed records that a retained message was deleted since the last
	// recompute, so a broker that never deletes one never pays for the scan.
	removed bool
}

// Generations start at one so that a peer's missing entry in told — zero —
// means "never told" rather than "told the empty set".
func newPresence() *presence {
	return &presence{gen: 1, own: map[int]bool{}, peers: map[string]map[int]bool{}, told: map[string]uint64{}}
}

func sortedShards(set map[int]bool) []int {
	out := make([]int, 0, len(set))
	for s := range set {
		out = append(out, s)
	}
	sort.Ints(out)
	return out
}

// notePresence records a retained record for a shard this node leads. It
// runs inside the store's lock, so the common case — a shard already in the
// set — is one map read under one mutex and nothing else.
//
// A shard new to the set is announced to every live peer before this
// returns, which is what makes the negative sound: the tap runs before the
// barrier on both retained-write paths (Manager.dispatch's local store, and
// applyRetainedWrite's forward), and Barrier waits for every peer's
// outbound, so no peer can still believe the shard is empty by the time the
// write is acknowledged to its publisher.
func (r *Replicator) notePresence(shard int, removal bool) {
	if removal {
		r.pres.mu.Lock()
		r.pres.removed = true
		r.pres.mu.Unlock()
		return
	}
	r.pres.mu.Lock()
	if r.pres.own[shard] {
		r.pres.mu.Unlock()
		return
	}
	r.pres.own[shard] = true
	r.pres.gen++
	r.pres.mu.Unlock()
	r.announcePresence()
}

// announcePresence sends the current set to every live peer that has not
// been told it over its current link. Its postcondition is what notePresence
// needs and is unconditional: on return, every peer that was up has been
// sent the set it had not been told.
//
// It reads the set per peer rather than taking one as an argument, so a
// generation that moves on mid-pass changes what is sent and never whether
// it is sent. An earlier version took the set as an argument and abandoned
// the pass when it found the generation had moved, which weakens the
// postcondition to "unless someone else was announcing at the time" — and
// the caller cannot use that, because its own write is acknowledged on the
// strength of the frames this pushed. Sending the newer set instead is
// always at least as good: the set only grows from the tap, and the one path
// that shrinks it abandons when a tap ran during its walk, so any set at a
// generation at or beyond the caller's still contains the caller's shard.
func (r *Replicator) announcePresence() {
	if r.o.Transport == nil {
		return
	}
	for _, p := range r.o.Transport.Peers() {
		if p.State != transport.Up {
			continue
		}
		r.pres.mu.Lock()
		set, gen := sortedShards(r.pres.own), r.pres.gen
		told := r.pres.told[p.Name]
		r.pres.mu.Unlock()
		if told == gen {
			continue
		}
		r.sendPresence(p.Name, set, gen)
	}
}

// sendPresence queues the set for one peer and records that it was told.
func (r *Replicator) sendPresence(peer string, set []int, gen uint64) {
	r.outboundFor(peer).push(frame{Kind: kindRetainedPresence, Shards: set})
	r.pres.mu.Lock()
	if r.pres.gen == gen {
		r.pres.told[peer] = gen
	}
	r.pres.mu.Unlock()
	r.count("hoardmq_retained_presence_total", map[string]string{"direction": "out"}, 1)
}

// applyPresence records a peer's statement about itself.
func (r *Replicator) applyPresence(peer string, shards []int) {
	set := make(map[int]bool, len(shards))
	for _, s := range shards {
		set[s] = true
	}
	r.pres.mu.Lock()
	r.pres.peers[peer] = set
	r.pres.mu.Unlock()
	r.count("hoardmq_retained_presence_total", map[string]string{"direction": "in"}, 1)
}

// forgetPresence drops what this node knew about a peer, and what the peer
// was told, because the link went away: the peer may gain retained messages
// while it is gone, and has not necessarily kept what it was told.
func (r *Replicator) forgetPresence(peer string) {
	r.pres.mu.Lock()
	delete(r.pres.peers, peer)
	delete(r.pres.told, peer)
	r.pres.mu.Unlock()
}

// forgetTold drops only what the peer was told, so the set is announced
// again over a link that has just come up.
func (r *Replicator) forgetTold(peer string) {
	r.pres.mu.Lock()
	delete(r.pres.told, peer)
	r.pres.mu.Unlock()
}

// mayHold reports whether the peer could hold a retained message matching
// the filter, and so whether a query is worth sending. Anything short of a
// current statement that rules it out answers true.
func (r *Replicator) mayHold(peer, ns, filter string) bool {
	r.pres.mu.Lock()
	defer r.pres.mu.Unlock()
	set, known := r.pres.peers[peer]
	if !known {
		return true
	}
	if topic.HasWildcard(filter) {
		// A wildcard filter has no single shard, so the only sound negative
		// is that the peer holds nothing at all.
		return len(set) > 0
	}
	return set[r.o.Map.ShardOf(ns, filter)]
}

// refreshPresence re-announces the set to any live peer that has not been
// told it over its current link, so a missed peer-up event repairs itself
// rather than leaving the peer querying forever. It first recomputes the
// set if a retained message has been removed since the last pass.
func (r *Replicator) refreshPresence() {
	if r.o.Transport == nil {
		return
	}
	r.recomputePresence(false)
	r.announcePresence()
}

// recomputePresence rebuilds the set from this node's own retained store
// and the current map. It is the only path that removes a shard.
//
// force recomputes even without a removal, for a map change: leadership may
// have moved under messages this node already held.
func (r *Replicator) recomputePresence(force bool) {
	r.pres.mu.Lock()
	if !force && !r.pres.removed {
		r.pres.mu.Unlock()
		return
	}
	r.pres.removed = false
	before := r.pres.gen
	r.pres.mu.Unlock()

	m := r.o.Map.Current()
	if m.Version == 0 {
		return
	}
	next := map[int]bool{}
	for _, msg := range r.o.Local.AllRetained() {
		s := r.o.Map.ShardOf(msg.Namespace, msg.Topic)
		if m.Shards[s].Leader == r.o.Node {
			next[s] = true
		}
	}

	r.pres.mu.Lock()
	defer r.pres.mu.Unlock()
	if r.pres.gen != before {
		// The tap added a shard while the store was being walked, so this
		// answer may already be missing it. Abandoning is the safe
		// direction: the set keeps the extra shard, which costs a query.
		r.pres.removed = true
		return
	}
	if sameShards(r.pres.own, next) {
		return
	}
	r.pres.own = next
	r.pres.gen++
}

func sameShards(a, b map[int]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for s := range a {
		if !b[s] {
			return false
		}
	}
	return true
}
