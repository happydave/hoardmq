package routing

import (
	"slices"
	"time"

	"github.com/happydave/hoardmq/internal/topic"
)

// A move is refused for its grace because this node cannot see whether a
// publish reached the moved session at its new owner. Once the new owner has
// confirmed it holds the session durably (Resolve), two paths carry that
// proof and stop being refused before the grace ends:
//
//   - a publish made here and forwarded to the new owner after the
//     confirmation was acted on: it arrives there after the session was held;
//   - a forward from the new owner whose message sequence is at or above the
//     mark its confirmation carried: the new owner routed it locally after it
//     held the session.
//
// A forward from any other node carries no proof, since that node routes by
// its own table, and is refused for the grace as before.

// clientMove is one client's move of one filter away from this node.
type clientMove struct {
	until time.Time
	// owner is the node that confirmed it holds the session, and mark the
	// first publish sequence that node routed with the session held, as its
	// confirmation reported; owner is empty until then, and mark zero when
	// the confirmation carried none.
	owner string
	mark  uint64
}

type clientKey struct{ ns, id string }

// moveView is what a decision reads of one moving filter matching a topic.
type moveView struct {
	filter string
	moves  []clientMove
}

// publishPath is how a publish reached this node's check: made here and
// forwarded to targets, or forwarded here by peer with the origin's sequence.
type publishPath struct {
	forwarded bool
	targets   []string
	peer      string
	seq       uint64
}

// moveProven decides whether a QoS 1 or 2 publish matching moving filters is
// shown to have reached every moved session. It is pure: accepting is what
// lets a publish be acknowledged while its topic is moving. known reports
// whether this node holds a live, visible entry of an owner for a filter.
func moveProven(views []moveView, path publishPath, now time.Time, known func(owner, filter string) bool) bool {
	if len(views) == 0 {
		return false
	}
	for _, v := range views {
		if share, _ := topic.SplitShared(v.filter); share != "" {
			// A group delivery goes through the group's dispatch, not the
			// direct targets, so nothing here shows where it went.
			return false
		}
		considered := 0
		for _, m := range v.moves {
			if !m.until.After(now) {
				continue
			}
			considered++
			if m.owner == "" || !known(m.owner, v.filter) {
				return false
			}
			if path.forwarded {
				if path.peer != m.owner || m.mark == 0 || path.seq < m.mark {
					return false
				}
			} else if !slices.Contains(path.targets, m.owner) {
				return false
			}
		}
		if considered == 0 {
			return false
		}
	}
	return true
}

// MovingClient is Moving for one client's session, so that a confirmation
// that the client's session is held elsewhere can be applied to its filters.
func (p *Plane) MovingClient(ns, id string, filters []string) {
	p.recordMove(ns, id, filters)
}

// Resolve records that owner confirmed it holds the client's session durably,
// with mark the first publish sequence it routed with the session held. It applies to the client's
// moves still inside their grace and changes nothing else.
func (p *Plane) Resolve(ns, id, owner string, mark uint64) {
	if owner == "" || owner == p.o.Node {
		return
	}
	now := time.Now()
	n := 0
	p.mu.Lock()
	for rid := range p.byClient[clientKey{ns, id}] {
		if m := p.moves[rid][id]; m != nil && m.until.After(now) {
			m.owner, m.mark = owner, mark
			n++
		}
	}
	p.mu.Unlock()
	if n > 0 {
		p.count("hoardmq_moves_resolved_total", nil, 1)
	}
}

// moveViews reads the moving filters matching a topic with their clients'
// moves. It is called only when MovingMatch holds, and finds the filters
// through the moving index rather than by walking every move.
func (p *Plane) moveViews(ns, name string) []moveView {
	filters := p.table.MovingFilters(ns, name)
	if len(filters) == 0 {
		return nil
	}
	out := make([]moveView, 0, len(filters))
	p.mu.Lock()
	for _, f := range filters {
		v := moveView{filter: f}
		for _, m := range p.moves[routeID{ns, f}] {
			v.moves = append(v.moves, *m)
		}
		out = append(out, v)
	}
	p.mu.Unlock()
	return out
}

// proven applies moveProven with this node's route table.
func (p *Plane) proven(ns string, views []moveView, path publishPath) bool {
	return moveProven(views, path, time.Now(), func(owner, filter string) bool {
		return p.table.Known(owner, ns, filter)
	})
}

// clearMovesLocked forgets the clients' moves of a filter whose move ended.
// Callers hold p.mu.
func (p *Plane) clearMovesLocked(id routeID) {
	for client := range p.moves[id] {
		k := clientKey{id.ns, client}
		delete(p.byClient[k], id)
		if len(p.byClient[k]) == 0 {
			delete(p.byClient, k)
		}
	}
	delete(p.moves, id)
}
