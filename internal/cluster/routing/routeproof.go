package routing

// A node may forget a lost owner's routes only while it holds the routes that
// replace them: those of every member that owns leases, as of the moment that
// member said, in the lease log, that its adoptions were finished. The log and
// the route updates travel on different connections, and a route update can
// be lost with the link up (a send that times out, a data connection
// superseded by a newer one), so a node checks that it holds them.
//
// Each node numbers its delta batches, one apart, from a seed taken at start
// like the entry versions, so a restarted node numbers above its previous
// incarnation. A delta's first entry carries its number. A full set carries
// the number of the last batch composed before it, and holds every entry of
// every batch up to that number, because an entry is merged into its node's
// own table before it is queued. A node's mark is how far its batches had
// gone when it read the mark: the last number, or the next one while entries
// wait to be composed. A peer holds a node's routes up to a mark when it has
// every batch up to the mark with no gap since a full set.

// routeProof is how far one peer's route updates are held.
type routeProof struct {
	whole bool   // no gap since the last full set
	last  uint64 // the last batch held
	cover uint64 // the last full set's number: batches up to it are held
}

// delta records a batch and reports whether it broke the proof. A batch at
// or below the last full set's number was composed before that set; any
// other batch that is not the next one, or one with no number, is a gap.
func (r *routeProof) delta(number uint64) (broke bool) {
	switch {
	case number != 0 && r.whole && number <= r.cover:
	case number != 0 && r.whole && number == r.last+1:
		r.last = number
	default:
		broke = r.whole
		r.whole = false
	}
	return broke
}

// full records a full set. One with no number (an older sender) proves
// nothing.
func (r *routeProof) full(number uint64) {
	r.whole, r.last, r.cover = number != 0, number, number
}

// holds reports whether every batch up to mark is held.
func (r *routeProof) holds(mark uint64) bool {
	return r != nil && r.whole && mark != 0 && r.last >= mark
}

// noteRoutes records a route update from a peer, after it was merged, and
// wakes the served checks when the peer's routes now reach a mark they wait
// on.
func (p *Plane) noteRoutes(peer string, isFull bool, number uint64) {
	p.mu.Lock()
	r := p.proofs[peer]
	if r == nil {
		r = &routeProof{}
		p.proofs[peer] = r
	}
	broke := false
	if isFull {
		r.full(number)
	} else {
		broke = r.delta(number)
	}
	waited := p.awaitRoutes[peer]
	wake := waited != 0 && r.holds(waited)
	p.mu.Unlock()
	if broke {
		p.count("hoardmq_route_update_gaps_total", nil, 1)
		p.log.Debug("a gap in a peer's route updates; its routes are proved again by its next full set", "peer", peer, "batch", number)
	}
	if wake {
		p.WakeServed()
	}
}

// routesHeld reports whether this node holds a peer's route updates up to a
// mark.
func (p *Plane) routesHeld(peer string, mark uint64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.proofs[peer].holds(mark)
}

// routeMarkLocked is this node's mark. Callers hold p.mu.
func (p *Plane) routeMarkLocked() uint64 {
	if len(p.pending) > 0 {
		return p.batches + 1
	}
	return p.batches
}

// RouteMark is how far this node's route updates have gone: every change
// queued before the call is in a batch numbered at most this. An emit
// merges its change and then queues it, so a change being emitted during the
// call may fall in a later batch.
func (p *Plane) RouteMark() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.routeMarkLocked()
}
