package replication

import (
	"context"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/store"
)

// Fetch returns a replica of the session for a new owner whose previous
// owner cannot hand the state over. It returns nil when no source answers
// with one, and answered says what that nil is worth: true when every node
// that was asked replied, false when a request ended without a reply or
// nobody could be asked yet. Only an answered nil is evidence that no copy
// exists. A source that is down is not asked and does not count against it;
// down names it, so the caller can judge whether it may still hold a copy.
// Under a map that no longer holds prev, down also names the other peers
// that are down, prev aside (namedDown). A peer that is back on its control
// connection and not yet Up cannot be asked on the data connection a fetch
// travels on, and is not down either: as a source or a former holder it may
// hold the copy, so it leaves the fetch unanswered.
//
// The sources are the nodes prev replicated this session to, not every
// holder of its shard. A shard's holders are its leader and all its
// followers, while an owner replicates to at most replicas-1 of them, so a
// three-node shard with two replicas has a holder that receives nothing.
// Its copy is whatever it last held, no barrier ever covered it, and a
// takeover that adopted it would hand the client a queue missing everything
// since — including messages the client has already been given, which it
// would then be given again.
func (r *Replicator) Fetch(ctx context.Context, ns, id, prev string) (st *store.SessionState, answered bool, down []string) {
	f := r.FetchKept(ctx, ns, id, prev)
	return f.State, f.Answered, f.Down
}

// Fetched is a fetch's whole result. Kept is a copy a former holder kept,
// found while a source did not answer, and KeptFrom that holder: never the
// fetch's answer, since the silent source may hold a newer copy, but what an
// adoption that gives up serves.
type Fetched struct {
	State    *store.SessionState
	Answered bool
	Down     []string
	Kept     *store.SessionState
	KeptFrom string
}

// FetchKept is Fetch with the unconfirmed kept copy.
func (r *Replicator) FetchKept(ctx context.Context, ns, id, prev string) Fetched {
	st, answered, down, kept, keptFrom := r.fetch(ctx, ns, id, prev)
	return Fetched{State: st, Answered: answered, Down: down, Kept: kept, KeptFrom: keptFrom}
}

func (r *Replicator) fetch(ctx context.Context, ns, id, prev string) (st *store.SessionState, answered bool, down []string, unconfirmed *store.SessionState, unconfirmedFrom string) {
	m := r.o.Map.Current()
	if m.Version == 0 {
		// No map yet: with peers, the holders are not known, so nobody was
		// asked.
		return nil, r.o.Transport == nil, nil, nil, ""
	}
	shard := r.o.Map.ShardOf(ns, id)
	sources := m.Holders(shard)
	if prev != "" {
		sources = m.Targets(shard, prev)
	}
	if contains(sources, r.o.Node) {
		finding := time.Now()
		ss, ok := r.follower.find(ns, id)
		r.fetches.localFinds.Add(1)
		r.fetches.localFindNs.Add(since(finding))
		if ok {
			r.fetches.local.Add(1)
			r.count("hoardmq_replication_fetches_total", map[string]string{"outcome": "local"}, 1)
			r.log.Debug("replica fetched locally", "client", id, "entries", len(ss.Entries))
			return ss, true, down, nil, ""
		}
	}
	if r.o.Transport == nil {
		return nil, true, nil, nil, ""
	}
	ctx, cancel := context.WithTimeout(ctx, r.o.AckTimeout)
	defer cancel()
	answers := make(chan fetchAnswer, len(sources)+len(r.o.Transport.Peers())+1)
	ask := func(peer string, source bool) {
		go func() {
			ans, ok := r.request(ctx, peer, frame{Kind: kindFetch, Namespace: ns, ClientID: id})
			answers <- fetchAnswer{peer: peer, source: source, frame: ans, ok: ok}
		}()
	}
	asked := map[string]bool{r.o.Node: true}
	waitingSources, sourceDown := 0, false
	var returning []string
	sourceReturning := false
	for _, h := range sources {
		asked[h] = true
		if h == r.o.Node {
			continue
		}
		switch fetchReach(r.peerInfo(h)) {
		case reachDown:
			sourceDown = true
			down = append(down, h)
			continue
		case reachReturning:
			sourceReturning = true
			returning = append(returning, h)
			continue
		}
		ask(h, true)
		waitingSources++
	}
	peerDown := false
	for _, p := range r.o.Transport.Peers() {
		if !asked[p.Name] && namedDown(prev, m.Basis, p.Name, fetchReach(p)) {
			down = append(down, p.Name)
			peerDown = true
		}
	}
	// A former holder keeps its copy when its hand-over went unacknowledged,
	// so the other live peers are asked too: this node first, since
	// the adopting leader is as likely as any peer to be the former holder that
	// kept it. They are asked once every source has answered, or once half the
	// budget has gone with one still silent, so that a late source does not
	// spend the time a kept copy needs.
	var kept *store.SessionState
	keptFrom, keptAsked, waitingKept := "", false, 0
	askKept := func() {
		keptAsked = true
		if !contains(sources, r.o.Node) {
			if ss, ok := r.follower.find(ns, id); ok {
				kept, keptFrom = ss, r.o.Node
				return
			}
		}
		if ctx.Err() != nil {
			return
		}
		for _, p := range r.o.Transport.Peers() {
			if asked[p.Name] {
				continue
			}
			switch fetchReach(p) {
			case reachAsk:
				asked[p.Name] = true
				ask(p.Name, false)
				waitingKept++
			case reachReturning:
				asked[p.Name] = true
				returning = append(returning, p.Name)
			}
		}
	}
	hedge := time.NewTimer(r.o.AckTimeout / 2)
	defer hedge.Stop()
	if waitingSources == 0 {
		askKept()
	}
	unanswered, sourceUnanswered := false, sourceReturning
	for waitingSources > 0 || (kept == nil && waitingKept > 0) {
		select {
		case a := <-answers:
			found := a.ok && a.Found && a.State != nil
			if !a.ok {
				unanswered = true
			}
			if a.source {
				waitingSources--
				if !a.ok {
					sourceUnanswered = true
				}
				if found {
					r.fetches.remote.Add(1)
					r.count("hoardmq_replication_fetches_total", map[string]string{"outcome": "remote"}, 1)
					r.audit("replication.fetch", ns+"/"+id, "ok", map[string]string{"from": a.peer})
					r.log.Debug("replica fetched", "client", id, "from", a.peer, "entries", len(a.State.Entries))
					return a.State, true, down, nil, ""
				}
				r.log.Debug("replica not found on holder", "client", id, "holder", a.peer, "answered", a.ok)
			} else {
				waitingKept--
				if found && kept == nil {
					kept, keptFrom = a.State, a.peer
				}
			}
			if waitingSources == 0 && !keptAsked {
				askKept()
			}
		case <-hedge.C:
			if !keptAsked {
				askKept()
			}
		}
	}
	if kept != nil && !sourceUnanswered {
		r.fetches.keptCopy.Add(1)
		r.count("hoardmq_replication_fetches_total", map[string]string{"outcome": "kept-copy"}, 1)
		r.audit("replication.fetch", ns+"/"+id, "kept-copy", map[string]string{"from": keptFrom})
		r.log.Info("replica fetched from a copy a former holder kept", "client", id, "from", keptFrom, "entries", len(kept.Entries))
		return kept, true, down, nil, ""
	}
	if kept != nil {
		// A source that did not answer may hold the copy the owner fed, newer
		// than this one, so the fetch reads unanswered.
		r.fetches.noneKeptUnconfirmed.Add(1)
		r.count("hoardmq_replication_fetches_total", map[string]string{"outcome": "none-kept-unconfirmed"}, 1)
		r.log.Info("replica not fetched: a former holder kept a copy, but a source did not answer", "client", id, "keptFrom", keptFrom, "sources", sources)
		unconfirmed, unconfirmedFrom = kept, keptFrom
	}
	if len(returning) > 0 {
		unanswered = true
		r.fetches.noneReturning.Add(1)
		r.count("hoardmq_replication_fetches_total", map[string]string{"outcome": "none-returning"}, 1)
		r.log.Debug("replica not fetched: a peer that may hold it is back on its control connection and cannot be asked yet", "client", id, "returning", returning, "sources", sources)
	}
	r.fetches.none.Add(1)
	if unanswered {
		r.fetches.noneUnanswered.Add(1)
	}
	if sourceDown {
		r.fetches.noneSourceDown.Add(1)
	}
	if peerDown {
		r.fetches.nonePeerDown.Add(1)
	}
	r.count("hoardmq_replication_fetches_total", map[string]string{"outcome": "none"}, 1)
	r.log.Debug("no replica on any source", "client", id, "sources", sources, "down", down, "prev", prev, "map", m.Version)
	return nil, !unanswered, down, unconfirmed, unconfirmedFrom
}

// namedDown reports whether a fetch names a peer it cannot reach in down,
// beside the down sources, for the caller to judge by the log. Under a map
// whose basis no longer holds prev, the sources are not the nodes prev
// replicated to, and the copy it fed may be on any peer. prev is judged as
// the previous owner, and a returning peer leaves the fetch unanswered.
func namedDown(prev string, basis []string, peer string, reach fetchPeerReach) bool {
	return prev != "" && !contains(basis, prev) && peer != prev && reach == reachDown
}

// fetchAnswer is one node's reply to a fetch request, or ok false when the
// request ended without one.
type fetchAnswer struct {
	peer   string
	source bool
	frame
	ok bool
}

// fetchPeerReach is what a fetch can do with a peer.
type fetchPeerReach int

const (
	reachAsk       fetchPeerReach = iota // Up: asked on the data connection
	reachReturning                       // control connection in place, not Up yet: cannot be asked, may hold the copy
	reachDown                            // neither: not asked, and the caller judges it
)

// fetchReach classifies a peer for a fetch. The transport reads a peer Up only
// once both its connections are in place, so a restarted peer, or one back
// from a partition, is reachable on its control connection before a fetch,
// which travels on the data connection, can reach it.
func fetchReach(p transport.PeerInfo) fetchPeerReach {
	switch {
	case p.State == transport.Up:
		return reachAsk
	case p.ControlUp:
		return reachReturning
	default:
		return reachDown
	}
}

// peerInfo is the transport's information about one peer; a peer it does not
// list reads as having no connection.
func (r *Replicator) peerInfo(name string) transport.PeerInfo {
	for _, p := range r.o.Transport.Peers() {
		if p.Name == name {
			return p
		}
	}
	return transport.PeerInfo{Name: name}
}
