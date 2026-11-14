package routing

import (
	"sync/atomic"
	"time"

	"github.com/happydave/hoardmq/internal/session"
)

// The plane's side of the route reconciler (session.OwnRoutes): a point read
// of this node's entry for a filter with its move state, the list of live own
// filters, and a correction that leaves move state alone, unlike emit, which
// ends a move: a move begun between the decision and the write must stand.

// moveSkip names a filter's move state in the plane: a held withdrawal, a move
// deadline, or an end timer, in that order; empty for none. It is pure.
func moveSkip(held, moving, ending bool) string {
	switch {
	case held:
		return "held"
	case moving:
		return "moving"
	case ending:
		return "ending"
	}
	return ""
}

// OwnRoute implements session.OwnRoutes.
func (p *Plane) OwnRoute(ns, filter string) session.OwnRoute {
	id := routeID{ns, filter}
	p.mu.Lock()
	_, moving := p.moving[id]
	skip := moveSkip(p.held[id] != nil, moving, p.ending[id] != nil)
	p.mu.Unlock()
	e, ok, indexed := p.table.ownState(ns, filter)
	if skip == "" && indexed {
		skip = "moving"
	}
	return session.OwnRoute{Exists: ok, Live: ok && !e.Tombstone, Version: e.Version, Count: e.Count, Skip: skip}
}

// LiveOwnFilters implements session.OwnRoutes.
func (p *Plane) LiveOwnFilters(add func(ns, filter string)) time.Duration {
	return p.table.eachLiveOwn(add)
}

// correctionRefusal decides a correction's write against this node's current
// entry for the filter (ok false when none) and whether the filter is in the
// moving index, all read under the table's write lock in the hold that would
// write. An announce from an entry that was not live stands while the entry is
// still not live at a version at most the first sighting's (a tombstone, or
// that tombstone pruned); every other correction only while the entry is the
// one both sightings read. It returns "" when the write stands. It is pure.
func correctionRefusal(c session.OwnRouteCorrection, cur Entry, ok, moving bool) string {
	if moving {
		return "moving"
	}
	if c.FromNotLive {
		if ok && (!cur.Tombstone || cur.Version > c.Version) {
			return "version"
		}
		return ""
	}
	if !ok || cur.Version != c.Version || cur.Tombstone == c.Live || (c.Live && cur.Count != c.EvidenceCount) {
		return "version"
	}
	return ""
}

// CorrectOwn implements session.OwnRoutes. A filter with a held withdrawal, a
// move deadline or an end timer is refused under the plane's lock, which is
// released before the table's is taken: Observe takes them in that order.
func (p *Plane) CorrectOwn(c session.OwnRouteCorrection) (bool, string) {
	id := routeID{c.Namespace, c.Filter}
	p.mu.Lock()
	_, moving := p.moving[id]
	busy := moveSkip(p.held[id] != nil, moving, p.ending[id] != nil) != ""
	p.mu.Unlock()
	if busy {
		return false, "held"
	}
	if p.beforeCorrect != nil {
		p.beforeCorrect(c)
	}
	var refused string
	e := Entry{Namespace: c.Namespace, Filter: c.Filter, Owner: p.o.Node, Count: c.Count, Tombstone: c.Count == 0}
	e, merged := p.table.mergeOwnCorrection(e, &p.version, func(cur Entry, ok, moving bool) bool {
		refused = correctionRefusal(c, cur, ok, moving)
		return refused == ""
	})
	if p.afterCorrect != nil {
		p.afterCorrect(c, merged)
	}
	if !merged {
		return false, refused
	}
	p.ownMerged(e)
	return true, ""
}

// ownState is this node's entry for a filter and whether the filter is in the
// moving index, under one read hold.
func (t *Table) ownState(ns, filter string) (Entry, bool, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	k := entryKey{t.self, ns, filter}
	moving := t.movingCount[k] > 0
	if e := t.entries[k]; e != nil {
		return *e, true, moving
	}
	return Entry{}, false, moving
}

// eachLiveOwn calls add for each of this node's live entries under one read
// hold, and returns how long it held the lock.
func (t *Table) eachLiveOwn(add func(ns, filter string)) time.Duration {
	t.mu.RLock()
	began := time.Now()
	for k, e := range t.entries {
		if k.owner == t.self && !e.Tombstone {
			add(k.ns, k.filter)
		}
	}
	d := time.Since(began)
	t.mu.RUnlock()
	return d
}

// mergeOwnCorrection is MergeOwnIf whose decision also reads whether the
// filter is in the moving index, in the same write hold as the merge.
func (t *Table) mergeOwnCorrection(e Entry, version *atomic.Uint64, stands func(cur Entry, ok, moving bool) bool) (Entry, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	k := entryKey{t.self, e.Namespace, e.Filter}
	var cur Entry
	c := t.entries[k]
	if c != nil {
		cur = *c
	}
	if !stands(cur, c != nil, t.movingCount[k] > 0) {
		return e, false
	}
	e.Version = version.Add(1)
	t.merge([]Entry{e})
	return e, true
}
