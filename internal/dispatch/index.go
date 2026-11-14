package dispatch

import (
	"time"

	"github.com/happydave/hoardmq/internal/session"
	"github.com/happydave/hoardmq/internal/topic"
)

// The filter index lets a reader visit every filter the router holds a slice
// at a time, releasing the read lock between slices: a walk of the trie holds
// it for the whole trie, and a writer queued behind that hold stops every
// later match. It is a slice of the trie nodes holding a direct subscriber or
// a group, each node knowing its position, with one walk's position kept
// beside it.
const (
	// walkStepFilters and walkStepSubs bound what one WalkStep copies under
	// the read lock. A node that would take a step past either is left for
	// the next step, which takes it alone if it must.
	walkStepFilters = 4096
	walkStepSubs    = 4096
)

// holding reports whether a trie node holds a direct subscriber or a group.
func (n *node) holding() bool { return len(n.subs) > 0 || len(n.groups) > 0 }

// indexPut records a node that has just come to hold a subscriber or a group.
// The write lock is held.
func (r *Router) indexPut(ns, filter string, n *node) {
	n.ns, n.filter = ns, filter
	r.index = append(r.index, n)
	n.pos = len(r.index)
}

// indexDelete forgets a node that holds neither any more. The write lock is
// held. Every position below the walk's is visited: a node moved behind it is
// one already visited, and the node taken from the end, not yet visited, is
// put at the walk's position, which steps back to it. So a node held
// throughout a walk is visited once, however the index changes under it.
func (r *Router) indexDelete(n *node) {
	if n.pos == 0 {
		return
	}
	h, last, at := n.pos-1, len(r.index)-1, int(r.walkAt.Load())
	n.pos = 0
	if h < at {
		r.indexMove(at-1, h)
		r.indexMove(last, at-1)
		r.walkAt.Store(int64(at - 1))
	} else {
		r.indexMove(last, h)
	}
	r.index[last] = nil
	r.index = r.index[:last]
}

// indexMove puts the node at position from into position to.
func (r *Router) indexMove(from, to int) {
	if from == to {
		return
	}
	m := r.index[from]
	r.index[to] = m
	m.pos = to + 1
}

// Holds implements session.RouterIndex: whether s is a subscriber of filter,
// as the client wrote it.
func (r *Router) Holds(s *session.Session, filter string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	root := r.root(s.Namespace(), false)
	if root == nil {
		return false
	}
	share, inner := topic.SplitShared(filter)
	n := root.walk(inner, false)
	if n == nil {
		return false
	}
	if share == "" {
		_, ok := n.subs[s]
		return ok
	}
	g := n.groups[share]
	if g == nil {
		return false
	}
	_, ok := g.members[s]
	return ok
}

// SubscriberCount implements session.RouterIndex: a direct filter's
// subscribers, or a group's members.
func (r *Router) SubscriberCount(ns, filter string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	root := r.root(ns, false)
	if root == nil {
		return 0
	}
	share, inner := topic.SplitShared(filter)
	n := root.walk(inner, false)
	if n == nil {
		return 0
	}
	if share == "" {
		return len(n.subs)
	}
	if g := n.groups[share]; g != nil {
		return len(g.members)
	}
	return 0
}

// WalkStep implements session.RouterIndex. Cursor 0 starts a walk; any other
// continues the one walk the router keeps. It copies nodes from the walk's
// position under one read hold until the next would take the step past
// walkStepFilters filters or walkStepSubs subscribers, taking at least one,
// and returns the copies, the cursor to continue with, whether the walk is
// done, and how long the read lock was held.
func (r *Router) WalkStep(cursor int) ([]session.RoutedFilter, int, bool, time.Duration) {
	var out []session.RoutedFilter
	r.mu.RLock()
	began := time.Now()
	if cursor == 0 {
		r.walkAt.Store(0)
	}
	i := int(r.walkAt.Load())
	filters, subs := 0, 0
	// One backing array for the step's subscribers: a slice for each filter
	// leaves the collector thousands of objects a step.
	backing := make([]*session.Session, 0, walkStepSubs)
	take := func(m map[*session.Session]session.Subscription) []*session.Session {
		start := len(backing)
		for s := range m {
			backing = append(backing, s)
		}
		return backing[start:len(backing):len(backing)]
	}
	for ; i < len(r.index); i++ {
		n := r.index[i]
		nf, nsubs := 0, 0
		if len(n.subs) > 0 {
			nf++
			nsubs += len(n.subs)
		}
		for _, g := range n.groups {
			nf++
			nsubs += len(g.members)
		}
		if filters > 0 && (filters+nf > walkStepFilters || subs+nsubs > walkStepSubs) {
			break
		}
		if len(n.subs) > 0 {
			out = append(out, session.RoutedFilter{Namespace: n.ns, Filter: n.filter, Subs: take(n.subs)})
		}
		for name, g := range n.groups {
			out = append(out, session.RoutedFilter{Namespace: n.ns, Filter: topic.SharePrefix + name + "/" + n.filter, Group: true, Subs: take(g.members)})
		}
		filters += nf
		subs += nsubs
	}
	r.walkAt.Store(int64(i))
	done := i >= len(r.index)
	hold := time.Since(began)
	r.mu.RUnlock()
	return out, i + 1, done, hold
}

// Indexed is the number of trie nodes the filter index holds.
func (r *Router) Indexed() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.index)
}
