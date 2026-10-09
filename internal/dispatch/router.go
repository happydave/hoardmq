package dispatch

import (
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/session"
	"github.com/happydave/hoardmq/internal/topic"
)

// Router is the trie-backed session.Router.
type Router struct {
	strategy Strategy
	mu       sync.Mutex
	roots    map[string]*node // by namespace
	retained *retained
	rng      *rand.Rand

	// observer is told when a namespace's direct filter gains its first
	// local subscriber or loses its last, and when a shared group's local
	// member count changes; count is the new local count. It runs without
	// the router lock.
	observer func(ns, filter string, count int)
	// decider, when set, says whether this node delivers a message to a
	// shared group it matched: the routing plane answers no for messages
	// forwarded from another node, and on the publishing node only when this
	// node owns the group's dispatch and its strategy chose a local member;
	// otherwise it forwards the message itself. It runs under the router
	// lock and must not call back into the router.
	decider func(ns, groupKey string, msg *message.Message) bool
}

// SetObserver installs the filter observer (see Router.observer).
func (r *Router) SetObserver(fn func(ns, filter string, count int)) {
	r.mu.Lock()
	r.observer = fn
	r.mu.Unlock()
}

// SetGroupDecider installs the shared-group decider (see Router.decider).
func (r *Router) SetGroupDecider(fn func(ns, groupKey string, msg *message.Message) bool) {
	r.mu.Lock()
	r.decider = fn
	r.mu.Unlock()
}

// notify fires the observer after the lock is released.
func (r *Router) notify(ns, filter string, count int) {
	if r.observer != nil {
		r.observer(ns, filter, count)
	}
}

// New creates a router with the given shared-subscription strategy.
func New(strategy Strategy) *Router {
	if strategy == "" {
		strategy = RoundRobin
	}
	return &Router{strategy: strategy, roots: map[string]*node{}, retained: newRetained(), rng: rand.New(rand.NewSource(1))}
}

func (r *Router) root(ns string, create bool) *node {
	n, ok := r.roots[ns]
	if !ok && create {
		n = newNode()
		r.roots[ns] = n
	}
	return n
}

// Add implements session.Router.
func (r *Router) Add(s *session.Session, sub session.Subscription) bool {
	r.mu.Lock()
	root := r.root(s.Namespace(), true)
	share, inner := topic.SplitShared(sub.Filter)
	n := root.walk(inner, true)
	var count int
	var existed bool
	if share == "" {
		if n.subs == nil {
			n.subs = map[*session.Session]session.Subscription{}
		}
		_, existed = n.subs[s]
		n.subs[s] = sub
		count = len(n.subs)
	} else {
		if n.groups == nil {
			n.groups = map[string]*group{}
		}
		g := n.groups[share]
		if g == nil {
			g = &group{name: share, filter: inner, members: map[*session.Session]session.Subscription{}}
			n.groups[share] = g
		}
		_, existed = g.members[s]
		g.members[s] = sub
		count = len(g.members)
	}
	r.mu.Unlock()
	if !existed && (share != "" || count == 1) {
		r.notify(s.Namespace(), sub.Filter, count)
	}
	return existed
}

// Remove implements session.Router.
func (r *Router) Remove(s *session.Session, filter string) bool {
	r.mu.Lock()
	root := r.root(s.Namespace(), false)
	if root == nil {
		r.mu.Unlock()
		return false
	}
	share, inner := topic.SplitShared(filter)
	n := root.walk(inner, false)
	if n == nil {
		r.mu.Unlock()
		return false
	}
	removed := false
	count := -1
	if share == "" {
		if _, ok := n.subs[s]; ok {
			delete(n.subs, s)
			removed = true
			if len(n.subs) == 0 {
				count = 0
			}
		}
	} else if g := n.groups[share]; g != nil {
		if _, ok := g.members[s]; ok {
			delete(g.members, s)
			removed = true
			count = len(g.members)
			if g.sticky == s {
				g.sticky = nil
			}
			if len(g.members) == 0 {
				delete(n.groups, share)
			}
		}
	}
	if removed {
		root.prune(inner)
	}
	r.mu.Unlock()
	if count >= 0 {
		r.notify(s.Namespace(), filter, count)
	}
	return removed
}

// RemoveAll implements session.Router. It removes the session's filters one
// by one, so its cost is the session's own subscriptions: searching the trie
// for them visited every other session's too, under the lock, on every
// takeover and eviction (WI 1484).
func (r *Router) RemoveAll(s *session.Session, filters []string) {
	for _, f := range filters {
		r.Remove(s, f)
	}
}

// Match implements session.Router: one entry per session across every
// non-shared subscription the topic reaches, with the highest QoS and all
// identifiers, plus one entry per shared group with the chosen member.
func (r *Router) Match(msg *message.Message, publisher *session.Session) []session.Match {
	r.mu.Lock()
	defer r.mu.Unlock()
	root := r.root(msg.Namespace, false)
	if root == nil {
		return nil
	}
	hits := root.match(msg.Topic)
	per := map[*session.Session]*session.Match{}
	var order []*session.Session
	var out []session.Match
	for _, h := range hits {
		for s, sub := range h.n.subs {
			if sub.NoLocal && s == publisher {
				continue
			}
			m := per[s]
			if m == nil {
				m = &session.Match{Session: s}
				per[s] = m
				order = append(order, s)
			}
			if sub.QoS > m.QoS {
				m.QoS = sub.QoS
			}
			if sub.Identifier != 0 {
				m.Identifiers = append(m.Identifiers, sub.Identifier)
			}
			if sub.RetainAsPublished {
				m.RetainAsPublished = true
			}
		}
		names := make([]string, 0, len(h.n.groups))
		for name := range h.n.groups {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			g := h.n.groups[name]
			if r.decider != nil && !r.decider(msg.Namespace, groupKey(name, g.filter), msg) {
				continue
			}
			member, ok := g.choose(r.strategy, msg, nil, r.rng)
			if !ok {
				continue
			}
			sub := g.members[member]
			var ids []uint32
			if sub.Identifier != 0 {
				ids = []uint32{sub.Identifier}
			}
			out = append(out, session.Match{Session: member, QoS: sub.QoS, Identifiers: ids, RetainAsPublished: sub.RetainAsPublished, Group: groupKey(name, g.filter)})
		}
	}
	sort.Slice(order, func(i, j int) bool { return order[i].ClientID() < order[j].ClientID() })
	direct := make([]session.Match, 0, len(order))
	for _, s := range order {
		m := per[s]
		sort.Slice(m.Identifiers, func(i, j int) bool { return m.Identifiers[i] < m.Identifiers[j] })
		direct = append(direct, *m)
	}
	return append(direct, out...)
}

func groupKey(share, inner string) string { return share + "/" + inner }

// MatchGroup implements session.Router: a fresh choice for a redistributed
// message, excluding the departed member.
func (r *Router) MatchGroup(ns, key string, msg *message.Message, exclude *session.Session) (session.Match, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	root := r.root(ns, false)
	if root == nil {
		return session.Match{}, false
	}
	share, inner, _ := cutGroupKey(key)
	n := root.walk(inner, false)
	if n == nil {
		return session.Match{}, false
	}
	g := n.groups[share]
	if g == nil {
		return session.Match{}, false
	}
	member, ok := g.choose(r.strategy, msg, exclude, r.rng)
	if !ok {
		return session.Match{}, false
	}
	sub := g.members[member]
	var ids []uint32
	if sub.Identifier != 0 {
		ids = []uint32{sub.Identifier}
	}
	return session.Match{Session: member, QoS: sub.QoS, Identifiers: ids, RetainAsPublished: sub.RetainAsPublished, Group: key}, true
}

func cutGroupKey(key string) (share, inner string, ok bool) {
	for i := 0; i < len(key); i++ {
		if key[i] == '/' {
			return key[:i], key[i+1:], true
		}
	}
	return "", key, false
}

// Retain implements session.Router.
func (r *Router) Retain(msg *message.Message) { r.retained.set(msg) }

// RetainedFor implements session.Router.
func (r *Router) RetainedFor(ns, filter string, now time.Time) []*message.Message {
	return r.retained.matching(ns, filter, now)
}

// Sweep implements session.Router.
func (r *Router) Sweep(now time.Time) { r.retained.sweep(now) }

// AllRetained implements session.Router.
func (r *Router) AllRetained() []*message.Message { return r.retained.all() }

// LoadRetained implements session.Router.
func (r *Router) LoadRetained(ms []*message.Message) {
	for _, m := range ms {
		r.retained.set(m)
	}
}

// RetainedCount is the number of retained messages held.
func (r *Router) RetainedCount() int { return r.retained.count() }

// RetainedCounts is the number of retained messages by namespace.
func (r *Router) RetainedCounts() map[string]int { return r.retained.counts() }

// Matcher is one subscription a topic reaches.
type Matcher struct {
	ClientID string
	// Filter is the subscription as the client made it, $share prefix
	// included for a shared one; Group is the share name, empty otherwise.
	Filter string
	Group  string
	QoS    byte
}

// Matchers lists every subscription a topic reaches on this node, plain and
// shared, sorted by client then filter. It is a view: it chooses no shared
// group member and asks no decider, so the next delivery is the one it would
// have been. Match does both.
func (r *Router) Matchers(ns, topic string) []Matcher {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Matcher, 0)
	root := r.root(ns, false)
	if root == nil {
		return out
	}
	for _, h := range root.match(topic) {
		for s, sub := range h.n.subs {
			out = append(out, Matcher{ClientID: s.ClientID(), Filter: sub.Filter, QoS: sub.QoS})
		}
		for name, g := range h.n.groups {
			for s, sub := range g.members {
				out = append(out, Matcher{ClientID: s.ClientID(), Filter: "$share/" + name + "/" + g.filter, Group: name, QoS: sub.QoS})
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ClientID != out[j].ClientID {
			return out[i].ClientID < out[j].ClientID
		}
		return out[i].Filter < out[j].Filter
	})
	return out
}
