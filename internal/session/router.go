package session

import (
	"sort"
	"sync"
	"time"

	"github.com/happydave/hoardmq/internal/message"
)

// Subscription is one filter a session holds, with its options.
type Subscription struct {
	Filter            string
	QoS               byte
	NoLocal           bool
	RetainAsPublished bool
	RetainHandling    byte
	Identifier        uint32
}

// Match is one delivery target for a message: a session, the QoS granted
// across every subscription of that session the message matched, the
// subscription identifiers to attach, and the shared group it was chosen
// through, if any.
type Match struct {
	Session           *Session
	QoS               byte
	Identifiers       []uint32
	RetainAsPublished bool
	Group             string
}

// Router maps topics to subscribers and holds retained messages. The
// dispatch package implements it with the trie; the exact-match router here
// serves this package's tests.
type Router interface {
	// Add stores a subscription and reports whether the session already had
	// one for the filter.
	Add(s *Session, sub Subscription) (existed bool)
	Remove(s *Session, filter string) bool
	// RemoveAll removes a departing session's subscriptions, given the
	// filters it holds. The router is not searched for others: every
	// subscription reaches the router through the session's own set.
	RemoveAll(s *Session, filters []string)
	// Match returns one entry per session (or per shared group) that the
	// message reaches, in a deterministic order. publisher is used for No
	// Local and the hash strategy and may be nil.
	Match(msg *message.Message, publisher *Session) []Match
	// MatchGroup chooses a member of a shared group other than exclude.
	MatchGroup(namespace, group string, msg *message.Message, exclude *Session) (Match, bool)
	// Retain stores or, for an empty payload, removes a retained message.
	Retain(msg *message.Message)
	// RetainedFor returns the live retained messages a filter matches.
	RetainedFor(namespace, filter string, now time.Time) []*message.Message
	// Sweep drops expired retained messages.
	Sweep(now time.Time)
	// AllRetained lists every retained message, for snapshots.
	AllRetained() []*message.Message
	// LoadRetained installs retained messages recovered from a store.
	LoadRetained([]*message.Message)
}

type routeKey struct{ ns, filter string }

// exactRouter matches topic names to identical filters only and has no
// shared groups.
type exactRouter struct {
	mu       sync.RWMutex
	routes   map[routeKey]map[*Session]Subscription
	retained map[routeKey]*message.Message
}

func newExactRouter() *exactRouter {
	return &exactRouter{routes: map[routeKey]map[*Session]Subscription{}, retained: map[routeKey]*message.Message{}}
}

func (r *exactRouter) Add(s *Session, sub Subscription) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := routeKey{s.key.ns, sub.Filter}
	m := r.routes[k]
	if m == nil {
		m = map[*Session]Subscription{}
		r.routes[k] = m
	}
	_, existed := m[s]
	m[s] = sub
	return existed
}

func (r *exactRouter) Remove(s *Session, filter string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := routeKey{s.key.ns, filter}
	m := r.routes[k]
	if _, ok := m[s]; !ok {
		return false
	}
	delete(m, s)
	if len(m) == 0 {
		delete(r.routes, k)
	}
	return true
}

func (r *exactRouter) RemoveAll(s *Session, filters []string) {
	for _, f := range filters {
		r.Remove(s, f)
	}
}

func (r *exactRouter) Match(msg *message.Message, publisher *Session) []Match {
	r.mu.RLock()
	defer r.mu.RUnlock()
	m := r.routes[routeKey{msg.Namespace, msg.Topic}]
	out := make([]Match, 0, len(m))
	for s, sub := range m {
		if sub.NoLocal && s == publisher {
			continue
		}
		var ids []uint32
		if sub.Identifier != 0 {
			ids = []uint32{sub.Identifier}
		}
		out = append(out, Match{Session: s, QoS: sub.QoS, Identifiers: ids, RetainAsPublished: sub.RetainAsPublished})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Session.key.id < out[j].Session.key.id })
	return out
}

func (r *exactRouter) MatchGroup(string, string, *message.Message, *Session) (Match, bool) {
	return Match{}, false
}

func (r *exactRouter) Retain(msg *message.Message) {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := routeKey{msg.Namespace, msg.Topic}
	if len(msg.Payload) == 0 {
		delete(r.retained, k)
		return
	}
	r.retained[k] = msg
}

func (r *exactRouter) RetainedFor(ns, filter string, now time.Time) []*message.Message {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if m, ok := r.retained[routeKey{ns, filter}]; ok && !m.Expired(now) {
		return []*message.Message{m}
	}
	return nil
}

func (r *exactRouter) Sweep(time.Time) {}

func (r *exactRouter) AllRetained() []*message.Message {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*message.Message, 0, len(r.retained))
	for _, m := range r.retained {
		out = append(out, m)
	}
	return out
}

func (r *exactRouter) LoadRetained(ms []*message.Message) {
	for _, m := range ms {
		r.Retain(m)
	}
}
