package session

import (
	"sort"
	"sync"
	"time"

	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
	"github.com/happydave/hoardmq/internal/topic"
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

// RetainedEntry is one held retained topic (store.RetainedEntry).
type RetainedEntry = store.RetainedEntry

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
	// Retain stores a retained message unversioned or, for an empty payload,
	// removes whatever the topic holds: a copy drop.
	Retain(msg *message.Message)
	// RetainUnder is Retain for a new unversioned write: it stores nothing
	// over a topic holding a versioned entry (store.UnversionedRefused), nor a
	// message that would add a topic to a namespace already holding limit
	// topics (0 is none), and reports why it did not store it.
	RetainUnder(msg *message.Message, limit int) store.RetainRefusal
	// RetainedFor returns the live retained messages a filter matches.
	RetainedFor(namespace, filter string, now time.Time) []*message.Message
	// Sweep drops expired retained messages.
	Sweep(now time.Time)
	// AllRetained lists every retained message held, tombstones left out.
	AllRetained() []*message.Message
	// RetainStamped installs a leader's client write or clear (an empty
	// payload) stamped (epoch, the router's next number) in one critical
	// section, unless it would add a topic past limit; a tombstone counts as
	// a held topic. It returns the version and whether it installed.
	RetainStamped(msg *message.Message, epoch uint64, limit int) (store.Version, bool)
	// MergeRetained installs a copy (a tombstone when its payload is empty) at
	// a version by store.MergeRetained, handOver on a hand-over or hand-on
	// path, and reports whether it did, with what is held after. An empty
	// payload at the zero Version is a copy drop, outside merge.
	MergeRetained(msg *message.Message, v store.Version, handOver bool) (bool, RetainedEntry)
	// MarkRetainedAppended records the journal's append count at a topic's
	// record, while its entry is still msg at v.
	MarkRetainedAppended(msg *message.Message, v store.Version, count uint64)
	// HeldRetained is what is held for a topic, tombstone included.
	HeldRetained(ns, topic string) (RetainedEntry, bool)
	// DropTombstoneIf removes a topic's tombstone only while it is the one
	// at v.
	DropTombstoneIf(ns, topic string, v store.Version) bool
	// RetainedEntries lists every held entry, tombstones included.
	RetainedEntries() []RetainedEntry
	// SeedRetainedVersion makes later stamps take numbers above n.
	SeedRetainedVersion(n uint64)
	// SweepLeading drops expired retained messages: on a shard this node
	// leads (leads gives the epoch) each becomes a fresh-stamped tombstone,
	// returned for the caller to journal; elsewhere it is removed here alone.
	SweepLeading(now time.Time, leads func(ns, topic string) (uint64, bool)) []RetainedEntry
}

// RoutedFilter is one filter a router holds subscribers for, as a walk step
// copied it: the filter as a client wrote it ($share/<name>/<inner> for a
// group) and its subscriber objects (a group's members).
type RoutedFilter struct {
	Namespace string
	Filter    string
	Group     bool
	Subs      []*Session
}

// RouterIndex is what the reconciler reads of a router, each under the
// router's read lock: whether it holds a session object for a filter, a
// filter's subscriber count, and a walk of every filter it holds, a step at a
// time, each step's copy bounded and the lock released between steps. A
// router without it leaves the reconciler off.
type RouterIndex interface {
	Holds(s *Session, filter string) bool
	SubscriberCount(ns, filter string) int
	// WalkStep copies from cursor (0 to start) and returns the copies, the
	// cursor to resume from, whether the walk is done, and how long it held
	// the lock.
	WalkStep(cursor int) (filters []RoutedFilter, next int, done bool, hold time.Duration)
}

type routeKey struct{ ns, filter string }

// exactRouter matches topic names to identical filters only and has no
// shared groups.
type exactRouter struct {
	mu       sync.RWMutex
	routes   map[routeKey]map[*Session]Subscription
	retained map[routeKey]*message.Message // a tombstone has an empty payload
	vers     map[routeKey]store.Version
	appended map[routeKey]uint64
	n        uint64
}

func newExactRouter() *exactRouter {
	return &exactRouter{routes: map[routeKey]map[*Session]Subscription{}, retained: map[routeKey]*message.Message{},
		vers: map[routeKey]store.Version{}, appended: map[routeKey]uint64{}}
}

// putLocked stores m at v. The lock is held.
func (r *exactRouter) putLocked(m *message.Message, v store.Version) {
	k := routeKey{m.Namespace, m.Topic}
	r.retained[k] = m
	if v.Zero() {
		delete(r.vers, k)
	} else {
		r.vers[k] = v
	}
	delete(r.appended, k)
}

func (r *exactRouter) dropLocked(k routeKey) {
	delete(r.retained, k)
	delete(r.vers, k)
	delete(r.appended, k)
}

func (r *exactRouter) heldLocked(k routeKey) (RetainedEntry, bool) {
	m, ok := r.retained[k]
	if !ok {
		return RetainedEntry{}, false
	}
	return RetainedEntry{Msg: m, RV: r.vers[k], Tombstone: len(m.Payload) == 0, Appended: r.appended[k]}, true
}

func (r *exactRouter) RetainStamped(msg *message.Message, epoch uint64, limit int) (store.Version, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := routeKey{msg.Namespace, msg.Topic}
	if _, holds := r.retained[k]; !holds && limit > 0 {
		held := 0
		for rk := range r.retained {
			if rk.ns == msg.Namespace {
				held++
			}
		}
		if held >= limit {
			return store.Version{}, false
		}
	}
	r.n++
	v := store.Version{E: epoch, N: r.n}
	r.putLocked(msg, v)
	return v, true
}

func (r *exactRouter) MergeRetained(msg *message.Message, v store.Version, handOver bool) (bool, RetainedEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := routeKey{msg.Namespace, msg.Topic}
	if len(msg.Payload) == 0 && v.Zero() {
		r.dropLocked(k)
		return true, RetainedEntry{}
	}
	cur, exists := r.heldLocked(k)
	if !store.MergeRetained(cur.RV, exists, v, handOver) {
		return false, cur
	}
	if v.N > r.n {
		r.n = v.N
	}
	r.putLocked(msg, v)
	after, _ := r.heldLocked(k)
	return true, after
}

func (r *exactRouter) MarkRetainedAppended(msg *message.Message, v store.Version, count uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := routeKey{msg.Namespace, msg.Topic}
	if r.retained[k] == msg && r.vers[k] == v {
		r.appended[k] = count
	}
}

func (r *exactRouter) HeldRetained(ns, topic string) (RetainedEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.heldLocked(routeKey{ns, topic})
}

func (r *exactRouter) DropTombstoneIf(ns, topic string, v store.Version) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := routeKey{ns, topic}
	if m, ok := r.retained[k]; !ok || len(m.Payload) != 0 || r.vers[k] != v {
		return false
	}
	r.dropLocked(k)
	return true
}

func (r *exactRouter) RetainedEntries() []RetainedEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]RetainedEntry, 0, len(r.retained))
	for k := range r.retained {
		e, _ := r.heldLocked(k)
		out = append(out, e)
	}
	return out
}

func (r *exactRouter) SeedRetainedVersion(n uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n > r.n {
		r.n = n
	}
}

func (r *exactRouter) SweepLeading(now time.Time, leads func(string, string) (uint64, bool)) []RetainedEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	var stamped []RetainedEntry
	for k, m := range r.retained {
		if len(m.Payload) == 0 || !m.Expired(now) {
			continue
		}
		if epoch, ok := leads(k.ns, k.filter); ok && epoch > 0 {
			r.n++
			tomb := &message.Message{Namespace: k.ns, Topic: k.filter, Retain: true}
			v := store.Version{E: epoch, N: r.n}
			r.putLocked(tomb, v)
			stamped = append(stamped, RetainedEntry{Msg: tomb, RV: v, Tombstone: true})
			continue
		}
		r.dropLocked(k)
	}
	return stamped
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

// Holds implements RouterIndex.
func (r *exactRouter) Holds(s *Session, filter string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.routes[routeKey{s.key.ns, filter}][s]
	return ok
}

// SubscriberCount implements RouterIndex.
func (r *exactRouter) SubscriberCount(ns, filter string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.routes[routeKey{ns, filter}])
}

// WalkStep implements RouterIndex in one step: this router serves tests.
func (r *exactRouter) WalkStep(int) ([]RoutedFilter, int, bool, time.Duration) {
	r.mu.RLock()
	began := time.Now()
	out := make([]RoutedFilter, 0, len(r.routes))
	for k, m := range r.routes {
		f := RoutedFilter{Namespace: k.ns, Filter: k.filter, Group: topic.IsShared(k.filter), Subs: make([]*Session, 0, len(m))}
		for s := range m {
			f.Subs = append(f.Subs, s)
		}
		out = append(out, f)
	}
	hold := time.Since(began)
	r.mu.RUnlock()
	return out, 1, true, hold
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
		r.dropLocked(k)
		return
	}
	r.putLocked(msg, store.Version{})
}

func (r *exactRouter) RetainUnder(msg *message.Message, limit int) store.RetainRefusal {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := routeKey{msg.Namespace, msg.Topic}
	_, holds := r.retained[k]
	if store.UnversionedRefused(r.vers[k], holds) {
		return store.RetainVersionedHeld
	}
	if len(msg.Payload) > 0 && !holds && limit > 0 {
		held := 0
		for rk := range r.retained {
			if rk.ns == msg.Namespace {
				held++
			}
		}
		if held >= limit {
			return store.RetainOverLimit
		}
	}
	if len(msg.Payload) == 0 {
		r.dropLocked(k)
		return store.RetainStored
	}
	r.putLocked(msg, store.Version{})
	return store.RetainStored
}

func (r *exactRouter) RetainedFor(ns, filter string, now time.Time) []*message.Message {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if m, ok := r.retained[routeKey{ns, filter}]; ok && len(m.Payload) > 0 && !m.Expired(now) {
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
		if len(m.Payload) > 0 {
			out = append(out, m)
		}
	}
	return out
}
