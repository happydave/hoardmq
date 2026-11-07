package dispatch

import (
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/session"
	"github.com/happydave/hoardmq/internal/store"
	"github.com/happydave/hoardmq/internal/topic"
)

// retained holds the last retained message per topic, per namespace.
// Lookups hold the read lock only while they walk the index, so they run
// alongside each other and a write waits for no more than a walk.
type retained struct {
	mu   sync.RWMutex
	byNS map[string]*retainedNS
	// n is the number the last stamp took; stamps only rise from it, across
	// expiry, drops and restarts (SeedRetainedVersion).
	n atomic.Uint64
}

// retainedNS is one namespace's retained messages: by topic, and as a trie of
// topic levels that a wildcard lookup walks instead of every topic.
type retainedNS struct {
	topics map[string]*message.Message
	root   *rnode
	// vers is each stamped entry's version; an entry absent from it is
	// unversioned. A tombstone is an entry with an empty payload: held, and
	// counted against the limit, never served.
	vers map[string]store.Version
	// appended is the journal's append count at each entry's record, 0 until
	// it appended (MarkRetainedAppended).
	appended map[string]uint64
}

// rnode is one topic level in a namespace's trie; msg is set when a retained
// topic ends at it.
type rnode struct {
	children map[string]*rnode
	msg      *message.Message
}

// walkHook, when set, runs inside a wildcard lookup's walk, under the read
// lock: tests hold a lookup there.
var walkHook func()

func newRetained() *retained { return &retained{byNS: map[string]*retainedNS{}} }

// RetainedEntry is session.RetainedEntry, the router's held entry.
type RetainedEntry = session.RetainedEntry

// nsFor returns a namespace's index, made on first use. The write lock is held.
func (r *retained) nsFor(name string) *retainedNS {
	ns := r.byNS[name]
	if ns == nil {
		ns = &retainedNS{topics: map[string]*message.Message{}, root: &rnode{}, vers: map[string]store.Version{}, appended: map[string]uint64{}}
		r.byNS[name] = ns
	}
	return ns
}

// put stores m (a tombstone when its payload is empty) at version v in its
// namespace's index. The write lock is held.
func (r *retained) put(m *message.Message, v store.Version) {
	ns := r.nsFor(m.Namespace)
	ns.topics[m.Topic] = m
	if v.Zero() {
		delete(ns.vers, m.Topic)
	} else {
		ns.vers[m.Topic] = v
	}
	delete(ns.appended, m.Topic)
	n := ns.root
	for _, level := range strings.Split(m.Topic, "/") {
		next := n.children[level]
		if next == nil {
			if n.children == nil {
				n.children = map[string]*rnode{}
			}
			next = &rnode{}
			n.children[level] = next
		}
		n = next
	}
	n.msg = m
}

// held is what a namespace holds for a topic. The lock is held.
func (r *retained) held(name, t string) (e RetainedEntry, ok bool) {
	ns := r.byNS[name]
	if ns == nil {
		return e, false
	}
	m, ok := ns.topics[t]
	if !ok {
		return e, false
	}
	return RetainedEntry{Msg: m, RV: ns.vers[t], Tombstone: len(m.Payload) == 0, Appended: ns.appended[t]}, true
}

// seed raises the stamp counter to at least n.
func (r *retained) seed(n uint64) {
	for {
		c := r.n.Load()
		if c >= n || r.n.CompareAndSwap(c, n) {
			return
		}
	}
}

// stampUnder installs a leader's client write (a clear when its payload is
// empty) stamped (epoch, the next number), in one critical section, unless it
// would add a topic past limit (0 is none): a tombstone counts as a held
// topic, so a clear of a held topic always fits and a clear of an unheld one
// needs room. It reports the version and whether it installed.
func (r *retained) stampUnder(m *message.Message, epoch uint64, limit int) (store.Version, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ns := r.byNS[m.Namespace]
	held, holds := 0, false
	if ns != nil {
		held = len(ns.topics)
		_, holds = ns.topics[m.Topic]
	}
	if !retainedFits(held, limit, holds, false) {
		return store.Version{}, false
	}
	v := store.Version{E: epoch, N: r.n.Add(1)}
	r.put(m, v)
	return v, true
}

// merge installs a copy at version v by store.MergeRetained (handOver for a
// hand-over or hand-on path) and reports whether it did, with what is held
// after. An empty payload at the zero Version is a copy drop, outside merge.
func (r *retained) merge(m *message.Message, v store.Version, handOver bool) (installed bool, after RetainedEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(m.Payload) == 0 && v.Zero() {
		if ns := r.byNS[m.Namespace]; ns != nil {
			if _, ok := ns.topics[m.Topic]; ok {
				r.remove(m.Namespace, ns, m.Topic)
			}
		}
		return true, RetainedEntry{}
	}
	cur, exists := r.held(m.Namespace, m.Topic)
	if !store.MergeRetained(cur.RV, exists, v, handOver) {
		return false, cur
	}
	if v.N > r.n.Load() {
		r.seed(v.N)
	}
	r.put(m, v)
	after, _ = r.held(m.Namespace, m.Topic)
	return true, after
}

// markAppended records the journal's append count at a topic's record, while
// the entry is still m at version v: two unversioned writes share a version,
// so the version alone cannot tell the entry appended from a later one.
func (r *retained) markAppended(m *message.Message, v store.Version, count uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if ns := r.byNS[m.Namespace]; ns != nil {
		if ns.topics[m.Topic] == m && ns.vers[m.Topic] == v {
			ns.appended[m.Topic] = count
		}
	}
}

// dropTombstoneIf removes a topic's tombstone only while it is the one at
// version v, and reports whether it did.
func (r *retained) dropTombstoneIf(name, t string, v store.Version) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	ns := r.byNS[name]
	if ns == nil {
		return false
	}
	m, ok := ns.topics[t]
	if !ok || len(m.Payload) != 0 || ns.vers[t] != v {
		return false
	}
	r.remove(name, ns, t)
	return true
}

// entries lists every held entry, tombstones included.
func (r *retained) entries() []RetainedEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []RetainedEntry
	for _, ns := range r.byNS {
		for t, m := range ns.topics {
			out = append(out, RetainedEntry{Msg: m, RV: ns.vers[t], Tombstone: len(m.Payload) == 0, Appended: ns.appended[t]})
		}
	}
	return out
}

// sweepLeading is sweep for a node with replication: an expired message on a
// shard this node leads (leads gives the epoch of the map it leads it under)
// becomes a tombstone stamped fresh, returned for the caller to journal; on
// another shard it is removed here alone, as a copy drop.
func (r *retained) sweepLeading(now time.Time, leads func(ns, topic string) (uint64, bool)) []RetainedEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	var stamped []RetainedEntry
	for name, ns := range r.byNS {
		for t, m := range ns.topics {
			if len(m.Payload) == 0 || !m.Expired(now) {
				continue
			}
			if epoch, ok := leads(name, t); ok && epoch > 0 {
				tomb := &message.Message{Namespace: name, Topic: t, Retain: true}
				v := store.Version{E: epoch, N: r.n.Add(1)}
				r.put(tomb, v)
				stamped = append(stamped, RetainedEntry{Msg: tomb, RV: v, Tombstone: true})
				continue
			}
			r.remove(name, ns, t)
		}
	}
	return stamped
}

// retainedFits decides whether a namespace holding held retained topics may
// store a write under limit (0 is none): a delete or a replacement of a topic
// it holds always fits.
func retainedFits(held, limit int, holds, deletes bool) bool {
	return deletes || holds || limit == 0 || held < limit
}

// set stores a message unversioned or, for an empty payload, removes the
// entry, whatever is held: a copy drop.
func (r *retained) set(m *message.Message) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(m.Payload) > 0 {
		r.put(m, store.Version{})
		return
	}
	if ns := r.byNS[m.Namespace]; ns != nil {
		if _, ok := ns.topics[m.Topic]; ok {
			r.remove(m.Namespace, ns, m.Topic)
		}
	}
}

// setUnder stores a new unversioned write, a message or, for an empty
// payload, a removal, unless the topic holds a versioned entry
// (store.UnversionedRefused) or the write would add a topic to a namespace
// already holding limit topics (0 is none). It reports why it did not.
func (r *retained) setUnder(m *message.Message, limit int) store.RetainRefusal {
	r.mu.Lock()
	defer r.mu.Unlock()
	ns := r.byNS[m.Namespace]
	deletes := len(m.Payload) == 0
	held, holds := 0, false
	if ns != nil {
		held = len(ns.topics)
		_, holds = ns.topics[m.Topic]
	}
	if holds && store.UnversionedRefused(ns.vers[m.Topic], true) {
		return store.RetainVersionedHeld
	}
	if !retainedFits(held, limit, holds, deletes) {
		return store.RetainOverLimit
	}
	if deletes {
		if holds {
			r.remove(m.Namespace, ns, m.Topic)
		}
		return store.RetainStored
	}
	r.put(m, store.Version{})
	return store.RetainStored
}

// remove drops a held topic from its namespace, pruning the trie nodes it
// leaves empty, and the namespace once it holds nothing. The write lock is
// held.
func (r *retained) remove(name string, ns *retainedNS, t string) {
	delete(ns.topics, t)
	delete(ns.vers, t)
	delete(ns.appended, t)
	levels := strings.Split(t, "/")
	path := make([]*rnode, 0, len(levels)+1)
	n := ns.root
	for _, level := range levels {
		path = append(path, n)
		n = n.children[level]
		if n == nil {
			return
		}
	}
	n.msg = nil
	for i := len(levels) - 1; i >= 0 && n.msg == nil && len(n.children) == 0; i-- {
		delete(path[i].children, levels[i])
		n = path[i]
	}
	if len(ns.topics) == 0 {
		delete(r.byNS, name)
	}
}

// matching returns the live retained messages a filter matches, in byte
// order of topic name. Expired entries are left out, and removed once the
// read lock is released.
func (r *retained) matching(ns, filter string, now time.Time) []*message.Message {
	out, expired := r.lookup(ns, filter, now)
	if len(expired) > 0 {
		r.dropExpired(ns, expired, now)
	}
	return out
}

// dropExpired removes the expired unversioned messages a lookup met, each
// only while it is still its topic's message: a write since the read lock was
// released may have replaced it. A versioned one is left to the sweep, which
// at the shard's leader leaves a tombstone at a fresh stamp; removed here, an
// older copy handed over later would install into the emptied topic.
func (r *retained) dropExpired(name string, expired []*message.Message, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ns := r.byNS[name]
	if ns == nil {
		return
	}
	for _, m := range expired {
		if _, versioned := ns.vers[m.Topic]; versioned {
			continue
		}
		if ns.topics[m.Topic] == m && m.Expired(now) {
			r.remove(name, ns, m.Topic)
		}
	}
}

func (r *retained) lookup(name, filter string, now time.Time) (out, expired []*message.Message) {
	found := r.find(name, filter)
	// Outside the lock: a stored message is replaced, never changed.
	dollarRule := strings.HasPrefix(filter, "+") || strings.HasPrefix(filter, "#")
	for _, m := range found {
		// MQTT-4.7.2-1: a filter starting with a wildcard does not match a
		// topic starting with '$'. The walk follows every other rule.
		if dollarRule && strings.HasPrefix(m.Topic, "$") {
			continue
		}
		if len(m.Payload) == 0 {
			continue // a tombstone: held, never served
		}
		if m.Expired(now) {
			expired = append(expired, m)
			continue
		}
		out = append(out, m)
	}
	slices.SortFunc(out, func(a, b *message.Message) int { return strings.Compare(a.Topic, b.Topic) })
	return out, expired
}

// find returns the messages a filter's levels reach, unordered, under the
// read lock.
func (r *retained) find(name, filter string) []*message.Message {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ns := r.byNS[name]
	if ns == nil {
		return nil
	}
	if !topic.HasWildcard(filter) {
		if m, ok := ns.topics[filter]; ok {
			return []*message.Message{m}
		}
		return nil
	}
	if walkHook != nil {
		walkHook()
	}
	var found []*message.Message
	collect(ns.root, strings.Split(filter, "/"), &found)
	return found
}

// collect appends the messages under n that the filter levels can reach: a
// literal level its child, '+' every child, '#' n and everything below it.
func collect(n *rnode, levels []string, found *[]*message.Message) {
	if len(levels) == 0 {
		if n.msg != nil {
			*found = append(*found, n.msg)
		}
		return
	}
	switch levels[0] {
	case "#":
		subtree(n, found)
	case "+":
		for _, c := range n.children {
			collect(c, levels[1:], found)
		}
	default:
		if c := n.children[levels[0]]; c != nil {
			collect(c, levels[1:], found)
		}
	}
}

func subtree(n *rnode, found *[]*message.Message) {
	if n.msg != nil {
		*found = append(*found, n.msg)
	}
	for _, c := range n.children {
		subtree(c, found)
	}
}

// sweep drops every expired entry.
func (r *retained) sweep(now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for name, ns := range r.byNS {
		for t, m := range ns.topics {
			if len(m.Payload) > 0 && m.Expired(now) {
				r.remove(name, ns, t)
			}
		}
	}
}

// all lists every entry.
func (r *retained) all() []*message.Message {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []*message.Message
	for _, ns := range r.byNS {
		for _, m := range ns.topics {
			if len(m.Payload) > 0 {
				out = append(out, m)
			}
		}
	}
	return out
}

// counts is the number of retained messages in each namespace holding one.
func (r *retained) counts() map[string]int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]int, len(r.byNS))
	for name, ns := range r.byNS {
		n := 0
		for _, m := range ns.topics {
			if len(m.Payload) > 0 {
				n++
			}
		}
		if n > 0 {
			out[name] = n
		}
	}
	return out
}

// count is the number of retained messages, for tests and metrics.
func (r *retained) count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n := 0
	for _, ns := range r.byNS {
		for _, m := range ns.topics {
			if len(m.Payload) > 0 {
				n++
			}
		}
	}
	return n
}
