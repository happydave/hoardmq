// Package routing is the cluster routing plane: a per-node table of route
// entries (topic filter to owning node) replicated by version and tombstone
// and merged by union, a matcher from topic to remote nodes and shared
// groups, and the forwarding of publishes over the peer stream in production
// order with one delivery per shared group across the cluster.
package routing

import (
	"sort"
	"strings"
	"sync"

	"github.com/happydave/hoardmq/internal/topic"
)

// Entry is one route: a filter (with its $share prefix when shared) held by
// an owner node at a version; a tombstone marks removal; Count is the
// owner's local member count for a shared group (1 for a direct filter).
type Entry struct {
	Namespace string `json:"ns"`
	Filter    string `json:"filter"`
	Owner     string `json:"owner"`
	Version   uint64 `json:"version"`
	Tombstone bool   `json:"tombstone,omitempty"`
	Count     int    `json:"count,omitempty"`
	// Batch, on the first entry of a delta only, is the sender's number for
	// the batch (routeproof.go). Cleared before the entry is merged.
	Batch uint64 `json:"batch,omitempty"`
}

type entryKey struct{ owner, ns, filter string }

// Table holds every owner's entries and answers matches.
type Table struct {
	mu      sync.RWMutex
	self    string
	entries map[entryKey]*Entry
	hidden  map[string]bool // owners whose entries are excluded from matching
	roots   map[string]*node
	// moving indexes the filters this node is handing over: their routes are
	// still advertised to the other nodes for the move grace, but the
	// sessions behind them have gone. It is a separate index because this
	// node cannot see its own routes through the ones above — Match skips
	// self and HiddenMatch reports only hidden peers — which is exactly how
	// the old owner came to answer for a session it no longer had (WI 1353).
	moving      map[string]*node // namespace → matcher over moving filters
	movingCount map[entryKey]int
	// liveLocal and liveRemote are the non-tombstoned entries owned by this
	// node and by others, kept as entries change so Count does not walk the
	// table: the gauge that reads it runs on every route update, and a walk
	// there took a third of a 100k storm's CPU (WI 1481).
	liveLocal, liveRemote int
}

// NewTable creates an empty table for a node.
func NewTable(self string) *Table {
	return &Table{self: self, entries: map[entryKey]*Entry{}, hidden: map[string]bool{}, roots: map[string]*node{},
		moving: map[string]*node{}, movingCount: map[entryKey]int{}}
}

// SetMoving records or clears a filter this node is handing over to another
// node. While it is set, MovingMatch reports topics it matches, so a publish
// arriving here is not acknowledged for a session that has already left.
func (t *Table) SetMoving(ns, filter string, on bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	k := entryKey{t.self, ns, filter}
	if on {
		if t.movingCount[k] > 0 {
			return
		}
		t.movingCount[k] = 1
		root := t.moving[ns]
		if root == nil {
			root = newNode()
			t.moving[ns] = root
		}
		_, inner := topic.SplitShared(filter)
		n := root.walk(inner, true)
		if n.direct == nil {
			n.direct = map[string]bool{}
		}
		n.direct[t.self] = true
		if n.moving == nil {
			n.moving = map[string]bool{}
		}
		n.moving[filter] = true
		return
	}
	if t.movingCount[k] == 0 {
		return
	}
	delete(t.movingCount, k)
	root := t.moving[ns]
	if root == nil {
		return
	}
	_, inner := topic.SplitShared(filter)
	if n := root.walk(inner, false); n != nil {
		// A shared filter and a direct one index at the same level; the
		// level answers while either is still moving.
		delete(n.moving, filter)
		if len(n.moving) == 0 {
			delete(n.direct, t.self)
		}
	}
	root.prune(inner)
	if len(t.movingCount) == 0 {
		delete(t.moving, ns)
	}
}

// MovingMatch reports whether a topic matches a filter this node is handing
// over. A shared filter is indexed by its inner filter, as the route index
// does, so $share/g/t/0 answers for topic t/0.
func (t *Table) MovingMatch(ns, name string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	root := t.moving[ns]
	if root == nil {
		return false
	}
	for _, n := range root.match(name) {
		if len(n.direct) > 0 {
			return true
		}
	}
	return false
}

// MovingFilters lists the filters this node is handing over that match a
// topic, as they were recorded (a shared one with its $share prefix).
func (t *Table) MovingFilters(ns, name string) []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	root := t.moving[ns]
	if root == nil {
		return nil
	}
	var out []string
	for _, n := range root.match(name) {
		for f := range n.moving {
			out = append(out, f)
		}
	}
	return out
}

// Known reports whether an owner holds a live entry for a filter that
// matching can see: not withdrawn, and the owner not hidden.
func (t *Table) Known(owner, ns, filter string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	e := t.entries[entryKey{owner, ns, filter}]
	return e != nil && !e.Tombstone && !t.hidden[owner]
}

// Moving reports how many filters this node is currently handing over, for
// the gauge and for tests.
func (t *Table) Moving() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.movingCount)
}

// Merge applies incoming entries: a higher version replaces, and at equal
// versions a tombstone wins. It returns the entries that changed the table.
func (t *Table) Merge(in []Entry) []Entry {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.merge(in)
}

// merge is Merge's body; the caller holds the lock.
func (t *Table) merge(in []Entry) []Entry {
	var changed []Entry
	for i := range in {
		e := in[i]
		k := entryKey{e.Owner, e.Namespace, e.Filter}
		cur := t.entries[k]
		if cur != nil {
			if e.Version < cur.Version || (e.Version == cur.Version && (cur.Tombstone || !e.Tombstone)) {
				continue
			}
		}
		c := e
		if cur != nil && !cur.Tombstone {
			t.live(k, -1)
		}
		if !c.Tombstone {
			t.live(k, 1)
		}
		t.entries[k] = &c
		t.reindex(k, &c)
		changed = append(changed, c)
	}
	return changed
}

// MergeFull applies an owner's complete entry set. Beyond Merge's per-entry
// rules it is authoritative about absence: an entry held for that owner which
// the set does not carry, and whose version is below the set's snapshot
// version, is withdrawn. That is what lets a node that has restarted retire
// the entries its previous incarnation published — it no longer knows they
// exist, so it can never tombstone them one by one, but it can say what it
// holds now.
//
// The snapshot version is taken from the owner's own version counter when the
// set is composed, so any entry it publishes afterwards carries a higher
// version and survives a set that was already in flight.
func (t *Table) MergeFull(owner string, in []Entry, snapshot uint64) (changed, withdrawn []Entry) {
	present := make(map[entryKey]bool, len(in))
	for i := range in {
		if in[i].Owner == owner {
			present[entryKey{owner, in[i].Namespace, in[i].Filter}] = true
		}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	changed = t.merge(in)
	for k, e := range t.entries {
		if k.owner != owner || present[k] || e.Tombstone || e.Version >= snapshot {
			continue
		}
		delete(t.entries, k)
		t.live(k, -1)
		tomb := *e
		tomb.Tombstone = true
		t.reindex(k, &tomb)
		withdrawn = append(withdrawn, tomb)
	}
	return changed, withdrawn
}

// live adjusts the live count for an entry's scope; the caller holds the lock.
func (t *Table) live(k entryKey, d int) {
	if k.owner == t.self {
		t.liveLocal += d
	} else {
		t.liveRemote += d
	}
}

// reindex updates the matcher for one entry; the caller holds the lock.
func (t *Table) reindex(k entryKey, e *Entry) {
	root := t.roots[e.Namespace]
	if root == nil {
		root = newNode()
		t.roots[e.Namespace] = root
	}
	share, inner := topic.SplitShared(e.Filter)
	n := root.walk(inner, true)
	if e.Tombstone {
		if share == "" {
			delete(n.direct, k.owner)
		} else if g := n.groups[share]; g != nil {
			delete(g, k.owner)
			if len(g) == 0 {
				delete(n.groups, share)
			}
		}
		root.prune(inner)
		return
	}
	if share == "" {
		if n.direct == nil {
			n.direct = map[string]bool{}
		}
		n.direct[k.owner] = true
		return
	}
	if n.groups == nil {
		n.groups = map[string]map[string]int{}
	}
	g := n.groups[share]
	if g == nil {
		g = map[string]int{}
		n.groups[share] = g
	}
	g[k.owner] = e.Count
}

// Hide excludes an owner's entries from matching (the peer is down) or
// includes them again.
func (t *Table) Hide(owner string, hidden bool) {
	t.mu.Lock()
	if hidden {
		t.hidden[owner] = true
	} else {
		delete(t.hidden, owner)
	}
	t.mu.Unlock()
}

// HiddenOwners lists the owners whose entries are hidden, sorted.
func (t *Table) HiddenOwners() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]string, 0, len(t.hidden))
	for owner := range t.hidden {
		out = append(out, owner)
	}
	sort.Strings(out)
	return out
}

// Forget deletes an owner's entries: the membership removed the node, or its
// sessions are served elsewhere.
func (t *Table) Forget(owner string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for k, e := range t.entries {
		if k.owner != owner {
			continue
		}
		delete(t.entries, k)
		if !e.Tombstone {
			t.live(k, -1)
		}
		tomb := *e
		tomb.Tombstone = true
		t.reindex(k, &tomb)
	}
	delete(t.hidden, owner)
}

// Own returns this node's live and tombstoned entries, for a full sync.
func (t *Table) Own() []Entry {
	return t.byOwner(t.self)
}

func (t *Table) byOwner(owner string) []Entry {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var out []Entry
	for k, e := range t.entries {
		if k.owner == owner {
			out = append(out, *e)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		return out[i].Filter < out[j].Filter
	})
	return out
}

// Lookup returns an owner's entry for a filter.
func (t *Table) Lookup(owner, ns, filter string) (Entry, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	e := t.entries[entryKey{owner, ns, filter}]
	if e == nil {
		return Entry{}, false
	}
	return *e, true
}

// Group is a shared group a topic matched: its key and the member count per
// node, hidden owners excluded.
type Group struct {
	Key     string // $share/{name}/{filter}
	Members map[string]int
}

// Match returns the remote nodes with a live direct entry matching the
// topic, and the shared groups matched with members per node (including
// this node's own count, so the owner election sees everyone).
// OwnCount is the subscriber count this node last advertised for a filter;
// zero when it advertises none.
func (t *Table) OwnCount(ns, filter string) int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if e := t.entries[entryKey{t.self, ns, filter}]; e != nil && !e.Tombstone {
		return e.Count
	}
	return 0
}

// HiddenMatch reports whether an owner whose entries are hidden (a peer
// the transport reports down) holds a route matching the topic: its
// sessions cannot receive the message until they are re-homed, so a QoS 1
// or 2 publish must not be acknowledged as if they had.
func (t *Table) HiddenMatch(ns, name string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	root := t.roots[ns]
	if root == nil {
		return false
	}
	for _, n := range root.match(name) {
		for owner := range n.direct {
			if t.hidden[owner] {
				return true
			}
		}
		for _, members := range n.groups {
			for owner, count := range members {
				if t.hidden[owner] && count > 0 {
					return true
				}
			}
		}
	}
	return false
}

func (t *Table) Match(ns, name string) (nodes []string, groups []Group) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	root := t.roots[ns]
	if root == nil {
		return nil, nil
	}
	seen := map[string]bool{}
	byKey := map[string]*Group{}
	for _, n := range root.match(name) {
		for owner := range n.direct {
			if owner == t.self || t.hidden[owner] || seen[owner] {
				continue
			}
			seen[owner] = true
			nodes = append(nodes, owner)
		}
		for share, members := range n.groups {
			key := topic.SharePrefix + share + "/" + n.filter
			g := byKey[key]
			if g == nil {
				g = &Group{Key: key, Members: map[string]int{}}
				byKey[key] = g
			}
			for owner, count := range members {
				if t.hidden[owner] || count <= 0 {
					continue
				}
				g.Members[owner] = count
			}
		}
	}
	sort.Strings(nodes)
	for _, g := range byKey {
		if len(g.Members) > 0 {
			groups = append(groups, *g)
		}
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].Key < groups[j].Key })
	return nodes, groups
}

// Owner elects a group's dispatch owner: the node with the most members,
// lowest name on ties.
func (g Group) Owner() string {
	best, bestCount := "", -1
	for node, count := range g.Members {
		if count > bestCount || (count == bestCount && node < best) {
			best, bestCount = node, count
		}
	}
	return best
}

// Nodes lists a group's member nodes sorted by name.
func (g Group) Nodes() []string {
	out := make([]string, 0, len(g.Members))
	for n := range g.Members {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Count is the number of live (non-tombstoned) entries by scope.
func (t *Table) Count() (local, remote int) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.liveLocal, t.liveRemote
}

// node is one level of the filter trie: direct owners, groups by share name
// with member counts per owner, children by level and the two wildcards.
type node struct {
	filter   string // the full inner filter at this node, for group keys
	direct   map[string]bool
	groups   map[string]map[string]int
	moving   map[string]bool // the moving index only: the full filters recorded here
	children map[string]*node
	plus     *node
	hash     *node
}

func newNode() *node { return &node{} }

// walk descends to the node for a filter, creating levels when create is
// set.
func (n *node) walk(filter string, create bool) *node {
	cur := n
	levels := strings.Split(filter, "/")
	for i, level := range levels {
		var next *node
		switch level {
		case "+":
			next = cur.plus
		case "#":
			next = cur.hash
		default:
			next = cur.children[level]
		}
		if next == nil {
			if !create {
				return nil
			}
			next = newNode()
			next.filter = strings.Join(levels[:i+1], "/")
			switch level {
			case "+":
				cur.plus = next
			case "#":
				cur.hash = next
			default:
				if cur.children == nil {
					cur.children = map[string]*node{}
				}
				cur.children[level] = next
			}
		}
		cur = next
	}
	return cur
}

func (n *node) empty() bool {
	return len(n.direct) == 0 && len(n.groups) == 0 && len(n.moving) == 0 && len(n.children) == 0 && n.plus == nil && n.hash == nil
}

// prune removes empty nodes along a filter's path.
func (n *node) prune(filter string) {
	levels := strings.Split(filter, "/")
	var path []*node
	cur := n
	for _, level := range levels {
		var next *node
		switch level {
		case "+":
			next = cur.plus
		case "#":
			next = cur.hash
		default:
			next = cur.children[level]
		}
		if next == nil {
			return
		}
		path = append(path, cur)
		cur = next
	}
	for i := len(levels) - 1; i >= 0; i-- {
		child := cur
		if !child.empty() {
			return
		}
		parent := path[i]
		switch levels[i] {
		case "+":
			parent.plus = nil
		case "#":
			parent.hash = nil
		default:
			delete(parent.children, levels[i])
		}
		cur = parent
	}
}

// match collects the nodes whose filters match a topic name, following the
// specification's rules: + matches one level, # the rest, and a filter that
// starts with a wildcard does not match a name that starts with $.
func (n *node) match(name string) []*node {
	levels := strings.Split(name, "/")
	var out []*node
	var walk func(cur *node, i int)
	walk = func(cur *node, i int) {
		if i == len(levels) {
			if len(cur.direct) > 0 || len(cur.groups) > 0 {
				out = append(out, cur)
			}
			if cur.hash != nil {
				out = append(out, cur.hash)
			}
			return
		}
		if cur.hash != nil {
			out = append(out, cur.hash)
		}
		if child := cur.children[levels[i]]; child != nil {
			walk(child, i+1)
		}
		if cur.plus != nil {
			walk(cur.plus, i+1)
		}
	}
	if strings.HasPrefix(name, "$") {
		// Only exact first levels may match a $-topic.
		if child := n.children[levels[0]]; child != nil {
			walk(child, 1)
		}
		return out
	}
	walk(n, 0)
	return out
}
