// Package shardmap is the consensus component that assigns virtual shards
// to nodes: one leader and its followers per shard, computed
// deterministically from the live member set and changed only by proposal
// from the consensus leader.
package shardmap

import (
	"encoding/json"
	"errors"
	"hash/fnv"
	"sort"
	"sync"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
)

// Kind is the component's command kind in the state machine.
const Kind consensus.Kind = consensus.KindReserved + 1

// Shard is one virtual shard's assignment.
type Shard struct {
	Leader    string   `json:"leader"`
	Followers []string `json:"followers,omitempty"`
}

// Map is one version of the assignment. Basis is the sorted live node list
// it was computed from and Replicas the copy count it was computed with.
type Map struct {
	Version  uint64   `json:"version"`
	Basis    []string `json:"basis"`
	Replicas int      `json:"replicas"`
	Shards   []Shard  `json:"shards"`
}

// Result is what a proposal gets back.
type Result struct {
	Applied bool   `json:"applied"`
	Version uint64 `json:"version"`
}

// Observer is told of every applied map change with the previous and the
// new map. It runs on the apply goroutine and must not block.
type Observer func(old, current Map)

// Table is the component: the current map and its readers.
type Table struct {
	shards int

	mu         sync.RWMutex
	m          Map
	observers  []Observer
	recovering bool
}

// SetRecovering implements consensus.Recoverable: while recovering the map
// is updated without notifying observers.
func (t *Table) SetRecovering(on bool) {
	t.mu.Lock()
	t.recovering = on
	t.mu.Unlock()
}

// Recovered implements consensus.Recoverable: observers are told once, from
// the zero map to the current one, so they reconcile against the final state.
func (t *Table) Recovered() {
	t.mu.RLock()
	m := t.m
	observers := append([]Observer(nil), t.observers...)
	t.mu.RUnlock()
	if m.Version == 0 {
		return
	}
	for _, o := range observers {
		o(Map{}, m)
	}
}

// New makes an empty table for the given shard count. Until a map is
// applied, Leader reports the local node for every shard and Followers none.
func New(shards int) *Table {
	if shards <= 0 {
		shards = 1
	}
	return &Table{shards: shards}
}

// Compute assigns shards over the basis: shard i's leader is basis[i mod n]
// and its followers the next copies-1 nodes cyclically, where copies is
// bounded by the basis size. The basis is sorted first.
func Compute(version uint64, basis []string, replicas, shards int) Map {
	nodes := append([]string(nil), basis...)
	sort.Strings(nodes)
	m := Map{Version: version, Basis: nodes, Replicas: replicas, Shards: make([]Shard, shards)}
	n := len(nodes)
	if n == 0 {
		return m
	}
	copies := min(max(replicas, 1), n)
	for i := range m.Shards {
		leader := nodes[i%n]
		followers := make([]string, 0, copies-1)
		for j := 1; j < copies; j++ {
			followers = append(followers, nodes[(i+j)%n])
		}
		m.Shards[i] = Shard{Leader: leader, Followers: followers}
	}
	return m
}

// Encode serialises a map for proposal.
func Encode(m Map) []byte {
	b, _ := json.Marshal(m)
	return b
}

// Apply implements consensus.Component: a map with a higher version than the
// current one replaces it; anything else is a no-op reported as not applied.
func (t *Table) Apply(_ uint64, payload []byte) (any, error) {
	var m Map
	if err := json.Unmarshal(payload, &m); err != nil {
		return nil, err
	}
	if len(m.Shards) != t.shards {
		return nil, errors.New("shardmap: shard count does not match this node's configuration")
	}
	t.mu.Lock()
	if m.Version <= t.m.Version {
		v := t.m.Version
		t.mu.Unlock()
		return Result{Applied: false, Version: v}, nil
	}
	old := t.m
	t.m = m
	var observers []Observer
	if !t.recovering {
		observers = append(observers, t.observers...)
	}
	t.mu.Unlock()
	for _, o := range observers {
		o(old, m)
	}
	return Result{Applied: true, Version: m.Version}, nil
}

// Snapshot implements consensus.Component.
func (t *Table) Snapshot() ([]byte, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return json.Marshal(t.m)
}

// Restore implements consensus.Component.
func (t *Table) Restore(data []byte) error {
	var m Map
	if len(data) > 0 {
		if err := json.Unmarshal(data, &m); err != nil {
			return err
		}
	}
	t.mu.Lock()
	old := t.m
	if m.Version < old.Version {
		// The map only ever moves forward. A restore that carries an older
		// map than this node has applied would move replicas back to where
		// an earlier membership put them, and the holders of a copy would
		// drop it — which is how the last copy of a dead owner's session is
		// lost. Apply guards the same way.
		t.mu.Unlock()
		return nil
	}
	t.m = m
	var observers []Observer
	if !t.recovering {
		observers = append(observers, t.observers...)
	}
	t.mu.Unlock()
	if m.Version != old.Version {
		for _, o := range observers {
			o(old, m)
		}
	}
	return nil
}

// Observe registers a change observer.
func (t *Table) Observe(o Observer) {
	t.mu.Lock()
	t.observers = append(t.observers, o)
	t.mu.Unlock()
}

// Current returns a copy of the map.
func (t *Table) Current() Map {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.m
}

// Version is the current map version; zero before any map was applied.
func (t *Table) Version() uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.m.Version
}

// Count is the shard count.
func (t *Table) Count() int { return t.shards }

// ShardOf maps a namespaced name (a client identifier or a topic) to a
// shard.
func (t *Table) ShardOf(ns, name string) int { return ShardOf(ns, name, t.shards) }

// ShardOf is the hash: FNV-32a of the namespace, a separator and the name.
func ShardOf(ns, name string, shards int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(ns))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(name))
	return int(h.Sum32() % uint32(shards)) //nolint:gosec // shards is a small positive count
}

// Leader returns the shard's leader; local is returned before any map was
// applied.
func (t *Table) Leader(shard int, local string) string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if shard < 0 || shard >= len(t.m.Shards) {
		return local
	}
	return t.m.Shards[shard].Leader
}

// Holders returns the shard's leader followed by its followers; nil before
// any map was applied.
func (t *Table) Holders(shard int) []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return holders(t.m, shard)
}

func holders(m Map, shard int) []string {
	if shard < 0 || shard >= len(m.Shards) {
		return nil
	}
	s := m.Shards[shard]
	out := make([]string, 0, 1+len(s.Followers))
	out = append(out, s.Leader)
	return append(out, s.Followers...)
}

// Holders is the leader followed by the followers of a shard in this map.
func (m Map) Holders(shard int) []string { return holders(m, shard) }

// Leaders lists the distinct leaders of this map, sorted.
func (m Map) Leaders() []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range m.Shards {
		if s.Leader != "" && !seen[s.Leader] {
			seen[s.Leader] = true
			out = append(out, s.Leader)
		}
	}
	sort.Strings(out)
	return out
}

// Targets lists the nodes a session owned by owner is replicated to under
// this map: the shard's holders other than the owner, at most replicas-1
// of them.
func (m Map) Targets(shard int, owner string) []string {
	var out []string
	for _, h := range holders(m, shard) {
		if h != owner {
			out = append(out, h)
		}
	}
	if want := m.Replicas - 1; want >= 0 && len(out) > want {
		out = out[:want]
	}
	return out
}

// Holds reports whether node is a holder of the shard.
func (m Map) Holds(shard int, node string) bool {
	for _, h := range holders(m, shard) {
		if h == node {
			return true
		}
	}
	return false
}

// SameBasis reports whether the map was computed from this basis and copy
// count.
func (m Map) SameBasis(basis []string, replicas int) bool {
	if m.Replicas != replicas || len(m.Basis) != len(basis) {
		return false
	}
	sorted := append([]string(nil), basis...)
	sort.Strings(sorted)
	for i := range sorted {
		if sorted[i] != m.Basis[i] {
			return false
		}
	}
	return true
}
