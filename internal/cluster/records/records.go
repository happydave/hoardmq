// Package records is a replicated store of named collections of keyed
// records, applied from the consensus log: every node applies the same
// writes at the same log positions, so a record is the same on every node
// once its write has applied there. A record's version is the log index of
// its last write, which a writer can make its next write conditional on.
package records

import (
	extrecords "github.com/happydave/hoardmq/ext/records"

	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
)

// Kind is the component's command kind.
const Kind consensus.Kind = consensus.KindReserved + 2

// Bounds.
const (
	MaxCollection = 64
	MaxKey        = 256
	MaxValue      = 8 << 10
	// DefaultMaxRecords bounds a collection.
	DefaultMaxRecords = 100000
)

// The record values are the extension API's.
type (
	Record = extrecords.Record
	Entry  = extrecords.Entry
	Result = extrecords.Result
)

// Op is one write.
type Op struct {
	Delete     bool            `json:"del,omitempty"`
	Collection string          `json:"c"`
	Key        string          `json:"k"`
	Value      json.RawMessage `json:"v,omitempty"`
	// If makes the write conditional: 0 requires that the record not exist,
	// N that it be at version N. Nil is unconditional.
	If *uint64   `json:"if,omitempty"`
	By string    `json:"by,omitempty"`
	At time.Time `json:"at"`
}

// Refusals.
const (
	RefusedExists  = "exists"
	RefusedMissing = "missing"
	RefusedVersion = "version"
	RefusedFull    = "full"
	RefusedInvalid = "invalid"
)

// Component is the replicated state.
type Component struct {
	maxRecords int

	mu          sync.RWMutex
	collections map[string]map[string]Record
	applied     uint64
	changed     chan struct{}
}

// New builds an empty component; maxRecords of zero is DefaultMaxRecords.
func New(maxRecords int) *Component {
	if maxRecords <= 0 {
		maxRecords = DefaultMaxRecords
	}
	return &Component{maxRecords: maxRecords, collections: map[string]map[string]Record{}, changed: make(chan struct{})}
}

// ValidName reports whether a collection name is well formed: dot-separated
// lower-case words, each starting with a letter.
func ValidName(c string) bool {
	if c == "" || len(c) > MaxCollection {
		return false
	}
	start := true
	for i := 0; i < len(c); i++ {
		ch := c[i]
		switch {
		case ch >= 'a' && ch <= 'z':
		case ch >= '0' && ch <= '9' && !start:
		case ch == '.' && !start && i < len(c)-1:
			start = true
			continue
		default:
			return false
		}
		start = false
	}
	return true
}

// Decide is the decision on one write against the collection's current
// state: whether it applies, and why not. It reads nothing but its
// arguments.
func Decide(op Op, current Record, exists bool, count, maxRecords int) string {
	if !ValidName(op.Collection) || op.Key == "" || len(op.Key) > MaxKey || !utf8.ValidString(op.Key) {
		return RefusedInvalid
	}
	if op.If != nil {
		switch {
		case *op.If == 0 && exists:
			return RefusedExists
		case *op.If != 0 && (!exists || current.Version != *op.If):
			if !exists && op.Delete {
				return RefusedMissing
			}
			return RefusedVersion
		}
	}
	if op.Delete {
		if !exists {
			return RefusedMissing
		}
		return ""
	}
	if len(op.Value) > MaxValue || !isObject(op.Value) {
		return RefusedInvalid
	}
	if !exists && count >= maxRecords {
		return RefusedFull
	}
	return ""
}

func isObject(v json.RawMessage) bool {
	var m map[string]json.RawMessage
	return json.Unmarshal(v, &m) == nil && m != nil
}

// Apply implements consensus.Component.
func (c *Component) Apply(index uint64, payload []byte) (any, error) {
	var op Op
	if err := json.Unmarshal(payload, &op); err != nil {
		c.mu.Lock()
		c.advance(index)
		c.mu.Unlock()
		return Result{Index: index, Reason: RefusedInvalid}, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.advance(index)
	coll := c.collections[op.Collection]
	cur, exists := coll[op.Key]
	res := Result{Index: index, Version: cur.Version}
	if why := Decide(op, cur, exists, len(coll), c.maxRecords); why != "" {
		res.Reason = why
		return res, nil
	}
	if exists {
		prev := cur
		res.Previous = &prev
	}
	if op.Delete {
		delete(coll, op.Key)
		if len(coll) == 0 {
			delete(c.collections, op.Collection)
		}
		res.OK, res.Version = true, 0
		return res, nil
	}
	if coll == nil {
		coll = map[string]Record{}
		c.collections[op.Collection] = coll
	}
	created := index
	if exists {
		created = cur.Created
	}
	coll[op.Key] = Record{Value: append(json.RawMessage(nil), op.Value...), Version: index, Created: created, By: op.By, At: op.At}
	res.OK, res.Version = true, index
	return res, nil
}

// advance records the applied index and wakes the waiters. Called with mu
// held.
func (c *Component) advance(index uint64) {
	if index > c.applied {
		c.applied = index
	}
	close(c.changed)
	c.changed = make(chan struct{})
}

// Get reads one record.
func (c *Component) Get(collection, key string) (Record, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	r, ok := c.collections[collection][key]
	return r, ok
}

// List reads a collection in key order, after the cursor, at most limit;
// more reports whether any are left.
func (c *Component) List(collection, after string, limit int) (items []Entry, more bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	coll := c.collections[collection]
	keys := make([]string, 0, len(coll))
	for k := range coll {
		if k > after {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	if len(keys) > limit {
		keys, more = keys[:limit], true
	}
	for _, k := range keys {
		items = append(items, Entry{Key: k, Record: coll[k]})
	}
	return items, more
}

// Counts is every collection's size.
func (c *Component) Counts() map[string]int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]int, len(c.collections))
	for name, coll := range c.collections {
		out[name] = len(coll)
	}
	return out
}

// Applied is the last log index applied to this component.
func (c *Component) Applied() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.applied
}

// WaitApplied returns once index has applied here, or with ctx's error.
func (c *Component) WaitApplied(ctx context.Context, index uint64) error {
	for {
		c.mu.RLock()
		done, ch := c.applied >= index, c.changed
		c.mu.RUnlock()
		if done {
			return nil
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

type snapshot struct {
	Applied     uint64                       `json:"applied"`
	Collections map[string]map[string]Record `json:"collections"`
}

// Snapshot implements consensus.Component.
func (c *Component) Snapshot() ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return json.Marshal(snapshot{Applied: c.applied, Collections: c.collections})
}

// Restore implements consensus.Component: the snapshot replaces the state.
func (c *Component) Restore(data []byte) error {
	var s snapshot
	if len(data) > 0 {
		if err := json.Unmarshal(data, &s); err != nil {
			return fmt.Errorf("records snapshot: %w", err)
		}
	}
	if s.Collections == nil {
		s.Collections = map[string]map[string]Record{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.collections = s.Collections
	c.advance(s.Applied)
	return nil
}

// Proposer puts a command of a kind through the consensus core, wherever the
// leader is, and returns the component's result as JSON.
type Proposer func(ctx context.Context, kind consensus.Kind, payload []byte) (json.RawMessage, error)

// ErrUnavailable is a write that could not be proposed or confirmed.
var ErrUnavailable = errors.New("records: the cluster did not confirm the write")

// Store is the component's front: writes through the proposer, reads from
// this node's applied state.
type Store struct {
	*Component
	Propose Proposer
}

// Put writes a record; ifVersion, when not nil, is its condition.
func (s *Store) Put(ctx context.Context, collection, key string, value json.RawMessage, ifVersion *uint64, by string, at time.Time) (Result, error) {
	return s.write(ctx, Op{Collection: collection, Key: key, Value: value, If: ifVersion, By: by, At: at})
}

// Delete removes a record; ifVersion, when not nil, is its condition.
func (s *Store) Delete(ctx context.Context, collection, key string, ifVersion *uint64, by string, at time.Time) (Result, error) {
	return s.write(ctx, Op{Delete: true, Collection: collection, Key: key, If: ifVersion, By: by, At: at})
}

// write proposes the operation and returns once this node has applied it,
// so a read here after it sees it.
func (s *Store) write(ctx context.Context, op Op) (Result, error) {
	payload, err := json.Marshal(op)
	if err != nil {
		return Result{}, err
	}
	raw, err := s.Propose(ctx, Kind, payload)
	if err != nil {
		return Result{}, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	var res Result
	if err := json.Unmarshal(raw, &res); err != nil {
		return Result{}, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	if err := s.WaitApplied(ctx, res.Index); err != nil {
		return res, fmt.Errorf("%w: applied at %d, not yet here: %v", ErrUnavailable, res.Index, err)
	}
	return res, nil
}
