package records

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
)

type recordsMeta struct {
	Applied uint64 `json:"applied"`
}

// entryRecord is one record of the component's snapshot section.
type entryRecord struct {
	Collection string `json:"c"`
	Key        string `json:"k"`
	Record     Record `json:"r"`
}

// recordsView is the collections at one index: maps copied at capture, since
// an apply writes into them, holding Record values whose bytes a put never
// shares.
type recordsView struct {
	applied     uint64
	collections map[string]map[string]Record
}

// Capture implements consensus.Streamer.
func (c *Component) Capture() (consensus.SectionView, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v := &recordsView{applied: c.applied, collections: make(map[string]map[string]Record, len(c.collections))}
	for name, coll := range c.collections {
		cp := make(map[string]Record, len(coll))
		for k, r := range coll {
			cp[k] = r
		}
		v.collections[name] = cp
	}
	return v, nil
}

// Encoding implements consensus.SectionView.
func (v *recordsView) Encoding() consensus.Encoding { return consensus.EncodingJSON }

// WriteRecords implements consensus.SectionView: the meta record, then one
// record per entry in collection and key order.
func (v *recordsView) WriteRecords(emit func([]byte) error) error {
	b, err := json.Marshal(recordsMeta{Applied: v.applied})
	if err != nil {
		return err
	}
	if err := emit(b); err != nil {
		return err
	}
	names := make([]string, 0, len(v.collections))
	for name := range v.collections {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		coll := v.collections[name]
		keys := make([]string, 0, len(coll))
		for k := range coll {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			b, err := json.Marshal(entryRecord{Collection: name, Key: k, Record: coll[k]})
			if err != nil {
				return err
			}
			if err := emit(b); err != nil {
				return err
			}
		}
	}
	return nil
}

type stagedRecords struct {
	c           *Component
	applied     uint64
	collections map[string]map[string]Record
}

// Stage implements consensus.Streamer.
func (c *Component) Stage(e consensus.Encoding, next func() ([]byte, bool, error)) (consensus.Staged, error) {
	if e != consensus.EncodingJSON {
		return nil, fmt.Errorf("%w: records encoding %d not supported", consensus.ErrSnapshotFormat, e)
	}
	rec, ok, err := next()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("records section: no meta record")
	}
	var meta recordsMeta
	if err := json.Unmarshal(rec, &meta); err != nil {
		return nil, fmt.Errorf("records meta: %w", err)
	}
	st := &stagedRecords{c: c, applied: meta.Applied, collections: map[string]map[string]Record{}}
	for {
		rec, ok, err := next()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		var e entryRecord
		if err := json.Unmarshal(rec, &e); err != nil {
			return nil, fmt.Errorf("records entry: %w", err)
		}
		coll := st.collections[e.Collection]
		if coll == nil {
			coll = map[string]Record{}
			st.collections[e.Collection] = coll
		}
		coll[e.Key] = e.Record
	}
	return st, nil
}

// Install implements consensus.Staged, as Restore replaces the state.
func (s *stagedRecords) Install() {
	c := s.c
	c.mu.Lock()
	defer c.mu.Unlock()
	c.collections = s.collections
	c.advance(s.applied)
}
