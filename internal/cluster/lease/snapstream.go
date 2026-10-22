package lease

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
)

// tableMeta is the first record of the table's snapshot section: everything
// but the leases.
type tableMeta struct {
	Nodes  map[string]*NodeInfo `json:"nodes"`
	Level  uint64               `json:"level,omitempty"`
	LostAt map[string]uint64    `json:"lostAt,omitempty"`
}

// tableView is the table at one index. The leases and node records it points
// to are never written again (see cloneNode), so it costs a pointer each.
type tableView struct {
	meta   tableMeta
	leases []*Lease
}

// Capture implements consensus.Streamer.
func (t *Table) Capture() (consensus.SectionView, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	v := &tableView{meta: tableMeta{Nodes: make(map[string]*NodeInfo, len(t.nodes)), Level: t.level.Load(), LostAt: make(map[string]uint64, len(t.lostAt))}, leases: make([]*Lease, 0, len(t.leases))}
	for name, n := range t.nodes {
		v.meta.Nodes[name] = n
	}
	for name, i := range t.lostAt {
		v.meta.LostAt[name] = i
	}
	for _, l := range t.leases {
		v.leases = append(v.leases, l)
	}
	return v, nil
}

// Encoding implements consensus.SectionView.
func (v *tableView) Encoding() consensus.Encoding { return consensus.EncodingCompact }

// WriteRecords implements consensus.SectionView: the meta record, then one
// record per lease in key order.
func (v *tableView) WriteRecords(emit func([]byte) error) error {
	b, err := json.Marshal(v.meta)
	if err != nil {
		return err
	}
	if err := emit(b); err != nil {
		return err
	}
	sort.Slice(v.leases, func(i, j int) bool {
		if v.leases[i].Namespace != v.leases[j].Namespace {
			return v.leases[i].Namespace < v.leases[j].Namespace
		}
		return v.leases[i].ClientID < v.leases[j].ClientID
	})
	for _, l := range v.leases {
		if err := emit(encodeCompactLease(l)); err != nil {
			return err
		}
	}
	return nil
}

type stagedTable struct {
	t      *Table
	leases map[key]*Lease
	meta   tableMeta
}

// Stage implements consensus.Streamer.
func (t *Table) Stage(e consensus.Encoding, next func() ([]byte, bool, error)) (consensus.Staged, error) {
	if e != consensus.EncodingJSON && e != consensus.EncodingCompact {
		return nil, fmt.Errorf("%w: lease table encoding %d not supported", consensus.ErrSnapshotFormat, e)
	}
	rec, ok, err := next()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("lease table section: no meta record")
	}
	st := &stagedTable{t: t, leases: map[key]*Lease{}}
	if err := json.Unmarshal(rec, &st.meta); err != nil {
		return nil, fmt.Errorf("lease table meta: %w", err)
	}
	for {
		rec, ok, err := next()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		var l *Lease
		if e == consensus.EncodingCompact {
			if l, err = decodeCompactLease(rec); err != nil {
				return nil, err
			}
		} else {
			l = &Lease{}
			if err := json.Unmarshal(rec, l); err != nil {
				return nil, fmt.Errorf("lease record: %w", err)
			}
		}
		st.leases[key{l.Namespace, l.ClientID}] = l
	}
	return st, nil
}

// Install implements consensus.Staged, as Restore replaces the state.
func (s *stagedTable) Install() {
	t := s.t
	t.mu.Lock()
	t.leases = s.leases
	t.nodes = s.meta.Nodes
	if t.nodes == nil {
		t.nodes = map[string]*NodeInfo{}
	}
	t.level.Store(s.meta.Level)
	t.lostAt = s.meta.LostAt
	if t.lostAt == nil {
		t.lostAt = map[string]uint64{}
	}
	onRestore := t.onRestore
	t.mu.Unlock()
	if onRestore != nil {
		onRestore()
	}
}
