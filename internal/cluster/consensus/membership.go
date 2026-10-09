package consensus

import (
	"encoding/json"
	"sort"
	"sync"
)

// Membership maps Raft ids to names, addresses and roles. Roles come from
// the library's configuration state and are set by the shell when a
// configuration change applies; names and addresses come from the change's
// context bytes.
type Membership struct {
	mu      sync.RWMutex
	members map[uint64]*Member
}

func newMembership() *Membership { return &Membership{members: map[uint64]*Member{}} }

// Meta is what a configuration change carries in its context bytes.
type Meta struct {
	Name    string `json:"name"`
	ID      uint64 `json:"id"`
	Address string `json:"address"`
}

// EncodeMeta serialises the context of a configuration change.
func EncodeMeta(m Meta) []byte {
	b, _ := json.Marshal(m)
	return b
}

// DecodeMeta parses the context of a configuration change.
func DecodeMeta(b []byte) (Meta, bool) {
	var m Meta
	if len(b) == 0 || json.Unmarshal(b, &m) != nil || m.Name == "" {
		return Meta{}, false
	}
	return m, true
}

// Upsert records a node's name and address; the role is left as it is, or
// learner for a new row.
func (m *Membership) Upsert(meta Meta) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if row, ok := m.members[meta.ID]; ok {
		row.Name = meta.Name
		if meta.Address != "" {
			row.Address = meta.Address
		}
		return
	}
	m.members[meta.ID] = &Member{Name: meta.Name, ID: meta.ID, Address: meta.Address, Role: Learner}
}

// SetRoles applies the library's configuration state: voters and learners;
// ids in neither are removed.
func (m *Membership) SetRoles(voters, learners []uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	keep := map[uint64]Role{}
	for _, id := range voters {
		keep[id] = Voter
	}
	for _, id := range learners {
		if _, isVoter := keep[id]; !isVoter {
			keep[id] = Learner
		}
	}
	for id := range m.members {
		if _, ok := keep[id]; !ok {
			delete(m.members, id)
		}
	}
	for id, role := range keep {
		row, ok := m.members[id]
		if !ok {
			row = &Member{ID: id}
			m.members[id] = row
		}
		row.Role = role
	}
}

// SetCaughtUp records the leader's view of a member's progress.
func (m *Membership) SetCaughtUp(id uint64, caughtUp bool) {
	m.mu.Lock()
	if row, ok := m.members[id]; ok {
		row.CaughtUp = caughtUp
	}
	m.mu.Unlock()
}

// Lookup finds a member by id.
func (m *Membership) Lookup(id uint64) (Member, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	row, ok := m.members[id]
	if !ok {
		return Member{}, false
	}
	return *row, true
}

// ByName finds a member by name.
func (m *Membership) ByName(name string) (Member, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, row := range m.members {
		if row.Name == name {
			return *row, true
		}
	}
	return Member{}, false
}

// Has reports whether an id is a member.
func (m *Membership) Has(id uint64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.members[id]
	return ok
}

// List returns the members sorted by name.
func (m *Membership) List() []Member {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Member, 0, len(m.members))
	for _, row := range m.members {
		out = append(out, *row)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (m *Membership) snapshot() (json.RawMessage, error) {
	return json.Marshal(m.List())
}

func (m *Membership) restore(data json.RawMessage) error {
	var list []Member
	if len(data) > 0 {
		if err := json.Unmarshal(data, &list); err != nil {
			return err
		}
	}
	m.mu.Lock()
	m.members = map[uint64]*Member{}
	for i := range list {
		row := list[i]
		row.CaughtUp = false
		m.members[row.ID] = &row
	}
	m.mu.Unlock()
	return nil
}
