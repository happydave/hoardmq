package consensus

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
)

// Machine is the replicated state machine: components by kind, applied in
// log order, snapshotted as one versioned document whose sections are the
// components' bytes. Sections with no registered component are preserved
// across restore and snapshot so a reader that lacks a component (an older
// build during a rolling upgrade) does not lose them.
type Machine struct {
	mu         sync.RWMutex
	components map[Kind]Component
	unknown    map[Kind][]byte
	applied    uint64
	recovering bool
	membership *Membership
	config     *Config
	epoch      *Epoch
}

// NewMachine builds a machine with the built-in components: membership
// (kept by the shell), cluster configuration and the epoch.
func NewMachine() *Machine {
	m := &Machine{components: map[Kind]Component{}, unknown: map[Kind][]byte{}, membership: newMembership(), config: &Config{values: map[string]string{}}, epoch: &Epoch{}}
	m.components[KindConfig] = m.config
	m.components[KindEpoch] = m.epoch
	return m
}

// Register adds a component for a kind. It panics on a duplicate kind or a
// kind below KindReserved, which are the package's own.
func (m *Machine) Register(k Kind, c Component) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r, ok := c.(Recoverable); ok && m.recovering {
		r.SetRecovering(true)
	}
	if k < KindReserved {
		panic(fmt.Sprintf("consensus: kind %d is reserved", k))
	}
	if _, dup := m.components[k]; dup {
		panic(fmt.Sprintf("consensus: kind %d registered twice", k))
	}
	m.components[k] = c
	if data, ok := m.unknown[k]; ok {
		_ = c.Restore(data)
		delete(m.unknown, k)
	}
}

// Apply applies one committed command. The payload's first byte is the kind.
func (m *Machine) Apply(index uint64, cmd []byte) (any, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.applied = index
	if len(cmd) == 0 {
		return nil, nil
	}
	c, ok := m.components[Kind(cmd[0])]
	if !ok {
		return nil, fmt.Errorf("%w: %d", ErrUnknownKind, cmd[0])
	}
	return c.Apply(index, cmd[1:])
}

// BeginRecovery puts every Recoverable component into silent mode; nothing
// outward-facing runs until EndRecovery.
func (m *Machine) BeginRecovery() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.recovering {
		return
	}
	m.recovering = true
	for _, c := range m.components {
		if r, ok := c.(Recoverable); ok {
			r.SetRecovering(true)
		}
	}
}

// EndRecovery leaves silent mode and tells every Recoverable component, in
// kind order, to reconcile against the state it now holds.
func (m *Machine) EndRecovery() {
	m.mu.Lock()
	if !m.recovering {
		m.mu.Unlock()
		return
	}
	m.recovering = false
	kinds := make([]int, 0, len(m.components))
	for k := range m.components {
		kinds = append(kinds, int(k))
	}
	sort.Ints(kinds)
	var rs []Recoverable
	for _, k := range kinds {
		if r, ok := m.components[Kind(k)].(Recoverable); ok {
			rs = append(rs, r)
		}
	}
	m.mu.Unlock()
	for _, r := range rs {
		r.SetRecovering(false)
		r.Recovered()
	}
}

// Recovering reports whether the machine is in silent mode.
func (m *Machine) Recovering() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.recovering
}

// Applied is the index of the last command applied.
func (m *Machine) Applied() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.applied
}

// SetApplied records the applied index after a snapshot restore.
func (m *Machine) SetApplied(index uint64) {
	m.mu.Lock()
	m.applied = index
	m.mu.Unlock()
}

// Membership is the machine's member table.
func (m *Machine) Membership() *Membership { return m.membership }

// ConfigValue reads one cluster configuration value.
func (m *Machine) ConfigValue(key string) (string, bool) { return m.config.Get(key) }

// EpochValue reads the monotone epoch.
func (m *Machine) EpochValue() uint64 { return m.epoch.Value() }

// Encode builds a command for a kind.
func Encode(k Kind, payload []byte) []byte {
	out := make([]byte, 0, 1+len(payload))
	out = append(out, byte(k))
	return append(out, payload...)
}

// snapshotDoc is the project-owned machine snapshot: a version and the
// sections, keyed by kind.
type snapshotDoc struct {
	Version    int             `json:"version"`
	Applied    uint64          `json:"applied"`
	Membership json.RawMessage `json:"membership"`
	Sections   map[Kind][]byte `json:"sections"`
}

// snapshotVersion is the machine document version.
const snapshotVersion = 1

// Snapshot serialises every component and the membership.
func (m *Machine) Snapshot() ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	doc := snapshotDoc{Version: snapshotVersion, Applied: m.applied, Sections: map[Kind][]byte{}}
	for k, c := range m.components {
		data, err := c.Snapshot()
		if err != nil {
			return nil, fmt.Errorf("snapshot of kind %d: %w", k, err)
		}
		doc.Sections[k] = data
	}
	for k, data := range m.unknown {
		doc.Sections[k] = data
	}
	mem, err := m.membership.snapshot()
	if err != nil {
		return nil, err
	}
	doc.Membership = mem
	return json.Marshal(doc)
}

// Restore replaces every component's state from a snapshot document.
func (m *Machine) Restore(data []byte) error {
	var doc snapshotDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		return fmt.Errorf("machine snapshot: %w", err)
	}
	if doc.Version != snapshotVersion {
		return fmt.Errorf("machine snapshot: version %d not supported", doc.Version)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, section := range doc.Sections {
		if c, ok := m.components[k]; ok {
			if err := c.Restore(section); err != nil {
				return fmt.Errorf("restore of kind %d: %w", k, err)
			}
		} else {
			m.unknown[k] = section
		}
	}
	if err := m.membership.restore(doc.Membership); err != nil {
		return err
	}
	m.applied = doc.Applied
	return nil
}

// Config is the cluster-wide configuration component: a set of string
// values that must agree across nodes. A command is key, then value, each
// length-prefixed; an empty value deletes.
type Config struct {
	mu     sync.RWMutex
	values map[string]string
}

// ConfigCommand encodes a set or delete.
func ConfigCommand(key, value string) []byte {
	out := binary.BigEndian.AppendUint16(nil, uint16(len(key)))
	out = append(out, key...)
	out = binary.BigEndian.AppendUint16(out, uint16(len(value)))
	return Encode(KindConfig, append(out, value...))
}

// Apply sets or deletes a value and returns the previous value.
func (c *Config) Apply(_ uint64, payload []byte) (any, error) {
	if len(payload) < 2 {
		return nil, errors.New("config command: short")
	}
	n := int(binary.BigEndian.Uint16(payload))
	payload = payload[2:]
	if len(payload) < n+2 {
		return nil, errors.New("config command: short key")
	}
	key := string(payload[:n])
	payload = payload[n:]
	vn := int(binary.BigEndian.Uint16(payload))
	payload = payload[2:]
	if len(payload) != vn {
		return nil, errors.New("config command: value length")
	}
	value := string(payload)
	c.mu.Lock()
	defer c.mu.Unlock()
	prev := c.values[key]
	if value == "" {
		delete(c.values, key)
	} else {
		c.values[key] = value
	}
	return prev, nil
}

// Get reads a value.
func (c *Config) Get(key string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok := c.values[key]
	return v, ok
}

// Snapshot and Restore serialise the values as a sorted JSON object.
func (c *Config) Snapshot() ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return json.Marshal(c.values)
}

// Restore replaces the values.
func (c *Config) Restore(data []byte) error {
	values := map[string]string{}
	if len(data) > 0 {
		if err := json.Unmarshal(data, &values); err != nil {
			return err
		}
	}
	c.mu.Lock()
	c.values = values
	c.mu.Unlock()
	return nil
}

// Keys lists the configuration keys, sorted.
func (c *Config) Keys() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	keys := make([]string, 0, len(c.values))
	for k := range c.values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Epoch is the monotone counter component. A command's payload is ignored;
// each application advances the epoch by one and returns the new value.
type Epoch struct {
	mu    sync.RWMutex
	value uint64
}

// EpochCommand encodes an epoch bump.
func EpochCommand() []byte { return Encode(KindEpoch, nil) }

// Apply advances the epoch.
func (e *Epoch) Apply(_ uint64, _ []byte) (any, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.value++
	return e.value, nil
}

// Value reads the epoch.
func (e *Epoch) Value() uint64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.value
}

// Snapshot and Restore serialise the epoch.
func (e *Epoch) Snapshot() ([]byte, error) {
	return binary.BigEndian.AppendUint64(nil, e.Value()), nil
}

// Restore replaces the epoch.
func (e *Epoch) Restore(data []byte) error {
	if len(data) != 8 {
		return errors.New("epoch snapshot: length")
	}
	e.mu.Lock()
	e.value = binary.BigEndian.Uint64(data)
	e.mu.Unlock()
	return nil
}
