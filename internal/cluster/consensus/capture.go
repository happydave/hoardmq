package consensus

import (
	"encoding/json"
	"fmt"
	"sort"
)

// Streamer is a component whose snapshot is written and read as a stream of
// records, so neither side holds the whole section at once. A component
// without it is captured by its Snapshot bytes as one record.
type Streamer interface {
	// Capture returns a view of the component's state as it stands, which
	// later applies must not change. It is called on the consensus loop
	// between applies and must be cheap: a copy of pointers or of small maps.
	Capture() (SectionView, error)
	// Stage builds new state from a section's records without touching the
	// live state; next returns ok false after the last record.
	Stage(e Encoding, next func() ([]byte, bool, error)) (Staged, error)
}

// SectionView is a captured state, written off the consensus loop.
type SectionView interface {
	Encoding() Encoding
	WriteRecords(emit func([]byte) error) error
}

// Staged is a component's state built from a snapshot, waiting to replace
// the live state.
type Staged interface {
	// Install replaces the live state. It is called on the consensus loop
	// with the machine's lock held.
	Install()
}

// rawSection is a section kept as its records: one no registered component
// claims, preserved so a build that lacks the component does not lose it.
type rawSection struct {
	enc     Encoding
	records [][]byte
}

type capturedSection struct {
	kind Kind
	view SectionView // a streamer's view, or nil
	raw  rawSection  // otherwise
}

// Capture is the machine's state at one applied index.
type Capture struct {
	applied    uint64
	membership []byte
	sections   []capturedSection
}

// Applied is the index the capture holds the state at.
func (c *Capture) Applied() uint64 { return c.applied }

// Capture takes the machine's state as it stands. It is called on the
// consensus loop, so no apply runs between the components' views.
func (m *Machine) Capture() (*Capture, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	c := &Capture{applied: m.applied}
	mem, err := m.membership.snapshot()
	if err != nil {
		return nil, err
	}
	c.membership = mem
	kinds := make([]int, 0, len(m.components)+len(m.unknown))
	for k := range m.components {
		kinds = append(kinds, int(k))
	}
	for k := range m.unknown {
		if _, ok := m.components[k]; !ok {
			kinds = append(kinds, int(k))
		}
	}
	sort.Ints(kinds)
	for _, ki := range kinds {
		k := Kind(ki)
		comp, ok := m.components[k]
		if !ok {
			c.sections = append(c.sections, capturedSection{kind: k, raw: m.unknown[k]})
			continue
		}
		if s, ok := comp.(Streamer); ok {
			view, err := s.Capture()
			if err != nil {
				return nil, fmt.Errorf("capture of kind %d: %w", k, err)
			}
			c.sections = append(c.sections, capturedSection{kind: k, view: view})
			continue
		}
		data, err := comp.Snapshot()
		if err != nil {
			return nil, fmt.Errorf("snapshot of kind %d: %w", k, err)
		}
		c.sections = append(c.sections, capturedSection{kind: k, raw: rawSection{enc: EncodingBlob, records: [][]byte{data}}})
	}
	return c, nil
}

// Write writes the captured sections, the membership first.
func (c *Capture) Write(fw *FileWriter) error {
	if err := writeRaw(fw, KindMembership, rawSection{enc: EncodingBlob, records: [][]byte{c.membership}}); err != nil {
		return err
	}
	for _, s := range c.sections {
		if s.view == nil {
			if err := writeRaw(fw, s.kind, s.raw); err != nil {
				return err
			}
			continue
		}
		if err := fw.Section(s.kind, s.view.Encoding()); err != nil {
			return err
		}
		if err := s.view.WriteRecords(fw.Record); err != nil {
			return fmt.Errorf("records of kind %d: %w", s.kind, err)
		}
		if err := fw.EndSection(); err != nil {
			return err
		}
	}
	return nil
}

func writeRaw(fw *FileWriter, k Kind, raw rawSection) error {
	if err := fw.Section(k, raw.enc); err != nil {
		return err
	}
	for _, r := range raw.records {
		if err := fw.Record(r); err != nil {
			return err
		}
	}
	return fw.EndSection()
}

// StagedMachine is a whole snapshot decoded and waiting to be installed.
type StagedMachine struct {
	m          *Machine
	applied    uint64
	membership json.RawMessage
	staged     map[Kind]Staged
	blobs      map[Kind][]byte
	unknown    map[Kind]rawSection
}

// Applied is the index the staged state is at.
func (s *StagedMachine) Applied() uint64 { return s.applied }

// Stage decodes a snapshot file into new state for every component, leaving
// the live state alone. It refuses an encoding a component does not take
// and a component's blob that is not exactly one record.
func (m *Machine) Stage(fr *FileReader, hd FileHeader) (*StagedMachine, error) {
	m.mu.RLock()
	comps := make(map[Kind]Component, len(m.components))
	for k, c := range m.components {
		comps[k] = c
	}
	m.mu.RUnlock()
	st := &StagedMachine{m: m, applied: hd.Applied, staged: map[Kind]Staged{}, blobs: map[Kind][]byte{}, unknown: map[Kind]rawSection{}}
	seenMembership := false
	for {
		k, e, ok, err := fr.NextSection()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		next := fr.Record
		comp, registered := comps[k]
		streamer, streams := comp.(Streamer)
		switch {
		case k == KindMembership:
			rec, err := oneRecord(k, e, next)
			if err != nil {
				return nil, err
			}
			st.membership, seenMembership = rec, true
		case registered && streams && e != EncodingBlob:
			ended := false
			tracked := func() ([]byte, bool, error) {
				if ended {
					return nil, false, nil
				}
				rec, ok, err := next()
				if !ok && err == nil {
					ended = true
				}
				return rec, ok, err
			}
			staged, err := streamer.Stage(e, tracked)
			if err != nil {
				return nil, fmt.Errorf("stage of kind %d: %w", k, err)
			}
			if !ended {
				if err := drain(next); err != nil {
					return nil, err
				}
			}
			st.staged[k] = staged
		case registered:
			rec, err := oneRecord(k, e, next)
			if err != nil {
				return nil, err
			}
			st.blobs[k] = rec
		default:
			raw := rawSection{enc: e}
			for {
				rec, ok, err := next()
				if err != nil {
					return nil, err
				}
				if !ok {
					break
				}
				raw.records = append(raw.records, rec)
			}
			st.unknown[k] = raw
		}
	}
	if !seenMembership {
		return nil, fmt.Errorf("%w: no membership section", ErrSnapshotFormat)
	}
	return st, nil
}

// oneRecord reads a blob section: exactly one record.
func oneRecord(k Kind, e Encoding, next func() ([]byte, bool, error)) ([]byte, error) {
	if e != EncodingBlob {
		return nil, fmt.Errorf("%w: kind %d has encoding %d, not supported for it", ErrSnapshotFormat, k, e)
	}
	rec, ok, err := next()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("%w: kind %d has no record", ErrSnapshotFormat, k)
	}
	if _, more, err := next(); err != nil || more {
		if err == nil {
			err = fmt.Errorf("%w: kind %d has more than one record", ErrSnapshotFormat, k)
		}
		return nil, err
	}
	return rec, nil
}

// drain reads a section to its end; a streamer that stops early has left
// records it did not understand.
func drain(next func() ([]byte, bool, error)) error {
	_, more, err := next()
	if err != nil {
		return err
	}
	if more {
		return fmt.Errorf("%w: records after the section's end", ErrSnapshotFormat)
	}
	return nil
}

// Install replaces the machine's state with the staged state. It is called
// on the consensus loop. A component registered with no section keeps its
// state, as a format 1 restore leaves it.
func (s *StagedMachine) Install() error {
	m := s.m
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, staged := range s.staged {
		if _, ok := m.components[k]; !ok {
			return fmt.Errorf("install: kind %d is no longer registered", k)
		}
		staged.Install()
	}
	for k, data := range s.blobs {
		c, ok := m.components[k]
		if !ok {
			return fmt.Errorf("install: kind %d is no longer registered", k)
		}
		if err := c.Restore(data); err != nil {
			return fmt.Errorf("restore of kind %d: %w", k, err)
		}
	}
	m.unknown = s.unknown
	if err := m.membership.restore(s.membership); err != nil {
		return err
	}
	m.applied = s.applied
	return nil
}
