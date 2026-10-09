package replication

import (
	"sort"
	"sync"
	"time"
)

// Why the shard-map driver checked the basis.
const (
	causeTick        = "tick"        // its periodic check
	causeRetry       = "retry"       // sooner, after an attempt that failed
	causeDeclaration = "declaration" // woken by a declaration, or a drain's hand-off, this node applied
)

// MapEvent is what this node saw of one shard-map version, in wall-clock
// Unix nanoseconds; zero means it did not see that step. Cause, DecidedNs and
// ProposedNs are set only on the node whose driver proposed the version.
type MapEvent struct {
	Version uint64   `json:"version"`
	Basis   []string `json:"basis,omitempty"`
	Cause   string   `json:"cause,omitempty"`
	// DecidedNs is when the driver computed the map, before proposing it;
	// ProposedNs when the proposal returned applied.
	DecidedNs    int64 `json:"decided_ns,omitempty"`
	ProposedNs   int64 `json:"proposed_ns,omitempty"`
	AppliedNs    int64 `json:"applied_ns,omitempty"`
	ReconciledNs int64 `json:"reconciled_ns,omitempty"`
}

// MapTimeline is the last mapTimelineCap versions this node saw, oldest
// first, and how many times its driver checked the basis, by cause.
type MapTimeline struct {
	Events []MapEvent       `json:"events,omitempty"`
	Checks map[string]int64 `json:"checks,omitempty"`
}

const mapTimelineCap = 16

type mapTimeline struct {
	mu     sync.Mutex
	events []MapEvent
	checks map[string]int64
}

// at returns the event for version, adding it and dropping the oldest when
// the timeline is full. Callers hold mu.
func (t *mapTimeline) at(version uint64) *MapEvent {
	for i := range t.events {
		if t.events[i].Version == version {
			return &t.events[i]
		}
	}
	t.events = append(t.events, MapEvent{Version: version})
	sort.Slice(t.events, func(i, j int) bool { return t.events[i].Version < t.events[j].Version })
	if len(t.events) > mapTimelineCap {
		t.events = t.events[len(t.events)-mapTimelineCap:]
	}
	for i := range t.events {
		if t.events[i].Version == version {
			return &t.events[i]
		}
	}
	// Older than every event a full timeline holds: recorded nowhere.
	return &MapEvent{}
}

func (t *mapTimeline) checked(cause string) {
	t.mu.Lock()
	if t.checks == nil {
		t.checks = map[string]int64{}
	}
	t.checks[cause]++
	t.mu.Unlock()
}

func (t *mapTimeline) decided(version uint64, basis []string, cause string, decided, proposed time.Time) {
	t.mu.Lock()
	e := t.at(version)
	e.Basis, e.Cause, e.DecidedNs, e.ProposedNs = basis, cause, decided.UnixNano(), proposed.UnixNano()
	t.mu.Unlock()
}

func (t *mapTimeline) applied(version uint64, basis []string, at time.Time) {
	t.mu.Lock()
	e := t.at(version)
	e.Basis, e.AppliedNs = basis, at.UnixNano()
	t.mu.Unlock()
}

func (t *mapTimeline) reconciled(version uint64, at time.Time) {
	t.mu.Lock()
	t.at(version).ReconciledNs = at.UnixNano()
	t.mu.Unlock()
}

// MapTimeline reports the shard-map versions this node saw and its driver's
// checks. Nothing reads it to decide.
func (r *Replicator) MapTimeline() MapTimeline {
	r.maps.mu.Lock()
	defer r.maps.mu.Unlock()
	out := MapTimeline{Events: make([]MapEvent, len(r.maps.events))}
	for i, e := range r.maps.events {
		e.Basis = append([]string(nil), e.Basis...)
		out.Events[i] = e
	}
	if len(r.maps.checks) > 0 {
		out.Checks = make(map[string]int64, len(r.maps.checks))
		for k, v := range r.maps.checks {
			out.Checks[k] = v
		}
	}
	return out
}
