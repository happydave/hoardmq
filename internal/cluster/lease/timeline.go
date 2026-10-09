package lease

import (
	"sync"
	"sync/atomic"
	"time"
)

// NodeLiveness is when this node last saw each step of another node's
// liveness, in wall-clock Unix nanoseconds; zero means never. The clock is the
// process's own, not Options.Clock, so a harness that skews the lease clock
// can still line these up with its own marks. Nothing reads them to decide.
type NodeLiveness struct {
	// LastAliveAppliedNs is when this node last applied an alive record
	// from the node.
	LastAliveAppliedNs int64 `json:"last_alive_applied_ns,omitempty"`
	// DeclarationProposedNs is when this node, as leader, proposed the
	// node's declaration.
	DeclarationProposedNs int64 `json:"declaration_proposed_ns,omitempty"`
	// DeclarationAppliedNs is when this node applied the node's
	// declaration.
	DeclarationAppliedNs int64 `json:"declaration_applied_ns,omitempty"`
}

type livenessTimes struct {
	lastAlive, proposed, applied atomic.Int64
}

// livenessTimeline holds one entry per node name this node has heard of,
// which the membership bounds.
type livenessTimeline struct{ m sync.Map } // string -> *livenessTimes

func (l *livenessTimeline) of(node string) *livenessTimes {
	if v, ok := l.m.Load(node); ok {
		return v.(*livenessTimes)
	}
	v, _ := l.m.LoadOrStore(node, &livenessTimes{})
	return v.(*livenessTimes)
}

func (l *livenessTimeline) snapshot() map[string]NodeLiveness {
	out := map[string]NodeLiveness{}
	l.m.Range(func(k, v any) bool {
		t := v.(*livenessTimes)
		out[k.(string)] = NodeLiveness{LastAliveAppliedNs: t.lastAlive.Load(), DeclarationProposedNs: t.proposed.Load(),
			DeclarationAppliedNs: t.applied.Load()}
		return true
	})
	if len(out) == 0 {
		return nil
	}
	return out
}

func wallNs() int64 { return time.Now().UnixNano() }

// declared runs on the apply goroutine for every node a command declared
// dead. Options.Declared must not block.
func (s *Service) declared(node string) {
	s.timeline.of(node).applied.Store(wallNs())
	s.wakeTransit()
	if s.o.Declared != nil {
		s.o.Declared(node)
	}
}
