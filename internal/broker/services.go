package broker

import (
	"time"

	"github.com/happydave/hoardmq/ext"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/module"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/session"
)

// services is what modules may ask of the broker.
type services struct {
	s *Server
}

func (v services) Publish(m *message.Message) {
	if v.s.sessions == nil {
		return
	}
	v.s.sessions.Inject(m)
}

func (v services) Disconnect(namespace, clientID string, reason packet.ReasonCode) bool {
	if v.s.sessions == nil {
		return false
	}
	c, ok := v.s.sessions.Conn(namespace, clientID)
	if !ok {
		return false
	}
	c.Close(reason)
	return true
}

func (v services) Sessions() []session.Record {
	if v.s.sessions == nil {
		return nil
	}
	return v.s.sessions.Records()
}

func (v services) Retained(namespace, filter string) []*message.Message {
	return v.s.router.RetainedFor(namespace, filter, time.Now())
}

// Ready is the readiness signal: the broker serves, is not draining, its
// consensus core is a member that has caught up with the leader, its leases
// are not fenced, its build understands the cluster's feature level, and the
// topology has been stable for the configured interval.
func (v services) Ready() bool {
	ready, _ := v.s.Readiness()
	return ready
}

// Serving is the serving signal (Server.Serving).
func (v services) Serving() (bool, string) { return v.s.Serving() }

func (v services) Drain() { v.s.Drain() }

// Live is the liveness signal (Server.Liveness).
func (v services) Live() (bool, string) { return v.s.Liveness() }

// Liveness reports whether the node can still recover without a restart, and
// when not, why: its consensus loop stopped on an error, or its own or its
// replica journal cannot be rewritten over damage.
func (s *Server) Liveness() (bool, string) {
	var consensusErr, journalErr, replicaErr error
	if s.consensus != nil {
		consensusErr = s.consensus.Failed()
	}
	if j, ok := s.store.(ext.RecoverabilityProvider); ok {
		journalErr = j.Unrecoverable()
	}
	if s.replica != nil {
		replicaErr = s.replica.Unrecoverable()
	}
	return liveness(consensusErr, journalErr, replicaErr)
}

// liveness decides the liveness signal from its three causes, the first in
// this order naming the reason.
func liveness(consensusErr, journalErr, replicaErr error) (bool, string) {
	switch {
	case consensusErr != nil:
		return false, "consensus-stopped: " + consensusErr.Error()
	case journalErr != nil:
		return false, "journal-unrecoverable: " + journalErr.Error()
	case replicaErr != nil:
		return false, "replica-journal-unrecoverable: " + replicaErr.Error()
	}
	return true, ""
}

// Records is the replicated records store, nil until the core is open.
func (v services) Records() module.RecordStore {
	if st := v.s.records.Load(); st != nil {
		return st
	}
	return nil
}
func (v services) DrainStatus() module.DrainStatus { return v.s.DrainStatus() }

// degradedRoutesUnknown is the degraded state of a ready node that does not
// know every owner's routes, and so refuses every QoS 1 or 2 publish that
// needs one (WI 1351).
const degradedRoutesUnknown = "routes-unknown"

// Serving reports whether this node is ready and knows every owner's routes,
// so that a QoS 1 or 2 publish can be acknowledged, and when it is not, why.
// A ready node that does not know them is degraded; readiness does not
// report it, so a load balancer keeps the node in service (WI 1571).
func (s *Server) Serving() (bool, string) {
	ready, reason := s.Readiness()
	return serving(ready, reason, s.routes == nil || s.routes.OwnersKnown())
}

// degraded is 1 while this node is degraded, and how long the routes it
// lacks have been unknown, in seconds; 0 and 0 otherwise.
func (s *Server) degraded(now time.Time) (state, seconds float64) {
	if ok, reason := s.Serving(); ok || reason != degradedRoutesUnknown {
		return 0, 0
	}
	episodes, open := s.routes.OwnerKnowledge()
	if !open {
		return 1, 0
	}
	return 1, now.Sub(episodes[len(episodes)-1].Start).Seconds()
}

// serving decides the serving signal from readiness and owner knowledge.
func serving(ready bool, readyReason string, ownersKnown bool) (bool, string) {
	switch {
	case !ready:
		return false, "not-ready: " + readyReason
	case !ownersKnown:
		return false, degradedRoutesUnknown
	}
	return true, ""
}

// consensusHeld reports whether the consensus core holds the log.
func (s *Server) consensusHeld() bool {
	if s.consensus == nil {
		return false
	}
	_, held := s.consensus.Held()
	return held
}

// Readiness reports the readiness signal and, when false, the reason.
func (s *Server) Readiness() (bool, string) {
	switch {
	case s.Draining():
		return false, "draining"
	case !s.ready.Load():
		return false, "not-serving"
	case s.leases != nil && s.leases.BelowLevel(), s.consensusHeld():
		// Ahead of recovering and behind: a node below the level stays both,
		// and holds, until it is restarted on a build that understands the
		// level.
		return false, "level"
	case s.consensus != nil && s.consensus.Recovering():
		return false, "recovering"
	case s.consensus != nil && s.consensus.Behind():
		return false, "behind"
	case s.consensus != nil && !s.consensus.Ready():
		return false, "consensus"
	case s.leases != nil && s.leases.Fenced():
		return false, "fenced"
	case s.topology != nil && !s.topology.Stable():
		return false, "topology"
	}
	return true, ""
}
