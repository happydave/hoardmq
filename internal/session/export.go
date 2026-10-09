package session

import (
	"time"

	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

// ConnAge reports how long the session's live connection has been accepted,
// when it has one whose connection layer reports an age.
func (m *Manager) ConnAge(ns, id string) (time.Duration, bool) {
	sh := m.shardFor(key{ns, id})
	sh.mu.Lock()
	s := sh.sessions[key{ns, id}]
	sh.mu.Unlock()
	if s == nil {
		return 0, false
	}
	s.mu.Lock()
	c := s.conn
	s.mu.Unlock()
	if a, ok := c.(interface{ Age() time.Duration }); ok {
		return a.Age(), true
	}
	return 0, false
}

// Export returns the persisted form of one session, online or offline, for
// a takeover by another node. The session is left in place; the caller
// evicts it once the new owner has it.
func (m *Manager) Export(ns, id string) (*store.SessionState, bool) {
	sh := m.shardFor(key{ns, id})
	sh.mu.Lock()
	s := sh.sessions[key{ns, id}]
	sh.mu.Unlock()
	if s == nil {
		return nil, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshot(), true
}

// Import installs one offline session received from its previous owner,
// replacing any state this node held for the key without publishing a Will.
// In-flight entries become unsent, as after a restart, so the client's next
// connection gets them again.
func (m *Manager) Import(ss *store.SessionState) { m.ImportIf(ss, nil) }

// ImportIf imports ss as Import does when admit allows it. admit is called
// under the client's shard lock, before anything is changed, and a refused
// import changes nothing. An admitted import marks the client until it
// returns, and a Seal of the client waits for the mark to clear: a takeover
// pass finds the session installed, or its request's grant made admit refuse.
// A nil admit admits. It reports whether it imported.
func (m *Manager) ImportIf(ss *store.SessionState, admit func() bool) bool {
	k := key{ss.Namespace, ss.ClientID}
	sh := m.shardFor(k)
	sh.mu.Lock()
	for wait := sh.importing[k]; wait != nil; wait = sh.importing[k] {
		sh.mu.Unlock()
		m.importWaits.Add(1)
		<-wait
		sh.mu.Lock()
	}
	if admit != nil && !admit() {
		sh.mu.Unlock()
		return false
	}
	done := make(chan struct{})
	sh.importing[k] = done
	sh.mu.Unlock()
	defer func() {
		sh.mu.Lock()
		delete(sh.importing, k)
		sh.mu.Unlock()
		close(done)
	}()
	if m.importPause != nil {
		m.importPause()
	}
	m.importState(ss)
	return true
}

func (m *Manager) importState(ss *store.SessionState) {
	began := time.Now()
	defer func() {
		m.imports.Add(1)
		m.importNs.Add(int64(time.Since(began)))
	}()
	if _, live := m.ConnAge(ss.Namespace, ss.ClientID); live {
		m.closedEvictImport.Add(1)
	}
	// A sealed copy is replaced: the whole write below begins with the delete
	// its records were waiting for.
	sh0 := m.shardFor(key{ss.Namespace, ss.ClientID})
	sh0.mu.Lock()
	delete(sh0.sealed, key{ss.Namespace, ss.ClientID})
	sh0.mu.Unlock()
	m.Evict(ss.Namespace, ss.ClientID, packet.SessionTakenOver)
	m.load(&store.State{Sessions: []*store.SessionState{ss}}, true)
	// Recovery does not journal; an imported session is new to this node's
	// log, so write it whole.
	k := key{ss.Namespace, ss.ClientID}
	sh := m.shardFor(k)
	sh.mu.Lock()
	s := sh.sessions[k]
	sh.mu.Unlock()
	if s != nil {
		s.mu.Lock()
		m.writer().full(s)
		s.mu.Unlock()
		m.syncIfDirty()
	}
}

// Take exports a session and removes it in one step, so nothing can be
// enqueued for it here after the snapshot the new owner receives. The
// connection, if any, is closed with the reason; the Will is not published.
func (m *Manager) Take(ns, id string, reason packet.ReasonCode) (*store.SessionState, bool) {
	m.markMoving(ns, id)
	sh := m.shardFor(key{ns, id})
	sh.mu.Lock()
	s := sh.sessions[key{ns, id}]
	if s == nil {
		sh.mu.Unlock()
		return nil, false
	}
	s.mu.Lock()
	st := s.snapshot()
	st.Delivered = s.sentWithin(m.clock.Now())
	var old Connection
	if s.conn != nil {
		old = s.conn
		delete(sh.byConn, old)
		s.detach()
	}
	s.will, s.willArmed = nil, false
	redistribute := m.discardLocked(sh, s)
	s.mu.Unlock()
	sh.mu.Unlock()
	if old != nil {
		m.closedTake.Add(1)
		old.Close(reason)
	}
	m.redistribute(s, redistribute)
	m.syncIfDirty()
	return st, true
}

// Evict ends a session on this node because its lease moved elsewhere: the
// connection, if any, is closed with the reason, and the state is dropped
// without publishing the Will (the new owner carries it). It reports whether
// a session existed.
func (m *Manager) Evict(ns, id string, reason packet.ReasonCode) bool {
	m.markMoving(ns, id)
	sh := m.shardFor(key{ns, id})
	sh.mu.Lock()
	s := sh.sessions[key{ns, id}]
	if s == nil {
		sh.mu.Unlock()
		return false
	}
	s.mu.Lock()
	var old Connection
	if s.conn != nil {
		old = s.conn
		delete(sh.byConn, old)
		s.detach()
	}
	s.will, s.willArmed = nil, false
	redistribute := m.discardLocked(sh, s)
	s.mu.Unlock()
	sh.mu.Unlock()
	if old != nil {
		m.closedEvict.Add(1)
		old.Close(reason)
	}
	m.redistribute(s, redistribute)
	m.syncIfDirty()
	return true
}

// markMoving tells the routing plane which filters are about to leave this
// node with their session, before the removal reaches the dispatcher's
// observer. The other nodes keep forwarding here for the grace, where the
// message finds no session and its publish is refused, rather than matching
// nothing anywhere while the new owner's route is in flight.
func (m *Manager) markMoving(ns, id string) {
	if m.opts.Moving == nil {
		return
	}
	sh := m.shardFor(key{ns, id})
	sh.mu.Lock()
	s := sh.sessions[key{ns, id}]
	sh.mu.Unlock()
	if s == nil {
		return
	}
	s.mu.Lock()
	filters := make([]string, 0, len(s.subs))
	for f := range s.subs {
		filters = append(filters, f)
	}
	s.mu.Unlock()
	if len(filters) > 0 {
		m.opts.Moving(ns, id, filters)
	}
}

// Detach closes a session's connection with the reason but keeps its state,
// so the new owner can still pull it. The connection's own disconnect
// callback finds the session already detached and publishes no Will.
func (m *Manager) Detach(ns, id string, reason packet.ReasonCode) {
	sh := m.shardFor(key{ns, id})
	sh.mu.Lock()
	s := sh.sessions[key{ns, id}]
	if s == nil {
		sh.mu.Unlock()
		return
	}
	s.mu.Lock()
	var old Connection
	if s.conn != nil {
		old = s.conn
		delete(sh.byConn, old)
		s.detach()
	}
	s.mu.Unlock()
	sh.mu.Unlock()
	if old != nil {
		m.closedDetach.Add(1)
		old.Close(reason)
	}
}

// SetEpoch stamps the lease epoch on a session so its record and journal
// entry carry it.
func (m *Manager) SetEpoch(ns, id string, epoch uint64) {
	sh := m.shardFor(key{ns, id})
	sh.mu.Lock()
	s := sh.sessions[key{ns, id}]
	sh.mu.Unlock()
	if s == nil {
		return
	}
	s.mu.Lock()
	s.epoch = epoch
	m.writer().upsert(s)
	s.mu.Unlock()
	m.syncIfDirty()
}

// ended tells the Ended callback that a session's state is gone for good:
// expired, or replaced by a Clean Start. Evictions do not count; the state
// moved.
func (m *Manager) ended(s *Session) {
	if m.opts.Ended == nil || s == nil {
		return
	}
	s.mu.Lock()
	epoch := s.epoch
	s.mu.Unlock()
	m.opts.Ended(s.key.ns, s.key.id, epoch)
}

// Displaced reports how many connections this node closed because a second
// CONNECT arrived for the same client identifier. It is one of three sources
// of a SessionTakenOver disconnect and the only one that involves no lease
// movement at all, so a measurement that reads a client's disconnect reason
// cannot attribute it without this figure (WI 1495).
func (m *Manager) Displaced() int64 { return m.displaced.Load() }

// CloseCounts reports connections this node closed, by the call that closed
// them. Together with Displaced they account for every SessionTakenOver a
// client on this node can observe (WI 1495).
func (m *Manager) CloseCounts() (take, evict, detach int64) {
	return m.closedTake.Load(), m.closedEvict.Load(), m.closedDetach.Load()
}

// ImportCost reports how many sessions were imported and the time spent doing
// it, journal sync included: an import made inside a CONNECT holds that
// handshake for as long.
func (m *Manager) ImportCost() (n int64, total time.Duration) {
	return m.imports.Load(), time.Duration(m.importNs.Load())
}

// ImportWaits reports how many Seals, and how many imports, waited for an
// import of the same client to finish (ImportIf).
func (m *Manager) ImportWaits() (seals, imports int64) {
	return m.sealWaits.Load(), m.importWaits.Load()
}

// EvictsFromImport is the subset of the evictions that came from adopting a
// lease and pulling its state, rather than from the lease service's deferred
// reclaim. The two are the same act with different causes and want telling
// apart (WI 1513).
func (m *Manager) EvictsFromImport() int64 { return m.closedEvictImport.Load() }
