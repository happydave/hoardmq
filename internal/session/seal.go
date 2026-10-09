package session

import (
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

// A session handed to a takeover is sealed rather than removed: it leaves the
// session map and the router, so nothing is enqueued into it and no CONNECT
// resumes it, but nothing is journaled. Its records, and every replica copy of
// them, stay until the new owner confirms it holds the session, or until this
// node's own later grant takes it back. A delete journaled at hand-over time
// can reach a follower ahead of the grant and remove the only copy while the
// answer carrying the session is lost.

// sealedCopy is one sealed session: its state, the grant token it was sealed
// for, and what its delete must name when it is written.
type sealedCopy struct {
	st         *store.SessionState
	token      uint64
	epoch      uint64
	persistent bool
}

// SealOutcome says what Seal did.
type SealOutcome int

const (
	// SealNone: no session here, sealed or not.
	SealNone SealOutcome = iota
	// SealSealed: the session was sealed now.
	SealSealed
	// SealAlready: it was already sealed; Raised says whether for a lower token.
	SealAlready
	// SealNewer: the session here is held at a token above the one asked for,
	// so it was granted here again after the request was made; left as it is.
	SealNewer
)

// SealResult is Seal's answer. State is a copy the caller may stamp; Token is
// the token the copy is sealed for now (for SealNewer, the session's own).
type SealResult struct {
	State   *store.SessionState
	Token   uint64
	Outcome SealOutcome
	Raised  bool
}

// Seal hands a session over without deleting it: the connection is closed with
// reason and the Will is not published, as Take does, and the state is kept
// sealed for token. A session already sealed is answered from its copy.
func (m *Manager) Seal(ns, id string, reason packet.ReasonCode, token uint64) SealResult {
	k := key{ns, id}
	sh := m.shardFor(k)
	sh.mu.Lock()
	if wait := sh.importing[k]; wait != nil {
		// An import is installing the session: seal what it installs.
		sh.mu.Unlock()
		m.sealWaits.Add(1)
		<-wait
		return m.Seal(ns, id, reason, token)
	}
	s := sh.sessions[k]
	if s == nil {
		defer sh.mu.Unlock()
		c := sh.sealed[k]
		if c == nil {
			return SealResult{}
		}
		raised := c.token < token
		if raised {
			c.token = token
		}
		cp := *c.st
		return SealResult{State: &cp, Token: c.token, Outcome: SealAlready, Raised: raised}
	}
	s.mu.Lock()
	epoch := s.epoch
	s.mu.Unlock()
	sh.mu.Unlock()
	if epoch > token {
		return SealResult{Token: epoch, Outcome: SealNewer}
	}
	m.markMoving(ns, id)
	sh.mu.Lock()
	if sh.sessions[k] != s || sh.importing[k] != nil {
		// Replaced, or being replaced, between the look and the lock: seal
		// whatever is here once it is in place.
		sh.mu.Unlock()
		return m.Seal(ns, id, reason, token)
	}
	s.mu.Lock()
	if s.epoch > token {
		epoch := s.epoch
		s.mu.Unlock()
		sh.mu.Unlock()
		return SealResult{Token: epoch, Outcome: SealNewer}
	}
	st := s.snapshot()
	st.Delivered = s.sentWithin(m.clock.Now())
	c := &sealedCopy{st: st, token: token, epoch: s.epoch, persistent: s.persistent}
	var old Connection
	if s.conn != nil {
		old = s.conn
		delete(sh.byConn, old)
		s.detach()
	}
	s.will, s.willArmed = nil, false
	redistribute := m.removeLocked(sh, s)
	sh.sealed[k] = c
	s.mu.Unlock()
	sh.mu.Unlock()
	if old != nil {
		m.closedTake.Add(1)
		old.Close(reason)
	}
	m.redistribute(s, redistribute)
	cp := *st
	return SealResult{State: &cp, Token: token, Outcome: SealSealed}
}

// Sealed returns a copy of the session's sealed state and the token it is
// sealed for.
func (m *Manager) Sealed(ns, id string) (*store.SessionState, uint64, bool) {
	sh := m.shardFor(key{ns, id})
	sh.mu.Lock()
	defer sh.mu.Unlock()
	c := sh.sealed[key{ns, id}]
	if c == nil {
		return nil, 0, false
	}
	cp := *c.st
	return &cp, c.token, true
}

// DropSealed gives a sealed session up for good, journaling its delete, when
// it is sealed for a token at or below upTo. It reports whether it did.
func (m *Manager) DropSealed(ns, id string, upTo uint64) bool {
	k := key{ns, id}
	sh := m.shardFor(k)
	sh.mu.Lock()
	c := sh.sealed[k]
	if c == nil || c.token > upTo {
		sh.mu.Unlock()
		return false
	}
	delete(sh.sealed, k)
	m.journalSealedDelete(c)
	sh.mu.Unlock()
	m.syncIfDirty()
	return true
}

// EvictOffline evicts a session held at an epoch at or below upTo that has no
// connection, as Evict does; a connected session, or one held at a later
// grant, is left. It reports whether it evicted.
func (m *Manager) EvictOffline(ns, id string, upTo uint64) bool {
	k := key{ns, id}
	if !m.offlineAtOrBelow(k, upTo) {
		return false
	}
	m.markMoving(ns, id)
	sh := m.shardFor(k)
	sh.mu.Lock()
	s := sh.sessions[k]
	if s == nil {
		sh.mu.Unlock()
		return false
	}
	s.mu.Lock()
	if s.conn != nil || s.epoch > upTo {
		s.mu.Unlock()
		sh.mu.Unlock()
		return false
	}
	redistribute := m.discardLocked(sh, s)
	s.mu.Unlock()
	sh.mu.Unlock()
	m.redistribute(s, redistribute)
	m.syncIfDirty()
	return true
}

func (m *Manager) offlineAtOrBelow(k key, upTo uint64) bool {
	sh := m.shardFor(k)
	sh.mu.Lock()
	s := sh.sessions[k]
	sh.mu.Unlock()
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn == nil && s.epoch <= upTo
}

// HeldAt reports the token a session held here is served at.
func (m *Manager) HeldAt(ns, id string) (uint64, bool) {
	sh := m.shardFor(key{ns, id})
	sh.mu.Lock()
	s := sh.sessions[key{ns, id}]
	sh.mu.Unlock()
	if s == nil {
		return 0, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.epoch, true
}

// NextPublishSeq is the sequence the next publish on this node will take.
// Every publish takes one before it is routed, so one numbered at or above a
// value read after a session was installed was routed with that session in
// place. It is never zero.
func (m *Manager) NextPublishSeq() uint64 { return m.seq.Load() + 1 }

// SealedCount is the number of sessions sealed here.
func (m *Manager) SealedCount() int {
	n := 0
	for _, sh := range m.shards {
		sh.mu.Lock()
		n += len(sh.sealed)
		sh.mu.Unlock()
	}
	return n
}

// SealStats reports CONNECTs refused because their session is sealed for a
// newer grant, and sealed sessions a CONNECT resumed.
func (m *Manager) SealStats() (refused, resumed int64) {
	return m.connectsRefusedSealed.Load(), m.sealsResumed.Load()
}

// journalSealedDelete writes a sealed session's delete, under the epoch its
// records carry. The caller holds the shard's lock.
func (m *Manager) journalSealedDelete(c *sealedCopy) {
	if m.opts.Journal == nil || !c.persistent {
		return
	}
	if err := m.opts.Journal.SessionDelete(store.Key{Namespace: c.st.Namespace, ClientID: c.st.ClientID, Epoch: c.epoch}); err != nil {
		m.log.Error("journal append failed", "err", err)
		m.count("hoardmq_journal_failures_total", opAppend, 1)
		return
	}
	m.journaled.Add(1)
}

// sealedVerdict is what a CONNECT does with a sealed copy of its session.
type sealedVerdict int

const (
	// sealedRefuse: the copy was sealed for a grant at or above the CONNECT's,
	// so the session is another node's and is not resumed here.
	sealedRefuse sealedVerdict = iota
	// sealedDiscard: the lease came back and the client asked for a clean
	// start; the copy's delete is journaled before the new session is.
	sealedDiscard
	// sealedResume: the lease came back; the session is resumed from the copy.
	sealedResume
)

// sealedConnect decides what a CONNECT at leaseEpoch does with a copy sealed
// for sealedFor. It is pure: discarding journals a delete, so the decision is
// tested apart from the act.
func sealedConnect(sealedFor, leaseEpoch uint64, cleanStart bool) sealedVerdict {
	switch {
	case leaseEpoch <= sealedFor:
		return sealedRefuse
	case cleanStart:
		return sealedDiscard
	}
	return sealedResume
}
