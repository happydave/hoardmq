package session

import (
	"slices"
	"sync/atomic"
	"time"

	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

// Journal is the store provider seam: the manager records every change to
// persistent state through it and calls Sync before acknowledging a QoS 1 or
// 2 publish, a SUBSCRIBE, an UNSUBSCRIBE or a CONNECT. The store package
// implements it over the write-ahead log.
type Journal interface {
	SessionUpsert(meta store.SessionMeta) error
	SessionDelete(k store.Key) error
	Subscribe(k store.Key, sub store.SubscriptionRecord) error
	Unsubscribe(k store.Key, filter string) error
	InboundQoS2(k store.Key, packetID uint16, add bool) error
	InboundAccepted(k store.Key, packetID uint16) error
	Enqueue(k store.Key, e store.Entry) error
	Sent(k store.Key, entrySeq uint64, packetID uint16) error
	InflightState(k store.Key, packetID uint16, state byte) error
	Complete(k store.Key, packetID uint16) error
	Dropped(k store.Key, entrySeq uint64) error
	Retain(m *message.Message) error
	RetainDelete(ns, topic string) error
	Sync() error
}

// journalWriter wraps the journal with the persistence rule (only persistent
// sessions are recorded) and error handling: a failed append marks the
// session to be written whole again, or, on a refusing writer, refuses the
// publish the writer belongs to. A nil writer is a no-op.
type journalWriter struct {
	j         Journal
	journaled *atomic.Uint64
	mgr       *Manager
	// refusing marks a writer whose writes belong to a publish whose
	// acknowledgement waits on them: a failed append sets failed, in place of
	// the session's journalErr, and the publish is refused for it.
	refusing bool
	failed   bool
}

// The journal operations a failure is counted under.
var (
	opAppend = map[string]string{"op": "append"}
	opSync   = map[string]string{"op": "sync"}
)

func (m *Manager) writer() *journalWriter {
	if m.opts.Journal == nil {
		return nil
	}
	return &journalWriter{j: m.opts.Journal, journaled: &m.journaled, mgr: m}
}

// ackWriter is the writer for the records of a publish that is acknowledged:
// a failed append is reported on the writer.
func (m *Manager) ackWriter() *journalWriter {
	if m.opts.Journal == nil {
		return nil
	}
	return &journalWriter{j: m.opts.Journal, journaled: &m.journaled, mgr: m, refusing: true}
}

// record writes one record of s and reports whether it is in the journal;
// nothing to journal counts as in.
func (w *journalWriter) record(s *Session, f func() error) bool {
	if w == nil || !s.persistent {
		return true
	}
	if err := f(); err != nil {
		if w.refusing {
			w.failed = true
		} else {
			s.unwritten = true
			if s.mgr != nil {
				s.mgr.armRewrite(s)
			}
		}
		s.log.Error("journal append failed", "err", err)
		if s.mgr != nil {
			s.mgr.count("hoardmq_journal_failures_total", opAppend, 1)
		}
		return false
	}
	w.journaled.Add(1)
	return true
}

// withdraws decides whether a copy whose record failed to append leaves the
// session: only when the publish that wrote it is refused for the failure, so
// its retry is the one copy. A copy nobody retries — a redistribution, a Will,
// a QoS 0 publish — stays, since withdrawing it would lose it.
func withdraws(refused, journaled bool) bool { return refused && !journaled }

// refuses reports whether a failed append through w refuses a publish.
func (w *journalWriter) refuses() bool { return w != nil && w.refusing }

// unjournaled reports whether an append through w failed.
func (w *journalWriter) unjournaled() bool { return w != nil && w.failed }

func (w *journalWriter) upsert(s *Session) {
	w.record(s, func() error { return w.j.SessionUpsert(s.meta()) })
}

// writes reports whether a record of s goes through w at all: a journal is
// attached and the session is persistent.
func (w *journalWriter) writes(s *Session) bool { return w != nil && s.persistent }

func (w *journalWriter) delete(s *Session) {
	w.record(s, func() error { return w.j.SessionDelete(s.storeKey()) })
}

// deleteCopies journals the session's delete whether or not s is persistent:
// the copies it removes were written by another node. A persistent session
// whose delete fails is written whole later, delete first; for one that is
// not, the failure is counted.
func (w *journalWriter) deleteCopies(s *Session) {
	if w == nil {
		return
	}
	if s.persistent {
		w.delete(s)
		return
	}
	if err := w.j.SessionDelete(s.storeKey()); err != nil {
		s.log.Error("journal append failed", "err", err)
		if s.mgr != nil {
			s.mgr.count("hoardmq_journal_failures_total", opAppend, 1)
		}
		return
	}
	w.journaled.Add(1)
}

func (w *journalWriter) subscribe(s *Session, sub Subscription) bool {
	return w.record(s, func() error {
		return w.j.Subscribe(s.storeKey(), store.SubscriptionRecord{Filter: sub.Filter, QoS: sub.QoS, NoLocal: sub.NoLocal, RetainAsPublished: sub.RetainAsPublished, RetainHandling: sub.RetainHandling, Identifier: sub.Identifier})
	})
}

func (w *journalWriter) unsubscribe(s *Session, filter string) bool {
	return w.record(s, func() error { return w.j.Unsubscribe(s.storeKey(), filter) })
}

func (w *journalWriter) inbound(s *Session, id uint16, add bool) bool {
	return w.record(s, func() error { return w.j.InboundQoS2(s.storeKey(), id, add) })
}

func (w *journalWriter) accepted(s *Session, id uint16) {
	w.record(s, func() error { return w.j.InboundAccepted(s.storeKey(), id) })
}

// enqueued records a message added to a session. A QoS 1 or 2 message
// carries its packet identifier from the moment it is queued — the
// replicas need it to re-send the same exchange after a takeover — so the
// identifier no longer implies the message has gone out; sent says that.
func (w *journalWriter) enqueued(s *Session, seq uint64, m *message.Message, t target, packetID uint16, sent bool) bool {
	return w.record(s, func() error {
		e := store.Entry{Seq: seq, Message: m, QoS: t.qos, Identifiers: t.ids, Retain: t.retain, Group: t.group, PacketID: packetID}
		if sent {
			e.State = store.StatePublishSent
		}
		return w.j.Enqueue(s.storeKey(), e)
	})
}

func (w *journalWriter) sent(s *Session, seq uint64, packetID uint16) {
	w.record(s, func() error { return w.j.Sent(s.storeKey(), seq, packetID) })
}

func (w *journalWriter) state(s *Session, packetID uint16, st inflightState) bool {
	return w.record(s, func() error {
		v := store.StatePublishSent
		if st == pubrecReceived {
			v = store.StatePubrecReceived
		}
		return w.j.InflightState(s.storeKey(), packetID, v)
	})
}

func (w *journalWriter) complete(s *Session, packetID uint16) {
	w.record(s, func() error { return w.j.Complete(s.storeKey(), packetID) })
}

func (w *journalWriter) dropped(s *Session, seq uint64) {
	w.record(s, func() error { return w.j.Dropped(s.storeKey(), seq) })
}

// full records a session whole: its metadata, subscriptions, inbound
// identifiers and entries, as after an import.
func (w *journalWriter) full(s *Session) {
	w.record(s, func() error {
		st := s.snapshot()
		k := store.Key{Namespace: st.Namespace, ClientID: st.ClientID, Epoch: st.Epoch}
		// Whole means whole: the delete first, so the log — and every
		// replica applying it — replaces whatever it held for the session
		// rather than merging the new state into it.
		if err := w.j.SessionDelete(k); err != nil {
			return err
		}
		if err := w.j.SessionUpsert(st.SessionMeta); err != nil {
			return err
		}
		for _, sub := range st.Subscriptions {
			if err := w.j.Subscribe(k, sub); err != nil {
				return err
			}
		}
		for _, id := range st.InboundQoS2 {
			if err := w.j.InboundQoS2(k, id, true); err != nil {
				return err
			}
			if slices.Contains(st.InboundAccepted, id) {
				if err := w.j.InboundAccepted(k, id); err != nil {
					return err
				}
			}
		}
		for _, e := range st.Entries {
			if err := w.j.Enqueue(k, e); err != nil {
				return err
			}
		}
		return nil
	})
}

// armRewrite arranges for s to be written whole after an own record failed to
// append. The caller holds s's lock.
func (m *Manager) armRewrite(s *Session) {
	if s.rewriteArmed {
		return
	}
	s.rewriteArmed = true
	m.clock.AfterFunc(pubrelRetry, func() {
		s.mu.Lock()
		s.rewriteArmed = false
		s.mu.Unlock()
		if m.rewrite(s) == rewriteFailed {
			s.mu.Lock()
			m.armRewrite(s)
			s.mu.Unlock()
		}
	})
}

type rewriteResult int

const (
	rewritten     rewriteResult = iota // written whole, or nothing was due
	rewriteFailed                      // an append failed; the mark stays
	rewriteGone                        // the session is no longer this manager's
)

// holds reports whether s is still this manager's session.
func (m *Manager) holds(s *Session) bool {
	sh := m.shardFor(s.key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	return sh.sessions[s.key] == s
}

// rewrite writes a marked session whole, so the journal and, through the tap,
// the replicas hold every record its failed append left out: the delete
// first, as an import writes it. It holds the shard's lock across the write,
// so a take cannot remove the session between the check and the records.
func (m *Manager) rewrite(s *Session) rewriteResult {
	s.mu.Lock()
	due := s.unwritten
	s.mu.Unlock()
	if !due {
		return rewritten
	}
	release := m.admit()
	defer release()
	sh := m.shardFor(s.key)
	sh.mu.Lock()
	if sh.sessions[s.key] != s {
		sh.mu.Unlock()
		return rewriteGone
	}
	s.mu.Lock()
	if !s.unwritten {
		s.mu.Unlock()
		sh.mu.Unlock()
		return rewritten
	}
	w := m.ackWriter()
	w.full(s)
	failed := w.unjournaled()
	if !failed {
		s.unwritten = false
	}
	s.mu.Unlock()
	sh.mu.Unlock()
	if failed {
		return rewriteFailed
	}
	m.syncIfDirty()
	return rewritten
}

// retain records a retained store change; retained messages are always
// persistent, so this bypasses the session rule.
func (w *journalWriter) retain(m *message.Message) {
	if w == nil {
		return
	}
	var err error
	if len(m.Payload) == 0 {
		err = w.j.RetainDelete(m.Namespace, m.Topic)
	} else {
		err = w.j.Retain(m)
	}
	if err != nil {
		if w.refusing {
			w.failed = true
		}
		w.mgr.log.Error("journal append failed", "topic", m.Topic, "err", err)
		w.mgr.count("hoardmq_journal_failures_total", opAppend, 1)
		return
	}
	w.journaled.Add(1)
}

// syncIfDirty makes the caller's journaled records durable. It returns at
// once only when a Sync that started after the caller's last write has
// returned nil: a Sync still in flight may fail, and a caller that did not
// wait for it would acknowledge over records it dropped (WI 1812). It returns
// false on failure.
func (m *Manager) syncIfDirty() bool {
	if m.opts.Journal == nil {
		return true
	}
	n := m.journaled.Load()
	if m.synced.Load() >= n {
		return true
	}
	if err := m.opts.Journal.Sync(); err != nil {
		m.log.Error("journal sync failed", "err", err)
		m.count("hoardmq_journal_failures_total", opSync, 1)
		return false
	}
	for {
		c := m.synced.Load()
		if c >= n || m.synced.CompareAndSwap(c, n) {
			return true
		}
	}
}

// meta is the persisted view of the session's metadata; the caller holds
// the session lock.
func (s *Session) meta() store.SessionMeta {
	meta := store.SessionMeta{
		Namespace: s.key.ns, ClientID: s.key.id, Epoch: s.epoch, Version: byte(s.version),
		Username: s.identity.Username, ExpiryInterval: s.expiryInterval, ExpiresAt: s.expiresAt,
	}
	if s.will != nil {
		meta.Will = &store.WillRecord{Message: s.will.Message, Delay: s.will.Delay, Armed: s.willArmed}
	}
	return meta
}

// snapshot is the persisted view of the whole session; the caller holds the
// session lock.
func (s *Session) snapshot() *store.SessionState {
	st := &store.SessionState{SessionMeta: s.meta(), Subscriptions: make([]store.SubscriptionRecord, 0, len(s.subs)), InboundQoS2: make([]uint16, 0, len(s.inboundQoS2)), Entries: make([]store.Entry, 0, len(s.inflight)+len(s.queue))}
	for _, sub := range s.subs {
		st.Subscriptions = append(st.Subscriptions, store.SubscriptionRecord{Filter: sub.Filter, QoS: sub.QoS, NoLocal: sub.NoLocal, RetainAsPublished: sub.RetainAsPublished, RetainHandling: sub.RetainHandling, Identifier: sub.Identifier})
	}
	for id, a := range s.inboundQoS2 {
		st.InboundQoS2 = append(st.InboundQoS2, id)
		if a.state == inboundAccepted {
			st.InboundAccepted = append(st.InboundAccepted, id)
		}
	}
	for _, f := range s.inflight {
		state := store.StatePublishSent
		if f.state == pubrecReceived {
			state = store.StatePubrecReceived
		}
		st.Entries = append(st.Entries, store.Entry{Seq: f.seq, Message: f.msg, QoS: f.qos, Identifiers: f.ids, Retain: f.retain, Group: f.group, PacketID: f.id, State: state})
	}
	for _, q := range s.queue {
		st.Entries = append(st.Entries, store.Entry{Seq: q.seq, Message: q.msg, QoS: q.qos, Identifiers: q.ids, Retain: q.retain, Group: q.group, PacketID: q.id})
	}
	return st
}

// Snapshot returns the persistent state of the sessions this node serves,
// which is what a resync or a rebalance sends its targets. Sealed copies are
// not in it: the session's new owner feeds the replicas now.
func (m *Manager) Snapshot() *store.State {
	st := store.NewState()
	// A snapshot must see a set of sessions that existed, so it takes every
	// shard rather than walking them one at a time (WI 1292 round 5, I3). It
	// copies the pointers and releases at once; the per-session snapshots
	// below are taken under each session's own mutex.
	sessions := m.listSessions(nil)
	for _, s := range sessions {
		s.mu.Lock()
		if s.persistent {
			st.Sessions = append(st.Sessions, s.snapshot())
		}
		s.mu.Unlock()
	}
	st.Retained = append(st.Retained, m.router.AllRetained()...)
	return st
}

// listSessions copies the session pointers under every shard lock and, when
// sealed is not nil, calls it for each sealed copy while those locks are held.
func (m *Manager) listSessions(sealed func(key, *sealedCopy)) []*Session {
	m.lockAll()
	defer m.unlockAll()
	var sessions []*Session
	for _, sh := range m.shards {
		for _, s := range sh.sessions {
			sessions = append(sessions, s)
		}
		if sealed != nil {
			for k, c := range sh.sealed {
				sealed(k, c)
			}
		}
	}
	return sessions
}

// CheckpointState is the persistent state a checkpoint of this node's own
// store writes: Snapshot's sessions and the sealed copies. A checkpoint
// replaces the journal before it, and a seal journals nothing, so a
// checkpoint without the sealed copies would delete them from disk before
// the new owner has confirmed it holds them; a node stopped then could not
// recover them.
//
// The store reads its index before calling this, and sessions are journaled
// while it runs, so each session's entry in Through is the journal's last
// index read while nothing could be journaled for it: under its lock, or for
// a sealed copy under its shard's (WI 1818).
func (m *Manager) CheckpointState(appended func() uint64) *store.State {
	st := store.NewState()
	// A key is listed once: it is in its shard's sessions or its sealed
	// copies, never both, and a session read again below is read as its key.
	put := func(k key, ss *store.SessionState, idx uint64) {
		// What a hand-over carries for the next owner is not state to recover.
		ss.Delivered = nil
		st.Sessions = append(st.Sessions, ss)
		st.Through = append(st.Through, store.SessionIndex{Namespace: k.ns, ClientID: k.id, Index: idx})
	}
	sessions := m.listSessions(func(k key, c *sealedCopy) {
		if c.persistent {
			cp := *c.st
			put(k, &cp, appended())
		}
	})
	if m.checkpointPause != nil {
		m.checkpointPause()
	}
	for _, s := range sessions {
		m.checkpointSession(s, appended, put)
	}
	st.Retained = append(st.Retained, m.router.AllRetained()...)
	return st
}

// checkpointSession reads a listed session for a checkpoint. One that left
// the map after it was listed — sealed, replaced or removed, each of which
// clears persistent — is read again as whatever holds its key now: the sealed
// copy, the session in its place, or nothing.
func (m *Manager) checkpointSession(s *Session, appended func() uint64, put func(key, *store.SessionState, uint64)) {
	for s != nil {
		s.mu.Lock()
		if s.persistent {
			ss, idx := s.snapshot(), appended()
			s.mu.Unlock()
			put(s.key, ss, idx)
			return
		}
		s.mu.Unlock()
		sh := m.shardFor(s.key)
		sh.mu.Lock()
		next := sh.sessions[s.key]
		switch next {
		case s:
			// Never persistent: nothing of it is journaled.
			next = nil
		case nil:
			if c := sh.sealed[s.key]; c != nil && c.persistent {
				cp := *c.st
				put(s.key, &cp, appended())
			}
		}
		sh.mu.Unlock()
		s = next
	}
}

// Load installs recovered state: offline sessions with their subscriptions,
// inbound identifiers, entries (in flight entries become unsent so the next
// connection gets them again), Wills and expiry timers. A session whose
// expiry has passed is discarded and its armed Will published.
func (m *Manager) Load(st *store.State) { m.load(st, false) }

// load is Load; handedOver marks sessions installed from a hand-over, which
// remember the messages they arrive holding.
func (m *Manager) load(st *store.State, handedOver bool) {
	now := m.clock.Now()
	m.router.LoadRetained(st.Retained)
	var wills []struct {
		w *Will
		s *Session
	}
	// Each session installs into its own shard and nothing here reads the
	// whole set, so this takes one shard at a time rather than all of them.
	// Holding every shard would also mean holding them across m.router.Add,
	// which broadcasts a route update — Import is one session, and it was
	// measured taking all 64 locks and the transport's with them.
	for _, ss := range st.Sessions {
		k := key{ss.Namespace, ss.ClientID}
		s, expired := m.rebuild(ss, now)
		if handedOver {
			s.installed = installedIdentities(ss)
		}
		if expired {
			if s.willArmed && s.will != nil {
				wills = append(wills, struct {
					w *Will
					s *Session
				}{s.will, s})
			}
			continue
		}
		// Reachable only now. Adding to the router publishes the session to
		// every concurrent publisher — Match reaches deliver, which writes
		// inflightByID and queuedIDs under the session's own mutex, while
		// rebuild writes them under none. Doing it before the state was built
		// raced, and the runtime caught it as "concurrent map read and map
		// write" in allocateID. The manager's lock never covered this:
		// delivery does not take it.
		for _, sb := range s.subs {
			m.router.Add(s, sb)
		}
		sh := m.shardFor(k)
		sh.mu.Lock()
		sh.sessions[k] = s
		s.mu.Lock()
		s.countQueue()
		s.mu.Unlock()
		sh.mu.Unlock()
		if s.willArmed && s.will != nil {
			w := s.will
			if w.Delay == 0 {
				wills = append(wills, struct {
					w *Will
					s *Session
				}{w, s})
				s.will, s.willArmed = nil, false
			} else {
				s.willTimer = m.clock.AfterFunc(time.Duration(w.Delay)*time.Second, func() { m.fireWill(s, w) })
			}
		}
		if s.expiryInterval != NeverExpires {
			s.expiryTimer = m.clock.AfterFunc(s.expiresAt.Sub(now), func() { m.expire(s) })
		}
	}
	for _, x := range wills {
		m.publishWill(x.w, x.s)
	}
}

// rebuild builds a session from its persisted form, reachable by nothing
// yet: no route, no shard map entry, no timer. expired reports a session whose
// expiry has passed (or that has none), which is not to be installed.
func (m *Manager) rebuild(ss *store.SessionState, now time.Time) (s *Session, expired bool) {
	s = newSession(key{ss.Namespace, ss.ClientID}, m.log)
	s.mgr = m
	s.epoch = ss.Epoch
	s.version = packet.Version(ss.Version)
	s.identity.Namespace, s.identity.ClientID, s.identity.Username = ss.Namespace, ss.ClientID, ss.Username
	s.expiryInterval = ss.ExpiryInterval
	s.expiresAt = ss.ExpiresAt
	if s.expiryInterval != NeverExpires && s.expiresAt.IsZero() {
		// The session was online when its state was written (a crash, or
		// an export during a live takeover): its expiry starts now, and
		// the connection that follows a takeover resets it.
		s.expiresAt = now.Add(time.Duration(s.expiryInterval) * time.Second)
	}
	s.persistent = true
	for _, id := range ss.Delivered {
		s.sent.add(sentMessage{id: id, at: now.UnixNano()}, now)
	}
	if ss.Will != nil {
		s.will = &Will{Message: ss.Will.Message, Delay: ss.Will.Delay}
		s.willArmed = ss.Will.Armed
	}
	for _, sub := range ss.Subscriptions {
		s.subs[sub.Filter] = Subscription{Filter: sub.Filter, QoS: sub.QoS, NoLocal: sub.NoLocal, RetainAsPublished: sub.RetainAsPublished, RetainHandling: sub.RetainHandling, Identifier: sub.Identifier}
	}
	for _, id := range ss.InboundQoS2 {
		a := &inboundAttempt{state: inboundUnconfirmed}
		if slices.Contains(ss.InboundAccepted, id) {
			a.state = inboundAccepted
		}
		s.inboundQoS2[id] = a
	}
	for _, e := range ss.Entries {
		if e.Seq > s.entrySeq {
			s.entrySeq = e.Seq
		}
		t := target{qos: e.QoS, ids: e.Identifiers, retain: e.Retain, group: e.Group}
		if e.State == store.StateQueued {
			q := &queued{seq: e.Seq, id: e.PacketID, msg: e.Message, target: t}
			if q.id != 0 {
				s.reserve(q.id)
				if q.id > s.nextID {
					s.nextID = q.id
				}
			}
			s.queue = append(s.queue, q)
			continue
		}
		f := &inflight{seq: e.Seq, id: e.PacketID, msg: e.Message, target: t}
		if e.State == store.StatePubrecReceived {
			f.state = pubrecReceived
		}
		s.inflight = append(s.inflight, f)
		s.inflightByID[f.id] = f
		if f.id > s.nextID {
			s.nextID = f.id
		}
	}
	lapsed := s.expiryInterval != NeverExpires && !s.expiresAt.IsZero() && !now.Before(s.expiresAt)
	return s, s.expiryInterval == 0 || lapsed
}
