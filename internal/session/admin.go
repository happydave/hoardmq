package session

import (
	"context"
	"sort"
	"strings"

	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

// CountPersistent counts the persistent sessions this node serves whose
// namespace and client identifier satisfy in. It holds one shard's lock at
// a time, to copy the sessions, and reads each one under its own mutex.
func (m *Manager) CountPersistent(in func(ns, id string) bool) int {
	var found []*Session
	for _, sh := range m.shards {
		sh.mu.Lock()
		for k, s := range sh.sessions {
			if in(k.ns, k.id) {
				found = append(found, s)
			}
		}
		sh.mu.Unlock()
	}
	n := 0
	for _, s := range found {
		s.mu.Lock()
		if s.persistent {
			n++
		}
		s.mu.Unlock()
	}
	return n
}

// ScanRecords lists the sessions of namespace ns whose client identifier
// contains search and sorts after the cursor, in identifier order, at most
// limit of them, and reports whether more remain. It holds one shard's lock
// at a time, and only to copy the matching sessions; each record is read
// under the session's own mutex afterwards. It never holds two shards, so a
// connect on another shard never waits for it.
func (m *Manager) ScanRecords(ns, search, after string, limit int) (out []Record, more bool) {
	var found []*Session
	for _, sh := range m.shards {
		sh.mu.Lock()
		for k, s := range sh.sessions {
			if k.ns == ns && k.id > after && strings.Contains(k.id, search) {
				found = append(found, s)
			}
		}
		sh.mu.Unlock()
	}
	sort.Slice(found, func(i, j int) bool { return found[i].key.id < found[j].key.id })
	if limit >= 0 && len(found) > limit {
		found, more = found[:limit], true
	}
	out = make([]Record, 0, len(found))
	for _, s := range found {
		s.mu.Lock()
		out = append(out, s.record())
		s.mu.Unlock()
	}
	return out, more
}

// held returns the session held for a key, taking only its shard's lock.
func (m *Manager) held(ns, id string) *Session {
	k := key{ns, id}
	sh := m.shardFor(k)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	return sh.sessions[k]
}

// Subscriptions returns a session's subscriptions in filter order, and
// whether the session is held here.
func (m *Manager) Subscriptions(ns, id string) ([]Subscription, bool) {
	s := m.held(ns, id)
	if s == nil {
		return nil, false
	}
	s.mu.Lock()
	out := make([]Subscription, 0, len(s.subs))
	for _, sub := range s.subs {
		out = append(out, sub)
	}
	s.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Filter < out[j].Filter })
	return out, true
}

// Conn returns the connection attached to a session held here, if any.
func (m *Manager) Conn(ns, id string) (Connection, bool) {
	s := m.held(ns, id)
	if s == nil {
		return nil, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn, s.conn != nil
}

// InvalidateResult is what Invalidate found and did.
type InvalidateResult struct {
	// Found is whether a session was held here; Refused whether admit
	// refused it, in which case nothing changed.
	Found, Refused bool
	// Scope names the discarded session for a durability wait.
	Scope store.Scope
	// WasOnline says a connection was closed; WillPublished that a Will went
	// out because the session ended.
	WasOnline, WillPublished bool
}

// Invalidate ends a session held here as an operator's action: the
// connection, if any, is closed with reason, the state is discarded and its
// journal delete written, the session's end is reported (the lease is
// released through Options.Ended), and its Will, when one is set on a
// connected session or armed on an offline one, is published, because the
// session has ended. admit is called with the session's token under its
// shard lock before anything changes; a refusal changes nothing.
func (m *Manager) Invalidate(ns, id string, reason packet.ReasonCode, admit func(token uint64) bool) InvalidateResult {
	release := m.admit()
	defer release()
	k := key{ns, id}
	sh := m.shardFor(k)
	sh.mu.Lock()
	s := sh.sessions[k]
	if s == nil {
		sh.mu.Unlock()
		return InvalidateResult{}
	}
	s.mu.Lock()
	res := InvalidateResult{Found: true, Scope: store.Scope{Namespace: ns, ClientID: id, Epoch: s.epoch}}
	if admit != nil && !admit(s.epoch) {
		s.mu.Unlock()
		sh.mu.Unlock()
		res.Refused = true
		return res
	}
	var old Connection
	if s.conn != nil {
		old = s.conn
		delete(sh.byConn, old)
		s.detach()
	}
	// A connected session's Will is set and not yet armed; an offline one
	// still has its Will only while it is armed (a normal DISCONNECT clears
	// it). Either way the session ends here, so it goes out now.
	will := s.will
	s.will, s.willArmed = nil, false
	redistribute := m.discardLocked(sh, s)
	s.mu.Unlock()
	sh.mu.Unlock()
	release()
	// The release is submitted before the client learns of the close, so its
	// reconnect's acquire is proposed after the release, not before it.
	m.ended(s)
	if old != nil {
		res.WasOnline = true
		old.Close(reason)
	}
	if will != nil {
		m.publishWill(will, s)
		res.WillPublished = true
	}
	m.redistribute(s, redistribute)
	m.syncIfDirty()
	return res
}

// QueryRetained returns the live retained messages a filter matches, from
// the whole cluster when replication is attached: the query a SUBSCRIBE
// makes.
func (m *Manager) QueryRetained(ns, filter string) []*message.Message {
	return m.retainedFor(ns, filter, m.clock.Now())
}

// DeleteRetained removes the retained message on a topic the way a retained
// PUBLISH with an empty payload does, without delivering anything to
// subscribers, and returns the scope a durability wait names. appended is
// false when the delete was stored here and its record failed to append: no
// durability wait can then answer for it. A delete handed to the shard
// leader is answered by the leader's acknowledgement.
func (m *Manager) DeleteRetained(ns, topic string) (scope store.Scope, appended bool) {
	msg := &message.Message{Namespace: ns, Topic: topic, Retain: true, Arrived: m.clock.Now()}
	local := true
	if r := m.Replication(); r != nil {
		local = r.StoreRetained(msg)
	}
	appended = true
	if local {
		m.router.Retain(msg)
		w := m.ackWriter()
		w.retain(msg)
		appended = !w.unjournaled()
	}
	if m.opts.Observer != nil {
		m.opts.Observer.OnRetained(context.Background(), msg)
	}
	return store.Scope{Namespace: ns, Topic: topic}, appended
}
