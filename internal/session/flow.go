package session

import (
	"context"
	"slices"
	"strconv"
	"time"

	"github.com/happydave/hoardmq/internal/hook"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/otel"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
	"github.com/happydave/hoardmq/internal/topic"
)

// lookup finds the session attached to a connection, or nil when the
// connection has been detached (after a takeover its callbacks are ignored).
func (m *Manager) lookup(c Connection) *Session {
	sh := m.shardForConn(c)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if s := sh.byConn[c]; s != nil && s.conn == c {
		return s
	}
	return nil
}

// Publish routes an inbound PUBLISH and returns the acknowledgement reason;
// the connection sends the acknowledgement when send is true. QoS 2 uses
// Method A: the message is routed on receipt and the packet identifier is
// remembered until PUBREL so a duplicate is not routed twice.
func (m *Manager) Publish(c Connection, p *packet.Publish) (code packet.ReasonCode, send bool) {
	sp := m.opts.Tracer.StartPublish(p.Properties.UserProperties)
	code, s, a, opened := m.publish(c, p, sp)
	if opened {
		m.settle(s, p.PacketID, a, code)
	}
	send = true
	if p.QoS == 2 && code.IsError() {
		send = m.releaseRefused(s, c, p.PacketID, a, code)
	}
	if sp.Recording() {
		m.endPublishSpan(sp, p, code)
	}
	return code, send
}

// endPublishSpan names what the publish span covered and ends it. Only the
// topic and counts are recorded: never the payload or a property's value.
func (m *Manager) endPublishSpan(sp otel.Span, p *packet.Publish, code packet.ReasonCode) {
	sp.String("messaging.system", "mqtt")
	sp.String("messaging.destination.name", p.Topic)
	sp.Int("hoardmq.qos", int64(p.QoS))
	sp.String("hoardmq.node", m.opts.Node)
	if code.IsError() {
		sp.Int("mqtt.reason_code", int64(code))
		sp.Error(code.String())
	}
	sp.End()
}

// releaseRefused releases the identifier of a QoS 2 publish refused with
// code and reports whether its PUBREC may go now. MQTT 5 §4.3.3: an error
// PUBREC ends the exchange, and the client may reuse the identifier for a
// new message at once, so a release that a recovery or a takeover from a
// replica does not see would answer that message as a duplicate and drop
// it. The PUBREC therefore waits, as the PUBCOMP does, until the release is
// in the journal and on the replicas. Only attempt a is released: the
// identifier may since have been opened for a newer one. A connection no
// longer attached is answered at once; its client resends on the next.
func (m *Manager) releaseRefused(s *Session, c Connection, id uint16, a *inboundAttempt, code packet.ReasonCode) bool {
	if s == nil {
		return true
	}
	s.mu.Lock()
	if a != nil && s.inboundQoS2[id] == a {
		delete(s.inboundQoS2, id)
		m.writer().inbound(s, id, false)
	}
	unwritten := s.unwritten
	attached := s.conn == c
	s.mu.Unlock()
	if !attached {
		return true
	}
	if !unwritten && m.recordsDurable(s) {
		return true
	}
	m.hold(s, heldKey{ackPubrec, id}, c, code)
	m.count("hoardmq_pubrec_held_total", nil, 1)
	s.log.Debug("error PUBREC held: the release is not journaled and on the replicas yet", "id", id)
	return false
}

// publish takes an inbound PUBLISH in. At QoS 2 it returns the attempt that
// holds the identifier, and whether this call opened it: the caller settles an
// attempt it opened.
func (m *Manager) publish(c Connection, p *packet.Publish, sp otel.Span) (code packet.ReasonCode, s *Session, a *inboundAttempt, opened bool) {
	s = m.lookup(c)
	if s == nil {
		return packet.UnspecifiedError, nil, nil, false
	}
	if sp.Recording() {
		sp.String("messaging.client.id", s.key.id)
		sp.String("hoardmq.namespace", s.key.ns)
	}
	inboundJournaled := true
	if p.QoS == 2 {
		s.mu.Lock()
		a = s.inboundQoS2[p.PacketID]
		if m.opts.ForgetInboundQoS2 {
			a = nil
		}
		state := inboundPending
		if a != nil {
			state = a.state
		} else {
			a = &inboundAttempt{state: inboundPending, done: make(chan struct{})}
			opened = true
			s.inboundQoS2[p.PacketID] = a
			// An error PUBREC still held for the identifier answered an
			// exchange this one supersedes; a refusal of this one holds its own.
			delete(s.acksHeld, heldKey{ackPubrec, p.PacketID})
			inboundJournaled = m.writer().inbound(s, p.PacketID, true)
		}
		s.mu.Unlock()
		if !opened {
			return m.publishAgain(c, a, state), s, a, false
		}
	}
	s.mu.Lock()
	pub := m.publisher(s)
	s.mu.Unlock()
	msg := message.FromPublish(p, pub, m.seq.Add(1), m.clock.Now())
	msg.Trace = sp.Context()
	// A QoS 0 publish is acknowledged by nothing, so it names nothing and the
	// fan-out costs no allocation. At QoS 1 and 2 the acknowledgement claims
	// durability for the publisher's own record and for every session this
	// message was delivered into, so the barrier is told all of them.
	var covers []store.Scope
	var into *[]store.Scope
	if p.QoS > 0 && m.Replication() != nil {
		covers = append(covers, s.scope())
		into = &covers
	}
	// A record the publish wrote into another session reports its failure
	// here, not on that session: this acknowledgement claims it.
	matched, unjournaled := m.dispatch(msg, s, into, p.QoS > 0)
	if sp.Recording() {
		sp.Int("hoardmq.matched", int64(matched))
	}
	if p.QoS == 0 {
		return packet.Success, s, a, opened
	}
	// Design invariant 5: the acknowledgement follows durability.
	if !m.syncIfDirty() {
		return packet.ImplementationSpecificError, s, a, opened
	}
	s.mu.Lock()
	refused := s.forwardErr || unjournaled || !inboundJournaled
	s.forwardErr = false
	s.mu.Unlock()
	if refused {
		return packet.ImplementationSpecificError, s, a, opened
	}
	if r := m.Replication(); r != nil {
		// Design invariant 5, across nodes: the followers hold it too.
		if err := r.Barrier(context.Background(), covers, msg.Forwards); err != nil {
			m.log.Warn("replication barrier failed", "client", s.key.id, "err", err)
			return packet.ImplementationSpecificError, s, a, opened
		}
	}
	if matched == 0 && c.Version() == packet.V5 {
		return packet.NoMatchingSubscribers, s, a, opened
	}
	return packet.Success, s, a, opened
}

// inboundAttempt is the state of an open inbound QoS 2 identifier.
type inboundAttempt struct {
	state inboundState
	// done is closed when the verdict, code, is reached. Only an attempt this
	// node opened has one.
	done chan struct{}
	code packet.ReasonCode
}

type inboundState byte

const (
	// inboundUnconfirmed: loaded, imported or taken over with no acceptance
	// recorded, so the publish may never have reached its subscribers.
	inboundUnconfirmed inboundState = iota
	inboundPending
	inboundAccepted
)

// publishAgain answers a PUBLISH under an identifier already open. Only an
// accepted publish is a duplicate to absorb. One whose verdict is pending is
// answered with that verdict. One whose acceptance did not survive a failure
// is refused: answering it as a duplicate would drop a message that may never
// have been delivered.
func (m *Manager) publishAgain(c Connection, a *inboundAttempt, state inboundState) packet.ReasonCode {
	switch state {
	case inboundAccepted:
		return packet.Success
	case inboundPending:
		<-a.done
		return a.code
	}
	m.count("hoardmq_qos2_unconfirmed_refused_total", map[string]string{"level": strconv.Itoa(int(c.Version()))}, 1)
	return packet.ImplementationSpecificError
}

// settle records the verdict of an attempt this node opened: an accepted one
// is marked and journaled as accepted, unless its identifier has since been
// released, and anything waiting on the verdict is given it.
func (m *Manager) settle(s *Session, id uint16, a *inboundAttempt, code packet.ReasonCode) {
	s.mu.Lock()
	if !code.IsError() && s.inboundQoS2[id] == a {
		a.state = inboundAccepted
		m.writer().accepted(s, id)
	}
	a.code = code
	s.mu.Unlock()
	close(a.done)
}

// heldKey names a held acknowledgement: its kind and packet identifier.
type heldKey struct {
	kind ackKind
	id   uint16
}

type ackKind byte

const (
	ackPubrec ackKind = iota
	ackPubcomp
)

// heldAck is an acknowledgement waiting for its identifier's release to be
// durable: the connection it is for and the reason it answers. Each hold has
// its own retry, which sends only while the hold it was armed for is the one
// recorded, so a later hold under the same key waits for its own release.
type heldAck struct {
	conn Connection
	code packet.ReasonCode
}

func (k heldKey) packet(code packet.ReasonCode) packet.Packet {
	if k.kind == ackPubrec {
		return &packet.Pubrec{Ack: packet.Ack{PacketID: k.id, ReasonCode: code}}
	}
	return &packet.Pubcomp{Ack: packet.Ack{PacketID: k.id, ReasonCode: code}}
}

// Pubrel completes an inbound QoS 2 exchange; the connection sends the
// PUBCOMP when send is true. After the PUBCOMP the client may reuse the
// identifier for a new message, so a release that a recovery or a takeover
// from a replica does not see would answer that message as a duplicate and
// drop it. The PUBCOMP therefore waits until the release is in the journal
// and on the replicas, and is held and retried otherwise. A PUBREL whose
// identifier is not open waits the same way: it may be the client's resend
// after a PUBCOMP held for a connection that has since gone.
func (m *Manager) Pubrel(c Connection, id uint16) (packet.ReasonCode, bool) {
	s := m.lookup(c)
	if s == nil {
		return packet.UnspecifiedError, true
	}
	k := heldKey{ackPubcomp, id}
	s.mu.Lock()
	if h, held := s.acksHeld[k]; held {
		// The retry armed for it is still running; a resend on a new
		// connection moves the hold there before the retry drops it.
		h.conn = c
		code := h.code
		s.mu.Unlock()
		return code, false
	}
	code := packet.PacketIdentifierNotFound
	if _, ok := s.inboundQoS2[id]; ok {
		delete(s.inboundQoS2, id)
		m.writer().inbound(s, id, false)
		code = packet.Success
	}
	// A failed append marks the session for a rewrite; until then the
	// release is in neither the journal nor, through the tap, the replicas.
	unwritten := s.unwritten
	s.mu.Unlock()
	if !unwritten && m.recordsDurable(s) {
		return code, true
	}
	m.hold(s, k, c, code)
	m.count("hoardmq_pubcomp_held_total", nil, 1)
	s.log.Debug("PUBCOMP held: the release is not journaled and on the replicas yet", "id", id)
	return code, false
}

// hold records an acknowledgement for c that waits for its release, and
// arms its retry.
func (m *Manager) hold(s *Session, k heldKey, c Connection, code packet.ReasonCode) {
	h := &heldAck{conn: c, code: code}
	s.mu.Lock()
	if s.acksHeld == nil {
		s.acksHeld = map[heldKey]*heldAck{}
	}
	s.acksHeld[k] = h
	s.mu.Unlock()
	m.retryHeld(s, k, h)
}

// retryHeld re-attempts the held acknowledgement h: it writes the session
// whole when an own record failed to append, then waits for the journal and
// the replicas, then sends it. It stops when h is no longer the hold recorded
// for k, and drops h when the session's connection is no longer the one h is
// for: the client resends on its next connection, and a session that left
// this node has none.
func (m *Manager) retryHeld(s *Session, k heldKey, h *heldAck) {
	m.clock.AfterFunc(pubrelRetry, func() {
		if !m.stillHeld(s, k, h, false) {
			return
		}
		switch m.rewrite(s) {
		case rewriteGone:
			return
		case rewriteFailed:
			m.retryHeld(s, k, h)
			return
		}
		if !m.recordsDurable(s) {
			m.retryHeld(s, k, h)
			return
		}
		s.mu.Lock()
		send := m.stillHeldLocked(s, k, h, true)
		c, code := h.conn, h.code
		s.mu.Unlock()
		if send {
			_ = c.Send(k.packet(code))
		}
	})
}

// stillHeld reports whether h is the hold recorded for k and is for the
// connection now attached, dropping it when the connection has gone; take
// also removes it, for a send.
func (m *Manager) stillHeld(s *Session, k heldKey, h *heldAck, take bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return m.stillHeldLocked(s, k, h, take)
}

func (m *Manager) stillHeldLocked(s *Session, k heldKey, h *heldAck, take bool) bool {
	if s.acksHeld[k] != h {
		return false
	}
	if s.conn != h.conn || take {
		delete(s.acksHeld, k)
	}
	return s.conn == h.conn
}

// Puback completes an outbound QoS 1 message and frees the quota for the
// next queued one.
func (m *Manager) Puback(c Connection, id uint16, _ packet.ReasonCode) {
	s := m.lookup(c)
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if f := s.removeInflight(id); f == nil {
		s.log.Debug("PUBACK for unknown packet identifier", "id", id)
	} else {
		m.writer().complete(s, id)
		m.event(hook.Acknowledged, s, f.msg, f.qos)
	}
	s.drain(m.clock.Now(), m.writer())
}

// Pubrec moves an outbound QoS 2 message to the second phase; the
// connection sends PUBREL when Success is returned. An error reason from
// the client ends the exchange.
func (m *Manager) Pubrec(c Connection, id uint16, reason packet.ReasonCode) (packet.ReasonCode, bool) {
	s := m.lookup(c)
	if s == nil {
		return packet.PacketIdentifierNotFound, true
	}
	s.mu.Lock()
	f := s.inflightByID[id]
	if f == nil {
		s.log.Debug("PUBREC for unknown packet identifier", "id", id)
		s.mu.Unlock()
		return packet.PacketIdentifierNotFound, true
	}
	if reason.IsError() {
		s.removeInflight(id)
		m.writer().complete(s, id)
		s.drain(m.clock.Now(), m.writer())
		s.mu.Unlock()
		return reason, true
	}
	f.state = pubrecReceived
	journaled := m.writer().state(s, id, pubrecReceived)
	s.mu.Unlock()
	// Exactly once across an owner loss. The client completes the exchange
	// when it receives the PUBREL and releases the packet identifier; after
	// that, a session rebuilt from a replica that does not know the client
	// has the message delivers it again as a new one. So the PUBREL waits
	// until the replicas hold the PUBREC state: the client's exchange never
	// gets ahead of what the cluster can recover. When that cannot be done —
	// the owner is cut off from its followers — the PUBREL is held rather
	// than sent, the client keeps the identifier open, and a rebuild's
	// re-send is recognised as the same exchange instead of a new message.
	if m.opts.PubrelUnreplicated {
		// Defects: send it regardless, the way a broker that treats the
		// replication as best effort would.
		return packet.Success, true
	}
	// A state whose append failed is not in the journal, and the tap never
	// fed it to the replicas: a sync or a barrier now would pass over it.
	if !journaled || !m.recordsDurable(s) {
		s.mu.Lock()
		if g := s.inflightByID[id]; g != nil {
			g.sent, g.held = false, true
		}
		s.mu.Unlock()
		m.count("hoardmq_pubrel_held_total", nil, 1)
		s.log.Debug("PUBREL held: the PUBREC state is not journaled and on the replicas yet", "id", id)
		m.retryPubrel(s, id)
		return packet.Success, false
	}
	return packet.Success, true
}

// recordsDurable makes the journal durable and waits for the replicas to
// hold the session's records. A single node (no replication attached) is
// trivially satisfied.
func (m *Manager) recordsDurable(s *Session) bool {
	if !m.syncIfDirty() {
		s.log.Warn("session records not synced")
		return false
	}
	r := m.Replication()
	if r == nil {
		return true
	}
	if err := r.Durable(context.Background(), []store.Scope{s.scope()}); err != nil {
		s.log.Debug("session records not replicated", "err", err)
		return false
	}
	return true
}

// retryPubrel re-attempts a held PUBREL: it writes the session whole when an
// own record failed to append, then waits for the journal and the replicas to
// hold the PUBREC state, then releases the hold and sends the PUBREL if a
// connection is attached; otherwise the next connection's drain sends it. It
// re-arms itself until then, and stops when the entry leaves the session or
// the session leaves this node — a new owner re-sends.
func (m *Manager) retryPubrel(s *Session, id uint16) {
	m.clock.AfterFunc(pubrelRetry, func() {
		s.mu.Lock()
		f := s.inflightByID[id]
		if f == nil || f.state != pubrecReceived || !f.held {
			s.mu.Unlock()
			return
		}
		unwritten := s.unwritten
		s.mu.Unlock()
		if !unwritten && !m.holds(s) {
			return
		}
		switch m.rewrite(s) {
		case rewriteGone:
			return
		case rewriteFailed:
			m.retryPubrel(s, id)
			return
		}
		if !m.recordsDurable(s) {
			m.retryPubrel(s, id)
			return
		}
		s.mu.Lock()
		f, conn := s.inflightByID[id], s.conn
		if f == nil || f.state != pubrecReceived || !f.held {
			s.mu.Unlock()
			return
		}
		f.held = false
		send := conn != nil && !f.sent
		if send {
			f.sent = true
		}
		s.mu.Unlock()
		if !send {
			return
		}
		if err := conn.Send(&packet.Pubrel{Ack: packet.Ack{PacketID: id}}); err != nil {
			s.mu.Lock()
			if g := s.inflightByID[id]; g != nil {
				g.sent = false
			}
			s.mu.Unlock()
		}
	})
}

// pubrelRetry is how long a held PUBREL waits before trying the replicas
// again.
const pubrelRetry = 250 * time.Millisecond

// Pubcomp completes an outbound QoS 2 message.
func (m *Manager) Pubcomp(c Connection, id uint16, _ packet.ReasonCode) {
	s := m.lookup(c)
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if f := s.removeInflight(id); f == nil {
		s.log.Debug("PUBCOMP for unknown packet identifier", "id", id)
	} else {
		m.writer().complete(s, id)
		m.event(hook.Acknowledged, s, f.msg, f.qos)
	}
	s.drain(m.clock.Now(), m.writer())
}

// Subscribe adds or replaces subscriptions and grants the requested QoS. For
// a persistent session a filter is granted only once its record is appended
// and synced; one whose append or sync failed is refused with 0x83 and put
// back as it was.
func (m *Manager) Subscribe(c Connection, p *packet.Subscribe) []packet.ReasonCode {
	codes := make([]packet.ReasonCode, len(p.Subscriptions))
	s := m.lookup(c)
	if s == nil {
		for i := range codes {
			codes[i] = packet.UnspecifiedError
		}
		return codes
	}
	var id uint32
	if len(p.Properties.SubscriptionIdentifiers) > 0 {
		id = p.Properties.SubscriptionIdentifiers[0]
	}
	w := m.ackWriter()
	var changes []filterChange
	s.mu.Lock()
	for i, ps := range p.Subscriptions {
		sub := Subscription{Filter: ps.Filter, QoS: ps.QoS, NoLocal: ps.NoLocal, RetainAsPublished: ps.RetainAsPublished, RetainHandling: ps.RetainHandling, Identifier: id}
		if s.version != packet.V5 {
			// The 3.x SUBSCRIBE carries only the QoS.
			sub.NoLocal, sub.RetainAsPublished, sub.RetainHandling, sub.Identifier = false, false, 0, 0
		}
		prev, had := s.subs[ps.Filter]
		s.subs[ps.Filter] = sub
		existed := m.router.Add(s, sub)
		if w.writes(s) {
			changes = append(changes, filterChange{index: i, filter: ps.Filter, prev: prev, had: had, set: sub, journaled: w.subscribe(s, sub)})
		}
		codes[i] = packet.ReasonCode(ps.QoS)
		if topic.IsShared(ps.Filter) {
			continue
		}
		handling := sub.RetainHandling
		if s.version != packet.V5 {
			handling = 0
		}
		if handling == 2 || (handling == 1 && existed) {
			continue
		}
		var ids []uint32
		if id != 0 {
			ids = []uint32{id}
		}
		s.pendingRetained = append(s.pendingRetained, retainedRequest{filter: ps.Filter, qos: ps.QoS, ids: ids})
	}
	s.mu.Unlock()
	if m.refuseUnsynced(changes, codes) {
		m.putBack(s, c, changes, true)
	}
	return codes
}

// filterChange is one filter a SUBSCRIBE or UNSUBSCRIBE changed in a
// persistent session: what the session held for it before, what the request
// set (an UNSUBSCRIBE sets nothing), and whether its record appended.
type filterChange struct {
	index     int
	filter    string
	prev      Subscription
	had       bool
	set       Subscription
	journaled bool
	refused   bool
}

// refuseUnsynced syncs the journal when any change's record appended, and
// marks each change whose append or sync failed refused, with 0x83 in codes.
// It reports whether any was refused.
func (m *Manager) refuseUnsynced(changes []filterChange, codes []packet.ReasonCode) bool {
	synced := false
	for _, ch := range changes {
		if ch.journaled {
			synced = m.syncIfDirty()
			break
		}
	}
	refused := false
	for i := range changes {
		if !changes[i].journaled || !synced {
			changes[i].refused = true
			codes[changes[i].index] = packet.ImplementationSpecificError
			refused = true
		}
	}
	return refused
}

// putBack returns each refused filter to what the session held before the
// request, in memory, in the router and, where the request's record reached
// the journal, by a record that undoes it. It does nothing once the session
// has left the connection that asked, or when the filter has changed since:
// whoever moved it owns it now.
func (m *Manager) putBack(s *Session, c Connection, changes []filterChange, subscribed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != c {
		return
	}
	w := m.writer()
	// Last first, so a filter the request named twice ends as it began.
	for i := len(changes) - 1; i >= 0; i-- {
		ch := changes[i]
		if !ch.refused {
			continue
		}
		cur, holds := s.subs[ch.filter]
		if subscribed && (!holds || cur != ch.set) || !subscribed && holds {
			continue
		}
		if subscribed {
			s.pendingRetained = slices.DeleteFunc(s.pendingRetained, func(r retainedRequest) bool { return r.filter == ch.filter })
		}
		if !ch.had {
			delete(s.subs, ch.filter)
			m.router.Remove(s, ch.filter)
			if ch.journaled {
				w.unsubscribe(s, ch.filter)
			}
			continue
		}
		s.subs[ch.filter] = ch.prev
		m.router.Add(s, ch.prev)
		if ch.journaled {
			w.subscribe(s, ch.prev)
		}
	}
}

// Subscribed delivers the retained messages a SUBSCRIBE asked for, after the
// SUBACK has gone out.
func (m *Manager) Subscribed(c Connection) {
	s := m.lookup(c)
	if s == nil {
		return
	}
	now := m.clock.Now()
	s.mu.Lock()
	pending := s.pendingRetained
	s.pendingRetained = nil
	for _, req := range pending {
		for _, r := range m.retainedFor(s.key.ns, req.filter, now) {
			qos := r.QoS
			if req.qos < qos {
				qos = req.qos
			}
			l := m.limits.Load()
			s.deliver(r, target{qos: qos, ids: req.ids, retain: true}, now, l.MaxQueued, l.DropNewest, m.writer())
		}
	}
	s.mu.Unlock()
	m.syncIfDirty()
}

// Unsubscribe removes subscriptions. For a persistent session a removal is
// acknowledged only once its record is appended and synced; one whose append
// or sync failed is refused with 0x83 and the subscription put back.
func (m *Manager) Unsubscribe(c Connection, p *packet.Unsubscribe) []packet.ReasonCode {
	codes := make([]packet.ReasonCode, len(p.Filters))
	s := m.lookup(c)
	if s == nil {
		for i := range codes {
			codes[i] = packet.UnspecifiedError
		}
		return codes
	}
	w := m.ackWriter()
	var changes []filterChange
	s.mu.Lock()
	for i, f := range p.Filters {
		prev, ok := s.subs[f]
		if !ok {
			codes[i] = packet.NoSubscriptionExisted
			continue
		}
		delete(s.subs, f)
		m.router.Remove(s, f)
		if w.writes(s) {
			changes = append(changes, filterChange{index: i, filter: f, prev: prev, had: true, journaled: w.unsubscribe(s, f)})
		}
		codes[i] = packet.Success
	}
	s.mu.Unlock()
	if m.refuseUnsynced(changes, codes) {
		m.putBack(s, c, changes, false)
	}
	return codes
}

// Disconnect ends a connection's association with its session: the Will is
// discarded or armed, the expiry override applied, and the session discarded,
// kept, or scheduled to expire.
func (m *Manager) Disconnect(c Connection, graceful bool, _ packet.ReasonCode, willWanted bool) {
	// The gate before the shard lock, never under it: this path journals,
	// and waiting for the store while holding a shard would rebuild the
	// convoy sharding removes, one shard at a time (WI 1292 round 5, I6).
	release := m.admit()
	defer release()
	sh := m.shardForConn(c)
	sh.mu.Lock()
	s := sh.byConn[c]
	if s == nil || s.conn != c {
		sh.mu.Unlock()
		return
	}
	delete(sh.byConn, c)
	s.mu.Lock()
	s.detach()
	if o := c.SessionExpiryOverride(); o != nil {
		if s.expiryInterval == 0 {
			s.log.Debug("Session Expiry Interval on DISCONNECT ignored: CONNECT set 0")
		} else if cap := m.limits.Load().MaxSessionExpiry; cap != 0 && *o > cap {
			s.expiryInterval = cap
		} else {
			s.expiryInterval = *o
		}
	}
	var will *Will
	if !willWanted {
		s.will = nil
	} else if s.will != nil {
		s.willArmed = true
		switch {
		case s.will.Delay == 0 || s.expiryInterval == 0:
			will = s.will
			s.will, s.willArmed = nil, false
		default:
			// The Will goes out when the delay elapses or the session ends,
			// whichever comes first.
			w := s.will
			wait := w.Delay
			if s.expiryInterval != NeverExpires && s.expiryInterval < wait {
				wait = s.expiryInterval
			}
			s.willTimer = m.clock.AfterFunc(time.Duration(wait)*time.Second, func() { m.fireWill(s, w) })
		}
	}
	var expired bool
	var redistribute []redistributable
	switch s.expiryInterval {
	case 0:
		redistribute = m.discardLocked(sh, s)
		expired = true
	case NeverExpires:
		m.writer().upsert(s)
	default:
		s.expiresAt = m.clock.Now().Add(time.Duration(s.expiryInterval) * time.Second)
		s.expiryTimer = m.clock.AfterFunc(time.Duration(s.expiryInterval)*time.Second, func() { m.expire(s) })
		m.writer().upsert(s)
	}
	s.mu.Unlock()
	sh.mu.Unlock()
	release()
	if will != nil {
		m.publishWill(will, s)
	}
	_ = graceful
	if expired {
		m.redistribute(s, redistribute)
		m.ended(s)
		s.log.Debug("session ended with the connection")
	}
}

// fireWill runs when the Will Delay elapses with no new connection.
func (m *Manager) fireWill(s *Session, w *Will) {
	s.mu.Lock()
	armed := s.willArmed && s.will == w && s.conn == nil
	if armed {
		s.will, s.willArmed, s.willTimer = nil, false, nil
		m.writer().upsert(s)
	}
	s.mu.Unlock()
	if armed {
		m.publishWill(w, s)
	}
}

// expire runs when the Session Expiry Interval elapses offline.
func (m *Manager) expire(s *Session) {
	sh := m.shardFor(s.key)
	sh.mu.Lock()
	s.mu.Lock()
	if s.conn != nil || sh.sessions[s.key] != s {
		s.mu.Unlock()
		sh.mu.Unlock()
		return
	}
	var will *Will
	if s.willArmed && s.will != nil {
		will = s.will
		s.will, s.willArmed = nil, false
	}
	redistribute := m.discardLocked(sh, s)
	s.mu.Unlock()
	sh.mu.Unlock()
	if will != nil {
		m.publishWill(will, s)
	}
	m.redistribute(s, redistribute)
	m.ended(s)
	s.log.Debug("session expired")
}
