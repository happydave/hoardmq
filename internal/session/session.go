// Package session owns Session State as MQTT 5.0 section 4.1 defines it and
// runs the QoS 1 and 2 state machines in both directions, queuing, Wills and
// expiry. It sits behind the connection layer's Core interface.
package session

import (
	"errors"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/conn"
	"github.com/happydave/hoardmq/internal/hook"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

// Connection is what a session needs from the connection layer; *conn.Conn
// satisfies it and tests use a fake.
type Connection interface {
	ClientID() string
	Version() packet.Version
	Identity() *auth.Identity
	Deliver(*packet.Publish) error
	Send(packet.Packet) error
	Close(packet.ReasonCode)
	SendQuota() int
	SessionExpiryOverride() *uint32
}

// NeverExpires is the Session Expiry Interval meaning the session persists
// until explicitly ended.
const NeverExpires = 0xFFFFFFFF

type key struct{ ns, id string }

type inflightState byte

const (
	publishSent inflightState = iota
	pubrecReceived
)

// target is how one message is to be delivered to this session.
type target struct {
	qos    byte
	ids    []uint32
	retain bool
	group  string // shared group the message came through, or ""
}

// inflight is one outbound QoS 1 or 2 message awaiting completion.
type inflight struct {
	seq uint64
	id  uint16
	msg *message.Message
	target
	state inflightState
	// sent is false when the message must be (re)transmitted on the current
	// connection: after a resumption every entry starts unsent.
	sent bool
	// held is set while the PUBREC state is not yet journaled and on the
	// replicas: no PUBREL is sent for the entry until the retry clears it.
	held bool
}

type queued struct {
	seq uint64
	id  uint16 // reserved at enqueue for QoS 1 and 2; 0 at QoS 0
	msg *message.Message
	target
}

// Will is the stored Will message with its delay.
type Will struct {
	Message *message.Message
	Delay   uint32
}

// Session is one client's state, online or offline. mu guards every field;
// the manager's map lock is taken before it when both are needed.
type Session struct {
	mu      sync.Mutex
	key     key
	epoch   uint64
	version packet.Version
	conn    Connection
	// online mirrors conn != nil without the lock, for the router's
	// shared-group choice, which runs under the router lock.
	online   atomic.Bool
	identity auth.Identity

	expiryInterval uint32
	expiresAt      time.Time
	expiryTimer    Timer
	// persistent sessions (non-zero expiry at connect) are journaled.
	persistent bool
	entrySeq   uint64
	// unwritten marks a session one of whose own records failed to append:
	// the journal and the replicas lack it until the session is written
	// whole (Manager.rewrite). rewriteArmed is set while a rewrite is due.
	unwritten    bool
	rewriteArmed bool
	forwardErr   bool // a forward to a subscriber's node failed since the last acknowledgement

	subs        map[string]Subscription
	inboundQoS2 map[uint16]*inboundAttempt
	// acksHeld holds an error PUBREC or a PUBCOMP not sent because the
	// release of the inbound identifier it answers was not yet in the journal
	// and on the replicas.
	acksHeld     map[heldKey]*heldAck
	inflight     []*inflight
	inflightByID map[uint16]*inflight
	queuedIDs    map[uint16]bool // identifiers reserved by queued messages
	queue        []*queued
	// queueCounted is the queue length last added to the manager's count of
	// queued messages.
	queueCounted int
	nextID       uint16
	drops        uint64
	expired      uint64
	// installed holds the identities of the messages this session held when
	// it was installed here from a hand-over. A forward of one of them can be
	// applied after the install: the previous owner sent it before sealing
	// the session, and it waits in the routing plane's apply queue while the
	// answer carrying the session is handled on arrival (WI 1521).
	installed map[message.ID]struct{}
	// sent lists, on a node that forwards, the messages this session was sent
	// within sentWindow, oldest first and at most sentMax: a hand-over
	// carries them to the new owner, which adds them to installed.
	sent sentList

	will      *Will
	willArmed bool
	willTimer Timer

	// pendingRetained holds retained-message requests from a SUBSCRIBE until
	// the SUBACK has been sent.
	pendingRetained []retainedRequest

	log *slog.Logger
	mgr *Manager
}

// sentMessage is one message a session was sent, with this node's time.
type sentMessage struct {
	id message.ID
	at int64 // Unix nanoseconds
}

// A forward of a message this node sent a session can be applied on the
// session's next owner after the hand-over: it waits behind at most one
// batch of the receiver's apply loop, whose durability wait is 5 s. The
// window is twice that; the cap bounds the memory of a session sent more.
var (
	sentWindow = 10 * time.Second
	sentMax    = 64
)

// noteSent remembers a message sent to the session, when the manager
// forwards and the message has an identity. The caller holds the lock.
func (s *Session) noteSent(m *message.Message, now time.Time) {
	if s.mgr == nil || s.mgr.opts.Forwarder == nil {
		return
	}
	id, ok := m.Identity()
	if !ok {
		return
	}
	s.sent.add(sentMessage{id: id, at: now.UnixNano()}, now)
}

// sentList is the messages a session was sent, oldest first: a ring of at
// most sentMax entries, n of them held from head.
type sentList struct {
	e    []sentMessage
	head int
	n    int
}

func (l *sentList) at(i int) sentMessage { return l.e[(l.head+i)%len(l.e)] }

// add appends e, first dropping the entries older than the window and, at
// the cap, the oldest.
func (l *sentList) add(e sentMessage, now time.Time) {
	cut := now.Add(-sentWindow).UnixNano()
	for l.n > 0 && l.at(0).at < cut {
		l.head, l.n = (l.head+1)%len(l.e), l.n-1
	}
	switch {
	case l.n < len(l.e):
		l.e[(l.head+l.n)%len(l.e)] = e
		l.n++
	case len(l.e) < sentMax:
		// Full below the cap: put it in order and grow it.
		slices.Reverse(l.e[:l.head])
		slices.Reverse(l.e[l.head:])
		slices.Reverse(l.e)
		l.e, l.head = append(l.e, e), 0
		l.n++
	default:
		l.e[l.head] = e
		l.head = (l.head + 1) % len(l.e)
	}
}

// held is how many entries the list holds.
func (l *sentList) held() int { return l.n }

// sentWithin returns the identities of the messages sent within the window,
// oldest first; nil when there are none. The caller holds the lock.
func (s *Session) sentWithin(now time.Time) []message.ID {
	cut := now.Add(-sentWindow).UnixNano()
	var out []message.ID
	for i := 0; i < s.sent.n; i++ {
		if e := s.sent.at(i); e.at >= cut {
			out = append(out, e.id)
		}
	}
	return out
}

type retainedRequest struct {
	filter string
	qos    byte
	ids    []uint32
}

func newSession(k key, log *slog.Logger) *Session {
	return &Session{
		key:          k,
		subs:         map[string]Subscription{},
		inboundQoS2:  map[uint16]*inboundAttempt{},
		inflightByID: map[uint16]*inflight{},
		log:          log.With("client", k.id, "namespace", k.ns),
	}
}

// Record is the read-only view of a session for observers and stores.
type Record struct {
	Namespace      string
	ClientID       string
	Epoch          uint64
	Version        packet.Version
	Online         bool
	ExpiryInterval uint32
	ExpiresAt      time.Time
	Subscriptions  int
	InFlight       int
	Queued         int
	Drops          uint64
	Expired        uint64
	HasWill        bool
}

// storeKey names this session for the journal, carrying the lease epoch the
// node held for it when the record was written. The replication tap stamps
// this on the frame instead of looking the epoch up: a node journals for a
// session only while it owns that session's lease (Manager.SetEpoch is set
// from the acquire result in broker.core.Connect, and ownership moving here
// detaches the session), so this is the epoch the record was written under.
// The caller holds s.mu, as every journal write does.
// scope names this session for a replication barrier, at its current epoch.
func (s *Session) scope() store.Scope {
	s.mu.Lock()
	defer s.mu.Unlock()
	return store.Scope{Namespace: s.key.ns, ClientID: s.key.id, Epoch: s.epoch}
}

func (s *Session) storeKey() store.Key {
	return store.Key{Namespace: s.key.ns, ClientID: s.key.id, Epoch: s.epoch}
}

func (s *Session) record() Record {
	return Record{
		Namespace: s.key.ns, ClientID: s.key.id, Epoch: s.epoch, Version: s.version, Online: s.conn != nil,
		ExpiryInterval: s.expiryInterval, ExpiresAt: s.expiresAt,
		Subscriptions: len(s.subs), InFlight: len(s.inflight), Queued: len(s.queue),
		Drops: s.drops, Expired: s.expired, HasWill: s.will != nil,
	}
}

// allocateID returns the lowest free packet identifier, or 0 when all 65535
// are in flight.
func (s *Session) allocateID() uint16 {
	for tries := 0; tries < 65535; tries++ {
		s.nextID++
		if s.nextID == 0 {
			s.nextID = 1
		}
		if _, busy := s.inflightByID[s.nextID]; !busy && !s.queuedIDs[s.nextID] {
			return s.nextID
		}
	}
	return 0
}

// reserve and release track identifiers held by queued messages, so
// allocateID cannot hand the same one to two messages.
func (s *Session) reserve(id uint16) {
	if s.queuedIDs == nil {
		s.queuedIDs = map[uint16]bool{}
	}
	s.queuedIDs[id] = true
}

func (s *Session) release(id uint16) { delete(s.queuedIDs, id) }

func (s *Session) removeInflight(id uint16) *inflight {
	f, ok := s.inflightByID[id]
	if !ok {
		return nil
	}
	delete(s.inflightByID, id)
	for i, x := range s.inflight {
		if x == f {
			s.inflight = append(s.inflight[:i], s.inflight[i+1:]...)
			break
		}
	}
	return f
}

// enqueue adds a message to the queue under the configured limit and drop
// strategy. It returns the entry dropped to make room, if any, and whether
// the message was accepted.
func (s *Session) enqueue(q *queued, maxQueued int, dropNewest bool) (dropped *queued, ok bool) {
	if maxQueued > 0 && len(s.queue) >= maxQueued {
		s.drops++
		if dropNewest {
			// A message that never joins the queue must not keep the
			// identifier it reserved, or a session under sustained queue
			// pressure runs the space down until nothing can be sent.
			s.release(q.id)
			return nil, false
		}
		dropped = s.queue[0]
		s.queue = s.queue[1:]
		s.release(dropped.id)
	}
	s.queue = append(s.queue, q)
	return dropped, true
}

// countQueue brings the manager's count of queued messages up to date with
// this session's queue. The caller holds the session lock, or the session
// is not yet reachable by anyone else.
func (s *Session) countQueue() {
	if s.mgr == nil {
		return
	}
	if d := len(s.queue) - s.queueCounted; d != 0 {
		s.mgr.queued.Add(int64(d))
		s.queueCounted = len(s.queue)
	}
}

// deliver hands one message to the session: sent at once when the connection
// has room and nothing is queued ahead of it, queued otherwise. The caller
// holds the session lock.
func (s *Session) deliver(m *message.Message, t target, now time.Time, maxQueued int, dropNewest bool, j *journalWriter) {
	defer s.countQueue()
	s.entrySeq++
	seq := s.entrySeq
	if s.conn != nil && len(s.queue) == 0 {
		sent, f := s.trySend(seq, m, t, now)
		if sent {
			if f != nil {
				j.enqueued(s, f.seq, f.msg, f.target, f.id, true)
			}
			return
		}
	}
	q := &queued{seq: seq, msg: m, target: t}
	if t.qos > 0 {
		// The identifier is reserved now, not when the message is finally
		// sent, so it is part of the enqueue record the replicas hold before
		// the publish is acknowledged. A session rebuilt from a replica then
		// re-sends under the same identifier, and a client that has not
		// completed the exchange recognises it rather than taking it for a
		// new message.
		q.id = s.allocateID()
		if q.id != 0 {
			s.reserve(q.id)
		}
	}
	dropped, ok := s.enqueue(q, maxQueued, dropNewest)
	if dropped != nil {
		j.dropped(s, dropped.seq)
		s.notify(hook.Dropped, dropped.msg, dropped.qos)
	}
	if !ok {
		s.notify(hook.Dropped, m, t.qos)
		return
	}
	if withdraws(j.refuses(), j.enqueued(s, seq, m, t, q.id, false)) {
		// Neither the journal nor the replicas hold it, and its publish is
		// refused: the publisher's retry is the copy that counts.
		s.queue = s.queue[:len(s.queue)-1]
		s.release(q.id)
		return
	}
	s.notify(hook.Queued, m, t.qos)
}

// notify reports a delivery event through the manager, when one is set.
func (s *Session) notify(ev hook.DeliveryEvent, m *message.Message, qos byte) {
	if s.mgr != nil {
		s.mgr.event(ev, s, m, qos)
	}
}

// wire applies the subscriber's protocol level: subscription identifiers
// exist only at level 5. RETAIN is carried at every level: it is set for a
// retained message sent on subscribe, and for a forwarded message only when
// Retain As Published applies, which level 3 and 4 subscriptions cannot set.
func (s *Session) wire(t target, id uint16, dup bool) message.Delivery {
	d := message.Delivery{QoS: t.qos, PacketID: id, Dup: dup, Retain: t.retain}
	if s.version == packet.V5 {
		d.SubscriptionIdentifiers = t.ids
	}
	return d
}

// trySend delivers one message on the live connection. It returns whether
// the message was consumed (sent, or expired and dropped) and the in-flight
// entry created for QoS 1 and 2; when the send quota or the identifier space
// leaves no room nothing was consumed.
func (s *Session) trySend(seq uint64, m *message.Message, t target, now time.Time) (bool, *inflight) {
	return s.trySendID(seq, 0, m, t, now)
}

// trySendID is trySend with an identifier already reserved for the message;
// zero allocates one.
func (s *Session) trySendID(seq uint64, reserved uint16, m *message.Message, t target, now time.Time) (bool, *inflight) {
	if m.Expired(now) {
		s.expired++
		s.notify(hook.Expired, m, t.qos)
		return true, nil
	}
	if t.qos == 0 {
		if errors.Is(s.conn.Deliver(m.ToPublish(s.wire(t, 0, false), now)), conn.ErrBusy) {
			return false, nil
		}
		s.noteSent(m, now)
		s.notify(hook.Delivered, m, 0)
		return true, nil
	}
	if s.conn.SendQuota() <= 0 {
		return false, nil
	}
	id := reserved
	if id == 0 {
		id = s.allocateID()
	}
	if id == 0 {
		return false, nil
	}
	s.release(id)
	f := &inflight{seq: seq, id: id, msg: m, target: t, sent: true}
	s.inflight = append(s.inflight, f)
	s.inflightByID[id] = f
	if err := s.conn.Deliver(m.ToPublish(s.wire(t, id, false), now)); err != nil {
		s.removeInflight(id)
		return false, nil
	}
	s.noteSent(m, now)
	s.notify(hook.Delivered, m, t.qos)
	return true, f
}

// dropExpiredInflight removes the in-flight PUBLISHes awaiting retransmission
// whose Message Expiry Interval elapsed while the session was offline; an
// expired message is not delivered. An entry already past PUBREC is kept: its
// payload has been sent and the exchange must still be completed. The caller
// holds the session lock.
func (s *Session) dropExpiredInflight(now time.Time, j *journalWriter) {
	for i := len(s.inflight) - 1; i >= 0; i-- {
		f := s.inflight[i]
		if f.sent || f.state == pubrecReceived || !f.msg.Expired(now) {
			continue
		}
		s.inflight = append(s.inflight[:i], s.inflight[i+1:]...)
		delete(s.inflightByID, f.id)
		s.expired++
		s.notify(hook.Expired, f.msg, f.qos)
		// An in-flight entry leaves the log by its packet identifier: the
		// dropped record matches only a queued entry.
		j.complete(s, f.id)
	}
}

// drain first (re)transmits in-flight messages not yet sent on the current
// connection, in order, then sends queued messages while the quota allows.
// The caller holds the session lock.
func (s *Session) drain(now time.Time, j *journalWriter) {
	if s.conn == nil {
		return
	}
	defer s.countQueue()
	s.dropExpiredInflight(now, j)
	for _, f := range s.inflight {
		if f.sent {
			continue
		}
		if f.state == pubrecReceived {
			if f.held {
				continue
			}
			if err := s.conn.Send(&packet.Pubrel{Ack: packet.Ack{PacketID: f.id}}); err != nil {
				return
			}
			f.sent = true
			continue
		}
		if s.conn.SendQuota() <= 0 {
			return
		}
		if err := s.conn.Deliver(f.msg.ToPublish(s.wire(f.target, f.id, true), now)); err != nil {
			return
		}
		f.sent = true
	}
	for len(s.queue) > 0 {
		q := s.queue[0]
		sent, f := s.trySendID(q.seq, q.id, q.msg, q.target, now)
		if !sent {
			return
		}
		s.queue = s.queue[1:]
		if f != nil {
			j.sent(s, f.seq, f.id)
		} else {
			// Sent at QoS 0, or expired on the way out: it never became
			// in-flight, so its reservation is released here rather than by
			// trySendID, and its entry leaves the log and the replicas, or a
			// restart or a takeover would send it again (WI 1831).
			s.release(q.id)
			j.dropped(s, q.seq)
		}
	}
}

// detach drops the connection: every in-flight entry becomes unsent so a
// resumption retransmits it.
func (s *Session) detach() {
	s.conn = nil
	s.online.Store(false)
	for _, f := range s.inflight {
		f.sent = false
	}
}

// Online reports whether a connection is attached. It takes no lock so the
// router may call it while holding its own.
func (s *Session) Online() bool { return s.online.Load() }

// Namespace and ClientID identify the session.
func (s *Session) Namespace() string { return s.key.ns }

// ClientID is the client identifier.
func (s *Session) ClientID() string { return s.key.id }

// sharedQoS1 returns the QoS 1 messages that arrived through a shared group
// and are still in flight or queued, for redistribution when the session
// ends; QoS 2 messages are never redistributed. The caller holds the lock.
func (s *Session) sharedQoS1() []redistributable {
	var out []redistributable
	for _, f := range s.inflight {
		if f.group != "" && f.qos == 1 {
			out = append(out, redistributable{msg: f.msg, group: f.group})
		}
	}
	for _, q := range s.queue {
		if q.group != "" && q.qos == 1 {
			out = append(out, redistributable{msg: q.msg, group: q.group})
		}
	}
	return out
}

type redistributable struct {
	msg   *message.Message
	group string
}

func (s *Session) stopTimers() {
	if s.expiryTimer != nil {
		s.expiryTimer.Stop()
		s.expiryTimer = nil
	}
	if s.willTimer != nil {
		s.willTimer.Stop()
		s.willTimer = nil
	}
}
