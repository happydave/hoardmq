package store

import (
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/happydave/hoardmq/internal/message"
)

// Record types. The byte is the log record's type field; payloads are JSON.
const (
	recSessionUpsert byte = 1
	recSessionDelete byte = 2
	recSubscribe     byte = 3
	recUnsubscribe   byte = 4
	recInboundAdd    byte = 5
	recInboundRemove byte = 6
	recEnqueue       byte = 7
	recSent          byte = 8
	recInflightState byte = 9
	recComplete      byte = 10
	recDropped       byte = 11
	recRetain        byte = 12
	recRetainDelete  byte = 13
)

// Entry states.
const (
	StateQueued         byte = 0
	StatePublishSent    byte = 1
	StatePubrecReceived byte = 2
)

// SubscriptionRecord is a persisted subscription.
type SubscriptionRecord struct {
	Filter            string `json:"filter"`
	QoS               byte   `json:"qos"`
	NoLocal           bool   `json:"noLocal,omitempty"`
	RetainAsPublished bool   `json:"rap,omitempty"`
	RetainHandling    byte   `json:"rh,omitempty"`
	Identifier        uint32 `json:"id,omitempty"`
}

// WillRecord is a persisted Will.
type WillRecord struct {
	Message *message.Message `json:"message"`
	Delay   uint32           `json:"delay"`
	Armed   bool             `json:"armed"`
}

// SessionMeta is a session's persisted metadata.
type SessionMeta struct {
	Namespace      string      `json:"ns"`
	ClientID       string      `json:"id"`
	Epoch          uint64      `json:"epoch"`
	Version        byte        `json:"version"`
	Username       string      `json:"username,omitempty"`
	ExpiryInterval uint32      `json:"expiry"`
	ExpiresAt      time.Time   `json:"expiresAt,omitempty"`
	Will           *WillRecord `json:"will,omitempty"`
}

// Entry is one queued or in-flight message of a session.
type Entry struct {
	Seq         uint64           `json:"seq"`
	Message     *message.Message `json:"message"`
	QoS         byte             `json:"qos"`
	Identifiers []uint32         `json:"ids,omitempty"`
	Retain      bool             `json:"retain,omitempty"`
	Group       string           `json:"group,omitempty"`
	PacketID    uint16           `json:"packetId,omitempty"`
	State       byte             `json:"state"`
}

// SessionState is everything persisted about one session.
type SessionState struct {
	SessionMeta
	Subscriptions []SubscriptionRecord `json:"subscriptions"`
	InboundQoS2   []uint16             `json:"inboundQoS2"`
	// InboundAccepted lists the open inbound identifiers whose publish was
	// accepted. An open identifier not listed may never have been.
	InboundAccepted []uint16 `json:"inboundAccepted,omitempty"`
	Entries         []Entry  `json:"entries"`
	// Delivered, on a state handed to a takeover only, names the messages
	// the session was sent recently, so the new owner does not give it one
	// of them again when a late forward of it arrives.
	Delivered []message.ID `json:"delivered,omitempty"`
}

// State is what recovery produces, what the manager loads and what a
// checkpoint stores.
type State struct {
	Sessions []*SessionState    `json:"sessions"`
	Retained []*message.Message `json:"retained"`
	// Through, in a checkpoint, lists the sessions whose state holds records
	// after the checkpoint's index: each holds every record of its session up
	// to Index and none after it. Recovery skips those records and clears it.
	Through []SessionIndex `json:"through,omitempty"`
	// At, set by a snapshot exact at one index for every key, is where the
	// checkpoint is written. Not persisted.
	At uint64 `json:"-"`

	index map[sessionKey]*SessionState // built on first lookup; nil after unmarshal
	// pos is each indexed session's place in Sessions, so a delete moves the
	// last session into the hole instead of scanning and shifting the slice.
	// Built with the index; nil after Normalize reorders the slice.
	pos map[sessionKey]int
	// rebuilds counts the times pos was found invalid and rebuilt by a scan.
	rebuilds int
}

// SessionIndex names a session and a log index.
type SessionIndex struct {
	Namespace string `json:"ns"`
	ClientID  string `json:"id"`
	Index     uint64 `json:"index"`
}

// heldRecords says which records after a checkpoint its state already holds.
type heldRecords struct {
	through map[sessionKey]uint64
	max     uint64
}

// heldBy takes a recovered checkpoint's Through list, clearing it from the
// state; nil when there is none.
func heldBy(st *State) *heldRecords {
	if len(st.Through) == 0 {
		st.Through = nil
		return nil
	}
	h := &heldRecords{through: make(map[sessionKey]uint64, len(st.Through))}
	for _, t := range st.Through {
		k := sessionKey{t.Namespace, t.ClientID}
		if t.Index > h.through[k] {
			h.through[k] = t.Index
		}
		if t.Index > h.max {
			h.max = t.Index
		}
	}
	st.Through = nil
	return h
}

// holds reports whether the checkpoint's state already holds the record. A
// retained record is never held: its apply sets the topic's value, and replay
// in log order ends at the log's value.
func (h *heldRecords) holds(index uint64, typ byte, payload []byte) bool {
	if h == nil || index > h.max || typ < recSessionUpsert || typ > recDropped {
		return false
	}
	var k sessionKey
	if json.Unmarshal(payload, &k) != nil {
		return false
	}
	t, ok := h.through[k]
	return ok && index <= t
}

// NewState returns an empty state whose slices marshal as arrays.
func NewState() *State {
	return &State{Sessions: make([]*SessionState, 0), Retained: make([]*message.Message, 0)}
}

type sessionKey struct {
	Namespace string `json:"ns"`
	ClientID  string `json:"id"`
}

type subscribeRecord struct {
	Namespace string             `json:"ns"`
	ClientID  string             `json:"id"`
	Sub       SubscriptionRecord `json:"sub"`
}

type unsubscribeRecord struct {
	Namespace string `json:"ns"`
	ClientID  string `json:"id"`
	Filter    string `json:"filter"`
}

type inboundRecord struct {
	Namespace string `json:"ns"`
	ClientID  string `json:"id"`
	PacketID  uint16 `json:"packetId"`
	// Accepted, on an inbound-add, marks the identifier's publish accepted. A
	// reader without the member applies it as a repeated opening.
	Accepted bool `json:"accepted,omitempty"`
}

type enqueueRecord struct {
	Namespace string `json:"ns"`
	ClientID  string `json:"id"`
	Entry     Entry  `json:"entry"`
}

type sentRecord struct {
	Namespace string `json:"ns"`
	ClientID  string `json:"id"`
	Seq       uint64 `json:"seq"`
	PacketID  uint16 `json:"packetId"`
}

type stateRecord struct {
	Namespace string `json:"ns"`
	ClientID  string `json:"id"`
	PacketID  uint16 `json:"packetId"`
	State     byte   `json:"state"`
}

type completeRecord struct {
	Namespace string `json:"ns"`
	ClientID  string `json:"id"`
	PacketID  uint16 `json:"packetId"`
}

type droppedRecord struct {
	Namespace string `json:"ns"`
	ClientID  string `json:"id"`
	Seq       uint64 `json:"seq"`
}

type retainKey struct {
	Namespace string `json:"ns"`
	Topic     string `json:"topic"`
}

// Find returns the session with the key, or nil.
func (st *State) Find(ns, id string) *SessionState { return st.find(ns, id) }

func (st *State) find(ns, id string) *SessionState {
	if st.index == nil {
		st.index = make(map[sessionKey]*SessionState, len(st.Sessions))
		for _, s := range st.Sessions {
			st.index[sessionKey{s.Namespace, s.ClientID}] = s
		}
		st.pos = positions(st.Sessions)
	}
	return st.index[sessionKey{ns, id}]
}

func positions(sessions []*SessionState) map[sessionKey]int {
	pos := make(map[sessionKey]int, len(sessions))
	for i, s := range sessions {
		pos[sessionKey{s.Namespace, s.ClientID}] = i
	}
	return pos
}

func (st *State) add(s *SessionState) {
	st.find("", "") // ensure the index exists
	st.Sessions = append(st.Sessions, s)
	k := sessionKey{s.Namespace, s.ClientID}
	st.index[k] = s
	if st.pos != nil {
		st.pos[k] = len(st.Sessions) - 1
	}
}

// Remove drops the session with the key, if present.
func (st *State) Remove(ns, id string) { st.remove(ns, id) }

func (st *State) remove(ns, id string) {
	s := st.find(ns, id)
	if s == nil {
		return
	}
	k := sessionKey{ns, id}
	i, ok := st.pos[k]
	if !ok || i >= len(st.Sessions) || st.Sessions[i] != s {
		// Invalidated by Normalize, or the slice was changed around the
		// index: a stale place must not delete another session.
		st.rebuilds++
		st.pos = positions(st.Sessions)
		if i, ok = st.pos[k]; !ok || st.Sessions[i] != s {
			delete(st.index, k)
			delete(st.pos, k)
			return
		}
	}
	last := len(st.Sessions) - 1
	if i != last {
		moved := st.Sessions[last]
		st.Sessions[i] = moved
		st.pos[sessionKey{moved.Namespace, moved.ClientID}] = i
	}
	st.Sessions[last] = nil
	st.Sessions = st.Sessions[:last]
	delete(st.index, k)
	delete(st.pos, k)
}

// Apply folds one record into the state. Records for a session that does
// not exist are ignored: the session was deleted after them.
func (st *State) Apply(typ byte, payload []byte) error {
	_, err := st.ApplyReport(typ, payload)
	return err
}

// Unapplied says why a well-formed record changed nothing when that is not
// what a record of its type does by design: the session it names is not held,
// or the session holds no entry it names. Deleting what is absent, adding an
// inbound identifier already present and the like are not reported.
type Unapplied string

const (
	UnappliedNoSession Unapplied = "no-session"
	UnappliedNoEntry   Unapplied = "no-entry"
)

// ApplyReport is Apply, and also reports a record that had nothing to apply
// to. The local log never produces one; on a replica it means the copy has
// diverged from its owner, or the record raced a whole-session replacement.
func (st *State) ApplyReport(typ byte, payload []byte) (Unapplied, error) {
	var un Unapplied
	switch typ {
	case recSessionUpsert:
		var meta SessionMeta
		if err := json.Unmarshal(payload, &meta); err != nil {
			return "", err
		}
		// The meta alone: an epoch above the held copy's is not a new
		// incarnation — a client reconnecting to its own owner bumps the
		// epoch without the session changing hands. A copy is replaced only
		// by the delete an import writes before its whole write.
		if s := st.find(meta.Namespace, meta.ClientID); s != nil {
			s.SessionMeta = meta
		} else {
			st.add(&SessionState{SessionMeta: meta, Subscriptions: make([]SubscriptionRecord, 0), InboundQoS2: make([]uint16, 0), Entries: make([]Entry, 0)})
		}
	case recSessionDelete:
		var k sessionKey
		if err := json.Unmarshal(payload, &k); err != nil {
			return "", err
		}
		st.remove(k.Namespace, k.ClientID)
	case recSubscribe:
		var r subscribeRecord
		if err := json.Unmarshal(payload, &r); err != nil {
			return "", err
		}
		if s := st.find(r.Namespace, r.ClientID); s != nil {
			replaced := false
			for i, sub := range s.Subscriptions {
				if sub.Filter == r.Sub.Filter {
					s.Subscriptions[i] = r.Sub
					replaced = true
				}
			}
			if !replaced {
				s.Subscriptions = append(s.Subscriptions, r.Sub)
			}
		} else {
			un = UnappliedNoSession
		}
	case recUnsubscribe:
		var r unsubscribeRecord
		if err := json.Unmarshal(payload, &r); err != nil {
			return "", err
		}
		if s := st.find(r.Namespace, r.ClientID); s != nil {
			for i, sub := range s.Subscriptions {
				if sub.Filter == r.Filter {
					s.Subscriptions = append(s.Subscriptions[:i], s.Subscriptions[i+1:]...)
					break
				}
			}
		}
	case recInboundAdd, recInboundRemove:
		var r inboundRecord
		if err := json.Unmarshal(payload, &r); err != nil {
			return "", err
		}
		s := st.find(r.Namespace, r.ClientID)
		if s == nil {
			if typ == recInboundAdd {
				un = UnappliedNoSession
			}
			return un, nil
		}
		if typ == recInboundRemove {
			s.InboundQoS2 = slices.DeleteFunc(s.InboundQoS2, func(id uint16) bool { return id == r.PacketID })
			s.InboundAccepted = slices.DeleteFunc(s.InboundAccepted, func(id uint16) bool { return id == r.PacketID })
			return un, nil
		}
		if !slices.Contains(s.InboundQoS2, r.PacketID) {
			s.InboundQoS2 = append(s.InboundQoS2, r.PacketID)
		}
		if r.Accepted && !slices.Contains(s.InboundAccepted, r.PacketID) {
			s.InboundAccepted = append(s.InboundAccepted, r.PacketID)
		}
	case recEnqueue:
		var r enqueueRecord
		if err := json.Unmarshal(payload, &r); err != nil {
			return "", err
		}
		if s := st.find(r.Namespace, r.ClientID); s != nil {
			s.Entries = append(s.Entries, r.Entry)
		} else {
			un = UnappliedNoSession
		}
	case recSent:
		var r sentRecord
		if err := json.Unmarshal(payload, &r); err != nil {
			return "", err
		}
		un = UnappliedNoSession
		if s := st.find(r.Namespace, r.ClientID); s != nil {
			un = UnappliedNoEntry
			for i := range s.Entries {
				if s.Entries[i].Seq == r.Seq {
					s.Entries[i].PacketID = r.PacketID
					s.Entries[i].State = StatePublishSent
					un = ""
				}
			}
		}
	case recInflightState:
		var r stateRecord
		if err := json.Unmarshal(payload, &r); err != nil {
			return "", err
		}
		un = UnappliedNoSession
		if s := st.find(r.Namespace, r.ClientID); s != nil {
			un = UnappliedNoEntry
			for i := range s.Entries {
				if s.Entries[i].PacketID == r.PacketID && s.Entries[i].State != StateQueued {
					s.Entries[i].State = r.State
					un = ""
				}
			}
		}
	case recComplete:
		var r completeRecord
		if err := json.Unmarshal(payload, &r); err != nil {
			return "", err
		}
		un = UnappliedNoSession
		if s := st.find(r.Namespace, r.ClientID); s != nil {
			un = UnappliedNoEntry
			for i := range s.Entries {
				if s.Entries[i].PacketID == r.PacketID && s.Entries[i].State != StateQueued {
					s.Entries = append(s.Entries[:i], s.Entries[i+1:]...)
					un = ""
					break
				}
			}
		}
	case recDropped:
		var r droppedRecord
		if err := json.Unmarshal(payload, &r); err != nil {
			return "", err
		}
		un = UnappliedNoSession
		if s := st.find(r.Namespace, r.ClientID); s != nil {
			un = UnappliedNoEntry
			for i := range s.Entries {
				if s.Entries[i].Seq == r.Seq && s.Entries[i].State == StateQueued {
					s.Entries = append(s.Entries[:i], s.Entries[i+1:]...)
					un = ""
					break
				}
			}
		}
	case recRetain:
		var m message.Message
		if err := json.Unmarshal(payload, &m); err != nil {
			return "", err
		}
		st.setRetained(&m)
	case recRetainDelete:
		var k retainKey
		if err := json.Unmarshal(payload, &k); err != nil {
			return "", err
		}
		st.setRetained(&message.Message{Namespace: k.Namespace, Topic: k.Topic})
	default:
		return "", fmt.Errorf("unknown record type %d", typ)
	}
	return un, nil
}

func (st *State) setRetained(m *message.Message) {
	for i, r := range st.Retained {
		if r.Namespace == m.Namespace && r.Topic == m.Topic {
			if len(m.Payload) == 0 {
				st.Retained = append(st.Retained[:i], st.Retained[i+1:]...)
			} else {
				st.Retained[i] = m
			}
			return
		}
	}
	if len(m.Payload) > 0 {
		st.Retained = append(st.Retained, m)
	}
}

// SetRetained stores a retained message or, for an empty payload, removes
// the topic's entry.
func (st *State) SetRetained(m *message.Message) { st.setRetained(m) }

// Normalize sorts for deterministic snapshots and comparisons.
func (st *State) Normalize() {
	st.pos = nil
	sort.Slice(st.Sessions, func(i, j int) bool {
		a, b := st.Sessions[i], st.Sessions[j]
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		return a.ClientID < b.ClientID
	})
	for _, s := range st.Sessions {
		sort.Slice(s.Subscriptions, func(i, j int) bool { return s.Subscriptions[i].Filter < s.Subscriptions[j].Filter })
		sort.Slice(s.InboundQoS2, func(i, j int) bool { return s.InboundQoS2[i] < s.InboundQoS2[j] })
	}
	sort.Slice(st.Retained, func(i, j int) bool {
		a, b := st.Retained[i], st.Retained[j]
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		return a.Topic < b.Topic
	})
}

// RecordsFor renders a session's state as the records that rebuild it: a
// metadata upsert, its subscriptions, inbound identifiers and entries. A
// follower applies them after a delete to replace its replica.
func RecordsFor(ss *SessionState) []Record {
	k := Key{Namespace: ss.Namespace, ClientID: ss.ClientID}
	out := make([]Record, 0, 1+len(ss.Subscriptions)+len(ss.InboundQoS2)+len(ss.Entries))
	add := func(typ byte, v any) {
		payload, _ := json.Marshal(v)
		out = append(out, Record{Type: typ, Key: k, Payload: payload})
	}
	add(recSessionUpsert, ss.SessionMeta)
	for _, sub := range ss.Subscriptions {
		add(recSubscribe, subscribeRecord{ss.Namespace, ss.ClientID, sub})
	}
	for _, id := range ss.InboundQoS2 {
		add(recInboundAdd, inboundRecord{Namespace: ss.Namespace, ClientID: ss.ClientID, PacketID: id})
		if slices.Contains(ss.InboundAccepted, id) {
			add(recInboundAdd, inboundRecord{Namespace: ss.Namespace, ClientID: ss.ClientID, PacketID: id, Accepted: true})
		}
	}
	for _, e := range ss.Entries {
		add(recEnqueue, enqueueRecord{ss.Namespace, ss.ClientID, e})
	}
	return out
}

// DeleteRecord is the record that removes a session.
func DeleteRecord(ns, id string) Record {
	payload, _ := json.Marshal(sessionKey{ns, id})
	return Record{Type: recSessionDelete, Key: Key{Namespace: ns, ClientID: id}, Payload: payload}
}

// RetainRecord is the record that stores a retained message or, for an
// empty payload, removes the topic's entry.
func RetainRecord(m *message.Message) Record {
	k := Key{Namespace: m.Namespace, Topic: m.Topic}
	if len(m.Payload) == 0 {
		payload, _ := json.Marshal(retainKey{m.Namespace, m.Topic})
		return Record{Type: recRetainDelete, Key: k, Payload: payload}
	}
	payload, _ := json.Marshal(m)
	return Record{Type: recRetain, Key: k, Payload: payload}
}

// RecordName is a record type's short name, for labels and messages.
func RecordName(typ byte) string {
	switch typ {
	case recSessionUpsert:
		return "session-upsert"
	case recSessionDelete:
		return "session-delete"
	case recSubscribe:
		return "subscribe"
	case recUnsubscribe:
		return "unsubscribe"
	case recInboundAdd:
		return "inbound-add"
	case recInboundRemove:
		return "inbound-remove"
	case recEnqueue:
		return "enqueue"
	case recSent:
		return "sent"
	case recInflightState:
		return "inflight-state"
	case recComplete:
		return "complete"
	case recDropped:
		return "dropped"
	case recRetain:
		return "retain"
	case recRetainDelete:
		return "retain-delete"
	}
	return "unknown"
}

// IsRetained reports whether a record type is a retained-store record.
func IsRetained(typ byte) bool { return typ == recRetain || typ == recRetainDelete }

// IsSessionDelete reports whether the record deletes a session: what an
// import writes before the session whole.
func IsSessionDelete(typ byte) bool { return typ == recSessionDelete }

// IsSessionUpsert reports whether a record writes a session's metadata: the
// record an owner writes when it creates, connects or imports the session.
func IsSessionUpsert(typ byte) bool { return typ == recSessionUpsert }

// IsRetainedDelete reports whether a record type removes a retained
// message, as against storing one.
func IsRetainedDelete(typ byte) bool { return typ == recRetainDelete }
