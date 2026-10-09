package store

import (
	"encoding/json"

	extstore "github.com/happydave/hoardmq/ext/store"
	"github.com/happydave/hoardmq/internal/message"
)

// The record format and the state it rebuilds are the extension API's
// (ext/store); the engine here writes, keeps and replays them.
type (
	SubscriptionRecord = extstore.SubscriptionRecord
	WillRecord         = extstore.WillRecord
	SessionMeta        = extstore.SessionMeta
	Entry              = extstore.Entry
	SessionState       = extstore.SessionState
	State              = extstore.State
	SessionIndex       = extstore.SessionIndex
	Unapplied          = extstore.Unapplied
)

// Entry states and the reasons a record changed nothing.
const (
	StateQueued         = extstore.StateQueued
	StatePublishSent    = extstore.StatePublishSent
	StatePubrecReceived = extstore.StatePubrecReceived

	UnappliedNoSession = extstore.UnappliedNoSession
	UnappliedNoEntry   = extstore.UnappliedNoEntry
)

// Record types, by the engine's names.
const (
	recSessionUpsert = extstore.RecordSessionUpsert
	recSessionDelete = extstore.RecordSessionDelete
	recSubscribe     = extstore.RecordSubscribe
	recUnsubscribe   = extstore.RecordUnsubscribe
	recInboundAdd    = extstore.RecordInboundAdd
	recInboundRemove = extstore.RecordInboundRemove
	recEnqueue       = extstore.RecordEnqueue
	recSent          = extstore.RecordSent
	recInflightState = extstore.RecordInflightState
	recComplete      = extstore.RecordComplete
	recDropped       = extstore.RecordDropped
	recRetain        = extstore.RecordRetain
	recRetainDelete  = extstore.RecordRetainDelete
)

// NewState returns an empty state whose slices marshal as arrays.
func NewState() *State { return extstore.NewState() }

// RecordsFor renders a session's state as the records that rebuild it.
func RecordsFor(ss *SessionState) []Record { return extstore.RecordsFor(ss) }

// DeleteRecord is the record that removes a session.
func DeleteRecord(ns, id string) Record { return extstore.DeleteRecord(ns, id) }

// RetainRecord is the record that stores a retained message or, for an
// empty payload, removes the topic's entry.
func RetainRecord(m *message.Message) Record { return extstore.RetainRecord(m) }

// RecordName is a record type's short name, for labels and messages.
func RecordName(typ byte) string { return extstore.RecordName(typ) }

// IsRetained reports whether a record type is a retained-store record.
func IsRetained(typ byte) bool { return extstore.IsRetained(typ) }

// IsSessionDelete reports whether the record deletes a session.
func IsSessionDelete(typ byte) bool { return extstore.IsSessionDelete(typ) }

// IsSessionUpsert reports whether a record writes a session's metadata.
func IsSessionUpsert(typ byte) bool { return extstore.IsSessionUpsert(typ) }

// IsRetainedDelete reports whether a record type removes a retained message.
func IsRetainedDelete(typ byte) bool { return extstore.IsRetainedDelete(typ) }

// sessionKey is the namespace and client identifier every session record's
// payload begins with.
type sessionKey struct {
	Namespace string `json:"ns"`
	ClientID  string `json:"id"`
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
