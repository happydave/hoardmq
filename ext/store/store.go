package store

import (
	"encoding/json"
	"os"
	"time"

	"github.com/happydave/hoardmq/ext/mqtt"
)

// Journal is what the broker records every change to persistent state
// through. It calls Sync before acknowledging a QoS 1 or 2 publish, a
// SUBSCRIBE, an UNSUBSCRIBE or a CONNECT, so a provider's Sync returns only
// once every record appended before it is durable.
type Journal interface {
	SessionUpsert(meta SessionMeta) error
	SessionDelete(k Key) error
	Subscribe(k Key, sub SubscriptionRecord) error
	Unsubscribe(k Key, filter string) error
	InboundQoS2(k Key, packetID uint16, add bool) error
	InboundAccepted(k Key, packetID uint16) error
	Enqueue(k Key, e Entry) error
	Sent(k Key, entrySeq uint64, packetID uint16) error
	InflightState(k Key, packetID uint16, state byte) error
	Complete(k Key, packetID uint16) error
	Dropped(k Key, entrySeq uint64) error
	Retain(m *mqtt.Message) error
	RetainDelete(ns, topic string) error
	Sync() error
}

// Key names what a record is about: a session (namespace and client
// identifier) or a retained topic (namespace and topic).
type Key struct {
	Namespace string
	ClientID  string
	Topic     string
	// Epoch is the lease generation the writing node held for this session
	// when it wrote the record. The replication tap stamps it on the frame
	// rather than looking it up: a node journals for a session only while it
	// owns that session's lease, so this is the epoch the record was actually
	// written under — where a lookup at fan-out time can read a newer epoch
	// belonging to a different owner. Zero for retained-message records,
	// which are keyed by topic and replicated by shard.
	Epoch uint64
}

// Scope names what a record belongs to, for callers that must say which
// records a durability wait covers: a session by namespace and client id, or
// a retained message by namespace and topic, and, when the caller knows it,
// the session's epoch. A peer's refusal of a record is remembered against
// the identity, with the epoch it was refused at: a later epoch writes the
// session whole again, so a barrier that names the later epoch is not held
// by the earlier refusal, while one that names no epoch (0) is held by any.
type Scope struct {
	Namespace string
	ClientID  string
	Topic     string
	Epoch     uint64
}

// Bare is the scope without its epoch: the identity a refusal is keyed by.
func (s Scope) Bare() Scope {
	return Scope{Namespace: s.Namespace, ClientID: s.ClientID, Topic: s.Topic}
}

// Scope keeps whichever identity the record has, and the epoch. A session
// record keeps its client id even when it also carries a topic; only a
// record with no client id is scoped by topic.
func (k Key) Scope() Scope {
	if k.ClientID != "" {
		return Scope{Namespace: k.Namespace, ClientID: k.ClientID, Epoch: k.Epoch}
	}
	return Scope{Namespace: k.Namespace, Topic: k.Topic}
}

// Zero reports whether the scope names nothing.
func (s Scope) Zero() bool { return s.ClientID == "" && s.Topic == "" }

// Record is one appended record as the tap sees it.
type Record struct {
	Type    byte
	Key     Key
	Payload []byte
}

// Snapshot returns the live state a checkpoint writes. Checkpoint calls it
// after reading the checkpoint's index, with a lock-free reader of the index
// of the last record appended. A record journaled between the two is in the
// state and after the index, so recovery would apply it twice: a snapshot that
// reads each session while it cannot be journaled lists the reader's value for
// it in State.Through, and one exact at a single index for everything sets
// State.At.
type Snapshot func(appended func() uint64) *State

// Options are the node's journal settings, which a store provider reads from
// its module context.
type Options struct {
	// SyncPolicy is "commit" (fsync before every acknowledgement that waits
	// on the journal) or "relaxed" (fsync on SyncInterval).
	SyncPolicy   string
	SyncInterval time.Duration
	SegmentBytes int64
	// GroupMaxDelay is how long a commit-policy sync may wait for concurrent
	// callers to join its fsync; GroupMaxSize fires a waiting group early.
	GroupMaxDelay time.Duration
	GroupMaxSize  int
	// Sync and IgnoreChecksum are the simulation harness's: a replacement
	// for fsync, and an injected defect that reads a record whose checksum
	// fails. A provider that has neither ignores them.
	Sync           func(*os.File) error
	IgnoreChecksum bool
}

// JournalStats is the journal's position, checkpoints and fsyncs, for the
// metrics.
type JournalStats struct {
	LastIndex       uint64
	CheckpointIndex uint64
	CheckpointAt    time.Time
	Fsyncs          int64
	FsyncSec        float64
	Sync            SyncStats
}

// SyncStats is the group-commit picture, for the harness and the metrics.
type SyncStats struct {
	Syncs          int64   `json:"syncs"`
	Ops            int64   `json:"ops_covered"`
	Callers        int64   `json:"callers"`
	WaitSec        float64 `json:"wait_sec_total"`
	HeldGroups     int64   `json:"held_groups"`
	Failures       int64   `json:"failures"`
	Repairs        int64   `json:"repairs"`
	RepairFailures int64   `json:"repair_failures"`
	// Checkpoints written from a snapshot, the time they took, and the part
	// of it under the store lock, which an append waits for.
	Checkpoints       int64   `json:"checkpoints"`
	CheckpointSec     float64 `json:"checkpoint_sec_total"`
	CheckpointHoldSec float64 `json:"checkpoint_hold_sec_total"`
}

// AppendStats is what the store's lock was held for by appends, readable
// without that lock. Held covers the log write and the tap; the log write is
// Held less Tap.
type AppendStats struct {
	Appends   int64   `json:"appends"`
	HeldSec   float64 `json:"held_sec_total"`
	TapSec    float64 `json:"tap_sec_total"`
	HeldMaxMs float64 `json:"held_max_ms"`
}

// The Encode functions return the record a journal call writes: its type,
// the key it is about and its payload. A provider appends the payload under
// the type and hands the record to the replication tap.

func encode(typ byte, k Key, v any) (Record, error) {
	payload, err := json.Marshal(v)
	if err != nil {
		return Record{}, err
	}
	return Record{Type: typ, Key: k, Payload: payload}, nil
}

// EncodeSessionUpsert is SessionUpsert's record.
func EncodeSessionUpsert(meta SessionMeta) (Record, error) {
	return encode(RecordSessionUpsert, Key{Namespace: meta.Namespace, ClientID: meta.ClientID, Epoch: meta.Epoch}, meta)
}

// EncodeSessionDelete is SessionDelete's record.
func EncodeSessionDelete(k Key) (Record, error) {
	return encode(RecordSessionDelete, k, sessionKey{k.Namespace, k.ClientID})
}

// EncodeSubscribe is Subscribe's record.
func EncodeSubscribe(k Key, sub SubscriptionRecord) (Record, error) {
	return encode(RecordSubscribe, k, subscribeRecord{k.Namespace, k.ClientID, sub})
}

// EncodeUnsubscribe is Unsubscribe's record.
func EncodeUnsubscribe(k Key, filter string) (Record, error) {
	return encode(RecordUnsubscribe, k, unsubscribeRecord{k.Namespace, k.ClientID, filter})
}

// EncodeInboundQoS2 is InboundQoS2's record.
func EncodeInboundQoS2(k Key, packetID uint16, add bool) (Record, error) {
	typ := RecordInboundRemove
	if add {
		typ = RecordInboundAdd
	}
	return encode(typ, k, inboundRecord{Namespace: k.Namespace, ClientID: k.ClientID, PacketID: packetID})
}

// EncodeInboundAccepted is InboundAccepted's record.
func EncodeInboundAccepted(k Key, packetID uint16) (Record, error) {
	return encode(RecordInboundAdd, k, inboundRecord{Namespace: k.Namespace, ClientID: k.ClientID, PacketID: packetID, Accepted: true})
}

// EncodeEnqueue is Enqueue's record.
func EncodeEnqueue(k Key, e Entry) (Record, error) {
	return encode(RecordEnqueue, k, enqueueRecord{k.Namespace, k.ClientID, e})
}

// EncodeSent is Sent's record.
func EncodeSent(k Key, entrySeq uint64, packetID uint16) (Record, error) {
	return encode(RecordSent, k, sentRecord{k.Namespace, k.ClientID, entrySeq, packetID})
}

// EncodeInflightState is InflightState's record.
func EncodeInflightState(k Key, packetID uint16, state byte) (Record, error) {
	return encode(RecordInflightState, k, stateRecord{k.Namespace, k.ClientID, packetID, state})
}

// EncodeComplete is Complete's record.
func EncodeComplete(k Key, packetID uint16) (Record, error) {
	return encode(RecordComplete, k, completeRecord{k.Namespace, k.ClientID, packetID})
}

// EncodeDropped is Dropped's record.
func EncodeDropped(k Key, entrySeq uint64) (Record, error) {
	return encode(RecordDropped, k, droppedRecord{k.Namespace, k.ClientID, entrySeq})
}

// EncodeRetain is Retain's record.
func EncodeRetain(m *mqtt.Message) (Record, error) {
	return encode(RecordRetain, Key{Namespace: m.Namespace, Topic: m.Topic}, m)
}

// EncodeRetainDelete is RetainDelete's record.
func EncodeRetainDelete(ns, topic string) (Record, error) {
	return encode(RecordRetainDelete, Key{Namespace: ns, Topic: topic}, retainKey{ns, topic})
}

// Known reports whether typ is a record type of this format.
func Known(typ byte) bool { return typ >= RecordSessionUpsert && typ <= RecordRetainDelete }
