package ext

import (
	"context"
	"net"
	"time"

	"github.com/happydave/hoardmq/ext/mqtt"
	"github.com/happydave/hoardmq/ext/store"
	"github.com/happydave/hoardmq/ext/trace"
)

// ConnectionInfo describes a client connection to hooks.
type ConnectionInfo struct {
	Namespace string
	ClientID  string
	Username  string
	CertCN    string
	Version   mqtt.Version
	Listener  string
	PeerAddr  string
}

// Action names what happens to an intercepted packet.
type Action byte

// Actions.
const (
	Continue Action = iota
	Modify
	Drop
	Disconnect
)

func (a Action) String() string {
	switch a {
	case Modify:
		return "modify"
	case Drop:
		return "drop"
	case Disconnect:
		return "disconnect"
	}
	return "continue"
}

// Outcome is an interceptor's verdict. Reason accompanies Disconnect, and
// Drop from a subscribe interceptor (the SUBACK reason code). The broker
// answers a Drop of an inbound QoS 1 or 2 PUBLISH with 0x80 at MQTT 5, so the
// client does not send it again; below MQTT 5, which has no reason code
// there, it closes the connection.
type Outcome struct {
	Action Action
	Reason mqtt.ReasonCode
}

// OK is the outcome that changes nothing.
var OK = Outcome{Action: Continue}

// HookPolicy is the timeout and fallback the broker applies to every hook
// call: a hook that has not returned within Timeout, or that panics, counts
// as Fallback (Continue or Disconnect). A zero timeout calls hooks inline
// with no deadline.
type HookPolicy struct {
	Timeout  time.Duration
	Fallback Action
}

// DeliveryEvent names what happened to a message for one session.
type DeliveryEvent byte

// Delivery events.
const (
	Delivered DeliveryEvent = iota
	Acknowledged
	Queued
	Dropped
	Expired
)

func (e DeliveryEvent) String() string {
	switch e {
	case Delivered:
		return "delivered"
	case Acknowledged:
		return "acknowledged"
	case Queued:
		return "queued"
	case Dropped:
		return "dropped"
	}
	return "expired"
}

// The hook families. Within a family the broker calls modules in the module
// order (the configuration's hooks.order, then identifier order). An
// interceptor chain stops at the first Drop or Disconnect; a Modify is kept
// and the chain goes on.

// ConnectionHook observes connection lifecycle.
type ConnectionHook interface {
	Connected(ctx context.Context, info ConnectionInfo)
	Disconnected(ctx context.Context, info ConnectionInfo, reason mqtt.ReasonCode, graceful bool)
}

// InboundInterceptor sees every decoded packet after CONNECT and may change
// it in place.
type InboundInterceptor interface {
	InterceptInbound(ctx context.Context, info ConnectionInfo, p mqtt.Packet) Outcome
}

// OutboundInterceptor sees every packet before it is written.
type OutboundInterceptor interface {
	InterceptOutbound(ctx context.Context, info ConnectionInfo, p mqtt.Packet) Outcome
}

// SubscribeInterceptor sees each filter of a SUBSCRIBE after validation and
// before authorization; it may change the subscription in place or refuse
// it with a reason code (Drop with Reason set).
type SubscribeInterceptor interface {
	InterceptSubscribe(ctx context.Context, info ConnectionInfo, sub *mqtt.Subscription) Outcome
}

// DeliveryListener observes delivery events.
type DeliveryListener interface {
	OnDelivery(ctx context.Context, event DeliveryEvent, namespace, clientID string, m *mqtt.Message, qos byte)
}

// RetainedListener observes retained store changes; an empty payload is a
// removal.
type RetainedListener interface {
	OnRetained(ctx context.Context, m *mqtt.Message)
}

// WillListener observes Will publication.
type WillListener interface {
	OnWill(ctx context.Context, namespace, clientID string, m *mqtt.Message)
}

// PublishSource names where an accepted publish came from.
type PublishSource byte

// Publish sources.
const (
	// SourceClient is a PUBLISH from a client connected to this node.
	SourceClient PublishSource = iota
	// SourceWill is a Will the node published for a session.
	SourceWill
	// SourceModule is a message a module published through Services.Publish.
	SourceModule
)

func (s PublishSource) String() string {
	switch s {
	case SourceWill:
		return "will"
	case SourceModule:
		return "module"
	}
	return "client"
}

// PublishAcceptedListener is told of each publish this node accepts, once,
// on the node that accepted it, after acceptance is decided and before any
// acknowledgement is written:
//   - QoS 0: after the message is dispatched;
//   - QoS 1: after the journal sync and the replication barrier, before the
//     PUBACK;
//   - QoS 2: after the same and after the packet identifier is marked
//     accepted, before the PUBREC; a retransmission of that identifier and
//     its PUBREL are not reported again;
//   - a retained publish at the step of its QoS, its Retain flag set; an
//     empty payload removes the retained message.
//
// A refused publish is never reported. Acknowledged with No Matching
// Subscribers is accepted. A Will is reported where it is published, and a
// module's publish once it is dispatched. A copy received from another node,
// a shared-group give, a retained hand-over and a takeover's import are not
// reported.
//
// The message is read-only; the broker changes nothing in it after
// acceptance, so a module may keep it. The call is made on the publishing
// goroutine under the hook policy and cannot refuse: blocking delays that
// publisher's acknowledgement and the reading of its next packet, and nothing
// else.
type PublishAcceptedListener interface {
	OnPublishAccepted(ctx context.Context, source PublishSource, m *mqtt.Message)
}

// PublishAdmission may refuse a client's PUBLISH before it is taken in. It is
// called once per PUBLISH, after authorization, topic alias resolution and
// the QoS 2 retransmission check, after a QoS 2 packet identifier is opened,
// and before any delivery; never for a retransmission of an accepted QoS 2
// publish, a Will or a module's publish.
//
// Continue and Modify admit; the message must not be changed. Drop refuses
// with Reason, or Unspecified Error (0x80) when Reason is not an error code;
// Disconnect refuses the same way and also closes the connection with that
// code. A refusal takes the path of any refused publish: an error PUBACK or
// PUBREC at MQTT 5 for QoS 1 and 2, and the connection closed for QoS 0 and
// below MQTT 5. A module that admits keeps nothing, since a later module may
// still refuse.
type PublishAdmission interface {
	AdmitPublish(ctx context.Context, info ConnectionInfo, m *mqtt.Message) Outcome
}

// StoreProvider is the persistence family (experimental, as ext/store is):
// the journal the broker writes every change through, recovery at start, and
// checkpoints. One store provider runs per node; configuring one replaces the
// built-in store.wal.
type StoreProvider interface {
	Journal() store.Journal
	Recover() (*store.State, error)
	// Checkpoint writes the state the snapshot returns; the provider reads
	// its own position before calling it.
	Checkpoint(store.Snapshot) error
	NeedsCheckpoint() bool
	Close() error
	// SetTap installs the replication layer's tap. The provider calls it
	// with every record it appends, in append order and before the append
	// returns, and runs what it returns, when not nil, once its own locks are
	// released, on the appending goroutine. The broker replicates a session
	// only through this tap, and refuses a store provider without it.
	SetTap(tap func(store.Record) func())
}

// AppendStatsProvider is a store provider that reports what its appends
// held its lock for.
type AppendStatsProvider interface {
	AppendStats() store.AppendStats
}

// SyncStatsProvider is a store provider that reports its group commits.
type SyncStatsProvider interface {
	SyncStats() store.SyncStats
}

// JournalStatsProvider is a store provider that reports its journal's
// position, checkpoints and fsyncs, for the node's metrics.
type JournalStatsProvider interface {
	JournalStats() store.JournalStats
}

// ListenerProvider is a module that serves a listener of its own and reports
// the address it bound.
type ListenerProvider interface {
	Addr() net.Addr
}

// TraceProvider is the trace family (experimental, as ext/trace is): the
// tracer the publish path records spans with.
type TraceProvider interface {
	Tracer() *trace.Tracer
}

// MetricsProvider counts and reads back; the metrics modules implement it.
type MetricsProvider interface {
	Add(name string, labels map[string]string, delta float64)
	Set(name string, labels map[string]string, value float64)
	Snapshot() []Metric
}

// Metric is one named series with its labels and value.
type Metric struct {
	Name   string
	Labels map[string]string
	Value  float64
}

// MetricsUser is a module that counts: the broker hands it the node's
// metrics provider once every module is provisioned.
type MetricsUser interface {
	UseMetrics(MetricsProvider)
}
