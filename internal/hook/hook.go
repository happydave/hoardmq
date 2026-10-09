// Package hook holds the types the broker core and the module registry
// share: connection facts, interceptor outcomes, delivery events, and the
// interfaces the core calls. It is a leaf package so the connection and
// session layers can call hooks without importing the registry.
package hook

import (
	"context"

	"github.com/happydave/hoardmq/ext"

	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/packet"
)

// The connection facts, outcomes and policy are the extension API's.
type (
	Info    = ext.ConnectionInfo
	Action  = ext.Action
	Outcome = ext.Outcome
	Policy  = ext.HookPolicy
)

// Actions.
const (
	Continue   = ext.Continue
	Modify     = ext.Modify
	Drop       = ext.Drop
	Disconnect = ext.Disconnect
)

// OK is the outcome that changes nothing.
var OK = ext.OK

// Interceptor is what the connection layer calls. A nil Interceptor means no
// modules; callers check for nil.
type Interceptor interface {
	Connected(ctx context.Context, info Info)
	Disconnected(ctx context.Context, info Info, reason packet.ReasonCode, graceful bool)
	Inbound(ctx context.Context, info Info, p packet.Packet) Outcome
	Outbound(ctx context.Context, info Info, p packet.Packet) Outcome
	Subscribe(ctx context.Context, info Info, sub *packet.Subscription) Outcome
}

// DeliveryEvent is the extension API's delivery event.
type DeliveryEvent = ext.DeliveryEvent

// Delivery events.
const (
	Delivered    = ext.Delivered
	Acknowledged = ext.Acknowledged
	Queued       = ext.Queued
	Dropped      = ext.Dropped
	Expired      = ext.Expired
)

// Observer is what the session manager calls. A nil Observer means no
// modules.
type Observer interface {
	OnDelivery(ctx context.Context, event DeliveryEvent, namespace, clientID string, m *message.Message, qos byte)
	// OnRetained reports a retained store change; an empty payload is a
	// removal.
	OnRetained(ctx context.Context, m *message.Message)
	OnWill(ctx context.Context, namespace, clientID string, m *message.Message)
	// OnPublishAccepted reports a publish this node accepted, once.
	OnPublishAccepted(ctx context.Context, source PublishSource, m *message.Message)
	// AdmitPublish asks whether a client's publish may be taken in; an
	// outcome other than Continue or Modify refuses it.
	AdmitPublish(ctx context.Context, info Info, m *message.Message) Outcome
	// Admitting reports whether any module checks publish admission, so a
	// publish builds no connection facts for a chain that is empty.
	Admitting() bool
}

// PublishSource is the extension API's publish source.
type PublishSource = ext.PublishSource

// Publish sources.
const (
	SourceClient = ext.SourceClient
	SourceWill   = ext.SourceWill
	SourceModule = ext.SourceModule
)

// Auditor records security and administrative events; the audit log
// implements it. A nil Auditor means auditing is off.
type Auditor interface {
	Audit(actor, action, target, outcome string, detail map[string]string)
}

// Metrics is the counting surface the core uses; the metrics module
// implements it and an exporter reads it back.
type Metrics interface {
	Add(name string, labels map[string]string, delta float64)
	Set(name string, labels map[string]string, value float64)
}
