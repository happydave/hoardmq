// Package hook holds the types the broker core and the module registry
// share: connection facts, interceptor outcomes, delivery events, and the
// interfaces the core calls. It is a leaf package so the connection and
// session layers can call hooks without importing the registry.
package hook

import (
	"context"
	"time"

	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/packet"
)

// Info describes a client connection to hooks.
type Info struct {
	Namespace string
	ClientID  string
	Username  string
	CertCN    string
	Version   packet.Version
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
// Drop from a subscribe interceptor (the SUBACK reason code).
type Outcome struct {
	Action Action
	Reason packet.ReasonCode
}

// OK is the outcome that changes nothing.
var OK = Outcome{Action: Continue}

// Policy is the timeout and fallback applied to every hook call. A zero
// timeout calls hooks inline with no deadline.
type Policy struct {
	Timeout  time.Duration
	Fallback Action // Continue or Disconnect
}

// Interceptor is what the connection layer calls. A nil Interceptor means no
// modules; callers check for nil.
type Interceptor interface {
	Connected(ctx context.Context, info Info)
	Disconnected(ctx context.Context, info Info, reason packet.ReasonCode, graceful bool)
	Inbound(ctx context.Context, info Info, p packet.Packet) Outcome
	Outbound(ctx context.Context, info Info, p packet.Packet) Outcome
	Subscribe(ctx context.Context, info Info, sub *packet.Subscription) Outcome
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

// Observer is what the session manager calls. A nil Observer means no
// modules.
type Observer interface {
	OnDelivery(ctx context.Context, event DeliveryEvent, namespace, clientID string, m *message.Message, qos byte)
	// OnRetained reports a retained store change; an empty payload is a
	// removal.
	OnRetained(ctx context.Context, m *message.Message)
	OnWill(ctx context.Context, namespace, clientID string, m *message.Message)
}

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
