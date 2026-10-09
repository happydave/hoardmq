// Package module is the broker's side of the extension registry: it
// instantiates the configured modules from the factories registered with the
// extension API (ext), provisions, validates, orders, starts and stops them,
// and dispatches the hook families to them. Every module reaches the broker
// through the extension API's contract, built-in or not.
package module

import (
	"github.com/happydave/hoardmq/ext"
	"github.com/happydave/hoardmq/internal/auth"
)

// The contract is the extension API's; the core names it here.
type (
	ID                   = ext.ID
	Capability           = ext.Capability
	Module               = ext.Module
	Validator            = ext.Validator
	Toggleable           = ext.Toggleable
	Reloader             = ext.Reloader
	Factory              = ext.Factory
	Services             = ext.Services
	RecordStore          = ext.RecordStore
	DrainStatus          = ext.DrainStatus
	Observability        = ext.Observability
	Collector            = ext.Collector
	Context              = ext.Context
	PasswordVerifier     = ext.PasswordVerifier
	ConnectionInfo       = ext.ConnectionInfo
	Outcome              = ext.Outcome
	Action               = ext.Action
	DeliveryEvent        = ext.DeliveryEvent
	HookPolicy           = ext.HookPolicy
	ConnectionHook       = ext.ConnectionHook
	InboundInterceptor   = ext.InboundInterceptor
	OutboundInterceptor  = ext.OutboundInterceptor
	SubscribeInterceptor = ext.SubscribeInterceptor
	DeliveryListener     = ext.DeliveryListener
	RetainedListener     = ext.RetainedListener
	WillListener         = ext.WillListener
	PublishAccepted      = ext.PublishAcceptedListener
	PublishAdmission     = ext.PublishAdmission
	PublishSource        = ext.PublishSource
	StoreProvider        = ext.StoreProvider
	MetricsUser          = ext.MetricsUser
	MetricsProvider      = ext.MetricsProvider
	Metric               = ext.Metric
	TraceProvider        = ext.TraceProvider
	Authenticator        = auth.Authenticator
	Authorizer           = auth.Authorizer
)

// Capabilities, actions and events, by the core's names.
const (
	CapConnection       = ext.CapConnection
	CapAuthentication   = ext.CapAuthentication
	CapAuthorization    = ext.CapAuthorization
	CapInbound          = ext.CapInbound
	CapOutbound         = ext.CapOutbound
	CapSubscribe        = ext.CapSubscribe
	CapDelivery         = ext.CapDelivery
	CapRetained         = ext.CapRetained
	CapWill             = ext.CapWill
	CapStore            = ext.CapStore
	CapMetrics          = ext.CapMetrics
	CapCertificate      = ext.CapCertificate
	CapMechanism        = ext.CapMechanism
	CapTraces           = ext.CapTraces
	CapPublishAccepted  = ext.CapPublishAccepted
	CapPublishAdmission = ext.CapPublishAdmission

	Continue   = ext.Continue
	Modify     = ext.Modify
	Drop       = ext.Drop
	Disconnect = ext.Disconnect

	Delivered    = ext.Delivered
	Acknowledged = ext.Acknowledged
	Queued       = ext.Queued
	Dropped      = ext.Dropped
	Expired      = ext.Expired
)

// OK is the outcome that changes nothing.
var OK = ext.OK

// Register makes a module available under its identifier (ext.Register).
func Register(id ID, f Factory) { ext.Register(id, f) }

// Known lists the registered identifiers in order (ext.Known).
func Known() []ID { return ext.Known() }
