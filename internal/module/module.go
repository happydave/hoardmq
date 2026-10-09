// Package module is the extension registry: the seam through which every
// authentication, authorization, persistence and observability component
// reaches the broker, built-in or not. Modules register a factory under a
// namespaced identifier, declare the hook families they provide, receive
// typed configuration nested under their identifier, and run the provision,
// start, stop lifecycle. The API is internal until the public seam work item.
package module

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"time"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/cluster/records"
	"github.com/happydave/hoardmq/internal/hook"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/otel"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/session"
	"github.com/happydave/hoardmq/internal/store"
)

// ID is a module identifier of the form family.name.
type ID string

// Family is the part of an identifier before the dot.
func (id ID) Family() string {
	f, _, _ := strings.Cut(string(id), ".")
	return f
}

// Capability is a bit set of the hook families a module provides.
type Capability uint32

// Capabilities. A module declares the union of the families it implements;
// the registry verifies the declaration against the module's methods.
const (
	CapConnection Capability = 1 << iota
	CapAuthentication
	CapAuthorization
	CapInbound
	CapOutbound
	CapSubscribe
	CapDelivery
	CapRetained
	CapWill
	CapStore
	CapMetrics
	// CapCertificate is an authenticator that decides on the client
	// certificate (auth.CertificateAuthenticator); it runs before every
	// other authenticator and mechanism.
	CapCertificate
	// CapMechanism provides MQTT 5 enhanced-authentication methods
	// (auth.Mechanism).
	CapMechanism
	// CapTraces provides the tracer the publish path records spans with
	// (TraceProvider).
	CapTraces
)

// Has reports whether c includes every bit of want.
func (c Capability) Has(want Capability) bool { return c&want == want }

// Module is the lifecycle every module implements.
type Module interface {
	ID() ID
	Capabilities() Capability
	// Provision parses and validates the module's configuration. It may read
	// files the configuration names; it must not create, open for writing
	// or start anything, because hoardmq check runs it.
	Provision(ctx *Context, raw json.RawMessage) error
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

// Toggleable modules can be enabled and disabled while the broker runs; a
// disabled module is skipped by every hook family. Store providers are never
// toggleable.
type Toggleable interface {
	SetEnabled(bool)
}

// Reloader modules re-read the files their configuration names when the
// broker's configuration is reloaded. On an error the module keeps what it
// had.
type Reloader interface {
	Reload() error
}

// Factory creates a module instance.
type Factory func() Module

// Services is what modules may ask of the broker.
type Services interface {
	// Publish injects a message as if published by the node itself.
	Publish(m *message.Message)
	// Disconnect closes a client's connection with a reason.
	Disconnect(namespace, clientID string, reason packet.ReasonCode) bool
	// Sessions lists session records.
	Sessions() []session.Record
	// Retained returns the retained messages a filter matches.
	Retained(namespace, filter string) []*message.Message
	// Ready reports whether the broker is serving; the cluster refines it.
	Ready() bool
	// Serving reports whether the node is ready and can acknowledge a QoS 1
	// or 2 publish, and when not, why: "not-ready: " and the readiness
	// reason, or a degraded state such as "routes-unknown".
	Serving() (bool, string)
	// Drain starts the node's graceful leave (idempotent) and returns at
	// once; DrainStatus reports its progress.
	Drain()
	DrainStatus() DrainStatus
	// Records is the replicated records store, nil until the node's
	// consensus core is open.
	Records() RecordStore
}

// RecordStore is the replicated records store: named collections of keyed
// JSON records that every node applies from the consensus log. Reads are
// this node's applied state; a write returns once it has committed and
// applied here.
type RecordStore interface {
	Get(collection, key string) (records.Record, bool)
	List(collection, after string, limit int) (items []records.Entry, more bool)
	Put(ctx context.Context, collection, key string, value json.RawMessage, ifVersion *uint64, by string, at time.Time) (records.Result, error)
	Delete(ctx context.Context, collection, key string, ifVersion *uint64, by string, at time.Time) (records.Result, error)
}

// DrainStatus is a drain's progress as the endpoint reports it.
type DrainStatus struct {
	Draining         bool   `json:"draining"`
	Phase            string `json:"phase,omitempty"`
	ClientsRemaining int    `json:"clientsRemaining"`
	LeasesRemaining  int    `json:"leasesRemaining"`
	// The sessions the drain handed off and still waits to see confirmed held
	// by their receiver; those confirmed; and those its confirmation timeout
	// ended with unconfirmed.
	SessionsAwaiting    int `json:"sessionsAwaiting"`
	SessionsConfirmed   int `json:"sessionsConfirmed"`
	SessionsUnconfirmed int `json:"sessionsUnconfirmed"`
	// CopiesKept is the replica copies this node keeps because it stopped
	// being their session's target when it left the shard map, until each
	// owner confirms the session held at its new targets.
	CopiesKept int `json:"copiesKept,omitempty"`
	// RetainedShardsKept is the retained shards whose copies this node keeps
	// because it stopped holding them, until each shard's leader releases them.
	RetainedShardsKept int  `json:"retainedShardsKept,omitempty"`
	Complete           bool `json:"complete"`
}

// Observability is the listener configuration the metrics module reads.
// AdminOwnsDrain is set when the admin API is configured: the drain is then
// served there, behind its authentication, and not on this listener.
type Observability struct {
	Address        string
	MetricsPath    string
	AdminOwnsDrain bool
}

// Collector is implemented by a metrics provider that lets the broker set
// at-scrape gauges: each registered function is called before a snapshot.
type Collector interface {
	AddCollector(func(set func(name string, labels map[string]string, value float64)))
}

// Context is what a module receives at provision.
type Context struct {
	Logger    *slog.Logger
	Node      string
	Namespace string
	DataDir   string
	// Store options for the store family, from the node configuration.
	Store store.Options
	// Observability is the listener the metrics module serves, if any.
	Observability Observability
	// Services is nil during hoardmq check.
	Services Services
	// PasswordCache is the node's cache of password verifications, which
	// every password authenticator verifies through; nil during hoardmq
	// check.
	PasswordCache *auth.VerifyCache
}

// Types shared with the core live in the hook package; aliases keep a
// module's imports to one package.
type (
	ConnectionInfo = hook.Info
	Outcome        = hook.Outcome
	Action         = hook.Action
	DeliveryEvent  = hook.DeliveryEvent
	HookPolicy     = hook.Policy
)

// Actions and events, re-exported.
const (
	Continue   = hook.Continue
	Modify     = hook.Modify
	Drop       = hook.Drop
	Disconnect = hook.Disconnect

	Delivered    = hook.Delivered
	Acknowledged = hook.Acknowledged
	Queued       = hook.Queued
	Dropped      = hook.Dropped
	Expired      = hook.Expired
)

// OK is the outcome that changes nothing.
var OK = hook.OK

// ConnectionHook observes connection lifecycle.
type ConnectionHook interface {
	Connected(ctx context.Context, info ConnectionInfo)
	Disconnected(ctx context.Context, info ConnectionInfo, reason packet.ReasonCode, graceful bool)
}

// InboundInterceptor sees every decoded packet after CONNECT and may change
// it in place.
type InboundInterceptor interface {
	InterceptInbound(ctx context.Context, info ConnectionInfo, p packet.Packet) Outcome
}

// OutboundInterceptor sees every packet before it is written.
type OutboundInterceptor interface {
	InterceptOutbound(ctx context.Context, info ConnectionInfo, p packet.Packet) Outcome
}

// SubscribeInterceptor sees each filter of a SUBSCRIBE after validation and
// before authorization; it may change the subscription in place or refuse
// it with a reason code (Drop with Reason set).
type SubscribeInterceptor interface {
	InterceptSubscribe(ctx context.Context, info ConnectionInfo, sub *packet.Subscription) Outcome
}

// DeliveryListener observes delivery events.
type DeliveryListener interface {
	OnDelivery(ctx context.Context, event DeliveryEvent, namespace, clientID string, m *message.Message, qos byte)
}

// RetainedListener observes retained store changes; an empty payload is a
// removal.
type RetainedListener interface {
	OnRetained(ctx context.Context, m *message.Message)
}

// WillListener observes Will publication.
type WillListener interface {
	OnWill(ctx context.Context, namespace, clientID string, m *message.Message)
}

// StoreProvider is the persistence seam; the log-backed store is the
// built-in implementation.
type StoreProvider interface {
	Journal() session.Journal
	Recover() (*store.State, error)
	// Checkpoint writes the state the snapshot returns; the provider reads
	// its own position before calling it (store.Snapshot).
	Checkpoint(store.Snapshot) error
	NeedsCheckpoint() bool
	Close() error
}

// MetricsUser is a module that counts: the broker hands it the metrics
// provider once the registry is built.
type MetricsUser interface {
	UseMetrics(MetricsProvider)
}

// MetricsProvider counts and reads back; the exporter work item adds the
// Prometheus surface over it.
type MetricsProvider interface {
	Add(name string, labels map[string]string, delta float64)
	Set(name string, labels map[string]string, value float64)
	Snapshot() []Metric
}

// TraceProvider is a module that records and exports spans.
type TraceProvider interface {
	Tracer() *otel.Tracer
}

// Metric is one named series with its labels and value.
type Metric struct {
	Name   string
	Labels map[string]string
	Value  float64
}

// Authenticator and Authorizer alias the auth package interfaces so a module
// file needs one import.
type (
	Authenticator = auth.Authenticator
	Authorizer    = auth.Authorizer
)
