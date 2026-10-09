// Package ext is HoardMQ's extension API: the contract every module meets,
// built-in or not. A module registers a factory under a namespaced identifier
// from its package's init, declares the hook families it provides, receives
// its configuration from the broker's modules.<id> object, and runs the
// lifecycle: provision, validate, start, stop, with hot enable and disable
// where its family allows.
//
// The API is versioned on its own (APIVersion) and imports only the standard
// library. ext/CHANGELOG.md states what each version may change; ext/mqtt,
// ext/auth and ext/records hold the types the hooks carry, and ext/store and
// ext/trace the experimental store and trace provider families.
package ext

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"sync"
)

// APIVersion is the extension API's semantic version, independent of the
// broker's.
const APIVersion = "0.2.0"

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
// the broker checks the declaration against the module's methods and refuses
// a module whose declaration and methods differ.
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
	// CapPublishAccepted is told of each publish the node accepts
	// (PublishAcceptedListener).
	CapPublishAccepted
	// CapPublishAdmission may refuse a client's publish before any delivery
	// (PublishAdmission).
	CapPublishAdmission
)

// Has reports whether c includes every bit of want.
func (c Capability) Has(want Capability) bool { return c&want == want }

// Module is the lifecycle every module implements.
type Module interface {
	ID() ID
	Capabilities() Capability
	// Provision parses the module's configuration. It may read files the
	// configuration names; it must not create, open for writing or start
	// anything, because hoardmq check runs it.
	Provision(ctx *Context, raw json.RawMessage) error
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

// Validator modules check their provisioned configuration as a whole once
// Provision has succeeded, at hoardmq check and at start alike; an error is
// reported against the module like a provision error. Like Provision,
// Validate writes nothing.
type Validator interface {
	Validate() error
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

var (
	factoriesMu sync.RWMutex
	factories   = map[ID]Factory{}
)

// Register makes a module available under its identifier. A module's package
// calls it from init; a duplicate identifier panics, because two modules
// claiming one name is a build error, not a configuration one.
func Register(id ID, f Factory) {
	factoriesMu.Lock()
	defer factoriesMu.Unlock()
	if _, dup := factories[id]; dup {
		panic("module: duplicate registration of " + string(id))
	}
	factories[id] = f
}

// Lookup returns the factory registered under id.
func Lookup(id ID) (Factory, bool) {
	factoriesMu.RLock()
	defer factoriesMu.RUnlock()
	f, ok := factories[id]
	return f, ok
}

// Known lists the registered identifiers in order.
func Known() []ID {
	factoriesMu.RLock()
	defer factoriesMu.RUnlock()
	out := make([]ID, 0, len(factories))
	for id := range factories {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
