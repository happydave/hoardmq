package module

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/jsonschema"
	"github.com/happydave/hoardmq/internal/otel"
)

var (
	factoriesMu sync.RWMutex
	factories   = map[ID]Factory{}
)

// Register makes a module available under its identifier. Built-ins call
// it from init; a duplicate identifier panics because it is a programming
// error, not a configuration one.
func Register(id ID, f Factory) {
	factoriesMu.Lock()
	defer factoriesMu.Unlock()
	if _, dup := factories[id]; dup {
		panic("module: duplicate registration of " + string(id))
	}
	factories[id] = f
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

// Defaults are the modules instantiated when their family is not configured.
var Defaults = []ID{"store.wal", "metrics.internal"}

// Registry holds the instantiated modules of one broker.
type Registry struct {
	ctx     *Context
	policy  HookPolicy
	modules []*entry
	byID    map[ID]*entry
	hooks   *Hooks
}

type entry struct {
	id ID
	m  Module
	// enabled is stored under the hooks lock, beside the module's own
	// SetEnabled, and loaded without it by the chains' wrappers on every
	// authentication and authorization.
	enabled atomic.Bool
	toggle  Toggleable
}

// Problem aliases the configuration problem type so check can merge lists.
type Problem = jsonschema.Problem

// New instantiates and provisions the configured modules plus the defaults
// for absent families, in identifier order. Every provisioning error is
// returned as a problem at /modules/<id>; the registry is usable only when
// the list is empty.
func New(cfg config.Document, ctx *Context, policy HookPolicy) (*Registry, []Problem) {
	r := &Registry{ctx: ctx, policy: policy, byID: map[ID]*entry{}}
	var problems []Problem
	ids := make([]ID, 0, len(cfg.Modules))
	for k := range cfg.Modules {
		ids = append(ids, ID(k))
	}
	families := map[string]bool{}
	for _, id := range ids {
		families[id.Family()] = true
	}
	for _, d := range Defaults {
		if !families[d.Family()] {
			ids = append(ids, d)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	storeCount := 0
	for _, id := range ids {
		pointer := "/modules/" + string(id)
		factoriesMu.RLock()
		f, ok := factories[id]
		factoriesMu.RUnlock()
		if !ok {
			problems = append(problems, Problem{Pointer: pointer, Message: "unknown module"})
			continue
		}
		m := f()
		if m.ID() != id {
			problems = append(problems, Problem{Pointer: pointer, Message: fmt.Sprintf("factory produced module %q", m.ID())})
			continue
		}
		if msg := checkCapabilities(m); msg != "" {
			problems = append(problems, Problem{Pointer: pointer, Message: msg})
			continue
		}
		if m.Capabilities().Has(CapStore) {
			storeCount++
			if _, toggleable := m.(Toggleable); toggleable {
				problems = append(problems, Problem{Pointer: pointer, Message: "a store provider must not be toggleable"})
				continue
			}
		}
		raw := cfg.Modules[string(id)]
		if raw == nil {
			raw = json.RawMessage("{}")
		}
		mctx := *ctx
		if ctx.Logger != nil {
			mctx.Logger = ctx.Logger.With("module", string(id))
		}
		if err := m.Provision(&mctx, raw); err != nil {
			problems = append(problems, Problem{Pointer: pointer, Message: err.Error()})
			continue
		}
		e := &entry{id: id, m: m}
		e.enabled.Store(true)
		e.toggle, _ = m.(Toggleable)
		r.modules = append(r.modules, e)
		r.byID[id] = e
	}
	if storeCount > 1 {
		problems = append(problems, Problem{Pointer: "/modules", Message: "more than one store provider configured"})
	}
	if len(problems) > 0 {
		return nil, problems
	}
	r.hooks = newHooks(r)
	return r, nil
}

// checkCapabilities verifies that a module implements the interface of every
// capability it declares and declares every interface it implements.
func checkCapabilities(m Module) string {
	caps := m.Capabilities()
	checks := []struct {
		c    Capability
		ok   bool
		name string
	}{
		{CapConnection, is[ConnectionHook](m), "connection"},
		{CapAuthentication, is[auth.Authenticator](m), "authentication"},
		{CapAuthorization, is[auth.Authorizer](m), "authorization"},
		{CapInbound, is[InboundInterceptor](m), "inbound"},
		{CapOutbound, is[OutboundInterceptor](m), "outbound"},
		{CapSubscribe, is[SubscribeInterceptor](m), "subscribe"},
		{CapDelivery, is[DeliveryListener](m), "delivery"},
		{CapRetained, is[RetainedListener](m), "retained"},
		{CapWill, is[WillListener](m), "will"},
		{CapStore, is[StoreProvider](m), "store"},
		{CapMetrics, is[MetricsProvider](m), "metrics"},
		{CapCertificate, is[auth.CertificateAuthenticator](m), "certificate"},
		{CapMechanism, is[auth.Mechanism](m), "mechanism"},
		{CapTraces, is[TraceProvider](m), "traces"},
	}
	for _, ch := range checks {
		declared := caps.Has(ch.c)
		if declared && !ch.ok {
			return "declares the " + ch.name + " capability without implementing it"
		}
		if !declared && ch.ok {
			return "implements the " + ch.name + " hooks without declaring the capability"
		}
	}
	return ""
}

func is[T any](m Module) bool {
	_, ok := m.(T)
	return ok
}

// Start starts every module in order; on failure the started ones are
// stopped in reverse and the error returned.
func (r *Registry) Start(ctx context.Context) error {
	for i, e := range r.modules {
		if err := e.m.Start(ctx); err != nil {
			for j := i - 1; j >= 0; j-- {
				_ = r.modules[j].m.Stop(ctx)
			}
			return fmt.Errorf("module %s: %w", e.id, err)
		}
	}
	return nil
}

// Stop stops every module in reverse order and returns the first error.
func (r *Registry) Stop(ctx context.Context) error {
	var first error
	for i := len(r.modules) - 1; i >= 0; i-- {
		if err := r.modules[i].m.Stop(ctx); err != nil && first == nil {
			first = fmt.Errorf("module %s: %w", r.modules[i].id, err)
		}
	}
	return first
}

// SetEnabled toggles a module. It returns false when the module is unknown
// or not toggleable. The change applies to every hook family at once.
func (r *Registry) SetEnabled(id ID, enabled bool) bool {
	e, ok := r.byID[id]
	if !ok || e.toggle == nil {
		return false
	}
	r.hooks.mu.Lock()
	e.enabled.Store(enabled)
	e.toggle.SetEnabled(enabled)
	r.hooks.mu.Unlock()
	return true
}

// Info describes one instantiated module.
type Info struct {
	ID         ID
	Enabled    bool
	Toggleable bool
}

// Reload asks every module that re-reads its files to do so, in order, and
// returns a problem for each that failed and kept what it had.
func (r *Registry) Reload() []Problem {
	var problems []Problem
	for _, e := range r.modules {
		if m, ok := e.m.(Reloader); ok {
			if err := m.Reload(); err != nil {
				problems = append(problems, Problem{Pointer: "/modules/" + string(e.id), Message: err.Error()})
			}
		}
	}
	return problems
}

// Describe lists the instantiated modules in order with their state.
func (r *Registry) Describe() []Info {
	r.hooks.mu.RLock()
	defer r.hooks.mu.RUnlock()
	out := make([]Info, 0, len(r.modules))
	for _, e := range r.modules {
		out = append(out, Info{ID: e.id, Enabled: e.enabled.Load(), Toggleable: e.toggle != nil})
	}
	return out
}

// Modules lists the instantiated identifiers in order.
func (r *Registry) Modules() []ID {
	out := make([]ID, 0, len(r.modules))
	for _, e := range r.modules {
		out = append(out, e.id)
	}
	return out
}

// Hooks returns the dispatcher the core calls.
func (r *Registry) Hooks() *Hooks { return r.hooks }

// Authenticators returns the authentication chain in module order, without
// the certificate authenticators.
func (r *Registry) Authenticators() auth.AuthenticatorChain {
	return r.chain(false)
}

// CertificateAuthenticators returns the certificate authenticators in module
// order: the connection runs them before the chain and before any
// mechanism, so a certificate one refuses is refused whatever else the
// client presents.
func (r *Registry) CertificateAuthenticators() auth.AuthenticatorChain {
	return r.chain(true)
}

func (r *Registry) chain(certificates bool) auth.AuthenticatorChain {
	var chain auth.AuthenticatorChain
	for _, e := range r.modules {
		if a, ok := e.m.(auth.Authenticator); ok && isCertificate(e.m) == certificates {
			chain = append(chain, gated{e: e, a: a})
		}
	}
	return chain
}

// isCertificate is the chain's partition: a module that declares it decides
// on certificates.
func isCertificate(m Module) bool {
	return m.Capabilities().Has(CapCertificate)
}

// Mechanism returns the enabled module providing an enhanced-authentication
// method.
func (r *Registry) Mechanism(method string) (auth.Mechanism, bool) {
	r.hooks.mu.RLock()
	defer r.hooks.mu.RUnlock()
	for _, e := range r.modules {
		mech, ok := e.m.(auth.Mechanism)
		if !ok || !e.enabled.Load() {
			continue
		}
		for _, name := range mech.Methods() {
			if name == method {
				return mech, true
			}
		}
	}
	return nil, false
}

// ShareMetrics hands the metrics provider to every module that counts.
func (r *Registry) ShareMetrics(m MetricsProvider) {
	for _, e := range r.modules {
		if u, ok := e.m.(MetricsUser); ok {
			u.UseMetrics(m)
		}
	}
}

// Authorizers returns the authorization chain in module order.
func (r *Registry) Authorizers() auth.AuthorizerChain {
	var chain auth.AuthorizerChain
	for _, e := range r.modules {
		if a, ok := e.m.(auth.Authorizer); ok {
			chain = append(chain, gatedZ{e: e, a: a})
		}
	}
	return chain
}

// Store returns the store provider, or nil when none is configured.
func (r *Registry) Store() StoreProvider {
	for _, e := range r.modules {
		if s, ok := e.m.(StoreProvider); ok {
			return s
		}
	}
	return nil
}

// Metrics returns the metrics provider, or nil.
func (r *Registry) Metrics() MetricsProvider {
	for _, e := range r.modules {
		if m, ok := e.m.(MetricsProvider); ok {
			return m
		}
	}
	return nil
}

// Tracer returns the tracer of the trace provider, or nil: tracing off.
func (r *Registry) Tracer() *otel.Tracer {
	for _, e := range r.modules {
		if p, ok := e.m.(TraceProvider); ok {
			return p.Tracer()
		}
	}
	return nil
}

// gated wraps an authenticator so a disabled module passes.
type gated struct {
	e *entry
	a auth.Authenticator
}

func (g gated) Authenticate(ctx context.Context, req *auth.Request) auth.Result {
	if !g.e.enabled.Load() {
		return auth.Result{Decision: auth.Pass}
	}
	return g.a.Authenticate(ctx, req)
}

type gatedZ struct {
	e *entry
	a auth.Authorizer
}

func (g gatedZ) Authorize(ctx context.Context, action auth.Action, topic string, id *auth.Identity) auth.Decision {
	if !g.e.enabled.Load() {
		return auth.Pass
	}
	return g.a.Authorize(ctx, action, topic, id)
}
