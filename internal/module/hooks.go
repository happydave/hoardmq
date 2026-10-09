package module

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/happydave/hoardmq/internal/hook"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/packet"
)

// Hooks dispatches each family to the enabled modules that provide it. It
// implements hook.Interceptor for the connection layer and hook.Observer for
// the session manager. Per-family slices are computed once; a toggle takes
// the mutex, so a disabled module vanishes from every family at once.
type Hooks struct {
	mu     sync.RWMutex
	policy hook.Policy
	log    *slog.Logger

	connection []*entry
	inbound    []*entry
	outbound   []*entry
	subscribe  []*entry
	delivery   []*entry
	retained   []*entry
	will       []*entry
	accepted   []*entry
	admission  []*entry

	// Timeouts and panics are counted per module for the operator.
	failures map[ID]int
}

func newHooks(r *Registry) *Hooks {
	h := &Hooks{policy: r.policy, failures: map[ID]int{}}
	if r.ctx != nil && r.ctx.Logger != nil {
		h.log = r.ctx.Logger.With("component", "hooks")
	} else {
		h.log = slog.Default()
	}
	for _, e := range r.modules {
		if _, ok := e.m.(ConnectionHook); ok {
			h.connection = append(h.connection, e)
		}
		if _, ok := e.m.(InboundInterceptor); ok {
			h.inbound = append(h.inbound, e)
		}
		if _, ok := e.m.(OutboundInterceptor); ok {
			h.outbound = append(h.outbound, e)
		}
		if _, ok := e.m.(SubscribeInterceptor); ok {
			h.subscribe = append(h.subscribe, e)
		}
		if _, ok := e.m.(DeliveryListener); ok {
			h.delivery = append(h.delivery, e)
		}
		if _, ok := e.m.(RetainedListener); ok {
			h.retained = append(h.retained, e)
		}
		if _, ok := e.m.(WillListener); ok {
			h.will = append(h.will, e)
		}
		if _, ok := e.m.(PublishAccepted); ok {
			h.accepted = append(h.accepted, e)
		}
		if _, ok := e.m.(PublishAdmission); ok {
			h.admission = append(h.admission, e)
		}
	}
	return h
}

// Failures reports how many timeouts and panics each module has caused.
func (h *Hooks) Failures() map[ID]int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make(map[ID]int, len(h.failures))
	for k, v := range h.failures {
		out[k] = v
	}
	return out
}

func (h *Hooks) fail(e *entry, what string) {
	h.mu.Lock()
	h.failures[e.id]++
	h.mu.Unlock()
	h.log.Warn("hook "+what, "module", string(e.id), "fallback", h.policy.Fallback.String())
}

// call runs one hook under the policy: inline when no timeout is set,
// otherwise in a goroutine with a deadline. A panic or a timeout yields the
// fallback outcome.
func (h *Hooks) call(ctx context.Context, e *entry, f func(context.Context) hook.Outcome) hook.Outcome {
	fallback := hook.Outcome{Action: h.policy.Fallback, Reason: packet.ImplementationSpecificError}
	if h.policy.Timeout <= 0 {
		out, panicked := h.guard(ctx, f)
		if panicked {
			h.fail(e, "panicked")
			return fallback
		}
		return out
	}
	cctx, cancel := context.WithTimeout(ctx, h.policy.Timeout)
	defer cancel()
	type result struct {
		out      hook.Outcome
		panicked bool
	}
	done := make(chan result, 1)
	go func() {
		out, panicked := h.guard(cctx, f)
		done <- result{out, panicked}
	}()
	select {
	case r := <-done:
		if r.panicked {
			h.fail(e, "panicked")
			return fallback
		}
		return r.out
	case <-cctx.Done():
		h.fail(e, "timed out")
		return fallback
	}
}

func (h *Hooks) guard(ctx context.Context, f func(context.Context) hook.Outcome) (out hook.Outcome, panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			panicked = true
			h.log.Error("hook panic", "panic", fmt.Sprint(r))
		}
	}()
	return f(ctx), false
}

func (h *Hooks) enabled(list []*entry) []*entry {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]*entry, 0, len(list))
	for _, e := range list {
		if e.enabled.Load() {
			out = append(out, e)
		}
	}
	return out
}

// Connected implements hook.Interceptor.
func (h *Hooks) Connected(ctx context.Context, info hook.Info) {
	for _, e := range h.enabled(h.connection) {
		m := e.m.(ConnectionHook)
		h.call(ctx, e, func(c context.Context) hook.Outcome { m.Connected(c, info); return hook.OK })
	}
}

// Disconnected implements hook.Interceptor.
func (h *Hooks) Disconnected(ctx context.Context, info hook.Info, reason packet.ReasonCode, graceful bool) {
	for _, e := range h.enabled(h.connection) {
		m := e.m.(ConnectionHook)
		h.call(ctx, e, func(c context.Context) hook.Outcome { m.Disconnected(c, info, reason, graceful); return hook.OK })
	}
}

// Inbound implements hook.Interceptor: the first outcome that is not
// Continue ends the chain.
func (h *Hooks) Inbound(ctx context.Context, info hook.Info, p packet.Packet) hook.Outcome {
	result := hook.OK
	for _, e := range h.enabled(h.inbound) {
		m := e.m.(InboundInterceptor)
		out := h.call(ctx, e, func(c context.Context) hook.Outcome { return m.InterceptInbound(c, info, p) })
		switch out.Action {
		case hook.Modify:
			result = out
		case hook.Drop, hook.Disconnect:
			return out
		}
	}
	return result
}

// Outbound implements hook.Interceptor.
func (h *Hooks) Outbound(ctx context.Context, info hook.Info, p packet.Packet) hook.Outcome {
	result := hook.OK
	for _, e := range h.enabled(h.outbound) {
		m := e.m.(OutboundInterceptor)
		out := h.call(ctx, e, func(c context.Context) hook.Outcome { return m.InterceptOutbound(c, info, p) })
		switch out.Action {
		case hook.Modify:
			result = out
		case hook.Drop, hook.Disconnect:
			return out
		}
	}
	return result
}

// Subscribe implements hook.Interceptor.
func (h *Hooks) Subscribe(ctx context.Context, info hook.Info, sub *packet.Subscription) hook.Outcome {
	result := hook.OK
	for _, e := range h.enabled(h.subscribe) {
		m := e.m.(SubscribeInterceptor)
		out := h.call(ctx, e, func(c context.Context) hook.Outcome { return m.InterceptSubscribe(c, info, sub) })
		switch out.Action {
		case hook.Modify:
			result = out
		case hook.Drop, hook.Disconnect:
			return out
		}
	}
	return result
}

// OnDelivery implements hook.Observer.
func (h *Hooks) OnDelivery(ctx context.Context, event hook.DeliveryEvent, ns, id string, m *message.Message, qos byte) {
	for _, e := range h.enabled(h.delivery) {
		l := e.m.(DeliveryListener)
		h.call(ctx, e, func(c context.Context) hook.Outcome { l.OnDelivery(c, event, ns, id, m, qos); return hook.OK })
	}
}

// OnRetained implements hook.Observer.
func (h *Hooks) OnRetained(ctx context.Context, m *message.Message) {
	for _, e := range h.enabled(h.retained) {
		l := e.m.(RetainedListener)
		h.call(ctx, e, func(c context.Context) hook.Outcome { l.OnRetained(c, m); return hook.OK })
	}
}

// OnWill implements hook.Observer.
func (h *Hooks) OnWill(ctx context.Context, ns, id string, m *message.Message) {
	for _, e := range h.enabled(h.will) {
		l := e.m.(WillListener)
		h.call(ctx, e, func(c context.Context) hook.Outcome { l.OnWill(c, ns, id, m); return hook.OK })
	}
}

// OnPublishAccepted implements hook.Observer.
func (h *Hooks) OnPublishAccepted(ctx context.Context, source hook.PublishSource, m *message.Message) {
	if len(h.accepted) == 0 {
		return
	}
	for _, e := range h.enabled(h.accepted) {
		l := e.m.(PublishAccepted)
		h.call(ctx, e, func(c context.Context) hook.Outcome { l.OnPublishAccepted(c, source, m); return hook.OK })
	}
}

// AdmitPublish implements hook.Observer: the first outcome that is Drop or
// Disconnect refuses the publish and ends the chain.
func (h *Hooks) AdmitPublish(ctx context.Context, info hook.Info, m *message.Message) hook.Outcome {
	if len(h.admission) == 0 {
		return hook.OK
	}
	for _, e := range h.enabled(h.admission) {
		a := e.m.(PublishAdmission)
		out := h.call(ctx, e, func(c context.Context) hook.Outcome { return a.AdmitPublish(c, info, m) })
		switch out.Action {
		case hook.Drop, hook.Disconnect:
			return out
		}
	}
	return hook.OK
}

// Admitting implements hook.Observer.
func (h *Hooks) Admitting() bool { return len(h.admission) > 0 }

// Empty reports whether no module provides interceptors or listeners, so the
// core can skip the calls entirely.
func (h *Hooks) Empty() bool {
	return len(h.connection)+len(h.inbound)+len(h.outbound)+len(h.subscribe)+len(h.delivery)+len(h.retained)+len(h.will)+
		len(h.accepted)+len(h.admission) == 0
}

var _ hook.Interceptor = (*Hooks)(nil)
var _ hook.Observer = (*Hooks)(nil)
