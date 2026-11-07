package module

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

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

	connection  []*entry
	inbound     []*entry
	outbound    []*entry
	subscribe   []*entry
	unsubscribe []*entry
	delivery    []*entry
	retained    []*entry
	will        []*entry
	accepted    []*entry
	admission   []*entry

	// Timeouts and panics are counted per module for the operator.
	failures map[ID]int

	// cluster holds one queue per ClusterListener. clusterMu orders an
	// enqueue against the stop, so an event is either queued before the
	// stop counts what is left or counted dropped by the enqueue itself.
	cluster       []*clusterQueue
	clusterMu     sync.Mutex
	clusterOn     bool
	clusterOff    bool
	clusterStop   chan struct{}
	clusterWorker sync.WaitGroup
}

// clusterQueueSize bounds each listener's queue of cluster events.
const clusterQueueSize = 1024

type clusterQueue struct {
	e       *entry
	ch      chan ClusterEvent
	dropped atomic.Int64
}

func newHooks(r *Registry) *Hooks {
	h := &Hooks{policy: r.policy, failures: map[ID]int{}, clusterStop: make(chan struct{})}
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
		if _, ok := e.m.(UnsubscribeInterceptor); ok {
			h.unsubscribe = append(h.unsubscribe, e)
		}
		if _, ok := e.m.(ClusterListener); ok {
			h.cluster = append(h.cluster, &clusterQueue{e: e, ch: make(chan ClusterEvent, clusterQueueSize)})
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

// Inbound implements hook.Interceptor: the first Drop or Disconnect ends
// the chain, and is returned with the module that gave it.
func (h *Hooks) Inbound(ctx context.Context, info hook.Info, p packet.Packet) (hook.Outcome, string) {
	result, by := hook.OK, ""
	for _, e := range h.enabled(h.inbound) {
		m := e.m.(InboundInterceptor)
		out := h.call(ctx, e, func(c context.Context) hook.Outcome { return m.InterceptInbound(c, info, p) })
		switch out.Action {
		case hook.Modify:
			result, by = out, string(e.id)
		case hook.Drop, hook.Disconnect:
			return out, string(e.id)
		}
	}
	return result, by
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

// Unsubscribe implements hook.Interceptor.
func (h *Hooks) Unsubscribe(ctx context.Context, info hook.Info, filter string) hook.Outcome {
	for _, e := range h.enabled(h.unsubscribe) {
		m := e.m.(UnsubscribeInterceptor)
		out := h.call(ctx, e, func(c context.Context) hook.Outcome { return m.InterceptUnsubscribe(c, info, filter) })
		switch out.Action {
		case hook.Drop, hook.Disconnect:
			return out
		}
	}
	return hook.OK
}

// Clustering reports whether any module listens for cluster events, so the
// broker can skip watching for them.
func (h *Hooks) Clustering() bool { return len(h.cluster) > 0 }

// Cluster queues ev for every enabled ClusterListener without waiting: the
// callers are the consensus apply path, the drain and the membership poll,
// none of which may block on a module. A full queue, or a stopped one, drops
// the event and counts it for that module.
func (h *Hooks) Cluster(ev ClusterEvent) {
	if len(h.cluster) == 0 {
		return
	}
	h.clusterMu.Lock()
	defer h.clusterMu.Unlock()
	for _, q := range h.cluster {
		if !q.e.enabled.Load() {
			continue
		}
		if h.clusterOff {
			q.dropped.Add(1)
			continue
		}
		select {
		case q.ch <- ev:
		default:
			q.dropped.Add(1)
		}
	}
}

// ClusterDropped reports the cluster events dropped per listening module.
func (h *Hooks) ClusterDropped() map[ID]int64 {
	out := make(map[ID]int64, len(h.cluster))
	for _, q := range h.cluster {
		out[q.e.id] = q.dropped.Load()
	}
	return out
}

// startCluster starts one worker per listener, which calls the module under
// the hook policy. It runs once; after stopCluster it does nothing.
func (h *Hooks) startCluster() {
	h.clusterMu.Lock()
	defer h.clusterMu.Unlock()
	if h.clusterOn || h.clusterOff {
		return
	}
	h.clusterOn = true
	for _, q := range h.cluster {
		h.clusterWorker.Add(1)
		go h.clusterWork(q)
	}
}

func (h *Hooks) clusterWork(q *clusterQueue) {
	defer h.clusterWorker.Done()
	l := q.e.m.(ClusterListener)
	for {
		select {
		case <-h.clusterStop:
			return
		case ev := <-q.ch:
			// An event taken as the stop arrives belongs to the stop.
			select {
			case <-h.clusterStop:
				q.dropped.Add(1)
				return
			default:
			}
			if !q.e.enabled.Load() {
				continue
			}
			h.call(context.Background(), q.e, func(c context.Context) hook.Outcome { l.OnClusterEvent(c, ev); return hook.OK })
		}
	}
}

// stopCluster stops the workers and counts every event still queued as
// dropped. It waits for a call in progress for the hook timeout and a second
// more; a listener still in its call after that is left to return on its
// own, and is given nothing further.
func (h *Hooks) stopCluster() {
	h.clusterMu.Lock()
	if h.clusterOff {
		h.clusterMu.Unlock()
		return
	}
	h.clusterOff = true
	close(h.clusterStop)
	h.clusterMu.Unlock()
	done := make(chan struct{})
	go func() {
		h.clusterWorker.Wait()
		close(done)
	}()
	grace := time.NewTimer(h.policy.Timeout + time.Second)
	defer grace.Stop()
	select {
	case <-done:
	case <-grace.C:
	}
	for _, q := range h.cluster {
		q.dropQueued()
	}
}

func (q *clusterQueue) dropQueued() {
	for {
		select {
		case <-q.ch:
			q.dropped.Add(1)
		default:
			return
		}
	}
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
	return len(h.connection)+len(h.inbound)+len(h.outbound)+len(h.subscribe)+len(h.unsubscribe)+len(h.delivery)+len(h.retained)+len(h.will)+
		len(h.accepted)+len(h.admission) == 0
}

var _ hook.Interceptor = (*Hooks)(nil)
var _ hook.Observer = (*Hooks)(nil)
