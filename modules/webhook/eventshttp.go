package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/happydave/hoardmq/ext"
	"github.com/happydave/hoardmq/ext/mqtt"
)

// EventsID is events.http's identifier.
const EventsID ext.ID = "events.http"

func init() {
	ext.Register(EventsID, func() ext.Module { return &eventsHTTP{readFile: readFileOS, now: time.Now} })
}

const (
	metricEventsSent     = "hoardmq_events_http_sent_total"
	metricEventsDropped  = "hoardmq_events_http_dropped_total"
	metricEventsBuffered = "hoardmq_events_http_buffered"

	// maxBackoff caps the doubling wait between attempts.
	maxBackoff = 5 * time.Second
)

// Drop reasons, the dropped counter's reason label.
const (
	dropBufferFull = "buffer_full"
	dropFailed     = "failed"
	dropStopped    = "stopped"
)

// eventNames are the families events.http can post.
var eventNames = []string{
	"client.connected", "client.disconnected", "session.subscribe", "session.unsubscribe", "message.publish",
	"message.delivered", "message.acked", "message.queued", "message.dropped", "message.expired",
	"retained.changed", "will.published",
	"cluster.member.joined", "cluster.member.left", "cluster.lease.moved", "cluster.drain.started",
}

var deliveryNames = map[ext.DeliveryEvent]string{
	ext.Delivered: "message.delivered", ext.Acknowledged: "message.acked", ext.Queued: "message.queued",
	ext.Dropped: "message.dropped", ext.Expired: "message.expired",
}

type eventsConfig struct {
	endpointConfig
	Events []string `json:"events"`
	Mode   string   `json:"mode"`
	Buffer struct {
		MaxEvents *int `json:"maxEvents"`
	} `json:"buffer"`
	Batch struct {
		MaxEvents  *int `json:"maxEvents"`
		MaxDelayMs *int `json:"maxDelayMs"`
	} `json:"batch"`
	Retry struct {
		MaxAttempts *int `json:"maxAttempts"`
		BackoffMs   *int `json:"backoffMs"`
	} `json:"retry"`
	Payload string `json:"payload"`
}

type eventsHTTP struct {
	cfg      eventsConfig
	ep       *endpoint
	node, ns string
	log      *slog.Logger
	readFile func(string) ([]byte, error)
	now      func() time.Time

	enabled             map[string]bool
	sync, omitPayload   bool
	batchMax, attempts  int
	batchDelay, backoff time.Duration
	bufferMax           int
	metrics             atomic.Pointer[metricsBox]
	drops               dropLog

	// mu orders an enqueue against Stop: no event enters the buffer once
	// running is false.
	mu      sync.RWMutex
	running bool
	ch      chan json.RawMessage
	quit    chan struct{}
	done    chan struct{}
	// sendCtx ends what the worker is sending when Stop's own context ends.
	sendCtx    context.Context
	cancelSend context.CancelFunc
}

func (*eventsHTTP) ID() ext.ID { return EventsID }

func (*eventsHTTP) Capabilities() ext.Capability {
	return ext.CapConnection | ext.CapSubscribe | ext.CapUnsubscribe | ext.CapDelivery | ext.CapRetained | ext.CapWill |
		ext.CapPublishAccepted | ext.CapCluster
}

func (e *eventsHTTP) Provision(ctx *ext.Context, raw json.RawMessage) error {
	if err := decodeStrict(raw, &e.cfg); err != nil {
		return err
	}
	e.node, e.ns, e.log = ctx.Node, ctx.Namespace, ctx.Logger
	if e.log == nil {
		e.log = slog.Default()
	}
	e.drops.log = e.log
	return nil
}

// Validate checks the configuration and reads the files it names.
func (e *eventsHTTP) Validate() error {
	ep, problems := e.cfg.build(5000, false, e.readFile)
	switch e.cfg.Mode {
	case "", "async", "sync":
	default:
		problems = append(problems, `mode must be "async" or "sync"`)
	}
	switch e.cfg.Payload {
	case "", "base64", "omit":
	default:
		problems = append(problems, `payload must be "base64" or "omit"`)
	}
	problems = inRange(problems, "buffer.maxEvents", e.cfg.Buffer.MaxEvents, 1, 1_000_000)
	problems = inRange(problems, "batch.maxEvents", e.cfg.Batch.MaxEvents, 1, 1000)
	problems = inRange(problems, "batch.maxDelayMs", e.cfg.Batch.MaxDelayMs, 0, 10000)
	problems = inRange(problems, "retry.maxAttempts", e.cfg.Retry.MaxAttempts, 1, 10)
	problems = inRange(problems, "retry.backoffMs", e.cfg.Retry.BackoffMs, 10, 10000)
	known := map[string]bool{}
	for _, n := range eventNames {
		known[n] = true
	}
	enabled := map[string]bool{}
	if e.cfg.Events == nil {
		enabled = known
	}
	for _, n := range e.cfg.Events {
		if !known[n] {
			problems = append(problems, fmt.Sprintf("events: unknown event %q", n))
		}
		enabled[n] = true
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	e.ep, e.enabled = ep, enabled
	e.sync, e.omitPayload = e.cfg.Mode == "sync", e.cfg.Payload == "omit"
	e.bufferMax = intOr(e.cfg.Buffer.MaxEvents, 10_000)
	e.batchMax = intOr(e.cfg.Batch.MaxEvents, 1)
	e.batchDelay = time.Duration(intOr(e.cfg.Batch.MaxDelayMs, 100)) * time.Millisecond
	e.attempts = intOr(e.cfg.Retry.MaxAttempts, 3)
	e.backoff = time.Duration(intOr(e.cfg.Retry.BackoffMs, 200)) * time.Millisecond
	return nil
}

// UseMetrics receives the node's metrics provider and the buffer's gauge.
func (e *eventsHTTP) UseMetrics(m ext.MetricsProvider) {
	e.metrics.Store(&metricsBox{m: m})
	if c, ok := m.(ext.Collector); ok {
		c.AddCollector(func(set func(string, map[string]string, float64)) {
			e.mu.RLock()
			ch := e.ch
			e.mu.RUnlock()
			set(metricEventsBuffered, nil, float64(len(ch)))
		})
	}
}

func (e *eventsHTTP) add(name string, labels map[string]string, n int) {
	if b := e.metrics.Load(); b != nil {
		b.m.Add(name, labels, float64(n))
	}
}

func (e *eventsHTTP) Start(context.Context) error {
	if e.ep == nil {
		if err := e.Validate(); err != nil {
			return err
		}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.running = true
	if e.sync {
		return nil
	}
	e.ch = make(chan json.RawMessage, e.bufferMax)
	e.quit, e.done = make(chan struct{}), make(chan struct{})
	e.sendCtx, e.cancelSend = context.WithCancel(context.Background())
	go e.run()
	return nil
}

// Stop stops taking events and sends what the buffer holds until ctx ends
// or timeoutMs passes, since the broker's own context has no deadline; what
// is left then is dropped as stopped.
func (e *eventsHTTP) Stop(ctx context.Context) error {
	e.mu.Lock()
	was := e.running
	e.running = false
	e.mu.Unlock()
	if !was || e.sync {
		e.drops.flush()
		e.ep.client.CloseIdleConnections()
		return nil
	}
	close(e.quit)
	ctx, cancel := context.WithTimeout(ctx, e.ep.timeout)
	defer cancel()
	select {
	case <-e.done:
	case <-ctx.Done():
		e.cancelSend()
		<-e.done
	}
	e.cancelSend()
	e.drops.flush()
	e.ep.client.CloseIdleConnections()
	return nil
}

// run is the async worker: it gathers batches from the buffer and sends
// them, and at Stop sends the rest.
func (e *eventsHTTP) run() {
	defer close(e.done)
	for {
		var batch []json.RawMessage
		select {
		case ev := <-e.ch:
			batch = append(batch, ev)
		case <-e.quit:
			e.finish()
			return
		}
		e.deliver(e.sendCtx, e.fill(batch), dropStopped)
	}
}

// fill adds to a batch until it is full, batch.maxDelayMs has passed, or
// Stop is called.
func (e *eventsHTTP) fill(batch []json.RawMessage) []json.RawMessage {
	if len(batch) >= e.batchMax {
		return batch
	}
	t := time.NewTimer(e.batchDelay)
	defer t.Stop()
	for len(batch) < e.batchMax {
		select {
		case ev := <-e.ch:
			batch = append(batch, ev)
		case <-t.C:
			return batch
		case <-e.quit:
			return batch
		}
	}
	return batch
}

// finish sends what the buffer holds at Stop.
func (e *eventsHTTP) finish() {
	for {
		var batch []json.RawMessage
		for len(batch) < e.batchMax {
			select {
			case ev := <-e.ch:
				batch = append(batch, ev)
				continue
			default:
			}
			break
		}
		if len(batch) == 0 {
			return
		}
		e.deliver(e.sendCtx, batch, dropStopped)
	}
}

// deliver sends a batch with its attempts. An attempt fails on a transport
// error or a non-2xx answer; a batch whose attempts are spent is dropped as
// failed, and one whose context ends first as ended.
func (e *eventsHTTP) deliver(ctx context.Context, batch []json.RawMessage, ended string) {
	var body []byte
	if e.batchMax == 1 {
		body = batch[0]
	} else {
		body, _ = json.Marshal(batch)
	}
	wait := e.backoff
	var last error
	for attempt := 1; attempt <= e.attempts; attempt++ {
		if ctx.Err() != nil {
			e.drop(ended, len(batch), ctx.Err())
			return
		}
		resp, err := e.ep.post(ctx, body, nil)
		switch {
		case err != nil:
			last = errors.New(redact(err))
		case resp.status < 200 || resp.status > 299:
			last = fmt.Errorf("status %d", resp.status)
		default:
			e.add(metricEventsSent, nil, len(batch))
			return
		}
		if ctx.Err() != nil {
			e.drop(ended, len(batch), ctx.Err())
			return
		}
		if attempt == e.attempts {
			break
		}
		t := time.NewTimer(wait)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			e.drop(ended, len(batch), ctx.Err())
			return
		}
		wait = nextBackoff(wait, e.backoff)
	}
	e.drop(dropFailed, len(batch), last)
}

// nextBackoff doubles a wait up to 5 s, or up to the first wait when that
// is longer.
func nextBackoff(wait, first time.Duration) time.Duration {
	return min(2*wait, max(maxBackoff, first))
}

func (e *eventsHTTP) drop(reason string, n int, err error) {
	e.add(metricEventsDropped, map[string]string{"reason": reason}, n)
	e.drops.note(reason, n, err)
}

// emit posts or buffers one event when its family is configured.
func (e *eventsHTTP) emit(ctx context.Context, name, namespace string, fields map[string]any) {
	e.emitAt(ctx, name, namespace, e.now(), fields)
}

// emitAt is emit with the time the event happened, for an event the broker
// queued before this module saw it.
func (e *eventsHTTP) emitAt(ctx context.Context, name, namespace string, at time.Time, fields map[string]any) {
	fields["event"], fields["timestamp"], fields["node"], fields["namespace"] = name, at.UnixMilli(), e.node, namespace
	data, err := json.Marshal(fields)
	if err != nil {
		e.drop(dropFailed, 1, err)
		return
	}
	if e.sync {
		e.mu.RLock()
		running := e.running
		e.mu.RUnlock()
		if !running {
			e.drop(dropStopped, 1, errors.New("the module is stopped"))
			return
		}
		e.deliver(ctx, []json.RawMessage{data}, dropFailed)
		return
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if !e.running {
		e.drop(dropStopped, 1, errors.New("the module is stopped"))
		return
	}
	select {
	case e.ch <- data:
	default:
		e.drop(dropBufferFull, 1, fmt.Errorf("the buffer holds %d events", e.bufferMax))
	}
}

func (e *eventsHTTP) wants(name string) bool { return e.enabled[name] }

func (e *eventsHTTP) payload(fields map[string]any, p []byte) {
	if !e.omitPayload {
		fields["payload"] = p
	}
}

func connFields(info ext.ConnectionInfo) map[string]any {
	return map[string]any{
		"clientid": info.ClientID, "username": info.Username, "peername": info.PeerAddr,
		"listener": info.Listener, "proto_ver": int(info.Version),
	}
}

// Connected posts client.connected.
func (e *eventsHTTP) Connected(ctx context.Context, info ext.ConnectionInfo) {
	if e.wants("client.connected") {
		e.emit(ctx, "client.connected", info.Namespace, connFields(info))
	}
}

// Disconnected posts client.disconnected.
func (e *eventsHTTP) Disconnected(ctx context.Context, info ext.ConnectionInfo, reason mqtt.ReasonCode, graceful bool) {
	if e.wants("client.disconnected") {
		f := connFields(info)
		f["reason"], f["graceful"] = int(reason), graceful
		e.emit(ctx, "client.disconnected", info.Namespace, f)
	}
}

// InterceptSubscribe posts session.subscribe and changes nothing: the
// request, before authorization decides it.
func (e *eventsHTTP) InterceptSubscribe(ctx context.Context, info ext.ConnectionInfo, sub *mqtt.Subscription) ext.Outcome {
	if e.wants("session.subscribe") {
		e.emit(ctx, "session.subscribe", info.Namespace, map[string]any{
			"clientid": info.ClientID, "username": info.Username, "topic": sub.Filter, "qos": int(sub.QoS),
		})
	}
	return ext.OK
}

// InterceptUnsubscribe posts session.unsubscribe and changes nothing: the
// request, before authorization decides it.
func (e *eventsHTTP) InterceptUnsubscribe(ctx context.Context, info ext.ConnectionInfo, filter string) ext.Outcome {
	if e.wants("session.unsubscribe") {
		e.emit(ctx, "session.unsubscribe", info.Namespace, map[string]any{
			"clientid": info.ClientID, "username": info.Username, "topic": filter,
		})
	}
	return ext.OK
}

// OnClusterEvent posts the cluster.* events. It runs on the broker's queue
// for this module, never on the path that raised the event, so sync mode
// holds up only that queue.
func (e *eventsHTTP) OnClusterEvent(ctx context.Context, ev ext.ClusterEvent) {
	name := "cluster." + ev.Kind.String()
	if !e.wants(name) {
		return
	}
	at := ev.Time
	if at.IsZero() {
		at = e.now()
	}
	switch ev.Kind {
	case ext.LeaseMoved:
		e.emitAt(ctx, name, ev.Namespace, at, map[string]any{"clientid": ev.ClientID, "from": ev.From, "to": ev.To})
	default:
		e.emitAt(ctx, name, e.ns, at, map[string]any{"member": ev.Member})
	}
}

// OnPublishAccepted posts message.publish.
func (e *eventsHTTP) OnPublishAccepted(ctx context.Context, source ext.PublishSource, m *mqtt.Message) {
	if e.wants("message.publish") {
		f := map[string]any{
			"source": source.String(), "clientid": m.Publisher.ClientID, "username": m.Publisher.Username,
			"topic": m.Topic, "qos": int(m.QoS), "retain": m.Retain,
		}
		e.payload(f, m.Payload)
		e.emit(ctx, "message.publish", m.Namespace, f)
	}
}

// OnDelivery posts the delivery events.
func (e *eventsHTTP) OnDelivery(ctx context.Context, event ext.DeliveryEvent, namespace, clientID string, m *mqtt.Message, qos byte) {
	name := deliveryNames[event]
	if name != "" && e.wants(name) {
		f := map[string]any{"clientid": clientID, "topic": m.Topic, "qos": int(qos)}
		e.payload(f, m.Payload)
		e.emit(ctx, name, namespace, f)
	}
}

// OnRetained posts retained.changed; an empty payload is a removal.
func (e *eventsHTTP) OnRetained(ctx context.Context, m *mqtt.Message) {
	if e.wants("retained.changed") {
		f := map[string]any{"topic": m.Topic}
		e.payload(f, m.Payload)
		e.emit(ctx, "retained.changed", m.Namespace, f)
	}
}

// OnWill posts will.published.
func (e *eventsHTTP) OnWill(ctx context.Context, namespace, clientID string, m *mqtt.Message) {
	if e.wants("will.published") {
		f := map[string]any{"clientid": clientID, "topic": m.Topic, "qos": int(m.QoS)}
		e.payload(f, m.Payload)
		e.emit(ctx, "will.published", namespace, f)
	}
}

// dropLog logs drops at Warn at most once a second per reason, with the
// count since the last line.
type dropLog struct {
	log *slog.Logger

	mu      sync.Mutex
	last    map[string]time.Time
	pending map[string]int
	lastErr map[string]error
}

func (d *dropLog) note(reason string, n int, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.last == nil {
		d.last, d.pending, d.lastErr = map[string]time.Time{}, map[string]int{}, map[string]error{}
	}
	d.pending[reason] += n
	d.lastErr[reason] = err
	now := time.Now()
	if now.Sub(d.last[reason]) < time.Second {
		return
	}
	d.last[reason] = now
	d.emit(reason)
}

// flush logs every reason with drops not yet logged.
func (d *dropLog) flush() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for reason := range d.pending {
		d.emit(reason)
	}
}

func (d *dropLog) emit(reason string) {
	n := d.pending[reason]
	if n == 0 || d.log == nil {
		return
	}
	d.pending[reason] = 0
	var msg string
	if err := d.lastErr[reason]; err != nil {
		msg = err.Error()
	}
	d.log.Warn("events.http dropped events", "reason", reason, "count", n, "err", msg)
}
