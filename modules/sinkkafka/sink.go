// Package sinkkafka is sink.kafka, HoardMQ's Kafka forwarder: every publish
// the node accepts on a topic a route matches is produced to that route's
// Kafka topic. It is built on the public extension API alone, as a module
// from outside the broker would be, and every binary built from the broker's
// entry point links it; it runs only when modules.sink.kafka is configured.
//
// The forwarder buffers in memory, bounded by buffer.maxRecords and
// buffer.maxBytes. While the buffer is full, buffer.whenFull "refuse" (the
// default) refuses a client's matching QoS 1 or 2 publish with Quota Exceeded,
// and "drop" accepts it and drops its forward. Every record ends counted:
// produced, or dropped with its reason. What a node holds in its buffer when
// it crashes is lost uncounted; the buffered gauges are that exposure.
package sinkkafka

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/happydave/hoardmq/ext"
	"github.com/happydave/hoardmq/ext/mqtt"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ID is the module's identifier.
const ID ext.ID = "sink.kafka"

func init() {
	ext.Register(ID, func() ext.Module { return &sink{newProducer: newClient} })
}

// The module's series. The buffered gauges are set at each scrape.
const (
	metricProduced        = "hoardmq_sink_kafka_produced_total"
	metricDropped         = "hoardmq_sink_kafka_dropped_total"
	metricRefused         = "hoardmq_sink_kafka_refused_total"
	metricBufferedRecords = "hoardmq_sink_kafka_buffered_records"
	metricBufferedBytes   = "hoardmq_sink_kafka_buffered_bytes"
)

// waitPoll is how often a held publish looks for room in the buffer.
const waitPoll = 5 * time.Millisecond

// producer is what the module needs of a Kafka client; *kgo.Client is one.
type producer interface {
	TryProduce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error))
	BufferedProduceRecords() int64
	BufferedProduceBytes() int64
	Flush(ctx context.Context) error
	UnsafeAbortBufferedRecords()
	Close()
}

type sink struct {
	cfg         config
	node, ns    string
	log         *slog.Logger
	set         *settings
	newProducer func(*settings) (producer, error)

	mu      sync.Mutex
	metrics ext.MetricsProvider

	// p is set by Start and cleared by Stop; running gates the hooks.
	p       atomic.Pointer[producerBox]
	running atomic.Bool

	drops dropLog
}

type producerBox struct{ p producer }

func (*sink) ID() ext.ID { return ID }

func (*sink) Capabilities() ext.Capability {
	return ext.CapPublishAccepted | ext.CapPublishAdmission
}

// Provision parses the configuration and connects to nothing.
func (s *sink) Provision(ctx *ext.Context, raw json.RawMessage) error {
	c, err := decode(raw)
	if err != nil {
		return err
	}
	s.cfg, s.node, s.ns = c, ctx.Node, ctx.Namespace
	s.log = ctx.Logger
	if s.log == nil {
		s.log = slog.Default()
	}
	s.drops.log = s.log
	return nil
}

// Validate checks the configuration as a whole and reads the files it names;
// it writes nothing and connects to nothing.
func (s *sink) Validate() error {
	set, err := validate(s.cfg, s.node, s.ns, os.ReadFile)
	if err != nil {
		return err
	}
	s.set = set
	return nil
}

// UseMetrics receives the node's metrics provider, and registers the
// buffered gauges when it can set them at a scrape.
func (s *sink) UseMetrics(m ext.MetricsProvider) {
	s.mu.Lock()
	s.metrics = m
	s.mu.Unlock()
	if c, ok := m.(ext.Collector); ok {
		c.AddCollector(func(set func(string, map[string]string, float64)) {
			var records, bytes int64
			if b := s.p.Load(); b != nil {
				records, bytes = b.p.BufferedProduceRecords(), b.p.BufferedProduceBytes()
			}
			set(metricBufferedRecords, nil, float64(records))
			set(metricBufferedBytes, nil, float64(bytes))
		})
	}
}

// Start builds the Kafka client; it connects when it first produces.
func (s *sink) Start(context.Context) error {
	if s.set == nil {
		if err := s.Validate(); err != nil {
			return err
		}
	}
	p, err := s.newProducer(s.set)
	if err != nil {
		return err
	}
	s.p.Store(&producerBox{p})
	s.running.Store(true)
	return nil
}

// Stop flushes for up to flushTimeoutMs, then abandons what is left, which
// is counted shutdown, and closes the client. A record still awaiting Kafka's
// answer when it is abandoned may have been written; it is counted dropped
// all the same.
func (s *sink) Stop(context.Context) error {
	s.running.Store(false)
	b := s.p.Load()
	if b == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.set.flush)
	err := b.p.Flush(ctx)
	cancel()
	if err != nil {
		left := b.p.BufferedProduceRecords()
		s.log.Warn("sink.kafka: flush incomplete at stop; abandoning the buffer", "records", left, "err", err)
		b.p.UnsafeAbortBufferedRecords()
	}
	b.p.Close()
	s.p.Store(nil)
	s.drops.flush()
	return nil
}

// AdmitPublish refuses a client's matching QoS 1 or 2 publish with Quota
// Exceeded while the buffer is full under the refuse policy.
func (s *sink) AdmitPublish(_ context.Context, _ ext.ConnectionInfo, m *mqtt.Message) ext.Outcome {
	b := s.p.Load()
	if b == nil || !s.running.Load() || !s.set.refuse || m.QoS == 0 {
		return ext.OK
	}
	routes := s.set.matches(m)
	if !refuses(s.set.refuse, ext.SourceClient, m.QoS, len(routes) > 0,
		b.p.BufferedProduceRecords(), b.p.BufferedProduceBytes(), s.set.maxRecords, s.set.maxBytes) {
		return ext.OK
	}
	s.add(metricRefused, map[string]string{"topic": routes[0].topic})
	return ext.Outcome{Action: ext.Drop, Reason: mqtt.QuotaExceeded}
}

// OnPublishAccepted produces one record per Kafka topic the message's routes
// name. Every record is counted once, by its promise.
func (s *sink) OnPublishAccepted(_ context.Context, source ext.PublishSource, m *mqtt.Message) {
	if s.set == nil {
		return
	}
	routes := s.set.matches(m)
	if len(routes) == 0 {
		return
	}
	b := s.p.Load()
	held := waits(s.set.refuse, source, m.QoS)
	for _, r := range routes {
		rec := s.set.record(r, source, m)
		if b == nil || !s.running.Load() {
			s.count(rec, errStopped, held)
			continue
		}
		if held {
			s.waitForRoom(b.p)
		}
		b.p.TryProduce(context.Background(), rec, func(rec *kgo.Record, err error) { s.count(rec, err, held) })
	}
}

// waitForRoom holds a publish for up to buffer.waitMs while the buffer is
// full. It is the module's own wait: a record produced with a context that
// is cancelled later can be failed after it is buffered, so no record carries
// one.
func (s *sink) waitForRoom(p producer) {
	deadline := time.Now().Add(s.set.wait)
	for p.BufferedProduceRecords() >= s.set.maxRecords || p.BufferedProduceBytes() >= s.set.maxBytes {
		if !time.Now().Before(deadline) {
			return
		}
		time.Sleep(waitPoll)
	}
}

// count records a record's result: produced, or dropped with its reason.
func (s *sink) count(rec *kgo.Record, err error, held bool) {
	reason := outcome(err, held, !s.running.Load())
	if reason == "" {
		s.add(metricProduced, map[string]string{"topic": rec.Topic})
		return
	}
	s.add(metricDropped, map[string]string{"topic": rec.Topic, "reason": reason})
	s.drops.note(reason, rec.Topic, err)
}

func (s *sink) add(name string, labels map[string]string) {
	s.mu.Lock()
	m := s.metrics
	s.mu.Unlock()
	if m != nil {
		m.Add(name, labels, 1)
	}
}

// dropLog logs drops at Warn, at most once a second per reason, with the
// count since the last line.
type dropLog struct {
	log *slog.Logger

	mu      sync.Mutex
	last    map[string]time.Time
	pending map[string]int
	lastErr map[string]error
}

func (d *dropLog) note(reason, topic string, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.last == nil {
		d.last, d.pending, d.lastErr = map[string]time.Time{}, map[string]int{}, map[string]error{}
	}
	d.pending[reason]++
	d.lastErr[reason] = err
	now := time.Now()
	if now.Sub(d.last[reason]) < time.Second {
		return
	}
	d.last[reason] = now
	d.emit(reason, topic)
}

// flush logs every reason with drops not yet logged.
func (d *dropLog) flush() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for reason := range d.pending {
		d.emit(reason, "")
	}
}

func (d *dropLog) emit(reason, topic string) {
	n := d.pending[reason]
	if n == 0 || d.log == nil {
		return
	}
	attrs := []any{"reason", reason, "records", n}
	if topic != "" {
		attrs = append(attrs, "topic", topic)
	}
	if err := d.lastErr[reason]; err != nil && !errors.Is(err, errStopped) {
		attrs = append(attrs, "err", err)
	}
	d.log.Warn("sink.kafka: records dropped", attrs...)
	d.pending[reason] = 0
}
