// Package sinkpostgres is sink.postgres, a HoardMQ sink that writes each
// publish the node accepts on a topic a route matches into that route's
// Postgres table, in batches, with sink.kafka's buffer and policy. It is
// built outside the broker's module, on the public extension API, so pgx is
// a dependency of this module alone. Build it into a binary with
//
//	hoardmq-build --with github.com/happydave/hoardmq-sinkpostgres=<this directory>
//
// The buffer is bounded by buffer.maxRecords and buffer.maxBytes. While it is
// full, buffer.whenFull "refuse" (the default) refuses a client's matching
// QoS 1 or 2 publish with Quota Exceeded, and "drop" accepts it and drops its
// rows. Every row ends counted: written, or dropped with its reason. What the
// buffer holds when a node crashes is lost uncounted; the buffered gauges are
// that exposure.
package sinkpostgres

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/happydave/hoardmq/ext"
	"github.com/happydave/hoardmq/ext/mqtt"
)

// ID is the module's identifier.
const ID ext.ID = "sink.postgres"

const (
	metricWritten         = "hoardmq_sink_postgres_written_total"
	metricDropped         = "hoardmq_sink_postgres_dropped_total"
	metricRefused         = "hoardmq_sink_postgres_refused_total"
	metricRetries         = "hoardmq_sink_postgres_retries_total"
	metricBufferedRecords = "hoardmq_sink_postgres_buffered_records"
	metricBufferedBytes   = "hoardmq_sink_postgres_buffered_bytes"

	// waitPoll is how often a held publish looks for room.
	waitPoll = 5 * time.Millisecond
	// The retry backoff after a transient failure.
	firstBackoff = 100 * time.Millisecond
	maxBackoff   = 5 * time.Second
)

// columns are the table's columns, in the order each row's values are.
var columns = []string{"topic", "payload", "qos", "retain", "client_id", "namespace", "source", "accepted_at"}

func init() {
	ext.Register(ID, func() ext.Module { return &sink{readFile: os.ReadFile, open: openPool} })
}

// writer writes one batch of rows to one table.
type writer interface {
	Write(ctx context.Context, r route, rows [][]any) error
	Close()
}

type sink struct {
	cfg      config
	ns       string
	log      *slog.Logger
	set      *settings
	readFile func(string) ([]byte, error)
	open     func(*settings) (writer, error)

	metrics atomic.Pointer[metricsBox]
	running atomic.Bool
	q       *queue
	w       writer
	done    chan struct{}
	stopCtx context.Context
	stop    context.CancelFunc
	drops   dropLog
}

type metricsBox struct{ m ext.MetricsProvider }

func (*sink) ID() ext.ID { return ID }

func (*sink) Capabilities() ext.Capability { return ext.CapPublishAccepted | ext.CapPublishAdmission }

func (s *sink) Provision(ctx *ext.Context, raw json.RawMessage) error {
	if err := decodeStrict(raw, &s.cfg); err != nil {
		return err
	}
	s.ns, s.log = ctx.Namespace, ctx.Logger
	if s.log == nil {
		s.log = slog.Default()
	}
	s.drops.log = s.log
	return nil
}

// Validate checks the configuration and reads passwordFile; it connects to
// nothing.
func (s *sink) Validate() error {
	set, problems := s.cfg.validate(s.ns, s.readFile)
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	s.set = set
	return nil
}

// UseMetrics receives the node's metrics provider and sets the buffered
// gauges at each scrape.
func (s *sink) UseMetrics(m ext.MetricsProvider) {
	s.metrics.Store(&metricsBox{m: m})
	if c, ok := m.(ext.Collector); ok {
		c.AddCollector(func(set func(string, map[string]string, float64)) {
			if q := s.q; q != nil {
				records, bytes := q.size()
				set(metricBufferedRecords, nil, float64(records))
				set(metricBufferedBytes, nil, float64(bytes))
			}
		})
	}
}

func (s *sink) add(name string, labels map[string]string, n int) {
	if b := s.metrics.Load(); b != nil {
		b.m.Add(name, labels, float64(n))
	}
}

// Start opens the pool, which connects on first use, and starts the writer.
func (s *sink) Start(context.Context) error {
	if s.set == nil {
		if err := s.Validate(); err != nil {
			return err
		}
	}
	w, err := s.open(s.set)
	if err != nil {
		return err
	}
	s.w = w
	s.q = newQueue(s.set.maxRecords, s.set.maxBytes)
	s.done = make(chan struct{})
	s.stopCtx, s.stop = context.WithCancel(context.Background())
	go s.run()
	s.running.Store(true)
	return nil
}

// Stop takes no new rows, writes what the buffer holds for up to
// flushTimeoutMs, then counts what is left, a batch in flight included,
// dropped as shutdown.
func (s *sink) Stop(context.Context) error {
	if !s.running.Swap(false) {
		return nil
	}
	s.q.close()
	t := time.NewTimer(s.set.flush)
	select {
	case <-s.done:
		t.Stop()
	case <-t.C:
		s.stop()
		<-s.done
	}
	s.stop()
	for _, b := range s.q.drain() {
		s.drop(b.route.table, reasonShutdown, len(b.rows), errStopped)
	}
	s.w.Close()
	s.drops.flush()
	return nil
}

// errStopped marks rows the module took no more or could not write before
// its stop ended.
var errStopped = errors.New("sink.postgres is stopping")

// AdmitPublish refuses a client's matching QoS 1 or 2 publish with Quota
// Exceeded while the buffer is full under the refuse policy.
func (s *sink) AdmitPublish(_ context.Context, _ ext.ConnectionInfo, m *mqtt.Message) ext.Outcome {
	if !s.running.Load() || !s.set.refuse || m.QoS == 0 {
		return ext.OK
	}
	routes := s.set.matches(m)
	records, bytes := s.q.size()
	if !refuses(s.set.refuse, ext.SourceClient, m.QoS, len(routes) > 0, records, bytes, s.set.maxRecords, s.set.maxBytes) {
		return ext.OK
	}
	s.add(metricRefused, map[string]string{"table": routes[0].table}, 1)
	return ext.Outcome{Action: ext.Drop, Reason: mqtt.QuotaExceeded}
}

// OnPublishAccepted takes one row per distinct table the message's routes
// name.
func (s *sink) OnPublishAccepted(_ context.Context, source ext.PublishSource, m *mqtt.Message) {
	if s.set == nil {
		return
	}
	routes := s.set.matches(m)
	if len(routes) == 0 {
		return
	}
	held := waits(s.set.refuse, source, m.QoS)
	for _, r := range routes {
		if !s.running.Load() {
			s.drop(r.table, reasonShutdown, 1, errStopped)
			continue
		}
		vals, size := rowOf(source, m)
		if held {
			s.waitForRoom()
		}
		if !s.q.push(r, vals, size) {
			reason := reasonBufferFull
			if held {
				reason = reasonOverflow
			}
			s.drop(r.table, reason, 1, nil)
		}
	}
}

// rowOf is a message's row, in columns' order, and its size in the buffer.
func rowOf(source ext.PublishSource, m *mqtt.Message) ([]any, int64) {
	var client any
	if m.Publisher.ClientID != "" {
		client = m.Publisher.ClientID
	}
	payload := m.Payload
	if payload == nil {
		payload = []byte{}
	}
	at := m.Arrived
	if at.IsZero() {
		at = time.Now()
	}
	return []any{m.Topic, payload, int16(m.QoS), m.Retain, client, m.Namespace, source.String(), at},
		int64(len(m.Topic) + len(payload) + len(m.Publisher.ClientID))
}

// waitForRoom holds a publish for up to buffer.waitMs while the buffer is
// full.
func (s *sink) waitForRoom() {
	deadline := time.Now().Add(s.set.wait)
	for s.q.full() && time.Now().Before(deadline) {
		time.Sleep(waitPoll)
	}
}

func (s *sink) drop(table, reason string, n int, err error) {
	s.add(metricDropped, map[string]string{"table": table, "reason": reason}, n)
	s.drops.note(reason, table, n, err)
}

// run is the writer: one batch of one table at a time, retried while the
// failure is transient, until Stop ends it.
func (s *sink) run() {
	defer close(s.done)
	backoff := firstBackoff
	for {
		b, ok := s.q.next(s.stopCtx, s.set.maxRows, s.set.linger)
		if !ok {
			return
		}
		for {
			ctx, cancel := context.WithTimeout(s.stopCtx, s.set.timeout)
			err := s.w.Write(ctx, b.route, b.rows)
			cancel()
			switch {
			case err == nil:
				s.q.done(b)
				s.add(metricWritten, map[string]string{"table": b.route.table}, len(b.rows))
				backoff = firstBackoff
			case s.stopCtx.Err() != nil:
				// Stop's flush time ran out: the batch is drained and counted
				// with the rest.
				s.q.putBack(b)
				return
			case transient(err):
				s.add(metricRetries, nil, 1)
				s.drops.retry(b.route.table, err)
				t := time.NewTimer(backoff)
				select {
				case <-t.C:
				case <-s.stopCtx.Done():
					t.Stop()
					s.q.putBack(b)
					return
				}
				backoff = min(2*backoff, maxBackoff)
				continue
			default:
				s.q.done(b)
				s.drop(b.route.table, reasonRejected, len(b.rows), err)
			}
			break
		}
	}
}
