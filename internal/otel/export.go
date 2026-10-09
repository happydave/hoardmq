package otel

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// ExportConfig is where spans go and how they are batched.
type ExportConfig struct {
	// Endpoint is the collector's OTLP/HTTP traces URL, used as given.
	Endpoint      string
	BatchSize     int
	QueueSize     int
	FlushInterval time.Duration
	Timeout       time.Duration
	// Client, when set, replaces the exporter's HTTP client.
	Client *http.Client
}

// scopeName is the instrumentation scope every span is exported under.
const scopeName = "github.com/happydave/hoardmq"

// Export limits.
const (
	maxAttempts = 3
	firstRetry  = 100 * time.Millisecond
	stopFlush   = 2 * time.Second
)

// Drop reasons, as the dropped counter labels them.
const (
	dropQueueFull    = "queue_full"
	dropExportFailed = "export_failed"
	dropRejected     = "rejected"
	dropShutdown     = "shutdown"
)

type exporter struct {
	cfg      ExportConfig
	client   *http.Client
	resource []byte // the encoded resource, the same for every request
	queue    chan SpanData
	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
	closed   atomic.Bool

	exported atomic.Int64
	dropped  [4]atomic.Int64 // by dropReasons
	exports  [3]atomic.Int64 // ok, retried, failed
}

var dropReasons = [4]string{dropQueueFull, dropExportFailed, dropRejected, dropShutdown}

func dropIndex(reason string) int {
	for i, r := range dropReasons {
		if r == reason {
			return i
		}
	}
	return 0
}

func newExporter(cfg ExportConfig, service, node string) *exporter {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 512
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 8192
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = time.Second
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}
	client := cfg.Client
	if client == nil {
		client = &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone()}
	}
	res, _ := json.Marshal(resource{Attributes: []keyValue{
		stringKV("service.name", service), stringKV("service.instance.id", node),
	}})
	return &exporter{cfg: cfg, client: client, resource: res, queue: make(chan SpanData, cfg.QueueSize),
		stop: make(chan struct{}), done: make(chan struct{})}
}

// Start starts the exporter's goroutine; a tracer with a sink has none.
func (t *Tracer) Start() {
	if t != nil && t.exp != nil {
		go t.exp.run()
	}
}

// Stop sends what is queued, waiting up to two seconds or ctx, and counts
// anything left as dropped.
func (t *Tracer) Stop(ctx context.Context) {
	if t == nil || t.exp == nil {
		return
	}
	t.exp.shutdown(ctx)
}

func (e *exporter) enqueue(d SpanData) {
	if e.closed.Load() {
		e.dropped[dropIndex(dropShutdown)].Add(1)
		return
	}
	select {
	case e.queue <- d:
	default:
		e.dropped[dropIndex(dropQueueFull)].Add(1)
	}
}

func (e *exporter) stats(s *Stats) {
	s.Exported = e.exported.Load()
	for i, r := range dropReasons {
		s.Dropped[r] = e.dropped[i].Load()
	}
	s.Exports["ok"], s.Exports["retried"], s.Exports["failed"] = e.exports[0].Load(), e.exports[1].Load(), e.exports[2].Load()
	s.Queued = len(e.queue)
}

func (e *exporter) shutdown(ctx context.Context) {
	e.stopOnce.Do(func() {
		e.closed.Store(true)
		close(e.stop)
	})
	ctx, cancel := context.WithTimeout(ctx, stopFlush)
	defer cancel()
	select {
	case <-e.done:
	case <-ctx.Done():
	}
}

func (e *exporter) run() {
	defer close(e.done)
	batch := make([]SpanData, 0, e.cfg.BatchSize)
	timer := time.NewTimer(e.cfg.FlushInterval)
	defer timer.Stop()
	for {
		select {
		case d := <-e.queue:
			batch = append(batch, d)
			if len(batch) >= e.cfg.BatchSize {
				e.send(context.Background(), batch)
				batch = batch[:0]
			}
		case <-timer.C:
			if len(batch) > 0 {
				e.send(context.Background(), batch)
				batch = batch[:0]
			}
			timer.Reset(e.cfg.FlushInterval)
		case <-e.stop:
			e.flushOnStop(batch)
			return
		}
	}
}

// flushOnStop sends the batch in hand and everything queued, within the stop
// budget; what it cannot send is counted.
func (e *exporter) flushOnStop(batch []SpanData) {
	ctx, cancel := context.WithTimeout(context.Background(), stopFlush)
	defer cancel()
	for {
		for len(batch) < e.cfg.BatchSize {
			select {
			case d := <-e.queue:
				batch = append(batch, d)
				continue
			default:
			}
			break
		}
		if len(batch) == 0 {
			return
		}
		if ctx.Err() != nil {
			e.dropped[dropIndex(dropShutdown)].Add(int64(len(batch) + len(e.queue)))
			return
		}
		e.send(ctx, batch)
		batch = batch[:0]
	}
}

// Response outcomes.
type outcome int

const (
	outcomeDone outcome = iota
	outcomeRetry
	outcomeDrop
)

// exportDecision says what a response means for a batch: a 2xx is done; 429,
// 502, 503 and 504 and a failed transport are retried until the last attempt,
// after the larger of the doubling backoff and Retry-After capped at the flush
// interval; every other status drops the batch (OTLP 1.11.0, Failures).
func exportDecision(status int, transportFailed bool, attempt int, retryAfter, flush time.Duration) (outcome, time.Duration) {
	if !transportFailed && status >= 200 && status < 300 {
		return outcomeDone, 0
	}
	retryable := transportFailed || status == http.StatusTooManyRequests || status == http.StatusBadGateway ||
		status == http.StatusServiceUnavailable || status == http.StatusGatewayTimeout
	if !retryable || attempt >= maxAttempts {
		return outcomeDrop, 0
	}
	wait := firstRetry << (attempt - 1)
	if retryAfter > flush {
		retryAfter = flush
	}
	if retryAfter > wait {
		wait = retryAfter
	}
	return outcomeRetry, wait
}

// parseRetryAfter reads Retry-After as seconds or an HTTP-date.
func parseRetryAfter(v string, now time.Time) time.Duration {
	if v == "" {
		return 0
	}
	if n, err := strconv.Atoi(v); err == nil && n >= 0 {
		return time.Duration(n) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil && t.After(now) {
		return t.Sub(now)
	}
	return 0
}

func (e *exporter) send(ctx context.Context, batch []SpanData) {
	body, err := encodeRequest(e.resource, batch)
	if err != nil {
		e.exports[2].Add(1)
		e.dropped[dropIndex(dropExportFailed)].Add(int64(len(batch)))
		return
	}
	for attempt := 1; ; attempt++ {
		status, retryAfter, rejected, err := e.post(ctx, body)
		act, wait := exportDecision(status, err != nil, attempt, retryAfter, e.cfg.FlushInterval)
		switch act {
		case outcomeDone:
			e.exports[0].Add(1)
			if rejected > int64(len(batch)) {
				rejected = int64(len(batch))
			}
			e.exported.Add(int64(len(batch)) - rejected)
			e.dropped[dropIndex(dropRejected)].Add(rejected)
			return
		case outcomeDrop:
			e.exports[2].Add(1)
			e.dropped[dropIndex(dropExportFailed)].Add(int64(len(batch)))
			return
		}
		e.exports[1].Add(1)
		t := time.NewTimer(wait)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			e.exports[2].Add(1)
			e.dropped[dropIndex(dropExportFailed)].Add(int64(len(batch)))
			return
		}
	}
}

func (e *exporter) post(ctx context.Context, body []byte) (status int, retryAfter time.Duration, rejected int64, err error) {
	ctx, cancel := context.WithTimeout(ctx, e.cfg.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.cfg.Endpoint, bytes.NewReader(body))
	if err != nil {
		return 0, 0, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := e.client.Do(req)
	if err != nil {
		return 0, 0, 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		rejected = partialRejected(data)
	}
	return resp.StatusCode, parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()), rejected, nil
}

// partialRejected reads partialSuccess.rejectedSpans, a 64-bit integer a
// server may write as a string or a number.
func partialRejected(body []byte) int64 {
	var r struct {
		PartialSuccess struct {
			RejectedSpans json.RawMessage `json:"rejectedSpans"`
		} `json:"partialSuccess"`
	}
	if json.Unmarshal(body, &r) != nil || len(r.PartialSuccess.RejectedSpans) == 0 {
		return 0
	}
	raw := bytes.Trim(r.PartialSuccess.RejectedSpans, `"`)
	n, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// The OTLP 1.11.0 JSON shapes the exporter writes.
type (
	resource struct {
		Attributes []keyValue `json:"attributes"`
	}
	keyValue struct {
		Key   string   `json:"key"`
		Value anyValue `json:"value"`
	}
	anyValue struct {
		StringValue *string  `json:"stringValue,omitempty"`
		IntValue    string   `json:"intValue,omitempty"` // int64 as a decimal string
		BoolValue   *bool    `json:"boolValue,omitempty"`
		DoubleValue *float64 `json:"doubleValue,omitempty"`
	}
	spanJSON struct {
		TraceID           string     `json:"traceId"`
		SpanID            string     `json:"spanId"`
		TraceState        string     `json:"traceState,omitempty"`
		ParentSpanID      string     `json:"parentSpanId,omitempty"`
		Name              string     `json:"name"`
		Kind              int        `json:"kind"`
		StartTimeUnixNano string     `json:"startTimeUnixNano"`
		EndTimeUnixNano   string     `json:"endTimeUnixNano"`
		Attributes        []keyValue `json:"attributes,omitempty"`
		Status            *status    `json:"status,omitempty"`
	}
	status struct {
		Message string `json:"message,omitempty"`
		Code    int    `json:"code"`
	}
)

func stringKV(k, v string) keyValue { return keyValue{Key: k, Value: anyValue{StringValue: &v}} }

func attrKV(a Attr) keyValue {
	kv := keyValue{Key: a.Key}
	switch a.Type {
	case AttrInt:
		kv.Value.IntValue = strconv.FormatInt(a.Int, 10)
	case AttrBool:
		b := a.Bool
		kv.Value.BoolValue = &b
	case AttrDouble:
		f := a.Double
		kv.Value.DoubleValue = &f
	default:
		s := a.Str
		kv.Value.StringValue = &s
	}
	return kv
}

func encodeSpan(d SpanData) spanJSON {
	s := spanJSON{
		TraceID: hex.EncodeToString(d.TraceID[:]), SpanID: hex.EncodeToString(d.SpanID[:]), TraceState: d.TraceState,
		Name: d.Name, Kind: int(d.Kind),
		StartTimeUnixNano: strconv.FormatUint(uint64(d.Start), 10), EndTimeUnixNano: strconv.FormatUint(uint64(d.End), 10),
	}
	if d.HasParent {
		s.ParentSpanID = hex.EncodeToString(d.ParentSpanID[:])
	}
	for _, a := range d.Attrs {
		s.Attributes = append(s.Attributes, attrKV(a))
	}
	if d.Status != StatusUnset {
		s.Status = &status{Code: int(d.Status), Message: d.StatusMessage}
	}
	return s
}

type (
	exportRequest struct {
		ResourceSpans []resourceSpans `json:"resourceSpans"`
	}
	resourceSpans struct {
		Resource   json.RawMessage `json:"resource"`
		ScopeSpans []scopeSpans    `json:"scopeSpans"`
	}
	scopeSpans struct {
		Scope scopeJSON  `json:"scope"`
		Spans []spanJSON `json:"spans"`
	}
	scopeJSON struct {
		Name string `json:"name"`
	}
)

// encodeRequest builds one ExportTraceServiceRequest of the batch under the
// node's resource and the broker's scope. It fails only on a double attribute
// JSON cannot carry (NaN or an infinity).
func encodeRequest(resourceJSON []byte, batch []SpanData) ([]byte, error) {
	spans := make([]spanJSON, len(batch))
	for i, d := range batch {
		spans[i] = encodeSpan(d)
	}
	return json.Marshal(exportRequest{ResourceSpans: []resourceSpans{{
		Resource:   resourceJSON,
		ScopeSpans: []scopeSpans{{Scope: scopeJSON{Name: scopeName}, Spans: spans}},
	}}})
}
