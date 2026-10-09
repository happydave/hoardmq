package otel

import (
	"math"
	"math/rand/v2"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
)

// Kind is an OTLP span kind, with its protocol value.
type Kind uint8

// Span kinds.
const (
	KindServer   Kind = 2
	KindClient   Kind = 3
	KindProducer Kind = 4
	KindConsumer Kind = 5
)

// StatusCode is an OTLP status code, with its protocol value.
type StatusCode uint8

// Status codes.
const (
	StatusUnset StatusCode = 0
	StatusError StatusCode = 2
)

// Config configures a tracer.
type Config struct {
	// Ratio is the share of traces without a sampled parent that are sampled,
	// 0 to 1.
	Ratio float64
	// ServiceName and Node name the resource every span belongs to.
	ServiceName string
	Node        string
	// Sink, when set, receives every ended span on the ending goroutine and
	// nothing is exported: for tests.
	Sink func(SpanData)
	// Export is where and how spans are sent when Sink is nil.
	Export ExportConfig
}

// Tracer samples, records and hands ended spans to its exporter. A nil
// Tracer is tracing off.
type Tracer struct {
	ratio   float64
	service string
	node    string
	enabled atomic.Bool
	sink    func(SpanData)
	exp     *exporter
	started atomic.Int64
}

// New builds a tracer; Start starts its exporter.
func New(cfg Config) *Tracer {
	t := &Tracer{ratio: cfg.Ratio, service: cfg.ServiceName, node: cfg.Node, sink: cfg.Sink}
	if t.service == "" {
		t.service = "hoardmq"
	}
	if cfg.Sink == nil {
		t.exp = newExporter(cfg.Export, t.service, t.node)
	}
	t.enabled.Store(true)
	return t
}

// SetEnabled turns sampling on and off; off, nothing is recorded, a sampled
// parent included.
func (t *Tracer) SetEnabled(on bool) {
	if t != nil {
		t.enabled.Store(on)
	}
}

// Node is the node the tracer's spans belong to.
func (t *Tracer) Node() string {
	if t == nil {
		return ""
	}
	return t.node
}

// sampleDecision is the head sampler: a parent decides by its sampled flag;
// with none, the trace is sampled when draw falls below ratio of the uint64
// range.
func sampleDecision(hasParent, parentSampled bool, ratio float64, draw uint64) bool {
	if hasParent {
		return parentSampled
	}
	switch {
	case ratio >= 1:
		return true
	case ratio <= 0:
		return false
	}
	return draw < uint64(math.Ldexp(ratio, 64))
}

// StartPublish begins the span for a PUBLISH received from a client: a
// child of the traceparent in its user properties when that parses, or a
// new trace, sampled at the head.
func (t *Tracer) StartPublish(props []packet.UserProperty) Span {
	if t == nil || !t.enabled.Load() {
		return Span{}
	}
	parent, ok := FromUserProperties(props)
	var draw uint64
	if !ok {
		draw = rand.Uint64()
	}
	if !sampleDecision(ok, parent.Sampled(), t.ratio, draw) {
		return Span{}
	}
	return t.start("publish", KindServer, parent, ok)
}

// StartChild begins a span under parent when parent is sampled.
func (t *Tracer) StartChild(parent SpanContext, name string, kind Kind) Span {
	if t == nil || !parent.Sampled() || !parent.Valid() || !t.enabled.Load() {
		return Span{}
	}
	return t.start(name, kind, parent, true)
}

// StartRemote begins a span under a parent received from another node as a
// traceparent; a value that does not parse, or is not sampled, starts
// nothing (a failed parse is the zero context, which StartChild refuses).
func (t *Tracer) StartRemote(traceparent string, name string, kind Kind) Span {
	if t == nil || traceparent == "" || !t.enabled.Load() {
		return Span{}
	}
	parent, _ := ParseTraceparent(traceparent)
	return t.StartChild(parent, name, kind)
}

func (t *Tracer) start(name string, kind Kind, parent SpanContext, hasParent bool) Span {
	r := &record{t: t, name: name, kind: kind, start: time.Now().UnixNano(), hasParent: hasParent}
	r.ctx.Flags = flagSampled
	if hasParent {
		r.ctx.TraceID, r.ctx.State, r.parent = parent.TraceID, parent.State, parent.SpanID
	} else {
		fillID(r.ctx.TraceID[:])
	}
	fillID(r.ctx.SpanID[:])
	r.attrs = make([]Attr, 0, 8)
	t.started.Add(1)
	return Span{r: r}
}

// fillID fills b with random bytes, never all zero.
func fillID(b []byte) {
	for {
		for i := 0; i < len(b); i += 8 {
			v := rand.Uint64()
			for j := 0; j < 8 && i+j < len(b); j++ {
				b[i+j] = byte(v >> (8 * j))
			}
		}
		for _, x := range b {
			if x != 0 {
				return
			}
		}
	}
}

// Span is a span being recorded; the zero Span records nothing and every
// method on it does nothing.
type Span struct{ r *record }

type record struct {
	t         *Tracer
	ctx       SpanContext
	parent    [8]byte
	hasParent bool
	name      string
	kind      Kind
	start     int64
	attrs     []Attr
	status    StatusCode
	statusMsg string
}

// Recording reports whether the span is sampled.
func (s Span) Recording() bool { return s.r != nil }

// Context is the span's context, the zero context when not recording.
func (s Span) Context() SpanContext {
	if s.r == nil {
		return SpanContext{}
	}
	return s.r.ctx
}

// String sets a string attribute.
func (s Span) String(key, value string) {
	if s.r != nil {
		s.r.attrs = append(s.r.attrs, Attr{Key: key, Type: AttrString, Str: value})
	}
}

// Int sets an integer attribute.
func (s Span) Int(key string, value int64) {
	if s.r != nil {
		s.r.attrs = append(s.r.attrs, Attr{Key: key, Type: AttrInt, Int: value})
	}
}

// Bool sets a boolean attribute.
func (s Span) Bool(key string, value bool) {
	if s.r != nil {
		s.r.attrs = append(s.r.attrs, Attr{Key: key, Type: AttrBool, Bool: value})
	}
}

// Error marks the span failed with a message.
func (s Span) Error(msg string) {
	if s.r != nil {
		s.r.status, s.r.statusMsg = StatusError, msg
	}
}

// End finishes the span and hands it on; it never waits for the exporter.
func (s Span) End() {
	r := s.r
	if r == nil {
		return
	}
	d := SpanData{
		Name: r.name, Kind: r.kind, TraceID: r.ctx.TraceID, SpanID: r.ctx.SpanID, TraceState: r.ctx.State,
		HasParent: r.hasParent, ParentSpanID: r.parent, Start: r.start, End: time.Now().UnixNano(),
		Attrs: r.attrs, Status: r.status, StatusMessage: r.statusMsg,
	}
	if r.t.sink != nil {
		r.t.sink(d)
		return
	}
	r.t.exp.enqueue(d)
}

// AttrType names an attribute value's type.
type AttrType uint8

// Attribute value types.
const (
	AttrString AttrType = iota
	AttrInt
	AttrBool
	AttrDouble
)

// Attr is one span attribute.
type Attr struct {
	Key    string
	Type   AttrType
	Str    string
	Int    int64
	Bool   bool
	Double float64
}

// Value renders the attribute's value as text, for tests and logs.
func (a Attr) Value() string {
	switch a.Type {
	case AttrInt:
		return strconv.FormatInt(a.Int, 10)
	case AttrBool:
		return strconv.FormatBool(a.Bool)
	case AttrDouble:
		return strconv.FormatFloat(a.Double, 'g', -1, 64)
	}
	return a.Str
}

// SpanData is an ended span.
type SpanData struct {
	Name          string
	Kind          Kind
	TraceID       [16]byte
	SpanID        [8]byte
	TraceState    string
	HasParent     bool
	ParentSpanID  [8]byte
	Start, End    int64 // Unix nanoseconds
	Attrs         []Attr
	Status        StatusCode
	StatusMessage string
}

// Attr returns the value of the attribute named key, and whether it is set.
func (d SpanData) Attr(key string) (string, bool) {
	for _, a := range d.Attrs {
		if a.Key == key {
			return a.Value(), true
		}
	}
	return "", false
}

// Stats is what the tracer and its exporter have done, read lock-free.
type Stats struct {
	Started  int64            `json:"started"`
	Exported int64            `json:"exported"`
	Dropped  map[string]int64 `json:"dropped"`
	Exports  map[string]int64 `json:"exports"`
	Queued   int              `json:"queued"`
}

// Stats reports the counters; a tracer with a sink exports nothing.
func (t *Tracer) Stats() Stats {
	if t == nil {
		return Stats{}
	}
	s := Stats{Started: t.started.Load(), Dropped: map[string]int64{}, Exports: map[string]int64{}}
	if t.exp != nil {
		t.exp.stats(&s)
	}
	return s
}
