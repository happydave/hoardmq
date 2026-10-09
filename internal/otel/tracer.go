package otel

import "github.com/happydave/hoardmq/ext/trace"

// The tracer and its spans are the extension API's (ext/trace); this package
// is the OTLP exporter traces.otlp supplies it.
type (
	Kind       = trace.Kind
	StatusCode = trace.StatusCode
	Tracer     = trace.Tracer
	Span       = trace.Span
	SpanData   = trace.SpanData
	AttrType   = trace.AttrType
	Attr       = trace.Attr
	Stats      = trace.Stats
)

// Span kinds, status codes and attribute types.
const (
	KindServer   = trace.KindServer
	KindClient   = trace.KindClient
	KindProducer = trace.KindProducer
	KindConsumer = trace.KindConsumer

	StatusUnset = trace.StatusUnset
	StatusError = trace.StatusError

	AttrString = trace.AttrString
	AttrInt    = trace.AttrInt
	AttrBool   = trace.AttrBool
	AttrDouble = trace.AttrDouble
)

// Config configures a tracer exporting OTLP over HTTP.
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

// New builds a tracer whose exporter sends OTLP over HTTP; Start starts it.
func New(cfg Config) *Tracer {
	service := cfg.ServiceName
	if service == "" {
		service = "hoardmq"
	}
	tc := trace.Config{Ratio: cfg.Ratio, ServiceName: service, Node: cfg.Node, Sink: cfg.Sink}
	if cfg.Sink == nil {
		tc.Exporter = newExporter(cfg.Export, service, cfg.Node)
	}
	return trace.New(tc)
}
