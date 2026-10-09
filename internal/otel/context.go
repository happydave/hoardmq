// Package otel exports the broker's OpenTelemetry spans as OTLP over HTTP in
// the JSON encoding, batched here, without the OpenTelemetry SDK: the exporter
// traces.otlp supplies. The tracer that records the spans, and the trace
// context a PUBLISH carries, are the extension API's (ext/trace, ext/mqtt),
// named here too.
package otel

import (
	"github.com/happydave/hoardmq/ext/mqtt"
	"github.com/happydave/hoardmq/internal/packet"
)

// SpanContext is the extension API's W3C span context.
type SpanContext = mqtt.SpanContext

// ParseTraceparent reads a W3C traceparent (mqtt.ParseTraceparent).
func ParseTraceparent(s string) (SpanContext, bool) { return mqtt.ParseTraceparent(s) }

// FromUserProperties reads the trace context a PUBLISH carries
// (mqtt.FromUserProperties).
func FromUserProperties(props []packet.UserProperty) (SpanContext, bool) {
	return mqtt.FromUserProperties(props)
}
