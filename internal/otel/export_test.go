package otel_test

import (
	"context"
	"encoding/hex"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/otel"
	"github.com/happydave/hoardmq/internal/otel/otlptest"
	"github.com/happydave/hoardmq/internal/packet"
)

const tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

func exporting(t *testing.T, c *otlptest.Collector, mutate func(*otel.ExportConfig)) *otel.Tracer {
	t.Helper()
	cfg := otel.ExportConfig{Endpoint: c.Endpoint(), BatchSize: 64, QueueSize: 64, FlushInterval: 20 * time.Millisecond, Timeout: time.Second}
	if mutate != nil {
		mutate(&cfg)
	}
	tr := otel.New(otel.Config{Ratio: 1, Node: "n1", Export: cfg})
	tr.Start()
	t.Cleanup(func() { tr.Stop(context.Background()) })
	return tr
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestExportIsConformantOTLPJSON: a root span, a child with every attribute
// type and an error, and a tracestate reach the in-process collector with no
// violation, and decode to what was recorded.
func TestExportIsConformantOTLPJSON(t *testing.T) {
	c := otlptest.New(t)
	tr := exporting(t, c, nil)
	pub := tr.StartPublish([]packet.UserProperty{{Name: "traceparent", Value: tp}, {Name: "tracestate", Value: "k=v"}})
	pub.String("messaging.destination.name", "a/b")
	pub.Int("hoardmq.qos", -7)
	pub.Bool("hoardmq.retain", true)
	child := tr.StartChild(pub.Context(), "forward", otel.KindProducer)
	child.Error("send failed")
	child.End()
	pub.End()
	root := tr.StartPublish(nil)
	root.End()
	waitFor(t, "three spans", func() bool { return len(c.Spans()) >= 3 })
	if vs := c.Violations(); len(vs) != 0 {
		t.Fatalf("violations: %v\nbody: %s", vs, c.Bodies()[0])
	}
	byName := map[string][]otlptest.Span{}
	for _, s := range c.Spans() {
		byName[s.Name] = append(byName[s.Name], s)
	}
	if len(byName["publish"]) != 2 || len(byName["forward"]) != 1 {
		t.Fatalf("spans %+v", c.Spans())
	}
	var p, r otlptest.Span
	for _, s := range byName["publish"] {
		if s.ParentSpanID != "" {
			p = s
		} else {
			r = s
		}
	}
	f := byName["forward"][0]
	if p.TraceID != "4bf92f3577b34da6a3ce929d0e0e4736" || p.ParentSpanID != "00f067aa0ba902b7" || p.Kind != 2 || p.TraceState != "k=v" {
		t.Errorf("publish %+v", p)
	}
	if p.Attributes["messaging.destination.name"] != "a/b" || p.Attributes["hoardmq.qos"] != "-7" || p.Attributes["hoardmq.retain"] != "true" {
		t.Errorf("publish attributes %v", p.Attributes)
	}
	if f.TraceID != p.TraceID || f.ParentSpanID != p.SpanID || f.Kind != 4 || f.StatusCode != 2 || f.StatusMessage != "send failed" {
		t.Errorf("forward %+v", f)
	}
	if r.ParentSpanID != "" || r.TraceID == p.TraceID || r.StatusCode != 0 {
		t.Errorf("root %+v", r)
	}
	if p.Resource["service.name"] != "hoardmq" || p.Resource["service.instance.id"] != "n1" || p.Scope != "github.com/happydave/hoardmq" {
		t.Errorf("resource %v scope %q", p.Resource, p.Scope)
	}
	if p.End < p.Start || p.Start == 0 {
		t.Errorf("times %d..%d", p.Start, p.End)
	}
	if _, err := hex.DecodeString(p.SpanID); err != nil || len(p.SpanID) != 16 {
		t.Errorf("span id %q", p.SpanID)
	}
}

// TestExporterRetriesRetryableStatus: a 503 is retried and the batch
// delivered once; a 400 is not retried.
func TestExporterRetriesRetryableStatus(t *testing.T) {
	c := otlptest.New(t)
	c.Respond(func(n int) (int, http.Header, string) {
		if n == 1 {
			return http.StatusServiceUnavailable, nil, `{}`
		}
		return http.StatusOK, nil, `{}`
	})
	tr := exporting(t, c, nil)
	tr.StartPublish(nil).End()
	waitFor(t, "the span delivered", func() bool { return len(c.Spans()) == 1 })
	waitFor(t, "the export counted", func() bool { return tr.Stats().Exports["ok"] == 1 })
	st := tr.Stats()
	if c.Requests() != 2 || st.Exports["retried"] != 1 || st.Exported != 1 {
		t.Errorf("requests %d, stats %+v", c.Requests(), st)
	}

	bad := otlptest.New(t)
	bad.Respond(func(int) (int, http.Header, string) { return http.StatusBadRequest, nil, `{}` })
	tr2 := exporting(t, bad, nil)
	tr2.StartPublish(nil).End()
	waitFor(t, "the failure counted", func() bool { return tr2.Stats().Exports["failed"] == 1 })
	time.Sleep(200 * time.Millisecond) // longer than two retry backoffs: a retry would have arrived
	if n := bad.Requests(); n != 1 {
		t.Errorf("a 400 was sent %d times, want 1", n)
	}
	if st := tr2.Stats(); st.Dropped["export_failed"] != 1 || st.Exports["retried"] != 0 {
		t.Errorf("stats %+v", st)
	}
}

// TestPartialSuccessCountsRejectedSpans: a 200 naming rejected spans counts
// them dropped and does not retry.
func TestPartialSuccessCountsRejectedSpans(t *testing.T) {
	c := otlptest.New(t)
	c.Respond(func(int) (int, http.Header, string) {
		return http.StatusOK, nil, `{"partialSuccess":{"rejectedSpans":"2","errorMessage":"too old"}}`
	})
	tr := exporting(t, c, func(e *otel.ExportConfig) { e.FlushInterval = time.Hour; e.BatchSize = 3 })
	for i := 0; i < 3; i++ {
		tr.StartPublish(nil).End()
	}
	waitFor(t, "the export", func() bool { return tr.Stats().Exports["ok"] == 1 })
	if st := tr.Stats(); st.Exported != 1 || st.Dropped["rejected"] != 2 || c.Requests() != 1 {
		t.Errorf("stats %+v, requests %d", st, c.Requests())
	}
}

// TestFullQueueDropsWithoutBlocking: with the collector holding every
// request, ending more spans than the queue holds returns at once and counts
// the excess.
func TestFullQueueDropsWithoutBlocking(t *testing.T) {
	c := otlptest.New(t)
	c.Hold()
	tr := exporting(t, c, func(e *otel.ExportConfig) { e.QueueSize = 8; e.BatchSize = 1; e.FlushInterval = time.Hour })
	tr.StartPublish(nil).End()
	waitFor(t, "the first request held", func() bool { return tr.Stats().Queued == 0 })
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 8+10; i++ {
			tr.StartPublish(nil).End()
		}
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		c.Release()
		t.Fatal("ending spans waited for the exporter")
	}
	if st := tr.Stats(); st.Dropped["queue_full"] != 10 {
		t.Errorf("dropped %v, want 10 queue_full", st.Dropped)
	}
	c.Release()
}

// TestStopFlushesQueuedSpans: spans ended just before Stop are sent.
func TestStopFlushesQueuedSpans(t *testing.T) {
	c := otlptest.New(t)
	cfg := otel.ExportConfig{Endpoint: c.Endpoint(), BatchSize: 100, QueueSize: 100, FlushInterval: time.Hour, Timeout: time.Second}
	tr := otel.New(otel.Config{Ratio: 1, Node: "n1", Export: cfg})
	tr.Start()
	for i := 0; i < 5; i++ {
		tr.StartPublish(nil).End()
	}
	tr.Stop(context.Background())
	if n := len(c.Spans()); n != 5 {
		t.Fatalf("%d spans reached the collector after Stop, want 5", n)
	}
	tr.StartPublish(nil).End()
	if st := tr.Stats(); st.Dropped["shutdown"] != 1 {
		t.Errorf("a span ended after Stop: dropped %v", st.Dropped)
	}
}

// TestExportCarriesOnlyAttributes: the encoder carries what a span was given
// and nothing of the publish it describes.
func TestExportCarriesOnlyAttributes(t *testing.T) {
	c := otlptest.New(t)
	tr := exporting(t, c, nil)
	sp := tr.StartPublish([]packet.UserProperty{{Name: "secret", Value: "hunter2"}})
	sp.String("messaging.destination.name", "visible/topic")
	sp.End()
	waitFor(t, "the span", func() bool { return len(c.Spans()) == 1 })
	body := string(c.Bodies()[0])
	if !strings.Contains(body, "visible/topic") {
		t.Fatalf("the topic attribute is not in the body: %s", body)
	}
	if strings.Contains(body, "hunter2") || strings.Contains(body, "secret") {
		t.Errorf("a user property reached the export: %s", body)
	}
}
