package broker

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/otel/otlptest"
	"github.com/happydave/hoardmq/internal/packet"
)

// TestPublishIsTracedToARemoteDelivery (criterion 1): a publish on n1
// carrying a sampled traceparent, delivered to a subscriber on n2, exports
// n1's publish and forward spans and n2's delivery, one trace in parent order,
// that the in-process OTLP endpoint accepts without a violation; the
// subscriber receives the user properties unaltered and in order, and no
// exported byte carries the payload or a property's value.
func TestPublishIsTracedToARemoteDelivery(t *testing.T) {
	c := otlptest.New(t)
	nodes := startTrio(t, func(_ string, d *config.Document) {
		d.Modules["traces.otlp"] = json.RawMessage(`{"endpoint":"` + c.Endpoint() + `","sampleRatio":0,"flushIntervalMs":20}`)
	})
	sub, _ := nodes["n2"].connect("tcp", packet.V5, &packet.Connect{ClientID: "sub", CleanStart: true}, false)
	sub.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "x/#", QoS: 1}}})
	sub.read()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if n, _ := nodes["n1"].srv.Routes().Table().Match("default", "x/1"); len(n) == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	pub, _ := nodes["n1"].connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)

	// Unsampled: nothing is recorded on either node.
	unsampled := []packet.UserProperty{{Name: "traceparent", Value: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00"}}
	pub.send(&packet.Publish{Topic: "x/0", QoS: 1, PacketID: 1, Payload: []byte("quiet"), Properties: packet.Properties{UserProperties: unsampled}})
	pub.read()
	m0 := sub.read().(*packet.Publish)
	sub.send(&packet.Puback{Ack: packet.Ack{PacketID: m0.PacketID}})

	const clientTrace, clientSpan = "4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7"
	props := []packet.UserProperty{{Name: "a", Value: "private-value"}, {Name: "traceparent", Value: "00-" + clientTrace + "-" + clientSpan + "-01"}, {Name: "z", Value: "2"}}
	pub.send(&packet.Publish{Topic: "x/1", QoS: 1, PacketID: 2, Payload: []byte("secret-payload"), Properties: packet.Properties{UserProperties: props}})
	if a, ok := pub.read().(*packet.Puback); !ok || a.ReasonCode.IsError() {
		t.Fatalf("PUBACK %+v", a)
	}
	m, ok := sub.read().(*packet.Publish)
	if !ok || string(m.Payload) != "secret-payload" {
		t.Fatalf("delivery %+v", m)
	}
	sub.send(&packet.Puback{Ack: packet.Ack{PacketID: m.PacketID}})
	got := m.Properties.UserProperties
	if len(got) != len(props) {
		t.Fatalf("the subscriber received properties %v, want %v", got, props)
	}
	for i := range props {
		if got[i] != props[i] {
			t.Fatalf("the subscriber received properties %v, want %v unaltered and in order", got, props)
		}
	}

	byName := map[string]otlptest.Span{}
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && len(byName) < 3 {
		for _, s := range c.Spans() {
			byName[s.Name] = s
		}
		time.Sleep(20 * time.Millisecond)
	}
	if vs := c.Violations(); len(vs) != 0 {
		t.Fatalf("the endpoint found violations: %v", vs)
	}
	pubSpan, fwd, del := byName["publish"], byName["forward"], byName["deliver"]
	if pubSpan.SpanID == "" || fwd.SpanID == "" || del.SpanID == "" {
		t.Fatalf("spans received: %+v", c.Spans())
	}
	if n := len(c.Spans()); n != 3 {
		t.Errorf("%d spans exported, want 3 (the unsampled publish recorded nothing): %+v", n, c.Spans())
	}
	if pubSpan.TraceID != clientTrace || pubSpan.ParentSpanID != clientSpan || pubSpan.Kind != 2 || pubSpan.Resource["service.instance.id"] != "n1" {
		t.Errorf("publish span %+v", pubSpan)
	}
	if fwd.TraceID != clientTrace || fwd.ParentSpanID != pubSpan.SpanID || fwd.Kind != 4 || fwd.Resource["service.instance.id"] != "n1" || fwd.Attributes["hoardmq.peer"] != "n2" {
		t.Errorf("forward span %+v", fwd)
	}
	if del.TraceID != clientTrace || del.ParentSpanID != fwd.SpanID || del.Kind != 5 || del.Resource["service.instance.id"] != "n2" || del.Attributes["hoardmq.delivered"] != "1" {
		t.Errorf("deliver span %+v", del)
	}
	var all []byte
	for _, b := range c.Bodies() {
		all = append(all, b...)
	}
	if !bytes.Contains(all, []byte(`"x/1"`)) {
		t.Fatalf("the topic is not in the export, so its absence checks prove nothing: %s", all)
	}
	for _, secret := range []string{"secret-payload", "private-value"} {
		if bytes.Contains(all, []byte(secret)) {
			t.Errorf("%q reached the export", secret)
		}
	}
	// The exporter's own counters reach the scrape.
	if st := nodes["n1"].srv.registry.Tracer().Stats(); st.Started != 2 || st.Exported != 2 {
		t.Errorf("n1's tracer %+v, want 2 started and 2 exported", st)
	}
}

// TestTracingCountersReachTheScrape: the exporter's counters are read from
// /metrics.
func TestTracingCountersReachTheScrape(t *testing.T) {
	c := otlptest.New(t)
	f := start(t, func(d *config.Document) {
		metricsOn(d)
		d.Modules["traces.otlp"] = json.RawMessage(`{"endpoint":"` + c.Endpoint() + `","sampleRatio":1,"flushIntervalMs":10}`)
	})
	body := mustScrape(t, f)
	requireSeries(t, body, "hoardmq_otel_spans_started_total", func(v float64) bool { return v == 0 }, "0 before a publish")
	pub, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	pub.send(&packet.Publish{Topic: "o/1", QoS: 1, PacketID: 1, Payload: []byte("x")})
	pub.read()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && len(c.Spans()) == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	body = mustScrape(t, f)
	requireSeries(t, body, "hoardmq_otel_spans_started_total", func(v float64) bool { return v == 1 }, "1")
	requireSeries(t, body, "hoardmq_otel_spans_exported_total", func(v float64) bool { return v == 1 }, "1")
	requireSeries(t, body, `hoardmq_otel_exports_total{outcome="ok"}`, func(v float64) bool { return v == 1 }, "1")
	requireSeries(t, body, `hoardmq_otel_spans_dropped_total{reason="queue_full"}`, func(v float64) bool { return v == 0 }, "0")
	requireSeries(t, body, "hoardmq_otel_queue_depth", func(v float64) bool { return v == 0 }, "0")
}

// scrapeValue reads one series from a /metrics body; ok is false when absent.
func scrapeValue(body, series string) (float64, bool) {
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, series+" ") {
			v, err := strconv.ParseFloat(strings.TrimPrefix(line, series+" "), 64)
			return v, err == nil
		}
	}
	return 0, false
}

func mustScrape(t *testing.T, f *fixture) string {
	t.Helper()
	code, body := scrape(t, f, "/metrics")
	if code != 200 {
		t.Fatalf("scrape %d", code)
	}
	return body
}

func requireSeries(t *testing.T, body, series string, pred func(float64) bool, want string) {
	t.Helper()
	v, ok := scrapeValue(body, series)
	if !ok || !pred(v) {
		t.Errorf("%s = %v (present %v), want %s", series, v, ok, want)
	}
}
