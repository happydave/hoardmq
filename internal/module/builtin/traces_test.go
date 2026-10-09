package builtin

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/module"
	"github.com/happydave/hoardmq/internal/otel/otlptest"
	"github.com/happydave/hoardmq/internal/packet"
)

// TestTracesModuleConfiguration: the defaults, every refused value, a
// provision that opens nothing, and a disabled module that samples nothing
// even under a sampled parent.
func TestTracesModuleConfiguration(t *testing.T) {
	ctx := &module.Context{Node: "n1"}
	provision := func(raw string) error {
		return (&otlpTraces{}).Provision(ctx, json.RawMessage(raw))
	}
	if err := provision(`{"endpoint":"http://127.0.0.1:4318/v1/traces"}`); err != nil {
		t.Fatalf("minimal configuration: %v", err)
	}
	bad := map[string]string{
		`{}`: "endpoint",
		`{"endpoint":"ftp://collector/v1/traces"}`:    "endpoint",
		`{"endpoint":"http:///v1/traces"}`:            "endpoint",
		`{"endpoint":"http://c","sampleRatio":1.5}`:   "sampleRatio",
		`{"endpoint":"http://c","sampleRatio":-0.1}`:  "sampleRatio",
		`{"endpoint":"http://c","batchSize":8193}`:    "batchSize",
		`{"endpoint":"http://c","flushIntervalMs":5}`: "flushIntervalMs",
		`{"endpoint":"http://c","queueSize":-1}`:      "queueSize",
		`{"endpoint":"http://c","timeoutMs":60001}`:   "timeoutMs",
		`{"endpoint":"http://c","unknown":1}`:         "unknown",
	}
	for raw, member := range bad {
		err := provision(raw)
		if err == nil || !strings.Contains(err.Error(), member) {
			t.Errorf("%s: %v, want a problem naming %s", raw, err, member)
		}
	}

	c := otlptest.New(t)
	m := &otlpTraces{}
	if err := m.Provision(ctx, json.RawMessage(`{"endpoint":"`+c.Endpoint()+`","sampleRatio":0,"flushIntervalMs":10}`)); err != nil {
		t.Fatal(err)
	}
	if c.Requests() != 0 {
		t.Fatal("provision reached the collector")
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = m.Stop(context.Background()) }()
	parent := []packet.UserProperty{{Name: "traceparent", Value: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}}
	m.SetEnabled(false)
	if m.Tracer().StartPublish(parent).Recording() {
		t.Error("a disabled module recorded a span under a sampled parent")
	}
	m.SetEnabled(true)
	sp := m.Tracer().StartPublish(parent)
	if !sp.Recording() {
		t.Fatal("an enabled module did not follow a sampled parent")
	}
	sp.End()
	deadline := time.Now().Add(5 * time.Second)
	for len(c.Spans()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := c.Spans(); len(got) != 1 || got[0].Resource["service.instance.id"] != "n1" || got[0].Resource["service.name"] != "hoardmq" {
		t.Fatalf("exported %+v", got)
	}
}
