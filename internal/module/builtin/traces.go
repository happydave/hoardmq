package builtin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	"github.com/happydave/hoardmq/internal/module"
	"github.com/happydave/hoardmq/internal/otel"
)

func init() {
	module.Register("traces.otlp", func() module.Module { return &otlpTraces{} })
}

// otlpTraces is traces.otlp: spans for the publish path, exported as OTLP
// over HTTP in the JSON encoding. Disabled, it samples nothing.
type otlpTraces struct {
	tracer *otel.Tracer
}

type otlpConfig struct {
	Endpoint        string   `json:"endpoint"`
	SampleRatio     *float64 `json:"sampleRatio"`
	ServiceName     string   `json:"serviceName"`
	BatchSize       int      `json:"batchSize"`
	FlushIntervalMs int      `json:"flushIntervalMs"`
	QueueSize       int      `json:"queueSize"`
	TimeoutMs       int      `json:"timeoutMs"`
}

// The module's defaults and bounds.
const (
	defaultSampleRatio = 0.01
	defaultBatchSize   = 512
	defaultFlushMs     = 1000
	defaultQueueSize   = 8192
	defaultTimeoutMs   = 5000
)

func (*otlpTraces) ID() module.ID                   { return "traces.otlp" }
func (*otlpTraces) Capabilities() module.Capability { return module.CapTraces }

func (o *otlpTraces) Provision(ctx *module.Context, raw json.RawMessage) error {
	var cfg otlpConfig
	if err := decode(raw, &cfg); err != nil {
		return err
	}
	u, err := url.Parse(cfg.Endpoint)
	if cfg.Endpoint == "" || err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("endpoint must be an http or https URL, got %q", cfg.Endpoint)
	}
	ratio := defaultSampleRatio
	if cfg.SampleRatio != nil {
		ratio = *cfg.SampleRatio
	}
	if ratio < 0 || ratio > 1 {
		return fmt.Errorf("sampleRatio must be from 0 to 1, got %v", ratio)
	}
	checks := []struct {
		name           string
		v, def, lo, hi int
	}{
		{"batchSize", cfg.BatchSize, defaultBatchSize, 1, 8192},
		{"flushIntervalMs", cfg.FlushIntervalMs, defaultFlushMs, 10, 60000},
		{"queueSize", cfg.QueueSize, defaultQueueSize, 1, 1000000},
		{"timeoutMs", cfg.TimeoutMs, defaultTimeoutMs, 100, 60000},
	}
	vals := make([]int, len(checks))
	for i, c := range checks {
		v := c.v
		if v == 0 {
			v = c.def
		}
		if v < c.lo || v > c.hi {
			return fmt.Errorf("%s must be from %d to %d, got %d", c.name, c.lo, c.hi, v)
		}
		vals[i] = v
	}
	o.tracer = otel.New(otel.Config{Ratio: ratio, ServiceName: cfg.ServiceName, Node: ctx.Node, Export: otel.ExportConfig{
		Endpoint: cfg.Endpoint, BatchSize: vals[0], FlushInterval: time.Duration(vals[1]) * time.Millisecond,
		QueueSize: vals[2], Timeout: time.Duration(vals[3]) * time.Millisecond,
	}})
	return nil
}

func (o *otlpTraces) Start(context.Context) error {
	o.tracer.Start()
	return nil
}

func (o *otlpTraces) Stop(ctx context.Context) error {
	o.tracer.Stop(ctx)
	return nil
}

// Tracer implements module.TraceProvider.
func (o *otlpTraces) Tracer() *otel.Tracer { return o.tracer }

// SetEnabled implements module.Toggleable.
func (o *otlpTraces) SetEnabled(on bool) { o.tracer.SetEnabled(on) }
