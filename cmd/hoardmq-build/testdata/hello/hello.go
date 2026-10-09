// Package hello is a worked example of a HoardMQ module built outside the
// broker's tree: hook.hello records its lifecycle and the connections and
// publishes it sees in a file, and counts publishes in the node's metrics.
// Build it into a binary with
//
//	hoardmq-build --with example.com/hoardmq-hello=<this directory>
//
// and configure it as {"modules": {"hook.hello": {"log": "<file>"}}}.
package hello

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/happydave/hoardmq/ext"
	"github.com/happydave/hoardmq/ext/mqtt"
)

func init() {
	ext.Register("hook.hello", func() ext.Module { return &hello{} })
}

type config struct {
	Log string `json:"log"`
}

type hello struct {
	mu      sync.Mutex
	cfg     config
	steps   []string // lifecycle steps before the log is open
	log     *os.File
	metrics ext.MetricsProvider
}

func (*hello) ID() ext.ID { return "hook.hello" }

func (*hello) Capabilities() ext.Capability {
	return ext.CapConnection | ext.CapInbound | ext.CapDelivery
}

// Provision parses the configuration and writes nothing: hoardmq check runs
// it.
func (h *hello) Provision(_ *ext.Context, raw json.RawMessage) error {
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&h.cfg); err != nil {
		return err
	}
	h.steps = append(h.steps, "provision")
	return nil
}

// Validate refuses a configuration without a log file.
func (h *hello) Validate() error {
	if h.cfg.Log == "" {
		return errors.New("log is required")
	}
	h.steps = append(h.steps, "validate")
	return nil
}

// Start opens the log and writes the steps that came before it.
func (h *hello) Start(context.Context) error {
	f, err := os.OpenFile(h.cfg.Log, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	h.mu.Lock()
	h.log = f
	h.mu.Unlock()
	for _, s := range h.steps {
		h.write(s)
	}
	h.write("start")
	return nil
}

func (h *hello) Stop(context.Context) error {
	h.write("stop")
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.log == nil {
		return nil
	}
	err := h.log.Close()
	h.log = nil
	return err
}

func (h *hello) write(line string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.log != nil {
		_, _ = fmt.Fprintln(h.log, line)
	}
}

// SetEnabled makes the module hot-toggleable; while it is disabled the broker
// calls none of its hooks.
func (h *hello) SetEnabled(on bool) { h.write(fmt.Sprintf("enabled %t", on)) }

// UseMetrics receives the node's metrics provider.
func (h *hello) UseMetrics(m ext.MetricsProvider) {
	h.mu.Lock()
	h.metrics = m
	h.mu.Unlock()
}

func (h *hello) Connected(_ context.Context, info ext.ConnectionInfo) {
	h.write("connected " + info.ClientID)
}

func (h *hello) Disconnected(context.Context, ext.ConnectionInfo, mqtt.ReasonCode, bool) {}

func (h *hello) InterceptInbound(_ context.Context, _ ext.ConnectionInfo, p mqtt.Packet) ext.Outcome {
	if pub, ok := p.(*mqtt.Publish); ok {
		h.write("publish " + pub.Topic)
		h.mu.Lock()
		m := h.metrics
		h.mu.Unlock()
		if m != nil {
			m.Add("hoardmq_hello_publishes_total", nil, 1)
		}
	}
	return ext.OK
}

func (h *hello) OnDelivery(context.Context, ext.DeliveryEvent, string, string, *mqtt.Message, byte) {}
