// Package testmod is a module that declares every hook family and records
// what it sees. Tests register it and steer its outcomes through the
// exported Control; it is not part of the default binary.
package testmod

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/module"
	"github.com/happydave/hoardmq/internal/packet"
)

// ID is the test module's identifier.
const ID module.ID = "hook.test"

// Control steers the module's behaviour from a test. Fields are read under
// the mutex on every call.
type Control struct {
	mu sync.Mutex
	// InboundOutcome is returned for every inbound PUBLISH whose payload
	// equals Trigger; other packets continue. Modify rewrites the payload
	// to Rewrite.
	Trigger        string
	InboundOutcome module.Outcome
	Rewrite        string
	// OutboundDrop drops every outbound PUBLISH whose payload equals it.
	OutboundDrop string
	// RefuseFilter is refused on subscribe with reason 0x8F; LowerQoS lowers
	// every other filter's QoS to 0.
	RefuseFilter string
	LowerQoS     bool
	// Sleep delays every hook call, to exercise the timeout.
	Sleep time.Duration
	// Panic makes every inbound hook call panic.
	Panic bool

	Calls []string
}

func (c *Control) record(s string) {
	c.mu.Lock()
	c.Calls = append(c.Calls, s)
	c.mu.Unlock()
}

// Set changes the control under its lock, so a test never races the hooks.
func (c *Control) Set(f func(*Control)) {
	c.mu.Lock()
	f(c)
	c.mu.Unlock()
}

// Recorded returns a copy of the calls.
func (c *Control) Recorded() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.Calls))
	copy(out, c.Calls)
	return out
}

// Reset clears the calls.
func (c *Control) Reset() {
	c.mu.Lock()
	c.Calls = nil
	c.mu.Unlock()
}

// Shared is the control every instance reads, so tests can steer the module
// the registry instantiated.
var Shared = &Control{}

// Register makes the module available; safe to call once per process.
var registerOnce sync.Once

func Register() {
	registerOnce.Do(func() {
		module.Register(ID, func() module.Module { return &mod{c: Shared} })
	})
}

type mod struct {
	c       *Control
	enabled bool
	started bool
}

func (*mod) ID() module.ID { return ID }
func (*mod) Capabilities() module.Capability {
	return module.CapConnection | module.CapAuthentication | module.CapAuthorization | module.CapInbound |
		module.CapOutbound | module.CapSubscribe | module.CapDelivery | module.CapRetained | module.CapWill
}
func (m *mod) Provision(_ *module.Context, raw json.RawMessage) error {
	var cfg struct {
		Fail bool `json:"fail"`
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return err
	}
	if cfg.Fail {
		return fmt.Errorf("configured to fail provisioning")
	}
	m.enabled = true
	return nil
}
func (m *mod) Start(context.Context) error { m.started = true; m.c.record("start"); return nil }
func (m *mod) Stop(context.Context) error  { m.started = false; m.c.record("stop"); return nil }
func (m *mod) SetEnabled(b bool)           { m.enabled = b }

func (m *mod) pause() {
	m.c.mu.Lock()
	d := m.c.Sleep
	panics := m.c.Panic
	m.c.mu.Unlock()
	if d > 0 {
		time.Sleep(d)
	}
	if panics {
		panic("testmod: configured panic")
	}
}

func (m *mod) Authenticate(context.Context, *auth.Request) auth.Result {
	m.c.record("authenticate")
	return auth.Result{Decision: auth.Pass}
}

func (m *mod) Authorize(context.Context, auth.Action, string, *auth.Identity) auth.Decision {
	m.c.record("authorize")
	return auth.Pass
}

func (m *mod) Connected(_ context.Context, info module.ConnectionInfo) {
	m.c.record("connected " + info.ClientID)
}

func (m *mod) Disconnected(_ context.Context, info module.ConnectionInfo, reason packet.ReasonCode, graceful bool) {
	m.c.record(fmt.Sprintf("disconnected %s %s graceful=%v", info.ClientID, reason, graceful))
}

func (m *mod) InterceptInbound(_ context.Context, info module.ConnectionInfo, p packet.Packet) module.Outcome {
	m.c.record("inbound " + p.Type().String())
	m.pause()
	pub, ok := p.(*packet.Publish)
	if !ok {
		return module.OK
	}
	m.c.mu.Lock()
	defer m.c.mu.Unlock()
	if m.c.Trigger == "" || string(pub.Payload) != m.c.Trigger {
		return module.OK
	}
	if m.c.InboundOutcome.Action == module.Modify {
		pub.Payload = []byte(m.c.Rewrite)
	}
	return m.c.InboundOutcome
}

func (m *mod) InterceptOutbound(_ context.Context, info module.ConnectionInfo, p packet.Packet) module.Outcome {
	m.c.record("outbound " + p.Type().String())
	m.c.mu.Lock()
	defer m.c.mu.Unlock()
	if pub, ok := p.(*packet.Publish); ok && m.c.OutboundDrop != "" && string(pub.Payload) == m.c.OutboundDrop {
		return module.Outcome{Action: module.Drop}
	}
	return module.OK
}

func (m *mod) InterceptSubscribe(_ context.Context, info module.ConnectionInfo, sub *packet.Subscription) module.Outcome {
	m.c.record("subscribe " + sub.Filter)
	m.c.mu.Lock()
	defer m.c.mu.Unlock()
	if m.c.RefuseFilter != "" && sub.Filter == m.c.RefuseFilter {
		return module.Outcome{Action: module.Drop, Reason: packet.TopicFilterInvalid}
	}
	if m.c.LowerQoS && sub.QoS > 0 {
		sub.QoS = 0
		return module.Outcome{Action: module.Modify}
	}
	return module.OK
}

func (m *mod) OnDelivery(_ context.Context, event module.DeliveryEvent, ns, id string, msg *message.Message, qos byte) {
	m.c.record(fmt.Sprintf("delivery %s %s %s qos=%d", event, id, msg.Payload, qos))
}

func (m *mod) OnRetained(_ context.Context, msg *message.Message) {
	m.c.record(fmt.Sprintf("retained %s %d", msg.Topic, len(msg.Payload)))
}

func (m *mod) OnWill(_ context.Context, ns, id string, msg *message.Message) {
	m.c.record("will " + id + " " + msg.Topic)
}
