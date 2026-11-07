// Package payloadschema is schema.json, HoardMQ's payload schema validation,
// built on the public extension API alone. It checks each client PUBLISH
// whose topic a rule's filter matches against that rule's JSON Schema, from
// the publish admission check: after authorization, the topic alias and the
// QoS 2 retransmission check, before any delivery. A violation continues,
// refuses or disconnects as the rule says, is counted, and a refusal is
// recorded in the audit log when asked.
//
// The validator is the package's own, a closed subset of JSON Schema draft
// 2020-12 that refuses any keyword it does not implement.
package payloadschema

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/happydave/hoardmq/ext"
	"github.com/happydave/hoardmq/ext/mqtt"
)

// ID is the module's identifier.
const ID ext.ID = "schema.json"

const (
	maxRules = 1000

	metricChecks       = "hoardmq_payload_schema_checks_total"
	metricRefusals     = "hoardmq_payload_schema_refusals_total"
	metricAuditDropped = "hoardmq_payload_schema_audit_dropped_total"
)

// reasons are the codes a rule may refuse with: valid on a PUBACK, a PUBREC
// and a DISCONNECT alike.
var reasons = map[int]bool{0x80: true, 0x83: true, 0x87: true, 0x90: true, 0x97: true, 0x99: true}

var ruleName = regexp.MustCompile(`^[a-z0-9_-]{1,64}$`)

func init() {
	ext.Register(ID, func() ext.Module { return &module{readFile: os.ReadFile} })
}

type ruleConfig struct {
	Name       string          `json:"name"`
	Filter     string          `json:"filter"`
	Namespace  string          `json:"namespace"`
	Schema     json.RawMessage `json:"schema"`
	SchemaFile string          `json:"schemaFile"`
	OnInvalid  string          `json:"onInvalid"`
	Reason     *int            `json:"reason"`
}

type config struct {
	Rules []ruleConfig `json:"rules"`
	Audit bool         `json:"audit"`
}

// rule is a validated rule.
type rule struct {
	name, filter, namespace string
	schema                  *Schema
	action                  ext.Action // Continue, Drop or Disconnect
	reason                  mqtt.ReasonCode
}

type module struct {
	cfg      config
	ns       string
	log      *slog.Logger
	audit    ext.Auditor
	readFile func(string) ([]byte, error)

	mu      sync.RWMutex
	rules   []rule
	metrics atomic.Pointer[metricsBox]
}

type metricsBox struct{ m ext.MetricsProvider }

func (*module) ID() ext.ID { return ID }

func (*module) Capabilities() ext.Capability { return ext.CapPublishAdmission }

func (m *module) Provision(ctx *ext.Context, raw json.RawMessage) error {
	if len(bytes.TrimSpace(raw)) == 0 {
		raw = json.RawMessage("{}")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&m.cfg); err != nil {
		return err
	}
	m.ns, m.log, m.audit = ctx.Namespace, ctx.Logger, ctx.Audit
	if m.log == nil {
		m.log = slog.Default()
	}
	return nil
}

// Validate checks the rules and compiles their schemas, reading the files
// they name.
func (m *module) Validate() error {
	rules, err := m.compile()
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.rules = rules
	m.mu.Unlock()
	return nil
}

// Reload reads the schema files again; on an error the rules held stay.
func (m *module) Reload() error { return m.Validate() }

func (m *module) compile() ([]rule, error) {
	var problems []string
	add := func(format string, a ...any) { problems = append(problems, fmt.Sprintf(format, a...)) }
	switch {
	case len(m.cfg.Rules) == 0:
		add("rules: at least one rule")
	case len(m.cfg.Rules) > maxRules:
		add("rules: at most %d", maxRules)
	}
	seen := map[string]bool{}
	var out []rule
	for i, rc := range m.cfg.Rules {
		at := fmt.Sprintf("rules/%d", i)
		r := rule{name: rc.Name, filter: rc.Filter, namespace: rc.Namespace, action: ext.Drop, reason: mqtt.PayloadFormatInvalid}
		if r.namespace == "" {
			r.namespace = m.ns
		}
		switch {
		case !ruleName.MatchString(rc.Name):
			add("%s/name: 1 to 64 of a-z, 0-9, _ and -", at)
		case seen[rc.Name]:
			add("%s/name: %q is used twice", at, rc.Name)
		}
		seen[rc.Name] = true
		if why := mqtt.ValidateTopicFilter(rc.Filter); why != "" {
			add("%s/filter: %s", at, why)
		} else if mqtt.IsSharedFilter(rc.Filter) {
			add("%s/filter: a shared filter matches no topic", at)
		}
		switch rc.OnInvalid {
		case "", "drop":
		case "disconnect":
			r.action = ext.Disconnect
		case "continue":
			r.action = ext.Continue
		default:
			add(`%s/onInvalid: "drop", "disconnect" or "continue"`, at)
		}
		if rc.Reason != nil {
			if !reasons[*rc.Reason] {
				add("%s/reason: one of 128, 131, 135, 144, 151, 153 (0x80, 0x83, 0x87, 0x90, 0x97, 0x99)", at)
			} else {
				r.reason = mqtt.ReasonCode(*rc.Reason)
			}
		}
		var src []byte
		switch {
		case len(rc.Schema) > 0 && rc.SchemaFile != "":
			add("%s: schema and schemaFile are exclusive", at)
		case len(rc.Schema) > 0:
			src = rc.Schema
		case rc.SchemaFile != "":
			data, err := m.readFile(rc.SchemaFile)
			if err != nil {
				add("%s/schemaFile: %s is unreadable", at, rc.SchemaFile)
			}
			src = data
		default:
			add("%s: schema or schemaFile is required", at)
		}
		if src != nil {
			s, err := Compile(src)
			if err != nil {
				add("%s/schema: %v", at, err)
			}
			r.schema = s
		}
		out = append(out, r)
	}
	if len(problems) > 0 {
		return nil, errors.New(strings.Join(problems, "; "))
	}
	return out, nil
}

func (m *module) Start(context.Context) error { return nil }
func (m *module) Stop(context.Context) error  { return nil }

// UseMetrics receives the node's metrics provider.
func (m *module) UseMetrics(p ext.MetricsProvider) { m.metrics.Store(&metricsBox{m: p}) }

func (m *module) count(name string, labels map[string]string) {
	if b := m.metrics.Load(); b != nil {
		b.m.Add(name, labels, 1)
	}
}

// verdict is decide's answer: the outcome, and for a violation the rule and
// what was wrong.
type verdict struct {
	out       ext.Outcome
	rule      *rule
	violation string
}

// decide is the module's decision on a publish: no matching rule admits it
// unchecked; the first matching rule checks the payload and, on a violation,
// answers its action and reason.
func decide(rules []rule, namespace, topic string, payload []byte) verdict {
	for i := range rules {
		r := &rules[i]
		if r.namespace != namespace || !mqtt.MatchTopic(r.filter, topic) {
			continue
		}
		v, err := Decode(payload)
		why := ""
		if err != nil {
			why = err.Error()
		} else if bad := r.schema.Validate(v); bad != nil {
			why = bad.Error()
		}
		if why == "" {
			return verdict{out: ext.OK, rule: r}
		}
		out := ext.Outcome{Action: r.action, Reason: r.reason}
		if r.action == ext.Continue {
			out = ext.OK
		}
		return verdict{out: out, rule: r, violation: why}
	}
	return verdict{out: ext.OK}
}

// AdmitPublish checks a client's publish against the first rule that
// matches its topic.
func (m *module) AdmitPublish(_ context.Context, info ext.ConnectionInfo, msg *mqtt.Message) ext.Outcome {
	m.mu.RLock()
	rules := m.rules
	m.mu.RUnlock()
	v := decide(rules, msg.Namespace, msg.Topic, msg.Payload)
	if v.rule == nil {
		return ext.OK
	}
	outcome := "valid"
	if v.violation != "" {
		outcome = "invalid"
	}
	m.count(metricChecks, map[string]string{"rule": v.rule.name, "outcome": outcome})
	if v.out.Action != ext.Drop && v.out.Action != ext.Disconnect {
		return v.out
	}
	m.count(metricRefusals, map[string]string{"rule": v.rule.name, "action": v.out.Action.String()})
	if m.cfg.Audit && m.audit != nil {
		detail := map[string]string{"topic": msg.Topic, "qos": strconv.Itoa(int(msg.QoS)), "violation": v.violation}
		if !m.audit.Audit(info.ClientID, "payload.invalid", v.rule.name, v.out.Reason.String(), detail) {
			m.count(metricAuditDropped, nil)
		}
	}
	return v.out
}
