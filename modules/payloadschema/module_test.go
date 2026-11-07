package payloadschema

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/happydave/hoardmq/ext"
	"github.com/happydave/hoardmq/ext/mqtt"
)

type fakeMetrics struct {
	mu sync.Mutex
	v  map[string]float64
}

func (m *fakeMetrics) Add(name string, labels map[string]string, d float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.v == nil {
		m.v = map[string]float64{}
	}
	parts := []string{}
	for k, v := range labels {
		parts = append(parts, k+"="+v)
	}
	sort.Strings(parts)
	m.v[name+"{"+strings.Join(parts, ",")+"}"] += d
}
func (m *fakeMetrics) Set(string, map[string]string, float64) {}
func (m *fakeMetrics) Snapshot() []ext.Metric                 { return nil }
func (m *fakeMetrics) get(k string) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.v[k]
}

type event struct {
	actor, action, target, outcome string
	detail                         map[string]string
}

type fakeAuditor struct {
	mu     sync.Mutex
	full   bool
	events []event
}

func (a *fakeAuditor) Audit(actor, action, target, outcome string, detail map[string]string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.full {
		return false
	}
	a.events = append(a.events, event{actor, action, target, outcome, detail})
	return true
}

func provisioned(t *testing.T, doc string, audit ext.Auditor) (*module, error) {
	t.Helper()
	m := &module{readFile: os.ReadFile}
	ctx := &ext.Context{Logger: slog.New(slog.DiscardHandler), Namespace: "default", Audit: audit}
	if err := m.Provision(ctx, json.RawMessage(doc)); err != nil {
		return m, err
	}
	return m, m.Validate()
}

// TestConfiguration: good documents validate; each bad one is refused
// naming its key.
func TestConfiguration(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "s.json")
	if err := os.WriteFile(file, []byte(`{"type":"object"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	r := func(extra string) string {
		return `{"rules":[{"name":"r","filter":"a/#","schema":{"type":"object"}` + extra + `}]}`
	}
	for _, good := range []string{
		r(``),
		r(`,"onInvalid":"disconnect","reason":135,"namespace":"tenant"`),
		r(`,"onInvalid":"continue"`),
		`{"rules":[{"name":"f","filter":"b/+","schemaFile":"` + file + `"}],"audit":true}`,
	} {
		if _, err := provisioned(t, good, nil); err != nil {
			t.Errorf("%s: %v", good, err)
		}
	}
	many := make([]string, maxRules+1)
	for i := range many {
		many[i] = fmt.Sprintf(`{"name":"r%d","filter":"a","schema":true}`, i)
	}
	for _, c := range []struct{ doc, want string }{
		{`{}`, "at least one rule"},
		{`{"rules":[` + strings.Join(many, ",") + `]}`, "at most 1000"},
		{`{"rules":[{"name":"r","filter":"a","schema":true},{"name":"r","filter":"b","schema":true}]}`, `rules/1/name: "r" is used twice`},
		{`{"rules":[{"name":"R!","filter":"a","schema":true}]}`, "rules/0/name"},
		{`{"rules":[{"name":"r","filter":"$share/g/a","schema":true}]}`, "rules/0/filter: a shared filter"},
		{`{"rules":[{"name":"r","filter":"a/#/b","schema":true}]}`, "rules/0/filter"},
		{`{"rules":[{"name":"r","filter":"a","schema":true,"schemaFile":"` + file + `"}]}`, "exclusive"},
		{`{"rules":[{"name":"r","filter":"a"}]}`, "schema or schemaFile is required"},
		{`{"rules":[{"name":"r","filter":"a","schemaFile":"` + filepath.Join(dir, "absent") + `"}]}`, "rules/0/schemaFile"},
		{r(`,"onInvalid":"refuse"`), "rules/0/onInvalid"},
		{r(`,"reason":16`), "rules/0/reason"},
		{r(`,"reason":145`), "rules/0/reason"},
		{r(`,"reason":256`), "rules/0/reason"},
		{`{"rules":[{"name":"r","filter":"a","schema":{"if":true}}]}`, "rules/0/schema: /if: keyword not supported"},
		{`{"rules":[],"retries":1}`, "retries"},
	} {
		_, err := provisioned(t, c.doc, nil)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%.80s: %v, want %q", c.doc, err, c.want)
		}
	}
}

// TestDecide: the first matching rule decides; no match admits unchecked; a
// violation answers the rule's action and reason.
func TestDecide(t *testing.T) {
	m, err := provisioned(t, `{"rules":[
		{"name":"strict","filter":"s/x","schema":{"type":"object","required":["v"]},"onInvalid":"disconnect","reason":135},
		{"name":"loose","filter":"s/#","schema":{"type":"object"}},
		{"name":"note","filter":"n/#","schema":{"type":"number"},"onInvalid":"continue"},
		{"name":"other","filter":"o/#","namespace":"tenant","schema":false}]}`, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		ns, topic, payload string
		rule               string
		action             ext.Action
		reason             mqtt.ReasonCode
		invalid            bool
	}{
		{"default", "s/x", `{"v":1}`, "strict", ext.Continue, 0, false},
		{"default", "s/x", `{}`, "strict", ext.Disconnect, mqtt.NotAuthorized, true},
		{"default", "s/y", `{}`, "loose", ext.Continue, 0, false},
		{"default", "s/y", `[]`, "loose", ext.Drop, mqtt.PayloadFormatInvalid, true},
		{"default", "s/y", `{} {}`, "loose", ext.Drop, mqtt.PayloadFormatInvalid, true},
		{"default", "s/y", ``, "loose", ext.Drop, mqtt.PayloadFormatInvalid, true},
		{"default", "s/y", `not json`, "loose", ext.Drop, mqtt.PayloadFormatInvalid, true},
		{"default", "n/1", `"x"`, "note", ext.Continue, 0, true},
		{"default", "o/1", `{}`, "", ext.Continue, 0, false},
		{"tenant", "o/1", `{}`, "other", ext.Drop, mqtt.PayloadFormatInvalid, true},
		{"default", "z", `x`, "", ext.Continue, 0, false},
	} {
		v := decide(m.rules, c.ns, c.topic, []byte(c.payload))
		name := ""
		if v.rule != nil {
			name = v.rule.name
		}
		if name != c.rule || v.out.Action != c.action || (c.action != ext.Continue && v.out.Reason != c.reason) || (v.violation != "") != c.invalid {
			t.Errorf("%s %s %q: rule %q %v %v %q", c.ns, c.topic, c.payload, name, v.out.Action, v.out.Reason, v.violation)
		}
	}
}

// TestAdmitPublish: counted by rule and outcome, refusals audited with the
// topic, QoS and violation and never the payload; nothing audited when audit
// is off; a full audit queue is counted and the publish still refused.
func TestAdmitPublish(t *testing.T) {
	doc := `{"rules":[{"name":"r","filter":"t/#","schema":{"properties":{"temp":{"type":"number","maximum":100}}}},
		{"name":"c","filter":"c/#","schema":false,"onInvalid":"continue"}],"audit":true}`
	au, mx := &fakeAuditor{}, &fakeMetrics{}
	m, err := provisioned(t, doc, au)
	if err != nil {
		t.Fatal(err)
	}
	m.UseMetrics(mx)
	info := ext.ConnectionInfo{Namespace: "default", ClientID: "dev-1"}
	pub := func(topic, payload string) ext.Outcome {
		return m.AdmitPublish(context.Background(), info, &mqtt.Message{Namespace: "default", Topic: topic, QoS: 1, Payload: []byte(payload)})
	}
	if out := pub("t/1", `{"temp":20}`); out != ext.OK {
		t.Fatalf("valid: %v", out)
	}
	if out := pub("t/1", `{"temp":"secret-payload-120"}`); out.Action != ext.Drop || out.Reason != mqtt.PayloadFormatInvalid {
		t.Fatalf("invalid: %v", out)
	}
	if out := pub("c/1", `{}`); out != ext.OK {
		t.Fatalf("continue: %v", out)
	}
	for k, want := range map[string]float64{
		"hoardmq_payload_schema_checks_total{outcome=valid,rule=r}":     1,
		"hoardmq_payload_schema_checks_total{outcome=invalid,rule=r}":   1,
		"hoardmq_payload_schema_checks_total{outcome=invalid,rule=c}":   1,
		"hoardmq_payload_schema_refusals_total{action=drop,rule=r}":     1,
		"hoardmq_payload_schema_refusals_total{action=continue,rule=c}": 0,
	} {
		if got := mx.get(k); got != want {
			t.Errorf("%s = %v, want %v", k, got, want)
		}
	}
	if len(au.events) != 1 {
		t.Fatalf("audited %d events, want the one refusal", len(au.events))
	}
	e := au.events[0]
	if e.actor != "dev-1" || e.action != "payload.invalid" || e.target != "r" || e.outcome != mqtt.PayloadFormatInvalid.String() ||
		e.detail["topic"] != "t/1" || e.detail["qos"] != "1" || !strings.Contains(e.detail["violation"], "/temp") {
		t.Fatalf("event %+v", e)
	}
	for _, v := range e.detail {
		if strings.Contains(v, "secret-payload") {
			t.Fatalf("the payload reached the audit record: %+v", e)
		}
	}
	au.full = true
	if out := pub("t/1", `{"temp":101}`); out.Action != ext.Drop {
		t.Fatalf("with the audit queue full: %v", out)
	}
	if got := mx.get("hoardmq_payload_schema_audit_dropped_total{}"); got != 1 {
		t.Fatalf("audit dropped %v", got)
	}

	quiet := &fakeAuditor{}
	off, err := provisioned(t, strings.Replace(doc, `"audit":true`, `"audit":false`, 1), quiet)
	if err != nil {
		t.Fatal(err)
	}
	off.AdmitPublish(context.Background(), info, &mqtt.Message{Namespace: "default", Topic: "t/1", Payload: []byte(`{"temp":101}`)})
	if len(quiet.events) != 0 {
		t.Fatalf("audit false recorded %v", quiet.events)
	}
}

// TestReload: a schema file read again; a broken one keeps the rules held.
func TestReload(t *testing.T) {
	file := filepath.Join(t.TempDir(), "s.json")
	if err := os.WriteFile(file, []byte(`{"maximum":10}`), 0o600); err != nil {
		t.Fatal(err)
	}
	m, err := provisioned(t, `{"rules":[{"name":"r","filter":"t","schemaFile":"`+file+`"}]}`, nil)
	if err != nil {
		t.Fatal(err)
	}
	admit := func(p string) ext.Action {
		return m.AdmitPublish(context.Background(), ext.ConnectionInfo{}, &mqtt.Message{Namespace: "default", Topic: "t", Payload: []byte(p)}).Action
	}
	if admit(`20`) != ext.Drop {
		t.Fatal("20 under maximum 10 admitted")
	}
	if err := os.WriteFile(file, []byte(`{"maximum":30}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := m.Reload(); err != nil || admit(`20`) != ext.Continue {
		t.Fatalf("after reload: %v", err)
	}
	if err := os.WriteFile(file, []byte(`{"if":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := m.Reload(); err == nil || admit(`20`) != ext.Continue || admit(`40`) != ext.Drop {
		t.Fatalf("a broken file: %v; the held rules (maximum 30) must stay", err)
	}
}
