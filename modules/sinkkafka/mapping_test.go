package sinkkafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/happydave/hoardmq/ext"
	"github.com/happydave/hoardmq/ext/mqtt"
	"github.com/twmb/franz-go/pkg/kgo"
)

func settingsOf(t *testing.T, doc string) *settings {
	t.Helper()
	cfg, err := decode(json.RawMessage(doc))
	if err != nil {
		t.Fatal(err)
	}
	s, err := validate(cfg, "n1", "default", nil)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// documented is the record docs/modules.md's mapping gives, built field by field
// from the message, for comparison with the module's.
func documented(m *mqtt.Message, source ext.PublishSource, topic string, key []byte) string {
	h := fmt.Sprintf("mqtt.topic=%s;mqtt.qos=%d;mqtt.source=%s;", m.Topic, m.QoS, source)
	if m.Publisher.ClientID != "" {
		h += "mqtt.clientId=" + m.Publisher.ClientID + ";"
	}
	if m.Retain {
		h += "mqtt.retain=true;"
	}
	if m.ContentType != nil {
		h += "mqtt.contentType=" + *m.ContentType + ";"
	}
	if m.PayloadFormat != nil {
		h += "mqtt.payloadFormat=" + strconv.Itoa(int(*m.PayloadFormat)) + ";"
	}
	if m.ResponseTopic != nil {
		h += "mqtt.responseTopic=" + *m.ResponseTopic + ";"
	}
	if m.CorrelationData != nil {
		h += "mqtt.correlationData=" + string(m.CorrelationData) + ";"
	}
	if m.Namespace != "default" {
		h += "mqtt.namespace=" + m.Namespace + ";"
	}
	for _, p := range m.UserProperties {
		h += p.Name + "=" + p.Value + ";"
	}
	return fmt.Sprintf("%s|%q|%s|%d|%s", topic, key, m.Payload, m.Arrived.UnixMilli(), h)
}

func rendered(rec *kgo.Record) string {
	return fmt.Sprintf("%s|%q|%s|%d|%s", rec.Topic, rec.Key, rec.Value, rec.Timestamp.UnixMilli(), headers(rec))
}

// TestDefaultsMapAsBefore: without the new keys, and with them at their
// defaults, every key source gives the documented record, whole.
func TestDefaultsMapAsBefore(t *testing.T) {
	routes := `"routes":[{"filter":"a/#","topic":"t1"},{"filter":"a/#","topic":"t2","key":"clientId"},
		{"filter":"a/#","topic":"t3","key":"userProperty","keyProperty":"site"},{"filter":"a/#","topic":"t4","key":"none"},
		{"filter":"a/#","topic":"t5","namespace":"acme"}]`
	m := message("a/b", 1)
	m.Retain = true
	ct, rt, pf := "text/plain", "r/1", byte(1)
	m.ContentType, m.ResponseTopic, m.PayloadFormat, m.CorrelationData = &ct, &rt, &pf, []byte("cd")
	m.UserProperties = []mqtt.UserProperty{{Name: "site", Value: "north"}, {Name: "x", Value: "1"}}
	keys := map[string][]byte{"t1": []byte("a/b"), "t2": []byte("c1"), "t3": []byte("north"), "t4": nil}
	for _, doc := range []string{
		`{"brokers":["k:9092"],` + routes + `}`,
		`{"brokers":["k:9092"],` + routes + `,"headers":{"metadata":true,"userProperties":true,"prefix":"mqtt.","userPropertyPrefix":""}}`,
	} {
		s := settingsOf(t, doc)
		got := s.matches(m)
		if len(got) != 4 {
			t.Fatalf("%d routes", len(got))
		}
		for _, r := range got {
			if want, rec := documented(m, ext.SourceClient, r.topic, keys[r.topic]), rendered(s.record(r, ext.SourceClient, m)); rec != want {
				t.Errorf("%s:\n got %s\nwant %s", r.topic, rec, want)
			}
		}
		acme := message("a/b", 0)
		acme.Namespace = "acme"
		r := s.matches(acme)[0]
		if want, rec := documented(acme, ext.SourceWill, "t5", []byte("a/b")), rendered(s.record(r, ext.SourceWill, acme)); rec != want {
			t.Errorf("another namespace:\n got %s\nwant %s", rec, want)
		}
	}
}

// TestSegmentKey: a level of the topic, the fallback when it has none, an
// empty level as an empty key.
func TestSegmentKey(t *testing.T) {
	s := settingsOf(t, `{"brokers":["k:9092"],"routes":[
		{"filter":"devices/+/telemetry","topic":"fixed","key":{"segment":1}},
		{"filter":"devices/#","topic":"open","key":{"segment":2}},
		{"filter":"devices/#","topic":"byclient","key":{"segment":2,"fallback":"clientId"}},
		{"filter":"devices/#","topic":"nokey","key":{"segment":2,"fallback":"none"}},
		{"filter":"#","topic":"first","key":{"segment":0}}]}`)
	keyOf := func(topic, kafka string) ([]byte, bool) {
		m := message(topic, 0)
		for _, r := range s.matches(m) {
			if r.topic == kafka {
				return s.record(r, ext.SourceClient, m).Key, fellBack(r, m)
			}
		}
		t.Fatalf("%s matched no route to %s", topic, kafka)
		return nil, false
	}
	for _, c := range []struct {
		topic, kafka string
		key          []byte
		fell         bool
	}{
		{"devices/d7/telemetry", "fixed", []byte("d7"), false},
		{"devices/d7/x", "open", []byte("x"), false},
		{"devices/d7", "open", []byte("devices/d7"), true},
		{"devices/d7", "byclient", []byte("c1"), true},
		{"devices/d7", "nokey", nil, true},
		{"devices//", "open", []byte{}, false},
		{"devices/d7/x", "first", []byte("devices"), false},
	} {
		key, fell := keyOf(c.topic, c.kafka)
		if string(key) != string(c.key) || (key == nil) != (c.key == nil) || fell != c.fell {
			t.Errorf("%s → %s: key %q (nil %v), fell back %v; want %q (nil %v), %v", c.topic, c.kafka, key, key == nil, fell, c.key, c.key == nil, c.fell)
		}
	}
}

// TestKeyAndHeaderRefusals: each refusal names its field.
func TestKeyAndHeaderRefusals(t *testing.T) {
	many := make([]string, 33)
	for i := range many {
		many[i] = fmt.Sprintf(`"h%d":"v"`, i)
	}
	for _, c := range []struct{ route, extra, want string }{
		{`{"filter":"a/+","topic":"t","key":{"segment":2}}`, ``, "routes[0].key.segment: 2 is past the topic's last level"},
		{`{"filter":"a/#","topic":"t","key":{"segment":-1}}`, ``, "routes[0].key.segment: -1 is outside"},
		{`{"filter":"a/#","topic":"t","key":{"segment":128}}`, ``, "routes[0].key.segment: 128 is outside"},
		{`{"filter":"a/#","topic":"t","key":{"segment":1,"fallback":"userProperty"}}`, ``, "routes[0].key.fallback"},
		{`{"filter":"a/#","topic":"t","key":{"segment":1},"keyProperty":"site"}`, ``, "routes[0].keyProperty"},
		{`{"filter":"a/#","topic":"t","key":{"fallback":"none"}}`, ``, "routes[0].key.segment: is required"},
		{`{"filter":"a/#","topic":"t","key":"segment"}`, ``, "routes[0].key.segment: is required"},
		{`{"filter":"a/#","topic":"t","key":"partition"}`, ``, "routes[0].key"},
		{`{"filter":"a/#","topic":"t","headers":{` + strings.Join(many, ",") + `}}`, ``, "routes[0].headers: at most 32"},
		{`{"filter":"a/#","topic":"t","headers":{"":"v"}}`, ``, "routes[0].headers: a name"},
		{`{"filter":"a/#","topic":"t","headers":{"h":"` + strings.Repeat("v", 1025) + `"}}`, ``, "routes[0].headers.h"},
		{`{"filter":"a/#","topic":"t"}`, `,"headers":{"prefix":"` + strings.Repeat("p", 65) + `"}`, "headers.prefix"},
		{`{"filter":"a/#","topic":"t"}`, `,"headers":{"userPropertyPrefix":"` + strings.Repeat("p", 65) + `"}`, "headers.userPropertyPrefix"},
	} {
		cfg, err := decode(json.RawMessage(`{"brokers":["k:9092"],"routes":[` + c.route + `]` + c.extra + `}`))
		if err == nil {
			_, err = validate(cfg, "n1", "default", nil)
		}
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%.70s: %v, want %q", c.route+c.extra, err, c.want)
		}
	}
	if _, err := decode(json.RawMessage(`{"brokers":["k:9092"],"routes":[{"filter":"a","topic":"t","key":{"segment":0,"level":1}}]}`)); err == nil || !strings.Contains(err.Error(), "level") {
		t.Errorf("an unknown member of the key: %v", err)
	}
}

// TestPrefixesAndStaticHeaders: the prefixes rename, static headers follow
// in name order, with metadata off too, and no option forwards the username.
func TestPrefixesAndStaticHeaders(t *testing.T) {
	m := message("a/b", 1)
	m.UserProperties = []mqtt.UserProperty{{Name: "site", Value: "north"}}
	for _, c := range []struct{ headersCfg, want string }{
		{`{"prefix":"x-"}`, "x-topic=a/b;x-qos=1;x-source=client;x-clientId=c1;site=north;env=prod;zone=b;"},
		{`{"prefix":""}`, "topic=a/b;qos=1;source=client;clientId=c1;site=north;env=prod;zone=b;"},
		{`{"userPropertyPrefix":"p."}`, "mqtt.topic=a/b;mqtt.qos=1;mqtt.source=client;mqtt.clientId=c1;p.site=north;env=prod;zone=b;"},
		{`{"metadata":false,"userProperties":false}`, "env=prod;zone=b;"},
	} {
		s := settingsOf(t, `{"brokers":["k:9092"],"routes":[{"filter":"a/#","topic":"t","headers":{"zone":"b","env":"prod"}}],"headers":`+c.headersCfg+`}`)
		rec := s.record(s.matches(m)[0], ext.SourceClient, m)
		if got := headers(rec); got != c.want {
			t.Errorf("%s:\n got %q\nwant %q", c.headersCfg, got, c.want)
		}
		if strings.Contains(headers(rec), "alice") {
			t.Errorf("%s: the username was forwarded", c.headersCfg)
		}
	}
}

// TestKeyFallbacksCounted: each fallback is counted by the Kafka topic.
func TestKeyFallbacksCounted(t *testing.T) {
	s, _, m := started(t, `{"brokers":["k:9092"],"routes":[{"filter":"d/#","topic":"tk","key":{"segment":2}}]}`, 10)
	for _, topic := range []string{"d/1/x", "d/1", "d"} {
		s.OnPublishAccepted(context.Background(), ext.SourceClient, message(topic, 0))
	}
	if got := m.get("hoardmq_sink_kafka_key_fallbacks_total{topic=tk}"); got != 2 {
		t.Fatalf("fallbacks %v, want 2", got)
	}
}
