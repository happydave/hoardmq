package sinkkafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/ext"
	"github.com/happydave/hoardmq/ext/mqtt"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestConfigErrorsNameTheirField(t *testing.T) {
	noFiles := func(name string) ([]byte, error) {
		if name == "/run/secret" {
			return []byte("s3cret\n"), nil
		}
		return nil, fmt.Errorf("open %s: %w", name, fs.ErrNotExist)
	}
	route := `{"filter":"a/#","topic":"t"}`
	cases := []struct {
		doc, want string
	}{
		{`{"routes":[` + route + `]}`, "brokers: at least one broker is required"},
		{`{"brokers":["k"],"routes":[` + route + `]}`, `brokers[0]: "k" is not host:port`},
		{`{"brokers":["k:9092"]}`, "routes: at least one route is required"},
		{`{"brokers":["k:9092"],"routes":[{"filter":"a/#/b","topic":"t"}]}`, "routes[0].filter: '#' must be the last level"},
		{`{"brokers":["k:9092"],"routes":[{"filter":"$share/g/a","topic":"t"}]}`, "routes[0].filter: \"$share/g/a\" is a shared subscription filter"},
		{`{"brokers":["k:9092"],"routes":[{"filter":"a","topic":"a/b"}]}`, `routes[0].topic: "a/b" is not a valid Kafka topic name`},
		{`{"brokers":["k:9092"],"routes":[{"filter":"a","topic":"` + strings.Repeat("x", 250) + `"}]}`, "routes[0].topic:"},
		{`{"brokers":["k:9092"],"routes":[{"filter":"a","topic":".."}]}`, `routes[0].topic: ".." is not`},
		{`{"brokers":["k:9092"],"routes":[{"filter":"a","topic":"t","key":"partition"}]}`, `routes[0].key: "partition" is not one of`},
		{`{"brokers":["k:9092"],"routes":[{"filter":"a","topic":"t","key":"userProperty"}]}`, "routes[0].keyProperty: is required"},
		{`{"brokers":["k:9092"],"routes":[{"filter":"a","topic":"t","keyProperty":"site"}]}`, "routes[0].keyProperty: is only for key"},
		{`{"brokers":["k:9092"],"routes":[` + route + `],"acks":"none"}`, `acks: "none" is not one of all, leader`},
		{`{"brokers":["k:9092"],"routes":[` + route + `],"compression":"brotli"}`, "compression: \"brotli\" is not one of"},
		{`{"brokers":["k:9092"],"routes":[` + route + `],"lingerMs":1001}`, "lingerMs: 1001 is outside 0–1000"},
		{`{"brokers":["k:9092"],"routes":[` + route + `],"buffer":{"maxRecords":0}}`, "buffer.maxRecords: must be at least 1"},
		{`{"brokers":["k:9092"],"routes":[` + route + `],"buffer":{"maxBytes":1048575}}`, "buffer.maxBytes: must be at least 1048576"},
		{`{"brokers":["k:9092"],"routes":[` + route + `],"buffer":{"whenFull":"block"}}`, `buffer.whenFull: "block" is not one of refuse, drop`},
		{`{"brokers":["k:9092"],"routes":[` + route + `],"buffer":{"waitMs":10001}}`, "buffer.waitMs: 10001 is outside"},
		{`{"brokers":["k:9092"],"routes":[` + route + `],"flushTimeoutMs":-1}`, "flushTimeoutMs: -1 is outside"},
		{`{"brokers":["k:9092"],"routes":[` + route + `],"tls":{"certFile":"/c.pem"}}`, "tls.certFile, tls.keyFile: give both or neither"},
		{`{"brokers":["k:9092"],"routes":[` + route + `],"tls":{"caFile":"/nope.pem"}}`, "tls.caFile: open /nope.pem"},
		{`{"brokers":["k:9092"],"routes":[` + route + `],"sasl":{"mechanism":"gssapi","username":"u","passwordFile":"/run/secret"}}`, `sasl.mechanism: "gssapi" is not one of`},
		{`{"brokers":["k:9092"],"routes":[` + route + `],"sasl":{"mechanism":"plain","username":"u","passwordFile":"/nope"}}`, "sasl.passwordFile: open /nope"},
		{`{"brokers":["k:9092"],"routes":[` + route + `],"sasl":{"mechanism":"plain","passwordFile":"/run/secret"}}`, "sasl.username: is required"},
	}
	for _, c := range cases {
		cfg, err := decode(json.RawMessage(c.doc))
		if err != nil {
			t.Errorf("%s: decode: %v", c.doc, err)
			continue
		}
		_, err = validate(cfg, "n1", "default", noFiles)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%.80s: got %v, want %q", c.doc, err, c.want)
		}
	}
	if _, err := decode(json.RawMessage(`{"brokers":["k:9092"],"routes":[],"retries":3}`)); err == nil || !strings.Contains(err.Error(), "retries") {
		t.Errorf("an unknown field: %v", err)
	}
}

func TestConfigDefaults(t *testing.T) {
	cfg, err := decode(json.RawMessage(`{"brokers":["k:9092"],"routes":[{"filter":"a/#","topic":"t"}],
		"sasl":{"mechanism":"scram-sha-512","username":"u","passwordFile":"/run/secret"}}`))
	if err != nil {
		t.Fatal(err)
	}
	s, err := validate(cfg, "n1", "default", func(string) ([]byte, error) { return []byte("s3cret\n"), nil })
	if err != nil {
		t.Fatal(err)
	}
	r := s.routes[0]
	if s.clientID != "hoardmq-n1" || !s.metadata || !s.userProperties || s.leaderAcks || s.compression != "none" ||
		s.linger != 5*time.Millisecond || s.maxRecords != 100_000 || s.maxBytes != 64<<20 || !s.refuse ||
		s.wait != time.Second || s.flush != 5*time.Second || r.key != keyTopic || r.namespace != "default" ||
		s.saslPassword != "s3cret" {
		t.Fatalf("defaults %+v", s)
	}
}

func message(topic string, qos byte) *mqtt.Message {
	return &mqtt.Message{Namespace: "default", Topic: topic, QoS: qos, Payload: []byte("p"),
		Publisher: mqtt.Publisher{Namespace: "default", ClientID: "c1", Username: "alice"}, Arrived: time.Unix(1_700_000_000, 0)}
}

func headers(rec *kgo.Record) string {
	var b strings.Builder
	for _, h := range rec.Headers {
		fmt.Fprintf(&b, "%s=%s;", h.Key, h.Value)
	}
	return b.String()
}

func TestMapping(t *testing.T) {
	cfg, _ := decode(json.RawMessage(`{"brokers":["k:9092"],"routes":[
		{"filter":"s/+/temp","topic":"temps"},
		{"filter":"s/#","topic":"temps","key":"clientId"},
		{"filter":"s/#","topic":"all","key":"userProperty","keyProperty":"site"},
		{"filter":"#","topic":"every","key":"none"},
		{"filter":"t/#","topic":"tenant","namespace":"acme"}]}`))
	s, err := validate(cfg, "n1", "default", nil)
	if err != nil {
		t.Fatal(err)
	}
	m := message("s/a/temp", 1)
	m.Retain = true
	ct, rt, pf := "application/json", "replies", byte(1)
	m.ContentType, m.ResponseTopic, m.PayloadFormat, m.CorrelationData = &ct, &rt, &pf, []byte{0, 1}
	m.UserProperties = []mqtt.UserProperty{{Name: "site", Value: "north"}, {Name: "x", Value: "1"}, {Name: "x", Value: "2"}}
	routes := s.matches(m)
	if len(routes) != 3 || routes[0].topic != "temps" || routes[0].key != keyTopic || routes[1].topic != "all" || routes[2].topic != "every" {
		t.Fatalf("routes %+v", routes)
	}
	rec := s.record(routes[0], ext.SourceClient, m)
	if string(rec.Key) != "s/a/temp" || string(rec.Value) != "p" || !rec.Timestamp.Equal(m.Arrived) {
		t.Errorf("record %+v", rec)
	}
	want := "mqtt.topic=s/a/temp;mqtt.qos=1;mqtt.source=client;mqtt.clientId=c1;mqtt.retain=true;mqtt.contentType=application/json;" +
		"mqtt.payloadFormat=1;mqtt.responseTopic=replies;mqtt.correlationData=\x00\x01;site=north;x=1;x=2;"
	if got := headers(rec); got != want {
		t.Errorf("headers\n got %q\nwant %q", got, want)
	}
	if strings.Contains(headers(rec), "alice") {
		t.Error("the username was forwarded")
	}
	if k := s.record(routes[1], ext.SourceClient, m).Key; string(k) != "north" {
		t.Errorf("user property key %q", k)
	}
	if k := s.record(routes[2], ext.SourceClient, m).Key; k != nil {
		t.Errorf("key none gave %q", k)
	}

	plain := message("s/b/x", 0)
	plain.Payload = nil
	plain.Publisher.ClientID = ""
	rs := s.matches(plain)
	if len(rs) != 3 || rs[0].topic != "temps" || rs[0].key != keyClientID {
		t.Fatalf("s/b/x routes %+v", rs)
	}
	rec = s.record(rs[0], ext.SourceModule, plain)
	if rec.Key != nil || rec.Value == nil || len(rec.Value) != 0 {
		t.Errorf("empty client id or payload: key %q value %v", rec.Key, rec.Value)
	}
	if got := headers(rec); got != "mqtt.topic=s/b/x;mqtt.qos=0;mqtt.source=module;" {
		t.Errorf("minimal headers %q", got)
	}
	if k := s.record(rs[1], ext.SourceModule, plain).Key; k != nil {
		t.Errorf("a missing user property gave key %q", k)
	}

	if rs := s.matches(message("$SYS/x", 0)); len(rs) != 0 {
		t.Errorf("$SYS matched %+v", rs)
	}
	other := message("t/1", 0)
	if rs := s.matches(other); len(rs) != 1 || rs[0].topic != "every" {
		t.Errorf("default namespace t/1: %+v", rs)
	}
	other.Namespace = "acme"
	rs = s.matches(other)
	if len(rs) != 1 || rs[0].topic != "tenant" {
		t.Fatalf("namespace acme: %+v", rs)
	}
	if got := headers(s.record(rs[0], ext.SourceWill, other)); !strings.Contains(got, "mqtt.namespace=acme;") || !strings.Contains(got, "mqtt.source=will;") {
		t.Errorf("namespace header %q", got)
	}

	s.metadata = false
	if got := headers(s.record(routes[0], ext.SourceClient, m)); got != "site=north;x=1;x=2;" {
		t.Errorf("metadata off: %q", got)
	}
	s.userProperties = false
	if got := headers(s.record(routes[0], ext.SourceClient, m)); got != "" {
		t.Errorf("both off: %q", got)
	}
}

func TestRefusesAtTheBounds(t *testing.T) {
	cases := []struct {
		name           string
		refuse         bool
		source         ext.PublishSource
		qos            byte
		matched        bool
		records, bytes int64
		want           bool
	}{
		{"QoS 1 at the record bound", true, ext.SourceClient, 1, true, 10, 0, true},
		{"QoS 2 at the record bound", true, ext.SourceClient, 2, true, 10, 0, true},
		{"at the byte bound", true, ext.SourceClient, 1, true, 0, 100, true},
		{"one below both bounds", true, ext.SourceClient, 1, true, 9, 99, false},
		{"QoS 0 at a full buffer", true, ext.SourceClient, 0, true, 10, 100, false},
		{"no route matched", true, ext.SourceClient, 1, false, 10, 100, false},
		{"drop policy", false, ext.SourceClient, 1, true, 10, 100, false},
		{"a Will", true, ext.SourceWill, 1, true, 10, 100, false},
		{"a module", true, ext.SourceModule, 1, true, 10, 100, false},
	}
	for _, c := range cases {
		if got := refuses(c.refuse, c.source, c.qos, c.matched, c.records, c.bytes, 10, 100); got != c.want {
			t.Errorf("%s: %t, want %t", c.name, got, c.want)
		}
	}
}

const testDoc = `{"brokers":["k:9092"],"routes":[{"filter":"t/#","topic":"tk"}],"flushTimeoutMs":50,`

func TestCountingUnderEachPolicy(t *testing.T) {
	t.Run("drop", func(t *testing.T) {
		s, fp, m := started(t, testDoc+`"buffer":{"maxRecords":1,"whenFull":"drop"}}`, 1)
		s.OnPublishAccepted(context.Background(), ext.SourceClient, message("t/1", 1))
		s.OnPublishAccepted(context.Background(), ext.SourceClient, message("t/2", 1))
		if got := m.get(seriesKey(metricDropped, map[string]string{"topic": "tk", "reason": reasonBufferFull})); got != 1 {
			t.Fatalf("buffer_full %v, want 1", got)
		}
		fp.finish(1, nil)
		if got := m.get(seriesKey(metricProduced, map[string]string{"topic": "tk"})); got != 1 {
			t.Fatalf("produced %v, want 1", got)
		}
		if out := s.AdmitPublish(context.Background(), ext.ConnectionInfo{}, message("t/3", 1)); out.Action != ext.Continue {
			t.Fatalf("the drop policy refused: %+v", out)
		}
	})
	t.Run("refuse, the wait expires", func(t *testing.T) {
		s, _, m := started(t, testDoc+`"buffer":{"maxRecords":1,"waitMs":50}}`, 1)
		s.OnPublishAccepted(context.Background(), ext.SourceClient, message("t/1", 1))
		start := time.Now()
		s.OnPublishAccepted(context.Background(), ext.SourceClient, message("t/2", 1))
		if waited := time.Since(start); waited < 50*time.Millisecond {
			t.Fatalf("the full buffer held the publish %v, want at least 50ms", waited)
		}
		if got := m.get(seriesKey(metricDropped, map[string]string{"topic": "tk", "reason": reasonOverflow})); got != 1 {
			t.Fatalf("overflow %v, want 1", got)
		}
		out := s.AdmitPublish(context.Background(), ext.ConnectionInfo{}, message("t/3", 1))
		if out.Action != ext.Drop || out.Reason != mqtt.QuotaExceeded {
			t.Fatalf("a full buffer admitted a QoS 1 publish: %+v", out)
		}
		if got := m.get(seriesKey(metricRefused, map[string]string{"topic": "tk"})); got != 1 {
			t.Fatalf("refused %v, want 1", got)
		}
		// QoS 0 is never refused; its forward is dropped and counted.
		if out := s.AdmitPublish(context.Background(), ext.ConnectionInfo{}, message("t/4", 0)); out.Action != ext.Continue {
			t.Fatalf("QoS 0 refused: %+v", out)
		}
		s.OnPublishAccepted(context.Background(), ext.SourceClient, message("t/4", 0))
		if got := m.get(seriesKey(metricDropped, map[string]string{"topic": "tk", "reason": reasonBufferFull})); got != 1 {
			t.Fatalf("QoS 0 buffer_full %v, want 1", got)
		}
	})
	t.Run("refuse, room comes during the wait", func(t *testing.T) {
		s, fp, m := started(t, testDoc+`"buffer":{"maxRecords":1,"waitMs":2000}}`, 1)
		s.OnPublishAccepted(context.Background(), ext.SourceClient, message("t/1", 1))
		go func() {
			time.Sleep(30 * time.Millisecond) // the room arrives while the second publish waits
			fp.finish(1, nil)
		}()
		s.OnPublishAccepted(context.Background(), ext.SourceClient, message("t/2", 1))
		if got := m.total(metricDropped); got != 0 {
			t.Fatalf("a publish that found room was dropped (%v)", got)
		}
		if n := fp.BufferedProduceRecords(); n != 1 {
			t.Fatalf("buffered %d, want the second publish", n)
		}
	})
	t.Run("rejected and produced", func(t *testing.T) {
		s, fp, m := started(t, testDoc+`"buffer":{"maxRecords":10}}`, 10)
		for i := range 3 {
			s.OnPublishAccepted(context.Background(), ext.SourceClient, message(fmt.Sprintf("t/%d", i), 1))
		}
		fp.finish(1, errFake)
		fp.finish(2, nil)
		if got := m.get(seriesKey(metricDropped, map[string]string{"topic": "tk", "reason": reasonRejected})); got != 1 {
			t.Fatalf("rejected %v, want 1", got)
		}
		if got := m.total(metricProduced) + m.total(metricDropped); got != 3 {
			t.Fatalf("outcomes counted %v for 3 records", got)
		}
	})
}

func TestStopCountsWhatItAbandons(t *testing.T) {
	s, fp, m := started(t, testDoc+`"buffer":{"maxRecords":10}}`, 10)
	for i := range 3 {
		s.OnPublishAccepted(context.Background(), ext.SourceClient, message(fmt.Sprintf("t/%d", i), 1))
	}
	if got := m.get(seriesKey(metricBufferedRecords, nil)); got != 3 {
		t.Fatalf("buffered gauge %v, want 3", got)
	}
	start := time.Now()
	if err := s.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("stop took %v", d)
	}
	if got := m.get(seriesKey(metricDropped, map[string]string{"topic": "tk", "reason": reasonShutdown})); got != 3 {
		t.Fatalf("shutdown %v, want 3", got)
	}
	if !fp.closed {
		t.Fatal("the client was not closed")
	}
	s.OnPublishAccepted(context.Background(), ext.SourceClient, message("t/9", 1))
	if got := m.get(seriesKey(metricDropped, map[string]string{"topic": "tk", "reason": reasonShutdown})); got != 4 {
		t.Fatalf("a publish after stop: shutdown %v, want 4", got)
	}
	if got := m.get(seriesKey(metricBufferedRecords, nil)); got != 0 {
		t.Fatalf("buffered gauge after stop %v", got)
	}
}

func TestOutcome(t *testing.T) {
	cases := []struct {
		err             error
		waited, stopped bool
		want            string
	}{
		{nil, false, false, ""},
		{kgo.ErrMaxBuffered, false, false, reasonBufferFull},
		{kgo.ErrMaxBuffered, true, false, reasonOverflow},
		{kgo.ErrAborting, false, false, reasonShutdown},
		{kgo.ErrClientClosed, false, false, reasonShutdown},
		{errStopped, false, false, reasonShutdown},
		{errFake, false, true, reasonShutdown},
		{errFake, false, false, reasonRejected},
		{fmt.Errorf("wrapped: %w", errors.New("x")), true, false, reasonRejected},
	}
	for _, c := range cases {
		if got := outcome(c.err, c.waited, c.stopped); got != c.want {
			t.Errorf("outcome(%v, %t, %t) = %q, want %q", c.err, c.waited, c.stopped, got, c.want)
		}
	}
}

// TestPublishDuringStopIsNotProduced: a publish accepted while the stop
// flushes is counted shutdown and never handed to the client that is
// closing.
func TestPublishDuringStopIsNotProduced(t *testing.T) {
	// The flush outlasts the test's publish by far, so the publish lands
	// while the stop is flushing.
	s, fp, m := started(t, `{"brokers":["k:9092"],"routes":[{"filter":"t/#","topic":"tk"}],"flushTimeoutMs":1000,"buffer":{"maxRecords":10}}`, 10)
	s.OnPublishAccepted(context.Background(), ext.SourceClient, message("t/1", 1))
	stopped := make(chan struct{})
	go func() { _ = s.Stop(context.Background()); close(stopped) }()
	deadline := time.Now().Add(5 * time.Second)
	for _, flushed := fp.state(); !flushed; _, flushed = fp.state() {
		if time.Now().After(deadline) {
			t.Fatal("the stop never flushed")
		}
		time.Sleep(time.Millisecond)
	}
	s.OnPublishAccepted(context.Background(), ext.SourceClient, message("t/2", 1))
	<-stopped
	if attempts, _ := fp.state(); attempts != 1 {
		t.Fatalf("produce attempts %d, want 1: the publish during the stop reached the client", attempts)
	}
	if got := m.get(seriesKey(metricDropped, map[string]string{"topic": "tk", "reason": reasonShutdown})); got != 2 {
		t.Fatalf("shutdown %v, want 2", got)
	}
}
