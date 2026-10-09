package sinkkafka

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// The live test runs the default binary against a real Kafka. It runs only
// when SINKKAFKA_LIVE_BROKERS names the Kafka (host:port) and
// SINKKAFKA_LIVE_CONTAINER the podman container it runs in, which the test
// pauses, stops and starts; the procedure is in the README's Kafka forwarder
// section. The names avoid the HOARDMQ_ prefix, which the broker it starts
// would read as configuration overrides and refuse.
func liveKafka(t *testing.T) (brokers, container string) {
	t.Helper()
	brokers, container = os.Getenv("SINKKAFKA_LIVE_BROKERS"), os.Getenv("SINKKAFKA_LIVE_CONTAINER")
	if brokers == "" || container == "" {
		t.Skip("SINKKAFKA_LIVE_BROKERS and SINKKAFKA_LIVE_CONTAINER are not set: no live Kafka for this run")
	}
	return brokers, container
}

// buildBroker builds the default binary from the checkout under test: the
// shipped binary is what is under test.
func buildBroker(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(t.TempDir(), "hoardmq")
	cmd := exec.Command("go", "build", "-trimpath", "-o", bin, "./cmd/hoardmq")
	cmd.Dir = root
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	return bin
}

func podman(t *testing.T, args ...string) {
	t.Helper()
	if out, err := exec.Command("podman", args...).CombinedOutput(); err != nil {
		t.Fatalf("podman %v: %v\n%s", args, err, out)
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

// node is one broker process started by the test.
type node struct {
	t       *testing.T
	pid     int
	addr    string
	metrics string
	mu      sync.Mutex
	lines   []string
	exited  chan struct{}
	waitErr error
}

func (n *node) log() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return strings.Join(n.lines, "\n")
}

// serve starts the broker with sink.kafka configured as given, and waits
// for its MQTT listener.
func serve(t *testing.T, bin string, sinkCfg map[string]any) *node {
	t.Helper()
	dir := t.TempDir()
	acl := filepath.Join(dir, "acl.json")
	if err := os.WriteFile(acl, []byte(`{"rules":[{"topic":"#","allow":["publish","subscribe","unsubscribe"]}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	metricsAddr := freeAddr(t)
	doc := map[string]any{
		"version":       1,
		"node":          map[string]any{"dataDir": filepath.Join(dir, "data")},
		"listeners":     []any{map[string]any{"name": "t", "type": "tcp", "address": "127.0.0.1:0"}},
		"auth":          map[string]any{"anonymous": true},
		"observability": map[string]any{"address": metricsAddr},
		"modules":       map[string]any{"sink.kafka": sinkCfg, "authz.file": map[string]any{"path": acl}, "metrics.prometheus": map[string]any{}},
		"log":           map[string]any{"format": "json"},
	}
	data, _ := json.Marshal(doc)
	path := filepath.Join(dir, "hoardmq.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin, "serve", "-config", path)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdout = io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	n := &node{t: t, pid: cmd.Process.Pid, metrics: "http://" + metricsAddr + "/metrics", exited: make(chan struct{})}
	addr := make(chan string, 1)
	readDone := make(chan struct{})
	go func() {
		sc := bufio.NewScanner(stderr)
		sc.Buffer(make([]byte, 64<<10), 1<<20)
		for sc.Scan() {
			line := sc.Text()
			n.mu.Lock()
			n.lines = append(n.lines, line)
			n.mu.Unlock()
			var rec struct {
				Msg     string `json:"msg"`
				Address string `json:"address"`
			}
			if json.Unmarshal([]byte(line), &rec) == nil && rec.Msg == "listening" && rec.Address != "" {
				select {
				case addr <- rec.Address:
				default:
				}
			}
		}
		close(readDone)
	}()
	go func() { <-readDone; n.waitErr = cmd.Wait(); close(n.exited) }()
	t.Cleanup(func() {
		select {
		case <-n.exited:
		default:
			_ = syscall.Kill(n.pid, syscall.SIGKILL)
			<-n.exited
		}
	})
	select {
	case n.addr = <-addr:
	case <-n.exited:
		t.Fatalf("serve exited: %v\n%s", n.waitErr, n.log())
	case <-time.After(60 * time.Second):
		t.Fatalf("no listening line:\n%s", n.log())
	}
	return n
}

// metric reads one series from /metrics; labels in the exposition's own
// order and quoting, or "" for none.
func (n *node) metric(name, labels string) float64 {
	n.t.Helper()
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Get(n.metrics)
	if err != nil {
		n.t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	want := name
	if labels != "" {
		want += "{" + labels + "}"
	}
	for _, line := range strings.Split(string(body), "\n") {
		if v, ok := strings.CutPrefix(line, want+" "); ok {
			f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
			if err != nil {
				n.t.Fatalf("%s: %v", line, err)
			}
			return f
		}
	}
	return 0
}

func (n *node) waitMetric(name, labels string, atLeast float64) {
	n.t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for n.metric(name, labels) < atLeast {
		if time.Now().After(deadline) {
			n.t.Fatalf("%s{%s} = %v, want at least %v", name, labels, n.metric(name, labels), atLeast)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// mqttClient is a minimal MQTT 5 client over the broker's codec.
type mqttClient struct {
	t    *testing.T
	conn net.Conn
	enc  packet.Encoder
	dec  *packet.Decoder
}

func dial(t *testing.T, addr, id string) *mqttClient {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err == nil {
			c := &mqttClient{t: t, conn: conn, enc: packet.Encoder{Version: packet.V5}, dec: packet.NewDecoder(conn)}
			c.dec.Version = packet.V5
			_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
			if err = c.enc.Encode(conn, &packet.Connect{ProtocolName: "MQTT", ProtocolLevel: packet.V5, CleanStart: true, KeepAlive: 60, ClientID: id}); err == nil {
				var p packet.Packet
				if p, err = c.dec.Read(); err == nil {
					if ack, ok := p.(*packet.Connack); ok && ack.ReasonCode == packet.Success {
						_ = conn.SetDeadline(time.Time{})
						t.Cleanup(func() { _ = conn.Close() })
						return c
					}
					err = fmt.Errorf("answered %#v", p)
				}
			}
			_ = conn.Close()
		}
		if time.Now().After(deadline) {
			t.Fatalf("connect %s: %v", id, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (c *mqttClient) send(p packet.Packet) {
	c.t.Helper()
	_ = c.conn.SetDeadline(time.Now().Add(30 * time.Second))
	if err := c.enc.Encode(c.conn, p); err != nil {
		c.t.Fatal(err)
	}
}

func (c *mqttClient) read() packet.Packet {
	c.t.Helper()
	_ = c.conn.SetDeadline(time.Now().Add(30 * time.Second))
	p, err := c.dec.Read()
	if err != nil {
		c.t.Fatal(err)
	}
	return p
}

// publish sends p and completes its exchange, returning the reason code of
// its acknowledgement (Success for QoS 0).
func (c *mqttClient) publish(p *packet.Publish) packet.ReasonCode {
	c.t.Helper()
	c.send(p)
	switch p.QoS {
	case 1:
		return c.read().(*packet.Puback).ReasonCode
	case 2:
		rec := c.read().(*packet.Pubrec)
		if rec.ReasonCode.IsError() {
			return rec.ReasonCode
		}
		c.send(&packet.Pubrel{Ack: packet.Ack{PacketID: p.PacketID}})
		c.read()
		return rec.ReasonCode
	}
	return packet.Success
}

// createTopics creates the topics with one partition each.
func createTopics(t *testing.T, brokers string, topics ...string) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	req := kmsg.NewPtrCreateTopicsRequest()
	req.TimeoutMillis = 30000
	for _, name := range topics {
		rt := kmsg.NewCreateTopicsRequestTopic()
		rt.Topic, rt.NumPartitions, rt.ReplicationFactor = name, 1, 1
		req.Topics = append(req.Topics, rt)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("precondition: Kafka at %s did not answer CreateTopics: %v", brokers, err)
	}
	for _, rt := range resp.Topics {
		if err := kerr.ErrorForCode(rt.ErrorCode); err != nil {
			t.Fatalf("precondition: topic %s not created: %v", rt.Topic, err)
		}
	}
}

// consume reads a topic from its start until want records have arrived or
// the time is up.
func consume(t *testing.T, brokers, topic string, want int, within time.Duration) []*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	var got []*kgo.Record
	deadline := time.Now().Add(within)
	for len(got) < want && time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		fs := cl.PollFetches(ctx)
		cancel()
		fs.EachRecord(func(r *kgo.Record) { got = append(got, r) })
	}
	return got
}

func header(r *kgo.Record, key string) (string, bool) {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value), true
		}
	}
	return "", false
}

func headerList(r *kgo.Record) string {
	var b strings.Builder
	for _, h := range r.Headers {
		fmt.Fprintf(&b, "%s=%q;", h.Key, h.Value)
	}
	return b.String()
}

// TestLiveKafka shows the documented mapping in a real Kafka, then each
// unavailability policy with every drop counted, then the stop's count.
func TestLiveKafka(t *testing.T) {
	brokers, container := liveKafka(t)
	bin := buildBroker(t)
	out, err := exec.Command(bin, "version").CombinedOutput()
	if err != nil || !strings.Contains(string(out), " sink.kafka") {
		t.Fatalf("the default binary does not carry sink.kafka: %v\n%s", err, out)
	}
	run := strconv.FormatInt(time.Now().UnixNano(), 36)
	sensors, alarms := "t1317-sensors-"+run, "t1317-alarms-"+run
	createTopics(t, brokers, sensors, alarms)
	t.Cleanup(func() { _ = exec.Command("podman", "unpause", container).Run() })
	cfg := func(whenFull string) map[string]any {
		return map[string]any{
			"brokers": []string{brokers},
			"routes": []any{
				map[string]any{"filter": "sensors/+/temp", "topic": sensors, "key": "topic"},
				map[string]any{"filter": "alarms/#", "topic": alarms, "key": "userProperty", "keyProperty": "site"},
			},
			"lingerMs":       0,
			"flushTimeoutMs": 2000,
			"buffer":         map[string]any{"maxRecords": 20, "whenFull": whenFull, "waitMs": 200},
		}
	}
	sensorsLabel := `reason="buffer_full",topic="` + sensors + `"`

	t.Run("mapping", func(t *testing.T) {
		n := serve(t, bin, cfg("refuse"))
		c := dial(t, n.addr, "pub-map")
		start := time.Now()
		ct, pf := "application/json", byte(1)
		p1 := &packet.Publish{Topic: "sensors/a/temp", QoS: 1, PacketID: 1, Payload: []byte(`{"c":21.5}`)}
		p1.Properties.ContentType = &ct
		p1.Properties.PayloadFormatIndicator = &pf
		p1.Properties.UserProperties = []packet.UserProperty{{Name: "unit", Value: "C"}, {Name: "tag", Value: "a"}, {Name: "tag", Value: "b"}}
		if code := c.publish(p1); code.IsError() {
			t.Fatalf("QoS 1: %s", code)
		}
		if code := c.publish(&packet.Publish{Topic: "sensors/b/temp", Payload: []byte("q0")}); code.IsError() {
			t.Fatalf("QoS 0: %s", code)
		}
		if code := c.publish(&packet.Publish{Topic: "sensors/b/temp", QoS: 2, PacketID: 2, Payload: []byte("q2")}); code.IsError() {
			t.Fatalf("QoS 2: %s", code)
		}
		alarm := &packet.Publish{Topic: "alarms/x", QoS: 1, PacketID: 3, Retain: true, Payload: []byte("fire")}
		alarm.Properties.UserProperties = []packet.UserProperty{{Name: "site", Value: "north"}}
		if code := c.publish(alarm); code.IsError() {
			t.Fatalf("retained alarm: %s", code)
		}
		if code := c.publish(&packet.Publish{Topic: "other/t", QoS: 1, PacketID: 4, Payload: []byte("nowhere")}); code.IsError() {
			t.Fatalf("other/t: %s", code)
		}
		end := time.Now()

		got := consume(t, brokers, sensors, 3, 30*time.Second)
		if len(got) != 3 {
			t.Fatalf("sensors topic: %d records, want 3", len(got))
		}
		r := got[0]
		wantHeaders := `mqtt.topic="sensors/a/temp";mqtt.qos="1";mqtt.source="client";mqtt.clientId="pub-map";` +
			`mqtt.contentType="application/json";mqtt.payloadFormat="1";unit="C";tag="a";tag="b";`
		if string(r.Key) != "sensors/a/temp" || string(r.Value) != `{"c":21.5}` || headerList(r) != wantHeaders {
			t.Errorf("record 0: key %q value %q headers %s\nwant headers %s", r.Key, r.Value, headerList(r), wantHeaders)
		}
		for i, r := range got {
			if r.Timestamp.Before(start.Truncate(time.Millisecond)) || r.Timestamp.After(end) {
				t.Errorf("record %d: timestamp %v outside the run %v–%v", i, r.Timestamp, start, end)
			}
			t.Logf("sensors[%d] offset %d key %q value %q headers %s", i, r.Offset, r.Key, r.Value, headerList(r))
		}
		if q, _ := header(got[1], "mqtt.qos"); string(got[1].Value) != "q0" || q != "0" {
			t.Errorf("record 1: %q qos %s", got[1].Value, q)
		}
		if q, _ := header(got[2], "mqtt.qos"); string(got[2].Value) != "q2" || q != "2" || string(got[2].Key) != "sensors/b/temp" {
			t.Errorf("record 2: %q qos %s key %q", got[2].Value, q, got[2].Key)
		}
		al := consume(t, brokers, alarms, 1, 30*time.Second)
		if len(al) != 1 {
			t.Fatalf("alarms topic: %d records, want 1", len(al))
		}
		if retain, _ := header(al[0], "mqtt.retain"); string(al[0].Key) != "north" || string(al[0].Value) != "fire" || retain != "true" {
			t.Errorf("alarm: key %q value %q headers %s", al[0].Key, al[0].Value, headerList(al[0]))
		}
		t.Logf("alarms[0] key %q value %q headers %s", al[0].Key, al[0].Value, headerList(al[0]))
		for _, rr := range append(got, al...) {
			if bytes.Equal(rr.Value, []byte("nowhere")) {
				t.Error("other/t, which no route matches, reached Kafka")
			}
		}
		n.waitMetric("hoardmq_sink_kafka_produced_total", `topic="`+sensors+`"`, 3)
		if p := n.metric("hoardmq_sink_kafka_produced_total", `topic="`+sensors+`"`); p != 3 {
			t.Errorf("produced counted %v for %s, want 3", p, sensors)
		}
		if p := n.metric("hoardmq_sink_kafka_produced_total", `topic="`+alarms+`"`); p != 1 {
			t.Errorf("produced counted %v for %s, want 1", p, alarms)
		}
	})

	before := len(consume(t, brokers, sensors, 3, 10*time.Second))

	t.Run("refuse", func(t *testing.T) {
		n := serve(t, bin, cfg("refuse"))
		c := dial(t, n.addr, "pub-refuse")
		podman(t, "pause", container)
		var accepted []string
		refused := 0
		for i := 1; i <= 200 && refused == 0; i++ {
			payload := fmt.Sprintf("r-%03d", i)
			switch code := c.publish(&packet.Publish{Topic: "sensors/c/temp", QoS: 1, PacketID: uint16(i), Payload: []byte(payload)}); {
			case code == packet.QuotaExceeded:
				refused++
			case code.IsError():
				t.Fatalf("publish %s: %s", payload, code)
			default:
				accepted = append(accepted, payload)
			}
		}
		if refused == 0 {
			t.Fatalf("200 publishes into a paused Kafka, none refused")
		}
		c.publish(&packet.Publish{Topic: "sensors/c/temp", Payload: []byte("zero-while-full")})
		n.waitMetric("hoardmq_sink_kafka_dropped_total", sensorsLabel, 1)
		buffered, refusedCount := n.metric("hoardmq_sink_kafka_buffered_records", ""), n.metric("hoardmq_sink_kafka_refused_total", `topic="`+sensors+`"`)
		t.Logf("paused: %d acknowledged, refused counted %v, buffered %v, QoS 0 dropped %v", len(accepted), refusedCount, buffered,
			n.metric("hoardmq_sink_kafka_dropped_total", sensorsLabel))
		if refusedCount < 1 || buffered < 20 {
			t.Fatalf("refused %v, buffered %v; want at least 1 and 20", refusedCount, buffered)
		}
		podman(t, "unpause", container)
		got := consume(t, brokers, sensors, before+len(accepted), 90*time.Second)
		var phase []string
		for _, r := range got[before:] {
			phase = append(phase, string(r.Value))
		}
		if strings.Join(phase, ",") != strings.Join(accepted, ",") {
			t.Fatalf("after the pause Kafka holds\n%v\nwant every acknowledged publish, in order\n%v", phase, accepted)
		}
		if d := n.metric("hoardmq_sink_kafka_dropped_total", `reason="overflow",topic="`+sensors+`"`); d != 0 {
			t.Errorf("overflow %v", d)
		}
	})

	t.Run("drop", func(t *testing.T) {
		before := len(consume(t, brokers, sensors, before+1000, 15*time.Second))
		n := serve(t, bin, cfg("drop"))
		c := dial(t, n.addr, "pub-drop")
		podman(t, "stop", "-t", "10", container)
		for i := 1; i <= 40; i++ {
			payload := fmt.Sprintf("d-%03d", i)
			if code := c.publish(&packet.Publish{Topic: "sensors/d/temp", QoS: 1, PacketID: uint16(i), Payload: []byte(payload)}); code.IsError() {
				t.Fatalf("the drop policy refused %s: %s", payload, code)
			}
		}
		n.waitMetric("hoardmq_sink_kafka_dropped_total", sensorsLabel, 20)
		dropped, buffered := n.metric("hoardmq_sink_kafka_dropped_total", sensorsLabel), n.metric("hoardmq_sink_kafka_buffered_records", "")
		t.Logf("stopped: 40 acknowledged, dropped %v, buffered %v", dropped, buffered)
		if dropped+buffered != 40 {
			t.Fatalf("dropped %v + buffered %v != 40 accepted", dropped, buffered)
		}
		podman(t, "start", container)
		got := consume(t, brokers, sensors, before+int(buffered), 120*time.Second)
		var phase []string
		for _, r := range got[min(before, len(got)):] {
			phase = append(phase, string(r.Value))
		}
		var want []string
		for i := 1; i <= int(buffered); i++ {
			want = append(want, fmt.Sprintf("d-%03d", i))
		}
		if strings.Join(phase, ",") != strings.Join(want, ",") {
			t.Fatalf("after the restart Kafka holds\n%v\nwant the %v publishes not counted dropped\n%v", phase, buffered, want)
		}
	})

	t.Run("shutdown", func(t *testing.T) {
		n := serve(t, bin, cfg("refuse"))
		c := dial(t, n.addr, "pub-stop")
		podman(t, "pause", container)
		for i := 1; i <= 5; i++ {
			if code := c.publish(&packet.Publish{Topic: "sensors/e/temp", QoS: 1, PacketID: uint16(i), Payload: []byte("s")}); code.IsError() {
				t.Fatalf("publish %d: %s", i, code)
			}
		}
		n.waitMetric("hoardmq_sink_kafka_buffered_records", "", 5)
		start := time.Now()
		if err := syscall.Kill(n.pid, syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		select {
		case <-n.exited:
		case <-time.After(2*time.Second + 5*time.Second + 5*time.Second):
			t.Fatalf("the broker did not exit in time:\n%s", n.log())
		}
		t.Logf("exited %v after SIGTERM", time.Since(start).Round(time.Millisecond))
		if !regexp.MustCompile(`flush incomplete at stop.*"records":5`).MatchString(n.log()) {
			t.Fatalf("no stop line naming 5 abandoned records:\n%s", n.log())
		}
		podman(t, "unpause", container)
	})
}
