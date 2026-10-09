package broker

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/audit"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/trace"
)

func scrape(t *testing.T, f *fixture, path string) (int, string) {
	t.Helper()
	addr := f.srv.ObservabilityAddr()
	if addr == nil {
		t.Fatal("observability listener not bound")
	}
	resp, err := http.Get("http://" + addr.String() + path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestMetricsScrapeAuditAndTrace(t *testing.T) {
	f := start(t, func(d *config.Document) {
		d.Observability.Address = "127.0.0.1:0"
		d.Modules["metrics.prometheus"] = json.RawMessage(`{}`)
		d.Limits.MaxQueuedMessages = 1
		d.Auth.Anonymous = true
	})
	if got := f.srv.Registry().Modules(); len(got) != 3 || string(got[1]) != "metrics.prometheus" {
		t.Fatalf("modules %v", got)
	}
	if code, body := scrape(t, f, "/healthz"); code != 200 || body != "ok\n" {
		t.Fatalf("healthz %d %q", code, body)
	}
	if code, _ := scrape(t, f, "/readyz"); code != 200 {
		t.Fatalf("readyz %d", code)
	}
	if code, _ := scrape(t, f, "/nope"); code != 404 {
		t.Fatalf("unknown path %d", code)
	}

	// Traffic: a durable subscriber, two publishes, a queue drop while it is
	// offline, and a refused connection.
	exp := uint32(600)
	sub, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "durable", CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	sub.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "m/#", QoS: 1}}})
	sub.read()
	pub, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	if err := f.srv.StartTrace("pub", 50); err != nil {
		t.Fatal(err)
	}
	pub.send(&packet.Publish{Topic: "m/1", Payload: []byte("a")})
	pub.send(&packet.Publish{Topic: "m/2", QoS: 1, PacketID: 1, Payload: []byte("b")})
	pub.read()
	sub.readPublish()
	p := sub.readPublish()
	sub.send(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID}})
	_ = sub.c.Close()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if rec, ok := f.srv.Sessions().Record("durable"); ok && !rec.Online {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	for i := 2; i <= 4; i++ {
		pub.send(&packet.Publish{Topic: "m/3", QoS: 1, PacketID: uint16(i), Payload: []byte("q")})
		pub.read()
	}
	// A refused CONNECT: credentials nobody checks.
	_, ack := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "bad", CleanStart: true, HasUsername: true, Username: "u", HasPassword: true, Password: []byte("p")}, false)
	if ack.ReasonCode != packet.BadUserNameOrPassword {
		t.Fatalf("refusal %s", ack.ReasonCode)
	}

	events, err := f.srv.StopTrace("pub")
	if err != nil {
		t.Fatal(err)
	}
	var seen []string
	for _, e := range events {
		seen = append(seen, string(e.Direction)+":"+e.Type)
	}
	joined := strings.Join(seen, ",")
	if !strings.Contains(joined, "in:PUBLISH") || !strings.Contains(joined, "out:PUBACK") || strings.Contains(joined, "SUBSCRIBE") {
		t.Fatalf("trace %v", joined)
	}
	for _, e := range events {
		if e.Type == "PUBLISH" && e.Direction == trace.In && e.Topic == "" {
			t.Fatalf("publish event without topic %+v", e)
		}
	}
	if f.srv.Trace("pub") != nil {
		t.Fatal("recording still active after stop")
	}
	if err := f.srv.StartTrace("nobody", 10); err == nil {
		t.Fatal("trace of unknown client accepted")
	}
	// The subscriber's connection was never traced.
	if f.srv.Trace("durable") != nil {
		t.Fatal("untraced client has a recording")
	}

	// The refused connection closes asynchronously; wait until only the
	// publisher remains so the active gauge is deterministic.
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && f.srv.Connections() != 1 {
		time.Sleep(10 * time.Millisecond)
	}
	code, body := scrape(t, f, "/metrics")
	if code != 200 {
		t.Fatalf("metrics %d", code)
	}
	for _, want := range []string{
		`hoardmq_connections_total{listener="tcp"} 3`,
		`hoardmq_connections_active 1`,
		`hoardmq_sessions_active{namespace="default"} 2`,
		`hoardmq_subscriptions_active{namespace="default"} 1`,
		`hoardmq_publishes_total{namespace="default",qos="0"} 1`,
		`hoardmq_publishes_total{namespace="default",qos="1"} 4`,
		`hoardmq_deliveries_total{event="delivered",namespace="default"} 2`,
		`hoardmq_deliveries_total{event="acknowledged",namespace="default"} 1`,
		`hoardmq_deliveries_total{event="queued",namespace="default"} 3`,
		`hoardmq_drops_total{namespace="default",reason="queue_full"} 2`,
		`hoardmq_auth_failures_total{listener="tcp",reason="Bad User Name or Password (0x86)"} 1`,
		`hoardmq_retained_active{namespace="default"} 0`,
		"# TYPE hoardmq_log_index gauge",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics missing %q\n%s", want, body)
		}
	}

	for _, want := range []string{
		"hoardmq_audit_records_total ",
		"hoardmq_audit_dropped_total 0",
		"hoardmq_audit_write_failures_total 0",
		"hoardmq_audit_queue_depth ",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics missing %q\n%s", want, body)
		}
	}

	// Audit: the refusal, the trace start and stop, the server start. The
	// writer is asynchronous; wait for it before reading the file.
	f.srv.SetModuleEnabled("authz.file", false)
	if err := f.srv.audit.Load().Flush(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(f.srv.AuditDir(), "audit.log"))
	if err != nil {
		t.Fatal(err)
	}
	var actions []string
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		var e audit.Event
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("audit line %q: %v", line, err)
		}
		actions = append(actions, e.Action+"/"+e.Target+"/"+e.Outcome)
	}
	all := strings.Join(actions, "\n")
	for _, want := range []string{"server.start/", "trace.start/pub/ok", "auth.failure/tcp/Bad User Name or Password (0x86)", "trace.stop/pub/ok", "module.disable/authz.file/ok"} {
		if !strings.Contains(all, want) {
			t.Errorf("audit missing %q in\n%s", want, all)
		}
	}
}

func TestObservabilityOffByDefault(t *testing.T) {
	f := start(t, nil)
	if f.srv.ObservabilityAddr() != nil {
		t.Fatal("listener bound without an address")
	}
	if f.srv.AuditDir() == "" {
		t.Fatal("audit log off although enabled by default")
	}
	g := start(t, func(d *config.Document) { d.Observability.Audit.Enabled = false })
	if g.srv.AuditDir() != "" {
		t.Fatal("audit log on when disabled")
	}
}

// A refusal storm against a small audit queue: the connections are refused
// without waiting on the writer, the drops are counted in the scrape, and
// the log itself records how many events it is missing. An administrative
// event recorded during the storm is kept.
func TestAuditStormDropsRefusalsOnly(t *testing.T) {
	f := start(t, func(d *config.Document) {
		d.Observability.Address = "127.0.0.1:0"
		d.Modules["metrics.prometheus"] = json.RawMessage(`{}`)
		d.Observability.Audit.Queue = 64
	})
	log := f.srv.audit.Load()
	if err := log.Flush(); err != nil {
		t.Fatal(err)
	}
	log.Pause()
	const storm = 300
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < storm; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			nc := f.dial("tcp", false)
			defer func() { _ = nc.Close() }()
			enc := &packet.Encoder{Version: packet.V5}
			_ = nc.SetDeadline(time.Now().Add(5 * time.Second))
			if err := enc.Encode(nc, &packet.Connect{ProtocolLevel: packet.V5, ClientID: fmt.Sprintf("bad-%d", i), CleanStart: true, HasUsername: true, Username: "u", HasPassword: true, Password: []byte("p")}); err != nil {
				t.Error(err)
				return
			}
			dec := packet.NewDecoder(nc)
			dec.Version = packet.V5
			p, err := dec.Read()
			if err != nil {
				t.Error(err)
				return
			}
			if ack, ok := p.(*packet.Connack); !ok || ack.ReasonCode != packet.BadUserNameOrPassword {
				t.Errorf("refusal %v", p)
			}
		}(i)
	}
	wg.Wait()
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("storm of %d refusals took %v with the audit writer held", storm, d)
	}
	if st := log.Stats(); st.Dropped == 0 || st.Depth != 64 {
		t.Fatalf("after the storm %+v", st)
	}
	// An administrative event is lossless: with the queue full it waits for
	// the writer rather than being dropped, and returns once it has space.
	admin := make(chan struct{})
	go func() {
		defer close(admin)
		f.srv.SetModuleEnabled("authz.file", false)
	}()
	select {
	case <-admin:
		t.Fatal("lossless record returned with the queue full and the writer held")
	case <-time.After(50 * time.Millisecond):
	}
	code, body := scrape(t, f, "/metrics")
	if code != 200 || !strings.Contains(body, "hoardmq_audit_dropped_total ") || strings.Contains(body, "hoardmq_audit_dropped_total 0\n") {
		t.Fatalf("scrape %d lacks a non-zero drop count:\n%s", code, body)
	}
	log.Resume()
	select {
	case <-admin:
	case <-time.After(5 * time.Second):
		t.Fatal("lossless record did not return after the writer resumed")
	}
	if err := log.Flush(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(f.srv.AuditDir(), "audit.log"))
	if err != nil {
		t.Fatal(err)
	}
	var dropped, refusals, disables int
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		var e audit.Event
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("audit line %q: %v", line, err)
		}
		switch e.Action {
		case "audit.dropped":
			dropped++
			n, _ := strconv.Atoi(e.Detail["count"])
			if n == 0 || n+refusals > storm {
				t.Fatalf("dropped record %+v against %d refusals so far", e, refusals)
			}
		case "auth.failure":
			refusals++
		case "module.disable":
			disables++
		}
	}
	st := log.Stats()
	if dropped != 1 || disables != 1 || uint64(refusals)+st.Dropped != storm {
		t.Fatalf("dropped records %d, disables %d, refusals written %d, dropped %d", dropped, disables, refusals, st.Dropped)
	}
}
