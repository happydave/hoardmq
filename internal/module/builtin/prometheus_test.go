package builtin

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/hook"
	"github.com/happydave/hoardmq/internal/module"
)

func TestRender(t *testing.T) {
	got := Render([]module.Metric{
		{Name: "hoardmq_sessions_active", Labels: map[string]string{"namespace": "default"}, Value: 3},
		{Name: "hoardmq_publishes_total", Labels: map[string]string{"qos": "1", "namespace": "default"}, Value: 7},
		{Name: "hoardmq_publishes_total", Labels: map[string]string{"qos": "0", "namespace": "default"}, Value: 2.5},
		{Name: "hoardmq_drops_total", Labels: map[string]string{"namespace": "a\"b\\c\nd", "reason": "queue_full"}, Value: 1},
		{Name: "hoardmq_log_index", Value: 42},
	})
	want := `# TYPE hoardmq_drops_total counter
hoardmq_drops_total{namespace="a\"b\\c\nd",reason="queue_full"} 1
# TYPE hoardmq_log_index gauge
hoardmq_log_index 42
# TYPE hoardmq_publishes_total counter
hoardmq_publishes_total{namespace="default",qos="0"} 2.5
hoardmq_publishes_total{namespace="default",qos="1"} 7
# TYPE hoardmq_sessions_active gauge
hoardmq_sessions_active{namespace="default"} 3
`
	if got != want {
		t.Fatalf("render\n got:\n%s\nwant:\n%s", got, want)
	}
}

func TestPrometheusModuleServes(t *testing.T) {
	m := &prometheus{}
	ctx := &module.Context{Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Observability: module.Observability{Address: "127.0.0.1:0", MetricsPath: "/metrics"}}
	if err := m.Provision(ctx, json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	ready := false
	m.ready = func() bool { return ready }
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = m.Stop(context.Background()) }()
	m.Add("hoardmq_connections_total", map[string]string{"listener": "tcp"}, 2)
	m.AddCollector(func(set func(string, map[string]string, float64)) {
		set("hoardmq_sessions_active", map[string]string{"namespace": "default"}, 5)
	})
	base := "http://" + m.Addr().String()
	get := func(path string) (int, string) {
		resp, err := http.Get(base + path)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	if code, body := get("/metrics"); code != 200 || !strings.Contains(body, `hoardmq_connections_total{listener="tcp"} 2`) || !strings.Contains(body, `hoardmq_sessions_active{namespace="default"} 5`) {
		t.Fatalf("metrics %d\n%s", code, body)
	}
	if code, body := get("/healthz"); code != 200 || body != "ok\n" {
		t.Fatalf("healthz %d %q", code, body)
	}
	if code, _ := get("/readyz"); code != 503 {
		t.Fatalf("readyz before ready %d", code)
	}
	ready = true
	if code, body := get("/readyz"); code != 200 || body != "ready\n" {
		t.Fatalf("readyz %d %q", code, body)
	}
	if code, _ := get("/other"); code != 404 {
		t.Fatalf("other %d", code)
	}
	if err := m.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := http.Get(base + "/healthz"); err == nil {
		t.Fatal("listener still serving after Stop")
	}
	// Without an address the module serves nothing and still counts.
	m2 := &prometheus{}
	if err := m2.Provision(&module.Context{Logger: ctx.Logger}, json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	if err := m2.Start(context.Background()); err != nil || m2.Addr() != nil {
		t.Fatalf("no-address start: %v %v", err, m2.Addr())
	}
	m2.Add("x_total", nil, 1)
	if len(m2.Snapshot()) != 1 {
		t.Fatal("counting without a listener")
	}
}

func TestPrometheusConfigErrors(t *testing.T) {
	m := &prometheus{}
	ctx := &module.Context{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if err := m.Provision(ctx, json.RawMessage(`{"nope":1}`)); err == nil {
		t.Fatal("unknown key accepted")
	}
	if err := m.Provision(ctx, json.RawMessage(`{"address":"127.0.0.1:0","metricsPath":"/m"}`)); err != nil || m.address != "127.0.0.1:0" || m.metricsPath != "/m" {
		t.Fatalf("%v %+v", err, m)
	}
}

func TestDrainEndpoint(t *testing.T) {
	m := &prometheus{}
	ctx := &module.Context{Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Observability: module.Observability{Address: "127.0.0.1:0", MetricsPath: "/metrics"}}
	if err := m.Provision(ctx, json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	started := 0
	m.drain = func() { started++ }
	m.drainStatus = func() module.DrainStatus {
		return module.DrainStatus{Draining: started > 0, Phase: "redirecting-clients", ClientsRemaining: 3}
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = m.Stop(context.Background()) }()
	base := "http://" + m.Addr().String()
	resp, err := http.Get(base + "/drain")
	if err != nil {
		t.Fatal(err)
	}
	var st module.DrainStatus
	_ = json.NewDecoder(resp.Body).Decode(&st)
	_ = resp.Body.Close()
	if resp.StatusCode != 200 || st.Draining {
		t.Fatalf("GET before drain: %d %+v", resp.StatusCode, st)
	}
	resp, err = http.Post(base+"/drain", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = json.NewDecoder(resp.Body).Decode(&st)
	_ = resp.Body.Close()
	if resp.StatusCode != 202 || started != 1 || !st.Draining || st.ClientsRemaining != 3 {
		t.Fatalf("POST drain: %d started=%d %+v", resp.StatusCode, started, st)
	}
	req, _ := http.NewRequest(http.MethodDelete, base+"/drain", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 405 {
		t.Fatalf("DELETE drain: %d", resp.StatusCode)
	}
}

// With the admin API configured this listener's /drain refuses every method
// and starts nothing; the probes and /metrics are unchanged.
func TestDrainDoorClosedWhenAdminOwnsDrain(t *testing.T) {
	if !drainDoorOpen(false) || drainDoorOpen(true) {
		t.Fatal("drainDoorOpen")
	}
	m := &prometheus{}
	ctx := &module.Context{Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Observability: module.Observability{Address: "127.0.0.1:0", MetricsPath: "/metrics", AdminOwnsDrain: true}}
	if err := m.Provision(ctx, json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	started := 0
	m.drain = func() { started++ }
	m.drainStatus = func() module.DrainStatus { return module.DrainStatus{Draining: started > 0} }
	m.ready = func() bool { return true }
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = m.Stop(context.Background()) }()
	base := "http://" + m.Addr().String()
	for _, method := range []string{http.MethodPost, http.MethodGet} {
		req, _ := http.NewRequest(method, base+"/drain", nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden || !strings.Contains(string(body), "admin API") {
			t.Fatalf("%s /drain: %d %q", method, resp.StatusCode, body)
		}
	}
	if started != 0 {
		t.Fatalf("the closed door started %d drains", started)
	}
	resp, err := http.Get(base + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("readyz %d", resp.StatusCode)
	}
}

// Describe reports each module's state as SetEnabled leaves it.
func TestRegistryDescribe(t *testing.T) {
	cfg := config.Default()
	cfg.Node.DataDir = t.TempDir()
	reg, problems := module.New(cfg, &module.Context{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, hook.Policy{})
	if len(problems) > 0 {
		t.Fatal(problems)
	}
	if !reg.SetEnabled("metrics.internal", false) {
		t.Fatal("metrics.internal is not toggleable")
	}
	got := map[module.ID]module.Info{}
	for _, i := range reg.Describe() {
		got[i.ID] = i
	}
	if i := got["metrics.internal"]; !i.Toggleable || i.Enabled {
		t.Fatalf("metrics.internal %+v", i)
	}
	if i := got["store.wal"]; i.Toggleable || !i.Enabled {
		t.Fatalf("store.wal %+v", i)
	}
	if len(got) != len(reg.Modules()) {
		t.Fatalf("described %d of %d", len(got), len(reg.Modules()))
	}
}
