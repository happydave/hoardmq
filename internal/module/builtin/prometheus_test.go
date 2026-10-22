package builtin

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
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

// Over a real socket from an address of this host that is not loopback, the
// listener bound on all interfaces serves /metrics and refuses /drain.
func TestDrainRefusesARemoteCallerOverTheNetwork(t *testing.T) {
	ip := nonLoopbackIPv4(t)
	m := &prometheus{}
	ctx := &module.Context{Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Observability: module.Observability{Address: "0.0.0.0:0", MetricsPath: "/metrics"}}
	if err := m.Provision(ctx, json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	started := 0
	m.drain = func() { started++ }
	m.drainStatus = func() module.DrainStatus { return module.DrainStatus{Draining: started > 0} }
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = m.Stop(context.Background()) }()
	m.Add("hoardmq_connections_total", nil, 1)
	_, port, _ := net.SplitHostPort(m.Addr().String())
	base := "http://" + net.JoinHostPort(ip, port)
	// No pooled connection outlives the test into the listener's shutdown.
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	resp, err := client.Get(base + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != 200 || !strings.Contains(string(body), "hoardmq_connections_total 1") {
		t.Fatalf("GET /metrics from %s: %d %q", ip, resp.StatusCode, body)
	}
	for _, method := range []string{http.MethodPost, http.MethodGet} {
		req, _ := http.NewRequest(method, base+"/drain", nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden || !strings.Contains(string(body), "loopback") {
			t.Errorf("%s /drain from %s: %d %q", method, ip, resp.StatusCode, body)
		}
	}
	if started != 0 {
		t.Fatalf("a remote caller started %d drains", started)
	}
}

func TestDrainRefusal(t *testing.T) {
	for _, c := range []struct {
		addr string
		open bool
	}{
		{"127.0.0.1:40000", true},
		{"127.8.9.10:1", true},
		{"[::1]:1", true},
		{"[::ffff:127.0.0.1]:1", true},
		{"10.1.2.3:1", false},
		{"192.0.2.7:9090", false},
		{"[2001:db8::1]:1", false},
		{"[fe80::1%eth0]:1", false},
		{"0.0.0.0:1", false},
		{"localhost:1", false},
		{"garbage", false},
		{"", false},
	} {
		r := drainRefusal(false, c.addr)
		if c.open && r != "" {
			t.Errorf("%q refused: %q", c.addr, r)
		}
		if !c.open && !strings.Contains(r, "loopback") {
			t.Errorf("%q: %q, want the loopback refusal", c.addr, r)
		}
	}
}

// Through the listener's mux, a peer off the loopback gets every path but
// /drain: it is shown the probes and metrics first, so the refusals below are
// of /drain alone.
func TestDrainRefusedToARemotePeer(t *testing.T) {
	m := &prometheus{}
	ctx := &module.Context{Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Observability: module.Observability{MetricsPath: "/metrics"}}
	if err := m.Provision(ctx, json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	started := 0
	m.drain = func() { started++ }
	m.drainStatus = func() module.DrainStatus { return module.DrainStatus{Draining: started > 0} }
	m.ready = func() bool { return true }
	m.serving = func() (bool, string) { return true, "" }
	m.Add("hoardmq_connections_total", nil, 1)
	h := m.handler()
	serve := func(method, path string) (int, string) {
		req := httptest.NewRequest(method, path, nil)
		req.RemoteAddr = "10.1.2.3:51000"
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code, rec.Body.String()
	}
	if code, body := serve(http.MethodGet, "/metrics"); code != 200 || !strings.Contains(body, "hoardmq_connections_total 1") {
		t.Fatalf("GET /metrics from a remote peer: %d %q", code, body)
	}
	for path, want := range map[string]string{"/healthz": "ok\n", "/readyz": "ready\n", "/servingz": "serving\n"} {
		if code, body := serve(http.MethodGet, path); code != 200 || body != want {
			t.Fatalf("GET %s from a remote peer: %d %q", path, code, body)
		}
	}
	if code, _ := serve(http.MethodGet, "/other"); code != http.StatusNotFound {
		t.Fatalf("GET /other from a remote peer: %d", code)
	}
	if code, body := serve(http.MethodPost, "/drain"); code != http.StatusForbidden || !strings.Contains(body, "loopback") || started != 0 {
		t.Errorf("POST /drain from a remote peer: %d %q, %d drains started", code, body, started)
	}
	if code, body := serve(http.MethodGet, "/drain"); code != http.StatusForbidden || strings.Contains(body, "draining") {
		t.Errorf("GET /drain from a remote peer: %d %q", code, body)
	}
}

// nonLoopbackIPv4 returns an IPv4 address of an interface that is up and not
// loopback, or skips the test when the host has none.
func nonLoopbackIPv4(t *testing.T) string {
	t.Helper()
	ifaces, err := net.Interfaces()
	if err != nil {
		t.Fatal(err)
	}
	for _, ifc := range ifaces {
		if ifc.Flags&net.FlagUp == 0 || ifc.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := ifc.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			if n, ok := a.(*net.IPNet); ok && n.IP.To4() != nil && !n.IP.IsLoopback() {
				return n.IP.String()
			}
		}
	}
	t.Skip("no non-loopback IPv4 address on this host")
	return ""
}

// With the admin API configured this listener's /drain refuses every method
// and starts nothing; the probes and /metrics are unchanged.
func TestDrainDoorClosedWhenAdminOwnsDrain(t *testing.T) {
	if r := drainRefusal(true, "127.0.0.1:1"); !strings.Contains(r, "admin API") {
		t.Fatalf("drainRefusal with the admin API owning the drain: %q", r)
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

// TestOrderNamesDefaults: hooks.order may name a default module the registry
// instantiates, and it then comes first; a default replaced by a configured
// module of its family is not instantiated, and naming it is a problem.
func TestOrderNamesDefaults(t *testing.T) {
	ctx := &module.Context{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	cfg := config.Default()
	cfg.Node.DataDir = t.TempDir()
	cfg.Hooks.Order = []string{"store.wal"}
	reg, problems := module.New(cfg, ctx, hook.Policy{})
	if len(problems) > 0 {
		t.Fatal(problems)
	}
	if ids := reg.Modules(); len(ids) != 2 || ids[0] != "store.wal" || ids[1] != "metrics.internal" {
		t.Fatalf("modules %v, want store.wal first", ids)
	}

	cfg.Modules = map[string]json.RawMessage{"metrics.prometheus": json.RawMessage(`{}`)}
	cfg.Hooks.Order = []string{"metrics.internal"}
	_, problems = module.New(cfg, ctx, hook.Policy{})
	if len(problems) != 1 || problems[0].Pointer != "/hooks/order/0" || problems[0].Message != "not an instantiated module" {
		t.Fatalf("a replaced default named: %v", problems)
	}
}
