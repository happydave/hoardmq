package builtin

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"testing"

	"github.com/happydave/hoardmq/internal/module"
)

// liveServices offers the liveness the broker's services offer; the
// embedded interface is never called.
type liveServices struct {
	module.Services
	live   bool
	reason string
}

func (s *liveServices) Ready() bool                     { return true }
func (s *liveServices) Serving() (bool, string)         { return true, "" }
func (s *liveServices) Drain()                          {}
func (s *liveServices) DrainStatus() module.DrainStatus { return module.DrainStatus{} }
func (s *liveServices) Live() (bool, string)            { return s.live, s.reason }

// readyOnly offers no liveness.
type readyOnly struct{ liveServices }

func (readyOnly) Live() {}

// TestHealthzReadsLiveness: /healthz answers ok while the services report
// the node live or offer no liveness, and 503 naming the reason while not.
func TestHealthzReadsLiveness(t *testing.T) {
	get := func(t *testing.T, svc module.Services) (int, string) {
		t.Helper()
		m := &prometheus{}
		ctx := &module.Context{Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Services: svc,
			Observability: module.Observability{Address: "127.0.0.1:0", MetricsPath: "/metrics"}}
		if err := m.Provision(ctx, json.RawMessage(`{}`)); err != nil {
			t.Fatal(err)
		}
		if err := m.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = m.Stop(context.Background()) }()
		resp, err := http.Get("http://" + m.Addr().String() + "/healthz")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	if code, body := get(t, &liveServices{live: true}); code != 200 || body != "ok\n" {
		t.Errorf("live: %d %q, want 200 ok", code, body)
	}
	if code, body := get(t, &liveServices{reason: "consensus-stopped: raft log save: injected"}); code != 503 || body != "not live: consensus-stopped: raft log save: injected\n" {
		t.Errorf("not live: %d %q, want 503 naming the reason", code, body)
	}
	if code, body := get(t, &readyOnly{liveServices{reason: "never read"}}); code != 200 || body != "ok\n" {
		t.Errorf("services without liveness: %d %q, want 200 ok", code, body)
	}
}
