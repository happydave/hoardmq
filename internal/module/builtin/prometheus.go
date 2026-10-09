package builtin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/happydave/hoardmq/internal/module"
)

func init() {
	module.Register("metrics.prometheus", func() module.Module { return &prometheus{} })
}

// prometheus is metrics.prometheus: the internal counters plus an HTTP
// listener serving the text exposition format, liveness and readiness. With
// no address it behaves as metrics.internal.
type prometheus struct {
	metrics
	address     string
	metricsPath string
	ready       func() bool
	serving     func() (bool, string)
	drain       func()
	drainStatus func() module.DrainStatus
	// adminOwnsDrain closes /drain here: the admin API serves it.
	adminOwnsDrain bool
	srv            *http.Server
	ln             net.Listener
}

type prometheusConfig struct {
	Address     string `json:"address"`
	MetricsPath string `json:"metricsPath"`
}

func (*prometheus) ID() module.ID { return "metrics.prometheus" }

func (p *prometheus) Provision(ctx *module.Context, raw json.RawMessage) error {
	var cfg prometheusConfig
	if err := decode(raw, &cfg); err != nil {
		return err
	}
	p.address = cfg.Address
	if p.address == "" {
		p.address = ctx.Observability.Address
	}
	p.metricsPath = cfg.MetricsPath
	if p.metricsPath == "" {
		p.metricsPath = ctx.Observability.MetricsPath
	}
	if p.metricsPath == "" {
		p.metricsPath = "/metrics"
	}
	p.adminOwnsDrain = ctx.Observability.AdminOwnsDrain
	if ctx.Services != nil {
		p.ready = ctx.Services.Ready
		p.serving = ctx.Services.Serving
		p.drain = ctx.Services.Drain
		p.drainStatus = ctx.Services.DrainStatus
	}
	p.series = map[string]*module.Metric{}
	p.enabled.Store(true)
	return nil
}

func (p *prometheus) Start(context.Context) error {
	if p.address == "" {
		return nil
	}
	ln, err := net.Listen("tcp", p.address)
	if err != nil {
		return fmt.Errorf("observability listener: %w", err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc(p.metricsPath, p.serveMetrics)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if p.ready != nil && p.ready() {
			_, _ = w.Write([]byte("ready\n"))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("not ready\n"))
	})
	// /servingz is not a probe: a degraded node stays in service, and this
	// says whether it can acknowledge a QoS 1 or 2 publish.
	mux.HandleFunc("/servingz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if p.serving == nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("not ready: unknown\n"))
			return
		}
		ok, reason := p.serving()
		if ok {
			_, _ = w.Write([]byte("serving\n"))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(servingText(reason) + "\n"))
	})
	// The drain endpoint: POST starts the graceful leave, GET reports it.
	mux.HandleFunc("/drain", func(w http.ResponseWriter, r *http.Request) {
		if !drainDoorOpen(p.adminOwnsDrain) {
			http.Error(w, "the drain is served by the admin API", http.StatusForbidden)
			return
		}
		if p.drainStatus == nil {
			http.Error(w, "drain unavailable", http.StatusServiceUnavailable)
			return
		}
		switch r.Method {
		case http.MethodPost:
			p.drain()
			w.WriteHeader(http.StatusAccepted)
		case http.MethodGet:
		default:
			w.Header().Set("Allow", "GET, POST")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(p.drainStatus())
	})
	p.ln = ln
	p.srv = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = p.srv.Serve(ln) }()
	return nil
}

// drainDoorOpen decides whether this listener's /drain may start or report a
// drain: not once the admin API owns it, since this listener authenticates
// nobody.
func drainDoorOpen(adminOwnsDrain bool) bool { return !adminOwnsDrain }

func (p *prometheus) Stop(context.Context) error {
	if p.srv == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := p.srv.Shutdown(ctx)
	p.srv = nil
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Addr is the bound address, or nil when not serving.
func (p *prometheus) Addr() net.Addr {
	if p.ln == nil {
		return nil
	}
	return p.ln.Addr()
}

func (p *prometheus) serveMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = w.Write([]byte(Render(p.Snapshot())))
}

// Render writes metrics in the Prometheus text exposition format: one TYPE
// line per name (counter for _total names, gauge otherwise), samples with
// sorted, escaped labels, in name then label order.
func Render(metrics []module.Metric) string {
	sort.Slice(metrics, func(i, j int) bool {
		if metrics[i].Name != metrics[j].Name {
			return metrics[i].Name < metrics[j].Name
		}
		return key(metrics[i].Name, metrics[i].Labels) < key(metrics[j].Name, metrics[j].Labels)
	})
	var b strings.Builder
	last := ""
	for _, m := range metrics {
		if m.Name != last {
			kind := "gauge"
			if strings.HasSuffix(m.Name, "_total") {
				kind = "counter"
			}
			fmt.Fprintf(&b, "# TYPE %s %s\n", m.Name, kind)
			last = m.Name
		}
		b.WriteString(m.Name)
		if len(m.Labels) > 0 {
			keys := make([]string, 0, len(m.Labels))
			for k := range m.Labels {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			b.WriteString("{")
			for i, k := range keys {
				if i > 0 {
					b.WriteString(",")
				}
				b.WriteString(k)
				b.WriteString(`="`)
				b.WriteString(escapeLabel(m.Labels[k]))
				b.WriteString(`"`)
			}
			b.WriteString("}")
		}
		b.WriteString(" ")
		b.WriteString(strconv.FormatFloat(m.Value, 'g', -1, 64))
		b.WriteString("\n")
	}
	return b.String()
}

func escapeLabel(v string) string {
	v = strings.ReplaceAll(v, `\`, `\\`)
	v = strings.ReplaceAll(v, "\n", `\n`)
	return strings.ReplaceAll(v, `"`, `\"`)
}

// servingText is /servingz's answer for a node that is not serving: "not
// ready: " and the readiness reason, or "degraded: " and the state.
func servingText(reason string) string {
	if r, ok := strings.CutPrefix(reason, "not-ready: "); ok {
		return "not ready: " + r
	}
	return "degraded: " + reason
}
