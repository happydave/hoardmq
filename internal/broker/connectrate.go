package broker

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/ratelimit"
)

// connectGate holds each listener's connect-rate bucket and the node's, and
// answers the connection's question under one lock; the refusals are counted
// by listener and limit, readable without it.
type connectGate struct {
	mu        sync.Mutex
	listeners map[string]ratelimit.Bucket
	node      ratelimit.Bucket
	refused   map[string]*atomic.Int64 // "<listener>/<limit>"
	now       func() time.Time
}

// newConnectGate is nil when no listener and not the node sets a rate: the
// connection then asks nothing.
func newConnectGate(cfg config.Document) *connectGate {
	limited := cfg.Limits.ConnectRate > 0
	for _, l := range cfg.Listeners {
		limited = limited || l.ConnectRate > 0
	}
	if !limited {
		return nil
	}
	now := time.Now()
	bucket := func(rate, burst int) ratelimit.Bucket {
		return ratelimit.New(float64(rate), float64(config.Burst(rate, burst)), now)
	}
	g := &connectGate{listeners: map[string]ratelimit.Bucket{}, node: bucket(cfg.Limits.ConnectRate, cfg.Limits.ConnectBurst),
		refused: map[string]*atomic.Int64{}, now: time.Now}
	for _, l := range cfg.Listeners {
		g.listeners[l.Name] = bucket(l.ConnectRate, l.ConnectBurst)
		for _, limit := range []string{"listener", "node"} {
			g.refused[l.Name+"/"+limit] = new(atomic.Int64)
		}
	}
	return g
}

// AdmitConnect implements conn.ConnectGate.
func (g *connectGate) AdmitConnect(listener string) (bool, string) {
	g.mu.Lock()
	l, n, v := ratelimit.Admit(g.listeners[listener], g.node, g.now())
	g.listeners[listener], g.node = l, n
	g.mu.Unlock()
	if v == ratelimit.Admitted {
		return true, ""
	}
	if c := g.refused[listener+"/"+v.Limit()]; c != nil {
		c.Add(1)
	}
	return false, v.Limit()
}

// Refused is the refusals so far by "<listener>/<limit>", only those above 0.
func (g *connectGate) Refused() map[string]int64 {
	if g == nil {
		return nil
	}
	out := map[string]int64{}
	for k, c := range g.refused {
		if n := c.Load(); n > 0 {
			out[k] = n
		}
	}
	return out
}

// carry keeps what the gate it replaces had: each listener's bucket, whose
// limits the administered record does not change, and the refusal counts.
func (g *connectGate) carry(old *connectGate) {
	old.mu.Lock()
	for name, b := range old.listeners {
		if cur, ok := g.listeners[name]; ok && cur.Rate == b.Rate && cur.Burst == b.Burst {
			g.listeners[name] = b
		}
	}
	old.mu.Unlock()
	g.refused = old.refused
}

// rateGate is the connect-rate limits in force, nil when none is set.
func (s *Server) rateGate() *connectGate { return s.admit.rate.Load() }
