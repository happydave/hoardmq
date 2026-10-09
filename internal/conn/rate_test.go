package conn

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/packet"
)

// gate refuses with limit when it is set, and counts the questions.
type gate struct {
	limit string
	asked atomic.Int32
}

func (g *gate) AdmitConnect(string) (bool, string) {
	g.asked.Add(1)
	return g.limit == "", g.limit
}

// counters keeps every metric added, keyed name{label=value,...}.
type counters struct {
	mu sync.Mutex
	m  map[string]float64
}

func (c *counters) Add(name string, labels map[string]string, d float64) {
	var parts []string
	for k, v := range labels {
		parts = append(parts, k+"="+v)
	}
	sort.Strings(parts)
	c.mu.Lock()
	if c.m == nil {
		c.m = map[string]float64{}
	}
	c.m[name+"{"+strings.Join(parts, ",")+"}"] += d
	c.mu.Unlock()
}
func (c *counters) Set(string, map[string]string, float64) {}
func (c *counters) get(k string) float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.m[k]
}

func rateOptions(g *gate) (Options, *countingAuth, *counters, *recordingAudit) {
	o := defaultOptions()
	a := always(auth.Allow)
	m := &counters{}
	rec := &recordingAudit{}
	o.Authenticators, o.ConnectGate, o.Metrics, o.Audit = auth.AuthenticatorChain{a}, g, m, rec
	return o, a, m, rec
}

func credentialConnect(v packet.Version) *packet.Connect {
	return &packet.Connect{ProtocolLevel: v, ClientID: "rated", CleanStart: true, HasUsername: true, Username: "u", HasPassword: true, Password: []byte("p")}
}

// Over the rate, a CONNECT is answered 0x9F at level 5 and closed with no
// CONNACK at levels 4 and 3, before any credential is examined or the core
// asked; the refusal is counted and audited.
func TestConnectRateRefusal(t *testing.T) {
	for _, v := range []packet.Version{packet.V5, packet.V4, packet.V3} {
		t.Run(fmt.Sprint(v), func(t *testing.T) {
			g := &gate{limit: "listener"}
			o, a, m, rec := rateOptions(g)
			core := &fakeCore{}
			h := newHarness(t, o, core)
			h.enc = &packet.Encoder{Version: v}
			h.dec.Version = v
			h.send(credentialConnect(v))
			if v == packet.V5 {
				if ack := h.readConnack(); ack.ReasonCode != packet.ConnectionRateExceeded {
					t.Fatalf("level 5 over the rate: %+v, want 0x9F", ack)
				}
			} else {
				h.expectEOF()
			}
			<-h.done
			core.mu.Lock()
			connects := len(core.connects)
			core.mu.Unlock()
			if calls := a.calls.Load(); calls != 0 || connects != 0 {
				t.Fatalf("over the rate: %d authenticator calls, %d core connects; want none", calls, connects)
			}
			if got := m.get("hoardmq_connect_rate_refused_total{limit=listener,listener=test}"); got != 1 {
				t.Fatalf("rate refusals counted %v, want 1: %v", got, m.m)
			}
			if got := m.get("hoardmq_auth_failures_total{listener=test,reason=Connection rate exceeded (0x9F)}"); got != 1 {
				t.Fatalf("CONNECT refusals by reason %v, want 1: %v", got, m.m)
			}
			rec.mu.Lock()
			defer rec.mu.Unlock()
			if len(rec.details) != 1 || rec.details[0]["reason"] != "rate: listener" {
				t.Fatalf("audit %v, want one record with reason rate: listener", rec.details)
			}
		})
	}
}

// A gate that admits leaves the CONNECT to the rest of the handshake.
func TestConnectRateAdmits(t *testing.T) {
	for _, v := range []packet.Version{packet.V5, packet.V4, packet.V3} {
		t.Run(fmt.Sprint(v), func(t *testing.T) {
			g := &gate{}
			o, a, _, _ := rateOptions(g)
			h := newHarness(t, o, &fakeCore{})
			if ack := h.connect(v, credentialConnect(v)); ack.ReasonCode != packet.Success {
				t.Fatalf("admitted: %+v", ack)
			}
			if g.asked.Load() != 1 || a.calls.Load() != 1 {
				t.Fatalf("gate asked %d times, authenticator %d; want 1 and 1", g.asked.Load(), a.calls.Load())
			}
		})
	}
}

// The rate is decided before the authentication method is looked at: an
// unknown method over the rate is 0x9F, not 0x8C.
func TestConnectRateBeforeMethod(t *testing.T) {
	o, _, m, _ := rateOptions(&gate{limit: "node"})
	h := newHarness(t, o, &fakeCore{})
	h.enc = &packet.Encoder{Version: packet.V5}
	h.dec.Version = packet.V5
	c := methodConnect("X-UNKNOWN", "")
	c.ProtocolLevel = packet.V5
	h.send(c)
	if ack := h.readConnack(); ack.ReasonCode != packet.ConnectionRateExceeded {
		t.Fatalf("an unknown method over the rate: %+v, want 0x9F", ack)
	}
	<-h.done
	if got := m.get("hoardmq_connect_rate_refused_total{limit=node,listener=test}"); got != 1 {
		t.Fatalf("a node refusal counted %v under limit=node, want 1: %v", got, m.m)
	}
}
