package conn

import (
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/packet"
)

// TestRefusalCountedBeforeItsConnack: a client that has a refused CONNECT's
// CONNACK finds the refusal counted, however long the connection takes after
// the write. The seam holds each refusal for 300 ms right after its CONNACK.
func TestRefusalCountedBeforeItsConnack(t *testing.T) {
	afterRefusalConnack = func() { time.Sleep(300 * time.Millisecond) }
	t.Cleanup(func() { afterRefusalConnack = nil })
	for _, c := range []struct {
		name   string
		set    func(o *Options, g *gate, core *fakeCore, connect *packet.Connect)
		want   packet.ReasonCode
		counts []string
	}{
		{"over the rate", func(o *Options, g *gate, _ *fakeCore, _ *packet.Connect) { g.limit = "listener" }, packet.ConnectionRateExceeded,
			[]string{"hoardmq_connect_rate_refused_total{limit=listener,listener=test}", "hoardmq_auth_failures_total{listener=test,reason=Connection rate exceeded (0x9F)}"}},
		{"overload", func(o *Options, g *gate, _ *fakeCore, _ *packet.Connect) { g.limit = LimitOverload }, packet.ServerBusy,
			[]string{"hoardmq_connect_rate_refused_total{limit=overload,listener=test}", "hoardmq_auth_failures_total{listener=test,reason=Server busy (0x89)}"}},
		{"banned", func(o *Options, _ *gate, _ *fakeCore, _ *packet.Connect) {
			o.Banned = func(string, string, bool, string) (string, bool) { return "clientid", true }
		}, packet.Banned,
			[]string{"hoardmq_connects_banned_total{kind=clientid,listener=test}", "hoardmq_auth_failures_total{listener=test,reason=Banned (0x8A)}"}},
		{"a Will Topic", func(_ *Options, _ *gate, _ *fakeCore, connect *packet.Connect) {
			connect.Will = &packet.Will{Topic: "a/#", Payload: []byte("gone")}
		}, packet.TopicNameInvalid,
			[]string{"hoardmq_auth_failures_total{listener=test,reason=Topic Name invalid (0x90)}"}},
		{"the core", func(_ *Options, _ *gate, core *fakeCore, _ *packet.Connect) {
			core.connectResult = ConnectResult{Reason: packet.ServerUnavailable}
		}, packet.ServerUnavailable,
			[]string{"hoardmq_auth_failures_total{listener=test,reason=Server unavailable (0x88)}"}},
		{"authentication", func(o *Options, _ *gate, _ *fakeCore, _ *packet.Connect) {
			o.Authenticators = auth.AuthenticatorChain{always(auth.Deny)}
		}, packet.BadUserNameOrPassword,
			[]string{"hoardmq_auth_failures_total{listener=test,reason=Bad User Name or Password (0x86)}"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			g := &gate{}
			o, _, m, _ := rateOptions(g)
			core := &fakeCore{}
			connect := credentialConnect(packet.V5)
			c.set(&o, g, core, connect)
			h := newHarness(t, o, core)
			h.enc = &packet.Encoder{Version: packet.V5}
			h.dec.Version = packet.V5
			h.send(connect)
			if ack := h.readConnack(); ack.ReasonCode != c.want {
				t.Fatalf("%+v, want %s", ack, c.want)
			}
			for _, k := range c.counts {
				if got := m.get(k); got != 1 {
					t.Errorf("%s = %v as the CONNACK arrived, want 1", k, got)
				}
			}
		})
	}
}
