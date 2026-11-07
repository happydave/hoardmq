package conn

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/happydave/hoardmq/internal/hook"
	"github.com/happydave/hoardmq/internal/packet"
)

// TestRefusalIgnored: only a Drop or Disconnect of a QoS 2 PUBLISH whose
// identifier is held open is set aside, and only a QoS 2 PUBLISH is looked up.
func TestRefusalIgnored(t *testing.T) {
	drop, disc := hook.Outcome{Action: hook.Drop}, hook.Outcome{Action: hook.Disconnect, Reason: packet.ImplementationSpecificError}
	for _, c := range []struct {
		name   string
		p      packet.Packet
		out    hook.Outcome
		open   bool
		want   bool
		lookup bool
	}{
		{"QoS 2 open, drop", &packet.Publish{QoS: 2, PacketID: 7}, drop, true, true, true},
		{"QoS 2 open, disconnect", &packet.Publish{QoS: 2, PacketID: 7}, disc, true, true, true},
		{"QoS 2 not open, drop", &packet.Publish{QoS: 2, PacketID: 7}, drop, false, false, true},
		{"QoS 1, drop", &packet.Publish{QoS: 1, PacketID: 7}, drop, true, false, false},
		{"QoS 0, drop", &packet.Publish{}, drop, true, false, false},
		{"PUBREL, disconnect", &packet.Pubrel{Ack: packet.Ack{PacketID: 7}}, disc, true, false, false},
		{"QoS 2 open, continue", &packet.Publish{QoS: 2, PacketID: 7}, hook.OK, true, false, false},
		{"QoS 2 open, modify", &packet.Publish{QoS: 2, PacketID: 7}, hook.Outcome{Action: hook.Modify}, true, false, false},
	} {
		looked := false
		got := refusalIgnored(c.p, c.out, func(id uint16) bool {
			looked = true
			if id != 7 {
				t.Errorf("%s: looked up %d", c.name, id)
			}
			return c.open
		})
		if got != c.want || looked != c.lookup {
			t.Errorf("%s: ignored %v (looked up %v), want %v (%v)", c.name, got, looked, c.want, c.lookup)
		}
	}
}

// dropAll drops every inbound PUBLISH, as module "hook.x".
type dropAll struct{ pingHooks }

func (dropAll) Inbound(_ context.Context, _ hook.Info, p packet.Packet) (hook.Outcome, string) {
	if _, ok := p.(*packet.Publish); ok {
		return hook.Outcome{Action: hook.Drop}, "hook.x"
	}
	return hook.OK, ""
}

// TestIgnoredRefusalOnTheWire: a dropped QoS 2 PUBLISH under an open
// identifier goes to the core and is answered with its verdict, counted on
// its own and logged at Debug with the module; one under an identifier not
// open is still dropped with 0x80.
func TestIgnoredRefusalOnTheWire(t *testing.T) {
	var logs bytes.Buffer
	var mu sync.Mutex
	o := defaultOptions()
	o.Hooks = dropAll{}
	m := &counters{}
	o.Metrics = m
	o.Logger = slog.New(slog.NewTextHandler(&lockedWriter{mu: &mu, w: &logs}, &slog.HandlerOptions{Level: slog.LevelDebug}))
	core := &fakeCore{open: map[uint16]bool{7: true}}
	h := newHarness(t, o, core)
	if ack := h.connect(packet.V5, nil); ack.ReasonCode != packet.Success {
		t.Fatal(ack)
	}
	h.send(&packet.Publish{Topic: "t", QoS: 2, PacketID: 7, Dup: true, Payload: []byte("x")})
	if r, ok := h.read().(*packet.Pubrec); !ok || r.PacketID != 7 || r.ReasonCode != packet.Success {
		t.Fatalf("the open resend: %#v", r)
	}
	h.send(&packet.Publish{Topic: "t", QoS: 2, PacketID: 8, Payload: []byte("x")})
	if r, ok := h.read().(*packet.Pubrec); !ok || r.PacketID != 8 || r.ReasonCode != packet.UnspecifiedError {
		t.Fatalf("the publish under 8: %#v", r)
	}
	core.mu.Lock()
	n := len(core.publishes)
	core.mu.Unlock()
	if n != 1 {
		t.Fatalf("the core got %d publishes, want the open resend alone", n)
	}
	m.mu.Lock()
	ignored, hooked := m.m["hoardmq_inbound_refusals_ignored_total{namespace=default}"], m.m["hoardmq_drops_total{namespace=default,reason=hook}"]
	m.mu.Unlock()
	if ignored != 1 || hooked != 1 {
		t.Fatalf("ignored %v, hook drops %v; want 1 and 1", ignored, hooked)
	}
	mu.Lock()
	out := logs.String()
	mu.Unlock()
	if !strings.Contains(out, "level=DEBUG") || !strings.Contains(out, "inbound refusal of an open QoS 2 resend ignored") ||
		!strings.Contains(out, "module=hook.x") || !strings.Contains(out, "packetId=7") {
		t.Fatalf("no Debug line naming the module:\n%s", out)
	}
}

type lockedWriter struct {
	mu *sync.Mutex
	w  *bytes.Buffer
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

// TestPublishedBytesCountTheWire: an alias-only PUBLISH adds its payload and
// no topic bytes, even though its alias is resolved before the count.
func TestPublishedBytesCountTheWire(t *testing.T) {
	h := newHarness(t, defaultOptions(), &fakeCore{})
	if ack := h.connect(packet.V5, nil); ack.ReasonCode != packet.Success {
		t.Fatal(ack)
	}
	alias := uint16(1)
	h.send(&packet.Publish{Topic: "abc", QoS: 1, PacketID: 1, Payload: []byte("12345"), Properties: packet.Properties{TopicAlias: &alias}})
	h.read()
	if got := h.conn.PublishedBytes(); got != 8 {
		t.Fatalf("after the naming publish: %d, want 8", got)
	}
	h.send(&packet.Publish{QoS: 1, PacketID: 2, Payload: []byte("123"), Properties: packet.Properties{TopicAlias: &alias}})
	h.read()
	if got := h.conn.PublishedBytes(); got != 11 {
		t.Fatalf("after the alias-only publish: %d, want 11", got)
	}
}
