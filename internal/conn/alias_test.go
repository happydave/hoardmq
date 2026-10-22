package conn

import (
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/packet"
)

func TestAliasFits(t *testing.T) {
	for _, c := range []struct {
		held, replaced, next, bound int
		fits                        bool
	}{
		{5, 0, 5, 10, true},
		{5, 0, 6, 10, false},
		{10, 4, 4, 10, true},
		{9, 2, 4, 10, false},
		{0, 0, 11, 10, false},
		{1 << 30, 0, 1 << 20, 0, true},
	} {
		if got := aliasFits(c.held, c.replaced, c.next, c.bound); got != c.fits {
			t.Errorf("aliasFits(%d, %d, %d, %d) = %v", c.held, c.replaced, c.next, c.bound, got)
		}
	}
}

// publishAliased sends a QoS 1 PUBLISH carrying an alias and returns the
// PUBACK's reason.
func publishAliased(h *harness, id, alias uint16, topic string) packet.ReasonCode {
	h.t.Helper()
	h.send(&packet.Publish{Topic: topic, QoS: 1, PacketID: id, Properties: packet.Properties{TopicAlias: &alias}, Payload: []byte("x")})
	p := h.read()
	ack, ok := p.(*packet.Puback)
	if !ok || ack.PacketID != id {
		h.t.Fatalf("publish %d (alias %d, %q): %T %+v", id, alias, topic, p, p)
	}
	return ack.ReasonCode
}

func (f *fakeCore) topics() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.publishes))
	for _, p := range f.publishes {
		out = append(out, p.Topic)
	}
	return out
}

// The inbound alias table holds at most MaxTopicAliasBytes of topics,
// replacing a mapping counting the difference; past it the connection closes
// with Quota exceeded and the PUBLISH is not processed.
func TestTopicAliasBytesBounded(t *testing.T) {
	opts := defaultOptions()
	opts.MaxTopicAliasBytes = 10
	core := &fakeCore{}
	h := newHarness(t, opts, core)
	h.connect(packet.V5, nil)
	for i, c := range []struct {
		alias uint16
		topic string
	}{{1, "abcde"}, {2, "fghij"}, {1, "abcd"}, {3, "z"}} {
		if r := publishAliased(h, uint16(i+1), c.alias, c.topic); r != packet.Success {
			t.Fatalf("alias %d %q within the bound: %s", c.alias, c.topic, r)
		}
	}
	alias := uint16(4)
	h.send(&packet.Publish{Topic: "y", QoS: 1, PacketID: 9, Properties: packet.Properties{TopicAlias: &alias}})
	if d := h.readEnd(); d == nil || d.ReasonCode != packet.QuotaExceeded {
		t.Fatalf("past the bound: %+v, want DISCONNECT 0x97", d)
	}
	if got := strings.Join(core.topics(), ","); got != "abcde,fghij,abcd,z" {
		t.Fatalf("processed %q", got)
	}
}

func TestTopicAliasBytesUnlimitedAtZero(t *testing.T) {
	opts := defaultOptions()
	opts.MaxTopicAliasBytes = 0
	opts.Capabilities.TopicAliasMaximum = 300
	core := &fakeCore{}
	h := newHarness(t, opts, core)
	h.connect(packet.V5, nil)
	for i := 0; i < 300; i++ {
		alias := uint16(i + 1)
		h.send(&packet.Publish{Topic: "t/" + strings.Repeat("y", 190) + string(rune('a'+i%26)), Properties: packet.Properties{TopicAlias: &alias}})
	}
	h.send(&packet.Pingreq{})
	if p := h.read(); p.Type() != packet.PINGRESP {
		t.Fatalf("after 300 aliases of about 200 bytes: %T %+v", p, p)
	}
	if n := len(core.topics()); n != 300 {
		t.Fatalf("processed %d of 300", n)
	}
}

// A mapping grants nothing: an alias set by a refused publish, or replaced by
// one, names the refused topic, and using it alone is refused again rather
// than closing the connection or reaching the topic it named before.
func TestRefusedPublishAliasIsRefusedAgain(t *testing.T) {
	opts := defaultOptions()
	opts.Authorizers = auth.AuthorizerChain{denyTopic("secret/x")}
	core := &fakeCore{}
	h := newHarness(t, opts, core)
	h.connect(packet.V5, nil)
	if r := publishAliased(h, 1, 1, "secret/x"); r != packet.NotAuthorized {
		t.Fatalf("refused topic with alias 1: %s", r)
	}
	if r := publishAliased(h, 2, 1, ""); r != packet.NotAuthorized {
		t.Fatalf("alias 1 alone: %s", r)
	}
	if r := publishAliased(h, 3, 2, "ok/a"); r != packet.Success {
		t.Fatalf("allowed topic with alias 2: %s", r)
	}
	if r := publishAliased(h, 4, 2, "secret/x"); r != packet.NotAuthorized {
		t.Fatalf("alias 2 replaced by the refused topic: %s", r)
	}
	if r := publishAliased(h, 5, 2, ""); r != packet.NotAuthorized {
		t.Fatalf("alias 2 alone after the refused replacement: %s", r)
	}
	h.send(&packet.Pingreq{})
	if p := h.read(); p.Type() != packet.PINGRESP {
		t.Fatalf("connection after the refusals: %T %+v", p, p)
	}
	if got := strings.Join(core.topics(), ","); got != "ok/a" {
		t.Fatalf("processed %q, want only ok/a", got)
	}
}

// A close for the alias bound is the server's decision, not the client's
// fault, so the flapping detector does not count it.
func TestTopicAliasBoundCloseIsNotAFlap(t *testing.T) {
	opts := defaultOptions()
	opts.MaxTopicAliasBytes = 4
	ended := make(chan Ending, 1)
	opts.OnEnded = func(_ *Conn, how Ending) { ended <- how }
	h := newHarness(t, opts, &fakeCore{})
	h.connect(packet.V5, nil)
	alias := uint16(1)
	h.send(&packet.Publish{Topic: "abcde", Properties: packet.Properties{TopicAlias: &alias}})
	if d := h.readEnd(); d == nil || d.ReasonCode != packet.QuotaExceeded {
		t.Fatalf("past the bound: %+v", d)
	}
	select {
	case how := <-ended:
		if how != EndedByServer {
			t.Fatalf("ended %v, want by the server", how)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the connection's end was not reported")
	}
}
