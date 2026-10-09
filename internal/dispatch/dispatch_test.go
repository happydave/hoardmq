package dispatch

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/session"
	"github.com/happydave/hoardmq/internal/topic"
)

// The dispatch package is exercised through the session manager, which is
// the only way to obtain *session.Session values; a fake connection stands
// in for the connection layer.

type fakeConn struct {
	id       string
	version  packet.Version
	quota    int
	sent     []*packet.Publish
	identity auth.Identity
}

func newConn(id string) *fakeConn {
	return &fakeConn{id: id, version: packet.V5, quota: 100, identity: auth.Identity{ClientID: id}}
}

func (f *fakeConn) ClientID() string               { return f.id }
func (f *fakeConn) Version() packet.Version        { return f.version }
func (f *fakeConn) Identity() *auth.Identity       { return &f.identity }
func (f *fakeConn) SessionExpiryOverride() *uint32 { return nil }
func (f *fakeConn) Send(packet.Packet) error       { return nil }
func (f *fakeConn) Close(packet.ReasonCode)        {}
func (f *fakeConn) SendQuota() int                 { return f.quota }
func (f *fakeConn) Deliver(p *packet.Publish) error {
	if p.QoS > 0 {
		f.quota--
	}
	f.sent = append(f.sent, p)
	return nil
}
func (f *fakeConn) payloads() string {
	var parts []string
	for _, p := range f.sent {
		parts = append(parts, string(p.Payload))
	}
	return strings.Join(parts, ",")
}

type allow struct{}

func (allow) Authorize(context.Context, auth.Action, string, *auth.Identity) auth.Decision {
	return auth.Allow
}

type harness struct {
	t      *testing.T
	m      *session.Manager
	r      *Router
	clock  *fakeClock
	conns  map[string]*fakeConn
	nextID uint16
}

func newHarness(t *testing.T, strategy Strategy) *harness {
	r := New(strategy)
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	m := session.New(session.Options{Router: r, Authorizers: auth.AuthorizerChain{allow{}}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Clock: clock, MaxQueued: 100})
	return &harness{t: t, m: m, r: r, clock: clock, conns: map[string]*fakeConn{}}
}

func (h *harness) connect(id string, clean bool, expiry uint32) *fakeConn {
	return h.connectAt(id, packet.V5, clean, expiry)
}

func (h *harness) connectAt(id string, v packet.Version, clean bool, expiry uint32) *fakeConn {
	c := newConn(id)
	c.version = v
	h.conns[id] = c
	pkt := &packet.Connect{ProtocolLevel: v, ClientID: id, CleanStart: clean}
	if expiry != 0 {
		pkt.Properties.SessionExpiryInterval = &expiry
	}
	h.m.Connect(c, pkt, 0)
	h.m.Resume(c)
	return c
}

func (h *harness) subscribe(c *fakeConn, id uint32, subs ...packet.Subscription) []packet.ReasonCode {
	p := &packet.Subscribe{PacketID: 1, Subscriptions: subs}
	if id != 0 {
		p.Properties.SubscriptionIdentifiers = []uint32{id}
	}
	codes := h.m.Subscribe(c, p)
	h.m.Subscribed(c)
	return codes
}

func (h *harness) publish(c *fakeConn, tp string, qos byte, payload string, retain bool) packet.ReasonCode {
	h.nextID++
	code, _ := h.m.Publish(c, &packet.Publish{Topic: tp, QoS: qos, PacketID: h.nextID, Payload: []byte(payload), Retain: retain})
	return code
}

type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time { return c.now }
func (c *fakeClock) AfterFunc(time.Duration, func()) session.Timer {
	return stopper{}
}

type stopper struct{}

func (stopper) Stop() bool { return true }

func sub(filter string, qos byte) packet.Subscription {
	return packet.Subscription{Filter: filter, QoS: qos}
}

func TestWildcardMatchingAgainstTopicMatch(t *testing.T) {
	h := newHarness(t, RoundRobin)
	pub := h.connect("pub", true, 0)
	filters := []string{"a", "a/b", "a/+", "+/b", "+/+", "a/#", "#", "+/#", "$SYS/#", "$SYS/+", "+/x/#", "a/b/c", "a/+/c", "/", "/+", "a/"}
	subs := map[string]*fakeConn{}
	for i, f := range filters {
		c := h.connect(fmt.Sprintf("s%02d", i), true, 0)
		h.subscribe(c, 0, sub(f, 0))
		subs[f] = c
	}
	topics := []string{"a", "a/b", "a/c", "b/b", "a/b/c", "a/x/c", "a/x/y", "$SYS/a", "$SYS/a/b", "/", "/x", "a/", "x", "a/b/c/d"}
	for _, tp := range topics {
		for _, c := range subs {
			c.sent = nil
		}
		h.publish(pub, tp, 0, tp, false)
		for _, f := range filters {
			want := topic.Match(f, tp)
			got := len(subs[f].sent) == 1
			if want != got {
				t.Errorf("filter %q topic %q: trie %v, topic.Match %v", f, tp, got, want)
			}
		}
	}
}

func TestRandomizedTrieMatchesTopicMatch(t *testing.T) {
	h := newHarness(t, RoundRobin)
	pub := h.connect("pub", true, 0)
	rng := rand.New(rand.NewSource(42))
	levels := []string{"a", "b", "c", "+", "#", "$x", ""}
	var filters []string
	subs := map[string]*fakeConn{}
	for i := 0; len(filters) < 60 && i < 1000; i++ {
		n := 1 + rng.Intn(3)
		parts := make([]string, n)
		for j := range parts {
			parts[j] = levels[rng.Intn(len(levels))]
		}
		f := strings.Join(parts, "/")
		if topic.ValidateFilter(f) != "" {
			continue
		}
		if _, dup := subs[f]; dup {
			continue
		}
		c := h.connect(fmt.Sprintf("r%03d", len(filters)), true, 0)
		h.subscribe(c, 0, sub(f, 0))
		subs[f] = c
		filters = append(filters, f)
	}
	names := []string{"a", "b", "c", "$x", "d", ""}
	for i := 0; i < 300; i++ {
		n := 1 + rng.Intn(3)
		parts := make([]string, n)
		for j := range parts {
			parts[j] = names[rng.Intn(len(names))]
		}
		tp := strings.Join(parts, "/")
		if topic.ValidateName(tp) != "" {
			continue
		}
		for _, c := range subs {
			c.sent = nil
		}
		h.publish(pub, tp, 0, "x", false)
		for _, f := range filters {
			if want, got := topic.Match(f, tp), len(subs[f].sent) == 1; want != got {
				t.Fatalf("filter %q topic %q: trie %v, topic.Match %v", f, tp, got, want)
			}
		}
	}
}

func TestOverlappingSubscriptionsOneDelivery(t *testing.T) {
	h := newHarness(t, RoundRobin)
	pub := h.connect("pub", true, 0)
	c := h.connect("c", true, 0)
	h.subscribe(c, 7, sub("a/b", 0))
	h.subscribe(c, 9, sub("a/+", 2))
	h.subscribe(c, 0, sub("#", 1))
	h.publish(pub, "a/b", 2, "once", false)
	if len(c.sent) != 1 {
		t.Fatalf("delivered %d times", len(c.sent))
	}
	p := c.sent[0]
	if p.QoS != 2 || len(p.Properties.SubscriptionIdentifiers) != 2 || p.Properties.SubscriptionIdentifiers[0] != 7 || p.Properties.SubscriptionIdentifiers[1] != 9 {
		t.Fatalf("%+v", p)
	}
	// Replacing a subscription updates its options in place.
	h.subscribe(c, 8, sub("a/b", 1))
	c.sent = nil
	h.publish(pub, "a/b", 0, "again", false)
	if ids := c.sent[0].Properties.SubscriptionIdentifiers; len(ids) != 2 || ids[0] != 8 {
		t.Fatalf("%v", ids)
	}
	// Unsubscribe one, the others still match; unsubscribe all, nothing does.
	h.m.Unsubscribe(c, &packet.Unsubscribe{PacketID: 2, Filters: []string{"a/+"}})
	c.sent = nil
	h.publish(pub, "a/b", 0, "x", false)
	if len(c.sent) != 1 || len(c.sent[0].Properties.SubscriptionIdentifiers) != 1 {
		t.Fatalf("%+v", c.sent)
	}
	h.m.Unsubscribe(c, &packet.Unsubscribe{PacketID: 3, Filters: []string{"a/b", "#"}})
	c.sent = nil
	h.publish(pub, "a/b", 0, "x", false)
	if len(c.sent) != 0 {
		t.Fatal("delivered after unsubscribe")
	}
}

func TestNoLocalAndRetainAsPublished(t *testing.T) {
	h := newHarness(t, RoundRobin)
	c := h.connect("c", true, 0)
	other := h.connect("o", true, 0)
	h.subscribe(c, 0, packet.Subscription{Filter: "t", NoLocal: true})
	h.subscribe(other, 0, packet.Subscription{Filter: "t", RetainAsPublished: true})
	h.publish(c, "t", 0, "mine", true)
	if len(c.sent) != 0 {
		t.Error("No Local delivered the publisher's own message")
	}
	if len(other.sent) != 1 || !other.sent[0].Retain {
		t.Errorf("Retain As Published: %+v", other.sent)
	}
	other.sent = nil
	h.publish(c, "t", 0, "plain", false)
	if other.sent[0].Retain {
		t.Error("RETAIN set on a message not published retained")
	}
	// A level 4 subscriber sees RETAIN on the stored message sent at
	// subscribe time, never on a forwarded message (Retain As Published is
	// not a 3.x option, whatever the decoded packet claims), and no
	// subscription identifiers.
	v4 := h.connectAt("v4", packet.V4, true, 0)
	h.subscribe(v4, 5, packet.Subscription{Filter: "t", RetainAsPublished: true})
	if len(v4.sent) != 1 || !v4.sent[0].Retain || string(v4.sent[0].Payload) != "mine" {
		t.Fatalf("level 4 retained on subscribe: %+v", v4.sent)
	}
	v4.sent = nil
	h.publish(c, "t", 0, "r", true)
	if p := v4.sent[0]; p.Retain || len(p.Properties.SubscriptionIdentifiers) != 0 {
		t.Errorf("level 4 forwarded: %+v", p)
	}
}

func TestRetainedStoreAndHandling(t *testing.T) {
	h := newHarness(t, RoundRobin)
	pub := h.connect("pub", true, 0)
	h.publish(pub, "r/1", 1, "one", true)
	h.publish(pub, "r/2", 0, "two", true)
	h.publish(pub, "r/2", 0, "two-b", true)
	h.publish(pub, "$SYS/x", 0, "sys", true)
	if h.r.RetainedCount() != 3 {
		t.Fatalf("retained %d", h.r.RetainedCount())
	}
	c := h.connect("c", true, 0)
	h.subscribe(c, 3, sub("r/#", 0))
	if got := c.payloads(); got != "one,two-b" {
		t.Fatalf("wildcard retained: %s", got)
	}
	for _, p := range c.sent {
		if !p.Retain || p.QoS != 0 || p.Properties.SubscriptionIdentifiers[0] != 3 {
			t.Errorf("retained delivery %+v", p)
		}
	}
	// Retain Handling 1: not again for an existing subscription.
	c.sent = nil
	h.subscribe(c, 0, packet.Subscription{Filter: "r/#", RetainHandling: 1})
	if len(c.sent) != 0 {
		t.Error("Retain Handling 1 resent for an existing subscription")
	}
	h.subscribe(c, 0, packet.Subscription{Filter: "r/1", RetainHandling: 1})
	if got := c.payloads(); got != "one" {
		t.Errorf("Retain Handling 1 new subscription: %s", got)
	}
	// Retain Handling 2: never; Retain Handling 0: again.
	c.sent = nil
	h.subscribe(c, 0, packet.Subscription{Filter: "r/1", RetainHandling: 2})
	if len(c.sent) != 0 {
		t.Error("Retain Handling 2 sent")
	}
	h.subscribe(c, 0, packet.Subscription{Filter: "r/1", RetainHandling: 0})
	if got := c.payloads(); got != "one" {
		t.Errorf("Retain Handling 0: %s", got)
	}
	// $ exclusion and explicit $SYS.
	c.sent = nil
	h.subscribe(c, 0, sub("#", 0))
	if got := c.payloads(); got != "one,two-b" {
		t.Errorf("# must not include $SYS: %s", got)
	}
	c.sent = nil
	h.subscribe(c, 0, sub("$SYS/+", 0))
	if got := c.payloads(); got != "sys" {
		t.Errorf("$SYS/+: %s", got)
	}
	// Level 4 always gets retained on subscribe regardless of handling.
	v4 := h.connectAt("v4", packet.V4, true, 0)
	h.subscribe(v4, 0, packet.Subscription{Filter: "r/1", RetainHandling: 2})
	if got := v4.payloads(); got != "one" {
		t.Errorf("level 4 retained: %s", got)
	}
	// Empty payload removes; nobody gets it on subscribe afterwards.
	h.publish(pub, "r/1", 0, "", true)
	c.sent = nil
	h.subscribe(c, 0, sub("r/1", 0))
	if len(c.sent) != 0 || h.r.RetainedCount() != 2 {
		t.Errorf("removal: sent %d retained %d", len(c.sent), h.r.RetainedCount())
	}
}

func TestRetainedExpiry(t *testing.T) {
	h := newHarness(t, RoundRobin)
	pub := h.connect("pub", true, 0)
	exp := uint32(10)
	h.m.Publish(pub, &packet.Publish{Topic: "e", Retain: true, Payload: []byte("x"), Properties: packet.Properties{MessageExpiryInterval: &exp}})
	h.clock.now = h.clock.now.Add(4 * time.Second)
	c := h.connect("c", true, 0)
	h.subscribe(c, 0, sub("e", 0))
	if len(c.sent) != 1 || *c.sent[0].Properties.MessageExpiryInterval != 6 {
		t.Fatalf("%+v", c.sent)
	}
	h.clock.now = h.clock.now.Add(7 * time.Second)
	c.sent = nil
	h.subscribe(c, 0, sub("e", 0))
	if len(c.sent) != 0 {
		t.Error("expired retained delivered")
	}
	h.m.Publish(pub, &packet.Publish{Topic: "f", Retain: true, Payload: []byte("x"), Properties: packet.Properties{MessageExpiryInterval: &exp}})
	h.clock.now = h.clock.now.Add(time.Hour)
	h.r.Sweep(h.clock.now)
	if h.r.RetainedCount() != 0 {
		t.Errorf("sweep left %d", h.r.RetainedCount())
	}
}

func TestSharedStrategies(t *testing.T) {
	for _, strategy := range []Strategy{RoundRobin, LocalFirst, Random, Sticky, Hash} {
		t.Run(string(strategy), func(t *testing.T) {
			h := newHarness(t, strategy)
			pub := h.connect("pub", true, 0)
			pub2 := h.connect("pub2", true, 0)
			a := h.connect("a", true, 0)
			b := h.connect("b", true, 0)
			h.subscribe(a, 0, sub("$share/g/t", 1))
			h.subscribe(b, 0, sub("$share/g/t", 1))
			for i := 0; i < 6; i++ {
				h.publish(pub, "t", 1, "m", false)
			}
			total := len(a.sent) + len(b.sent)
			if total != 6 {
				t.Fatalf("group delivered %d of 6", total)
			}
			switch strategy {
			case RoundRobin, LocalFirst:
				if len(a.sent) != 3 || len(b.sent) != 3 {
					t.Errorf("round robin split %d/%d", len(a.sent), len(b.sent))
				}
			case Sticky, Hash:
				if len(a.sent) != 6 && len(b.sent) != 6 {
					t.Errorf("%s split %d/%d", strategy, len(a.sent), len(b.sent))
				}
			}
			if strategy == Hash {
				a.sent, b.sent = nil, nil
				for i := 0; i < 4; i++ {
					h.publish(pub2, "t", 1, "m", false)
				}
				if len(a.sent) != 4 && len(b.sent) != 4 {
					t.Errorf("hash not consistent per publisher: %d/%d", len(a.sent), len(b.sent))
				}
			}
		})
	}
}

func TestSharedOfflineMembersAndRedistribution(t *testing.T) {
	h := newHarness(t, RoundRobin)
	pub := h.connect("pub", true, 0)
	a := h.connect("a", false, 3600)
	b := h.connect("b", false, 3600)
	h.subscribe(a, 0, sub("$share/g/t", 1))
	h.subscribe(b, 0, sub("$share/g/t", 1))
	// b disconnects with a live session: it is skipped while a is online.
	h.m.Disconnect(b, false, 0, false)
	for i := 0; i < 4; i++ {
		h.publish(pub, "t", 1, "m", false)
	}
	if len(a.sent) != 4 {
		t.Fatalf("online member got %d of 4", len(a.sent))
	}
	if rec, _ := h.m.Record("b"); rec.Queued != 0 {
		t.Fatalf("offline member queued %d", rec.Queued)
	}
	// Everyone offline: messages queue on one member.
	h.m.Disconnect(a, false, 0, false)
	h.publish(pub, "t", 1, "q1", false)
	h.publish(pub, "t", 1, "q2", false)
	ra, _ := h.m.Record("a")
	rb, _ := h.m.Record("b")
	if ra.Queued+rb.Queued != 2 {
		t.Fatalf("all-offline queued %d+%d", ra.Queued, rb.Queued)
	}
	// Round robin spread the two across both offline members. When one
	// member's session ends by Clean Start, its QoS 1 messages go to the
	// other member; a QoS 2 in flight would not.
	holder, other := "a", "b"
	if rb.Queued > ra.Queued {
		holder, other = "b", "a"
	}
	// The holder's unacknowledged in-flight QoS 1 messages count too.
	holderRec, _ := h.m.Record(holder)
	held := holderRec.Queued + holderRec.InFlight
	otherConn := h.connect(other, false, 3600)
	before := len(otherConn.sent)
	h.connect(holder, true, 0)
	after := otherConn.sent[before:]
	if len(after) != held || held == 0 {
		t.Fatalf("redistributed %d, holder had %d", len(after), held)
	}
	// QoS 2 in flight is dropped when the member's session ends.
	x := h.connect("x", false, 3600)
	y := h.connect("y", false, 3600)
	h.subscribe(x, 0, sub("$share/h/u", 2))
	h.subscribe(y, 0, sub("$share/h/u", 2))
	h.publish(pub, "u", 2, "q2msg", false)
	target, spare := x, y
	if len(y.sent) == 1 {
		target, spare = y, x
	}
	if len(target.sent) != 1 || target.sent[0].QoS != 2 {
		t.Fatalf("qos 2 shared delivery %+v %+v", x.sent, y.sent)
	}
	spareBefore := len(spare.sent)
	h.connect(target.id, true, 0)
	if len(spare.sent) != spareBefore {
		t.Error("QoS 2 in flight was redistributed")
	}
}

func TestSharedNoRetainedAndLevel4(t *testing.T) {
	h := newHarness(t, RoundRobin)
	pub := h.connect("pub", true, 0)
	h.publish(pub, "t", 0, "kept", true)
	c := h.connect("c", true, 0)
	h.subscribe(c, 0, sub("$share/g/t", 0))
	if len(c.sent) != 0 {
		t.Fatal("retained message sent on a shared subscribe")
	}
	v4 := h.connectAt("v4", packet.V4, true, 0)
	h.subscribe(v4, 0, sub("$share/g/t", 1))
	h.publish(pub, "t", 1, "one", false)
	h.publish(pub, "t", 1, "two", false)
	if len(c.sent)+len(v4.sent) != 2 || len(c.sent) != 1 {
		t.Fatalf("shared with level 4 member: %d/%d", len(c.sent), len(v4.sent))
	}
	// Direct and shared subscriptions on the same session both deliver.
	h.subscribe(c, 0, sub("t", 0))
	c.sent, v4.sent = nil, nil
	h.publish(pub, "t", 0, "both", false)
	h.publish(pub, "t", 0, "both", false)
	if len(c.sent) != 3 {
		t.Errorf("direct plus shared: %d", len(c.sent))
	}
	// Unsubscribing the shared filter keeps the direct one.
	h.m.Unsubscribe(c, &packet.Unsubscribe{PacketID: 2, Filters: []string{"$share/g/t"}})
	c.sent, v4.sent = nil, nil
	h.publish(pub, "t", 0, "direct", false)
	if len(c.sent) != 1 || len(v4.sent) != 1 {
		t.Errorf("after shared unsubscribe: %d/%d", len(c.sent), len(v4.sent))
	}
	msg := &message.Message{Namespace: "default", Topic: "t"}
	if _, ok := h.r.MatchGroup("default", "nope/t", msg, nil); ok {
		t.Error("unknown group matched")
	}
}

func TestTriePruneAndRemoveAll(t *testing.T) {
	h := newHarness(t, RoundRobin)
	c := h.connect("c", true, 0)
	h.subscribe(c, 0, sub("a/b/c", 0), sub("a/+/d", 0), sub("$share/g/a/#", 0))
	h.m.Unsubscribe(c, &packet.Unsubscribe{PacketID: 1, Filters: []string{"a/b/c"}})
	root := h.r.roots["default"]
	if root.walk("a/b", false) != nil {
		t.Error("empty branch not pruned")
	}
	if root.walk("a/+/d", false) == nil {
		t.Error("live branch pruned")
	}
	h.connect("c", true, 0) // Clean Start discards the session: RemoveAll.
	if len(root.children) != 0 {
		t.Errorf("trie not empty after RemoveAll: %v", root.children)
	}
}
