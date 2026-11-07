package broker

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/module"
	"github.com/happydave/hoardmq/internal/module/testmod"
	"github.com/happydave/hoardmq/internal/packet"
)

// index is the position of the first recorded call equal to want, or -1.
func index(calls []string, want string) int {
	for i, c := range calls {
		if c == want {
			return i
		}
	}
	return -1
}

// TestInboundSeesTheAliasResolved: an inbound interceptor is handed an
// alias-only PUBLISH with the topic the alias stands for, as the admission
// check after it is.
func TestInboundSeesTheAliasResolved(t *testing.T) {
	f := withTestModule(t, nil)
	sub, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "sub", CleanStart: true}, false)
	sub.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "a/b", QoS: 1}}})
	sub.read()
	pub, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	alias := uint16(1)
	pub.send(&packet.Publish{Topic: "a/b", QoS: 1, PacketID: 1, Payload: []byte("named"), Properties: packet.Properties{TopicAlias: &alias}})
	if a := pub.read().(*packet.Puback); a.ReasonCode != packet.Success {
		t.Fatalf("the naming publish: %s", a.ReasonCode)
	}
	pub.send(&packet.Publish{Topic: "", QoS: 1, PacketID: 2, Payload: []byte("aliased"), Properties: packet.Properties{TopicAlias: &alias}})
	if a := pub.read().(*packet.Puback); a.ReasonCode != packet.Success {
		t.Fatalf("the alias-only publish: %s", a.ReasonCode)
	}
	for _, want := range []string{"named", "aliased"} {
		if p := sub.readPublish(); p.Topic != "a/b" || string(p.Payload) != want {
			t.Fatalf("delivered %q %q, want a/b %q", p.Topic, p.Payload, want)
		}
	}
	calls := testmod.Shared.Recorded()
	if index(calls, `inbound PUBLISH "a/b" id=2 dup=false`) < 0 || index(calls, `inbound PUBLISH "" id=2 dup=false`) >= 0 {
		t.Fatalf("want the interceptor to see a/b for the alias-only publish; got:\n%s", strings.Join(calls, "\n"))
	}
}

// TestDroppedPublishKeepsItsAlias: an alias a PUBLISH sets is stored before
// the interceptors, so it holds even when they drop that PUBLISH.
func TestDroppedPublishKeepsItsAlias(t *testing.T) {
	f := withTestModule(t, nil)
	sub, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "sub", CleanStart: true}, false)
	sub.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "x", QoS: 1}}})
	sub.read()
	pub, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	testmod.Shared.Set(func(c *testmod.Control) { c.Trigger, c.InboundOutcome = "first", module.Outcome{Action: module.Drop} })
	alias := uint16(2)
	pub.send(&packet.Publish{Topic: "x", QoS: 1, PacketID: 1, Payload: []byte("first"), Properties: packet.Properties{TopicAlias: &alias}})
	if a := pub.read().(*packet.Puback); a.ReasonCode != packet.UnspecifiedError {
		t.Fatalf("the dropped publish: %s", a.ReasonCode)
	}
	pub.send(&packet.Publish{QoS: 1, PacketID: 2, Payload: []byte("second"), Properties: packet.Properties{TopicAlias: &alias}})
	if a, ok := pub.read().(*packet.Puback); !ok || a.ReasonCode != packet.Success {
		t.Fatalf("the alias-only publish after the drop: %#v, want PUBACK Success", a)
	}
	if p := sub.readPublish(); p.Topic != "x" || string(p.Payload) != "second" {
		t.Fatalf("delivered %q %q", p.Topic, p.Payload)
	}
}

// TestPublishChecksRunBeforeInbound: a PUBLISH a check closes, with that
// check's code, never reaches the inbound family.
func TestPublishChecksRunBeforeInbound(t *testing.T) {
	f := withTestModule(t, func(d *config.Document) { d.Limits.MaxTopicAliasBytes = 4 })
	zero, one, big := uint16(0), uint16(1), uint16(9)
	for _, c := range []struct {
		name string
		p    *packet.Publish
		want packet.ReasonCode
	}{
		// The decoder refuses alias 0 itself, so this row holds the code, not the order.
		{"alias 0", &packet.Publish{Topic: "t0", Payload: []byte("p"), Properties: packet.Properties{TopicAlias: &zero}}, packet.TopicAliasInvalid},
		{"unknown alias", &packet.Publish{Payload: []byte("p"), Properties: packet.Properties{TopicAlias: &big}}, packet.ProtocolError},
		{"past the byte bound", &packet.Publish{Topic: "long/topic", Payload: []byte("p"), Properties: packet.Properties{TopicAlias: &one}}, packet.QuotaExceeded},
	} {
		testmod.Shared.Reset()
		cl, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "c", CleanStart: true}, false)
		cl.send(c.p)
		got := cl.readEnd()
		if len(got) != 1 {
			t.Fatalf("%s: read %v before the close", c.name, got)
		}
		if d, ok := got[0].(*packet.Disconnect); !ok || d.ReasonCode != c.want {
			t.Fatalf("%s: %#v, want DISCONNECT %s", c.name, got[0], c.want)
		}
		if calls := testmod.Shared.Recorded(); has(calls, "inbound PUBLISH") {
			t.Errorf("%s: the interceptor saw it:\n%s", c.name, strings.Join(calls, "\n"))
		}
	}
}

// TestInboundRunsBeforeAuthorization: the inbound family is called before
// the authorizers, for a publish they then refuse, and an interceptor's
// refusal pre-empts them, so the client is answered with the interceptor's
// code, not authorization's.
func TestInboundRunsBeforeAuthorization(t *testing.T) {
	f := withTestModule(t, func(d *config.Document) {
		acl := filepath.Join(t.TempDir(), "acl.json")
		rules := `{"rules":[{"topic":"deny/#","deny":["publish"]},{"topic":"#","allow":["publish","subscribe","unsubscribe"]}]}`
		if err := os.WriteFile(acl, []byte(rules), 0o600); err != nil {
			t.Fatal(err)
		}
		d.Modules["authz.file"] = json.RawMessage(`{"path":"` + acl + `"}`)
		// hook.test's authorizer first, so its call is recorded before
		// authz.file decides.
		d.Hooks.Order = []string{string(testmod.ID)}
	})
	pub, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	pub.send(&packet.Publish{Topic: "deny/x", QoS: 1, PacketID: 1, Payload: []byte("p")})
	if a := pub.read().(*packet.Puback); a.ReasonCode != packet.NotAuthorized {
		t.Fatalf("a publish authorization refuses: %s", a.ReasonCode)
	}
	calls := testmod.Shared.Recorded()
	in, az := index(calls, `inbound PUBLISH "deny/x" id=1 dup=false`), index(calls, "authorize publish deny/x")
	if in < 0 || az < 0 || in > az || index(calls, "admit deny/x") >= 0 {
		t.Fatalf("want inbound before authorize and no admission; got:\n%s", strings.Join(calls, "\n"))
	}

	testmod.Shared.Reset()
	testmod.Shared.Set(func(c *testmod.Control) { c.Trigger, c.InboundOutcome = "q", module.Outcome{Action: module.Drop} })
	pub.send(&packet.Publish{Topic: "deny/y", QoS: 1, PacketID: 2, Payload: []byte("q")})
	if a := pub.read().(*packet.Puback); a.ReasonCode != packet.UnspecifiedError {
		t.Fatalf("an interceptor's drop of a publish authorization would refuse: %s, want 0x80", a.ReasonCode)
	}
	if calls := testmod.Shared.Recorded(); index(calls, "authorize publish deny/y") >= 0 {
		t.Fatalf("authorization ran after the interceptor dropped:\n%s", strings.Join(calls, "\n"))
	}
}

// TestInboundCannotRefuseAnOpenResend: a QoS 2 resend of an accepted
// publish reaches the inbound family, but its drop there is not honoured:
// the resend is answered Success, counted on its own, and the client's
// exchange completes, so its next message under the identifier is delivered.
// At 3.1.1 the connection stays open.
func TestInboundCannotRefuseAnOpenResend(t *testing.T) {
	f := withTestModule(t, nil)
	sub, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "sub", CleanStart: true}, false)
	sub.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "q", QoS: 0}}})
	sub.read()
	pub, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	pub.send(&packet.Publish{Topic: "q", QoS: 2, PacketID: 7, Payload: []byte("once")})
	if a := pub.read().(*packet.Pubrec); a.ReasonCode != packet.Success {
		t.Fatalf("the publish: %s", a.ReasonCode)
	}
	if p := sub.readPublish(); string(p.Payload) != "once" {
		t.Fatalf("delivered %q", p.Payload)
	}
	hookDrops := f.series("hoardmq_drops_total", map[string]string{"namespace": "default", "reason": "hook"})

	testmod.Shared.Set(func(c *testmod.Control) { c.Trigger, c.InboundOutcome = "once", module.Outcome{Action: module.Drop} })
	pub.send(&packet.Publish{Topic: "q", QoS: 2, PacketID: 7, Dup: true, Payload: []byte("once")})
	if a := pub.read().(*packet.Pubrec); a.ReasonCode != packet.Success {
		t.Fatalf("the resend an interceptor drops: %s, want Success", a.ReasonCode)
	}
	if calls := testmod.Shared.Recorded(); index(calls, `inbound PUBLISH "q" id=7 dup=true`) < 0 {
		t.Fatalf("the interceptor did not see the resend:\n%s", strings.Join(calls, "\n"))
	}
	sub.expectNoPacket()
	if got := f.series("hoardmq_inbound_refusals_ignored_total", map[string]string{"namespace": "default"}); got != 1 {
		t.Fatalf("ignored refusals %v, want 1", got)
	}
	if got := f.series("hoardmq_drops_total", map[string]string{"namespace": "default", "reason": "hook"}); got != hookDrops {
		t.Fatalf("hook drops moved from %v to %v", hookDrops, got)
	}
	// The client was answered Success, so it completes the exchange and may
	// reuse the identifier; the next message under it is delivered.
	pub.send(&packet.Pubrel{Ack: packet.Ack{PacketID: 7}})
	if c, ok := pub.read().(*packet.Pubcomp); !ok || c.PacketID != 7 {
		t.Fatalf("no PUBCOMP for 7: %#v", c)
	}
	testmod.Shared.Set(func(c *testmod.Control) { c.Trigger = "" })
	pub.send(&packet.Publish{Topic: "q", QoS: 2, PacketID: 7, Payload: []byte("new")})
	if a := pub.read().(*packet.Pubrec); a.ReasonCode != packet.Success {
		t.Fatalf("the new publish under 7: %s", a.ReasonCode)
	}
	if p := sub.readPublish(); string(p.Payload) != "new" {
		t.Fatalf("delivered %q, want new", p.Payload)
	}

	// 3.1.1: the dropped resend is answered, and the connection stays open.
	old, _ := f.connect("tcp", packet.V4, &packet.Connect{ClientID: "old", CleanStart: true}, false)
	old.send(&packet.Publish{Topic: "q", QoS: 2, PacketID: 9, Payload: []byte("old")})
	if _, ok := old.read().(*packet.Pubrec); !ok {
		t.Fatal("no PUBREC at 3.1.1")
	}
	if p := sub.readPublish(); string(p.Payload) != "old" {
		t.Fatalf("delivered %q", p.Payload)
	}
	testmod.Shared.Set(func(c *testmod.Control) { c.Trigger, c.InboundOutcome = "old", module.Outcome{Action: module.Drop} })
	old.send(&packet.Publish{Topic: "q", QoS: 2, PacketID: 9, Dup: true, Payload: []byte("old")})
	if _, ok := old.read().(*packet.Pubrec); !ok {
		t.Fatal("no PUBREC for the dropped resend at 3.1.1")
	}
	old.send(&packet.Pingreq{})
	if _, ok := old.read().(*packet.Pingresp); !ok {
		t.Fatal("the connection did not stay open")
	}
}

// TestHookTimeoutCannotRefuseAnOpenResend: under hooks.fallback disconnect,
// an interceptor that times out on a resend of an accepted publish does not
// close the connection, while one that times out on a new publish still does.
func TestHookTimeoutCannotRefuseAnOpenResend(t *testing.T) {
	f := withTestModule(t, func(d *config.Document) {
		d.Hooks.TimeoutMs = 100
		d.Hooks.Fallback = "disconnect"
	})
	pub, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	pub.send(&packet.Publish{Topic: "q", QoS: 2, PacketID: 7, Payload: []byte("once")})
	// With no subscriber the publish is accepted with No Matching Subscribers.
	if a := pub.read().(*packet.Pubrec); a.ReasonCode.IsError() {
		t.Fatalf("the publish: %s", a.ReasonCode)
	}
	testmod.Shared.Set(func(c *testmod.Control) { c.Sleep = 300 * time.Millisecond })
	t.Cleanup(func() { testmod.Shared.Set(func(c *testmod.Control) { c.Sleep = 0 }) })
	pub.send(&packet.Publish{Topic: "q", QoS: 2, PacketID: 7, Dup: true, Payload: []byte("once")})
	if a, ok := pub.read().(*packet.Pubrec); !ok || a.ReasonCode != packet.Success {
		t.Fatalf("the resend whose hook timed out: %#v, want PUBREC Success", a)
	}
	if got := f.series("hoardmq_inbound_refusals_ignored_total", map[string]string{"namespace": "default"}); got != 1 {
		t.Fatalf("ignored refusals %v, want 1", got)
	}
	pub.send(&packet.Publish{Topic: "q", QoS: 2, PacketID: 8, Payload: []byte("fresh")})
	got := pub.readEnd()
	if len(got) != 1 {
		t.Fatalf("a new publish whose hook timed out: read %v", got)
	}
	if d, ok := got[0].(*packet.Disconnect); !ok || d.ReasonCode != packet.ImplementationSpecificError {
		t.Fatalf("a new publish whose hook timed out: %#v, want DISCONNECT 0x83", got[0])
	}
}
