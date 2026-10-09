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

func withTestModule(t *testing.T, mutate func(*config.Document)) *fixture {
	t.Helper()
	testmod.Register()
	testmod.Shared.Reset()
	testmod.Shared.Set(func(c *testmod.Control) {
		c.Trigger, c.Rewrite, c.OutboundDrop, c.RefuseFilter = "", "", "", ""
		c.LowerQoS, c.Panic, c.Sleep = false, false, 0
		c.InboundOutcome = module.OK
		c.AdmitTrigger, c.AdmitOutcome = "", module.OK
	})
	return start(t, func(d *config.Document) {
		d.Modules[string(testmod.ID)] = json.RawMessage(`{}`)
		if mutate != nil {
			mutate(d)
		}
	})
}

func has(calls []string, want string) bool {
	for _, c := range calls {
		if strings.Contains(c, want) {
			return true
		}
	}
	return false
}

func waitFor(t *testing.T, want string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if has(testmod.Shared.Recorded(), want) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("hook call %q not recorded; got:\n%s", want, strings.Join(testmod.Shared.Recorded(), "\n"))
}

func TestEveryHookFamilyEndToEnd(t *testing.T) {
	f := withTestModule(t, nil)
	if got := f.srv.Registry().Modules(); len(got) != 4 {
		t.Fatalf("modules %v", got)
	}
	sub, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "sub", CleanStart: true}, false)
	pub, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true, Will: &packet.Will{Topic: "w", Payload: []byte("bye")}}, false)
	waitFor(t, "connected pub")
	waitFor(t, "authenticate")

	// Subscribe interceptor: refuse one filter, lower another's QoS.
	testmod.Shared.Set(func(c *testmod.Control) { c.RefuseFilter = "secret/#"; c.LowerQoS = true })
	sub.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "secret/#", QoS: 1}, {Filter: "t", QoS: 2}, {Filter: "w", QoS: 1}}})
	ack := sub.read().(*packet.Suback)
	if ack.ReasonCodes[0] != packet.TopicFilterInvalid || ack.ReasonCodes[1] != packet.GrantedQoS0 || ack.ReasonCodes[2] != packet.GrantedQoS0 {
		t.Fatalf("suback %v", ack.ReasonCodes)
	}
	testmod.Shared.Set(func(c *testmod.Control) { c.LowerQoS = false })

	// Inbound modify: the subscriber sees the rewritten payload.
	testmod.Shared.Set(func(c *testmod.Control) {
		c.Trigger, c.InboundOutcome, c.Rewrite = "orig", module.Outcome{Action: module.Modify}, "rewritten"
	})
	pub.send(&packet.Publish{Topic: "t", Payload: []byte("orig")})
	if p := sub.readPublish(); string(p.Payload) != "rewritten" {
		t.Fatalf("modify: %q", p.Payload)
	}
	waitFor(t, "delivery delivered sub rewritten qos=0")

	// Inbound drop of a QoS 1 publish: PUBACK 0x80, nothing delivered.
	testmod.Shared.Set(func(c *testmod.Control) { c.Trigger, c.InboundOutcome = "dropme", module.Outcome{Action: module.Drop} })
	pub.send(&packet.Publish{Topic: "t", QoS: 1, PacketID: 5, Payload: []byte("dropme")})
	if a := pub.read().(*packet.Puback); a.PacketID != 5 || a.ReasonCode != packet.UnspecifiedError {
		t.Fatalf("drop ack %+v", a)
	}
	sub.expectNoPacket()

	// Outbound drop: the publish never reaches the subscriber.
	testmod.Shared.Set(func(c *testmod.Control) { c.Trigger = ""; c.OutboundDrop = "hidden" })
	pub.send(&packet.Publish{Topic: "t", Payload: []byte("hidden")})
	sub.expectNoPacket()
	testmod.Shared.Set(func(c *testmod.Control) { c.OutboundDrop = "" })

	// Retained event.
	pub.send(&packet.Publish{Topic: "r", Retain: true, Payload: []byte("keep")})
	waitFor(t, "retained r 4")

	// Inbound disconnect with a reason.
	testmod.Shared.Set(func(c *testmod.Control) {
		c.Trigger, c.InboundOutcome = "kill", module.Outcome{Action: module.Disconnect, Reason: packet.AdministrativeAction}
	})
	pub.send(&packet.Publish{Topic: "t", Payload: []byte("kill")})
	if d, ok := pub.read().(*packet.Disconnect); !ok || d.ReasonCode != packet.AdministrativeAction {
		t.Fatalf("disconnect %+v", d)
	}
	// The publisher's end is abnormal: Will event, delivery to the watcher, disconnected event.
	waitFor(t, "will pub w")
	if p := sub.readPublish(); p.Topic != "w" || string(p.Payload) != "bye" {
		t.Fatalf("will delivery %+v", p)
	}
	waitFor(t, "disconnected pub Administrative action (0x98) graceful=false")

	// Metrics counted through the metrics module.
	var publishes float64
	for _, m := range f.srv.Registry().Metrics().Snapshot() {
		if m.Name == "hoardmq_publishes_total" {
			publishes += m.Value
		}
	}
	if publishes < 3 {
		t.Fatalf("publishes counted %v", publishes)
	}
}

func TestHookTimeoutEndToEnd(t *testing.T) {
	f := withTestModule(t, func(d *config.Document) { d.Hooks.TimeoutMs = 20; d.Hooks.Fallback = "disconnect" })
	sub, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "sub", CleanStart: true}, false)
	sub.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "t", QoS: 0}}})
	sub.read()
	pub, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	testmod.Shared.Set(func(c *testmod.Control) { c.Sleep = 200 * time.Millisecond })
	pub.send(&packet.Publish{Topic: "t", Payload: []byte("slow")})
	if d, ok := pub.read().(*packet.Disconnect); !ok || d.ReasonCode != packet.ImplementationSpecificError {
		t.Fatalf("timeout fallback %+v", d)
	}
	testmod.Shared.Set(func(c *testmod.Control) { c.Sleep = 0 })
	if n := f.srv.Registry().Hooks().Failures()[testmod.ID]; n == 0 {
		t.Fatal("timeout not counted")
	}
	// Let the closed publisher's disconnected event land before disabling.
	waitFor(t, "disconnected pub")
	// Disabling the module removes it from every family at once.
	if !f.srv.Registry().SetEnabled(testmod.ID, false) {
		t.Fatal("SetEnabled")
	}
	testmod.Shared.Reset()
	pub2, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "pub2", CleanStart: true}, false)
	pub2.send(&packet.Publish{Topic: "t", Payload: []byte("plain")})
	if p := sub.readPublish(); string(p.Payload) != "plain" {
		t.Fatalf("%q", p.Payload)
	}
	time.Sleep(50 * time.Millisecond)
	if calls := testmod.Shared.Recorded(); len(calls) != 0 {
		t.Fatalf("disabled module still called: %v", calls)
	}
}

func TestBuiltinsReachedOnlyThroughRegistry(t *testing.T) {
	f := start(t, nil)
	if got := strings.Join(idStrings(f.srv.Registry().Modules()), ","); got != "authz.file,metrics.internal,store.wal" {
		t.Fatalf("modules %s", got)
	}
	if f.srv.Registry().Store() == nil || f.srv.Registry().Metrics() == nil {
		t.Fatal("providers missing")
	}
}

func idStrings(ids []module.ID) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = string(id)
	}
	return out
}

func TestValidateModulesReportsTogether(t *testing.T) {
	doc := config.Default()
	doc.Node.DataDir = t.TempDir()
	doc.Modules = map[string]json.RawMessage{
		"auth.file":   json.RawMessage(`{"path":"/nonexistent/users.json"}`),
		"authz.file":  json.RawMessage(`{"path":"x","extra":1}`),
		"nope.module": json.RawMessage(`{}`),
	}
	problems := ValidateModules(doc, nil)
	var got []string
	for _, p := range problems {
		got = append(got, p.String())
	}
	joined := strings.Join(got, "\n")
	for _, want := range []string{"/modules/auth.file:", "/modules/authz.file:", "/modules/nope.module: unknown module"} {
		if !strings.Contains(joined, want) {
			t.Errorf("missing %q in\n%s", want, joined)
		}
	}
	// Read-only: nothing under the data directory.
	entries, _ := os.ReadDir(doc.Node.DataDir)
	if len(entries) != 0 {
		t.Errorf("ValidateModules created %v", entries)
	}
	_ = filepath.Join
}

// readEnd reads until the connection ends and returns the packets read
// before it, so a test can tell a close from an acknowledgement.
func (c *client) readEnd() []packet.Packet {
	c.t.Helper()
	var got []packet.Packet
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_ = c.c.SetReadDeadline(deadline)
		p, err := c.dec.Read()
		if err != nil {
			return got
		}
		got = append(got, p)
	}
	c.t.Fatalf("the connection stayed open; read %v", got)
	return nil
}

func TestPublishAdmissionAndAcceptanceEndToEnd(t *testing.T) {
	f := withTestModule(t, nil)
	sub, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "sub", CleanStart: true}, false)
	sub.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "t", QoS: 1}}})
	sub.read()
	pub, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)

	pub.send(&packet.Publish{Topic: "t", QoS: 1, PacketID: 1, Payload: []byte("ok")})
	if a := pub.read().(*packet.Puback); a.ReasonCode != packet.Success {
		t.Fatalf("puback %+v", a)
	}
	sub.readPublish()
	calls := testmod.Shared.Recorded()
	admit, accepted := -1, -1
	for i, c := range calls {
		if c == "admit t" && admit < 0 {
			admit = i
		}
		if c == "accepted client t 1" {
			accepted = i
		}
	}
	if admit < 0 || accepted < admit {
		t.Fatalf("want admit then accepted; got:\n%s", strings.Join(calls, "\n"))
	}

	// A refused admission: PUBACK with the module's reason, nothing delivered,
	// nothing reported accepted.
	testmod.Shared.Reset()
	testmod.Shared.Set(func(c *testmod.Control) {
		c.AdmitTrigger, c.AdmitOutcome = "refuse", module.Outcome{Action: module.Drop, Reason: packet.QuotaExceeded}
	})
	pub.send(&packet.Publish{Topic: "t", QoS: 1, PacketID: 2, Payload: []byte("refuse")})
	if a := pub.read().(*packet.Puback); a.PacketID != 2 || a.ReasonCode != packet.QuotaExceeded {
		t.Fatalf("refused puback %+v", a)
	}
	sub.expectNoPacket()
	if has(testmod.Shared.Recorded(), "accepted") {
		t.Fatalf("a refused publish was reported accepted:\n%s", strings.Join(testmod.Shared.Recorded(), "\n"))
	}
}

func TestPublishAdmissionRefusalClosesWhereNoAckCanSayNo(t *testing.T) {
	f := withTestModule(t, nil)
	testmod.Shared.Set(func(c *testmod.Control) {
		c.AdmitTrigger, c.AdmitOutcome = "refuse", module.Outcome{Action: module.Drop, Reason: packet.QuotaExceeded}
	})
	// MQTT 3.1.1 at QoS 1: no reason code on a PUBACK, so the connection is
	// closed and no PUBACK is written.
	v4, _ := f.connect("tcp", packet.V4, &packet.Connect{ClientID: "v4", CleanStart: true}, false)
	v4.send(&packet.Publish{Topic: "t", QoS: 1, PacketID: 1, Payload: []byte("refuse")})
	for _, p := range v4.readEnd() {
		if _, ok := p.(*packet.Puback); ok {
			t.Fatal("a refused level-4 publish was acknowledged")
		}
	}
	// QoS 0 at MQTT 5: nothing to acknowledge, so the connection is closed.
	v5, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "v5", CleanStart: true}, false)
	v5.send(&packet.Publish{Topic: "t", Payload: []byte("refuse")})
	got := v5.readEnd()
	if len(got) != 1 {
		t.Fatalf("want one DISCONNECT before the close, read %v", got)
	}
	if d, ok := got[0].(*packet.Disconnect); !ok || d.ReasonCode != packet.QuotaExceeded {
		t.Fatalf("want DISCONNECT 0x97, read %+v", got[0])
	}
}
