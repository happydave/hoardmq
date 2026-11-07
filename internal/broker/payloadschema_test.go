package broker

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
	_ "github.com/happydave/hoardmq/modules/payloadschema" // schema.json
)

// TestPayloadSchema runs schema.json on a node: a valid payload is
// delivered, an invalid one takes the refused-publish path by level and QoS
// with the rule's reason, a continue rule delivers and counts, an alias-only
// publish is checked on its resolved topic, and each refusal is in the audit
// log without its payload.
func TestPayloadSchema(t *testing.T) {
	f := start(t, func(d *config.Document) {
		d.Modules["schema.json"] = json.RawMessage(`{"audit":true,"rules":[
			{"name":"readings","filter":"r/#","schema":{"type":"object","required":["temp"],"properties":{"temp":{"type":"number","maximum":100}}}},
			{"name":"commands","filter":"cmd/#","schema":{"type":"object","required":["op"]},"onInvalid":"disconnect","reason":135},
			{"name":"notes","filter":"n/#","schema":{"type":"string"},"onInvalid":"continue"}]}`)
	})
	sub, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "sub", CleanStart: true}, false)
	sub.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "#", QoS: 0}}})
	sub.read()
	pub, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)

	pub.send(&packet.Publish{Topic: "r/1", QoS: 1, PacketID: 1, Payload: []byte(`{"temp":20}`)})
	if a := pub.read().(*packet.Puback); a.ReasonCode.IsError() {
		t.Fatalf("a valid reading: %s", a.ReasonCode)
	}
	if p := sub.readPublish(); string(p.Payload) != `{"temp":20}` {
		t.Fatalf("delivered %q", p.Payload)
	}
	pub.send(&packet.Publish{Topic: "r/1", QoS: 1, PacketID: 2, Payload: []byte(`{"temp":"hot-secret"}`)})
	if a := pub.read().(*packet.Puback); a.ReasonCode != packet.PayloadFormatInvalid {
		t.Fatalf("an invalid reading at QoS 1: %s, want 0x99", a.ReasonCode)
	}
	// QoS 2: the error PUBREC, and the identifier free at once for a valid one.
	pub.send(&packet.Publish{Topic: "r/2", QoS: 2, PacketID: 3, Payload: []byte(`{}`)})
	if a := pub.read().(*packet.Pubrec); a.ReasonCode != packet.PayloadFormatInvalid {
		t.Fatalf("an invalid reading at QoS 2: %s, want 0x99", a.ReasonCode)
	}
	pub.send(&packet.Publish{Topic: "r/2", QoS: 2, PacketID: 3, Payload: []byte(`{"temp":1}`)})
	if a := pub.read().(*packet.Pubrec); a.ReasonCode.IsError() {
		t.Fatalf("a valid reading under the refused identifier: %s", a.ReasonCode)
	}
	if p := sub.readPublish(); string(p.Payload) != `{"temp":1}` {
		t.Fatalf("delivered %q, want the valid reading only", p.Payload)
	}
	pub.send(&packet.Pubrel{Ack: packet.Ack{PacketID: 3}})
	pub.read()
	// An alias-only publish is checked on the topic its alias names.
	alias := uint16(1)
	pub.send(&packet.Publish{Topic: "r/3", QoS: 1, PacketID: 4, Payload: []byte(`{"temp":5}`), Properties: packet.Properties{TopicAlias: &alias}})
	pub.read()
	sub.readPublish()
	pub.send(&packet.Publish{QoS: 1, PacketID: 5, Payload: []byte(`{"temp":500}`), Properties: packet.Properties{TopicAlias: &alias}})
	if a := pub.read().(*packet.Puback); a.ReasonCode != packet.PayloadFormatInvalid {
		t.Fatalf("an invalid alias-only reading: %s, want 0x99", a.ReasonCode)
	}
	// continue: delivered, counted invalid.
	pub.send(&packet.Publish{Topic: "n/1", QoS: 1, PacketID: 6, Payload: []byte(`{"not":"a string"}`)})
	if a := pub.read().(*packet.Puback); a.ReasonCode.IsError() {
		t.Fatalf("a continue rule: %s", a.ReasonCode)
	}
	if p := sub.readPublish(); p.Topic != "n/1" {
		t.Fatalf("delivered %q, want n/1", p.Topic)
	}
	sub.expectNoPacket()
	if got := f.series("hoardmq_payload_schema_checks_total", map[string]string{"rule": "notes", "outcome": "invalid"}); got != 1 {
		t.Fatalf("notes invalid %v", got)
	}
	if got := f.series("hoardmq_payload_schema_refusals_total", map[string]string{"rule": "readings", "action": "drop"}); got != 3 {
		t.Fatalf("readings refused %v, want 3", got)
	}
	if got := f.series("hoardmq_drops_total", map[string]string{"namespace": "default", "reason": "admission"}); got != 3 {
		t.Fatalf("admission drops %v, want 3", got)
	}
	// QoS 0 at 5.0 refused: the connection closed.
	pub.send(&packet.Publish{Topic: "r/9", Payload: []byte(`[]`)})
	if got := pub.readEnd(); len(got) != 1 || got[0].(*packet.Disconnect).ReasonCode != packet.PayloadFormatInvalid {
		t.Fatalf("an invalid QoS 0 reading: %v", got)
	}
	// disconnect: the rule's reason on a DISCONNECT.
	cmd, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "cmd", CleanStart: true}, false)
	cmd.send(&packet.Publish{Topic: "cmd/1", QoS: 1, PacketID: 1, Payload: []byte(`{}`)})
	if got := cmd.readEnd(); len(got) == 0 || got[len(got)-1].(*packet.Disconnect).ReasonCode != packet.NotAuthorized {
		t.Fatalf("a disconnect rule: %v, want DISCONNECT 0x87", got)
	}
	// 3.1.1 QoS 1: closed, no PUBACK.
	old, _ := f.connect("tcp", packet.V4, &packet.Connect{ClientID: "old", CleanStart: true}, false)
	old.send(&packet.Publish{Topic: "r/1", QoS: 1, PacketID: 1, Payload: []byte(`{}`)})
	if got := old.readEnd(); len(got) != 0 {
		t.Fatalf("an invalid reading at 3.1.1: answered %v, want the connection closed", got)
	}
	sub.expectNoPacket()

	f.stop()
	files, _ := filepath.Glob(filepath.Join(f.dir, "data", "audit", "*"))
	var text strings.Builder
	for _, p := range files {
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		text.Write(b)
	}
	audit := text.String()
	if n := strings.Count(audit, `"action":"payload.invalid"`); n != 6 {
		t.Fatalf("%d payload.invalid records, want 6 (five readings, one command):\n%s", n, audit)
	}
	if !strings.Contains(audit, `"target":"readings"`) || !strings.Contains(audit, `"topic":"r/1"`) {
		t.Fatalf("the records lack their rule or topic:\n%s", audit)
	}
	if strings.Contains(audit, "hot-secret") {
		t.Fatal("a payload reached the audit log")
	}
}
