package conformance

import (
	"fmt"
	"testing"

	"github.com/happydave/hoardmq/internal/packet"
)

// A Will Topic is a Topic Name: no wildcard, at least one character, no
// U+0000. At 5.0 a wildcard or an empty topic is refused with CONNACK 0x90;
// at 3.1.1, which has no return code for it, the connection is closed. A
// U+0000 is a malformed string at either level and closes the connection.
func TestRawWillTopic(t *testing.T) {
	node := StartNode(t, nil)
	str := func(s string) []byte { return append([]byte{byte(len(s) >> 8), byte(len(s))}, s...) }
	// connect is a CONNECT with Clean Start and a QoS 0 Will on topic.
	connect := func(v packet.Version, id, topic string) []byte {
		var body []byte
		switch v {
		case packet.V5:
			body = append(str("MQTT"), 5, 0x06, 0, 10, 0x00)
			body = append(body, str(id)...)
			body = append(body, 0x00)
		default:
			body = append(str("MQTT"), 4, 0x06, 0, 10)
			body = append(body, str(id)...)
		}
		body = append(body, str(topic)...)
		body = append(body, str("gone")...)
		return append([]byte{0x10, byte(len(body))}, body...)
	}
	for i, c := range []struct {
		name  string
		ids   []string
		v     packet.Version
		topic string
		ack   bool              // a CONNACK is expected
		want  packet.ReasonCode // its reason code
		close bool
	}{
		{"5.0 valid", []string{"MQTT-3.1.3-11"}, packet.V5, "will/t", true, packet.Success, false},
		{"5.0 single-level wildcard", []string{"MQTT-4.7.0-1"}, packet.V5, "will/+", true, packet.TopicNameInvalid, true},
		{"5.0 multi-level wildcard", []string{"MQTT-4.7.0-1"}, packet.V5, "will/#", true, packet.TopicNameInvalid, true},
		{"5.0 empty", []string{"MQTT-4.7.3-1"}, packet.V5, "", true, packet.TopicNameInvalid, true},
		{"5.0 U+0000", []string{"MQTT-1.5.4-2", "MQTT-4.7.3-2"}, packet.V5, "will\x00t", false, 0, true},
		{"3.1.1 valid", []string{"MQTT-3.1.3-10"}, packet.V4, "will/t", true, packet.Success, false},
		{"3.1.1 single-level wildcard", []string{"MQTT-4.7.1-1"}, packet.V4, "will/+", false, 0, true},
		{"3.1.1 multi-level wildcard", []string{"MQTT-4.7.1-1"}, packet.V4, "#", false, 0, true},
		{"3.1.1 empty", []string{"MQTT-4.7.3-1"}, packet.V4, "", false, 0, true},
		{"3.1.1 U+0000", []string{"MQTT-4.7.3-2"}, packet.V4, "will\x00t", false, 0, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			Statement(t, SpecOf(c.v), c.ids...)
			n := node.For(t)
			r := n.DialRaw(c.v)
			r.SendBytes(connect(c.v, fmt.Sprintf("will-%d", i), c.topic))
			if c.ack {
				ack, ok := r.Read().(*packet.Connack)
				if !ok || ack.ReasonCode != c.want {
					t.Fatalf("connack %+v, want %s", ack, c.want)
				}
			}
			if c.close {
				r.ExpectClosed()
			}
		})
	}
}
