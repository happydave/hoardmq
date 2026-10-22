package broker

import (
	"strings"
	"testing"

	"github.com/happydave/hoardmq/internal/packet"
)

// At the default configuration a connection's inbound topic aliases may hold
// 1 MiB of topics: sixteen 64,000-byte topics fit, the seventeenth closes the
// connection with Quota exceeded.
func TestTopicAliasTableBoundedByDefault(t *testing.T) {
	f := start(t, nil)
	cl, ack := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "aliases", CleanStart: true, KeepAlive: 30}, false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("connack %s", ack.ReasonCode)
	}
	if ack.Properties.TopicAliasMaximum == nil || *ack.Properties.TopicAliasMaximum < 17 {
		t.Fatalf("topic alias maximum %v", ack.Properties.TopicAliasMaximum)
	}
	topic := func(i int) string { return "a/" + string(rune('a'+i)) + strings.Repeat("x", 64000-3) }
	for i := 0; i < 16; i++ {
		alias := uint16(i + 1)
		cl.send(&packet.Publish{Topic: topic(i), Properties: packet.Properties{TopicAlias: &alias}})
	}
	cl.send(&packet.Pingreq{})
	if p := cl.read(); p.Type() != packet.PINGRESP {
		t.Fatalf("after 1,024,000 bytes of aliases: %T %+v", p, p)
	}
	alias := uint16(17)
	cl.send(&packet.Publish{Topic: topic(16), Properties: packet.Properties{TopicAlias: &alias}})
	cl.send(&packet.Pingreq{})
	p := cl.read()
	if d, ok := p.(*packet.Disconnect); !ok || d.ReasonCode != packet.QuotaExceeded {
		t.Fatalf("past 1 MiB of aliases: %T %+v, want DISCONNECT 0x97", p, p)
	}
}
