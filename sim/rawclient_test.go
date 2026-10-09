package sim

import (
	"net"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
)

// rawClient is a hand-driven MQTT 5 connection for scenarios that need to see
// the packets themselves rather than what sim.Client makes of them.
//
// It exists so that a scenario reads reason codes by default. A scenario that
// asserts only the *type* of a reply passes whenever a reply arrives, which is
// how an exactly-once check stood for months while ignoring the error code
// that said the exchange had not been confirmed (WI 1486), and it is the same
// mistake sim.Client made about SUBACK until WI 1500.
type rawClient struct {
	t    *testing.T
	conn net.Conn
	enc  *packet.Encoder
	dec  *packet.Decoder
	name string
	id   uint16
	seq  uint64
}

// dialRaw connects, sends a CONNECT and requires a successful CONNACK.
func dialRaw(t *testing.T, addr, clientID string, deadline time.Duration) *rawClient {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.SetDeadline(time.Now().Add(deadline)); err != nil {
		t.Fatal(err)
	}
	c := &rawClient{t: t, conn: conn, name: clientID, enc: &packet.Encoder{Version: packet.V5}, dec: packet.NewDecoder(conn)}
	c.dec.Version = packet.V5
	c.send(&packet.Connect{ProtocolLevel: packet.V5, ClientID: clientID, CleanStart: true})
	if a, ok := c.read().(*packet.Connack); !ok || a.ReasonCode != packet.Success {
		t.Fatalf("connack %+v", a)
	}
	return c
}

func (c *rawClient) send(p packet.Packet) {
	c.t.Helper()
	if err := c.enc.Encode(c.conn, p); err != nil {
		c.t.Fatal(err)
	}
}

func (c *rawClient) read() packet.Packet {
	c.t.Helper()
	p, err := c.dec.Read()
	if err != nil {
		c.t.Fatal(err)
	}
	return p
}

// confirmed publishes until the broker answers with something other than
// 0x83, and returns that reason code along with every sequence number the
// broker confirmed.
//
// An acknowledgement of 0x83 means the broker could not confirm delivery in
// time, which says nothing about the reason code a scenario is asking about;
// under the whole-module gate's contention it happens, so such a publish is
// retried as a new message within a time budget (WI 1487). `onExhausted`
// supplies whatever diagnosis the scenario can give when the budget runs out,
// because "it was refused for twenty seconds" is not a result and must not be
// reported as one.
func (c *rawClient) confirmed(qos byte, topic string, budget time.Time, onExhausted func() string) (packet.ReasonCode, uint64) {
	c.t.Helper()
	for {
		c.id++
		c.seq++
		c.send(&packet.Publish{Topic: topic, QoS: qos, PacketID: c.id, Payload: payload(c.name, c.seq)})
		var code packet.ReasonCode
		switch a := c.read().(type) {
		case *packet.Puback:
			code = a.ReasonCode
		case *packet.Pubrec:
			code = a.ReasonCode
			if !code.IsError() {
				c.send(&packet.Pubrel{Ack: packet.Ack{PacketID: c.id}})
				if p, ok := c.read().(*packet.Pubcomp); !ok || p.ReasonCode.IsError() {
					c.t.Fatalf("PUBCOMP %+v", p)
				}
			}
		default:
			c.t.Fatalf("QoS %d publish answered with %T", qos, a)
		}
		if code != packet.ImplementationSpecificError {
			return code, c.seq
		}
		if time.Now().After(budget) {
			extra := ""
			if onExhausted != nil {
				extra = "; " + onExhausted()
			}
			c.t.Fatalf("the barrier failed on every attempt until the budget ran out, so the run says nothing about the reason code%s", extra)
		}
		c.t.Logf("QoS %d publish seq %d answered 0x83; retrying as a new message", qos, c.seq)
	}
}
