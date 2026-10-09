// Package packet is the MQTT wire codec for protocol levels 3 (MQTT V3.1,
// "MQIsdp"), 4 (MQTT 3.1.1) and 5 (MQTT 5.0). One decoder and one encoder
// cover every level; the level is fixed per connection from the CONNECT
// packet and selects the grammar for everything after it. The packets it
// reads and writes are the extension API's (ext/mqtt), named here too.
package packet

import (
	"fmt"

	"github.com/happydave/hoardmq/ext/mqtt"
)

// The packet types, protocol levels and properties are the extension API's.
type (
	Version      = mqtt.Version
	Type         = mqtt.Type
	Packet       = mqtt.Packet
	UserProperty = mqtt.UserProperty
	Properties   = mqtt.Properties
	Will         = mqtt.Will
	Connect      = mqtt.Connect
	Connack      = mqtt.Connack
	Publish      = mqtt.Publish
	Ack          = mqtt.Ack
	Subscription = mqtt.Subscription
	Subscribe    = mqtt.Subscribe
	Suback       = mqtt.Suback
	Unsubscribe  = mqtt.Unsubscribe
	Unsuback     = mqtt.Unsuback
	Disconnect   = mqtt.Disconnect
	Auth         = mqtt.Auth
	Puback       = mqtt.Puback
	Pubrec       = mqtt.Pubrec
	Pubrel       = mqtt.Pubrel
	Pubcomp      = mqtt.Pubcomp
	Pingreq      = mqtt.Pingreq
	Pingresp     = mqtt.Pingresp
)

// Protocol levels and control packet types.
const (
	V3          = mqtt.V3
	V4          = mqtt.V4
	V5          = mqtt.V5
	CONNECT     = mqtt.CONNECT
	CONNACK     = mqtt.CONNACK
	PUBLISH     = mqtt.PUBLISH
	PUBACK      = mqtt.PUBACK
	PUBREC      = mqtt.PUBREC
	PUBREL      = mqtt.PUBREL
	PUBCOMP     = mqtt.PUBCOMP
	SUBSCRIBE   = mqtt.SUBSCRIBE
	SUBACK      = mqtt.SUBACK
	UNSUBSCRIBE = mqtt.UNSUBSCRIBE
	UNSUBACK    = mqtt.UNSUBACK
	PINGREQ     = mqtt.PINGREQ
	PINGRESP    = mqtt.PINGRESP
	DISCONNECT  = mqtt.DISCONNECT
	AUTH        = mqtt.AUTH
)

// Error is a decode or encode failure carrying the reason code the connection
// should answer with. Every Error closes the connection; the caller chooses
// between a DISCONNECT and a CONNACK depending on where in the handshake it is.
type Error struct {
	Reason  ReasonCode
	Message string
	// Level is the protocol level the peer sent when Reason is
	// UnsupportedProtocolVersion, so the answer can use the peer's framing.
	Level byte
}

func (e *Error) Error() string {
	if e.Message == "" {
		return e.Reason.String()
	}
	return e.Reason.String() + ": " + e.Message
}

func malformed(format string, a ...any) *Error {
	return &Error{Reason: MalformedPacket, Message: fmt.Sprintf(format, a...)}
}

func protocolError(format string, a ...any) *Error {
	return &Error{Reason: ProtocolError, Message: fmt.Sprintf(format, a...)}
}
