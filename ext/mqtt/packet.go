// Package mqtt holds the MQTT control packets as data, the reason codes and
// protocol levels, and the message envelope the broker routes, stores and
// replicates. Interceptors receive these packets by pointer and may change
// them in place; the broker's own codec reads and writes the same values.
package mqtt

import "fmt"

// Version is the protocol level from the CONNECT packet.
type Version byte

// Supported protocol levels.
const (
	V3 Version = 3 // MQTT V3.1, protocol name "MQIsdp"
	V4 Version = 4 // MQTT 3.1.1, protocol name "MQTT"
	V5 Version = 5 // MQTT 5.0, protocol name "MQTT"
)

func (v Version) String() string {
	switch v {
	case V3:
		return "MQTT 3.1"
	case V4:
		return "MQTT 3.1.1"
	case V5:
		return "MQTT 5.0"
	}
	return fmt.Sprintf("protocol level %d", byte(v))
}

// Type is the control packet type from the fixed header.
type Type byte

// Control packet types.
const (
	CONNECT     Type = 1
	CONNACK     Type = 2
	PUBLISH     Type = 3
	PUBACK      Type = 4
	PUBREC      Type = 5
	PUBREL      Type = 6
	PUBCOMP     Type = 7
	SUBSCRIBE   Type = 8
	SUBACK      Type = 9
	UNSUBSCRIBE Type = 10
	UNSUBACK    Type = 11
	PINGREQ     Type = 12
	PINGRESP    Type = 13
	DISCONNECT  Type = 14
	AUTH        Type = 15
)

var typeNames = [...]string{"", "CONNECT", "CONNACK", "PUBLISH", "PUBACK", "PUBREC", "PUBREL", "PUBCOMP", "SUBSCRIBE", "SUBACK", "UNSUBSCRIBE", "UNSUBACK", "PINGREQ", "PINGRESP", "DISCONNECT", "AUTH"}

func (t Type) String() string {
	if int(t) < len(typeNames) && t != 0 {
		return typeNames[t]
	}
	return fmt.Sprintf("type %d", byte(t))
}

// Packet is any control packet.
type Packet interface {
	Type() Type
}

// UserProperty is one name-value pair from the User Property property.
type UserProperty struct {
	Name  string
	Value string
}

// Properties holds every MQTT 5.0 property. Pointer and slice members are nil
// when the property is absent. Which members a packet may carry is enforced by
// the codec per packet type.
type Properties struct {
	PayloadFormatIndicator          *byte    // 0x01
	MessageExpiryInterval           *uint32  // 0x02
	ContentType                     *string  // 0x03
	ResponseTopic                   *string  // 0x08
	CorrelationData                 []byte   // 0x09
	SubscriptionIdentifiers         []uint32 // 0x0B; at most one except in PUBLISH
	SessionExpiryInterval           *uint32  // 0x11
	AssignedClientIdentifier        *string  // 0x12
	ServerKeepAlive                 *uint16  // 0x13
	AuthenticationMethod            *string  // 0x15
	AuthenticationData              []byte   // 0x16
	RequestProblemInformation       *bool    // 0x17
	WillDelayInterval               *uint32  // 0x18
	RequestResponseInformation      *bool    // 0x19
	ResponseInformation             *string  // 0x1A
	ServerReference                 *string  // 0x1C
	ReasonString                    *string  // 0x1F
	ReceiveMaximum                  *uint16  // 0x21
	TopicAliasMaximum               *uint16  // 0x22
	TopicAlias                      *uint16  // 0x23
	MaximumQoS                      *byte    // 0x24
	RetainAvailable                 *bool    // 0x25
	UserProperties                  []UserProperty
	MaximumPacketSize               *uint32 // 0x27
	WildcardSubscriptionAvailable   *bool   // 0x28
	SubscriptionIdentifierAvailable *bool   // 0x29
	SharedSubscriptionAvailable     *bool   // 0x2A
}

// Will is the CONNECT Will message.
type Will struct {
	Topic      string
	Payload    []byte
	QoS        byte
	Retain     bool
	Properties Properties // level 5 only
}

// Connect is the CONNECT packet.
type Connect struct {
	ProtocolName  string
	ProtocolLevel Version
	CleanStart    bool // Clean Session at levels 3 and 4
	KeepAlive     uint16
	ClientID      string
	Will          *Will
	HasUsername   bool
	Username      string
	HasPassword   bool
	Password      []byte
	Properties    Properties
}

// Connack is the CONNACK packet. ReasonCode is a 5.0 code at every level; the
// encoder maps it to a 3.x return code when the connection's level needs one.
type Connack struct {
	SessionPresent bool
	ReasonCode     ReasonCode
	Properties     Properties
}

// Publish is the PUBLISH packet.
type Publish struct {
	Dup        bool
	QoS        byte
	Retain     bool
	Topic      string
	PacketID   uint16 // QoS 1 and 2 only
	Properties Properties
	Payload    []byte
}

// Ack is the body shared by PUBACK, PUBREC, PUBREL and PUBCOMP.
type Ack struct {
	PacketID   uint16
	ReasonCode ReasonCode
	Properties Properties
}

// Puback, Pubrec, Pubrel and Pubcomp are the QoS 1 and 2 acknowledgements.
type (
	Puback  struct{ Ack }
	Pubrec  struct{ Ack }
	Pubrel  struct{ Ack }
	Pubcomp struct{ Ack }
)

// Subscription is one topic filter with its options in a SUBSCRIBE packet.
type Subscription struct {
	Filter            string
	QoS               byte
	NoLocal           bool
	RetainAsPublished bool
	RetainHandling    byte
}

// Subscribe is the SUBSCRIBE packet.
type Subscribe struct {
	PacketID      uint16
	Properties    Properties
	Subscriptions []Subscription
}

// Suback is the SUBACK packet. At levels 3 and 4 the codes are 0, 1, 2 or
// 0x80 (Failure).
type Suback struct {
	PacketID    uint16
	Properties  Properties
	ReasonCodes []ReasonCode
}

// Unsubscribe is the UNSUBSCRIBE packet.
type Unsubscribe struct {
	PacketID   uint16
	Properties Properties
	Filters    []string
}

// Unsuback is the UNSUBACK packet. ReasonCodes is empty at levels 3 and 4.
type Unsuback struct {
	PacketID    uint16
	Properties  Properties
	ReasonCodes []ReasonCode
}

// Pingreq and Pingresp are the keep-alive packets.
type (
	Pingreq  struct{}
	Pingresp struct{}
)

// Disconnect is the DISCONNECT packet. At levels 3 and 4 only the type is
// carried; ReasonCode is Success and Properties are ignored.
type Disconnect struct {
	ReasonCode ReasonCode
	Properties Properties
}

// Auth is the AUTH packet, level 5 only.
type Auth struct {
	ReasonCode ReasonCode
	Properties Properties
}

func (*Connect) Type() Type     { return CONNECT }
func (*Connack) Type() Type     { return CONNACK }
func (*Publish) Type() Type     { return PUBLISH }
func (*Puback) Type() Type      { return PUBACK }
func (*Pubrec) Type() Type      { return PUBREC }
func (*Pubrel) Type() Type      { return PUBREL }
func (*Pubcomp) Type() Type     { return PUBCOMP }
func (*Subscribe) Type() Type   { return SUBSCRIBE }
func (*Suback) Type() Type      { return SUBACK }
func (*Unsubscribe) Type() Type { return UNSUBSCRIBE }
func (*Unsuback) Type() Type    { return UNSUBACK }
func (*Pingreq) Type() Type     { return PINGREQ }
func (*Pingresp) Type() Type    { return PINGRESP }
func (*Disconnect) Type() Type  { return DISCONNECT }
func (*Auth) Type() Type        { return AUTH }
