package mqtt

import "fmt"

// ReasonCode is an MQTT 5.0 reason code. The broker uses the 5.0 values at
// every protocol level and maps them to 3.x return codes when it encodes a
// packet for a 3.x client.
type ReasonCode byte

// Reason codes, MQTT 5.0 section 2.4.
const (
	Success                             ReasonCode = 0x00
	NormalDisconnection                 ReasonCode = 0x00
	GrantedQoS0                         ReasonCode = 0x00
	GrantedQoS1                         ReasonCode = 0x01
	GrantedQoS2                         ReasonCode = 0x02
	DisconnectWithWillMessage           ReasonCode = 0x04
	NoMatchingSubscribers               ReasonCode = 0x10
	NoSubscriptionExisted               ReasonCode = 0x11
	ContinueAuthentication              ReasonCode = 0x18
	ReAuthenticate                      ReasonCode = 0x19
	UnspecifiedError                    ReasonCode = 0x80
	MalformedPacket                     ReasonCode = 0x81
	ProtocolError                       ReasonCode = 0x82
	ImplementationSpecificError         ReasonCode = 0x83
	UnsupportedProtocolVersion          ReasonCode = 0x84
	ClientIdentifierNotValid            ReasonCode = 0x85
	BadUserNameOrPassword               ReasonCode = 0x86
	NotAuthorized                       ReasonCode = 0x87
	ServerUnavailable                   ReasonCode = 0x88
	ServerBusy                          ReasonCode = 0x89
	Banned                              ReasonCode = 0x8A
	ServerShuttingDown                  ReasonCode = 0x8B
	BadAuthenticationMethod             ReasonCode = 0x8C
	KeepAliveTimeout                    ReasonCode = 0x8D
	SessionTakenOver                    ReasonCode = 0x8E
	TopicFilterInvalid                  ReasonCode = 0x8F
	TopicNameInvalid                    ReasonCode = 0x90
	PacketIdentifierInUse               ReasonCode = 0x91
	PacketIdentifierNotFound            ReasonCode = 0x92
	ReceiveMaximumExceeded              ReasonCode = 0x93
	TopicAliasInvalid                   ReasonCode = 0x94
	PacketTooLarge                      ReasonCode = 0x95
	MessageRateTooHigh                  ReasonCode = 0x96
	QuotaExceeded                       ReasonCode = 0x97
	AdministrativeAction                ReasonCode = 0x98
	PayloadFormatInvalid                ReasonCode = 0x99
	RetainNotSupported                  ReasonCode = 0x9A
	QoSNotSupported                     ReasonCode = 0x9B
	UseAnotherServer                    ReasonCode = 0x9C
	ServerMoved                         ReasonCode = 0x9D
	SharedSubscriptionsNotSupported     ReasonCode = 0x9E
	ConnectionRateExceeded              ReasonCode = 0x9F
	MaximumConnectTime                  ReasonCode = 0xA0
	SubscriptionIdentifiersNotSupported ReasonCode = 0xA1
	WildcardSubscriptionsNotSupported   ReasonCode = 0xA2
)

var reasonNames = map[ReasonCode]string{
	0x00: "Success",
	0x01: "Granted QoS 1",
	0x02: "Granted QoS 2",
	0x04: "Disconnect with Will Message",
	0x10: "No matching subscribers",
	0x11: "No subscription existed",
	0x18: "Continue authentication",
	0x19: "Re-authenticate",
	0x80: "Unspecified error",
	0x81: "Malformed Packet",
	0x82: "Protocol Error",
	0x83: "Implementation specific error",
	0x84: "Unsupported Protocol Version",
	0x85: "Client Identifier not valid",
	0x86: "Bad User Name or Password",
	0x87: "Not authorized",
	0x88: "Server unavailable",
	0x89: "Server busy",
	0x8A: "Banned",
	0x8B: "Server shutting down",
	0x8C: "Bad authentication method",
	0x8D: "Keep Alive timeout",
	0x8E: "Session taken over",
	0x8F: "Topic Filter invalid",
	0x90: "Topic Name invalid",
	0x91: "Packet Identifier in use",
	0x92: "Packet Identifier not found",
	0x93: "Receive Maximum exceeded",
	0x94: "Topic Alias invalid",
	0x95: "Packet too large",
	0x96: "Message rate too high",
	0x97: "Quota exceeded",
	0x98: "Administrative action",
	0x99: "Payload format invalid",
	0x9A: "Retain not supported",
	0x9B: "QoS not supported",
	0x9C: "Use another server",
	0x9D: "Server moved",
	0x9E: "Shared Subscriptions not supported",
	0x9F: "Connection rate exceeded",
	0xA0: "Maximum connect time",
	0xA1: "Subscription Identifiers not supported",
	0xA2: "Wildcard Subscriptions not supported",
}

func (r ReasonCode) String() string {
	if n, ok := reasonNames[r]; ok {
		return fmt.Sprintf("%s (0x%02X)", n, byte(r))
	}
	return fmt.Sprintf("reason 0x%02X", byte(r))
}

// IsError reports whether the code signals failure (0x80 and above).
func (r ReasonCode) IsError() bool { return r >= 0x80 }
