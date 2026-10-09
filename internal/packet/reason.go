package packet

import "github.com/happydave/hoardmq/ext/mqtt"

// ReasonCode is the extension API's MQTT 5.0 reason code.
type ReasonCode = mqtt.ReasonCode

// Reason codes, MQTT 5.0 section 2.4.
const (
	Success                             = mqtt.Success
	NormalDisconnection                 = mqtt.NormalDisconnection
	GrantedQoS0                         = mqtt.GrantedQoS0
	GrantedQoS1                         = mqtt.GrantedQoS1
	GrantedQoS2                         = mqtt.GrantedQoS2
	DisconnectWithWillMessage           = mqtt.DisconnectWithWillMessage
	NoMatchingSubscribers               = mqtt.NoMatchingSubscribers
	NoSubscriptionExisted               = mqtt.NoSubscriptionExisted
	ContinueAuthentication              = mqtt.ContinueAuthentication
	ReAuthenticate                      = mqtt.ReAuthenticate
	UnspecifiedError                    = mqtt.UnspecifiedError
	MalformedPacket                     = mqtt.MalformedPacket
	ProtocolError                       = mqtt.ProtocolError
	ImplementationSpecificError         = mqtt.ImplementationSpecificError
	UnsupportedProtocolVersion          = mqtt.UnsupportedProtocolVersion
	ClientIdentifierNotValid            = mqtt.ClientIdentifierNotValid
	BadUserNameOrPassword               = mqtt.BadUserNameOrPassword
	NotAuthorized                       = mqtt.NotAuthorized
	ServerUnavailable                   = mqtt.ServerUnavailable
	ServerBusy                          = mqtt.ServerBusy
	Banned                              = mqtt.Banned
	ServerShuttingDown                  = mqtt.ServerShuttingDown
	BadAuthenticationMethod             = mqtt.BadAuthenticationMethod
	KeepAliveTimeout                    = mqtt.KeepAliveTimeout
	SessionTakenOver                    = mqtt.SessionTakenOver
	TopicFilterInvalid                  = mqtt.TopicFilterInvalid
	TopicNameInvalid                    = mqtt.TopicNameInvalid
	PacketIdentifierInUse               = mqtt.PacketIdentifierInUse
	PacketIdentifierNotFound            = mqtt.PacketIdentifierNotFound
	ReceiveMaximumExceeded              = mqtt.ReceiveMaximumExceeded
	TopicAliasInvalid                   = mqtt.TopicAliasInvalid
	PacketTooLarge                      = mqtt.PacketTooLarge
	MessageRateTooHigh                  = mqtt.MessageRateTooHigh
	QuotaExceeded                       = mqtt.QuotaExceeded
	AdministrativeAction                = mqtt.AdministrativeAction
	PayloadFormatInvalid                = mqtt.PayloadFormatInvalid
	RetainNotSupported                  = mqtt.RetainNotSupported
	QoSNotSupported                     = mqtt.QoSNotSupported
	UseAnotherServer                    = mqtt.UseAnotherServer
	ServerMoved                         = mqtt.ServerMoved
	SharedSubscriptionsNotSupported     = mqtt.SharedSubscriptionsNotSupported
	ConnectionRateExceeded              = mqtt.ConnectionRateExceeded
	MaximumConnectTime                  = mqtt.MaximumConnectTime
	SubscriptionIdentifiersNotSupported = mqtt.SubscriptionIdentifiersNotSupported
	WildcardSubscriptionsNotSupported   = mqtt.WildcardSubscriptionsNotSupported
)

// The reason codes each packet type may carry, MQTT 5.0 section 3.
var (
	connackReasons    = set(0x00, 0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8A, 0x8C, 0x8F, 0x90, 0x95, 0x97, 0x99, 0x9A, 0x9B, 0x9C, 0x9F)
	pubackReasons     = set(0x00, 0x10, 0x80, 0x83, 0x87, 0x90, 0x91, 0x97, 0x99)
	pubrelReasons     = set(0x00, 0x92)
	subackReasons     = set(0x00, 0x01, 0x02, 0x80, 0x83, 0x87, 0x8F, 0x91, 0x97, 0x9E, 0xA1, 0xA2)
	unsubackReasons   = set(0x00, 0x11, 0x80, 0x83, 0x87, 0x8F, 0x91)
	disconnectReasons = set(0x00, 0x04, 0x80, 0x81, 0x82, 0x83, 0x87, 0x89, 0x8B, 0x8D, 0x8E, 0x8F, 0x90, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0x99, 0x9A, 0x9B, 0x9C, 0x9D, 0x9E, 0x9F, 0xA0, 0xA1, 0xA2)
	authReasons       = set(0x00, 0x18, 0x19)
)

type reasonSet [256]bool

func set(codes ...ReasonCode) *reasonSet {
	var s reasonSet
	for _, c := range codes {
		s[c] = true
	}
	return &s
}

// connackReturnCode maps a 5.0 reason code to a 3.x CONNACK return code.
func connackReturnCode(r ReasonCode) byte {
	switch r {
	case Success:
		return 0
	case UnsupportedProtocolVersion:
		return 1
	case ClientIdentifierNotValid:
		return 2
	case ServerUnavailable:
		return 3
	case BadUserNameOrPassword:
		return 4
	case NotAuthorized:
		return 5
	}
	return 3
}

// connackReasonFromReturnCode is the inverse mapping used when decoding a 3.x
// CONNACK (as a client would, in tests and in cluster linking).
func connackReasonFromReturnCode(c byte) (ReasonCode, bool) {
	switch c {
	case 0:
		return Success, true
	case 1:
		return UnsupportedProtocolVersion, true
	case 2:
		return ClientIdentifierNotValid, true
	case 3:
		return ServerUnavailable, true
	case 4:
		return BadUserNameOrPassword, true
	case 5:
		return NotAuthorized, true
	}
	return 0, false
}
