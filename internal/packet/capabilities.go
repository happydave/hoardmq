package packet

// Capabilities are the server limits and optional-feature switches a CONNACK
// advertises. The shipped default declines nothing (design invariant 6); the
// operator switch lives in the configuration document's mqtt object.
type Capabilities struct {
	MaximumQoS                       byte
	RetainAvailable                  bool
	WildcardSubscriptionAvailable    bool
	SubscriptionIdentifiersAvailable bool
	SharedSubscriptionAvailable      bool
	TopicAliasMaximum                uint16
	// MaximumPacketSize and ReceiveMaximum are per-node limits; zero means
	// the specification default (no limit, and 65535 respectively) and is not
	// advertised.
	MaximumPacketSize uint32
	ReceiveMaximum    uint16
	// ServerKeepAlive, when non-zero, overrides the client's keep alive.
	ServerKeepAlive uint16
}

// DefaultCapabilities advertises full MQTT 5.0 support.
func DefaultCapabilities() Capabilities {
	return Capabilities{
		MaximumQoS:                       2,
		RetainAvailable:                  true,
		WildcardSubscriptionAvailable:    true,
		SubscriptionIdentifiersAvailable: true,
		SharedSubscriptionAvailable:      true,
		TopicAliasMaximum:                65535,
	}
}

// ConnackProperties returns the properties that advertise the capabilities.
// A property whose value equals the specification's meaning of "absent" is
// omitted, so the default capabilities advertise nothing but Topic Alias
// Maximum (whose absent value is 0, meaning aliases are not accepted).
func (c Capabilities) ConnackProperties() Properties {
	var p Properties
	if c.MaximumQoS < 2 {
		q := c.MaximumQoS
		p.MaximumQoS = &q
	}
	if !c.RetainAvailable {
		f := false
		p.RetainAvailable = &f
	}
	if !c.WildcardSubscriptionAvailable {
		f := false
		p.WildcardSubscriptionAvailable = &f
	}
	if !c.SubscriptionIdentifiersAvailable {
		f := false
		p.SubscriptionIdentifierAvailable = &f
	}
	if !c.SharedSubscriptionAvailable {
		f := false
		p.SharedSubscriptionAvailable = &f
	}
	if c.TopicAliasMaximum != 0 {
		t := c.TopicAliasMaximum
		p.TopicAliasMaximum = &t
	}
	if c.MaximumPacketSize != 0 {
		m := c.MaximumPacketSize
		p.MaximumPacketSize = &m
	}
	if c.ReceiveMaximum != 0 && c.ReceiveMaximum != 65535 {
		r := c.ReceiveMaximum
		p.ReceiveMaximum = &r
	}
	if c.ServerKeepAlive != 0 {
		k := c.ServerKeepAlive
		p.ServerKeepAlive = &k
	}
	return p
}
