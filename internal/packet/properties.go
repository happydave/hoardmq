package packet

// Property identifiers, MQTT 5.0 section 2.2.2.2.
const (
	propPayloadFormatIndicator          = 0x01
	propMessageExpiryInterval           = 0x02
	propContentType                     = 0x03
	propResponseTopic                   = 0x08
	propCorrelationData                 = 0x09
	propSubscriptionIdentifier          = 0x0B
	propSessionExpiryInterval           = 0x11
	propAssignedClientIdentifier        = 0x12
	propServerKeepAlive                 = 0x13
	propAuthenticationMethod            = 0x15
	propAuthenticationData              = 0x16
	propRequestProblemInformation       = 0x17
	propWillDelayInterval               = 0x18
	propRequestResponseInformation      = 0x19
	propResponseInformation             = 0x1A
	propServerReference                 = 0x1C
	propReasonString                    = 0x1F
	propReceiveMaximum                  = 0x21
	propTopicAliasMaximum               = 0x22
	propTopicAlias                      = 0x23
	propMaximumQoS                      = 0x24
	propRetainAvailable                 = 0x25
	propUserProperty                    = 0x26
	propMaximumPacketSize               = 0x27
	propWildcardSubscriptionAvailable   = 0x28
	propSubscriptionIdentifierAvailable = 0x29
	propSharedSubscriptionAvailable     = 0x2A
)

// propContext names the packet (or the Will) whose property set applies.
type propContext byte

const (
	ctxConnect propContext = iota
	ctxWill
	ctxConnack
	ctxPublish
	ctxPubAck // PUBACK, PUBREC, PUBREL, PUBCOMP
	ctxSubscribe
	ctxSuback
	ctxUnsubscribe
	ctxUnsuback
	ctxDisconnect
	ctxAuth
)

// allowed lists, per context, the identifiers MQTT 5.0 table 2-4 permits.
var allowed = map[propContext][]byte{
	ctxConnect:     {0x11, 0x21, 0x27, 0x22, 0x19, 0x17, 0x26, 0x15, 0x16},
	ctxWill:        {0x18, 0x01, 0x02, 0x03, 0x08, 0x09, 0x26},
	ctxConnack:     {0x11, 0x21, 0x24, 0x25, 0x27, 0x12, 0x22, 0x1F, 0x26, 0x28, 0x29, 0x2A, 0x13, 0x1A, 0x1C, 0x15, 0x16},
	ctxPublish:     {0x01, 0x02, 0x23, 0x08, 0x09, 0x26, 0x0B, 0x03},
	ctxPubAck:      {0x1F, 0x26},
	ctxSubscribe:   {0x0B, 0x26},
	ctxSuback:      {0x1F, 0x26},
	ctxUnsubscribe: {0x26},
	ctxUnsuback:    {0x1F, 0x26},
	ctxDisconnect:  {0x11, 0x1F, 0x26, 0x1C},
	ctxAuth:        {0x15, 0x16, 0x1F, 0x26},
}

var allowedSets = func() map[propContext]*[256]bool {
	out := map[propContext]*[256]bool{}
	for ctx, ids := range allowed {
		var s [256]bool
		for _, id := range ids {
			s[id] = true
		}
		out[ctx] = &s
	}
	return out
}()

// decodeProperties reads a property length and the properties it covers,
// enforcing the per-context allow list, the once-only rule and the value
// rules. Unknown identifiers are Malformed Packet; a property that is not
// valid for the context is Malformed Packet; repeats and bad values are
// Protocol Error.
func decodeProperties(r *reader, ctx propContext, p *Properties) *Error {
	length, err := r.varint()
	if err != nil {
		return err
	}
	if int(length) > r.remaining() {
		return malformed("property length %d overruns the packet", length)
	}
	sub := &reader{buf: r.buf[r.pos : r.pos+int(length)]}
	r.pos += int(length)
	permitted := allowedSets[ctx]
	var seen [256]bool
	for sub.remaining() > 0 {
		id, err := sub.varint()
		if err != nil {
			return err
		}
		if id > 255 || !isKnownProperty(byte(id)) {
			return malformed("unknown property identifier 0x%02X", id)
		}
		if !permitted[id] {
			return malformed("property 0x%02X is not valid in this packet", id)
		}
		repeatable := id == propUserProperty || (id == propSubscriptionIdentifier && ctx == ctxPublish)
		if seen[id] && !repeatable {
			return protocolError("property 0x%02X appears more than once", id)
		}
		seen[id] = true
		if err := decodeProperty(sub, byte(id), p); err != nil {
			return err
		}
	}
	if ctx == ctxConnect && p.AuthenticationData != nil && p.AuthenticationMethod == nil {
		return protocolError("Authentication Data without Authentication Method")
	}
	return nil
}

func isKnownProperty(id byte) bool {
	switch id {
	case 0x01, 0x02, 0x03, 0x08, 0x09, 0x0B, 0x11, 0x12, 0x13, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1C, 0x1F, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2A:
		return true
	}
	return false
}

func decodeProperty(r *reader, id byte, p *Properties) *Error {
	boolProp := func(name string) (*bool, *Error) {
		b, err := r.byte()
		if err != nil {
			return nil, err
		}
		if b > 1 {
			return nil, protocolError("%s must be 0 or 1, got %d", name, b)
		}
		v := b == 1
		return &v, nil
	}
	var err *Error
	switch id {
	case propPayloadFormatIndicator:
		var b byte
		if b, err = r.byte(); err == nil {
			if b > 1 {
				return protocolError("Payload Format Indicator must be 0 or 1, got %d", b)
			}
			p.PayloadFormatIndicator = &b
		}
	case propMessageExpiryInterval:
		var v uint32
		if v, err = r.uint32(); err == nil {
			p.MessageExpiryInterval = &v
		}
	case propContentType:
		p.ContentType, err = stringProp(r)
	case propResponseTopic:
		p.ResponseTopic, err = stringProp(r)
	case propCorrelationData:
		p.CorrelationData, err = r.bytes()
	case propSubscriptionIdentifier:
		var v uint32
		if v, err = r.varint(); err == nil {
			if v == 0 {
				return protocolError("Subscription Identifier must not be 0")
			}
			p.SubscriptionIdentifiers = append(p.SubscriptionIdentifiers, v)
		}
	case propSessionExpiryInterval:
		var v uint32
		if v, err = r.uint32(); err == nil {
			p.SessionExpiryInterval = &v
		}
	case propAssignedClientIdentifier:
		p.AssignedClientIdentifier, err = stringProp(r)
	case propServerKeepAlive:
		var v uint16
		if v, err = r.uint16(); err == nil {
			p.ServerKeepAlive = &v
		}
	case propAuthenticationMethod:
		p.AuthenticationMethod, err = stringProp(r)
	case propAuthenticationData:
		p.AuthenticationData, err = r.bytes()
	case propRequestProblemInformation:
		p.RequestProblemInformation, err = boolProp("Request Problem Information")
	case propWillDelayInterval:
		var v uint32
		if v, err = r.uint32(); err == nil {
			p.WillDelayInterval = &v
		}
	case propRequestResponseInformation:
		p.RequestResponseInformation, err = boolProp("Request Response Information")
	case propResponseInformation:
		p.ResponseInformation, err = stringProp(r)
	case propServerReference:
		p.ServerReference, err = stringProp(r)
	case propReasonString:
		p.ReasonString, err = stringProp(r)
	case propReceiveMaximum:
		var v uint16
		if v, err = r.uint16(); err == nil {
			if v == 0 {
				return protocolError("Receive Maximum must not be 0")
			}
			p.ReceiveMaximum = &v
		}
	case propTopicAliasMaximum:
		var v uint16
		if v, err = r.uint16(); err == nil {
			p.TopicAliasMaximum = &v
		}
	case propTopicAlias:
		var v uint16
		if v, err = r.uint16(); err == nil {
			if v == 0 {
				return &Error{Reason: TopicAliasInvalid, Message: "Topic Alias must not be 0"}
			}
			p.TopicAlias = &v
		}
	case propMaximumQoS:
		var b byte
		if b, err = r.byte(); err == nil {
			if b > 1 {
				return protocolError("Maximum QoS must be 0 or 1, got %d", b)
			}
			p.MaximumQoS = &b
		}
	case propRetainAvailable:
		p.RetainAvailable, err = boolProp("Retain Available")
	case propUserProperty:
		var name, value string
		if name, err = r.string(); err == nil {
			if value, err = r.string(); err == nil {
				p.UserProperties = append(p.UserProperties, UserProperty{Name: name, Value: value})
			}
		}
	case propMaximumPacketSize:
		var v uint32
		if v, err = r.uint32(); err == nil {
			if v == 0 {
				return protocolError("Maximum Packet Size must not be 0")
			}
			p.MaximumPacketSize = &v
		}
	case propWildcardSubscriptionAvailable:
		p.WildcardSubscriptionAvailable, err = boolProp("Wildcard Subscription Available")
	case propSubscriptionIdentifierAvailable:
		p.SubscriptionIdentifierAvailable, err = boolProp("Subscription Identifier Available")
	case propSharedSubscriptionAvailable:
		p.SharedSubscriptionAvailable, err = boolProp("Shared Subscription Available")
	}
	return err
}

func stringProp(r *reader) (*string, *Error) {
	s, err := r.string()
	if err != nil {
		return nil, err
	}
	return &s, nil
}

// empty reports whether no property is set.
func (p *Properties) empty() bool {
	return p.PayloadFormatIndicator == nil && p.MessageExpiryInterval == nil && p.ContentType == nil &&
		p.ResponseTopic == nil && p.CorrelationData == nil && len(p.SubscriptionIdentifiers) == 0 &&
		p.SessionExpiryInterval == nil && p.AssignedClientIdentifier == nil && p.ServerKeepAlive == nil &&
		p.AuthenticationMethod == nil && p.AuthenticationData == nil && p.RequestProblemInformation == nil &&
		p.WillDelayInterval == nil && p.RequestResponseInformation == nil && p.ResponseInformation == nil &&
		p.ServerReference == nil && p.ReasonString == nil && p.ReceiveMaximum == nil &&
		p.TopicAliasMaximum == nil && p.TopicAlias == nil && p.MaximumQoS == nil && p.RetainAvailable == nil &&
		len(p.UserProperties) == 0 && p.MaximumPacketSize == nil && p.WildcardSubscriptionAvailable == nil &&
		p.SubscriptionIdentifierAvailable == nil && p.SharedSubscriptionAvailable == nil
}

// encodeProperties writes the property length and the properties in
// identifier order. Properties not valid in the context are skipped, so a
// packet built for one context and reused in another cannot emit an invalid
// property.
func encodeProperties(w *writer, ctx propContext, p *Properties) {
	var body writer
	permitted := allowedSets[ctx]
	emit := func(id byte, f func()) {
		if permitted[id] {
			f()
		}
	}
	if p.PayloadFormatIndicator != nil {
		emit(propPayloadFormatIndicator, func() { body.byte(propPayloadFormatIndicator); body.byte(*p.PayloadFormatIndicator) })
	}
	if p.MessageExpiryInterval != nil {
		emit(propMessageExpiryInterval, func() { body.byte(propMessageExpiryInterval); body.uint32(*p.MessageExpiryInterval) })
	}
	if p.ContentType != nil {
		emit(propContentType, func() { body.byte(propContentType); body.string(*p.ContentType) })
	}
	if p.ResponseTopic != nil {
		emit(propResponseTopic, func() { body.byte(propResponseTopic); body.string(*p.ResponseTopic) })
	}
	if p.CorrelationData != nil {
		emit(propCorrelationData, func() { body.byte(propCorrelationData); body.bytes(p.CorrelationData) })
	}
	for _, id := range p.SubscriptionIdentifiers {
		emit(propSubscriptionIdentifier, func() { body.byte(propSubscriptionIdentifier); body.varint(id) })
	}
	if p.SessionExpiryInterval != nil {
		emit(propSessionExpiryInterval, func() { body.byte(propSessionExpiryInterval); body.uint32(*p.SessionExpiryInterval) })
	}
	if p.AssignedClientIdentifier != nil {
		emit(propAssignedClientIdentifier, func() { body.byte(propAssignedClientIdentifier); body.string(*p.AssignedClientIdentifier) })
	}
	if p.ServerKeepAlive != nil {
		emit(propServerKeepAlive, func() { body.byte(propServerKeepAlive); body.uint16(*p.ServerKeepAlive) })
	}
	if p.AuthenticationMethod != nil {
		emit(propAuthenticationMethod, func() { body.byte(propAuthenticationMethod); body.string(*p.AuthenticationMethod) })
	}
	if p.AuthenticationData != nil {
		emit(propAuthenticationData, func() { body.byte(propAuthenticationData); body.bytes(p.AuthenticationData) })
	}
	if p.RequestProblemInformation != nil {
		emit(propRequestProblemInformation, func() { body.byte(propRequestProblemInformation); body.byte(boolByte(*p.RequestProblemInformation)) })
	}
	if p.WillDelayInterval != nil {
		emit(propWillDelayInterval, func() { body.byte(propWillDelayInterval); body.uint32(*p.WillDelayInterval) })
	}
	if p.RequestResponseInformation != nil {
		emit(propRequestResponseInformation, func() { body.byte(propRequestResponseInformation); body.byte(boolByte(*p.RequestResponseInformation)) })
	}
	if p.ResponseInformation != nil {
		emit(propResponseInformation, func() { body.byte(propResponseInformation); body.string(*p.ResponseInformation) })
	}
	if p.ServerReference != nil {
		emit(propServerReference, func() { body.byte(propServerReference); body.string(*p.ServerReference) })
	}
	if p.ReasonString != nil {
		emit(propReasonString, func() { body.byte(propReasonString); body.string(*p.ReasonString) })
	}
	if p.ReceiveMaximum != nil {
		emit(propReceiveMaximum, func() { body.byte(propReceiveMaximum); body.uint16(*p.ReceiveMaximum) })
	}
	if p.TopicAliasMaximum != nil {
		emit(propTopicAliasMaximum, func() { body.byte(propTopicAliasMaximum); body.uint16(*p.TopicAliasMaximum) })
	}
	if p.TopicAlias != nil {
		emit(propTopicAlias, func() { body.byte(propTopicAlias); body.uint16(*p.TopicAlias) })
	}
	if p.MaximumQoS != nil {
		emit(propMaximumQoS, func() { body.byte(propMaximumQoS); body.byte(*p.MaximumQoS) })
	}
	if p.RetainAvailable != nil {
		emit(propRetainAvailable, func() { body.byte(propRetainAvailable); body.byte(boolByte(*p.RetainAvailable)) })
	}
	for _, up := range p.UserProperties {
		emit(propUserProperty, func() { body.byte(propUserProperty); body.string(up.Name); body.string(up.Value) })
	}
	if p.MaximumPacketSize != nil {
		emit(propMaximumPacketSize, func() { body.byte(propMaximumPacketSize); body.uint32(*p.MaximumPacketSize) })
	}
	if p.WildcardSubscriptionAvailable != nil {
		emit(propWildcardSubscriptionAvailable, func() {
			body.byte(propWildcardSubscriptionAvailable)
			body.byte(boolByte(*p.WildcardSubscriptionAvailable))
		})
	}
	if p.SubscriptionIdentifierAvailable != nil {
		emit(propSubscriptionIdentifierAvailable, func() {
			body.byte(propSubscriptionIdentifierAvailable)
			body.byte(boolByte(*p.SubscriptionIdentifierAvailable))
		})
	}
	if p.SharedSubscriptionAvailable != nil {
		emit(propSharedSubscriptionAvailable, func() {
			body.byte(propSharedSubscriptionAvailable)
			body.byte(boolByte(*p.SharedSubscriptionAvailable))
		})
	}
	w.varint(uint32(len(body.buf)))
	w.buf = append(w.buf, body.buf...)
}

func boolByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}
