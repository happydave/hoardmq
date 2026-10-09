package packet

import (
	"bytes"
	"testing"
)

func allProperties() Properties {
	return Properties{
		PayloadFormatIndicator:          u8(1),
		MessageExpiryInterval:           u32(3600),
		ContentType:                     str("text/plain"),
		ResponseTopic:                   str("reply/here"),
		CorrelationData:                 []byte{1, 2, 3},
		SubscriptionIdentifiers:         []uint32{1, 268435455},
		SessionExpiryInterval:           u32(0xFFFFFFFF),
		AssignedClientIdentifier:        str("auto-1"),
		ServerKeepAlive:                 u16(30),
		AuthenticationMethod:            str("SCRAM-SHA-256"),
		AuthenticationData:              []byte("nonce"),
		RequestProblemInformation:       boolp(true),
		WillDelayInterval:               u32(5),
		RequestResponseInformation:      boolp(true),
		ResponseInformation:             str("resp"),
		ServerReference:                 str("other:1883"),
		ReasonString:                    str("because"),
		ReceiveMaximum:                  u16(10),
		TopicAliasMaximum:               u16(20),
		TopicAlias:                      u16(3),
		MaximumQoS:                      u8(1),
		RetainAvailable:                 boolp(false),
		UserProperties:                  []UserProperty{{"k", "v"}, {"k", "v2"}},
		MaximumPacketSize:               u32(1024),
		WildcardSubscriptionAvailable:   boolp(false),
		SubscriptionIdentifierAvailable: boolp(false),
		SharedSubscriptionAvailable:     boolp(false),
	}
}

// only keeps the members valid for a context, as the encoder would.
func only(ctx propContext, p Properties) Properties {
	if ctx != ctxPublish && len(p.SubscriptionIdentifiers) > 1 {
		p.SubscriptionIdentifiers = p.SubscriptionIdentifiers[:1]
	}
	var w writer
	encodeProperties(&w, ctx, &p)
	var out Properties
	if err := decodeProperties(&reader{buf: w.buf}, ctx, &out); err != nil {
		panic(err)
	}
	return out
}

func samplePackets(v Version) []Packet {
	props := func(ctx propContext) Properties {
		if v != V5 {
			return Properties{}
		}
		return only(ctx, allProperties())
	}
	connect := &Connect{ProtocolLevel: v, CleanStart: true, KeepAlive: 60, ClientID: "client-1", Properties: props(ctxConnect)}
	if v == V3 {
		connect.ProtocolName = "MQIsdp"
	} else {
		connect.ProtocolName = "MQTT"
	}
	full := *connect
	full.Will = &Will{Topic: "will/t", Payload: []byte("bye"), QoS: 1, Retain: true, Properties: props(ctxWill)}
	full.HasUsername, full.Username = true, "user"
	full.HasPassword, full.Password = true, []byte("secret")
	emptyPassword := *connect
	emptyPassword.HasUsername, emptyPassword.HasPassword, emptyPassword.Password = true, true, []byte{}

	pubProps := props(ctxPublish)
	pubProps.TopicAlias = nil
	packets := []Packet{
		connect, &full, &emptyPassword,
		&Connack{SessionPresent: true, ReasonCode: Success, Properties: props(ctxConnack)},
		&Connack{ReasonCode: NotAuthorized},
		&Publish{QoS: 0, Topic: "a/b", Payload: []byte{}},
		&Publish{QoS: 1, PacketID: 7, Retain: true, Topic: "a/b", Payload: []byte("hello"), Properties: pubProps},
		&Publish{QoS: 2, Dup: true, PacketID: 65535, Topic: "x", Payload: bytes.Repeat([]byte{0xAB}, 300)},
		&Puback{Ack{PacketID: 1}},
		&Pubrec{Ack{PacketID: 2}},
		&Pubrel{Ack{PacketID: 3}},
		&Pubcomp{Ack{PacketID: 4}},
		&Subscribe{PacketID: 9, Properties: props(ctxSubscribe), Subscriptions: []Subscription{{Filter: "a/#", QoS: 2}, {Filter: "b/+", QoS: 0}}},
		&Suback{PacketID: 9, Properties: props(ctxSuback), ReasonCodes: []ReasonCode{GrantedQoS2, GrantedQoS0, 0x80}},
		&Unsubscribe{PacketID: 10, Properties: props(ctxUnsubscribe), Filters: []string{"a/#", "b/+"}},
		&Pingreq{}, &Pingresp{},
		&Disconnect{},
	}
	if v == V5 {
		packets = append(packets,
			&Publish{QoS: 0, Topic: "", Payload: []byte("aliased"), Properties: Properties{TopicAlias: u16(5)}},
			&Puback{Ack{PacketID: 1, ReasonCode: NoMatchingSubscribers}},
			&Pubrec{Ack{PacketID: 2, ReasonCode: QuotaExceeded, Properties: props(ctxPubAck)}},
			&Pubrel{Ack{PacketID: 3, ReasonCode: PacketIdentifierNotFound, Properties: props(ctxPubAck)}},
			&Pubcomp{Ack{PacketID: 4, Properties: props(ctxPubAck)}},
			&Subscribe{PacketID: 11, Properties: Properties{SubscriptionIdentifiers: []uint32{42}}, Subscriptions: []Subscription{{Filter: "$share/g/a", QoS: 1, RetainAsPublished: true, RetainHandling: 2}, {Filter: "c", NoLocal: true, RetainHandling: 1}}},
			&Suback{PacketID: 11, ReasonCodes: []ReasonCode{SharedSubscriptionsNotSupported}},
			&Unsuback{PacketID: 10, Properties: props(ctxUnsuback), ReasonCodes: []ReasonCode{Success, NoSubscriptionExisted}},
			&Disconnect{ReasonCode: SessionTakenOver},
			&Disconnect{ReasonCode: UseAnotherServer, Properties: props(ctxDisconnect)},
			&Auth{},
			&Auth{ReasonCode: ContinueAuthentication, Properties: props(ctxAuth)},
		)
	} else {
		packets = append(packets, &Unsuback{PacketID: 10})
	}
	return packets
}

func TestRoundTripEveryPacketEveryLevel(t *testing.T) {
	for _, v := range []Version{V3, V4, V5} {
		for _, p := range samplePackets(v) {
			t.Run(v.String()+"/"+p.Type().String(), func(t *testing.T) { roundTrip(t, v, p) })
		}
	}
}

func TestRoundTripEveryReasonCode(t *testing.T) {
	sets := []struct {
		name string
		set  *reasonSet
		mk   func(ReasonCode) Packet
	}{
		{"CONNACK", connackReasons, func(r ReasonCode) Packet { return &Connack{ReasonCode: r} }},
		{"PUBACK", pubackReasons, func(r ReasonCode) Packet { return &Puback{Ack{PacketID: 1, ReasonCode: r}} }},
		{"PUBREC", pubackReasons, func(r ReasonCode) Packet { return &Pubrec{Ack{PacketID: 1, ReasonCode: r}} }},
		{"PUBREL", pubrelReasons, func(r ReasonCode) Packet { return &Pubrel{Ack{PacketID: 1, ReasonCode: r}} }},
		{"PUBCOMP", pubrelReasons, func(r ReasonCode) Packet { return &Pubcomp{Ack{PacketID: 1, ReasonCode: r}} }},
		{"SUBACK", subackReasons, func(r ReasonCode) Packet { return &Suback{PacketID: 1, ReasonCodes: []ReasonCode{r}} }},
		{"UNSUBACK", unsubackReasons, func(r ReasonCode) Packet { return &Unsuback{PacketID: 1, ReasonCodes: []ReasonCode{r}} }},
		{"DISCONNECT", disconnectReasons, func(r ReasonCode) Packet { return &Disconnect{ReasonCode: r} }},
		{"AUTH", authReasons, func(r ReasonCode) Packet { return &Auth{ReasonCode: r} }},
	}
	for _, s := range sets {
		n := 0
		for code := 0; code < 256; code++ {
			if !s.set[code] {
				continue
			}
			n++
			roundTrip(t, V5, s.mk(ReasonCode(code)))
			if _, ok := reasonNames[ReasonCode(code)]; !ok {
				t.Errorf("%s reason 0x%02X has no name", s.name, code)
			}
		}
		if n == 0 {
			t.Errorf("%s has an empty reason set", s.name)
		}
	}
}

func TestConnackReturnCodeMappingAtLevel4(t *testing.T) {
	cases := map[ReasonCode]ReasonCode{
		Success: Success, UnsupportedProtocolVersion: UnsupportedProtocolVersion, ClientIdentifierNotValid: ClientIdentifierNotValid,
		ServerUnavailable: ServerUnavailable, BadUserNameOrPassword: BadUserNameOrPassword, NotAuthorized: NotAuthorized,
		Banned: ServerUnavailable, QuotaExceeded: ServerUnavailable,
	}
	for in, want := range cases {
		raw, err := (&Encoder{Version: V4}).Bytes(&Connack{ReasonCode: in})
		if err != nil {
			t.Fatal(err)
		}
		got, err := decodeRaw(V4, raw)
		if err != nil {
			t.Fatal(err)
		}
		if got.(*Connack).ReasonCode != want {
			t.Errorf("%s at level 4 decoded as %s, want %s", in, got.(*Connack).ReasonCode, want)
		}
	}
}

func TestPropertiesStrippedBelowLevel5(t *testing.T) {
	p := &Publish{QoS: 1, PacketID: 1, Topic: "t", Payload: []byte("x"), Properties: allProperties()}
	p.Properties.TopicAlias = nil
	for _, v := range []Version{V3, V4} {
		raw, err := (&Encoder{Version: v}).Bytes(p)
		if err != nil {
			t.Fatal(err)
		}
		want := hex(0x32, 0x06, 0x00, 0x01, 't', 0x00, 0x01, 'x')
		if !bytes.Equal(raw, want) {
			t.Errorf("%s: got % x want % x", v, raw, want)
		}
	}
}

func TestEncodeSkipsPropertiesInvalidForContext(t *testing.T) {
	// A CONNACK built with a PUBLISH-only property must not emit it.
	c := &Connack{Properties: Properties{TopicAlias: u16(1), ReasonString: str("r")}}
	raw, err := (&Encoder{Version: V5}).Bytes(c)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeRaw(V5, raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.(*Connack).Properties.TopicAlias != nil || *got.(*Connack).Properties.ReasonString != "r" {
		t.Errorf("got %+v", got.(*Connack).Properties)
	}
}

func TestEncodeStreamingMatchesBytes(t *testing.T) {
	for _, v := range []Version{V4, V5} {
		for _, p := range samplePackets(v) {
			enc := Encoder{Version: v}
			want, err := enc.Bytes(p)
			if err != nil {
				t.Fatal(err)
			}
			var buf bytes.Buffer
			if err := enc.Encode(&buf, p); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(buf.Bytes(), want) {
				t.Errorf("%s %s: streaming % x != % x", v, p.Type(), buf.Bytes(), want)
			}
		}
	}
}

func TestEncodeLargePayloadUsesFourByteLength(t *testing.T) {
	p := &Publish{Topic: "t", Payload: bytes.Repeat([]byte{1}, 3_000_000)}
	raw, err := (&Encoder{Version: V4}).Bytes(p)
	if err != nil {
		t.Fatal(err)
	}
	if raw[0] != 0x30 || raw[4]&0x80 != 0 || raw[3]&0x80 == 0 {
		t.Errorf("header % x", raw[:5])
	}
	got, err := decodeRaw(V4, raw)
	if err != nil || len(got.(*Publish).Payload) != 3_000_000 {
		t.Fatalf("decode: %v", err)
	}
}

func TestEncoderMaxPacketSize(t *testing.T) {
	enc := Encoder{Version: V5, MaxPacketSize: 10}
	_, err := enc.Bytes(&Publish{Topic: "topic", Payload: []byte("payload")})
	if reasonOf(t, err) != PacketTooLarge {
		t.Fatal(err)
	}
	if _, err := enc.Bytes(&Pingresp{}); err != nil {
		t.Fatal(err)
	}
}

func TestDefaultCapabilitiesDeclineNothing(t *testing.T) {
	c := DefaultCapabilities()
	if c.MaximumQoS != 2 || !c.RetainAvailable || !c.WildcardSubscriptionAvailable || !c.SubscriptionIdentifiersAvailable || !c.SharedSubscriptionAvailable || c.TopicAliasMaximum != 65535 {
		t.Fatalf("%+v", c)
	}
	p := c.ConnackProperties()
	if p.MaximumQoS != nil || p.RetainAvailable != nil || p.WildcardSubscriptionAvailable != nil || p.SubscriptionIdentifierAvailable != nil || p.SharedSubscriptionAvailable != nil {
		t.Errorf("default capabilities must advertise no declines: %+v", p)
	}
	if p.TopicAliasMaximum == nil || *p.TopicAliasMaximum != 65535 {
		t.Errorf("topic alias maximum not advertised")
	}
	declined := Capabilities{MaximumQoS: 0, TopicAliasMaximum: 0, ReceiveMaximum: 5, MaximumPacketSize: 100, ServerKeepAlive: 20}
	p = declined.ConnackProperties()
	if p.MaximumQoS == nil || *p.MaximumQoS != 0 || p.RetainAvailable == nil || *p.RetainAvailable || p.SharedSubscriptionAvailable == nil || p.TopicAliasMaximum != nil || *p.ReceiveMaximum != 5 || *p.MaximumPacketSize != 100 || *p.ServerKeepAlive != 20 {
		t.Errorf("declined capabilities: %+v", p)
	}
	roundTrip(t, V5, &Connack{Properties: p})
}
