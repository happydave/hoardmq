package packet

import (
	"fmt"
	"io"
	"strings"
)

// Decoder reads packets from a connection. It holds only the protocol level
// and a five byte header scratch area between packets; body buffers come from
// the pool while a packet is being read and go back when it has been decoded.
type Decoder struct {
	r io.Reader
	// Version is the connection's protocol level. ReadConnect sets it; a
	// caller that already knows the level may set it directly.
	Version Version
	// MaxPacketSize bounds the whole packet (header included). Zero means
	// the wire maximum. A larger packet is refused with Packet Too Large
	// before its body is read.
	MaxPacketSize uint32
	hdr           [5]byte
}

// NewDecoder returns a decoder over r with no version set.
func NewDecoder(r io.Reader) *Decoder { return &Decoder{r: r} }

// ReadConnect reads the first packet of a connection, requires it to be a
// CONNECT, and fixes the decoder's version from it. An unsupported protocol
// name or level is returned as an Error with Reason UnsupportedProtocolVersion
// and Level set to what the client sent.
func (d *Decoder) ReadConnect() (*Connect, error) {
	t, flags, body, class, err := d.readRaw()
	if err != nil {
		return nil, err
	}
	defer release(body, class)
	if t != CONNECT {
		return nil, protocolError("first packet is %s, not CONNECT", t)
	}
	if flags != 0 {
		return nil, malformed("CONNECT flags must be 0")
	}
	c, derr := decodeConnect(&reader{buf: body})
	if derr != nil {
		return nil, derr
	}
	d.Version = c.ProtocolLevel
	return c, nil
}

// Read reads the next packet in the connection's grammar. It returns an
// *Error for protocol violations (the connection must be closed after the
// caller sends its reason) and the underlying read error for transport
// failures.
func (d *Decoder) Read() (Packet, error) {
	if d.Version == 0 {
		return nil, fmt.Errorf("packet: decoder version not set; call ReadConnect first")
	}
	t, flags, body, class, err := d.readRaw()
	if err != nil {
		return nil, err
	}
	defer release(body, class)
	p, derr := decode(d.Version, t, flags, body)
	if derr != nil {
		return nil, derr
	}
	return p, nil
}

// readRaw reads one packet's header and body. The first byte is read on its
// own so that an idle connection blocks with no buffer allocated.
func (d *Decoder) readRaw() (Type, byte, []byte, int, error) {
	if _, err := io.ReadFull(d.r, d.hdr[:1]); err != nil {
		return 0, 0, nil, -1, err
	}
	t := Type(d.hdr[0] >> 4)
	flags := d.hdr[0] & 0x0F
	var length, multiplier uint32 = 0, 1
	n := 1
	for {
		if multiplier > 128*128*128 {
			return 0, 0, nil, -1, malformed("Remaining Length longer than four bytes")
		}
		if _, err := io.ReadFull(d.r, d.hdr[n:n+1]); err != nil {
			return 0, 0, nil, -1, wrapEOF(err)
		}
		b := d.hdr[n]
		n++
		length += uint32(b&0x7F) * multiplier
		if b&0x80 == 0 {
			break
		}
		multiplier *= 128
	}
	limit := uint32(MaxPacketSize)
	if d.MaxPacketSize != 0 && d.MaxPacketSize < limit {
		limit = d.MaxPacketSize
	}
	if length+uint32(n) > limit {
		return 0, 0, nil, -1, &Error{Reason: PacketTooLarge, Message: fmt.Sprintf("packet of %d bytes exceeds the maximum of %d", length+uint32(n), limit)}
	}
	body, class := acquire(int(length))
	if _, err := io.ReadFull(d.r, body); err != nil {
		release(body, class)
		return 0, 0, nil, -1, wrapEOF(err)
	}
	return t, flags, body, class, nil
}

// wrapEOF turns an EOF inside a packet into ErrUnexpectedEOF so a caller can
// tell a clean close from a truncated packet.
func wrapEOF(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}

// Decode decodes one packet from its type, flags and body in the given
// grammar. It is the in-memory form of Read, used by tests and by the fuzz
// targets; Read is the streaming form.
func Decode(v Version, t Type, flags byte, body []byte) (Packet, error) {
	p, err := decode(v, t, flags, body)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func decode(v Version, t Type, flags byte, body []byte) (Packet, *Error) {
	if t == AUTH && v != V5 {
		return nil, malformed("AUTH is not valid at %s", v)
	}
	switch t {
	case PUBLISH:
	case SUBSCRIBE, UNSUBSCRIBE, PUBREL:
		if flags != 0x02 {
			return nil, malformed("%s flags must be 0010", t)
		}
	case CONNECT, CONNACK, PUBACK, PUBREC, PUBCOMP, SUBACK, UNSUBACK, PINGREQ, PINGRESP, DISCONNECT, AUTH:
		if flags != 0 {
			return nil, malformed("%s flags must be 0000", t)
		}
	default:
		return nil, malformed("invalid packet type %d", byte(t))
	}
	r := &reader{buf: body}
	var p Packet
	var err *Error
	switch t {
	case CONNECT:
		p, err = decodeConnect(r)
	case CONNACK:
		p, err = decodeConnack(v, r)
	case PUBLISH:
		p, err = decodePublish(v, flags, r)
	case PUBACK, PUBREC, PUBREL, PUBCOMP:
		p, err = decodeAck(v, t, r)
	case SUBSCRIBE:
		p, err = decodeSubscribe(v, r)
	case SUBACK:
		p, err = decodeSuback(v, r)
	case UNSUBSCRIBE:
		p, err = decodeUnsubscribe(v, r)
	case UNSUBACK:
		p, err = decodeUnsuback(v, r)
	case PINGREQ:
		p = &Pingreq{}
	case PINGRESP:
		p = &Pingresp{}
	case DISCONNECT:
		p, err = decodeDisconnect(v, r)
	case AUTH:
		p, err = decodeAuth(r)
	}
	if err != nil {
		return nil, err
	}
	if t != PUBLISH && r.remaining() > 0 {
		return nil, malformed("%d bytes left over after %s", r.remaining(), t)
	}
	return p, nil
}

func decodeConnect(r *reader) (*Connect, *Error) {
	c := &Connect{}
	var err *Error
	if c.ProtocolName, err = r.string(); err != nil {
		return nil, err
	}
	level, err := r.byte()
	if err != nil {
		return nil, err
	}
	c.ProtocolLevel = Version(level)
	switch {
	case c.ProtocolName == "MQIsdp" && level == 3, c.ProtocolName == "MQTT" && (level == 4 || level == 5):
	default:
		return nil, &Error{Reason: UnsupportedProtocolVersion, Message: fmt.Sprintf("protocol %q level %d", c.ProtocolName, level), Level: level}
	}
	v := c.ProtocolLevel
	flags, err := r.byte()
	if err != nil {
		return nil, err
	}
	if flags&0x01 != 0 {
		return nil, malformed("CONNECT reserved flag set")
	}
	c.CleanStart = flags&0x02 != 0
	willFlag := flags&0x04 != 0
	willQoS := (flags >> 3) & 0x03
	willRetain := flags&0x20 != 0
	c.HasPassword = flags&0x40 != 0
	c.HasUsername = flags&0x80 != 0
	if !willFlag && (willQoS != 0 || willRetain) {
		return nil, malformed("Will QoS or Will Retain set without Will Flag")
	}
	if willQoS == 3 {
		return nil, malformed("Will QoS 3")
	}
	if c.HasPassword && !c.HasUsername && v != V5 {
		return nil, malformed("password flag without user name flag")
	}
	if c.KeepAlive, err = r.uint16(); err != nil {
		return nil, err
	}
	if v == V5 {
		if err = decodeProperties(r, ctxConnect, &c.Properties); err != nil {
			return nil, err
		}
	}
	if c.ClientID, err = r.string(); err != nil {
		return nil, err
	}
	if v == V3 {
		if n := len([]rune(c.ClientID)); n == 0 || n > 23 {
			return nil, &Error{Reason: ClientIdentifierNotValid, Message: fmt.Sprintf("client identifier of %d characters at protocol level 3", n)}
		}
	}
	if willFlag {
		w := &Will{QoS: willQoS, Retain: willRetain}
		if v == V5 {
			if err = decodeProperties(r, ctxWill, &w.Properties); err != nil {
				return nil, err
			}
		}
		if w.Topic, err = r.string(); err != nil {
			return nil, err
		}
		if w.Payload, err = r.bytes(); err != nil {
			return nil, err
		}
		c.Will = w
	}
	if c.HasUsername {
		if c.Username, err = r.string(); err != nil {
			return nil, err
		}
	}
	if c.HasPassword {
		if c.Password, err = r.bytes(); err != nil {
			return nil, err
		}
	}
	if r.remaining() > 0 {
		return nil, malformed("%d bytes left over after CONNECT", r.remaining())
	}
	return c, nil
}

func decodeConnack(v Version, r *reader) (*Connack, *Error) {
	c := &Connack{}
	flags, err := r.byte()
	if err != nil {
		return nil, err
	}
	if flags&0xFE != 0 {
		return nil, malformed("CONNACK acknowledge flags reserved bits set")
	}
	c.SessionPresent = flags&0x01 != 0
	code, err := r.byte()
	if err != nil {
		return nil, err
	}
	if v == V5 {
		c.ReasonCode = ReasonCode(code)
		if !connackReasons[c.ReasonCode] {
			return nil, protocolError("CONNACK reason code %s is not valid", c.ReasonCode)
		}
		if err = decodeProperties(r, ctxConnack, &c.Properties); err != nil {
			return nil, err
		}
		return c, nil
	}
	rc, ok := connackReasonFromReturnCode(code)
	if !ok {
		return nil, protocolError("CONNACK return code %d is not valid", code)
	}
	c.ReasonCode = rc
	return c, nil
}

func decodePublish(v Version, flags byte, r *reader) (*Publish, *Error) {
	p := &Publish{Dup: flags&0x08 != 0, QoS: (flags >> 1) & 0x03, Retain: flags&0x01 != 0}
	if p.QoS == 3 {
		return nil, malformed("PUBLISH QoS 3")
	}
	if p.Dup && p.QoS == 0 {
		return nil, malformed("PUBLISH DUP set with QoS 0")
	}
	var err *Error
	if p.Topic, err = r.string(); err != nil {
		return nil, err
	}
	if strings.ContainsAny(p.Topic, "+#") {
		return nil, protocolError("PUBLISH topic name contains a wildcard character")
	}
	if p.QoS > 0 {
		if p.PacketID, err = r.uint16(); err != nil {
			return nil, err
		}
		if p.PacketID == 0 {
			return nil, protocolError("PUBLISH packet identifier 0")
		}
	}
	if v == V5 {
		if err = decodeProperties(r, ctxPublish, &p.Properties); err != nil {
			return nil, err
		}
	}
	if p.Topic == "" && (v != V5 || p.Properties.TopicAlias == nil) {
		return nil, protocolError("PUBLISH topic name is empty")
	}
	p.Payload = r.rest()
	return p, nil
}

func decodeAck(v Version, t Type, r *reader) (Packet, *Error) {
	var a Ack
	var err *Error
	if a.PacketID, err = r.uint16(); err != nil {
		return nil, err
	}
	if a.PacketID == 0 {
		return nil, protocolError("%s packet identifier 0", t)
	}
	if v == V5 && r.remaining() > 0 {
		code, err := r.byte()
		if err != nil {
			return nil, err
		}
		a.ReasonCode = ReasonCode(code)
		valid := pubackReasons
		if t == PUBREL || t == PUBCOMP {
			valid = pubrelReasons
		}
		if !valid[a.ReasonCode] {
			return nil, protocolError("%s reason code %s is not valid", t, a.ReasonCode)
		}
		if r.remaining() > 0 {
			if err = decodeProperties(r, ctxPubAck, &a.Properties); err != nil {
				return nil, err
			}
		}
	}
	switch t {
	case PUBACK:
		return &Puback{a}, nil
	case PUBREC:
		return &Pubrec{a}, nil
	case PUBREL:
		return &Pubrel{a}, nil
	}
	return &Pubcomp{a}, nil
}

func decodeSubscribe(v Version, r *reader) (*Subscribe, *Error) {
	s := &Subscribe{}
	var err *Error
	if s.PacketID, err = r.uint16(); err != nil {
		return nil, err
	}
	if s.PacketID == 0 {
		return nil, protocolError("SUBSCRIBE packet identifier 0")
	}
	if v == V5 {
		if err = decodeProperties(r, ctxSubscribe, &s.Properties); err != nil {
			return nil, err
		}
	}
	for r.remaining() > 0 {
		var sub Subscription
		if sub.Filter, err = r.string(); err != nil {
			return nil, err
		}
		opts, err := r.byte()
		if err != nil {
			return nil, err
		}
		sub.QoS = opts & 0x03
		if sub.QoS == 3 {
			return nil, malformed("subscription QoS 3")
		}
		if v == V5 {
			if opts&0xC0 != 0 {
				return nil, malformed("subscription options reserved bits set")
			}
			sub.NoLocal = opts&0x04 != 0
			sub.RetainAsPublished = opts&0x08 != 0
			sub.RetainHandling = (opts >> 4) & 0x03
			if sub.RetainHandling == 3 {
				return nil, malformed("Retain Handling 3")
			}
			if sub.NoLocal && strings.HasPrefix(sub.Filter, "$share/") {
				return nil, protocolError("No Local on a shared subscription")
			}
		} else if opts&0xFC != 0 {
			return nil, malformed("subscription options reserved bits set")
		}
		s.Subscriptions = append(s.Subscriptions, sub)
	}
	if len(s.Subscriptions) == 0 {
		return nil, protocolError("SUBSCRIBE with no topic filters")
	}
	return s, nil
}

func decodeSuback(v Version, r *reader) (*Suback, *Error) {
	s := &Suback{}
	var err *Error
	if s.PacketID, err = r.uint16(); err != nil {
		return nil, err
	}
	if v == V5 {
		if err = decodeProperties(r, ctxSuback, &s.Properties); err != nil {
			return nil, err
		}
	}
	for r.remaining() > 0 {
		b, _ := r.byte()
		code := ReasonCode(b)
		if v == V5 {
			if !subackReasons[code] {
				return nil, protocolError("SUBACK reason code %s is not valid", code)
			}
		} else if b > 2 && b != 0x80 {
			return nil, protocolError("SUBACK return code 0x%02X is not valid", b)
		}
		s.ReasonCodes = append(s.ReasonCodes, code)
	}
	if len(s.ReasonCodes) == 0 {
		return nil, protocolError("SUBACK with no reason codes")
	}
	return s, nil
}

func decodeUnsubscribe(v Version, r *reader) (*Unsubscribe, *Error) {
	u := &Unsubscribe{}
	var err *Error
	if u.PacketID, err = r.uint16(); err != nil {
		return nil, err
	}
	if u.PacketID == 0 {
		return nil, protocolError("UNSUBSCRIBE packet identifier 0")
	}
	if v == V5 {
		if err = decodeProperties(r, ctxUnsubscribe, &u.Properties); err != nil {
			return nil, err
		}
	}
	for r.remaining() > 0 {
		f, err := r.string()
		if err != nil {
			return nil, err
		}
		u.Filters = append(u.Filters, f)
	}
	if len(u.Filters) == 0 {
		return nil, protocolError("UNSUBSCRIBE with no topic filters")
	}
	return u, nil
}

func decodeUnsuback(v Version, r *reader) (*Unsuback, *Error) {
	u := &Unsuback{}
	var err *Error
	if u.PacketID, err = r.uint16(); err != nil {
		return nil, err
	}
	if v != V5 {
		return u, nil
	}
	if err = decodeProperties(r, ctxUnsuback, &u.Properties); err != nil {
		return nil, err
	}
	for r.remaining() > 0 {
		b, _ := r.byte()
		code := ReasonCode(b)
		if !unsubackReasons[code] {
			return nil, protocolError("UNSUBACK reason code %s is not valid", code)
		}
		u.ReasonCodes = append(u.ReasonCodes, code)
	}
	if len(u.ReasonCodes) == 0 {
		return nil, protocolError("UNSUBACK with no reason codes")
	}
	return u, nil
}

func decodeDisconnect(v Version, r *reader) (*Disconnect, *Error) {
	d := &Disconnect{}
	if v != V5 || r.remaining() == 0 {
		return d, nil
	}
	b, _ := r.byte()
	d.ReasonCode = ReasonCode(b)
	if !disconnectReasons[d.ReasonCode] {
		return nil, protocolError("DISCONNECT reason code %s is not valid", d.ReasonCode)
	}
	if r.remaining() > 0 {
		if err := decodeProperties(r, ctxDisconnect, &d.Properties); err != nil {
			return nil, err
		}
	}
	return d, nil
}

func decodeAuth(r *reader) (*Auth, *Error) {
	a := &Auth{}
	if r.remaining() == 0 {
		return a, nil
	}
	b, _ := r.byte()
	a.ReasonCode = ReasonCode(b)
	if !authReasons[a.ReasonCode] {
		return nil, protocolError("AUTH reason code %s is not valid", a.ReasonCode)
	}
	if r.remaining() > 0 {
		if err := decodeProperties(r, ctxAuth, &a.Properties); err != nil {
			return nil, err
		}
	}
	return a, nil
}
