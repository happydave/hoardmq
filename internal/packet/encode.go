package packet

import (
	"fmt"
	"io"
)

// Encoder writes packets in a connection's grammar.
type Encoder struct {
	// Version is the connection's protocol level.
	Version Version
	// MaxPacketSize is the peer's Maximum Packet Size (header included);
	// zero means the wire maximum. A packet that would exceed it is not
	// written and Encode returns an *Error with Reason PacketTooLarge so the
	// caller can apply its own policy.
	MaxPacketSize uint32
}

// Encode writes one packet in a single Write.
func (e *Encoder) Encode(w io.Writer, p Packet) error {
	buf, class := acquire(256)
	defer release(buf, class)
	out, err := e.appendPacket(buf[:0], p)
	if err != nil {
		return err
	}
	_, werr := w.Write(out)
	return werr
}

// Bytes returns the packet's encoding.
func (e *Encoder) Bytes(p Packet) ([]byte, error) {
	return e.appendPacket(nil, p)
}

func (e *Encoder) appendPacket(dst []byte, p Packet) ([]byte, error) {
	if e.Version == 0 {
		return nil, fmt.Errorf("packet: encoder version not set")
	}
	v := e.Version
	body := writer{buf: dst[:0]}
	// Reserve the largest header, then slide the body back over the unused bytes.
	body.buf = append(body.buf, 0, 0, 0, 0, 0)
	var flags byte
	var err *Error
	switch x := p.(type) {
	case *Connect:
		err = encodeConnect(&body, x)
	case *Connack:
		encodeConnack(&body, v, x)
	case *Publish:
		flags = publishFlags(x)
		err = encodePublish(&body, v, x)
	case *Puback:
		encodeAck(&body, v, &x.Ack)
	case *Pubrec:
		encodeAck(&body, v, &x.Ack)
	case *Pubrel:
		flags = 0x02
		encodeAck(&body, v, &x.Ack)
	case *Pubcomp:
		encodeAck(&body, v, &x.Ack)
	case *Subscribe:
		flags = 0x02
		encodeSubscribe(&body, v, x)
	case *Suback:
		encodeSuback(&body, v, x)
	case *Unsubscribe:
		flags = 0x02
		encodeUnsubscribe(&body, v, x)
	case *Unsuback:
		encodeUnsuback(&body, v, x)
	case *Pingreq, *Pingresp:
	case *Disconnect:
		encodeDisconnect(&body, v, x)
	case *Auth:
		if v != V5 {
			return nil, malformed("AUTH is not valid at %s", v)
		}
		encodeAuth(&body, x)
	default:
		return nil, fmt.Errorf("packet: cannot encode %T", p)
	}
	if err != nil {
		return nil, err
	}
	length := len(body.buf) - 5
	if length > MaxRemainingLength {
		return nil, &Error{Reason: PacketTooLarge, Message: fmt.Sprintf("body of %d bytes exceeds the wire maximum", length)}
	}
	hdrLen := 1 + varintLen(uint32(length))
	if e.MaxPacketSize != 0 && uint32(length+hdrLen) > e.MaxPacketSize {
		return nil, &Error{Reason: PacketTooLarge, Message: fmt.Sprintf("packet of %d bytes exceeds the peer's maximum of %d", length+hdrLen, e.MaxPacketSize)}
	}
	start := 5 - hdrLen
	hdr := writer{buf: body.buf[start:start]}
	hdr.byte(byte(p.Type())<<4 | flags)
	hdr.varint(uint32(length))
	return body.buf[start:], nil
}

// flags returns the fixed-header flag nibble for a PUBLISH.
func publishFlags(p *Publish) byte {
	var f byte
	if p.Dup {
		f |= 0x08
	}
	f |= (p.QoS & 0x03) << 1
	if p.Retain {
		f |= 0x01
	}
	return f
}

// EncodeUnsupportedVersion writes the CONNACK that refuses a client whose
// protocol level the server does not speak, in the client's own framing:
// 5.0 framing with Unsupported Protocol Version for level 5 and above, 3.x
// framing with return code 1 below that.
func EncodeUnsupportedVersion(w io.Writer, level byte) error {
	var out []byte
	if level >= 5 {
		out = []byte{byte(CONNACK) << 4, 3, 0, byte(UnsupportedProtocolVersion), 0}
	} else {
		out = []byte{byte(CONNACK) << 4, 2, 0, 1}
	}
	_, err := w.Write(out)
	return err
}

func encodeConnect(w *writer, c *Connect) *Error {
	v := c.ProtocolLevel
	switch v {
	case V3:
		w.string("MQIsdp")
	case V4, V5:
		w.string("MQTT")
	default:
		return &Error{Reason: UnsupportedProtocolVersion, Message: fmt.Sprintf("cannot encode CONNECT at level %d", byte(v)), Level: byte(v)}
	}
	w.byte(byte(v))
	var flags byte
	if c.CleanStart {
		flags |= 0x02
	}
	if c.Will != nil {
		flags |= 0x04 | (c.Will.QoS&0x03)<<3
		if c.Will.Retain {
			flags |= 0x20
		}
	}
	if c.HasPassword {
		flags |= 0x40
	}
	if c.HasUsername {
		flags |= 0x80
	}
	w.byte(flags)
	w.uint16(c.KeepAlive)
	if v == V5 {
		encodeProperties(w, ctxConnect, &c.Properties)
	}
	w.string(c.ClientID)
	if c.Will != nil {
		if v == V5 {
			encodeProperties(w, ctxWill, &c.Will.Properties)
		}
		w.string(c.Will.Topic)
		w.bytes(c.Will.Payload)
	}
	if c.HasUsername {
		w.string(c.Username)
	}
	if c.HasPassword {
		w.bytes(c.Password)
	}
	return nil
}

func encodeConnack(w *writer, v Version, c *Connack) {
	w.byte(boolByte(c.SessionPresent))
	if v == V5 {
		w.byte(byte(c.ReasonCode))
		encodeProperties(w, ctxConnack, &c.Properties)
		return
	}
	w.byte(connackReturnCode(c.ReasonCode))
}

func encodePublish(w *writer, v Version, p *Publish) *Error {
	if p.QoS > 2 {
		return malformed("PUBLISH QoS %d", p.QoS)
	}
	w.string(p.Topic)
	if p.QoS > 0 {
		w.uint16(p.PacketID)
	}
	if v == V5 {
		encodeProperties(w, ctxPublish, &p.Properties)
	}
	w.buf = append(w.buf, p.Payload...)
	return nil
}

func encodeAck(w *writer, v Version, a *Ack) {
	w.uint16(a.PacketID)
	if v != V5 {
		return
	}
	if a.ReasonCode == Success && propertiesEmpty(&a.Properties) {
		return
	}
	w.byte(byte(a.ReasonCode))
	if !propertiesEmpty(&a.Properties) {
		encodeProperties(w, ctxPubAck, &a.Properties)
	}
}

func encodeSubscribe(w *writer, v Version, s *Subscribe) {
	w.uint16(s.PacketID)
	if v == V5 {
		encodeProperties(w, ctxSubscribe, &s.Properties)
	}
	for _, sub := range s.Subscriptions {
		w.string(sub.Filter)
		opts := sub.QoS & 0x03
		if v == V5 {
			if sub.NoLocal {
				opts |= 0x04
			}
			if sub.RetainAsPublished {
				opts |= 0x08
			}
			opts |= (sub.RetainHandling & 0x03) << 4
		}
		w.byte(opts)
	}
}

func encodeSuback(w *writer, v Version, s *Suback) {
	w.uint16(s.PacketID)
	if v == V5 {
		encodeProperties(w, ctxSuback, &s.Properties)
	}
	for _, code := range s.ReasonCodes {
		if v != V5 && code.IsError() {
			code = 0x80
		}
		w.byte(byte(code))
	}
}

func encodeUnsubscribe(w *writer, v Version, u *Unsubscribe) {
	w.uint16(u.PacketID)
	if v == V5 {
		encodeProperties(w, ctxUnsubscribe, &u.Properties)
	}
	for _, f := range u.Filters {
		w.string(f)
	}
}

func encodeUnsuback(w *writer, v Version, u *Unsuback) {
	w.uint16(u.PacketID)
	if v != V5 {
		return
	}
	encodeProperties(w, ctxUnsuback, &u.Properties)
	for _, code := range u.ReasonCodes {
		w.byte(byte(code))
	}
}

func encodeDisconnect(w *writer, v Version, d *Disconnect) {
	if v != V5 {
		return
	}
	if d.ReasonCode == NormalDisconnection && propertiesEmpty(&d.Properties) {
		return
	}
	w.byte(byte(d.ReasonCode))
	if !propertiesEmpty(&d.Properties) {
		encodeProperties(w, ctxDisconnect, &d.Properties)
	}
}

func encodeAuth(w *writer, a *Auth) {
	if a.ReasonCode == Success && propertiesEmpty(&a.Properties) {
		return
	}
	w.byte(byte(a.ReasonCode))
	if !propertiesEmpty(&a.Properties) {
		encodeProperties(w, ctxAuth, &a.Properties)
	}
}
