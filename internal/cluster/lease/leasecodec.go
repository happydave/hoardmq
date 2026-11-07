package lease

import (
	"encoding/binary"
	"errors"
	"time"

	"github.com/happydave/hoardmq/ext/mqtt"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

// The compact lease record of a snapshot section: no field names, nothing
// base64, nothing absent written. A pointer, slice or time that is nil or
// zero is marked absent in a flag word, so a record reads back to the value
// the JSON form gives, its nils and time zones included.

const (
	lOnline uint64 = 1 << iota
	lWill
	lExpiresAt
	lDisconnectedAt
	lPrincipal
)

const (
	wArmed uint64 = 1 << iota
	wRetain
	wPayloadFormat
	wContentType
	wResponseTopic
	wCorrelation
	wUserProps
	wExpiry
	wArrived
	wPayload
	wNoMessage
)

var errCompactRecord = errors.New("lease record: truncated or malformed")

type enc struct{ b []byte }

func (e *enc) uv(v uint64)    { e.b = binary.AppendUvarint(e.b, v) }
func (e *enc) str(s string)   { e.uv(uint64(len(s))); e.b = append(e.b, s...) }
func (e *enc) bytes(p []byte) { e.uv(uint64(len(p))); e.b = append(e.b, p...) }

// tm writes an instant and its zone offset, as JSON keeps them.
func (e *enc) tm(t time.Time) {
	e.b = binary.AppendVarint(e.b, t.UnixNano())
	_, off := t.Zone()
	e.b = binary.AppendVarint(e.b, int64(off))
}

func flag(on bool, f uint64) uint64 {
	if on {
		return f
	}
	return 0
}

// encodeCompactLease writes one lease.
func encodeCompactLease(l *Lease) []byte {
	e := &enc{b: make([]byte, 0, 64)}
	flags := flag(l.Online, lOnline) | flag(l.Will != nil, lWill) | flag(!l.ExpiresAt.IsZero(), lExpiresAt) | flag(!l.DisconnectedAt.IsZero(), lDisconnectedAt) |
		flag(l.Principal != 0, lPrincipal)
	e.uv(flags)
	e.str(l.Namespace)
	e.str(l.ClientID)
	e.str(l.Owner)
	e.uv(l.Epoch)
	e.uv(uint64(l.ExpiryInterval))
	if flags&lExpiresAt != 0 {
		e.tm(l.ExpiresAt)
	}
	if flags&lDisconnectedAt != 0 {
		e.tm(l.DisconnectedAt)
	}
	if l.Will != nil {
		encodeWill(e, l.Will)
	}
	if l.Principal != 0 {
		e.b = binary.BigEndian.AppendUint64(e.b, l.Principal)
	}
	return e.b
}

func encodeWill(e *enc, w *store.WillRecord) {
	m := w.Message
	if m == nil {
		e.uv(flag(w.Armed, wArmed) | wNoMessage)
		e.uv(uint64(w.Delay))
		return
	}
	flags := flag(w.Armed, wArmed) | flag(m.Retain, wRetain) | flag(m.PayloadFormat != nil, wPayloadFormat) |
		flag(m.ContentType != nil, wContentType) | flag(m.ResponseTopic != nil, wResponseTopic) |
		flag(m.CorrelationData != nil, wCorrelation) | flag(m.UserProperties != nil, wUserProps) |
		flag(!m.Expiry.IsZero(), wExpiry) | flag(!m.Arrived.IsZero(), wArrived) | flag(m.Payload != nil, wPayload)
	e.uv(flags)
	e.uv(uint64(w.Delay))
	e.str(m.Namespace)
	e.str(m.Topic)
	e.bytes(m.Payload)
	e.b = append(e.b, m.QoS)
	if m.PayloadFormat != nil {
		e.b = append(e.b, *m.PayloadFormat)
	}
	if m.ContentType != nil {
		e.str(*m.ContentType)
	}
	if m.ResponseTopic != nil {
		e.str(*m.ResponseTopic)
	}
	if m.CorrelationData != nil {
		e.bytes(m.CorrelationData)
	}
	if m.UserProperties != nil {
		e.uv(uint64(len(m.UserProperties)))
		for _, p := range m.UserProperties {
			e.str(p.Name)
			e.str(p.Value)
		}
	}
	if !m.Expiry.IsZero() {
		e.tm(m.Expiry)
	}
	e.str(m.Publisher.Namespace)
	e.str(m.Publisher.ClientID)
	e.str(m.Publisher.Username)
	e.str(m.Publisher.Node)
	e.uv(m.Seq)
	if !m.Arrived.IsZero() {
		e.tm(m.Arrived)
	}
	e.str(m.OriginCluster)
	e.b = append(e.b, m.Hops)
}

type dec struct {
	b   []byte
	err error
}

func (d *dec) uv() uint64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Uvarint(d.b)
	if n <= 0 {
		d.err = errCompactRecord
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *dec) sv() int64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Varint(d.b)
	if n <= 0 {
		d.err = errCompactRecord
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *dec) raw(n uint64) []byte {
	if d.err != nil {
		return nil
	}
	if n > uint64(len(d.b)) {
		d.err = errCompactRecord
		return nil
	}
	out := d.b[:n:n]
	d.b = d.b[n:]
	return out
}

func (d *dec) str() string   { return string(d.raw(d.uv())) }
func (d *dec) bytes() []byte { return append([]byte{}, d.raw(d.uv())...) }
func (d *dec) byte1() byte {
	if p := d.raw(1); len(p) == 1 {
		return p[0]
	}
	return 0
}

// tm reads an instant in the zone JSON would give it: UTC at offset zero,
// otherwise a fixed zone with no name.
func (d *dec) tm() time.Time {
	ns, off := d.sv(), d.sv()
	t := time.Unix(0, ns)
	if off == 0 {
		return t.UTC()
	}
	return t.In(time.FixedZone("", int(off)))
}

// decodeCompactLease reads one lease.
func decodeCompactLease(b []byte) (*Lease, error) {
	d := &dec{b: b}
	flags := d.uv()
	l := &Lease{Online: flags&lOnline != 0}
	l.Namespace = d.str()
	l.ClientID = d.str()
	l.Owner = d.str()
	l.Epoch = d.uv()
	l.ExpiryInterval = uint32(d.uv())
	if flags&lExpiresAt != 0 {
		l.ExpiresAt = d.tm()
	}
	if flags&lDisconnectedAt != 0 {
		l.DisconnectedAt = d.tm()
	}
	if flags&lWill != 0 {
		l.Will = decodeWill(d)
	}
	if flags&lPrincipal != 0 {
		if p := d.raw(8); len(p) == 8 {
			l.Principal = binary.BigEndian.Uint64(p)
		}
	}
	if d.err == nil && len(d.b) != 0 {
		d.err = errCompactRecord
	}
	if d.err != nil {
		return nil, d.err
	}
	return l, nil
}

func decodeWill(d *dec) *store.WillRecord {
	flags := d.uv()
	w := &store.WillRecord{Armed: flags&wArmed != 0, Delay: uint32(d.uv())}
	if flags&wNoMessage != 0 {
		return w
	}
	m := &message.Message{Retain: flags&wRetain != 0}
	m.Namespace = d.str()
	m.Topic = d.str()
	payload := d.bytes()
	if flags&wPayload != 0 {
		m.Payload = payload
	}
	m.QoS = d.byte1()
	if flags&wPayloadFormat != 0 {
		v := d.byte1()
		m.PayloadFormat = &v
	}
	if flags&wContentType != 0 {
		v := d.str()
		m.ContentType = &v
	}
	if flags&wResponseTopic != 0 {
		v := d.str()
		m.ResponseTopic = &v
	}
	if flags&wCorrelation != 0 {
		m.CorrelationData = d.bytes()
	}
	if flags&wUserProps != 0 {
		n := d.uv()
		if n > uint64(len(d.b)) {
			d.err = errCompactRecord
			return w
		}
		m.UserProperties = make([]mqtt.UserProperty, 0, n)
		for i := uint64(0); i < n && d.err == nil; i++ {
			m.UserProperties = append(m.UserProperties, mqtt.UserProperty{Name: d.str(), Value: d.str()})
		}
	}
	if flags&wExpiry != 0 {
		m.Expiry = d.tm()
	}
	m.Publisher.Namespace = d.str()
	m.Publisher.ClientID = d.str()
	m.Publisher.Username = d.str()
	m.Publisher.Node = d.str()
	m.Seq = d.uv()
	if flags&wArrived != 0 {
		m.Arrived = d.tm()
	}
	m.OriginCluster = d.str()
	m.Hops = d.byte1()
	w.Message = m
	return w
}
