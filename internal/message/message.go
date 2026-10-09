// Package message defines the envelope a published message travels in: what
// the dispatcher routes, what the log stores and what the cluster transport
// carries. Forwarding to a level 3 or 4 subscriber strips the properties at
// encode time only; the envelope always holds them.
package message

import (
	"math"
	"time"

	"github.com/happydave/hoardmq/internal/otel"
	"github.com/happydave/hoardmq/internal/packet"
)

// Publisher identifies who published a message and where.
type Publisher struct {
	Namespace string
	ClientID  string
	Username  string
	Node      string
}

// Message is the envelope.
type Message struct {
	Namespace string
	Topic     string
	Payload   []byte
	QoS       byte
	Retain    bool

	PayloadFormat   *byte
	ContentType     *string
	ResponseTopic   *string
	CorrelationData []byte
	UserProperties  []packet.UserProperty

	// Expiry is the absolute deadline from Message Expiry Interval, or zero
	// when the message does not expire.
	Expiry time.Time

	Publisher Publisher
	// Seq orders messages from one publisher connection.
	Seq     uint64
	Arrived time.Time
	// OriginCluster and Hops let cluster linking and bridges prevent loops.
	OriginCluster string
	Hops          uint8
	// Forwarded marks a message received from another node of this
	// cluster: it is delivered locally and never forwarded again. It does
	// not travel on the wire or into the store.
	Forwarded bool `json:"-"`
	// Forwards names the frames the routing plane sent for this message on
	// the node that published it, so the acknowledgement waits on those and
	// not on the peer's whole stream. It is written only on the publishing
	// goroutine, before the acknowledgement, and like Forwarded it does not
	// travel on the wire or into the store.
	Forwards []ForwardRef `json:"-"`
	// Trace is the span of this publish on the node routing it, sampled or
	// zero; the routing plane parents a forward's span on it. The span's
	// context crosses to another node in the forward's envelope, not here.
	Trace otel.SpanContext `json:"-"`
}

// ForwardRef is one forwarded frame: the peer it went to and the sequence it
// took on this node's stream to that peer.
type ForwardRef struct {
	Peer string
	Seq  uint64
}

// FromPublish builds the envelope from an inbound PUBLISH at arrival time.
func FromPublish(p *packet.Publish, pub Publisher, seq uint64, now time.Time) *Message {
	m := &Message{
		Namespace:       pub.Namespace,
		Topic:           p.Topic,
		Payload:         p.Payload,
		QoS:             p.QoS,
		Retain:          p.Retain,
		PayloadFormat:   p.Properties.PayloadFormatIndicator,
		ContentType:     p.Properties.ContentType,
		ResponseTopic:   p.Properties.ResponseTopic,
		CorrelationData: p.Properties.CorrelationData,
		UserProperties:  p.Properties.UserProperties,
		Publisher:       pub,
		Seq:             seq,
		Arrived:         now,
	}
	if e := p.Properties.MessageExpiryInterval; e != nil {
		m.Expiry = now.Add(time.Duration(*e) * time.Second)
	}
	return m
}

// ID identifies one publish across the cluster: the node it was published on,
// that node's sequence for it and its arrival time there. The sequence starts
// again when the node restarts; the arrival time tells the two apart.
type ID struct {
	Node    string `json:"node"`
	Seq     uint64 `json:"seq"`
	Arrived int64  `json:"arrived"` // Unix nanoseconds
}

// Identity returns the message's ID, and false when it has none: a message
// never sequenced by the node that took it in.
func (m *Message) Identity() (ID, bool) {
	if m.Seq == 0 || m.Arrived.IsZero() {
		return ID{}, false
	}
	return ID{Node: m.Publisher.Node, Seq: m.Seq, Arrived: m.Arrived.UnixNano()}, true
}

// Expired reports whether the message's deadline has passed.
func (m *Message) Expired(now time.Time) bool {
	return !m.Expiry.IsZero() && !now.Before(m.Expiry)
}

// Delivery describes how one subscriber receives the message.
type Delivery struct {
	QoS                     byte
	PacketID                uint16
	Dup                     bool
	Retain                  bool
	SubscriptionIdentifiers []uint32
}

// ToPublish builds the outbound PUBLISH for one subscriber. The remaining
// Message Expiry Interval is recomputed from the deadline; the caller checks
// Expired first, and the remainder is clamped to [1, math.MaxUint32] so that
// a deadline already passed cannot wrap the unsigned property.
func (m *Message) ToPublish(d Delivery, now time.Time) *packet.Publish {
	p := &packet.Publish{
		Dup:      d.Dup,
		QoS:      d.QoS,
		Retain:   d.Retain,
		Topic:    m.Topic,
		PacketID: d.PacketID,
		Payload:  m.Payload,
		Properties: packet.Properties{
			PayloadFormatIndicator:  m.PayloadFormat,
			ContentType:             m.ContentType,
			ResponseTopic:           m.ResponseTopic,
			CorrelationData:         m.CorrelationData,
			UserProperties:          m.UserProperties,
			SubscriptionIdentifiers: d.SubscriptionIdentifiers,
		},
	}
	if !m.Expiry.IsZero() {
		secs := uint32(1)
		switch remaining := math.Ceil(m.Expiry.Sub(now).Seconds()); {
		case remaining >= math.MaxUint32:
			secs = math.MaxUint32
		case remaining >= 1:
			secs = uint32(remaining)
		}
		p.Properties.MessageExpiryInterval = &secs
	}
	return p
}
