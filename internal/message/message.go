// Package message defines the envelope a published message travels in: what
// the dispatcher routes, what the log stores and what the cluster transport
// carries. Forwarding to a level 3 or 4 subscriber strips the properties at
// encode time only; the envelope always holds them.
package message

import (
	"time"

	"github.com/happydave/hoardmq/ext/mqtt"
	"github.com/happydave/hoardmq/internal/packet"
)

// The envelope and its parts are the extension API's.
type (
	Publisher  = mqtt.Publisher
	Message    = mqtt.Message
	ForwardRef = mqtt.ForwardRef
	ID         = mqtt.ID
	Delivery   = mqtt.Delivery
)

// FromPublish builds the envelope from an inbound PUBLISH at arrival time
// (mqtt.FromPublish).
func FromPublish(p *packet.Publish, pub Publisher, seq uint64, now time.Time) *Message {
	return mqtt.FromPublish(p, pub, seq, now)
}
