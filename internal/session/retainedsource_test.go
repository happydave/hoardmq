package session

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

// sourceReplication records the source each retained write is stored from,
// and stores nothing locally, as a node that leads none of the shards.
type sourceReplication struct {
	mu      sync.Mutex
	sources map[string]string // topic → source
}

func (r *sourceReplication) Barrier(context.Context, []store.Scope, []message.ForwardRef) error {
	return nil
}
func (r *sourceReplication) Durable(context.Context, []store.Scope) error { return nil }
func (r *sourceReplication) StoreRetained(*message.Message) bool {
	panic("StoreRetained called on a Replication that takes the source")
}
func (r *sourceReplication) StoreRetainedFrom(m *message.Message, source string) bool {
	r.mu.Lock()
	r.sources[m.Topic] = source
	r.mu.Unlock()
	return false
}
func (r *sourceReplication) QueryRetained(string, string, time.Time) []*message.Message {
	return nil
}

// TestRetainedWriteNamesItsSource: a client's QoS 1 and QoS 0 publishes, a
// Will and a module's publish are each stored with their own source.
func TestRetainedWriteNamesItsSource(t *testing.T) {
	m, _ := newManager(t, nil)
	r := &sourceReplication{sources: map[string]string{}}
	m.SetReplication(r)
	c := newFake("c", packet.V5, 100)
	connectV5(m, c, true, 0, &packet.Will{Topic: "will", Payload: []byte("gone"), QoS: 1, Retain: true})
	m.Publish(c, &packet.Publish{Topic: "q1", QoS: 1, PacketID: 1, Retain: true, Payload: []byte("a")})
	m.Publish(c, &packet.Publish{Topic: "q0", Retain: true, Payload: []byte("a")})
	m.Inject(&message.Message{Topic: "mod", Retain: true, Payload: []byte("a")})
	m.Disconnect(c, false, packet.KeepAliveTimeout, true)
	r.mu.Lock()
	defer r.mu.Unlock()
	for topic, want := range map[string]string{"q1": sourceClient, "q0": sourceClientQoS0, "will": sourceWill, "mod": sourceModule} {
		if got := r.sources[topic]; got != want {
			t.Errorf("%s stored from %q, want %q", topic, got, want)
		}
	}
}
