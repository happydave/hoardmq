package session

import (
	"io"
	"log/slog"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/packet"
)

// BenchmarkDeliverToOnlineSubscriber times one forwarded message delivered to
// one online subscriber on a node that forwards, the path that remembers what
// a session was sent. The clock does not move, so from the cap's worth of
// messages on every delivery also drops the oldest identity: the list's worst
// case.
func BenchmarkDeliverToOnlineSubscriber(b *testing.B) {
	for _, qos := range []byte{0, 1} {
		b.Run("qos="+strconv.Itoa(int(qos)), func(b *testing.B) {
			m := New(Options{MaxQueued: 100, Authorizers: auth.AuthorizerChain{allow{}}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Clock: newFakeClock(), Forwarder: refusingForwarder{}})
			x := newFake("x", packet.V5, 1<<30)
			connectV5(m, x, true, 600, nil)
			subscribe(m, x, qos, "t/x")
			msg := &message.Message{Topic: "t/x", QoS: qos, Payload: []byte("a payload of some tens of bytes"), Publisher: message.Publisher{ClientID: "pub", Node: "n2"}, Arrived: time.Unix(1_700_000_000, 0)}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				msg.Seq = uint64(i + 1)
				_, _, _ = m.InjectForwarded(msg)
				if qos == 1 {
					b.StopTimer()
					x.mu.Lock()
					p := x.sent[len(x.sent)-1].(*packet.Publish)
					x.sent = x.sent[:0]
					x.mu.Unlock()
					m.Puback(x, p.PacketID, packet.Success)
					b.StartTimer()
				} else if i%1024 == 0 {
					x.mu.Lock()
					x.sent = x.sent[:0]
					x.mu.Unlock()
				}
			}
		})
	}
}

// TestSentListMemory reports what the list costs a session, from its
// capacity: one message sent, the cap's worth, and a long run past it.
func TestSentListMemory(t *testing.T) {
	entry := int(reflect.TypeOf(sentMessage{}).Size())
	for _, per := range []int{1, 16, sentMax, 1000} {
		m := New(Options{MaxQueued: 100, Authorizers: auth.AuthorizerChain{allow{}}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Clock: newFakeClock(), Forwarder: refusingForwarder{}})
		c := newFake("x", packet.V5, 1<<30)
		connectV5(m, c, true, 600, nil)
		subscribe(m, c, 0, "t/x")
		for j := 0; j < per+1; j++ {
			_, _, _ = m.InjectForwarded(&message.Message{Topic: "t/x", Payload: []byte("p"), Publisher: message.Publisher{ClientID: "pub", Node: "n2"}, Seq: uint64(j + 1), Arrived: time.Unix(1_700_000_000, 0)})
		}
		s := m.lookup(c)
		s.mu.Lock()
		n, capacity := s.sent.held(), cap(s.sent.e)
		s.mu.Unlock()
		t.Logf("sent %d: %d entries held, capacity %d, %d bytes a session (%d an entry)", per+1, n, capacity, capacity*entry, entry)
	}
}
