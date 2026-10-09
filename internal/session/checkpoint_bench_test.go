package session

import (
	"io"
	"log/slog"
	"strconv"
	"testing"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/packet"
)

// BenchmarkCheckpointState times one checkpoint snapshot of n offline
// persistent sessions, each with a subscription and a queued QoS 1 message:
// the work a checkpoint does while it reads the sessions (WI 1818).
func BenchmarkCheckpointState(b *testing.B) {
	for _, n := range []int{10000, 100000} {
		b.Run("sessions="+strconv.Itoa(n), func(b *testing.B) {
			m := New(Options{MaxQueued: 100, Authorizers: auth.AuthorizerChain{allow{}}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Clock: newFakeClock()})
			pub := newFake("pub", packet.V5, 100)
			connectV5(m, pub, true, 0, nil)
			for i := 0; i < n; i++ {
				id := "client-" + strconv.Itoa(i)
				c := newFake(id, packet.V5, 100)
				connectV5(m, c, true, 3600, nil)
				subscribe(m, c, 1, "devices/"+id+"/state")
				m.Disconnect(c, false, packet.NormalDisconnection, false)
				publish(m, pub, "devices/"+id+"/state", 1, 1, "a payload of some tens of bytes")
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if st := m.CheckpointState(func() uint64 { return 0 }); len(st.Sessions) != n {
					b.Fatalf("snapshot holds %d sessions, want %d", len(st.Sessions), n)
				}
			}
		})
	}
}
