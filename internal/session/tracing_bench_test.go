package session

import (
	"io"
	"log/slog"
	"testing"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/otel"
	"github.com/happydave/hoardmq/internal/packet"
)

// BenchmarkPublishTracing is a QoS 1 publish to four subscribers with tracing
// off, on but not sampling, and sampling every publish.
func BenchmarkPublishTracing(b *testing.B) {
	for _, arm := range []struct {
		name string
		tr   *otel.Tracer
	}{
		{"off", nil},
		{"unsampled", otel.New(otel.Config{Ratio: 0, Node: "n1", Sink: func(otel.SpanData) {}})},
		{"sampled", otel.New(otel.Config{Ratio: 1, Node: "n1", Sink: func(otel.SpanData) {}})},
	} {
		b.Run(arm.name, func(b *testing.B) {
			m := New(Options{MaxQueued: 10, Authorizers: auth.AuthorizerChain{allow{}}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Clock: newFakeClock(), Tracer: arm.tr})
			pub := newFake("pub", packet.V5, 100)
			connectV5(m, pub, true, 0, nil)
			subs := make([]*fakeConn, 4)
			for i := range subs {
				subs[i] = newFake("sub"+string(rune('a'+i)), packet.V5, 100)
				connectV5(m, subs[i], true, 0, nil)
				subscribe(m, subs[i], 0, "t/x")
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				m.Publish(pub, &packet.Publish{Topic: "t/x", QoS: 1, PacketID: uint16(i%65535 + 1), Payload: []byte("x")})
				if i%1024 == 0 {
					b.StopTimer()
					for _, f := range append(subs, pub) {
						f.mu.Lock()
						f.sent = f.sent[:0]
						f.mu.Unlock()
					}
					b.StartTimer()
				}
			}
		})
	}
}
