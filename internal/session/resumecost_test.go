package session

import (
	"io"
	"log/slog"
	"strconv"
	"testing"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/packet"
)

// costManager is newManager for a benchmark, with no journal.
func costManager() *Manager {
	return New(Options{MaxQueued: 10, Authorizers: auth.AuthorizerChain{allow{}}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Clock: newFakeClock()})
}

// BenchmarkSealedResume times a CONNECT that resumes a sealed copy holding ten
// subscriptions; the copy's making and the session's eviction after are
// outside the timer.
func BenchmarkSealedResume(b *testing.B) {
	m := costManager()
	ns := m.opts.Namespace
	filters := make([]string, 10)
	for i := range filters {
		filters[i] = "t/" + strconv.Itoa(i)
	}
	b.ReportAllocs()
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		c := newFake("c", packet.V5, 100)
		connectAt(m, c, true, 600, 5)
		subscribe(m, c, 1, filters...)
		m.Disconnect(c, false, packet.NormalDisconnection, false)
		if res := m.Seal(ns, "c", packet.SessionTakenOver, 9); res.Outcome != SealSealed {
			b.Fatalf("seal: %+v", res)
		}
		b.StartTimer()
		res := connectAt(m, newFake("c", packet.V5, 100), false, 600, 12)
		b.StopTimer()
		if !res.SessionPresent {
			b.Fatalf("the copy was not resumed: %+v", res)
		}
		m.Evict(ns, "c", packet.SessionTakenOver)
	}
}

// BenchmarkSubscribe times one SUBSCRIBE of one filter on a connected
// session, cycling through 1,000 filters.
func BenchmarkSubscribe(b *testing.B) {
	m := costManager()
	c := newFake("c", packet.V5, 100)
	connectAt(m, c, true, 600, 5)
	filters := make([]string, 1000)
	for i := range filters {
		filters[i] = "t/" + strconv.Itoa(i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		subscribe(m, c, 1, filters[i%len(filters)])
	}
}
