package session

import (
	"io"
	"log/slog"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

// storeCounts is what a timed section cost the store.
type storeCounts struct{ appends, syncs, callers int64 }

func countsOf(st *store.Store) storeCounts {
	a, s := st.AppendStats(), st.SyncStats()
	return storeCounts{a.Appends, s.Syncs, s.Callers}
}

func (c storeCounts) since(before storeCounts) storeCounts {
	return storeCounts{c.appends - before.appends, c.syncs - before.syncs, c.callers - before.callers}
}

func benchManager(b *testing.B) (*Manager, *store.Store) {
	b.Helper()
	st, err := store.Open(b.TempDir(), store.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = st.Close() })
	m := New(Options{MaxQueued: 1000, Authorizers: auth.AuthorizerChain{allow{}}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Clock: newFakeClock(), Journal: st})
	return m, st
}

// BenchmarkReconnectDrainsQueuedQoS0 times a durable session's reconnect
// (Connect and Resume, which drains the queue and syncs) with k QoS 0
// messages queued while it was offline, over a real store with its fsync
// (WI 1831). It reports the store's appends and fsyncs in the timed part.
func BenchmarkReconnectDrainsQueuedQoS0(b *testing.B) {
	for _, k := range []int{1, 10, 100} {
		b.Run("queued="+strconv.Itoa(k), func(b *testing.B) {
			m, st := benchManager(b)
			pub := newFake("pub", packet.V5, 100)
			connectV5(m, pub, true, 0, nil)
			sub := newFake("sub", packet.V5, 100)
			connectV5(m, sub, true, 3600, nil)
			subscribe(m, sub, 0, "t")
			var cost storeCounts
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				m.Disconnect(sub, false, packet.NormalDisconnection, false)
				for j := 0; j < k; j++ {
					publish(m, pub, "t", 0, 0, "a payload of some tens of bytes")
				}
				sub = newFake("sub", packet.V5, 100)
				before := countsOf(st)
				b.StartTimer()
				connectV5(m, sub, false, 3600, nil)
				b.StopTimer()
				c := countsOf(st).since(before)
				cost.appends, cost.syncs = cost.appends+c.appends, cost.syncs+c.syncs
				if n := len(sub.publishes()); n != k {
					b.Fatalf("the reconnect sent %d messages, want %d", n, k)
				}
				b.StartTimer()
			}
			b.ReportMetric(float64(cost.appends)/float64(b.N), "appends/op")
			b.ReportMetric(float64(cost.syncs)/float64(b.N), "fsyncs/op")
		})
	}
}

// BenchmarkReconnectStormQueuedQoS0 reconnects 10,000 durable sessions, each
// with one QoS 0 message queued, from 1,000 goroutines at once: the drain's
// records under a reconnect storm's concurrency, where the group commit
// shares fsyncs (WI 1831). One op is the whole storm.
func BenchmarkReconnectStormQueuedQoS0(b *testing.B) {
	const sessions, workers = 10000, 1000
	m, st := benchManager(b)
	pub := newFake("pub", packet.V5, 100)
	connectV5(m, pub, true, 0, nil)
	subs := make([]*fakeConn, sessions)
	for i := range subs {
		id := "client-" + strconv.Itoa(i)
		subs[i] = newFake(id, packet.V5, 100)
		connectV5(m, subs[i], true, 3600, nil)
		subscribe(m, subs[i], 0, "devices/"+id)
	}
	var cost storeCounts
	var wall time.Duration
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		for j, c := range subs {
			m.Disconnect(c, false, packet.NormalDisconnection, false)
			publish(m, pub, "devices/client-"+strconv.Itoa(j), 0, 0, "a payload of some tens of bytes")
			subs[j] = newFake(c.id, packet.V5, 100)
		}
		before := countsOf(st)
		next := make(chan int, sessions)
		for j := range subs {
			next <- j
		}
		close(next)
		var wg sync.WaitGroup
		began := time.Now()
		b.StartTimer()
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := range next {
					connectV5(m, subs[j], false, 3600, nil)
				}
			}()
		}
		wg.Wait()
		b.StopTimer()
		wall += time.Since(began)
		c := countsOf(st).since(before)
		cost.appends, cost.syncs, cost.callers = cost.appends+c.appends, cost.syncs+c.syncs, cost.callers+c.callers
		for _, c := range subs {
			if len(c.publishes()) != 1 {
				b.Fatalf("%s was sent %d messages on its reconnect, want 1", c.id, len(c.publishes()))
			}
		}
		b.StartTimer()
	}
	n := float64(b.N)
	b.ReportMetric(float64(cost.appends)/n/sessions, "appends/reconnect")
	b.ReportMetric(float64(cost.syncs)/n, "fsyncs/storm")
	b.ReportMetric(float64(cost.callers)/n/sessions, "sync-callers/reconnect")
	b.ReportMetric(wall.Seconds()/n, "storm-s")
}
