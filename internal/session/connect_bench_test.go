package session

import (
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

// tapModel stands in for the replication tap's cost inside the store's
// critical section: a lookup against a shared table (the lease table) and a
// push onto a per-peer queue. It is a model of the shape, not the replicator
// itself — the point is what the store lock is held across, not what the
// replicator does with the record.
type tapModel struct {
	mu     sync.RWMutex
	leases map[string]uint64
	outMu  sync.Mutex
	out    []store.Record
	calls  atomic.Int64
}

func newTapModel() *tapModel {
	return &tapModel{leases: map[string]uint64{"warm": 1}}
}

// tap models the replication tap as it is now: the epoch rides on the record,
// so there is no lease-table read — only the push onto a per-peer queue.
func (t *tapModel) tap(rec store.Record) func() {
	t.calls.Add(1)
	_ = rec.Key.Epoch
	t.outMu.Lock()
	t.out = append(t.out, rec)
	if len(t.out) > 4096 {
		t.out = t.out[:0]
	}
	t.outMu.Unlock()
	return nil
}

// legacyTap is the pre-1292 shape: a lease-table read before the push.
func (t *tapModel) legacyTap(rec store.Record) func() {
	t.calls.Add(1)
	t.mu.RLock()
	_ = t.leases[rec.Key.ClientID]
	t.mu.RUnlock()
	t.outMu.Lock()
	t.out = append(t.out, rec)
	if len(t.out) > 4096 {
		t.out = t.out[:0]
	}
	t.outMu.Unlock()
	return nil
}

func benchStore(b *testing.B, tap func(store.Record) func()) *store.Store {
	b.Helper()
	st, err := store.Open(b.TempDir(), store.Options{
		SyncPolicy: store.Relaxed,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		Tap:        tap,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = st.Close() })
	return st
}

// BenchmarkConnectParallel measures the connect path under concurrency, which
// is what a reconnect storm after a node loss actually exercises. Manager.
// Connect holds the manager's single global mutex across the journal write,
// and the journal write takes the store's own global mutex across a WAL append
// and the tap — so the three variants below separate the cost of the lock
// itself from the cost of what is done while holding it.
//
// Run: go test ./internal/session -run '^$' -bench BenchmarkConnectParallel -benchtime 200000x -cpu 1,8,32
func BenchmarkConnectParallel(b *testing.B) {
	cases := []struct {
		name string
		open func(b *testing.B) Journal
	}{
		{"no-journal", func(*testing.B) Journal { return nil }},
		{"store", func(b *testing.B) Journal { return benchStore(b, nil) }},
		{"store+tap", func(b *testing.B) Journal { return benchStore(b, newTapModel().tap) }},
		// The tap used to read the lease table, which during a failover storm
		// is being hammered by takeovers and acquires (the 100,000-client run
		// saw 70,499 and 124,218 acquires against 33,333 clients a node). This
		// variant keeps a writer on that lock and a reader in the tap, which is
		// the shape WI 1292 removed: it is the measurement that justified
		// taking the lookup out, kept so the cost is on the record.
		{"legacy-tap-with-lease-read", func(b *testing.B) Journal {
			t := newTapModel()
			stop := make(chan struct{})
			go func() {
				for {
					select {
					case <-stop:
						return
					default:
					}
					// A writer on the lease table: a takeover or an acquire
					// applying. Go's RWMutex blocks new readers as soon as a
					// writer is waiting, so a steady stream of these starves
					// the tap's read.
					t.mu.Lock()
					time.Sleep(200 * time.Microsecond)
					t.mu.Unlock()
					time.Sleep(200 * time.Microsecond)
				}
			}()
			b.Cleanup(func() { close(stop) })
			return benchStore(b, t.legacyTap)
		}},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			j := c.open(b)
			opts := Options{
				MaxQueued:   10,
				Authorizers: auth.AuthorizerChain{allow{}},
				Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
			}
			if j != nil {
				opts.Journal = j
			}
			m := New(opts)
			b.Cleanup(m.Close)
			var n atomic.Uint64
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					id := fmt.Sprintf("c-%d", n.Add(1))
					fc := newFake(id, packet.V5, 16)
					expiry := uint32(3600)
					pkt := &packet.Connect{ProtocolLevel: packet.V5, ClientID: id}
					pkt.Properties.SessionExpiryInterval = &expiry
					m.Connect(fc, pkt, 0)
				}
			})
		})
	}
}
