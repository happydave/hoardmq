package replication

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/store"
)

// benchFollower is a follower over a replica store in a temporary directory,
// with no transport, map or metrics: what applying a record costs on its own.
func benchFollower(b *testing.B) *follower {
	b.Helper()
	rs, err := store.Open(b.TempDir(), store.Options{Logger: quiet})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = rs.Close() })
	return &follower{r: &Replicator{o: Options{Replica: rs}}, state: store.NewState()}
}

func upsertFor(id string) store.Record {
	return store.RecordsFor(&store.SessionState{SessionMeta: store.SessionMeta{Namespace: "ns", ClientID: id, Epoch: 1, ExpiryInterval: 3600}})[0]
}

// BenchmarkFollowerApply is a worker's apply of a session record, the lock
// trace included: one goroutine, and one per GOMAXPROCS contending for the
// follower's lock as a node's per-peer workers and fetch pool do. hold-ns/op is
// the mean hold of the lock the trace recorded.
func BenchmarkFollowerApply(b *testing.B) {
	b.Run("serial", func(b *testing.B) {
		f := benchFollower(b)
		rec := upsertFor("c")
		b.ResetTimer()
		for range b.N {
			if _, err := f.applyChecked(rec, nil, holdApply, "", "", false); err != nil {
				b.Fatal(err)
			}
		}
		reportHold(b, f)
	})
	b.Run("parallel", func(b *testing.B) {
		f := benchFollower(b)
		var next atomic.Int64
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			rec := upsertFor(fmt.Sprintf("c%d", next.Add(1)))
			for pb.Next() {
				if _, err := f.applyChecked(rec, nil, holdApply, "", "", false); err != nil {
					b.Error(err)
					return
				}
			}
		})
		reportHold(b, f)
	})
}

func reportHold(b *testing.B, f *follower) {
	if n := f.lockHolds.count[holdApply].Load(); n > 0 {
		b.ReportMetric(float64(f.lockHolds.total[holdApply].Load())/float64(n), "hold-ns/op")
	}
}

// BenchmarkLockTraceApply is the trace's part of one apply: the wait recorded
// inside the hold and the hold recorded after it.
func BenchmarkLockTraceApply(b *testing.B) {
	var c lockHoldCounters
	for range b.N {
		began := time.Now()
		c.waited(holdApply, 3*time.Microsecond)
		c.held(holdApply, began)
	}
}
