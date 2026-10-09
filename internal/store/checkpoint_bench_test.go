package store

import (
	"encoding/json"
	"strconv"
	"testing"
)

// BenchmarkCheckpointHold times what a checkpoint does under the store lock
// once its state is marshalled — the sync, the checkpoint file's write and
// fsync, and the truncation — for n sessions, each with its metadata, a
// subscription and a queued message. An append waits this long, and a
// follower's apply waits it holding the follower's lock (WI 1818).
func BenchmarkCheckpointHold(b *testing.B) {
	for _, n := range []int{10000, 100000} {
		b.Run("sessions="+strconv.Itoa(n), func(b *testing.B) {
			s, err := Open(b.TempDir(), Options{Logger: quiet(), CheckpointEvery: 1 << 30})
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = s.Close() }()
			for i := 0; i < n; i++ {
				id := "client-" + strconv.Itoa(i)
				k := Key{Namespace: "ns", ClientID: id, Epoch: 1}
				if err := s.SessionUpsert(SessionMeta{Namespace: "ns", ClientID: id, Epoch: 1, ExpiryInterval: 3600}); err != nil {
					b.Fatal(err)
				}
				if err := s.Subscribe(k, SubscriptionRecord{Filter: "devices/" + id + "/#", QoS: 1}); err != nil {
					b.Fatal(err)
				}
				if err := s.Enqueue(k, Entry{Seq: 1, Message: msg("ns", "devices/"+id+"/state", "a payload of some tens of bytes"), QoS: 1, PacketID: 1}); err != nil {
					b.Fatal(err)
				}
			}
			st, err := s.Recover()
			if err != nil {
				b.Fatal(err)
			}
			payload, err := json.Marshal(st)
			if err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				if err := s.Retain(msg("ns", "r", strconv.Itoa(i))); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				s.mu.Lock()
				err := s.wal.Sync()
				idx := s.wal.LastIndex()
				if err == nil {
					err = s.wal.WriteCheckpoint(idx, payload)
				}
				if err == nil {
					err = s.wal.Truncate(idx + 1)
				}
				s.mu.Unlock()
				b.StopTimer()
				if err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(len(payload))/(1<<20), "MiB")
		})
	}
}
