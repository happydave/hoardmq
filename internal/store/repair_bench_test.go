package store

import (
	"strconv"
	"testing"
)

// BenchmarkRepair times one repair of a journal holding n sessions, each with
// its metadata, a subscription and a queued message, checkpointed, with 10,000
// records after the checkpoint, as the store lock holds it (WI 1812).
func BenchmarkRepair(b *testing.B) {
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
			if err := s.Sync(); err != nil {
				b.Fatal(err)
			}
			st, err := s.Recover()
			if err != nil {
				b.Fatal(err)
			}
			if err := s.Checkpoint(fixed(st)); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				for j := 0; j < 10000; j++ {
					id := "client-" + strconv.Itoa(j%n)
					if err := s.Sent(Key{Namespace: "ns", ClientID: id, Epoch: 1}, 1, 1); err != nil {
						b.Fatal(err)
					}
				}
				s.mu.Lock()
				s.wholeAt = s.syncFailures.Load() - 1 // not whole, as after a failed fsync
				b.StartTimer()
				err := s.repairLocked()
				b.StopTimer()
				s.mu.Unlock()
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
