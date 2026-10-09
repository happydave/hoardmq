package session

import (
	"strconv"
	"sync"
	"testing"

	"github.com/happydave/hoardmq/internal/packet"
)

// BenchmarkInboundQoS2Exchange times one durable publisher's inbound QoS 2
// exchange, PUBLISH then PUBREL, over a real store with its fsync, and
// reports the store's appends and fsyncs per exchange (WI 2031).
func BenchmarkInboundQoS2Exchange(b *testing.B) {
	m, st := benchManager(b)
	pub := newFake("pub", packet.V5, 100)
	connectV5(m, pub, false, 3600, nil)
	before := countsOf(st)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := uint16(i%65535 + 1)
		if code := publish(m, pub, "t", 2, id, "a payload of some tens of bytes"); code.IsError() {
			b.Fatalf("publish answered %v", code)
		}
		if code, send := m.Pubrel(pub, id); code != packet.Success || !send {
			b.Fatalf("PUBREL answered %v, send %v", code, send)
		}
	}
	b.StopTimer()
	c := countsOf(st).since(before)
	b.ReportMetric(float64(c.appends)/float64(b.N), "appends/op")
	b.ReportMetric(float64(c.syncs)/float64(b.N), "fsyncs/op")
}

// BenchmarkInboundQoS2ExchangeConcurrent runs one inbound QoS 2 exchange on
// each of 1,000 durable publishers from 1,000 goroutines at once, so the
// group commit shares fsyncs as it does under load. One op is the batch.
func BenchmarkInboundQoS2ExchangeConcurrent(b *testing.B) {
	const publishers = 1000
	m, st := benchManager(b)
	pubs := make([]*fakeConn, publishers)
	for i := range pubs {
		pubs[i] = newFake("pub-"+strconv.Itoa(i), packet.V5, 100)
		connectV5(m, pubs[i], false, 3600, nil)
	}
	before := countsOf(st)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := uint16(i%65535 + 1)
		var wg sync.WaitGroup
		for _, p := range pubs {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if code := publish(m, p, "t", 2, id, "a payload of some tens of bytes"); code.IsError() {
					b.Errorf("publish answered %v", code)
					return
				}
				if code, send := m.Pubrel(p, id); code != packet.Success || !send {
					b.Errorf("PUBREL answered %v, send %v", code, send)
				}
			}()
		}
		wg.Wait()
	}
	b.StopTimer()
	c := countsOf(st).since(before)
	b.ReportMetric(float64(c.appends)/float64(b.N*publishers), "appends/exchange")
	b.ReportMetric(float64(c.syncs)/float64(b.N), "fsyncs/op")
}
