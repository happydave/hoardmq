package broker

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/session"
)

// TestOutboundInflightScale measures what the outbound in-flight bound costs:
// the memory a client that never acknowledges holds, and the rate a client
// that acknowledges at once, or after a delay, receives at. It runs only with
// HOARDMQ_INFLIGHT_SCALE set, to a comma-separated list of bounds; each bound
// is applied as the subscriber's Receive Maximum at MQTT 5.0, which sets the
// broker's send quota as a broker-side cap would, and "v4" measures a 3.1.1
// subscriber, which has no Receive Maximum. HOARDMQ_INFLIGHT_CAP, when set,
// is limits.maxInflightMessages for every run. It only measures: it fails
// only when a precondition does not hold.
func TestOutboundInflightScale(t *testing.T) {
	spec := os.Getenv("HOARDMQ_INFLIGHT_SCALE")
	if spec == "" {
		t.Skip("HOARDMQ_INFLIGHT_SCALE is not set")
	}
	for _, f := range strings.Split(spec, ",") {
		f = strings.TrimSpace(f)
		if f == "v4" {
			inflightScale(t, "v4", 0)
			continue
		}
		n, err := strconv.Atoi(f)
		if err != nil || n < 1 || n > 65535 {
			t.Fatalf("HOARDMQ_INFLIGHT_SCALE %q: each entry is v4 or a bound from 1 to 65535", spec)
		}
		inflightScale(t, f, uint16(n))
	}
}

// subscriberConnect is a QoS 1 subscriber's CONNECT: 3.1.1 when recvMax is
// 0, else 5.0 with that Receive Maximum.
func subscriberConnect(id string, recvMax uint16) (packet.Version, *packet.Connect) {
	c := &packet.Connect{ClientID: id, CleanStart: true, KeepAlive: 300}
	if recvMax == 0 {
		return packet.V4, c
	}
	c.Properties.ReceiveMaximum = &recvMax
	return packet.V5, c
}

func inflightScale(t *testing.T, label string, recvMax uint16) {
	const (
		slow     = 10
		queued   = 1000
		payload  = 256
		fastMsgs = 100000
	)
	quota := int64(recvMax)
	if recvMax == 0 {
		quota = 65535
	}
	limit := 65535
	if v := os.Getenv("HOARDMQ_INFLIGHT_CAP"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 65535 {
			t.Fatalf("HOARDMQ_INFLIGHT_CAP %q: a number from 1 to 65535", v)
		}
		limit = n
		label += fmt.Sprintf("/cap=%d", n)
	}
	quota = min(quota, int64(limit))
	// Each slow client is sent its quota and a full queue. The queue bound
	// is raised for the fill, so a message the socket has not yet taken is
	// queued rather than dropped, and set back to 1,000 before the heap is
	// read.
	perSlow := int(quota) + queued
	f := start(t, func(d *config.Document) {
		d.Limits.MaxQueuedMessages = perSlow
		d.Limits.MaxInflightMessages = limit
	})
	inject := func(topic string, i int) {
		p := make([]byte, payload)
		copy(p, fmt.Sprintf("%s-%d", topic, i))
		f.srv.sessions.Inject(&message.Message{Topic: topic, QoS: 1, Payload: p})
	}

	// The slow clients: each reads every packet and acknowledges none.
	var received [slow]atomic.Int64
	for i := 0; i < slow; i++ {
		v, c := subscriberConnect(fmt.Sprintf("slow-%d", i), recvMax)
		cl, ack := f.connect("tcp", v, c, false)
		if ack.ReasonCode != packet.Success {
			t.Fatalf("slow-%d: connack %v", i, ack.ReasonCode)
		}
		cl.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: fmt.Sprintf("slow/%d", i), QoS: 1}}})
		if _, ok := cl.read().(*packet.Suback); !ok {
			t.Fatalf("slow-%d: no SUBACK", i)
		}
		go func(i int) {
			for {
				_ = cl.c.SetReadDeadline(time.Now().Add(time.Minute))
				p, err := cl.dec.Read()
				if err != nil {
					return
				}
				if _, ok := p.(*packet.Publish); ok {
					received[i].Add(1)
				}
			}
		}(i)
	}
	runtime.GC()
	var base runtime.MemStats
	runtime.ReadMemStats(&base)
	began := time.Now()
	for i := 0; i < slow; i++ {
		for k := 0; k < perSlow; k++ {
			inject(fmt.Sprintf("slow/%d", i), k)
		}
	}
	deadline := time.Now().Add(5 * time.Minute)
	for i := 0; i < slow; i++ {
		for received[i].Load() < quota {
			if time.Now().After(deadline) {
				t.Fatalf("%s: slow-%d received %d, want %d", label, i, received[i].Load(), quota)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	time.Sleep(200 * time.Millisecond)
	f.srv.sessions.SetLimits(withQueue(f.srv.sessions.CurrentLimits(), queued))
	inflight, queuedAll := 0, 0
	for i := 0; i < slow; i++ {
		rec, ok := f.srv.sessions.Record(fmt.Sprintf("slow-%d", i))
		if !ok {
			t.Fatalf("%s: slow-%d has no session", label, i)
		}
		if int64(rec.InFlight) != quota || rec.Queued != queued {
			t.Fatalf("%s: slow-%d holds %d in flight and %d queued, want %d and %d", label, i, rec.InFlight, rec.Queued, quota, queued)
		}
		inflight += rec.InFlight
		queuedAll += rec.Queued
	}
	runtime.GC()
	var held runtime.MemStats
	runtime.ReadMemStats(&held)
	per := (int64(held.HeapAlloc) - int64(base.HeapAlloc)) / slow
	t.Logf("inflight-scale %s slow: %d clients, %d injected each in %s; in flight %d, queued %d in all; heap %d bytes a client (%.0f a held message)",
		label, slow, perSlow, time.Since(began).Round(time.Millisecond), inflight, queuedAll, per, float64(per)/float64((inflight+queuedAll)/slow))

	// The fast client, acknowledging at once, then after 1 ms.
	f.srv.sessions.SetLimits(withQueue(f.srv.sessions.CurrentLimits(), fastMsgs*2))
	for _, delay := range []time.Duration{0, time.Millisecond} {
		id := fmt.Sprintf("fast-%s", delay)
		v, c := subscriberConnect(id, recvMax)
		cl, ack := f.connect("tcp", v, c, false)
		if ack.ReasonCode != packet.Success {
			t.Fatalf("%s: connack %v", id, ack.ReasonCode)
		}
		topic := "fast/" + delay.String()
		cl.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: topic, QoS: 1}}})
		if _, ok := cl.read().(*packet.Suback); !ok {
			t.Fatalf("%s: no SUBACK", id)
		}
		var got atomic.Int64
		done := make(chan struct{})
		var writeMu sync.Mutex
		ackFor := func(pid uint16) {
			writeMu.Lock()
			defer writeMu.Unlock()
			_ = cl.c.SetWriteDeadline(time.Now().Add(10 * time.Second))
			_ = cl.enc.Encode(cl.c, &packet.Puback{Ack: packet.Ack{PacketID: pid}})
		}
		go func() {
			for {
				_ = cl.c.SetReadDeadline(time.Now().Add(time.Minute))
				p, err := cl.dec.Read()
				if err != nil {
					return
				}
				pub, ok := p.(*packet.Publish)
				if !ok {
					continue
				}
				if delay == 0 {
					ackFor(pub.PacketID)
				} else {
					pid := pub.PacketID
					time.AfterFunc(delay, func() { ackFor(pid) })
				}
				if got.Add(1) == fastMsgs {
					close(done)
				}
			}
		}()
		start := time.Now()
		for k := 0; k < fastMsgs; k++ {
			inject(topic, k)
		}
		injected := time.Since(start)
		select {
		case <-done:
		case <-time.After(3 * time.Minute):
			t.Fatalf("%s %s: received %d of %d", label, id, got.Load(), fastMsgs)
		}
		el := time.Since(start)
		t.Logf("inflight-scale %s fast ack-delay=%s: %d messages in %s (injected in %s): %.0f a second",
			label, delay, fastMsgs, el.Round(time.Millisecond), injected.Round(time.Millisecond), float64(fastMsgs)/el.Seconds())
		_ = cl.c.Close()
	}
}

// withQueue is l with its queue bound set to n.
func withQueue(l session.Limits, n int) session.Limits {
	l.MaxQueued = n
	return l
}
