package broker

import (
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
	_ "github.com/happydave/hoardmq/modules/sinkkafka" // sink.kafka, against a Kafka that is not there
)

// deadPort is a loopback address nothing listens on.
func deadPort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

func (f *fixture) series(name string, labels map[string]string) float64 {
	for _, m := range f.srv.Registry().Metrics().Snapshot() {
		if m.Name != name || len(m.Labels) != len(labels) {
			continue
		}
		match := true
		for k, v := range labels {
			if m.Labels[k] != v {
				match = false
			}
		}
		if match {
			return m.Value
		}
	}
	return 0
}

// waitSeries waits until a series reaches at least want.
func (f *fixture) waitSeries(t *testing.T, name string, labels map[string]string, want float64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for f.series(name, labels) < want {
		if time.Now().After(deadline) {
			t.Fatalf("%s%v = %v, want at least %v", name, labels, f.series(name, labels), want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestSinkKafkaWithNoKafka runs the real Kafka client against a broker that
// is not there: the buffer fills and holds, and each policy does what it
// says, every publish counted.
func TestSinkKafkaWithNoKafka(t *testing.T) {
	for _, policy := range []string{"refuse", "drop"} {
		t.Run(policy, func(t *testing.T) {
			kafka := deadPort(t)
			f := start(t, func(d *config.Document) {
				d.Modules["sink.kafka"] = json.RawMessage(fmt.Sprintf(
					`{"brokers":[%q],"routes":[{"filter":"k/#","topic":"tk"}],"lingerMs":0,"flushTimeoutMs":200,`+
						`"buffer":{"maxRecords":3,"whenFull":%q,"waitMs":100}}`, kafka, policy))
			})
			sub, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "sub", CleanStart: true}, false)
			sub.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "k/#", QoS: 1}}})
			sub.read()
			pub, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
			acks := func(n int, from uint16) []packet.ReasonCode {
				var codes []packet.ReasonCode
				for i := range n {
					pub.send(&packet.Publish{Topic: "k/1", QoS: 1, PacketID: from + uint16(i), Payload: []byte("x")})
					codes = append(codes, pub.read().(*packet.Puback).ReasonCode)
				}
				return codes
			}
			for i, c := range acks(3, 1) {
				if c != packet.Success {
					t.Fatalf("publish %d into a buffer with room: %s", i+1, c)
				}
			}
			f.waitSeries(t, "hoardmq_sink_kafka_buffered_records", nil, 3)
			if policy == "refuse" {
				if c := acks(1, 4)[0]; c != packet.QuotaExceeded {
					t.Fatalf("the fourth publish into a full buffer: %s, want 0x97", c)
				}
				if got := f.series("hoardmq_sink_kafka_refused_total", map[string]string{"topic": "tk"}); got != 1 {
					t.Fatalf("refused %v, want 1", got)
				}
				// QoS 0 is not refused: delivered over MQTT, its forward counted dropped.
				pub.send(&packet.Publish{Topic: "k/0", Payload: []byte("zero")})
				f.waitSeries(t, "hoardmq_sink_kafka_dropped_total", map[string]string{"topic": "tk", "reason": "buffer_full"}, 1)
			} else {
				for i, c := range acks(2, 4) {
					if c != packet.Success {
						t.Fatalf("drop policy, publish %d: %s", i+4, c)
					}
				}
				f.waitSeries(t, "hoardmq_sink_kafka_dropped_total", map[string]string{"topic": "tk", "reason": "buffer_full"}, 2)
			}
			if got := f.series("hoardmq_sink_kafka_buffered_records", nil); got != 3 {
				t.Fatalf("buffered %v, want 3", got)
			}
			delivered := 0
			for delivered < 3 {
				p := sub.readPublish()
				if p.QoS > 0 {
					sub.send(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID}})
				}
				delivered++
			}
			start := time.Now()
			f.stop()
			if d := time.Since(start); d > 200*time.Millisecond+time.Second+5*time.Second {
				t.Fatalf("shutdown took %v", d)
			}
			f.waitSeries(t, "hoardmq_sink_kafka_dropped_total", map[string]string{"topic": "tk", "reason": "shutdown"}, 3)
		})
	}
}
