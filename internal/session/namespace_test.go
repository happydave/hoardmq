package session

import (
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/happydave/hoardmq/internal/packet"
)

// seriesMetrics keeps every counter by name and sorted labels.
type seriesMetrics struct {
	mu sync.Mutex
	v  map[string]float64
}

func seriesKey(name string, labels map[string]string) string {
	parts := make([]string, 0, len(labels))
	for k, v := range labels {
		parts = append(parts, k+"="+v)
	}
	sort.Strings(parts)
	return name + "{" + strings.Join(parts, ",") + "}"
}

func (m *seriesMetrics) Add(name string, labels map[string]string, delta float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.v == nil {
		m.v = map[string]float64{}
	}
	m.v[seriesKey(name, labels)] += delta
}

func (m *seriesMetrics) Set(name string, labels map[string]string, value float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.v == nil {
		m.v = map[string]float64{}
	}
	m.v[seriesKey(name, labels)] = value
}

func (m *seriesMetrics) snapshot() map[string]float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]float64{}
	for k, v := range m.v {
		out[k] = v
	}
	return out
}

// TestNamespaceLabelsFollowTheSession: a node whose sessions live in another
// namespace counts its publishes, deliveries and drops under that namespace
// and none under default.
func TestNamespaceLabelsFollowTheSession(t *testing.T) {
	metrics := &seriesMetrics{}
	m, _ := newManager(t, func(o *Options) {
		o.Namespace = "tenant-a"
		o.Metrics = metrics
		o.MaxQueued = 1
	})
	pub := newFake("pub", packet.V5, 100)
	connectV5(m, pub, true, 0, nil)
	sub := newFake("sub", packet.V5, 100)
	connectV5(m, sub, true, 600, nil)
	subscribe(m, sub, 1, "t/x")
	m.Publish(pub, &packet.Publish{Topic: "t/x", QoS: 1, PacketID: 1, Payload: []byte("a")})
	m.Disconnect(sub, true, packet.Success, false)
	for id := uint16(2); id <= 4; id++ {
		m.Publish(pub, &packet.Publish{Topic: "t/x", QoS: 1, PacketID: id, Payload: []byte("b")})
	}
	got := metrics.snapshot()
	for _, want := range []string{
		"hoardmq_publishes_total{namespace=tenant-a,qos=1}",
		"hoardmq_deliveries_total{event=delivered,namespace=tenant-a}",
		"hoardmq_deliveries_total{event=queued,namespace=tenant-a}",
		"hoardmq_drops_total{namespace=tenant-a,reason=queue_full}",
	} {
		if got[want] == 0 {
			t.Errorf("%s not counted: %v", want, got)
		}
	}
	for k := range got {
		if strings.Contains(k, "namespace=default") {
			t.Errorf("counted under default: %s", k)
		}
	}
}
