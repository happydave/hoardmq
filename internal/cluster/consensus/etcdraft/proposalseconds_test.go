package etcdraft

import (
	"sync"
	"testing"
)

// labelledMetrics sums counters by name and outcome label.
type labelledMetrics struct {
	mu sync.Mutex
	v  map[string]float64
}

func (m *labelledMetrics) Add(name string, labels map[string]string, delta float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.v == nil {
		m.v = map[string]float64{}
	}
	m.v[name+"/"+labels["outcome"]] += delta
}

func (m *labelledMetrics) Set(string, map[string]string, float64) {}

func (m *labelledMetrics) get(key string) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.v[key]
}

// TestProposalLatencyIsCounted: an applied proposal adds its time from
// Propose to its outcome, under the outcome beside its count.
func TestProposalLatencyIsCounted(t *testing.T) {
	metrics := &labelledMetrics{}
	c := full(t, []string{"n1"}, func(o *Options) { o.Metrics = metrics })
	lead := c.settled(1)
	before := metrics.get("hoardmq_raft_proposals_total/applied")
	setConfig(t, lead, "k", "v")
	if got := metrics.get("hoardmq_raft_proposals_total/applied"); got != before+1 {
		t.Fatalf("applied proposals %v, want %v", got, before+1)
	}
	if got := metrics.get("hoardmq_raft_proposal_seconds_total/applied"); got <= 0 {
		t.Errorf("applied proposal seconds %v, want > 0", got)
	}
}
