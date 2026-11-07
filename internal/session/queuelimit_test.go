package session

import (
	"fmt"
	"testing"

	"github.com/happydave/hoardmq/internal/packet"
)

// TestQueueBoundIsNeverAboveTheConfiguredLimit: the decision, with the values
// that would raise the limit.
func TestQueueBoundIsNeverAboveTheConfiguredLimit(t *testing.T) {
	for _, c := range []struct {
		configured, principal, want int
	}{
		{10, 0, 10},  // no principal limit
		{10, 5, 5},   // lowered
		{10, 20, 10}, // never raised
		{10, 10, 10},
		{0, 5, 5}, // no configured limit: the principal's bounds it
		{0, 0, 0},
		{10, -3, 10},
	} {
		if got := queueBound(c.configured, c.principal); got != c.want {
			t.Errorf("queueBound(%d, %d) = %d, want %d", c.configured, c.principal, got, c.want)
		}
	}
}

// queuedFor leaves a session for id offline with the given queue limit
// attribute, publishes n QoS 1 messages to it, and returns how many its
// resumption is sent.
func queuedFor(t *testing.T, m *Manager, pub *fakeConn, id, limit string, n int) int {
	t.Helper()
	sub := newFake(id, packet.V5, 1000)
	if limit != "" {
		sub.identity.Attributes = map[string]string{QueueLimitAttribute: limit}
	}
	connectV5(m, sub, false, 600, nil)
	subscribe(m, sub, 1, "q/"+id)
	m.Disconnect(sub, true, packet.Success, false)
	for i := 0; i < n; i++ {
		publish(m, pub, "q/"+id, 1, uint16(i+1), fmt.Sprint(i))
	}
	back := newFake(id, packet.V5, 1000)
	back.identity = sub.identity
	connectV5(m, back, false, 600, nil)
	return len(back.publishes())
}

// TestPrincipalQueueLimitLowersButNeverRaises: with a configured limit of 10, a
// principal's 2 bounds its queue at 2, a principal's 50 at 10, and a value that
// is not a count is counted at each CONNECT and ignored.
func TestPrincipalQueueLimitLowersButNeverRaises(t *testing.T) {
	metrics := &seriesMetrics{}
	m, _ := newManager(t, func(o *Options) { o.MaxQueued = 10; o.Metrics = metrics })
	pub := newFake("pub", packet.V5, 1000)
	connectV5(m, pub, true, 0, nil)
	if got := queuedFor(t, m, pub, "low", "2", 5); got != 2 {
		t.Errorf("a principal limit of 2: %d sent on resume, want 2", got)
	}
	if got := queuedFor(t, m, pub, "high", "50", 15); got != 10 {
		t.Errorf("a principal limit of 50 under a configured 10: %d sent, want 10", got)
	}
	if got := queuedFor(t, m, pub, "none", "", 15); got != 10 {
		t.Errorf("no principal limit: %d sent, want the configured 10", got)
	}
	if got := queuedFor(t, m, pub, "bad", "lots", 15); got != 10 {
		t.Errorf("an unreadable limit: %d sent, want the configured 10", got)
	}
	// Each CONNECT reads the attribute, and queuedFor connects twice.
	if v := metrics.snapshot()["hoardmq_session_queue_limit_invalid_total{namespace=default}"]; v != 2 {
		t.Errorf("unreadable limits counted %v, want 2: %v", v, metrics.snapshot())
	}
	if v := metrics.snapshot()["hoardmq_drops_total{namespace=default,reason=queue_full}"]; v != 3+5+5+5 {
		t.Errorf("queue_full drops %v, want 18", v)
	}
}

// TestTheLatestConnectsIdentityDecidesTheLimit: a session takes the limit of
// the identity that connected it last, as a takeover's new owner does.
func TestTheLatestConnectsIdentityDecidesTheLimit(t *testing.T) {
	m, _ := newManager(t, func(o *Options) { o.MaxQueued = 10 })
	pub := newFake("pub", packet.V5, 1000)
	connectV5(m, pub, true, 0, nil)
	first := newFake("c1", packet.V5, 1000)
	first.identity.Attributes = map[string]string{QueueLimitAttribute: "2"}
	connectV5(m, first, false, 600, nil)
	subscribe(m, first, 1, "q/c1")
	m.Disconnect(first, true, packet.Success, false)
	if got := queuedFor(t, m, pub, "c1", "3", 6); got != 3 {
		t.Fatalf("after a connect with a limit of 3: %d sent, want 3", got)
	}
}
