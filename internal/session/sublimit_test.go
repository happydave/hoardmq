package session

import (
	"slices"
	"testing"

	"github.com/happydave/hoardmq/internal/packet"
)

// A session holds at most MaxSubscriptions filters: a new filter past the
// limit is refused alone with Quota exceeded and leaves no trace in the
// session, the router or the journal; a filter the session holds is replaced,
// whatever the count.
func TestSubscriptionLimit(t *testing.T) {
	j := newFakeJournal()
	metrics := &seriesMetrics{}
	m, _ := newManager(t, func(o *Options) { o.Journal = j; o.Metrics = metrics; o.MaxSubscriptions = 2 })
	pub := newFake("pub", packet.V5, 100)
	sub := newFake("sub", packet.V5, 100)
	connectV5(m, pub, true, 0, nil)
	connectV5(m, sub, false, 3600, nil)
	refusals := seriesKey("hoardmq_subscription_limit_refusals_total", map[string]string{"namespace": "default"})

	ask := func(filters ...string) []packet.ReasonCode {
		t.Helper()
		s := &packet.Subscribe{PacketID: 1}
		for _, f := range filters {
			s.Subscriptions = append(s.Subscriptions, packet.Subscription{Filter: f, QoS: 1})
		}
		return m.Subscribe(sub, s)
	}
	held := func() []string {
		s := m.lookup(sub)
		s.mu.Lock()
		defer s.mu.Unlock()
		var out []string
		for f := range s.subs {
			out = append(out, f)
		}
		slices.Sort(out)
		return out
	}
	delivered := func(topic string) bool {
		t.Helper()
		before := len(sub.publishes())
		publish(m, pub, topic, 0, 0, "x")
		return len(sub.publishes()) > before
	}

	if got := ask("a", "b"); !slices.Equal(got, []packet.ReasonCode{1, 1}) {
		t.Fatalf("a, b under the limit: %v", got)
	}
	if got := ask("c"); !slices.Equal(got, []packet.ReasonCode{packet.QuotaExceeded}) {
		t.Fatalf("c past the limit: %v", got)
	}
	if h := held(); !slices.Equal(h, []string{"a", "b"}) {
		t.Fatalf("held after the refusal: %v", h)
	}
	if n := j.count("subscribe sub a"); n != 1 {
		t.Fatalf("a granted filter was journaled %d times", n)
	}
	if n := j.count("subscribe sub c"); n != 0 {
		t.Fatalf("the refused filter was journaled %d times", n)
	}
	if delivered("c") {
		t.Fatal("a publish to the refused filter was delivered")
	}
	if got := metrics.snapshot()[refusals]; got != 1 {
		t.Fatalf("refusals counted %v, want 1", got)
	}

	if got := ask("a"); !slices.Equal(got, []packet.ReasonCode{1}) {
		t.Fatalf("a replaced at the limit: %v", got)
	}
	if got := ask("d", "a"); !slices.Equal(got, []packet.ReasonCode{packet.QuotaExceeded, 1}) {
		t.Fatalf("d new and a replaced in one SUBSCRIBE: %v", got)
	}
	if got := metrics.snapshot()[refusals]; got != 2 {
		t.Fatalf("refusals counted %v, want 2", got)
	}

	m.Unsubscribe(sub, &packet.Unsubscribe{PacketID: 2, Filters: []string{"b"}})
	if got := ask("c"); !slices.Equal(got, []packet.ReasonCode{1}) {
		t.Fatalf("c after b's unsubscribe: %v", got)
	}
	if !delivered("c") {
		t.Fatal("a publish to the granted filter was not delivered")
	}

	// A lowered limit trims nothing: a replacement is still granted and a new
	// filter refused.
	m.SetLimits(Limits{MaxQueued: 10, MaxSubscriptions: 1})
	if h := held(); !slices.Equal(h, []string{"a", "c"}) {
		t.Fatalf("held after the limit was lowered: %v", h)
	}
	if got := ask("c", "e"); !slices.Equal(got, []packet.ReasonCode{1, packet.QuotaExceeded}) {
		t.Fatalf("over a lowered limit: %v", got)
	}

	m.SetLimits(Limits{MaxQueued: 10})
	if got := ask("e", "f", "g", "h", "i"); !slices.Equal(got, []packet.ReasonCode{1, 1, 1, 1, 1}) {
		t.Fatalf("with no limit: %v", got)
	}
}

// The decision alone, at its boundary.
func TestSubscriptionFits(t *testing.T) {
	for _, c := range []struct {
		held, limit int
		holds, want bool
	}{
		{0, 0, false, true},
		{5, 0, false, true},
		{1, 2, false, true},
		{2, 2, false, false},
		{3, 2, false, false},
		{2, 2, true, true},
		{3, 2, true, true},
	} {
		if got := subscriptionFits(c.held, c.limit, c.holds); got != c.want {
			t.Errorf("subscriptionFits(%d, %d, %v) = %v, want %v", c.held, c.limit, c.holds, got, c.want)
		}
	}
}
