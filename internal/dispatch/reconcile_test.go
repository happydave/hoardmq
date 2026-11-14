package dispatch

import (
	"testing"

	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/session"
)

// The route reconciler on the real dispatcher, seen as a client sees it.

// objectOf is the session object the dispatcher holds for client on filter.
func objectOf(t *testing.T, r *Router, client, filter string) *session.Session {
	t.Helper()
	for c, done := 0, false; !done; {
		var fs []session.RoutedFilter
		fs, c, done, _ = r.WalkStep(c)
		for _, f := range fs {
			if f.Filter != filter {
				continue
			}
			for _, s := range f.Subs {
				if s.ClientID() == client {
					return s
				}
			}
		}
	}
	t.Fatalf("the dispatcher holds no %s on %s", client, filter)
	return nil
}

// leftover leaves client c's evicted session object in the dispatcher on
// filter, as a resume's late add once did.
func leftover(t *testing.T, h *orphanHarness, filter string) {
	t.Helper()
	c, _ := h.connect("c", true, 5)
	h.subscribe(c, filter)
	old := objectOf(t, h.r.Router, "c", filter)
	if !h.m.Evict("default", "c", packet.SessionTakenOver) {
		t.Fatal("c was not held")
	}
	h.r.Router.Add(old, session.Subscription{Filter: filter, QoS: 1})
}

func passes(t *testing.T, m *session.Manager, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if !m.ReconcilePass() {
			t.Fatal("the dispatcher cannot be reconciled")
		}
	}
}

// TestReconcileEndsASharedGroupLossToALeftover: a leftover member takes half
// a group's messages until the reconciler removes it; then the member the
// manager holds receives every one.
func TestReconcileEndsASharedGroupLossToALeftover(t *testing.T) {
	h := newOrphanHarness(t)
	leftover(t, h, "$share/g/t")
	d, _ := h.connect("d", true, 0)
	h.subscribe(d, "$share/g/t")
	h.m.Disconnect(d, false, packet.NormalDisconnection, false)
	p, _ := h.connect("p", true, 0)
	publish := func(from uint16) {
		for i := uint16(0); i < 4; i++ {
			if code, _ := h.m.Publish(p, &packet.Publish{Topic: "t", QoS: 1, PacketID: from + i, Payload: []byte("m")}); code != packet.Success {
				t.Fatalf("publish answered %v, want Success", code)
			}
		}
	}
	publish(1)
	if got := h.queued("d"); got != 2 {
		t.Fatalf("the premise: before the correction the held member has %d of 4, want 2", got)
	}
	passes(t, h.m, 2)
	publish(5)
	if got := h.queued("d") - 2; got != 4 {
		t.Fatalf("after two passes the only group member the manager holds has %d of 4", got)
	}
}

// TestReconcileEndsADirectPublishAcknowledgedToALeftover: a QoS 1 publish whose
// only match is a leftover is answered Success until the reconciler removes
// it, then No Matching Subscribers.
func TestReconcileEndsADirectPublishAcknowledgedToALeftover(t *testing.T) {
	h := newOrphanHarness(t)
	leftover(t, h, "t")
	p, _ := h.connect("p", true, 0)
	if code, _ := h.m.Publish(p, &packet.Publish{Topic: "t", QoS: 1, PacketID: 1, Payload: []byte("m")}); code != packet.Success {
		t.Fatalf("the premise: before the correction the publish was answered %v, want Success", code)
	}
	passes(t, h.m, 2)
	if code, _ := h.m.Publish(p, &packet.Publish{Topic: "t", QoS: 1, PacketID: 2, Payload: []byte("m")}); code != packet.NoMatchingSubscribers {
		t.Fatalf("after two passes the publish was answered %v, want No Matching Subscribers", code)
	}
	if got, _ := h.obs.count("t"); got != 0 {
		t.Fatalf("after the leftover's removal the dispatcher last reported count %d to the plane, want 0", got)
	}
}

// TestReconcileRestoresAHeldSubscriptionToTheDispatcher: a held subscription
// removed from the dispatcher alone is added back at the second pass; the
// plane is told count 1 again and a publish here is delivered.
func TestReconcileRestoresAHeldSubscriptionToTheDispatcher(t *testing.T) {
	h := newOrphanHarness(t)
	c, _ := h.connect("c", true, 0)
	h.subscribe(c, "k")
	h.r.Remove(objectOf(t, h.r.Router, "c", "k"), "k")
	p, _ := h.connect("p", true, 0)
	if code, _ := h.m.Publish(p, &packet.Publish{Topic: "k", QoS: 1, PacketID: 1, Payload: []byte("m")}); code != packet.NoMatchingSubscribers {
		t.Fatalf("the premise: with c's subscription out of the dispatcher the publish was answered %v", code)
	}
	passes(t, h.m, 1)
	if got, _ := h.obs.count("k"); got != 0 {
		t.Fatalf("corrected at the first sighting: the plane was told %d", got)
	}
	passes(t, h.m, 1)
	if got, _ := h.obs.count("k"); got != 1 {
		t.Fatalf("after two passes the plane was last told count %d, want 1", got)
	}
	before := len(c.sent)
	if code, _ := h.m.Publish(p, &packet.Publish{Topic: "k", QoS: 1, PacketID: 2, Payload: []byte("m")}); code != packet.Success {
		t.Fatalf("after two passes the publish was answered %v, want Success", code)
	}
	if len(c.sent) != before+1 {
		t.Fatalf("c was sent %d messages after the correction, want 1", len(c.sent)-before)
	}
}
