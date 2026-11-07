package session

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/happydave/hoardmq/internal/hook"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/packet"
)

// dropObserver records the payload of every Dropped delivery.
type dropObserver struct {
	recordingObserver
	mu      sync.Mutex
	dropped []string
}

func (o *dropObserver) OnDelivery(_ context.Context, ev hook.DeliveryEvent, _, _ string, m *message.Message, _ byte) {
	if ev != hook.Dropped {
		return
	}
	o.mu.Lock()
	o.dropped = append(o.dropped, string(m.Payload))
	o.mu.Unlock()
}

func (o *dropObserver) payloads() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.dropped...)
}

// offlineEnv is a manager with a publisher and a subscriber "sub" that holds
// a session for an hour, subscribed to t/hi at QoS 2 and t/lo at QoS 0.
type offlineEnv struct {
	m       *Manager
	j       *fakeJournal
	metrics *seriesMetrics
	obs     *dropObserver
	pub     *fakeConn
	sub     *fakeConn
}

func newOfflineEnv(t *testing.T, drop bool) *offlineEnv {
	t.Helper()
	e := &offlineEnv{j: newFakeJournal(), metrics: &seriesMetrics{}, obs: &dropObserver{recordingObserver: recordingObserver{log: &hookLog{fakeJournal: newFakeJournal()}, noAdmission: true}}}
	e.m, _ = newManager(t, func(o *Options) {
		o.Journal = e.j
		o.Metrics = e.metrics
		o.Observer = e.obs
		o.DropOfflineQoS0 = drop
	})
	e.pub = newFake("pub", packet.V5, 100)
	connectV5(e.m, e.pub, true, 0, nil)
	e.sub = newFake("sub", packet.V5, 100)
	connectV5(e.m, e.sub, false, 3600, nil)
	subscribe(e.m, e.sub, 2, "t/hi")
	subscribe(e.m, e.sub, 0, "t/lo")
	return e
}

func (e *offlineEnv) offline() { e.m.Disconnect(e.sub, true, packet.Success, false) }

// resume reconnects the subscriber and returns the payloads it is sent.
func (e *offlineEnv) resume() []string {
	c := newFake("sub", packet.V5, 100)
	connectV5(e.m, c, false, 3600, nil)
	var out []string
	for _, p := range c.publishes() {
		out = append(out, string(p.Payload)+"@"+string(rune('0'+p.QoS)))
	}
	return out
}

func (e *offlineEnv) offlineDrops() float64 {
	return e.metrics.snapshot()["hoardmq_drops_total{namespace=default,reason=offline_qos0}"]
}

// enqueues counts the journal's enqueue records for the subscriber.
func (e *offlineEnv) enqueues() int {
	n := 0
	e.j.mu.Lock()
	defer e.j.mu.Unlock()
	for _, c := range e.j.calls {
		if c == "enqueue sub" || c == "enqueue-sent sub" {
			n++
		}
	}
	return n
}

// TestOfflineQoS0IsDroppedAndCounted: with the drop set, an offline session
// is queued its QoS 1 and 2 messages and not its QoS 0 ones, each QoS 0 drop
// counted, reported and unjournaled. A QoS 1 publish matched at QoS 0 is a
// QoS 0 delivery.
func TestOfflineQoS0IsDroppedAndCounted(t *testing.T) {
	e := newOfflineEnv(t, true)
	e.offline()
	publish(e.m, e.pub, "t/hi", 0, 0, "q0")
	publish(e.m, e.pub, "t/hi", 1, 1, "q1")
	publish(e.m, e.pub, "t/hi", 2, 2, "q2")
	publish(e.m, e.pub, "t/lo", 1, 3, "capped")
	if got := e.enqueues(); got != 2 {
		t.Errorf("%d enqueue records for sub, want 2 (q1, q2): %v", got, e.j.calls)
	}
	if got := e.offlineDrops(); got != 2 {
		t.Errorf("offline_qos0 drops %v, want 2: %v", got, e.metrics.snapshot())
	}
	if got := e.obs.payloads(); strings.Join(got, ",") != "q0,capped" {
		t.Errorf("observer saw drops %q, want q0 and capped", got)
	}
	if got := strings.Join(e.resume(), ","); got != "q1@1,q2@2" {
		t.Errorf("resumed with %q, want q1@1,q2@2", got)
	}
}

// TestOfflineQoS0IsQueuedByDefault: without the drop, the same publishes are
// all queued and sent on resume, and nothing is counted as an offline drop.
func TestOfflineQoS0IsQueuedByDefault(t *testing.T) {
	e := newOfflineEnv(t, false)
	e.offline()
	publish(e.m, e.pub, "t/hi", 0, 0, "q0")
	publish(e.m, e.pub, "t/hi", 1, 1, "q1")
	publish(e.m, e.pub, "t/hi", 2, 2, "q2")
	publish(e.m, e.pub, "t/lo", 1, 3, "capped")
	if got := strings.Join(e.resume(), ","); got != "q0@0,q1@1,q2@2,capped@0" {
		t.Errorf("resumed with %q, want all four", got)
	}
	if got := e.offlineDrops(); got != 0 {
		t.Errorf("offline_qos0 drops %v under the default", got)
	}
}

// TestConnectedSessionStillQueuesQoS0: the drop applies only to a session with
// no connection; a connected one whose connection cannot take the message
// queues it as before, and sends it once the connection can.
func TestConnectedSessionStillQueuesQoS0(t *testing.T) {
	e := newOfflineEnv(t, true)
	e.sub.mu.Lock()
	e.sub.busy = true
	e.sub.mu.Unlock()
	publish(e.m, e.pub, "t/hi", 0, 0, "deferred")
	if got := e.offlineDrops(); got != 0 {
		t.Fatalf("a connected session's QoS 0 was dropped (%v)", got)
	}
	if got := e.enqueues(); got != 1 {
		t.Fatalf("%d enqueue records for the deferred QoS 0, want 1: %v", got, e.j.calls)
	}
}

// TestQueuedQoS0SurvivesTheConnectionEnding: the drop governs new arrivals; a
// QoS 0 queued while the session was connected stays queued when the
// connection ends and the drop is turned on, and is sent on resume, while one
// arriving after is dropped.
func TestQueuedQoS0SurvivesTheConnectionEnding(t *testing.T) {
	e := newOfflineEnv(t, false)
	e.sub.mu.Lock()
	e.sub.busy = true
	e.sub.mu.Unlock()
	publish(e.m, e.pub, "t/hi", 0, 0, "before")
	e.offline()
	l := e.m.CurrentLimits()
	l.DropOfflineQoS0 = true
	e.m.SetLimits(l)
	publish(e.m, e.pub, "t/hi", 0, 0, "after")
	if got := strings.Join(e.resume(), ","); got != "before@0" {
		t.Errorf("resumed with %q, want only the QoS 0 queued before", got)
	}
	if got := e.offlineDrops(); got != 1 {
		t.Errorf("offline_qos0 drops %v, want 1 (after)", got)
	}
}
