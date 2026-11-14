package dispatch

import (
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/session"
)

// A CONNECT that resumes a sealed copy installs the session in the map, then
// adds its subscriptions to the router. A removal while those adds are under
// way (an eviction, a seal, a take) must leave no subscriber for a session the
// manager no longer holds. These tests hold the resume's first add and remove
// the session meanwhile, through the real dispatcher, and check what a client
// would see.

// holdAddRouter is the dispatcher whose next Add for client, once armed,
// waits until released, after telling entered.
type holdAddRouter struct {
	*Router
	client  string
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (r *holdAddRouter) Add(s *session.Session, sub session.Subscription) bool {
	if s.ClientID() == r.client && r.armed.CompareAndSwap(true, false) {
		r.entered <- struct{}{}
		<-r.release
	}
	return r.Router.Add(s, sub)
}

// observed records the last count the dispatcher reported for each filter,
// as the routing plane receives it.
type observed struct {
	mu   sync.Mutex
	last map[string]int
}

func (o *observed) observe(_, filter string, count int) {
	o.mu.Lock()
	o.last[filter] = count
	o.mu.Unlock()
}

func (o *observed) count(filter string) (int, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	n, ok := o.last[filter]
	return n, ok
}

type orphanHarness struct {
	t   *testing.T
	m   *session.Manager
	r   *holdAddRouter
	obs *observed
}

func newOrphanHarness(t *testing.T) *orphanHarness {
	t.Helper()
	r := &holdAddRouter{Router: New(RoundRobin), client: "c", entered: make(chan struct{}, 1), release: make(chan struct{})}
	obs := &observed{last: map[string]int{}}
	r.SetObserver(obs.observe)
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	m := session.New(session.Options{Router: r, Authorizers: auth.AuthorizerChain{allow{}}, Logger: quiet, Clock: clock, MaxQueued: 100})
	return &orphanHarness{t: t, m: m, r: r, obs: obs}
}

func (h *orphanHarness) connect(id string, clean bool, epoch uint64) (*fakeConn, packet.ReasonCode) {
	c := newConn(id)
	expiry := uint32(600)
	pkt := &packet.Connect{ProtocolLevel: packet.V5, ClientID: id, CleanStart: clean}
	pkt.Properties.SessionExpiryInterval = &expiry
	res := h.m.Connect(c, pkt, epoch)
	h.m.Resume(c)
	return c, res.Reason
}

func (h *orphanHarness) subscribe(c *fakeConn, filter string) {
	h.t.Helper()
	codes := h.m.Subscribe(c, &packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: filter, QoS: 1}}})
	h.m.Subscribed(c)
	if len(codes) != 1 || codes[0] != packet.GrantedQoS1 {
		h.t.Fatalf("subscribe %s: %v", filter, codes)
	}
}

const removeWait = 500 * time.Millisecond

// orphan makes client c's session leave this node by an eviction while a
// CONNECT is resuming its sealed copy: c subscribes to filter, goes offline,
// is sealed for a takeover, and is resumed by a CONNECT at a later grant. With
// race, the eviction runs while the resume's router add is held; without it,
// after the CONNECT has returned (the control). Either way the manager ends
// holding no session for c.
func (h *orphanHarness) orphan(filter string, race bool) {
	h.orphanBy(filter, race, func() bool { return h.m.Evict("default", "c", packet.SessionTakenOver) })
}

// sealAgain is the other removal a takeover makes: a seal for a newer grant.
func (h *orphanHarness) sealAgain() bool {
	return h.m.Seal("default", "c", packet.SessionTakenOver, 15).Outcome == session.SealSealed
}

// orphanBy is orphan with the removal given.
func (h *orphanHarness) orphanBy(filter string, race bool, remove func() bool) {
	h.t.Helper()
	c, _ := h.connect("c", true, 5)
	h.subscribe(c, filter)
	h.m.Disconnect(c, false, packet.NormalDisconnection, false)
	if res := h.m.Seal("default", "c", packet.SessionTakenOver, 9); res.Outcome != session.SealSealed {
		h.t.Fatalf("seal: %+v", res)
	}
	if !race {
		if _, code := h.connect("c", false, 12); code != packet.Success {
			h.t.Fatalf("resume: %v", code)
		}
		if !remove() {
			h.t.Fatal("the resumed session was not in the map to remove")
		}
	} else {
		h.r.armed.Store(true)
		unblock := sync.OnceFunc(func() { close(h.r.release) })
		h.t.Cleanup(unblock)
		done := make(chan struct{})
		go func() {
			defer close(done)
			h.connect("c", false, 12)
		}()
		select {
		case <-h.r.entered:
		case <-time.After(5 * time.Second):
			h.t.Fatal("the resume never reached the router")
		}
		// A removal that waits for the held add (the add made under the
		// session's mutex) is given removeWait to show it is waiting; the wait
		// is bounded, not asserted.
		var removed bool
		removing := make(chan struct{})
		go func() {
			defer close(removing)
			removed = remove()
		}()
		select {
		case <-removing:
		case <-time.After(removeWait):
		}
		unblock()
		<-removing
		<-done
		if !removed {
			h.t.Fatal("the resumed session was not in the map to remove")
		}
	}
	if _, held := h.m.Export("default", "c"); held {
		h.t.Fatal("the premise: the manager should hold no session for c after the removal")
	}
}

// queued is how many messages a session the manager holds has queued or in
// flight.
func (h *orphanHarness) queued(id string) int {
	st, held := h.m.Export("default", id)
	if !held {
		return 0
	}
	return len(st.Entries)
}

// orphanCases runs a test body once with the eviction racing the resume's
// router add and once after it, the control.
func orphanCases(t *testing.T, body func(t *testing.T, race bool)) {
	for _, c := range []struct {
		name string
		race bool
	}{{"removed as the resume adds its routes", true}, {"removed after the resume (control)", false}} {
		t.Run(c.name, func(t *testing.T) { body(t, c.race) })
	}
}

// TestASharedGroupMessageIsNotGivenToASessionNoLongerHeld: the evicted
// client's group membership survives in the dispatcher. A QoS 1 publish
// acknowledged Success must be held by some session the manager holds; the
// group's other member, offline here, should hold every one.
func TestASharedGroupMessageIsNotGivenToASessionNoLongerHeld(t *testing.T) {
	orphanCases(t, func(t *testing.T, race bool) {
		h := newOrphanHarness(t)
		h.orphan("$share/g/t", race)
		d, _ := h.connect("d", true, 0)
		h.subscribe(d, "$share/g/t")
		h.m.Disconnect(d, false, packet.NormalDisconnection, false)
		p, _ := h.connect("p", true, 0)

		const n = 4
		for i := 0; i < n; i++ {
			code, _ := h.m.Publish(p, &packet.Publish{Topic: "t", QoS: 1, PacketID: uint16(i + 1), Payload: []byte("m")})
			if code != packet.Success {
				t.Fatalf("publish %d answered %v, want Success (the group has a member)", i, code)
			}
		}
		if got := h.queued("d"); got != n {
			t.Fatalf("%d QoS 1 publishes were acknowledged Success; the only group member the manager holds has %d of them, the rest went to a session no longer held", n, got)
		}
	})
}

// TestAPublishMatchingOnlyASessionNoLongerHeldIsNotAcknowledgedAsDelivered:
// no route to the client's new owner reaches this node (a single node here),
// so a QoS 1 publish for its filter reaches no session. It must be answered
// No Matching Subscribers, not Success.
func TestAPublishMatchingOnlyASessionNoLongerHeldIsNotAcknowledgedAsDelivered(t *testing.T) {
	orphanCases(t, func(t *testing.T, race bool) {
		h := newOrphanHarness(t)
		h.orphan("t", race)
		p, _ := h.connect("p", true, 0)
		code, _ := h.m.Publish(p, &packet.Publish{Topic: "t", QoS: 1, PacketID: 1, Payload: []byte("m")})
		if code != packet.NoMatchingSubscribers {
			t.Fatalf("a QoS 1 publish whose only match is a session the manager no longer holds was answered %v, want No Matching Subscribers", code)
		}
	})
}

// TestARouteIsWithdrawnOnceNoSessionHoldsItsFilter: the plane advertises a
// filter while the dispatcher's last report for it is above zero. Once no
// session for it is held here, including a later session of the same client
// that came and left, the last report must be 0.
func TestARouteIsWithdrawnOnceNoSessionHoldsItsFilter(t *testing.T) {
	orphanCases(t, func(t *testing.T, race bool) {
		h := newOrphanHarness(t)
		h.orphan("t", race)
		if got, _ := h.obs.count("t"); got != 0 {
			t.Errorf("no session for the filter is held here, but the dispatcher last reported count %d to the plane", got)
		}
		c, _ := h.connect("c", true, 20)
		h.subscribe(c, "t")
		if !h.m.Evict("default", "c", packet.SessionTakenOver) {
			t.Fatal("the later session was not in the map to evict")
		}
		if got, _ := h.obs.count("t"); got != 0 {
			t.Fatalf("after a later session of the client came and left, the dispatcher last reported count %d to the plane", got)
		}
	})
}

// TestARouteIsWithdrawnWhenASealRacesTheResume: the same race with the
// takeover's seal for a newer grant as the removal.
func TestARouteIsWithdrawnWhenASealRacesTheResume(t *testing.T) {
	orphanCases(t, func(t *testing.T, race bool) {
		h := newOrphanHarness(t)
		h.orphanBy("t", race, h.sealAgain)
		if got, _ := h.obs.count("t"); got != 0 {
			t.Fatalf("the session is sealed for a newer grant, but the dispatcher last reported count %d to the plane", got)
		}
	})
}
