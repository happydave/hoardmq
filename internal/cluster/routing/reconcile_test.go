package routing

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/dispatch"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/session"
)

// The route reconciler with the routing plane: own entries against the
// dispatcher, corrected through the plane's correction entry point.

type rcConn struct {
	id       string
	identity auth.Identity
}

func (c *rcConn) ClientID() string               { return c.id }
func (c *rcConn) Version() packet.Version        { return packet.V5 }
func (c *rcConn) Identity() *auth.Identity       { return &c.identity }
func (c *rcConn) SessionExpiryOverride() *uint32 { return nil }
func (c *rcConn) Send(packet.Packet) error       { return nil }
func (c *rcConn) Offer(packet.Packet) error      { return nil }
func (c *rcConn) Close(packet.ReasonCode)        {}
func (c *rcConn) SendQuota() int                 { return 1 << 20 }
func (c *rcConn) Deliver(*packet.Publish) error  { return nil }

type rcAllow struct{}

func (rcAllow) Authorize(context.Context, auth.Action, string, *auth.Identity) auth.Decision {
	return auth.Allow
}

type rcFixture struct {
	t   *testing.T
	p   *Plane
	r   *dispatch.Router
	m   *session.Manager
	met *labelMetrics
	log *lockedBuffer
	// observe, when set, stands between the dispatcher and the plane.
	observe func(ns, filter string, count int)
}

func newRCFixture(t *testing.T, grace time.Duration) *rcFixture {
	t.Helper()
	f := &rcFixture{t: t, p: movingPlane(t, grace), r: dispatch.New(dispatch.RoundRobin), met: newLabelMetrics(), log: &lockedBuffer{}}
	f.r.SetObserver(func(ns, filter string, count int) {
		if f.observe != nil {
			f.observe(ns, filter, count)
			return
		}
		f.p.Observe(ns, filter, count)
	})
	f.m = session.New(session.Options{Router: f.r, OwnRoutes: f.p, Authorizers: auth.AuthorizerChain{rcAllow{}}, Metrics: f.met, MaxQueued: 100,
		Logger: slog.New(slog.NewTextHandler(f.log, &slog.HandlerOptions{Level: slog.LevelDebug}))})
	return f
}

func (f *rcFixture) connect(id string) *rcConn {
	c := &rcConn{id: id, identity: auth.Identity{ClientID: id}}
	f.m.Connect(c, &packet.Connect{ProtocolLevel: packet.V5, ClientID: id, CleanStart: true}, 0)
	f.m.Resume(c)
	return c
}

func (f *rcFixture) subscribe(c *rcConn, filter string) {
	f.m.Subscribe(c, &packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: filter, QoS: 1}}})
	f.m.Subscribed(c)
}

func (f *rcFixture) unsubscribe(c *rcConn, filter string) {
	f.m.Unsubscribe(c, &packet.Unsubscribe{PacketID: 1, Filters: []string{filter}})
}

func (f *rcFixture) pass() {
	f.t.Helper()
	if !f.m.ReconcilePass() {
		f.t.Fatal("the dispatcher cannot be reconciled")
	}
}

func (f *rcFixture) own(filter string) (Entry, bool) {
	return f.p.table.Lookup("n1", "default", filter)
}

// seed merges an own entry directly, as a lost or inverted notification
// leaves it.
func (f *rcFixture) seed(filter string, count int) {
	f.p.table.MergeOwn(Entry{Namespace: "default", Filter: filter, Owner: "n1", Count: count, Tombstone: count == 0}, &f.p.version)
}

func (f *rcFixture) corrections(direction, kind string) float64 {
	return f.met.get("hoardmq_route_reconcile_corrections_total", "direction", direction, "kind", kind)
}

func (f *rcFixture) allCorrections() float64 {
	f.met.mu.Lock()
	defer f.met.mu.Unlock()
	var n float64
	for k, v := range f.met.n {
		if len(k) > 41 && k[:41] == "hoardmq_route_reconcile_corrections_total" {
			n += v
		}
	}
	return n
}

// TestReconcileReannouncesARouteTombstonedWhileHeld: a held, dispatched direct
// subscription whose own entry is a tombstone is live with count 1 again at
// the second pass, not the first; a group whose own count is one below its
// members gets the members' count.
func TestReconcileReannouncesARouteTombstonedWhileHeld(t *testing.T) {
	f := newRCFixture(t, time.Hour)
	f.subscribe(f.connect("c1"), "k/1")
	f.seed("k/1", 0)
	g1, g2 := f.connect("g1"), f.connect("g2")
	f.subscribe(g1, "$share/g/k/2")
	f.subscribe(g2, "$share/g/k/2")
	f.seed("$share/g/k/2", 1)

	f.pass()
	if e, _ := f.own("k/1"); !e.Tombstone {
		t.Fatal("corrected at the first sighting: k/1 is live after one pass")
	}
	f.pass()
	if e, ok := f.own("k/1"); !ok || e.Tombstone || e.Count != 1 {
		t.Fatalf("after two passes k/1's own entry is %+v, want live with count 1", e)
	}
	if e, ok := f.own("$share/g/k/2"); !ok || e.Tombstone || e.Count != 2 {
		t.Fatalf("after two passes the group's own entry is %+v, want live with count 2", e)
	}
	if n := f.corrections("announce", "unadvertised"); n != 1 {
		t.Fatalf("unadvertised corrections counted %v, want 1", n)
	}
	if n := f.corrections("announce", "count"); n != 1 {
		t.Fatalf("count corrections counted %v, want 1", n)
	}
	f.pass()
	f.pass()
	if n := f.allCorrections(); n != 2 {
		t.Fatalf("later passes corrected more: %v in all", n)
	}
}

// TestReconcileAnnouncesADirectFilterWhoseSubscribersChurn: subscribers come
// and go between the sightings without the filter reaching none; the evidence
// is "any subscriber", so the entry is corrected, with count 1.
func TestReconcileAnnouncesADirectFilterWhoseSubscribersChurn(t *testing.T) {
	f := newRCFixture(t, time.Hour)
	a, b, c := f.connect("a"), f.connect("b"), f.connect("c")
	f.subscribe(a, "k/3")
	f.subscribe(b, "k/3")
	f.seed("k/3", 0)
	f.pass()
	f.unsubscribe(a, "k/3")
	f.subscribe(c, "k/3")
	f.pass()
	if e, ok := f.own("k/3"); !ok || e.Tombstone || e.Count != 1 {
		t.Fatalf("after two passes k/3's own entry is %+v, want live with count 1", e)
	}
}

// TestReconcileAnnouncesFromATombstonePrunedBetweenTheSightings: an announce
// from a tombstone accepts that tombstone pruned; a withdrawal whose version
// moved between the sightings is judged again, not corrected.
func TestReconcileAnnouncesFromATombstonePrunedBetweenTheSightings(t *testing.T) {
	f := newRCFixture(t, time.Hour)
	f.subscribe(f.connect("c1"), "k/4")
	f.seed("k/4", 0)
	f.seed("k/5", 1) // no subscriber anywhere
	f.pass()
	f.p.table.PruneOwnTombstones(^uint64(0))
	f.seed("k/5", 1) // a live merge moves its version
	f.pass()
	if e, ok := f.own("k/4"); !ok || e.Tombstone || e.Count != 1 {
		t.Fatalf("k/4, its tombstone pruned between the sightings, is %+v, want live with count 1", e)
	}
	if e, _ := f.own("k/5"); e.Tombstone {
		t.Fatal("k/5 was withdrawn though its version moved between the sightings")
	}
	f.pass()
	if e, _ := f.own("k/5"); !e.Tombstone {
		t.Fatal("k/5 was not withdrawn once two sightings agreed")
	}
}

// TestReconcileWithdrawsAnAdvertisedRouteWithNoSubscriber: a live own entry
// with no subscriber is a tombstone at the second pass.
func TestReconcileWithdrawsAnAdvertisedRouteWithNoSubscriber(t *testing.T) {
	f := newRCFixture(t, time.Hour)
	f.seed("k/6", 1)
	f.pass()
	if e, _ := f.own("k/6"); e.Tombstone {
		t.Fatal("corrected at the first sighting")
	}
	f.pass()
	if e, ok := f.own("k/6"); !ok || !e.Tombstone {
		t.Fatalf("after two passes k/6's own entry is %+v, want a tombstone", e)
	}
	if n := f.corrections("withdraw", "advertised-undispatched"); n != 1 {
		t.Fatalf("corrections counted %v, want 1", n)
	}
}

// TestReconcileRefusesACorrectionALivePathOvertook: a live emit, or the start
// of a move, between the plane's check and the table's: the correction writes
// nothing, counted by reason.
func TestReconcileRefusesACorrectionALivePathOvertook(t *testing.T) {
	cases := []struct {
		name, kind, reason string
		withdraw           bool
		race               func(p *Plane, filter string)
		check              func(e Entry) bool
	}{
		{"an announce overtaken by an emit", "unadvertised", "version", false, func(p *Plane, filter string) { p.emit("default", filter, 1) }, func(e Entry) bool { return !e.Tombstone }},
		{"an announce overtaken by a move", "unadvertised", "moving", false, func(p *Plane, filter string) { p.table.SetMoving("default", filter, true) }, func(e Entry) bool { return e.Tombstone }},
		{"a withdrawal overtaken by an emit", "advertised-undispatched", "version", true, func(p *Plane, filter string) { p.emit("default", filter, 1) }, func(e Entry) bool { return !e.Tombstone }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newRCFixture(t, time.Hour)
			if tc.withdraw {
				f.seed("k/7", 1)
			} else {
				f.subscribe(f.connect("c1"), "k/7")
				f.seed("k/7", 0)
			}
			f.pass()
			f.p.beforeCorrect = func(c session.OwnRouteCorrection) { tc.race(f.p, c.Filter) }
			f.pass()
			f.p.beforeCorrect = nil
			if n := f.met.get("hoardmq_route_reconcile_refused_total", "kind", tc.kind, "reason", tc.reason); n != 1 {
				t.Fatalf("refused{%s} counted %v, want 1", tc.reason, n)
			}
			if n := f.allCorrections(); n != 0 {
				t.Fatalf("%v corrections, want 0", n)
			}
			if e, _ := f.own("k/7"); !tc.check(e) {
				t.Fatalf("the entry after the race is %+v", e)
			}
		})
	}
}

// TestReconcileLeavesThePlanesTransientsAlone: passes inside each transient of
// the plane's live paths, and after it, make no correction.
func TestReconcileLeavesThePlanesTransientsAlone(t *testing.T) {
	t.Run("a held withdrawal in its grace, then its timer", func(t *testing.T) {
		f := newRCFixture(t, 500*time.Millisecond)
		c := f.connect("c1")
		f.subscribe(c, "k/8")
		f.p.Moving("default", []string{"k/8"})
		f.m.Evict("default", "c1", packet.SessionTakenOver)
		if heldFor(f.p, "k/8") == nil {
			t.Fatal("the premise: the withdrawal is held")
		}
		f.pass()
		f.pass()
		waitFor(t, "the held withdrawal written", 5*time.Second, func() bool {
			e, _ := f.own("k/8")
			return e.Tombstone && heldFor(f.p, "k/8") == nil
		})
		f.pass()
		f.pass()
		if n := f.allCorrections(); n != 0 {
			t.Fatalf("%v corrections, want 0", n)
		}
		if n := f.met.get("hoardmq_route_reconcile_skipped_total", "reason", "held"); n < 2 {
			t.Fatalf("skipped{held} counted %v, want at least 2", n)
		}
	})
	t.Run("a late held callback after a newer hold", func(t *testing.T) {
		f := newRCFixture(t, 500*time.Millisecond)
		f.p.Observe("default", "t/0", 1)
		handOver(f.p, "t/0")
		first := heldFor(f.p, "t/0")
		f.p.Observe("default", "t/0", 1)
		handOver(f.p, "t/0")
		f.p.emitHeld("default", "t/0", first)
		f.pass()
		f.pass()
		waitFor(t, "the newer hold written", 5*time.Second, func() bool {
			e, _ := f.own("t/0")
			return e.Tombstone && heldFor(f.p, "t/0") == nil
		})
		f.pass()
		f.pass()
		if n := f.allCorrections(); n != 0 {
			t.Fatalf("%v corrections, want 0", n)
		}
	})
	t.Run("a notification held between the dispatcher and the plane for one pass", func(t *testing.T) {
		f := newRCFixture(t, time.Hour)
		deliver := holdNotification(t, f, "k/9")
		f.pass()
		deliver()
		f.pass()
		f.pass()
		if e, ok := f.own("k/9"); !ok || e.Tombstone || e.Count != 1 {
			t.Fatalf("after the notification k/9's own entry is %+v, want live with count 1", e)
		}
		if n := f.allCorrections(); n != 0 {
			t.Fatalf("%v corrections, want 0", n)
		}
	})
}

// holdNotification makes client c1 subscribe to filter with the dispatcher's
// notification to the plane held back, and returns its delivery: the
// subscribe has returned, the dispatcher holds the subscriber, and the plane
// has not heard.
func holdNotification(t *testing.T, f *rcFixture, filter string) (deliver func()) {
	t.Helper()
	var held []func()
	f.observe = func(ns, fl string, count int) {
		if fl == filter {
			held = append(held, func() { f.p.Observe(ns, fl, count) })
			return
		}
		f.p.Observe(ns, fl, count)
	}
	f.subscribe(f.connect("c1"), filter)
	if len(held) != 1 {
		t.Fatalf("the premise: %d notifications held, want 1", len(held))
	}
	return func() {
		f.observe = nil
		held[0]()
	}
}

// TestReconcileCorrectsANotificationHeldTwoPassesHarmlessly: held across two
// passes, a notification is indistinguishable from a lost one and is
// corrected once. When it arrives, its merge takes a later version and
// stands: the live path wins, and the state is consistent.
func TestReconcileCorrectsANotificationHeldTwoPassesHarmlessly(t *testing.T) {
	f := newRCFixture(t, time.Hour)
	deliver := holdNotification(t, f, "k/9")
	f.pass()
	f.pass()
	corrected, ok := f.own("k/9")
	if !ok || corrected.Tombstone || corrected.Count != 1 {
		t.Fatalf("after two passes k/9's own entry is %+v, want the correction's live entry", corrected)
	}
	deliver()
	after, _ := f.own("k/9")
	if after.Version <= corrected.Version || after.Tombstone || after.Count != 1 {
		t.Fatalf("the late notification's merge is %+v, want live with count 1 above the correction's version %d", after, corrected.Version)
	}
	f.pass()
	f.pass()
	if n := f.corrections("announce", "unadvertised"); n != 1 || f.allCorrections() != 1 {
		t.Fatalf("corrections: %v unadvertised, %v in all; want 1 and 1", n, f.allCorrections())
	}
	if e, _ := f.own("k/9"); e.Version != after.Version {
		t.Fatalf("a later pass wrote over the live path's merge: %+v", e)
	}
}

// TestReconcilePropertyConcurrentWithThePlane: subscribes and unsubscribes of
// several clients on one direct filter and one group, on four goroutines,
// with passes beside them. At each own-route correction the operations are
// paused and the difference is confirmed from the dispatcher and the table,
// each under its own lock: a correction never merges where they agree. Once
// the operations stop, two passes leave the own entries saying what the
// dispatcher holds.
func TestReconcilePropertyConcurrentWithThePlane(t *testing.T) {
	f := newRCFixture(t, time.Hour)
	filters := []string{"pc/1", "$share/g/pc/2"}
	var conns []*rcConn
	for i := 0; i < 8; i++ {
		conns = append(conns, f.connect(fmt.Sprintf("q%d", i)))
	}
	var gate sync.RWMutex
	var mu sync.Mutex
	var bad []string
	confirmed := false
	f.p.beforeCorrect = func(c session.OwnRouteCorrection) {
		gate.Lock()
		confirmed = !agrees(f, c.Filter)
	}
	f.p.afterCorrect = func(c session.OwnRouteCorrection, merged bool) {
		if merged && !confirmed {
			mu.Lock()
			bad = append(bad, fmt.Sprintf("%s merged count %d where the dispatcher and the table agreed", c.Filter, c.Count))
			mu.Unlock()
		}
		if merged && !agrees(f, c.Filter) {
			mu.Lock()
			bad = append(bad, fmt.Sprintf("%s merged count %d and still disagrees with the dispatcher", c.Filter, c.Count))
			mu.Unlock()
		}
		gate.Unlock()
	}
	var stop atomic.Bool
	var ran atomic.Int64
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(g + 7)))
			for !stop.Load() {
				c := conns[rng.Intn(len(conns))]
				fl := filters[rng.Intn(len(filters))]
				gate.RLock()
				if rng.Intn(2) == 0 {
					f.subscribe(c, fl)
				} else {
					f.unsubscribe(c, fl)
				}
				gate.RUnlock()
				ran.Add(1)
			}
		}(g)
	}
	for i := 0; i < 200; i++ {
		f.pass()
	}
	stop.Store(true)
	wg.Wait()
	if ran.Load() < 1000 {
		t.Fatalf("vacuous: %d operations", ran.Load())
	}
	mu.Lock()
	if len(bad) > 0 {
		t.Fatalf("corrections against the independent check: %q", bad)
	}
	mu.Unlock()
	f.pass()
	f.pass()
	for _, fl := range filters {
		if !agrees(f, fl) {
			e, _ := f.own(fl)
			t.Fatalf("settled, %s's own entry %+v disagrees with the dispatcher's %d", fl, e, f.r.SubscriberCount("default", fl))
		}
	}
	t.Logf("%d operations, %v corrections", ran.Load(), f.allCorrections())
}

// agrees reports whether a filter's own entry says what the dispatcher holds:
// live with count 1 for a direct filter with subscribers, the member count for
// a group, and not live with none.
func agrees(f *rcFixture, filter string) bool {
	n := f.r.SubscriberCount("default", filter)
	e, ok := f.own(filter)
	live := ok && !e.Tombstone
	switch {
	case n == 0:
		return !live
	case filter[0] == '$':
		return live && e.Count == n
	default:
		return live && e.Count == 1
	}
}

// TestReconcileCorrectionsReachOlderPeersAsDeltas: a reconciling node's
// announce and withdrawal reach a peer pinned to the first release's route
// sync and a current peer as ordinary deltas, merged by version, with nothing
// read as a bad route update.
func TestReconcileCorrectionsReachOlderPeersAsDeltas(t *testing.T) {
	a := frameNode(t, "a", frameOpts{maxVersion: transport.Version5, legacy: true})
	b := frameNode(t, "b", frameOpts{})
	c := frameNode(t, "c", frameOpts{})
	pairUp(t, a, b, c)
	reached := func(what string, live bool, version uint64) {
		t.Helper()
		for _, n := range []*fnode{a, c} {
			waitFor(t, what+" at "+n.name, 5*time.Second, func() bool {
				e, ok := n.plane.Table().Lookup("b", "default", "mx/1")
				return ok && e.Tombstone == !live && e.Version == version
			})
		}
	}
	merged, why := b.plane.CorrectOwn(session.OwnRouteCorrection{Namespace: "default", Filter: "mx/1", Count: 1, FromNotLive: true})
	if !merged {
		t.Fatalf("the announce was refused: %s", why)
	}
	e, _ := b.plane.Table().Lookup("b", "default", "mx/1")
	reached("the announce", true, e.Version)
	merged, why = b.plane.CorrectOwn(session.OwnRouteCorrection{Namespace: "default", Filter: "mx/1", Version: e.Version, Live: true, EvidenceCount: 1})
	if !merged {
		t.Fatalf("the withdrawal was refused: %s", why)
	}
	w, _ := b.plane.Table().Lookup("b", "default", "mx/1")
	reached("the withdrawal", false, w.Version)
	for _, n := range []*fnode{a, c} {
		if k := n.logs.count(`msg="bad route update"`); k != 0 {
			t.Fatalf("%s logged %d bad route updates", n.name, k)
		}
	}
}
