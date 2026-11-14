package session

import (
	"bytes"
	"fmt"
	"log/slog"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/happydave/hoardmq/internal/packet"
)

// lockedBuffer is a log destination safe for the reconciler's goroutine and
// the test's.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) lines(substr string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	for _, line := range strings.Split(l.b.String(), "\n") {
		if strings.Contains(line, substr) {
			out = append(out, line)
		}
	}
	return out
}

type reconcileFixture struct {
	t   *testing.T
	m   *Manager
	met *seriesMetrics
	log *lockedBuffer
}

func newReconcileFixture(t *testing.T, mutate func(*Options)) *reconcileFixture {
	t.Helper()
	f := &reconcileFixture{t: t, met: &seriesMetrics{}, log: &lockedBuffer{}}
	f.m, _ = newManager(t, func(o *Options) {
		o.Metrics = f.met
		o.Logger = slog.New(slog.NewTextHandler(f.log, &slog.HandlerOptions{Level: slog.LevelDebug}))
		o.Journal = newFakeJournal()
		if mutate != nil {
			mutate(o)
		}
	})
	return f
}

func (f *reconcileFixture) pass() {
	f.t.Helper()
	if !f.m.ReconcilePass() {
		f.t.Fatal("the router cannot be reconciled")
	}
}

func (f *reconcileFixture) metric(name string, labels map[string]string) float64 {
	return f.met.snapshot()[seriesKey(name, labels)]
}

func (f *reconcileFixture) corrections(direction, kind string) float64 {
	return f.metric("hoardmq_route_reconcile_corrections_total", map[string]string{"direction": direction, "kind": kind})
}

func (f *reconcileFixture) refused(kind, reason string) float64 {
	return f.metric("hoardmq_route_reconcile_refused_total", map[string]string{"kind": kind, "reason": reason})
}

// allCorrections sums every correction counted.
func (f *reconcileFixture) allCorrections() float64 {
	var n float64
	for k, v := range f.met.snapshot() {
		if strings.HasPrefix(k, "hoardmq_route_reconcile_corrections_total{") {
			n += v
		}
	}
	return n
}

// held is the session object the map holds for id.
func (f *reconcileFixture) held(id string) *Session {
	k := key{f.m.opts.Namespace, id}
	sh := f.m.shardFor(k)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	return sh.sessions[k]
}

func (f *reconcileFixture) client(id string, filters ...string) *fakeConn {
	c := newFake(id, packet.V5, 100)
	connectAt(f.m, c, true, 600, 1)
	if len(filters) > 0 {
		subscribe(f.m, c, 1, filters...)
	}
	return c
}

func (f *reconcileFixture) holds(s *Session, filter string) bool {
	return f.m.router.(RouterIndex).Holds(s, filter)
}

// TestReconcileReaddsAHeldSubscriptionMissingFromTheRouter: a held session's
// subscription removed from the router alone is added back at the second
// pass, not the first, counted and logged with the client and the filter.
func TestReconcileReaddsAHeldSubscriptionMissingFromTheRouter(t *testing.T) {
	f := newReconcileFixture(t, nil)
	f.client("c1", "t/1")
	s := f.held("c1")
	f.m.router.Remove(s, "t/1")

	f.pass()
	if f.holds(s, "t/1") {
		t.Fatal("corrected at the first sighting: the router holds c1 on t/1 after one pass")
	}
	if n := f.metric("hoardmq_route_reconcile_suspects", nil); n != 1 {
		t.Fatalf("after one pass %v suspects, want 1", n)
	}
	f.pass()
	if !f.holds(s, "t/1") {
		t.Fatal("after two passes the router still lacks c1 on t/1")
	}
	if n := f.corrections("announce", "held-undispatched"); n != 1 {
		t.Fatalf("corrections counted %v, want 1", n)
	}
	warns := f.log.lines("route reconciler corrected a difference")
	if len(warns) != 1 || !strings.Contains(warns[0], "client=c1") || !strings.Contains(warns[0], `filterStart="\"t/1\""`) || !strings.Contains(warns[0], "kind=held-undispatched") {
		t.Fatalf("the correction's Warn lines: %q", warns)
	}
	f.pass()
	if n := f.allCorrections(); n != 1 {
		t.Fatalf("a third pass made more corrections: %v in all", n)
	}
}

// TestReconcileRemovesASubscriberNoMapHolds: the leftover of a removed
// session, back in the router, is removed at the second pass; so is a held
// session's router entry for a filter its subscriptions lack.
func TestReconcileRemovesASubscriberNoMapHolds(t *testing.T) {
	f := newReconcileFixture(t, nil)
	f.client("c1", "t/1")
	old := f.held("c1")
	f.m.Evict(f.m.opts.Namespace, "c1", packet.SessionTakenOver)
	f.m.router.Add(old, Subscription{Filter: "t/1"})
	f.client("c2", "t/2")
	s2 := f.held("c2")
	f.m.router.Add(s2, Subscription{Filter: "t/extra"})

	f.pass()
	if !f.holds(old, "t/1") || !f.holds(s2, "t/extra") {
		t.Fatal("corrected at the first sighting")
	}
	f.pass()
	if f.holds(old, "t/1") {
		t.Fatal("after two passes the router still holds the removed object on t/1")
	}
	if f.holds(s2, "t/extra") {
		t.Fatal("after two passes the router still holds c2 on a filter c2 does not hold")
	}
	if !f.holds(s2, "t/2") {
		t.Fatal("c2's own subscription was removed")
	}
	if n := f.corrections("withdraw", "dispatched-unheld"); n != 2 {
		t.Fatalf("corrections counted %v, want 2", n)
	}
}

// TestReconcileLeavesADifferenceWhoseEvidenceChanged: an object replaced
// between the passes is not corrected; its suspect is counted cleared.
func TestReconcileLeavesADifferenceWhoseEvidenceChanged(t *testing.T) {
	f := newReconcileFixture(t, nil)
	f.client("c1", "t/1")
	s := f.held("c1")
	f.m.router.Remove(s, "t/1")
	f.pass()
	f.client("c1") // Clean Start: a new object, without t/1
	f.pass()
	if f.holds(s, "t/1") {
		t.Fatal("the replaced object was added back")
	}
	if n := f.allCorrections(); n != 0 {
		t.Fatalf("%v corrections, want 0", n)
	}
	if n := f.metric("hoardmq_route_reconcile_cleared_total", map[string]string{"kind": "held-undispatched"}); n != 1 {
		t.Fatalf("cleared counted %v, want 1", n)
	}
}

// TestReconcileRefusesAnAddForASessionRemovedAtTheWrite: the session is
// removed between the correction's read of the map and its decision; nothing
// is added, and the refusal is counted.
func TestReconcileRefusesAnAddForASessionRemovedAtTheWrite(t *testing.T) {
	f := newReconcileFixture(t, nil)
	f.client("c1", "t/1")
	s := f.held("c1")
	f.m.router.Remove(s, "t/1")
	f.pass()
	f.m.reconcileBetween = func(_, _, _ string, _ *Session) {
		f.m.Evict(f.m.opts.Namespace, "c1", packet.SessionTakenOver)
	}
	f.pass()
	if f.holds(s, "t/1") {
		t.Fatal("the reconciler added a subscriber for a session removed before its write")
	}
	if n := f.refused("held-undispatched", "removed"); n != 1 {
		t.Fatalf("refused{removed} counted %v, want 1", n)
	}
	if n := f.allCorrections(); n != 0 {
		t.Fatalf("%v corrections, want 0", n)
	}
}

// TestReconcileSkipsAKeyBeingImported: a leftover whose key an import holds is
// skipped at both passes, and judged again once the import is done.
func TestReconcileSkipsAKeyBeingImported(t *testing.T) {
	f := newReconcileFixture(t, nil)
	f.client("c1", "t/1")
	st, _ := f.m.Export(f.m.opts.Namespace, "c1")
	old := f.held("c1")
	f.m.Evict(f.m.opts.Namespace, "c1", packet.SessionTakenOver)
	f.m.router.Add(old, Subscription{Filter: "t/1"})

	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	f.m.importPause = func() { close(entered); <-release }
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.m.Import(st)
	}()
	<-entered
	f.pass()
	f.pass()
	if !f.holds(old, "t/1") {
		t.Fatal("a leftover of a key being imported was removed")
	}
	if n := f.metric("hoardmq_route_reconcile_skipped_total", map[string]string{"reason": "importing"}); n < 2 {
		t.Fatalf("skipped{importing} counted %v, want at least 2", n)
	}
	unblock()
	<-done
	f.m.importPause = nil
	f.pass()
	f.pass()
	if f.holds(old, "t/1") {
		t.Fatal("after the import and two passes the leftover is still in the router")
	}
	if s := f.held("c1"); s == nil || !f.holds(s, "t/1") {
		t.Fatal("the imported session is not in the router on t/1")
	}
	if n := f.corrections("withdraw", "dispatched-unheld"); n != 1 {
		t.Fatalf("corrections counted %v, want 1", n)
	}
}

// TestReconcileLogsAtMostACapOfCorrectionsAPass: past the cap, corrections are
// counted, not logged one by one, and summarised.
func TestReconcileLogsAtMostACapOfCorrectionsAPass(t *testing.T) {
	f := newReconcileFixture(t, nil)
	n := reconcileWarnCap + 5
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("c%d", i)
		f.client(id, "t/"+id)
		f.m.router.Remove(f.held(id), "t/"+id)
	}
	f.pass()
	f.pass()
	if got := f.corrections("announce", "held-undispatched"); got != float64(n) {
		t.Fatalf("corrections counted %v, want %d", got, n)
	}
	if got := len(f.log.lines("route reconciler corrected a difference")); got != reconcileWarnCap {
		t.Fatalf("%d correction Warns, want %d", got, reconcileWarnCap)
	}
	if got := f.metric("hoardmq_route_reconcile_warns_suppressed_total", nil); got != 5 {
		t.Fatalf("suppressed counted %v, want 5", got)
	}
	if s := f.log.lines("past the per-pass log limit"); len(s) != 1 || !strings.Contains(s[0], `"announce held-undispatched"=5`) {
		t.Fatalf("the summary Warn: %q", s)
	}
}

// TestReconcileLeavesTransientsAlone: passes inside each live path's transient
// and two after it make no correction. A transient is held for two passes
// where its state is not a difference; a sealed resume's gap is one (a held
// session's subscriptions not yet in the router), so it is held for one pass,
// as a transient shorter than the interval is: its second sighting finds the
// resume's adds made.
func TestReconcileLeavesTransientsAlone(t *testing.T) {
	cases := []struct {
		name   string
		inside int
		run    func(f *reconcileFixture, inside func())
	}{
		{"a sealed resume held before its routes", 1, func(f *reconcileFixture, inside func()) {
			sealForResume(f)
			hold(f, &f.m.resumePause, inside, func() { connectAt(f.m, newFake("c", packet.V5, 100), false, 600, 12) })
		}},
		{"an import held at its start", 2, func(f *reconcileFixture, inside func()) {
			f.client("c", "t")
			st, _ := f.m.Export(f.m.opts.Namespace, "c")
			hold(f, &f.m.importPause, inside, func() { f.m.Import(st) })
		}},
		{"a subscribe held after its lookup", 2, func(f *reconcileFixture, inside func()) {
			c := f.client("c", "t")
			hold(f, &f.m.subscribePause, inside, func() { subscribe(f.m, c, 1, "u") })
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newReconcileFixture(t, nil)
			passes := 0
			tc.run(f, func() {
				for i := 0; i < tc.inside; i++ {
					f.pass()
					passes++
				}
			})
			if passes != tc.inside {
				t.Fatalf("the transient was never held: %d passes inside it", passes)
			}
			f.pass()
			f.pass()
			if n := f.allCorrections(); n != 0 {
				t.Fatalf("%v corrections, want 0", n)
			}
		})
	}
}

// sealForResume leaves client c, subscribed to t, sealed for a newer grant.
func sealForResume(f *reconcileFixture) {
	c := f.client("c", "t")
	f.m.Disconnect(c, false, packet.NormalDisconnection, false)
	if res := f.m.Seal(f.m.opts.Namespace, "c", packet.SessionTakenOver, 9); res.Outcome != SealSealed {
		f.t.Fatalf("seal: %+v", res)
	}
}

// addCountingRouter counts, per filter, the adds that found the pair new: the
// adds a dispatcher would notify the routing plane of.
type addCountingRouter struct {
	*exactRouter
	mu  sync.Mutex
	new map[string]int
	old map[string]int
}

func (r *addCountingRouter) Add(s *Session, sub Subscription) bool {
	existed := r.exactRouter.Add(s, sub)
	r.mu.Lock()
	if existed {
		r.old[sub.Filter]++
	} else {
		r.new[sub.Filter]++
	}
	r.mu.Unlock()
	return existed
}

// TestReconcileCorrectsAResumeHeldTwoPassesHarmlessly: a sealed resume held
// across two passes is, for that long, the difference K1 exists for, and is
// corrected once. The resume's own add then finds the pair and notifies
// nothing: one notification in all, and the state consistent.
func TestReconcileCorrectsAResumeHeldTwoPassesHarmlessly(t *testing.T) {
	r := &addCountingRouter{exactRouter: newExactRouter(), new: map[string]int{}, old: map[string]int{}}
	f := newReconcileFixture(t, func(o *Options) { o.Router = r })
	sealForResume(f)
	r.mu.Lock()
	r.new["t"], r.old["t"] = 0, 0
	r.mu.Unlock()
	hold(f, &f.m.resumePause, func() { f.pass(); f.pass() }, func() { connectAt(f.m, newFake("c", packet.V5, 100), false, 600, 12) })
	f.pass()
	f.pass()
	if n := f.corrections("announce", "held-undispatched"); n != 1 || f.allCorrections() != 1 {
		t.Fatalf("corrections: %v held-undispatched, %v in all; want 1 and 1", n, f.allCorrections())
	}
	r.mu.Lock()
	added, again := r.new["t"], r.old["t"]
	r.mu.Unlock()
	if added != 1 || again != 1 {
		t.Fatalf("adds of the resumed subscription: %d new, %d already held; want 1 and 1 (the resume's own add finds the reconciler's)", added, again)
	}
	s := f.held("c")
	if s == nil || !f.holds(s, "t") {
		t.Fatal("the resumed session is not in the router on t")
	}
}

// hold sets *pause to wait inside op until inside has run, runs op, and
// returns once op has.
func hold(f *reconcileFixture, pause *func(), inside func(), op func()) {
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	f.t.Cleanup(unblock)
	var once sync.Once
	*pause = func() { once.Do(func() { close(entered); <-release }) }
	done := make(chan struct{})
	go func() {
		defer close(done)
		op()
	}()
	<-entered
	inside()
	unblock()
	<-done
	*pause = nil
}

// propertyOps drives random operations on clients, returning how many ran.
type propertyOps struct {
	f       *reconcileFixture
	clients []string
	filters []string
	epoch   atomic.Uint64
	mu      sync.Mutex
	conns   map[string]*fakeConn
}

func newPropertyOps(f *reconcileFixture, clients int) *propertyOps {
	p := &propertyOps{f: f, conns: map[string]*fakeConn{}}
	for i := 0; i < clients; i++ {
		p.clients = append(p.clients, fmt.Sprintf("p%d", i))
	}
	for i := 0; i < 8; i++ {
		p.filters = append(p.filters, fmt.Sprintf("f/%d", i))
	}
	p.epoch.Store(10)
	return p
}

// one runs one random operation on a random client.
func (p *propertyOps) one(rng *rand.Rand) {
	m := p.f.m
	id := p.clients[rng.Intn(len(p.clients))]
	p.mu.Lock()
	c := p.conns[id]
	p.mu.Unlock()
	switch op := rng.Intn(9); {
	case op == 0 || c == nil:
		c = newFake(id, packet.V5, 100)
		connectAt(m, c, rng.Intn(3) == 0, 600, p.epoch.Add(1))
		p.mu.Lock()
		p.conns[id] = c
		p.mu.Unlock()
	case op <= 2:
		subscribe(m, c, 1, p.filters[rng.Intn(len(p.filters))])
	case op == 3:
		m.Unsubscribe(c, &packet.Unsubscribe{PacketID: 1, Filters: []string{p.filters[rng.Intn(len(p.filters))]}})
	case op == 4:
		m.Disconnect(c, false, packet.NormalDisconnection, false)
	case op == 5:
		m.Evict(m.opts.Namespace, id, packet.SessionTakenOver)
	case op == 6:
		if st, ok := m.Export(m.opts.Namespace, id); ok {
			m.Import(st)
		}
	case op == 7:
		m.Seal(m.opts.Namespace, id, packet.SessionTakenOver, p.epoch.Add(1))
	default:
		c = newFake(id, packet.V5, 100)
		connectAt(m, c, false, 600, p.epoch.Add(1))
		p.mu.Lock()
		p.conns[id] = c
		p.mu.Unlock()
	}
}

// routed is how many (session, filter) pairs the router holds.
func routed(m *Manager) int {
	n := 0
	for c, done := 0, false; !done; {
		var fs []RoutedFilter
		fs, c, done, _ = m.router.(RouterIndex).WalkStep(c)
		for _, f := range fs {
			n += len(f.Subs)
		}
	}
	return n
}

// TestReconcilePropertySequential: operations one at a time, a pass after
// every few, make no correction; a difference seeded into such a run is
// corrected within two passes.
func TestReconcilePropertySequential(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			f := newReconcileFixture(t, nil)
			ops := newPropertyOps(f, 50)
			rng := rand.New(rand.NewSource(seed))
			passes, ran, peak := 0, 0, 0
			for i := 0; i < 600; i++ {
				ops.one(rng)
				ran++
				if i%5 == 4 {
					f.pass()
					passes++
					if n := routed(f.m); n > peak {
						peak = n
					}
				}
			}
			if passes < 100 || peak == 0 {
				t.Fatalf("vacuous: %d operations, %d passes, at most %d routed pairs", ran, passes, peak)
			}
			if n := f.allCorrections(); n != 0 {
				t.Fatalf("seed %d: %v corrections with no difference seeded; Warns %q", seed, n, f.log.lines("corrected a difference"))
			}
			// Seed one difference into the settled state: it is corrected.
			var victim *Session
			var filter string
			for _, id := range ops.clients {
				if s := f.held(id); s != nil {
					s.mu.Lock()
					for fl := range s.subs {
						filter = fl
					}
					s.mu.Unlock()
					if filter != "" {
						victim = s
						break
					}
				}
			}
			if victim == nil {
				t.Skip("no held session holds a filter at the end of this seed")
			}
			f.m.router.Remove(victim, filter)
			f.pass()
			f.pass()
			if !f.holds(victim, filter) {
				t.Fatalf("a seeded difference was not corrected within two passes")
			}
		})
	}
}

// TestReconcilePropertyConcurrent: operations on several goroutines with passes
// beside them. At each correction the operations are paused and the difference
// is confirmed from the session map and the router, each under its own lock;
// the correction must write exactly when it is confirmed. Once the operations
// stop, two passes leave no difference and a third makes no correction.
func TestReconcilePropertyConcurrent(t *testing.T) {
	hist := newObjectHistory()
	f := newReconcileFixture(t, func(o *Options) { o.Router = &historyRouter{exactRouter: newExactRouter(), h: hist} })
	f.m.traceSession = hist.trace
	ops := newPropertyOps(f, 20)
	var gate sync.RWMutex
	var confirmed atomic.Bool
	var mismatches []string
	var mmu sync.Mutex
	var histories []string
	f.m.reconcileBefore = func(kind, ns, filter string, s *Session) {
		gate.Lock()
		confirmed.Store(differs(f, kind, filter, s))
		if confirmed.Load() {
			histories = append(histories, correctionHistory(f, hist, kind, filter, s))
		}
	}
	f.m.reconcileAfter = func(kind, ns, filter string, s *Session, wrote bool) {
		if wrote != confirmed.Load() {
			mmu.Lock()
			mismatches = append(mismatches, fmt.Sprintf("%s %s %s: confirmed %v, wrote %v", kind, s.key.id, filter, confirmed.Load(), wrote))
			mmu.Unlock()
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
			rng := rand.New(rand.NewSource(int64(g + 1)))
			for !stop.Load() {
				gate.RLock()
				ops.one(rng)
				gate.RUnlock()
				ran.Add(1)
			}
		}(g)
	}
	passes := 0
	for passes < 60 {
		f.pass()
		passes++
	}
	stop.Store(true)
	wg.Wait()
	if ran.Load() < 1000 {
		t.Fatalf("vacuous: %d operations beside %d passes", ran.Load(), passes)
	}
	mmu.Lock()
	if len(mismatches) > 0 {
		t.Fatalf("corrections that did not match the independent check: %q", mismatches)
	}
	mmu.Unlock()
	if n := f.allCorrections(); n != 0 {
		t.Fatalf("%d operations, %d passes: %v corrections, want 0; their histories:\n%s", ran.Load(), passes, n, strings.Join(histories, "\n"))
	}
	if n := f.m.InstallsReplaced(); n != 0 {
		t.Fatalf("%d operations: %d session install(s) found another object for the client, want 0", ran.Load(), n)
	}
	f.pass()
	f.pass()
	before := f.allCorrections()
	f.pass()
	if after := f.allCorrections(); after != before {
		t.Fatalf("after the operations stopped a third pass corrected %v more", after-before)
	}
	for _, id := range ops.clients {
		s := f.held(id)
		if s == nil {
			continue
		}
		s.mu.Lock()
		subs := make([]string, 0, len(s.subs))
		for fl := range s.subs {
			subs = append(subs, fl)
		}
		s.mu.Unlock()
		for _, fl := range subs {
			if !f.holds(s, fl) {
				t.Fatalf("settled, %s holds %s and the router does not", id, fl)
			}
		}
	}
	for c, done := 0, false; !done; {
		var fs []RoutedFilter
		fs, c, done, _ = f.m.router.(RouterIndex).WalkStep(c)
		for _, rf := range fs {
			for _, s := range rf.Subs {
				if differs(f, "dispatched-unheld", rf.Filter, s) {
					t.Fatalf("settled, the router holds %s on %s and no map holds it with that filter", s.key.id, rf.Filter)
				}
			}
		}
	}
	t.Logf("%d operations, %d passes, %v corrections", ran.Load(), passes, f.allCorrections())
	for k, v := range f.met.snapshot() {
		if strings.HasPrefix(k, "hoardmq_route_reconcile_corrections_total{") || strings.HasPrefix(k, "hoardmq_route_reconcile_refused_total{") {
			t.Logf("%s %v", k, v)
		}
	}
	for _, w := range f.log.lines("corrected a difference") {
		t.Log(w)
	}
	for _, h := range histories {
		t.Log(h)
	}
}

// correctionHistory describes a confirmed correction: the object, the map's
// object for its key, and each one's history with the client's last events.
// The operations are paused while it runs.
func correctionHistory(f *reconcileFixture, hist *objectHistory, kind, filter string, s *Session) string {
	cur := f.held(s.key.id)
	var b strings.Builder
	fmt.Fprintf(&b, "CORRECTION %s client %s filter %s object %s (removed %v); the map holds %s\n", kind, s.key.id, filter, hist.objName(s), s.removedNow(), hist.objName(cur))
	obj, client := hist.of(s)
	b.WriteString("  object history:\n")
	for _, e := range obj {
		b.WriteString("    " + e + "\n")
	}
	if cur != nil && cur != s {
		cobj, _ := hist.of(cur)
		b.WriteString("  map's object history:\n")
		for _, e := range cobj {
			b.WriteString("    " + e + "\n")
		}
	}
	if len(client) > 80 {
		client = client[len(client)-80:]
	}
	b.WriteString("  client's last events:\n")
	for _, e := range client {
		b.WriteString("    " + e + "\n")
	}
	return b.String()
}

func (s *Session) removedNow() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.removed
}

// differs checks a K1 or K2 difference from the session map and the router,
// each read under its own lock.
func differs(f *reconcileFixture, kind, filter string, s *Session) bool {
	sh := f.m.shardFor(s.key)
	sh.mu.Lock()
	isMap := sh.sessions[s.key] == s
	sh.mu.Unlock()
	s.mu.Lock()
	_, holds := s.subs[filter]
	s.mu.Unlock()
	inRouter := f.holds(s, filter)
	if kind == "held-undispatched" {
		return isMap && holds && !inRouter
	}
	return inRouter && (!isMap || !holds)
}

// TestReconcileRemovesAnObjectNotTheMapsForItsKey: a second object for a
// client, still holding the filter in its own set (as an object a load
// overwrote does), is removed; the map's object for the key is kept.
func TestReconcileRemovesAnObjectNotTheMapsForItsKey(t *testing.T) {
	f := newReconcileFixture(t, nil)
	f.client("c1", "t/1")
	old := f.held("c1")
	f.m.Evict(f.m.opts.Namespace, "c1", packet.SessionTakenOver)
	f.client("c1", "t/1")
	cur := f.held("c1")
	old.mu.Lock()
	old.subs["t/1"] = Subscription{Filter: "t/1", QoS: 1}
	old.removed = false
	old.mu.Unlock()
	f.m.router.Add(old, Subscription{Filter: "t/1", QoS: 1})
	f.pass()
	f.pass()
	if f.holds(old, "t/1") {
		t.Fatal("after two passes the router still holds the object no map holds")
	}
	if !f.holds(cur, "t/1") {
		t.Fatal("the map's object for the key was removed")
	}
}

func TestK2Refusal(t *testing.T) {
	for _, c := range []struct {
		isMap, importing, holds, connected bool
		want                               string
	}{
		{false, false, true, false, ""},
		{false, false, false, false, ""},
		{true, false, false, false, ""},
		{false, true, true, false, "importing"},
		{true, false, true, false, "now-held"},
		{false, false, true, true, "connected"},
		{false, false, false, true, ""},
		{true, false, false, true, ""},
		{false, true, true, true, "importing"},
		{true, false, true, true, "now-held"},
	} {
		if got := k2Refusal(c.isMap, c.importing, c.holds, c.connected); got != c.want {
			t.Errorf("k2Refusal(map %v, importing %v, holds %v, connected %v) = %q, want %q", c.isMap, c.importing, c.holds, c.connected, got, c.want)
		}
	}
}
