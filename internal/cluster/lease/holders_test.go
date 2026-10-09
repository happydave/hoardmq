package lease

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

func TestHolderUnaccounted(t *testing.T) {
	for _, c := range []struct {
		name   string
		holder string
		live   bool
		want   bool
	}{
		{"down, alive by the log", "n8", true, true},
		{"down, declared dead or never recorded", "n8", false, false},
		{"this node", "n1", true, false},
		{"no holder", "", true, false},
	} {
		if got := holderUnaccounted(c.holder, "n1", c.live); got != c.want {
			t.Errorf("%s: holderUnaccounted = %v, want %v", c.name, got, c.want)
		}
	}
}

// aliveNode keeps name alive by the log: an alive record proposed through via
// every 200 ms until the returned stop, after which the leader declares it
// dead a ttl later. No transport knows it and it owns nothing.
func aliveNode(t *testing.T, via *tnode, name string) (stop func()) {
	t.Helper()
	propose := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if res, err := via.svc.submit(ctx, Op{Kind: OpAlive, Node: name, Now: time.Now()}); err != nil || !res.OK {
			t.Errorf("proposing an alive record for %s: %+v, %v", name, res, err)
		}
	}
	propose()
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		tick := time.NewTicker(200 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-done:
				return
			case <-tick.C:
				propose()
			}
		}
	}()
	stop = sync.OnceFunc(func() { close(done); wg.Wait() })
	t.Cleanup(stop)
	return stop
}

// downHolder has every fallback of the next cluster report n8 down.
func downHolder(t *testing.T) {
	fixtureDown = func(string, string) []string { return []string{"n8"} }
	t.Cleanup(func() { fixtureDown = nil })
}

// holderCluster is three nodes whose fallback is fb and reports holder n8 down,
// with n8 alive by the log. n1 owns x and holds nothing for it, so n1 answers a
// pull with no state; the adoption worker runs on opts, held back when nil.
func holderCluster(t *testing.T, fb func(context.Context, string, string, string) (*store.SessionState, bool), opts func(o *Options)) (a, b *tnode, stopHolder func()) {
	t.Helper()
	if opts == nil {
		holdWorkerBack(t)
	} else {
		fixtureOptions = opts
		t.Cleanup(func() { fixtureOptions = nil })
	}
	fixtureFallback = fb
	t.Cleanup(func() { fixtureFallback = nil })
	downHolder(t)
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	a, b = c.nodes["n1"], c.nodes["n2"]
	stopHolder = aliveNode(t, a, "n8")
	if _, err := acquire(t, a, "x", nil, 300, true); err != nil {
		t.Fatal(err)
	}
	ownedBy(t, b, "x", "n1")
	waitFor(t, "n2's table to show n8 alive", 5*time.Second, func() bool { return b.svc.table.Live("n8") })
	return a, b, stopHolder
}

// TestTakeoverWithAnUndeclaredHolderDownWaitsForItsDeclaration is WI 1765 (a)
// on the takeover path. The previous owner answers that it holds nothing and
// every replica asked answers "not found", but the one holder the fetch
// skipped as down is alive by the log and may come back with its copy. The
// client is refused and the adoption kept until the holder is declared dead.
func TestTakeoverWithAnUndeclaredHolderDownWaitsForItsDeclaration(t *testing.T) {
	_, b, stop := holderCluster(t, foundNowhere, nil)
	res, err := acquire(t, b, "x", nil, 300, false)
	if !refusedBusy(err) {
		t.Fatalf("takeover with an undeclared holder down, found nowhere: %+v, %v; want Server Busy", res, err)
	}
	if ad := adoptionOf(b, "x"); ad == nil || ad.job.prev != "n1" || !ad.job.takeover {
		t.Fatalf("adoption %+v; want one kept with n1 as its previous owner", ad)
	}
	if st := b.svc.LivenessStats(); st.TakeoverHolderUndeclared != 1 || st.TakeoverEmpty != 0 {
		t.Fatalf("counters: holder undeclared %d, empty %d; want 1, 0", st.TakeoverHolderUndeclared, st.TakeoverEmpty)
	}
	if _, err := acquire(t, b, "x", nil, 300, false); !refusedBusy(err) || !b.svc.table.Live("n8") {
		t.Fatalf("retry before the declaration: %v, n8 live %v; want Server Busy while n8 is live", err, b.svc.table.Live("n8"))
	}
	if st := b.svc.LivenessStats(); st.AdoptHolderUndeclared < 1 {
		t.Fatalf("the retry's adoption fetch counted %d holder waits, want at least 1", st.AdoptHolderUndeclared)
	}
	stop()
	declared(t, b, "n8")
	res, err = acquire(t, b, "x", nil, 300, false)
	wantEmpty(t, "retry after the holder's declaration", res, err, EmptyAdoption, "n1")
	if n := b.svc.adopting.unfinished(); n != 0 {
		t.Fatalf("%d adoptions unfinished after the fresh session was served", n)
	}
}

// TestAdoptionWithAnUndeclaredHolderDownWaitsForItsDeclaration is WI 1765 (a)
// on the adoption path, the shape corpus seed 1 traced: the previous owner is
// declared dead and cannot be asked, and the only holder is down. The CONNECT
// is refused until the holder is declared, then served fresh.
func TestAdoptionWithAnUndeclaredHolderDownWaitsForItsDeclaration(t *testing.T) {
	holdWorkerBack(t)
	fixtureFallback = foundNowhere
	t.Cleanup(func() { fixtureFallback = nil })
	downHolder(t)
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	lead := c.settled()
	stopOwner := undeclaredOwner(t, lead, "x")
	stopHolder := aliveNode(t, lead, "n8")
	ownedBy(t, lead, "x", "n9")
	stopOwner()
	ownedBy(t, lead, "x", lead.name)
	if lead.svc.table.Live("n9") || !lead.svc.table.Live("n8") {
		t.Fatalf("precondition: n9 live %v, n8 live %v; want the owner declared and the holder alive", lead.svc.table.Live("n9"), lead.svc.table.Live("n8"))
	}
	res, err := acquire(t, lead, "x", nil, 300, false)
	if !refusedBusy(err) {
		t.Fatalf("adoption with an undeclared holder down, found nowhere: %+v, %v; want Server Busy", res, err)
	}
	if st := lead.svc.LivenessStats(); st.AdoptHolderUndeclared != 1 || st.AdoptInlineEmpty != 0 || !lead.svc.adopting.unfinishedFor(key{"default", "x"}) {
		t.Fatalf("holder undeclared %d, inline empty %d, unfinished %v; want 1, 0, true", st.AdoptHolderUndeclared, st.AdoptInlineEmpty, lead.svc.adopting.unfinishedFor(key{"default", "x"}))
	}
	stopHolder()
	declared(t, lead, "n8")
	res, err = acquire(t, lead, "x", nil, 300, false)
	wantEmpty(t, "retry after the holder's declaration", res, err, EmptyAdoption, "n9")
}

// TestCopyIsServedDespiteAnUndeclaredHolderDown: Tier 1. A copy found anywhere
// is served at once, whatever holder is down.
func TestCopyIsServedDespiteAnUndeclaredHolderDown(t *testing.T) {
	_, b, _ := holderCluster(t, peerFetch(replicaOf("x")), nil)
	res, err := acquire(t, b, "x", nil, 300, false)
	if err != nil || !hasFilter(res.State, "t/replica") || res.Empty.Reason != "" {
		t.Fatalf("takeover with a copy and a holder down: state %v, evidence %+v, err %v; want the copy", res.State, res.Empty, err)
	}
	if st := b.svc.LivenessStats(); st.TakeoverHolderUndeclared != 0 || b.svc.adopting.unfinished() != 0 {
		t.Fatalf("holder undeclared %d, unfinished %d; want 0, 0", st.TakeoverHolderUndeclared, b.svc.adopting.unfinished())
	}
}

// TestAdoptionWithACopyIsServedDespiteAnUndeclaredHolderDown: on the adoption
// path too, a copy found anywhere is served at once, whatever holder is down.
func TestAdoptionWithACopyIsServedDespiteAnUndeclaredHolderDown(t *testing.T) {
	holdWorkerBack(t)
	fixtureFallback = peerFetch(replicaOf("x"))
	t.Cleanup(func() { fixtureFallback = nil })
	downHolder(t)
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	lead := c.settled()
	stopOwner := undeclaredOwner(t, lead, "x")
	aliveNode(t, lead, "n8")
	ownedBy(t, lead, "x", "n9")
	stopOwner()
	ownedBy(t, lead, "x", lead.name)
	if lead.svc.table.Live("n9") || !lead.svc.table.Live("n8") {
		t.Fatalf("precondition: n9 live %v, n8 live %v; want the owner declared and the holder alive", lead.svc.table.Live("n9"), lead.svc.table.Live("n8"))
	}
	res, err := acquire(t, lead, "x", nil, 300, false)
	if err != nil || !hasFilter(res.State, "t/replica") || res.Empty.Reason != "" {
		t.Fatalf("adoption with a copy and a holder down: state %v, evidence %+v, err %v; want the copy", res.State, res.Empty, err)
	}
	if st := lead.svc.LivenessStats(); st.AdoptHolderUndeclared != 0 || lead.svc.adopting.unfinished() != 0 {
		t.Fatalf("holder waits %d, unfinished %d; want 0, 0", st.AdoptHolderUndeclared, lead.svc.adopting.unfinished())
	}
}

// TestDeclaredHolderDownIsNotWaitedFor: a holder the leader has declared dead
// holds nothing back.
func TestDeclaredHolderDownIsNotWaitedFor(t *testing.T) {
	_, b, stop := holderCluster(t, foundNowhere, nil)
	stop()
	declared(t, b, "n8")
	res, err := acquire(t, b, "x", nil, 300, false)
	wantEmpty(t, "takeover with a declared holder down", res, err, EmptyTakeover, "n1")
	if st := b.svc.LivenessStats(); st.TakeoverHolderUndeclared != 0 || st.TakeoverEmpty != 1 {
		t.Fatalf("holder undeclared %d, empty %d; want 0, 1", st.TakeoverHolderUndeclared, st.TakeoverEmpty)
	}
}

// holderAdoption is holderCluster with the worker running on opts, after the
// takeover that records the adoption.
func holderAdoption(t *testing.T, opts func(o *Options)) (b *tnode, stop func()) {
	t.Helper()
	_, b, stop = holderCluster(t, foundNowhere, opts)
	if _, err := acquire(t, b, "x", nil, 300, false); !refusedBusy(err) {
		t.Fatalf("precondition not reached: the takeover was not refused (%v)", err)
	}
	return b, stop
}

// TestWorkerWaitsForAnUndeclaredHoldersDeclaration: its retries spent, the
// worker keeps the adoption while the holder is undeclared, and ends it once
// the holder is declared.
func TestWorkerWaitsForAnUndeclaredHoldersDeclaration(t *testing.T) {
	b, stop := holderAdoption(t, func(o *Options) {
		o.AdoptRetries, o.AdoptRetryDelay, o.SealTimeout = 1, 5*time.Millisecond, time.Minute
	})
	waitFor(t, "the worker to fetch past its retries", 10*time.Second, func() bool {
		return b.svc.LivenessStats().AdoptHolderUndeclared >= 4
	})
	if st := b.svc.LivenessStats(); st.AdoptGaveUp != 0 || !b.svc.adopting.unfinishedFor(key{"default", "x"}) || !b.svc.table.Live("n8") {
		t.Fatalf("after %d fetches with retries 1: gave up %d, unfinished %v, n8 live %v; want the adoption kept while n8 is live", st.AdoptHolderUndeclared, st.AdoptGaveUp, b.svc.adopting.unfinishedFor(key{"default", "x"}), b.svc.table.Live("n8"))
	}
	stop()
	declared(t, b, "n8")
	waitFor(t, "the worker to end the adoption", 15*time.Second, func() bool { return b.svc.adopting.unfinished() == 0 })
	if st := b.svc.LivenessStats(); st.AdoptWorkerEmpty != 1 || st.AdoptGaveUp != 0 {
		t.Fatalf("after the declaration: worker empty %d, gave up %d; want 1, 0", st.AdoptWorkerEmpty, st.AdoptGaveUp)
	}
}

// TestWorkerKeepsWaitingOnAnUndeclaredHolderPastTheBound: a holder that stays
// down and alive by the log may come back with its copy, so neither the
// retries nor the bound end the wait; the wait past the bound is counted
// once, and the holder's declaration ends it.
func TestWorkerKeepsWaitingOnAnUndeclaredHolderPastTheBound(t *testing.T) {
	b, stop := holderAdoption(t, func(o *Options) {
		o.AdoptRetries, o.AdoptRetryDelay, o.SealTimeout = 1, 5*time.Millisecond, 300*time.Millisecond
	})
	began := time.Now()
	waitFor(t, "six holder waits and twice the bound", 10*time.Second, func() bool {
		return b.svc.LivenessStats().AdoptHolderUndeclared >= 6 && time.Since(began) > 600*time.Millisecond
	})
	st := b.svc.LivenessStats()
	if st.AdoptGaveUp != 0 || !b.svc.adopting.unfinishedFor(key{"default", "x"}) || !b.svc.table.Live("n8") {
		t.Fatalf("after %d holder waits, past the bound with retries 1: gave up %d, unfinished %v, n8 live %v; want the adoption kept while n8 is live",
			st.AdoptHolderUndeclared, st.AdoptGaveUp, b.svc.adopting.unfinishedFor(key{"default", "x"}), b.svc.table.Live("n8"))
	}
	if st.AdoptWaitsPastBound != 1 || st.AdoptWaitingNow != 1 {
		t.Fatalf("waits past the bound %d, waiting now %d; want 1, 1", st.AdoptWaitsPastBound, st.AdoptWaitingNow)
	}
	stop()
	declared(t, b, "n8")
	waitFor(t, "the worker to end the adoption", 15*time.Second, func() bool { return b.svc.adopting.unfinished() == 0 })
	if st := b.svc.LivenessStats(); st.AdoptWorkerEmpty != 1 || st.AdoptGaveUp != 0 || st.AdoptWaitingNow != 0 || st.AdoptWaitsPastBound != 1 {
		t.Fatalf("after the declaration: worker empty %d, gave up %d, waiting now %d, waits past the bound %d; want 1, 0, 0, 1",
			st.AdoptWorkerEmpty, st.AdoptGaveUp, st.AdoptWaitingNow, st.AdoptWaitsPastBound)
	}
}

// partialView stands in for this node's transport, reporting the peers named
// in hide as a restarted peer looks before its data connection is up: not Up,
// with the control connection in place unless noControl. Frames still go
// through the real transport.
type partialView struct {
	Transport
	mu        sync.Mutex
	hide      map[string]bool
	noControl bool
}

func (v *partialView) Peers() []transport.PeerInfo {
	ps := v.Transport.Peers()
	v.mu.Lock()
	defer v.mu.Unlock()
	for i := range ps {
		if v.hide[ps[i].Name] {
			ps[i].State, ps[i].DataState, ps[i].ControlUp = transport.Connecting, transport.Down, !v.noControl
		}
	}
	return ps
}

func (v *partialView) set(peer string, noControl bool) {
	v.mu.Lock()
	v.hide[peer], v.noControl = true, noControl
	v.mu.Unlock()
}

// declaredOwnerCluster is three nodes with every node's transport behind a
// partialView and the adoption worker held back. A non-leader owns x and holds
// its session; its renewals are then dropped until the leader declares it dead
// and adopts x. The owner keeps answering pulls.
func declaredOwnerCluster(t *testing.T) (lead, owner *tnode, views map[string]*partialView) {
	t.Helper()
	views = map[string]*partialView{}
	var mu sync.Mutex
	fixtureOptions = func(o *Options) {
		o.AdoptionDelay, o.AdoptRetryDelay = time.Hour, time.Hour
		v := &partialView{Transport: o.Transport, hide: map[string]bool{}}
		mu.Lock()
		views[o.Node] = v
		mu.Unlock()
		o.Transport = v
	}
	t.Cleanup(func() { fixtureOptions = nil })
	fixtureFallback = foundNowhere
	t.Cleanup(func() { fixtureFallback = nil })
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	lead = c.settled()
	for _, name := range c.names {
		if name != lead.name {
			owner = c.nodes[name]
			break
		}
	}
	res, err := acquire(t, owner, "x", nil, 300, true)
	if err != nil {
		t.Fatal(err)
	}
	owner.sessions.put("default", "x", &store.SessionState{SessionMeta: store.SessionMeta{Namespace: "default", ClientID: "x", Epoch: res.Epoch}, Subscriptions: []store.SubscriptionRecord{{Filter: "t/owner", QoS: 1}}})
	for _, name := range c.names {
		if name != owner.name {
			owner.tr.SetFault(name, &transport.Fault{Drop: 1, Classes: []transport.Class{transport.LeaseAlive}, Rand: rand.New(rand.NewSource(1))})
		}
	}
	ownedBy(t, lead, "x", lead.name)
	if lead.svc.table.Live(owner.name) {
		t.Fatalf("precondition: %s is not declared dead on the leader", owner.name)
	}
	return lead, owner, views
}

// TestDeclaredOwnerReachableOnItsControlConnectionIsPulled is WI 1765 (b): the
// previous owner was declared dead but can be reached again — its control
// connection is up and its data connection is not yet. It is pulled, and its
// session served, rather than counted as holding nothing.
func TestDeclaredOwnerReachableOnItsControlConnectionIsPulled(t *testing.T) {
	lead, owner, views := declaredOwnerCluster(t)
	views[lead.name].set(owner.name, false)
	res, err := acquire(t, lead, "x", nil, 300, false)
	if err != nil || !hasFilter(res.State, "t/owner") {
		t.Fatalf("adoption from a declared owner reachable on its control connection: state %v, evidence %+v, err %v; want the owner's session", res.State, res.Empty, err)
	}
	if st := lead.svc.LivenessStats(); st.PullsControlOnly < 1 {
		t.Fatalf("pulls on the control connection alone: %d, want at least 1", st.PullsControlOnly)
	}
	if lead.svc.table.Live(owner.name) {
		t.Fatal("the owner's declaration was cleared before the fetch was judged: the case was not reached")
	}
}

// TestDeclaredOwnerOutOfReachIsNotWaitedFor: with no connection to it at all,
// a declared-dead owner holds nothing back.
func TestDeclaredOwnerOutOfReachIsNotWaitedFor(t *testing.T) {
	lead, owner, views := declaredOwnerCluster(t)
	views[lead.name].set(owner.name, true)
	res, err := acquire(t, lead, "x", nil, 300, false)
	wantEmpty(t, "adoption from a declared owner out of reach", res, err, EmptyAdoption, owner.name)
	if st := lead.svc.LivenessStats(); st.PullsControlOnly != 0 {
		t.Fatalf("pulls on the control connection alone: %d, want 0", st.PullsControlOnly)
	}
}

type confirmSeen struct {
	ns, id, from string
	token        uint64
}

// TestConfirmationReachesTheHoldersTheOwnerDoesNotFeed is WI 1765 (c)'s
// signal: a new owner's confirmation goes to its previous owner and to every
// holder it does not replicate to, once, and each receiver passes it on.
func TestConfirmationReachesTheHoldersTheOwnerDoesNotFeed(t *testing.T) {
	var mu sync.Mutex
	seen := map[string][]confirmSeen{}
	fixtureOptions = func(o *Options) {
		o.AdoptionDelay, o.AdoptRetryDelay = time.Hour, time.Hour
		node := o.Node
		o.Unfed = func(string, string) []string { return []string{"n2", "n3", node} }
		o.Confirmed = func(ns, id, from string, token uint64) {
			mu.Lock()
			seen[node] = append(seen[node], confirmSeen{ns, id, from, token})
			mu.Unlock()
		}
	}
	t.Cleanup(func() { fixtureOptions = nil })
	fixtureFallback = foundNowhere
	t.Cleanup(func() { fixtureFallback = nil })
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	a, b := c.nodes["n1"], c.nodes["n2"]
	if _, err := acquire(t, b, "x", nil, 300, true); err != nil {
		t.Fatal(err)
	}
	b.sessions.put("default", "x", sessionFor("x"))
	res, err := acquire(t, a, "x", nil, 300, false)
	if err != nil || res.State == nil || res.PrevOwner != "n2" {
		t.Fatalf("the takeover: %+v, %v", res, err)
	}
	st := res.State
	st.Epoch = res.Epoch
	a.sessions.Import(st)
	want := confirmSeen{"default", "x", "n1", res.Epoch}
	waitFor(t, "the giver and the holder to receive the confirmation", 5*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen["n2"]) >= 1 && len(seen["n3"]) >= 1
	})
	mu.Lock()
	got2, got3, got1 := seen["n2"], seen["n3"], seen["n1"]
	mu.Unlock()
	if len(got2) != 1 || got2[0] != want || len(got3) != 1 || got3[0] != want || len(got1) != 0 {
		t.Fatalf("confirmations passed on: n1 %v, n2 %v, n3 %v; want %v once each on n2 and n3", got1, got2, got3, want)
	}
	waitFor(t, "the requester to count its sends", 5*time.Second, func() bool { return a.svc.LivenessStats().ConfirmsSentHolders >= 1 })
	if ls := a.svc.LivenessStats(); ls.ConfirmsSent != 2 || ls.ConfirmsSentHolders != 1 {
		t.Fatalf("sent %d, to holders %d; want 2, 1 (the giver is not sent to twice, nor this node)", ls.ConfirmsSent, ls.ConfirmsSentHolders)
	}
}

// TestConfirmationToANodeThatHoldsNothingIsIgnored: a holder of an older build
// has no hook; a confirmation for a session it neither sealed nor holds
// changes nothing there.
func TestConfirmationToANodeThatHoldsNothingIsIgnored(t *testing.T) {
	c := newCluster(t, []string{"n1"}, 2*time.Second, nil)
	n := c.nodes["n1"]
	n.svc.serveConfirmed(key{"default", "x"}, []any{confirmIn{token: 40, peer: "n9"}})
	if ls := n.svc.LivenessStats(); ls.ConfirmsIgnored != 1 || ls.SealsConfirmed != 0 || ls.ReclaimedOnConfirm != 0 {
		t.Fatalf("counters %+v", ls)
	}
}

// TestConfirmationToAnUnreachableMemberIsCountedAndTheRestSent: a member the
// owner names that it cannot reach is counted unsent, and the others it names
// are still told.
func TestConfirmationToAnUnreachableMemberIsCountedAndTheRestSent(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]int{}
	fixtureOptions = func(o *Options) {
		o.AdoptionDelay, o.AdoptRetryDelay = time.Hour, time.Hour
		node := o.Node
		o.Unfed = func(string, string) []string { return []string{"n9", "n3"} }
		o.Confirmed = func(string, string, string, uint64) {
			mu.Lock()
			seen[node]++
			mu.Unlock()
		}
	}
	t.Cleanup(func() { fixtureOptions = nil })
	fixtureFallback = foundNowhere
	t.Cleanup(func() { fixtureFallback = nil })
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	a, b := c.nodes["n1"], c.nodes["n2"]
	if _, err := acquire(t, b, "x", nil, 300, true); err != nil {
		t.Fatal(err)
	}
	b.sessions.put("default", "x", sessionFor("x"))
	res, err := acquire(t, a, "x", nil, 300, false)
	if err != nil || res.State == nil || res.PrevOwner != "n2" {
		t.Fatalf("the takeover: %+v, %v", res, err)
	}
	st := res.State
	st.Epoch = res.Epoch
	a.sessions.Import(st)
	waitFor(t, "the giver and n3 to receive the confirmation", 5*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return seen["n2"] >= 1 && seen["n3"] >= 1
	})
	waitFor(t, "the unreachable member to be counted", 5*time.Second, func() bool { return a.svc.LivenessStats().ConfirmsUnsent >= 1 })
	if ls := a.svc.LivenessStats(); ls.ConfirmsUnsent != 1 || ls.ConfirmsSent != 2 || ls.ConfirmsSentHolders != 1 {
		t.Fatalf("unsent %d, sent %d, to unfed %d; want 1, 2, 1", ls.ConfirmsUnsent, ls.ConfirmsSent, ls.ConfirmsSentHolders)
	}
}

// TestDownHolderOutranksASilentSource: a replica source stays silent while
// the only holder is down and alive by the log. The holder may hold the copy
// the owner fed, so the wait is on it and the source's retries do not end it.
// Once the holder is declared, only the silent source is left, and it is
// given up after its whole retries, counted from the first fetch that waited
// on it alone.
func TestDownHolderOutranksASilentSource(t *testing.T) {
	_, b, stop := holderCluster(t, func(context.Context, string, string, string) (*store.SessionState, bool) { return nil, false }, func(o *Options) {
		o.AdoptRetries, o.AdoptRetryDelay, o.SealTimeout = 1, 5*time.Millisecond, time.Minute
	})
	if _, err := acquire(t, b, "x", nil, 300, false); !refusedBusy(err) {
		t.Fatalf("precondition not reached: the takeover was not refused (%v)", err)
	}
	waitFor(t, "the worker to fetch past its retries, or give up", 10*time.Second, func() bool {
		st := b.svc.LivenessStats()
		return st.AdoptWorkerUnanswered >= 4 || st.AdoptGaveUp >= 1
	})
	if st := b.svc.LivenessStats(); st.AdoptGaveUp != 0 || st.AdoptHolderUndeclared < 4 || !b.svc.adopting.unfinishedFor(key{"default", "x"}) {
		t.Fatalf("after %d fetches with a silent source, n8 down and retries 1: gave up %d, holder waits %d, unfinished %v; want the adoption kept for n8",
			st.AdoptWorkerUnanswered, st.AdoptGaveUp, st.AdoptHolderUndeclared, b.svc.adopting.unfinishedFor(key{"default", "x"}))
	}
	stop()
	declared(t, b, "n8")
	waitFor(t, "the worker to give the adoption up", 30*time.Second, func() bool { return b.svc.LivenessStats().AdoptGaveUp >= 1 })
	st := b.svc.LivenessStats()
	if silent := st.AdoptWorkerUnanswered - st.AdoptHolderUndeclared; silent != 2 {
		t.Fatalf("given up after %d fetches that waited on the silent source alone (%d unanswered, %d on n8); want 2, the retries plus one",
			silent, st.AdoptWorkerUnanswered, st.AdoptHolderUndeclared)
	}
}

// TestRefusalPastADownHoldersBoundNamesTheLeader: a CONNECT refused for an
// adoption waiting on a down holder alive by the log is told Server Busy until
// the adoption is older than SealTimeout, then Use Another Server naming the
// leader; on the leader itself it stays Server Busy. Either way the adoption
// stays.
func TestRefusalPastADownHoldersBoundNamesTheLeader(t *testing.T) {
	fixtureOptions = func(o *Options) {
		o.AdoptRetryDelay, o.SealTimeout = time.Hour, 500*time.Millisecond
	}
	t.Cleanup(func() { fixtureOptions = nil })
	fixtureFallback = foundNowhere
	t.Cleanup(func() { fixtureFallback = nil })
	downHolder(t)
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, func(name string) string { return "client." + name + ":1883" })
	leader := ""
	waitFor(t, "every node to know the leader", 10*time.Second, func() bool {
		leader, _ = c.nodes["n1"].svc.leaderHint.Load().(string)
		for _, n := range c.nodes {
			if h, _ := n.svc.leaderHint.Load().(string); h == "" || h != leader {
				return false
			}
		}
		return leader != ""
	})
	var follower, giver string
	for _, n := range c.names {
		switch {
		case n == leader:
		case follower == "":
			follower = n
		default:
			giver = n
		}
	}
	g := c.nodes[giver]
	aliveNode(t, g, "n8")
	for _, cs := range []struct {
		node, id string
		want     packet.ReasonCode
	}{{follower, "x", packet.UseAnotherServer}, {leader, "y", packet.ServerBusy}} {
		// The giver holds nothing and answers the pull, so only the down
		// holder holds the wait.
		if _, err := acquire(t, g, cs.id, nil, 300, true); err != nil {
			t.Fatal(err)
		}
		n := c.nodes[cs.node]
		ownedBy(t, n, cs.id, giver)
		waitFor(t, cs.node+"'s table to show n8 alive", 5*time.Second, func() bool { return n.svc.table.Live("n8") })
		began := time.Now()
		if _, err := acquire(t, n, cs.id, nil, 300, false); !refusedBusy(err) {
			t.Fatalf("%s: precondition not reached: the takeover was not refused (%v)", cs.node, err)
		}
		if _, err := acquire(t, n, cs.id, nil, 300, false); time.Since(began) < 500*time.Millisecond && !refusedBusy(err) {
			t.Fatalf("%s: a retry before the bound: %v, want Server Busy", cs.node, err)
		}
		time.Sleep(time.Until(began.Add(700 * time.Millisecond)))
		_, err := acquire(t, n, cs.id, nil, 300, false)
		var r *Refusal
		if !errors.As(err, &r) || r.Reason != cs.want || (cs.want == packet.UseAnotherServer && r.ServerReference != "client."+leader+":1883") {
			t.Fatalf("%s (leader %s): a retry past the bound: %v, want %s", cs.node, leader, err, cs.want)
		}
		if !n.svc.adopting.unfinishedFor(key{"default", cs.id}) {
			t.Fatalf("%s: the adoption ended with the refusal", cs.node)
		}
		if st := n.svc.LivenessStats(); st.AdoptWaitsPastBound != 1 || st.AdoptHolderUndeclared < 1 || (cs.want == packet.UseAnotherServer) != (st.AdoptRedirected == 1) {
			t.Fatalf("%s: waits past the bound %d, holder waits %d, redirected %d", cs.node, st.AdoptWaitsPastBound, st.AdoptHolderUndeclared, st.AdoptRedirected)
		}
	}
}
