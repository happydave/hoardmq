package lease

import (
	"context"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/store"
)

// refusedImport is one history of a session whose import a later grant
// refused, and the nodes in it.
type refusedImport struct {
	p, a, b *tnode
	t1, t2  uint64
	// first is the CONNECT holding the fetched state, when a CONNECT fetched
	// it; release lets the worker's held import run, when the worker did.
	first   acquireOutcome
	release func()
}

// refusedImportCluster is three nodes whose replicas hold nothing for x. p
// owns x and holds its session with a subscription only it has ("t/live").
// a takes the lease from p at t1 while p's answer to a's pull is lost, so p
// seals the session and a keeps a takeover adoption that waits on p. Then the
// adoption is fetched on a, from p's sealed copy: inline by a CONNECT left
// unsettled (byWorker false), or by the adoption worker, whose import is held
// (byWorker true), once the note a's own takeover left naming p has lapsed.
// b then takes the lease from a at t2 and pulls a; with
// answerLost, a's answer to b is lost, so b refuses the client and keeps an
// adoption that waits on a. On return a has seen t2 and has not yet decided
// the import; the state is in a's hands and in p's sealed copy, nowhere else.
func refusedImportCluster(t *testing.T, byWorker, answerLost bool) *refusedImport {
	t.Helper()
	x := key{"default", "x"}
	gate, entered := make(chan struct{}), make(chan struct{}, 1)
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release)
	if byWorker {
		fixtureOptions = func(o *Options) {
			o.AdoptionDelay, o.AdoptRetryDelay = 0, time.Hour
			o.ImportGate = func(_, id string) {
				if id == "x" {
					entered <- struct{}{}
					<-gate
				}
			}
		}
		t.Cleanup(func() { fixtureOptions = nil })
	} else {
		holdWorkerBack(t)
	}
	fixtureFallback = func(context.Context, string, string, string) (*store.SessionState, bool) { return nil, true }
	t.Cleanup(func() { fixtureFallback = nil })
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	r := &refusedImport{p: c.settled(), release: release}
	var others []string
	for name, n := range c.nodes {
		if n != r.p {
			others = append(others, name)
		}
	}
	sort.Strings(others)
	r.a, r.b = c.nodes[others[0]], c.nodes[others[1]]
	p, a, b := r.p, r.a, r.b
	rnd := rand.New(rand.NewSource(1))

	if _, err := acquire(t, p, "x", nil, 300, false); err != nil {
		t.Fatal(err)
	}
	live := sessionFor("x")
	live.Subscriptions = []store.SubscriptionRecord{{Filter: "t/live", QoS: 1}}
	p.sessions.put("default", "x", live)
	ownedBy(t, a, "x", p.name)

	// a takes the lease; p seals the session and its answer is lost.
	p.tr.SetFault(a.name, &transport.Fault{Drop: 1, Classes: []transport.Class{transport.TakeoverAck}, Rand: rnd})
	if _, _, err := connectAcquire(a, "x"); !refusedBusy(err) {
		t.Fatalf("precondition: a's takeover with p's answer lost: %v; want Server Busy", err)
	}
	p.tr.SetFault(a.name, nil)
	l, _ := a.svc.table.Lookup("default", "x")
	if l.Owner != a.name || !a.svc.adopting.unfinishedFor(x) {
		t.Fatalf("precondition: a owns x %v and keeps its adoption %v", l.Owner == a.name, a.svc.adopting.unfinishedFor(x))
	}
	r.t1 = l.Epoch
	if _, _, sealed := p.sessions.Sealed("default", "x"); !sealed {
		t.Fatal("precondition: p did not seal the session for a's pull")
	}
	ownedBy(t, b, "x", a.name)
	// a's takeover noted p, for a pull a answers with nothing, for the seal
	// timeout. The adoption is fetched after that note has lapsed, as one whose
	// previous owner answered late is.
	waitFor(t, "a's takeover note naming p to lapse", 3*a.svc.o.SealTimeout, func() bool {
		o, _ := a.svc.waitNotes.owner(x, a.svc.steady(), a.svc.o.SealTimeout)
		return o == ""
	})

	// The state is fetched on a from p's sealed copy, and held there.
	if byWorker {
		a.svc.adopting.mu.Lock()
		job := a.svc.adopting.m[x].job
		a.svc.adopting.mu.Unlock()
		a.svc.adopts.enqueue(x, job)
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("precondition: a's worker did not reach the import with p's state")
		}
	} else {
		r.first = outcomeOf(t, acquireUnsettled(a, "x", 300))
		if r.first.err != nil || !r.first.res.Local || !hasFilter(r.first.res.State, "t/live") || r.first.res.Epoch != r.t1 {
			t.Fatalf("precondition: a's CONNECT %+v (err %v); want p's session fetched inline at t1 %d", r.first.res, r.first.err, r.t1)
		}
	}

	// b takes the lease from a and pulls it.
	if answerLost {
		a.tr.SetFault(b.name, &transport.Fault{Drop: 1, Classes: []transport.Class{transport.TakeoverAck}, Rand: rnd})
	}
	_, _, err := connectAcquire(b, "x")
	if !refusedBusy(err) {
		t.Fatalf("precondition: b's takeover from a, which holds no session: %v; want Server Busy", err)
	}
	a.tr.SetFault(b.name, nil)
	// b's acquire can return before b's own table has applied it.
	ownedBy(t, b, "x", b.name)
	lb, _ := b.svc.table.Lookup("default", "x")
	r.t2 = lb.Epoch
	if lb.Owner != b.name || r.t2 <= r.t1 || !b.svc.adopting.unfinishedFor(x) {
		t.Fatalf("precondition: b owns x %v at %d (t1 %d) and keeps an adoption %v", lb.Owner == b.name, r.t2, r.t1, b.svc.adopting.unfinishedFor(x))
	}
	if seen := a.svc.tokens.Seen("default", "x"); seen < r.t2 {
		t.Fatalf("precondition: a has seen %d for x; want b's t2 %d from b's pull", seen, r.t2)
	}
	return r
}

// refuse decides a's held import: refused, since a has seen t2.
func (r *refusedImport) refuse(t *testing.T, byWorker bool) {
	t.Helper()
	if byWorker {
		refusedBefore := r.a.svc.adoptImportRefused.Load()
		r.release()
		waitFor(t, "a's worker to refuse its import", 5*time.Second, func() bool { return r.a.svc.adoptImportRefused.Load() > refusedBefore })
		waitFor(t, "a's adoption to end", 5*time.Second, func() bool { return !r.a.svc.adopting.unfinishedFor(key{"default", "x"}) })
		return
	}
	refuseFirstImport(t, r.a, r.first)
}

// judgeAfterRefusal checks what a keeps for the later owner once its import is
// refused: an entry holding its served fact until the new owner's fact or
// cover reaches t2 (a hand-on), and p named to a pull that finds nothing here.
func (r *refusedImport) judgeAfterRefusal(t *testing.T, handedBefore int64) {
	t.Helper()
	ts := r.a.svc.TransitStats()
	_, unfinished := r.a.svc.AdoptionState()
	pending := r.a.svc.waitingOn(key{"default", "x"})
	fact, _ := r.a.svc.table.ServedFact(r.b.name)
	cover, _ := r.a.svc.table.Cover(r.b.name)
	t.Logf("t1 %d, t2 %d; after a's refused import: a handed on %d (before %d), open %d, adoptions unfinished %d; a names %q to a pull; b's served fact %d, cover %d",
		r.t1, r.t2, ts.Handed, handedBefore, ts.Open, unfinished, pending, fact, cover)
	if ts.Handed != handedBefore+1 || unfinished == 0 {
		t.Errorf("a's refused import recorded %d hand-ons and leaves %d adoptions unfinished while b's fact %d and cover %d are below t2 %d; want one hand-on holding a's served fact", ts.Handed-handedBefore, unfinished, fact, cover, r.t2)
	}
	if pending != r.p.name {
		t.Errorf("a names %q to a pull for x after its refused import; want %s, the owner its adoption was fetching from", pending, r.p.name)
	}
}

// judgeLaterOwner lets the client retry on b until it is served: it must be
// served p's session, never one without it while p holds it sealed.
func (r *refusedImport) judgeLaterOwner(t *testing.T) {
	t.Helper()
	for i := 1; i <= 4; i++ {
		res, err := acquire(t, r.b, "x", nil, 300, false)
		_, _, sealed := r.p.sessions.Sealed("default", "x")
		t.Logf("b's CONNECT %d: err %v, local %v, token %d, state with t/live %v, evidence %+v; p holds a sealed copy %v", i, err, res.Local, res.Epoch, hasFilter(res.State, "t/live"), res.Empty, sealed)
		switch {
		case refusedBusy(err):
			continue
		case err != nil:
			t.Fatalf("b's CONNECT %d: %v", i, err)
		case !hasFilter(res.State, "t/live"):
			t.Fatalf("b served x at %d without p's session (evidence %+v) while p holds it sealed %v", res.Epoch, res.Empty, sealed)
		}
		return
	}
	t.Fatal("b refused x's CONNECT four times; want p's session served")
}

// TestLaterOwnerAnsweredBeforeARefusedConnectImport: b's pull reaches a while
// a's CONNECT holds p's state, so a's answer names p. Then the CONNECT's
// import is refused on t2.
func TestLaterOwnerAnsweredBeforeARefusedConnectImport(t *testing.T) {
	r := refusedImportCluster(t, false, false)
	handed := r.a.svc.TransitStats().Handed
	r.refuse(t, false)
	r.judgeAfterRefusal(t, handed)
	r.judgeLaterOwner(t)
}

// TestLaterOwnerAnsweredAfterARefusedConnectImport: b's first pull of a is
// lost, so b asks a again only after a's CONNECT's import was refused.
func TestLaterOwnerAnsweredAfterARefusedConnectImport(t *testing.T) {
	r := refusedImportCluster(t, false, true)
	handed := r.a.svc.TransitStats().Handed
	r.refuse(t, false)
	r.judgeAfterRefusal(t, handed)
	r.judgeLaterOwner(t)
}

// TestLaterOwnerAnsweredAfterARefusedWorkerImport is the same order with the
// adoption worker's import refused in place of a CONNECT's.
func TestLaterOwnerAnsweredAfterARefusedWorkerImport(t *testing.T) {
	r := refusedImportCluster(t, true, true)
	handed := r.a.svc.TransitStats().Handed
	r.refuse(t, true)
	r.judgeAfterRefusal(t, handed)
	r.judgeLaterOwner(t)
}

// TestRefusedTakeoverImportNamesItsPreviousOwner: a CONNECT on a takes x from
// p, which hands it the session; no adoption is involved. Once the note a's
// takeover left has lapsed, a later grant is seen and the import refused. A
// pull that then finds nothing on a is told of p, and nothing is handed on.
func TestRefusedTakeoverImportNamesItsPreviousOwner(t *testing.T) {
	holdWorkerBack(t)
	x := key{"default", "x"}
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	p := c.settled()
	var a *tnode
	for _, n := range c.nodes {
		if n != p {
			a = n
			break
		}
	}
	if _, err := acquire(t, p, "x", nil, 300, false); err != nil {
		t.Fatal(err)
	}
	live := sessionFor("x")
	live.Subscriptions = []store.SubscriptionRecord{{Filter: "t/live", QoS: 1}}
	p.sessions.put("default", "x", live)
	ownedBy(t, a, "x", p.name)
	o := outcomeOf(t, acquireUnsettled(a, "x", 300))
	if o.err != nil || o.res.PrevOwner != p.name || !hasFilter(o.res.State, "t/live") {
		o.res.Settle()
		t.Fatalf("precondition: a's takeover %+v (err %v); want p's session pulled from p", o.res, o.err)
	}
	waitFor(t, "a's takeover note naming p to lapse", 3*a.svc.o.SealTimeout, func() bool {
		owner, _ := a.svc.waitNotes.owner(x, a.svc.steady(), a.svc.o.SealTimeout)
		return owner == ""
	})
	handed := a.svc.TransitStats().Handed
	a.svc.tokens.Note("default", "x", o.res.Epoch+1)
	if a.svc.AdmitImport("default", "x", o.res.Epoch)() {
		o.res.Settle()
		t.Fatal("precondition: a admitted the import after seeing a later grant")
	}
	a.svc.NoteImportRefused("default", "x", o.res.Epoch, o.res.PrevOwner)
	o.res.Settle()
	named := a.svc.waitingOn(x)
	t.Logf("token %d, previous owner %q; after the refusal a names %q to a pull; hand-ons %d (before %d)", o.res.Epoch, o.res.PrevOwner, named, a.svc.TransitStats().Handed, handed)
	if named != p.name {
		t.Errorf("a names %q to a pull for x after its refused takeover import; want %s", named, p.name)
	}
	if got := a.svc.TransitStats().Handed; got != handed {
		t.Errorf("a's refused takeover import recorded %d hand-ons; want none, it held no adoption", got-handed)
	}
}

func TestRefusedImportRecords(t *testing.T) {
	for _, c := range []struct {
		name                      string
		adopting                  bool
		adoptionPrev, connectPrev string
		owner                     string
		handOn                    bool
	}{
		{"an adoption open, both owners set", true, "p", "q", "p", true},
		{"an adoption open, no CONNECT owner", true, "p", "", "p", true},
		{"no adoption, a CONNECT owner", false, "", "q", "q", false},
		{"neither", false, "", "", "", false},
	} {
		owner, handOn := refusedImportRecords(c.adopting, c.adoptionPrev, c.connectPrev)
		if owner != c.owner || handOn != c.handOn {
			t.Errorf("%s: refusedImportRecords = %q, %v; want %q, %v", c.name, owner, handOn, c.owner, c.handOn)
		}
	}
}
