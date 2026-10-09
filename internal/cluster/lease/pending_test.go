package lease

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

func TestWaitNoteKept(t *testing.T) {
	bound := time.Minute
	for _, c := range []struct {
		age  time.Duration
		want bool
	}{{0, true}, {bound - time.Millisecond, true}, {bound, false}, {time.Hour, false}} {
		if got := waitNoteKept(c.age, bound); got != c.want {
			t.Errorf("waitNoteKept(%v, %v) = %v, want %v", c.age, bound, got, c.want)
		}
	}
}

func TestWaitNotes(t *testing.T) {
	bound := time.Minute
	t0 := time.Unix(1000, 0)
	w := newWaitNotes()
	x, y := key{"default", "x"}, key{"default", "y"}
	w.note(x, "n9", t0, bound, false)
	if got, _ := w.owner(x, t0.Add(bound/2), bound); got != "n9" {
		t.Fatalf("a young note: owner %q, want n9", got)
	}
	if got, _ := w.owner(x, t0.Add(bound/2), bound); got != "n9" {
		t.Fatalf("a note read twice: owner %q, want n9 (reading does not remove it)", got)
	}
	if got, _ := w.owner(x, t0.Add(bound), bound); got != "" {
		t.Fatalf("a note at the bound: owner %q, want none", got)
	}
	w.note(y, "n8", t0.Add(bound+time.Second), bound, false)
	if n := w.len(); n != 1 {
		t.Fatalf("after a note past the bound: %d notes held, want the expired one swept", n)
	}
}

// TestWaitNoteKind: a later note replaces an earlier one for the key, kind and
// all, so the kind read is that of the last writer.
func TestWaitNoteKind(t *testing.T) {
	bound := time.Minute
	t0 := time.Unix(1000, 0)
	w := newWaitNotes()
	x := key{"default", "x"}
	w.note(x, "n9", t0, bound, false)
	w.note(x, "n7", t0, bound, true)
	if got, takeover := w.owner(x, t0, bound); got != "n7" || !takeover {
		t.Fatalf("a takeover's note over an ended adoption's: owner %q, takeover %v; want n7, true", got, takeover)
	}
	w.note(x, "n6", t0, bound, false)
	if got, takeover := w.owner(x, t0, bound); got != "n6" || takeover {
		t.Fatalf("an ended adoption's note over a takeover's: owner %q, takeover %v; want n6, false", got, takeover)
	}
}

// chainCluster is three nodes whose replica fallback is fb, with x owned by
// n9 (undeclaredOwner) and n2 having refused the client for it (WI 1759). n3's
// table shows n2 as x's owner: the client's retry on n3 takes the lease from
// the node that refused it. opts adjusts each node's options.
func chainCluster(t *testing.T, fb func(context.Context, string, string, string) (*store.SessionState, bool), opts func(o *Options)) (c *tcluster, n2, n3 *tnode, stop func()) {
	t.Helper()
	fixtureFallback = fb
	t.Cleanup(func() { fixtureFallback = nil })
	fixtureOptions = opts
	t.Cleanup(func() { fixtureOptions = nil })
	c = newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	n2, n3 = c.nodes["n2"], c.nodes["n3"]
	stop = undeclaredOwner(t, c.nodes["n1"], "x")
	ownedBy(t, n2, "x", "n9")
	if _, err := acquire(t, n2, "x", nil, 300, false); !refusedBusy(err) {
		t.Fatalf("precondition not reached: n2 did not refuse the client for n9 (%v)", err)
	}
	ownedBy(t, n3, "x", "n2")
	return c, n2, n3, stop
}

func heldBack(o *Options) { o.AdoptionDelay, o.AdoptRetryDelay = time.Hour, time.Hour }

func foundNowhere(context.Context, string, string, string) (*store.SessionState, bool) {
	return nil, true
}

func adoptionOf(n *tnode, id string) *adoption {
	n.svc.adopting.mu.Lock()
	defer n.svc.adopting.mu.Unlock()
	return n.svc.adopting.m[key{"default", id}]
}

func adoptionPrev(n *tnode, id string) string {
	n.svc.adopting.mu.Lock()
	defer n.svc.adopting.mu.Unlock()
	if ad := n.svc.adopting.m[key{"default", id}]; ad != nil {
		return ad.job.prev
	}
	return ""
}

// TestRetryOnAThirdNodeWaitsForTheOwnerTheRefusingNodeNamed is WI 1762's
// acceptance test. The client, refused on n2 for an owner not yet declared
// dead, retries on n3, which takes the lease from n2. n2 holds nothing and
// names the owner it is waiting on; with no copy anywhere, n3 refuses the client
// until that owner is declared dead, and then serves it fresh.
func TestRetryOnAThirdNodeWaitsForTheOwnerTheRefusingNodeNamed(t *testing.T) {
	_, n2, n3, stop := chainCluster(t, foundNowhere, heldBack)
	res, err := acquire(t, n3, "x", nil, 300, false)
	if !n3.svc.table.Live("n9") {
		t.Fatal("precondition not reached: n9 was declared dead before n3's takeover was judged")
	}
	if !refusedBusy(err) {
		t.Fatalf("retry on the third node, n9 undeclared, found nowhere: %+v, %v; want Server Busy", res, err)
	}
	if ad := adoptionOf(n3, "x"); ad == nil || ad.job.prev != "n9" || !ad.job.takeover {
		t.Fatalf("n3's adoption %+v; want one kept with n9 as its previous owner", ad)
	}
	if g, r := n2.svc.LivenessStats(), n3.svc.LivenessStats(); g.TakeoverAnsweredPending != 1 || r.TakeoverPendingOwner != 1 || r.TakeoverEmpty != 0 || r.TakeoverUnanswered != 1 {
		t.Fatalf("counters: n2 answered pending %d; n3 pending owner %d, empty %d, unanswered %d; want 1, 1, 0, 1", g.TakeoverAnsweredPending, r.TakeoverPendingOwner, r.TakeoverEmpty, r.TakeoverUnanswered)
	}
	ownedBy(t, n3, "x", "n3")
	if _, err := acquire(t, n3, "x", nil, 300, false); !refusedBusy(err) || !n3.svc.table.Live("n9") {
		t.Fatalf("retry on n3 before the declaration: %v, n9 live %v; want Server Busy while n9 is live", err, n3.svc.table.Live("n9"))
	}
	stop()
	declared(t, n3, "n9")
	res, err = acquire(t, n3, "x", nil, 300, false)
	wantEmpty(t, "retry on n3 after the declaration", res, err, EmptyAdoption, "n9")
	if n := n3.svc.adopting.unfinished(); n != 0 {
		t.Fatalf("%d adoptions unfinished on n3 after the fresh session was served", n)
	}
}

// TestNamedOwnerSurvivesAWorkerThatEndsTheAdoptionFirst: n2's adoption worker
// finds the lease moved and ends the adoption before n3's pull is served. The
// pass names the owner from the note the ended adoption left.
func TestNamedOwnerSurvivesAWorkerThatEndsTheAdoptionFirst(t *testing.T) {
	_, n2, n3, _ := chainCluster(t, foundNowhere, func(o *Options) {
		o.TakeoverTimeout = 4 * time.Second
		if o.Node != "n2" {
			heldBack(o)
			return
		}
		o.AdoptRetryDelay, o.AdoptRetries, o.SealTimeout = 20*time.Millisecond, 1000, time.Minute
		o.TakeoverDelay = 2 * time.Second
	})
	type result struct {
		res AcquireResult
		err error
	}
	done := make(chan result, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		r, err := n3.svc.Acquire(ctx, "default", "x", nil, 300, false)
		done <- result{r, err}
	}()
	k := key{"default", "x"}
	waitFor(t, "n2's worker to end its adoption as moved", 1800*time.Millisecond, func() bool {
		return n2.svc.LivenessStats().AdoptWorkerMoved >= 1 && !n2.svc.adopting.unfinishedFor(k)
	})
	select {
	case r := <-done:
		t.Fatalf("precondition not reached: n3's acquire returned (%+v, %v) before n2's worker ended the adoption", r.res, r.err)
	default:
	}
	r := <-done
	if !refusedBusy(r.err) {
		t.Fatalf("retry on the third node after n2's adoption ended: %+v, %v; want Server Busy", r.res, r.err)
	}
	if prev := adoptionPrev(n3, "x"); prev != "n9" {
		t.Fatalf("n3's adoption names %q; want n9", prev)
	}
	if st := n2.svc.LivenessStats(); st.TakeoverAnsweredPending != 1 || st.TakeoverAnsweredPendingNoted != 1 || st.PendingOwnerNotes != 1 {
		t.Fatalf("n2 answered pending %d, from a note %d, notes held %d; want 1, 1, 1", st.TakeoverAnsweredPending, st.TakeoverAnsweredPendingNoted, st.PendingOwnerNotes)
	}
}

// TestAdoptionFetchTakesTheWaitOverFromANamedOwner: n3's first pull of n2 goes
// unanswered, so n3 refuses and keeps an adoption naming n2 (WI 1574). Its
// retry pulls n2, which names n9: the adoption waits on n9 from then on, and is
// served fresh only after n9's declaration.
func TestAdoptionFetchTakesTheWaitOverFromANamedOwner(t *testing.T) {
	_, n3, stop := silentChain(t, foundNowhere)
	if _, err := acquire(t, n3, "x", nil, 300, false); !refusedBusy(err) || !n3.svc.table.Live("n9") {
		t.Fatalf("retry on n3, n2 naming n9: %v, n9 live %v; want Server Busy while n9 is live", err, n3.svc.table.Live("n9"))
	}
	if prev, st := adoptionPrev(n3, "x"), n3.svc.LivenessStats(); prev != "n9" || st.AdoptPendingOwner != 1 {
		t.Fatalf("after the retry: n3's adoption names %q, pending owner %d; want n9, 1", prev, st.AdoptPendingOwner)
	}
	stop()
	declared(t, n3, "n9")
	res, err := acquire(t, n3, "x", nil, 300, false)
	wantEmpty(t, "retry on n3 after the declaration", res, err, EmptyAdoption, "n9")
}

// TestNamedOwnerDeclaredDeadHoldsNothingBack: n9 is declared dead before the
// client retries on n3. n2 still names it, and n3, whose table shows the
// declaration, serves the client fresh at once.
func TestNamedOwnerDeclaredDeadHoldsNothingBack(t *testing.T) {
	_, n2, n3, stop := chainCluster(t, foundNowhere, heldBack)
	stop()
	declared(t, n3, "n9")
	res, err := acquire(t, n3, "x", nil, 300, false)
	wantEmpty(t, "retry on n3 after n9's declaration", res, err, EmptyTakeover, "n2")
	if g, r := n2.svc.LivenessStats(), n3.svc.LivenessStats(); g.TakeoverAnsweredPending != 1 || r.TakeoverPendingOwner != 0 {
		t.Fatalf("counters: n2 answered pending %d, n3 pending owner %d; want 1 (n9 named), 0 (n9 declared)", g.TakeoverAnsweredPending, r.TakeoverPendingOwner)
	}
}

// silentChain is chainCluster with n3's first takeover left unanswered: n2's
// answers to n3 are dropped, so n3 refuses and keeps an adoption naming n2
// (WI 1574). The fault is cleared and n3's table shows its own grant.
func silentChain(t *testing.T, fb func(context.Context, string, string, string) (*store.SessionState, bool)) (n2, n3 *tnode, stop func()) {
	t.Helper()
	_, n2, n3, stop = chainCluster(t, fb, heldBack)
	n2.tr.SetFault("n3", &transport.Fault{Drop: 1, Classes: []transport.Class{transport.TakeoverAck}, Rand: rand.New(rand.NewSource(1))})
	if _, err := acquire(t, n3, "x", nil, 300, false); !refusedBusy(err) {
		t.Fatalf("precondition not reached: n3's takeover with n2's answer dropped was not refused (%v)", err)
	}
	if prev := adoptionPrev(n3, "x"); prev != "n2" || n3.svc.LivenessStats().TakeoverOwnerSilent != 1 {
		t.Fatalf("precondition not reached: n3's adoption names %q, silent %d; want n2, 1", prev, n3.svc.LivenessStats().TakeoverOwnerSilent)
	}
	n2.tr.SetFault("n3", nil)
	ownedBy(t, n3, "x", "n3")
	return n2, n3, stop
}

// TestAdoptionFetchWithANamedOwnerDeclaredDeadServesFresh: the fetch's side of
// TestNamedOwnerDeclaredDeadHoldsNothingBack.
func TestAdoptionFetchWithANamedOwnerDeclaredDeadServesFresh(t *testing.T) {
	_, n3, stop := silentChain(t, foundNowhere)
	stop()
	declared(t, n3, "n9")
	res, err := acquire(t, n3, "x", nil, 300, false)
	wantEmpty(t, "retry on n3 after n9's declaration", res, err, EmptyAdoption, "n2")
	if st := n3.svc.LivenessStats(); st.AdoptPendingOwner != 0 {
		t.Fatalf("n3 pending owner %d; want 0 (n9 declared)", st.AdoptPendingOwner)
	}
}

// TestAdoptionFetchWithACopyIsServedDespiteANamedOwner: the fetch's side of
// TestThirdNodeWithACopyIsServedAtOnce.
func TestAdoptionFetchWithACopyIsServedDespiteANamedOwner(t *testing.T) {
	var copyNow atomic.Bool
	_, n3, _ := silentChain(t, func(context.Context, string, string, string) (*store.SessionState, bool) {
		if copyNow.Load() {
			return replicaOf("x"), true
		}
		return nil, true
	})
	copyNow.Store(true)
	res, err := acquire(t, n3, "x", nil, 300, false)
	if err != nil || !hasFilter(res.State, "t/replica") || res.Empty.Reason != "" {
		t.Fatalf("retry on n3 with a replica copy, n2 naming n9: state %v, evidence %+v, err %v; want the copy", res.State, res.Empty, err)
	}
	if st := n3.svc.LivenessStats(); st.AdoptPendingOwner != 0 || n3.svc.adopting.unfinished() != 0 {
		t.Fatalf("n3 pending owner %d, unfinished %d; want 0, 0", st.AdoptPendingOwner, n3.svc.adopting.unfinished())
	}
}

// TestThirdNodeWithACopyIsServedAtOnce: a copy found anywhere is served without
// waiting for the owner the refusing node named.
func TestThirdNodeWithACopyIsServedAtOnce(t *testing.T) {
	var copyNow atomic.Bool
	_, _, n3, _ := chainCluster(t, func(context.Context, string, string, string) (*store.SessionState, bool) {
		if copyNow.Load() {
			return replicaOf("x"), true
		}
		return nil, true
	}, heldBack)
	copyNow.Store(true)
	res, err := acquire(t, n3, "x", nil, 300, false)
	if err != nil || !hasFilter(res.State, "t/replica") || res.Empty.Reason != "" {
		t.Fatalf("retry on the third node with a replica copy: state %v, evidence %+v, err %v; want the copy", res.State, res.Empty, err)
	}
	if st := n3.svc.LivenessStats(); st.TakeoverPendingOwner != 1 || st.TakeoverPendingOwnerServed != 1 || n3.svc.adopting.unfinished() != 0 {
		t.Fatalf("n3: pending owner %d, served %d, unfinished %d; want 1, 1, 0", st.TakeoverPendingOwner, st.TakeoverPendingOwnerServed, n3.svc.adopting.unfinished())
	}
}

// TestNewRequesterGivenAnAnswerWithoutAPendingOwnerJudgesAsBefore: n2 answers
// as a build before this rule does. n3 finds no owner named and serves the
// client fresh, as WI 1759's build does: a mixed pair behaves as its older
// member.
func TestNewRequesterGivenAnAnswerWithoutAPendingOwnerJudgesAsBefore(t *testing.T) {
	_, n2, n3, _ := chainCluster(t, foundNowhere, func(o *Options) {
		heldBack(o)
		o.OmitPendingOwner = o.Node == "n2"
	})
	res, err := acquire(t, n3, "x", nil, 300, false)
	wantEmpty(t, "retry on n3 with n2 on the older answer", res, err, EmptyTakeover, "n2")
	if g, r := n2.svc.LivenessStats(), n3.svc.LivenessStats(); g.TakeoverAnsweredPending != 0 || r.TakeoverPendingOwner != 0 {
		t.Fatalf("counters: n2 answered pending %d, n3 pending owner %d; want 0, 0", g.TakeoverAnsweredPending, r.TakeoverPendingOwner)
	}
}

// takeoverAckBefore1762 is the takeover answer as builds before WI 1762
// declare and decode it.
type takeoverAckBefore1762 struct {
	Req        uint64              `json:"req"`
	State      *store.SessionState `json:"state,omitempty"`
	Superseded bool                `json:"superseded,omitempty"`
}

// TestTakeoverAnswerDecodesInAnOlderBuild: an older build decodes an answer
// naming a pending owner to exactly what it decodes from the answer it would
// have been sent, and this build sends that same answer when it names nobody.
func TestTakeoverAnswerDecodesInAnOlderBuild(t *testing.T) {
	named, _ := json.Marshal(takeoverAck{Req: 7, Pending: "n9"})
	plain, _ := json.Marshal(takeoverAck{Req: 7})
	older, _ := json.Marshal(takeoverAckBefore1762{Req: 7})
	if !bytes.Contains(named, []byte(`"pending":"n9"`)) {
		t.Fatalf("the answer naming n9 does not carry it: %s", named)
	}
	if !bytes.Equal(plain, older) {
		t.Fatalf("an answer naming nobody is %s; an older build sends %s", plain, older)
	}
	var fromNamed, fromPlain takeoverAckBefore1762
	if err := json.Unmarshal(named, &fromNamed); err != nil {
		t.Fatalf("an older build fails to decode %s: %v", named, err)
	}
	if err := json.Unmarshal(older, &fromPlain); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fromNamed, fromPlain) || fromNamed.Req != 7 {
		t.Fatalf("an older build decodes %+v from the named answer and %+v from its own; want the same", fromNamed, fromPlain)
	}
	var fromOlder takeoverAck
	if err := json.Unmarshal(older, &fromOlder); err != nil || fromOlder.Pending != "" || fromOlder.Req != 7 {
		t.Fatalf("this build decodes %+v, %v from an older answer; want req 7 and no owner named", fromOlder, err)
	}
}

// TestPendingOwnerRidesTheControlLane: an answer that took the control lane
// (marker byte, then the same JSON) reaches the waiting pull with its owner.
func TestPendingOwnerRidesTheControlLane(t *testing.T) {
	s := &Service{pulls: map[uint64]chan takeoverAck{}}
	ch := make(chan takeoverAck, 1)
	s.pulls[7] = ch
	body, _ := json.Marshal(takeoverAck{Req: 7, Pending: "n9"})
	s.receiveTakeoverRequest("n2", append([]byte{ackOnControl}, body...))
	select {
	case a := <-ch:
		if a.Pending != "n9" {
			t.Fatalf("the pull received %+v; want n9 named", a)
		}
	default:
		t.Fatal("the answer on the control lane did not reach the pull")
	}
}

// TestNamedOwnerFetchPastTheBoundRedirects: n3's adoption names n2, whose
// answers to it were lost, and is older than SealTimeout when the client
// retries on n3. That retry's pull is answered: n2 names n9. The fetch that
// takes the wait over is a wait on n9 like any other, so the refusal past the
// bound names the leader, or is Server Busy on the leader itself.
func TestNamedOwnerFetchPastTheBoundRedirects(t *testing.T) {
	fixtureFallback = foundNowhere
	t.Cleanup(func() { fixtureFallback = nil })
	fixtureOptions = func(o *Options) {
		o.AdoptionDelay, o.AdoptRetryDelay, o.SealTimeout = time.Hour, time.Hour, 300*time.Millisecond
	}
	t.Cleanup(func() { fixtureOptions = nil })
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, func(name string) string { return "client." + name + ":1883" })
	n2, n3 := c.nodes["n2"], c.nodes["n3"]
	undeclaredOwner(t, c.nodes["n1"], "x")
	ownedBy(t, n2, "x", "n9")
	if _, err := acquire(t, n2, "x", nil, 300, false); !refusedBusy(err) {
		t.Fatalf("precondition not reached: n2 did not refuse the client for n9 (%v)", err)
	}
	ownedBy(t, n3, "x", "n2")
	n2.tr.SetFault("n3", &transport.Fault{Drop: 1, Classes: []transport.Class{transport.TakeoverAck}, Rand: rand.New(rand.NewSource(1))})
	if _, err := acquire(t, n3, "x", nil, 300, false); !refusedBusy(err) || adoptionPrev(n3, "x") != "n2" {
		t.Fatalf("precondition not reached: n3's takeover with n2's answer dropped: %v, adoption names %q", err, adoptionPrev(n3, "x"))
	}
	recorded := time.Now() // the refused takeover recorded the adoption before it returned
	n2.tr.SetFault("n3", nil)
	ownedBy(t, n3, "x", "n3")
	leader := ""
	waitFor(t, "n3 to know the leader", 10*time.Second, func() bool {
		leader, _ = n3.svc.leaderHint.Load().(string)
		return leader != ""
	})
	time.Sleep(time.Until(recorded.Add(500 * time.Millisecond)))
	_, err := acquire(t, n3, "x", nil, 300, false)
	want, ref := packet.UseAnotherServer, "client."+leader+":1883"
	if leader == "n3" {
		want, ref = packet.ServerBusy, ""
	}
	var r *Refusal
	if !errors.As(err, &r) || r.Reason != want || r.ServerReference != ref || adoptionPrev(n3, "x") != "n9" {
		t.Fatalf("retry on n3 past the bound, n2 naming n9 (leader %s): %v, adoption names %q; want %s %q naming n9", leader, err, adoptionPrev(n3, "x"), want, ref)
	}
}
