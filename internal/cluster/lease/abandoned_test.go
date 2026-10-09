package lease

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/store"
)

// abandonedGrant is three nodes whose replica fallback is fb. n1 owns x and
// holds its live session; n2's CONNECT gives up on its acquire before the
// acquire is proposed, and the acquire is granted afterwards, from n1, with
// nobody waiting for it. The adoption worker is held back, so a CONNECT is
// the fetcher.
func abandonedGrant(t *testing.T, fb func(context.Context, string, string, string) (*store.SessionState, bool)) (c *tcluster, a, b *tnode) {
	t.Helper()
	holdWorkerBack(t)
	fixtureFallback = fb
	t.Cleanup(func() { fixtureFallback = nil })
	c = newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	a, b = c.nodes["n1"], c.nodes["n2"]
	if _, err := acquire(t, a, "x", nil, 300, true); err != nil {
		t.Fatal(err)
	}
	live := sessionFor("x")
	live.Subscriptions = []store.SubscriptionRecord{{Filter: "t/live", QoS: 1}}
	a.sessions.put("default", "x", live)
	ownedBy(t, b, "x", "n1")
	giveUp(t, b, "x")
	ownedBy(t, b, "x", "n2")
	return c, a, b
}

// giveUp acquires on a context that has already ended: the CONNECT is refused
// and its acquire stays queued, to be proposed and granted afterwards.
func giveUp(t *testing.T, n *tnode, id string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := n.svc.Acquire(ctx, "default", id, nil, 300, false); err == nil {
		t.Fatalf("precondition: %s's acquire of %s on an ended context was not refused", n.name, id)
	}
}

func hasFilter(st *store.SessionState, filter string) bool {
	if st == nil {
		return false
	}
	for _, s := range st.Subscriptions {
		if s.Filter == filter {
			return true
		}
	}
	return false
}

// TestAbandonedGrantIsFetchedFromTheReplica is WI 1740's acceptance test. The
// acquire commits after its CONNECT's context ended, the previous owner then
// crashes, and the client's next CONNECT on the node now holding its lease is
// served the replica's copy rather than a fresh session.
func TestAbandonedGrantIsFetchedFromTheReplica(t *testing.T) {
	replica := replicaOf("x")
	c, a, b := abandonedGrant(t, peerFetch(replica))
	c.stopNode(a)
	waitFor(t, "n2 to see n1 down", 5*time.Second, func() bool { ask, _ := b.svc.askable("n1"); return !ask })
	res, err := acquire(t, b, "x", nil, 300, false)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if !hasFilter(res.State, "t/replica") || res.Empty.Reason != "" {
		t.Fatalf("retry on the node the abandoned acquire made owner: state %v, evidence %+v; want the replica's copy and no evidence", res.State, res.Empty)
	}
	if n := b.svc.LivenessStats().GrantsUnfetchedAdopted; n != 1 {
		t.Fatalf("grants_unfetched_adopted %d, want 1", n)
	}
}

// TestAbandonedGrantIsPulledFromALiveOwner: the previous owner is alive and
// still holds the session it was never asked for.
func TestAbandonedGrantIsPulledFromALiveOwner(t *testing.T) {
	_, _, b := abandonedGrant(t, nil)
	res, err := acquire(t, b, "x", nil, 300, false)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if !hasFilter(res.State, "t/live") || res.Empty.Reason != "" {
		t.Fatalf("retry: state %v, evidence %+v; want n1's live session", res.State, res.Empty)
	}
}

// TestAbandonedGrantWithASessionHeldFetchesNothing: what this node holds is
// what the client is served from; nothing is fetched to put over it.
func TestAbandonedGrantWithASessionHeldFetchesNothing(t *testing.T) {
	var calls atomic.Int64
	_, _, b := abandonedGrant(t, func(context.Context, string, string, string) (*store.SessionState, bool) {
		calls.Add(1)
		return replicaOf("x"), true
	})
	b.sessions.put("default", "x", sessionFor("x"))
	imported := b.sessions.importedCount()
	res, err := acquire(t, b, "x", nil, 300, false)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if res.State != nil || calls.Load() != 0 || b.sessions.importedCount() != imported || b.svc.LivenessStats().GrantsUnfetchedAdopted != 0 {
		t.Fatalf("held session: state %v, fallback calls %d, imports %d→%d, adopted %d; want nothing fetched", res.State != nil, calls.Load(), imported, b.sessions.importedCount(), b.svc.LivenessStats().GrantsUnfetchedAdopted)
	}
}

// TestAbandonedGrantReachedThroughTheRacedBranch: the retry's lookup still saw
// the previous owner, so its own acquire applied after the abandoned one with
// no previous owner. It is constructed per client until the branch is reached.
func TestAbandonedGrantReachedThroughTheRacedBranch(t *testing.T) {
	holdWorkerBack(t)
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	a, b := c.nodes["n1"], c.nodes["n2"]
	for i := 0; i < 20; i++ {
		id := "r" + string(rune('a'+i))
		if _, err := acquire(t, a, id, nil, 300, true); err != nil {
			t.Fatal(err)
		}
		live := sessionFor(id)
		live.Subscriptions = []store.SubscriptionRecord{{Filter: "t/live", QoS: 1}}
		a.sessions.put("default", id, live)
		ownedBy(t, b, id, "n1")
		before := b.svc.LivenessStats().GrantsUnfetchedAdopted
		giveUp(t, b, id)
		res, err := acquire(t, b, id, nil, 300, false)
		if err == nil && res.Local {
			continue // the grant was in n2's table first: the local path, not this test's
		}
		if b.svc.LivenessStats().GrantsUnfetchedAdopted != before+1 {
			t.Fatalf("%s: the raced branch did not adopt the abandoned grant: state %v, evidence %+v, err %v", id, res.State != nil, res.Empty, err)
		}
		t.Logf("raced branch reached at client %d; first CONNECT refused: %v", i, err != nil)
		if err != nil {
			if !refusedBusy(err) {
				t.Fatalf("%s: raced branch: %v", id, err)
			}
			// n2's table had not applied its own grant yet: the adoption was
			// kept for this retry.
			ownedBy(t, b, id, "n2")
			res, err = acquire(t, b, id, nil, 300, false)
		}
		if err != nil || !hasFilter(res.State, "t/live") || res.Empty.Reason != "" {
			t.Fatalf("%s: state %v, evidence %+v, err %v; want n1's live session", id, res.State, res.Empty, err)
		}
		return
	}
	t.Fatal("precondition not reached in 20 clients: every retry found the abandoned grant already in n2's table")
}

// TestAbandonedGrantWithASilentOwnerIsRefused: the previous owner is asked and
// does not answer, and every replica answers "not found". The owner may still
// hand the session over, so the client is refused and the adoption kept.
func TestAbandonedGrantWithASilentOwnerIsRefused(t *testing.T) {
	_, a, b := abandonedGrant(t, func(context.Context, string, string, string) (*store.SessionState, bool) { return nil, true })
	silence(t, a)
	res, _, err := connectAcquire(b, "x")
	if !refusedBusy(err) {
		t.Fatalf("retry with the owner silent and no replica: %+v, %v; want Server Busy", res, err)
	}
	if !b.svc.adopting.unfinishedFor(key{"default", "x"}) {
		t.Fatal("the adoption was not kept for the client's next retry")
	}
}

// TestAbandonedGrantFoundNowhereIsServedWithItsEvidence: the previous owner
// has crashed and every replica answered "not found". Until the leader
// declares it dead it may still hold the session, so the client is refused and
// the adoption kept; after the declaration it is served fresh, with evidence.
func TestAbandonedGrantFoundNowhereIsServedWithItsEvidence(t *testing.T) {
	c, a, b := abandonedGrant(t, func(context.Context, string, string, string) (*store.SessionState, bool) { return nil, true })
	c.stopNode(a)
	waitFor(t, "n2 to see n1 down", 5*time.Second, func() bool { ask, _ := b.svc.askable("n1"); return !ask })
	res, err := acquire(t, b, "x", nil, 300, false)
	if !refusedBusy(err) {
		if !b.svc.table.Live("n1") {
			t.Fatal("precondition not reached: n1 was declared dead before the retry was judged")
		}
		t.Fatalf("retry before n1's declaration, found nowhere: %+v, %v; want Server Busy", res, err)
	}
	if !b.svc.adopting.unfinishedFor(key{"default", "x"}) || b.svc.LivenessStats().AdoptOwnerUndeclared != 1 {
		t.Fatalf("adoption kept %v, adopt_owner_undeclared %d; want the adoption kept and 1", b.svc.adopting.unfinishedFor(key{"default", "x"}), b.svc.LivenessStats().AdoptOwnerUndeclared)
	}
	declared(t, b, "n1")
	res, err = acquire(t, b, "x", nil, 300, false)
	wantEmpty(t, "found nowhere after the declaration", res, err, EmptyAdoption, "n1")
}

// TestTakeoverJobAheadOfTheTableIsUnanswered: a job recorded from this node's
// own acquire result, checked before its table has applied that grant, is
// retried rather than read as a lease that moved on. A job the table itself
// recorded has no such excuse.
func TestTakeoverJobAheadOfTheTableIsUnanswered(t *testing.T) {
	c := newCluster(t, []string{"n1"}, 2*time.Second, nil)
	s := c.nodes["n1"].svc
	k := key{"default", "ahead"}
	s.tokens.Note(k.ns, k.id, 999)
	for _, tc := range []struct {
		takeover bool
		want     fetchResult
	}{{true, fetchUnanswered}, {false, fetchMoved}} {
		ad := &adoption{job: adoptJob{epoch: 999, takeover: tc.takeover}, done: make(chan struct{})}
		if _, got, _, _ := s.fetchAdopted(context.Background(), k, ad, false); got != tc.want {
			t.Errorf("takeover=%v: fetch %d, want %d", tc.takeover, got, tc.want)
		}
	}
}

func TestKeepAbandoned(t *testing.T) {
	ttl := 30 * time.Second
	for _, tc := range []struct {
		age  time.Duration
		here bool
		want bool
	}{
		{time.Second, false, true},
		{time.Second, true, true},
		{ttl, true, true},
		{ttl, false, false},
		{time.Hour, false, false},
	} {
		if got := keepAbandoned(tc.age, ttl, tc.here); got != tc.want {
			t.Errorf("age %v, lease here %v: keep %v, want %v", tc.age, tc.here, got, tc.want)
		}
	}
}

func TestAbandonedSweep(t *testing.T) {
	a := newAbandonedAcquires()
	now := time.Unix(1000, 0)
	ttl := time.Minute
	kHere, kGone, kYoung := key{"d", "here"}, key{"d", "gone"}, key{"d", "young"}
	a.note(kHere, "n1", now, ttl)
	a.note(kGone, "n1", now, ttl)
	a.note(kYoung, "n1", now.Add(ttl), ttl)
	a.sweep(now.Add(ttl+time.Second), ttl, func(k key) bool { return k == kHere })
	if _, ok := a.take(kGone); ok {
		t.Error("an old note whose lease is not here was kept")
	}
	if _, ok := a.take(kHere); !ok {
		t.Error("an old note whose lease is here was dropped")
	}
	if _, ok := a.take(kYoung); !ok {
		t.Error("a young note was dropped")
	}
}
