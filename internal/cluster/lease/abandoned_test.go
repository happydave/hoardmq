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

// TestAbandonedGrantIsFetchedFromTheReplica: the acquire commits after its
// CONNECT's context ended, the previous owner then crashes, and the client's
// next CONNECT on the node now holding its lease is served the replica's copy
// rather than a fresh session.
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

func TestAbandonedSource(t *testing.T) {
	for _, c := range []struct {
		name    string
		prev    string
		granted uint64
		from    string
		current uint64
		want    string
	}{
		{"the grant at the current token, the lookup saw this node", "n1", 7, "n2", 7, "n2"},
		{"the grant at the current token, the lookup saw a third node", "n3", 7, "n2", 7, "n2"},
		{"a grant below the current token", "n3", 5, "n2", 7, "n3"},
		{"no grant recorded", "n3", 0, "", 7, "n3"},
		{"no grant recorded, the lookup saw this node", "n1", 0, "", 7, "n1"},
	} {
		if got := abandonedSource(c.prev, c.granted, c.from, c.current); got != c.want {
			t.Errorf("%s: abandonedSource = %q, want %q", c.name, got, c.want)
		}
	}
}

// TestGrantRecordedForAnAbandonedAcquire: the grant that moves a lease here is
// recorded on the key whether the CONNECT that asked for it gives up before
// the grant applies or after, and on no key nothing here is waiting for.
func TestGrantRecordedForAnAbandonedAcquire(t *testing.T) {
	c := newCluster(t, []string{"n1"}, 2*time.Second, nil)
	s := c.nodes["n1"].svc
	lease := func(id, owner string, epoch uint64, online bool) *Lease {
		return &Lease{Namespace: "default", ClientID: id, Owner: owner, Epoch: epoch, Online: online}
	}
	entry := func(id string) (abandonedAcquire, bool) {
		s.abandoned.mu.Lock()
		defer s.abandoned.mu.Unlock()
		e, ok := s.abandoned.m[key{"default", id}]
		return e, ok
	}
	now := time.Now()

	// Gave up first: the note names this node, as a stale lookup does.
	s.noteAbandoned(key{"default", "before"}, "n1")
	s.changed(lease("before", "n2", 5, true), lease("before", "n1", 7, true), now)
	if e, _ := entry("before"); !e.noted || e.prev != "n1" || e.granted != 7 || e.from != "n2" {
		t.Errorf("note then grant: %+v; want the note kept and the grant 7 from n2", e)
	}

	// Granted while the CONNECT was still in flight, then given up.
	k := key{"default", "after"}
	s.connecting.begin(k)
	s.changed(lease("after", "n2", 5, true), lease("after", "n1", 7, true), now)
	if e, ok := entry("after"); !ok || e.noted || e.granted != 7 || e.from != "n2" {
		t.Errorf("grant in flight: %+v, %v; want the grant 7 from n2 and no note", e, ok)
	}
	if _, taken := s.abandoned.takeNoted(k); taken {
		t.Error("a grant with no note was taken as a note")
	}
	s.noteAbandoned(k, "n1")
	s.connecting.end(k)
	e, _ := entry("after")
	if !e.noted || e.granted != 7 || e.from != "n2" || abandonedSource(e.prev, e.granted, e.from, 7) != "n2" {
		t.Errorf("grant then note: %+v; want the note joined to the grant 7 from n2", e)
	}

	// Nothing here waits on this client.
	s.changed(lease("idle", "n2", 5, true), lease("idle", "n1", 7, true), now)
	if e, ok := entry("idle"); ok {
		t.Errorf("a grant with no note and no CONNECT in flight was recorded: %+v", e)
	}
}

// TestAbandonedSweepDropsGrantsWithoutANote: a grant recorded with no note
// goes a ttl after it was written, whether or not the lease is here; a note
// is kept as before.
func TestAbandonedSweepDropsGrantsWithoutANote(t *testing.T) {
	a := newAbandonedAcquires()
	now := time.Unix(1000, 0)
	ttl := time.Minute
	kGrant, kNote := key{"d", "grant"}, key{"d", "note"}
	a.grant(kGrant, 7, "n2", true, now, ttl)
	a.note(kNote, "n2", now, ttl)
	a.sweep(now.Add(ttl+time.Second), ttl, func(key) bool { return true })
	if _, ok := a.take(kGrant); ok {
		t.Error("an old grant with no note was kept")
	}
	if _, ok := a.take(kNote); !ok {
		t.Error("an old note whose lease is here was dropped")
	}
	if a.grant(key{"d", "nobody"}, 7, "n2", false, now, ttl); a.len() != 0 {
		t.Error("a grant nothing waits on was recorded")
	}
}

// TestAdoptionNamingThisNodeIsRefused: an abandoned note that names this node,
// with no grant recorded, leaves the adoption no previous owner to account
// for. The client is refused Server Busy and the adoption kept, however often
// it retries, rather than served a fresh session from the replicas' "not
// found".
func TestAdoptionNamingThisNodeIsRefused(t *testing.T) {
	holdWorkerBack(t)
	fixtureFallback = func(context.Context, string, string, string) (*store.SessionState, bool) { return nil, true }
	t.Cleanup(func() { fixtureFallback = nil })
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	a := c.nodes["n1"]
	if _, err := acquire(t, a, "x", nil, 300, false); err != nil {
		t.Fatal(err)
	}
	ownedBy(t, a, "x", "n1")
	a.svc.noteAbandoned(key{"default", "x"}, "n1")
	for i := 0; i < 2; i++ {
		res, err := acquire(t, a, "x", nil, 300, false)
		if !refusedBusy(err) {
			t.Fatalf("CONNECT %d: state %v, evidence %+v, err %v; want Server Busy", i+1, res.State != nil, res.Empty, err)
		}
		if !a.svc.adopting.unfinishedFor(key{"default", "x"}) {
			t.Fatalf("CONNECT %d: the adoption was not kept", i+1)
		}
	}
	if n := a.svc.LivenessStats().AdoptOwnerUnknown; n < 2 {
		t.Fatalf("adopt_owner_unknown %d, want at least 2", n)
	}
}
