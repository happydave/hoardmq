package lease

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/store"
)

// takenByAbandonedFirst is three nodes whose replica fallback is fb, with every
// adoption worker held back. n2's first acquire of x is held before its
// proposal on an ended context; meanwhile x is granted on n3, which holds its
// session (t/live). n2's acquire is then released, refused, and left queued: it
// applies after, taking the lease from n3. With settle, the call returns once
// n2's table shows n2 as the owner; without, at once.
func takenByAbandonedFirst(t *testing.T, fb func(context.Context, string, string, string) (*store.SessionState, bool), settle bool) (c *tcluster, n2, n3 *tnode) {
	t.Helper()
	hold := newAcquireHold(false)
	fixtureFallback = fb
	t.Cleanup(func() { fixtureFallback = nil })
	fixtureOptions = func(o *Options) {
		heldBack(o)
		if o.Node == "n2" {
			o.AcquireGate = hold.gate
		}
	}
	t.Cleanup(func() { fixtureOptions = nil })
	c = newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	t.Cleanup(hold.unblock)
	n2, n3 = c.nodes["n2"], c.nodes["n3"]
	hold.armed.Store(true)
	gaveUp := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := n2.svc.Acquire(ctx, "default", "x", nil, 300, false)
		gaveUp <- err
	}()
	hold.wait(t, "n2's first acquire of x held before its proposal")
	res, err := acquire(t, n3, "x", nil, 300, false)
	if err != nil {
		t.Fatal(err)
	}
	live := sessionFor("x")
	live.Subscriptions = []store.SubscriptionRecord{{Filter: "t/live", QoS: 1}}
	live.Epoch = res.Epoch
	n3.sessions.put("default", "x", live)
	ownedBy(t, n2, "x", "n3")
	hold.unblock()
	if err := <-gaveUp; err == nil {
		t.Fatal("precondition: n2's acquire on an ended context was not refused")
	}
	if settle {
		ownedBy(t, n2, "x", "n2")
	}
	return c, n2, n3
}

// TestAFirstAcquireThatTookALiveOwnersLeaseIsFetchedFromIt is WI 2206's
// acceptance test: the client's next CONNECT on n2 gets the session n3 holds.
func TestAFirstAcquireThatTookALiveOwnersLeaseIsFetchedFromIt(t *testing.T) {
	_, n2, _ := takenByAbandonedFirst(t, nil, true)
	res, err := acquire(t, n2, "x", nil, 300, false)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if !hasFilter(res.State, "t/live") || res.Empty.Reason != "" {
		t.Fatalf("retry on n2: state %v, evidence %+v; want n3's session and no evidence", res.State, res.Empty)
	}
	st := n2.svc.LivenessStats()
	if st.FirstGrantsTakenAdopted != 1 || st.FirstAcquireMarks != 0 {
		t.Fatalf("first grants taken adopted %d, marks %d; want 1 and 0", st.FirstGrantsTakenAdopted, st.FirstAcquireMarks)
	}
}

// TestAFirstAcquireThatTookACrashedOwnersLeaseIsFetchedFromTheReplica: n3 has
// crashed and a replica holds a copy.
func TestAFirstAcquireThatTookACrashedOwnersLeaseIsFetchedFromTheReplica(t *testing.T) {
	c, n2, n3 := takenByAbandonedFirst(t, peerFetch(replicaOf("x")), true)
	c.stopNode(n3)
	waitFor(t, "n2 to see n3 down", 5*time.Second, func() bool { ask, _ := n2.svc.askable("n3"); return !ask })
	res, err := acquire(t, n2, "x", nil, 300, false)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if !hasFilter(res.State, "t/replica") || res.Empty.Reason != "" {
		t.Fatalf("retry on n2: state %v, evidence %+v; want the replica's copy and no evidence", res.State, res.Empty)
	}
}

// TestAFirstAcquireThatTookTheLeaseIsFetchedOnTheRacedBranch: the retry's
// lookup still shows n3, so its acquire is proposed after the abandoned one
// and applies with no previous owner.
func TestAFirstAcquireThatTookTheLeaseIsFetchedOnTheRacedBranch(t *testing.T) {
	_, n2, _ := takenByAbandonedFirst(t, nil, false)
	if l, _ := n2.svc.table.Lookup("default", "x"); l.Owner != "n3" {
		t.Fatalf("precondition: n2's table already shows %s before the retry", l.Owner)
	}
	res, err := acquire(t, n2, "x", nil, 300, false)
	if err != nil {
		if !refusedBusy(err) {
			t.Fatalf("retry: %v", err)
		}
		// n2's table had not applied its own grant: the adoption is kept for
		// the client's next CONNECT.
		t.Log("first CONNECT refused while n2's table was behind its grant")
		ownedBy(t, n2, "x", "n2")
		res, err = acquire(t, n2, "x", nil, 300, false)
	}
	if err != nil {
		t.Fatalf("retry after the refusal: %v", err)
	}
	if !hasFilter(res.State, "t/live") || res.Empty.Reason != "" {
		t.Fatalf("retry on n2: state %v, evidence %+v; want n3's session and no evidence", res.State, res.Empty)
	}
	if got := n2.svc.LivenessStats().FirstGrantsTakenAdopted; got != 1 {
		t.Fatalf("first grants taken adopted %d, want 1", got)
	}
}

// TestAPullOfALeaseAFirstAcquireTookNamesItsOwner: the client's next CONNECT
// is on n1, which takes the lease from n2. n2 holds nothing and names n3, so
// n1 refuses once and then pulls n3.
func TestAPullOfALeaseAFirstAcquireTookNamesItsOwner(t *testing.T) {
	c, n2, _ := takenByAbandonedFirst(t, func(context.Context, string, string, string) (*store.SessionState, bool) { return nil, true }, true)
	n1 := c.nodes["n1"]
	ownedBy(t, n1, "x", "n2")
	res, _, err := connectAcquire(n1, "x")
	if !refusedBusy(err) {
		t.Fatalf("first CONNECT on n1: %+v, %v; want Server Busy while n3 may hold the session", res, err)
	}
	if got := n2.svc.LivenessStats().TakeoverAnsweredPendingTaking; got < 1 {
		t.Fatalf("n2 answered pending from a takeover's note %d, want at least 1", got)
	}
	if got := n1.svc.LivenessStats().TakeoverPendingOwner; got != 1 {
		t.Fatalf("n1 takeover pending owner %d, want 1", got)
	}
	ownedBy(t, n1, "x", "n1")
	res, err = acquire(t, n1, "x", nil, 300, false)
	if err != nil || !hasFilter(res.State, "t/live") {
		t.Fatalf("retry on n1: state %v, evidence %+v, err %v; want n3's session", res.State, res.Empty, err)
	}
}

// TestACleanStartAfterAFirstAcquireTookTheLeaseFetchesNothing: the client
// asked for a new session.
func TestACleanStartAfterAFirstAcquireTookTheLeaseFetchesNothing(t *testing.T) {
	_, n2, _ := takenByAbandonedFirst(t, nil, true)
	res, err := acquire(t, n2, "x", nil, 300, true)
	if err != nil || res.State != nil || res.Empty.Reason != "" {
		t.Fatalf("clean start on n2: state %v, evidence %+v, err %v; want nothing fetched", res.State != nil, res.Empty, err)
	}
	st := n2.svc.LivenessStats()
	if st.FirstGrantsTakenAdopted != 0 || st.FirstAcquireMarks != 0 {
		t.Fatalf("first grants taken adopted %d, marks %d; want 0 and 0", st.FirstGrantsTakenAdopted, st.FirstAcquireMarks)
	}
}

// TestASessionHeldAfterAFirstAcquireTookTheLeaseIsServed: what n2 holds is what
// the client is served from; nothing is fetched to put over it.
func TestASessionHeldAfterAFirstAcquireTookTheLeaseIsServed(t *testing.T) {
	_, n2, n3 := takenByAbandonedFirst(t, nil, true)
	n2.sessions.put("default", "x", sessionFor("x"))
	imported := n2.sessions.importedCount()
	res, err := acquire(t, n2, "x", nil, 300, false)
	if err != nil || res.State != nil || n2.sessions.importedCount() != imported {
		t.Fatalf("held session: state %v, imports %d→%d, err %v; want nothing fetched", res.State != nil, imported, n2.sessions.importedCount(), err)
	}
	if !n3.sessions.has("default", "x") || n2.svc.LivenessStats().FirstGrantsTakenAdopted != 0 {
		t.Fatalf("n3 holds %v, first grants taken adopted %d; want n3's session left and 0", n3.sessions.has("default", "x"), n2.svc.LivenessStats().FirstGrantsTakenAdopted)
	}
}

// TestAGrantRecordedBeforeTheCONNECTGaveUpIsNoted: the grant took the lease
// while the first acquire's CONNECT still waited; when it then gives up, the
// owner the grant took the lease from is noted for a pull.
func TestAGrantRecordedBeforeTheCONNECTGaveUpIsNoted(t *testing.T) {
	c := newCluster(t, []string{"n1"}, 2*time.Second, nil)
	s := c.nodes["n1"].svc
	k := key{"default", "early"}
	s.firsts.begin(k, s.steady())
	s.firstTaken(k, 7, "n9")
	if owner, _ := s.waitNotes.owner(k, s.steady(), s.o.SealTimeout); owner != "" {
		t.Fatalf("a mark still in flight noted %q; its CONNECT notes for itself", owner)
	}
	s.abandonFirst(k)
	owner, takeover := s.waitNotes.owner(k, s.steady(), s.o.SealTimeout)
	if owner != "n9" || !takeover {
		t.Fatalf("note %q (takeover %v) after the CONNECT gave up; want n9 as a takeover's note", owner, takeover)
	}
}

func TestTakenFrom(t *testing.T) {
	for _, c := range []struct {
		name                    string
		abandoned               bool
		created, taken, current uint64
		from, seen, want        string
	}{
		{"taken at the current token", true, 0, 11, 11, "n3", "", "n3"},
		{"taken, seen another owner too", true, 0, 11, 11, "n3", "n4", "n3"},
		{"not abandoned", false, 0, 11, 11, "n3", "n3", ""},
		{"taken at an older token", true, 0, 11, 12, "n3", "", ""},
		{"taken at an older token, seen an owner", true, 0, 11, 12, "n3", "n4", ""},
		{"nothing recorded, seen an owner", true, 0, 0, 11, "", "n3", "n3"},
		{"nothing recorded, seen none", true, 0, 0, 11, "", "", ""},
		{"nothing recorded, seen this node", true, 0, 0, 11, "", "n2", ""},
		{"created here, moved since, seen an owner", true, 7, 0, 11, "", "n3", ""},
	} {
		if got := takenFrom(c.abandoned, c.created, c.taken, c.current, c.from, c.seen, "n2"); got != c.want {
			t.Errorf("%s: takenFrom = %q, want %q", c.name, got, c.want)
		}
	}
}

func TestFirstAcquireMarksRecordATake(t *testing.T) {
	f := newFirstAcquires()
	now := time.Unix(1000, 0)
	k := key{"d", "k"}
	if f.took(k, 5, "n9") {
		t.Fatal("a take recorded with no mark reported an abandoned mark")
	}
	f.begin(k, now)
	if f.took(k, 5, "n8") {
		t.Fatal("a mark in flight reported abandoned")
	}
	f.took(k, 6, "n9")
	if from, _ := f.abandon(k, now, time.Minute); from != "n9" {
		t.Fatalf("abandon after a take returned %q, want n9", from)
	}
	if !f.took(k, 8, "n7") {
		t.Fatal("an abandoned mark not reported abandoned")
	}
	m, ok := f.take(k)
	if !ok || m.taken != 8 || m.from != "n7" || m.created != 0 {
		t.Fatalf("take: %+v %v; want the later take, 8 from n7", m, ok)
	}
	f.begin(k, now)
	if from, _ := f.abandon(k, now, time.Minute); from != "" {
		t.Fatalf("abandon with no take returned %q, want none", from)
	}
}

// TestAFirstAcquireThatTookASilentOwnersLeaseIsRefused: n3 is asked and does
// not answer, and every replica answers "not found". n3 may still hand the
// session over, so the client is refused and the adoption kept.
func TestAFirstAcquireThatTookASilentOwnersLeaseIsRefused(t *testing.T) {
	_, n2, n3 := takenByAbandonedFirst(t, func(context.Context, string, string, string) (*store.SessionState, bool) { return nil, true }, true)
	silence(t, n3)
	res, _, err := connectAcquire(n2, "x")
	if !refusedBusy(err) {
		t.Fatalf("retry with n3 silent and no replica: %+v, %v; want Server Busy", res, err)
	}
	if !n2.svc.adopting.unfinishedFor(key{"default", "x"}) {
		t.Fatal("the adoption was not kept for the client's next retry")
	}
}

// TestAFirstAcquireThatTookACrashedOwnersLeaseFoundNowhereWaitsForTheDeclaration:
// n3 has crashed and every replica answers "not found". Until the leader
// declares n3 dead it may still hold the session, so the client is refused and
// the adoption kept; after the declaration it is served fresh, with evidence.
func TestAFirstAcquireThatTookACrashedOwnersLeaseFoundNowhereWaitsForTheDeclaration(t *testing.T) {
	c, n2, n3 := takenByAbandonedFirst(t, func(context.Context, string, string, string) (*store.SessionState, bool) { return nil, true }, true)
	c.stopNode(n3)
	waitFor(t, "n2 to see n3 down", 5*time.Second, func() bool { ask, _ := n2.svc.askable("n3"); return !ask })
	res, err := acquire(t, n2, "x", nil, 300, false)
	if !refusedBusy(err) {
		if !n2.svc.table.Live("n3") {
			t.Fatal("precondition not reached: n3 was declared dead before the retry was judged")
		}
		t.Fatalf("retry before n3's declaration, found nowhere: %+v, %v; want Server Busy", res, err)
	}
	if !n2.svc.adopting.unfinishedFor(key{"default", "x"}) {
		t.Fatal("the adoption was not kept for the client's next retry")
	}
	declared(t, n2, "n3")
	res, err = acquire(t, n2, "x", nil, 300, false)
	wantEmpty(t, "found nowhere after the declaration", res, err, EmptyAdoption, "n3")
}

// TestARetryOnTheOwnerAFirstAcquireTookFromResumesThere: the client comes back
// to n3, which takes the lease back from n2. n2's pass names n3, the requester
// itself, which is not an owner n3 waits on, so the session n3 still holds is
// resumed with no refusal.
func TestARetryOnTheOwnerAFirstAcquireTookFromResumesThere(t *testing.T) {
	_, n2, n3 := takenByAbandonedFirst(t, func(context.Context, string, string, string) (*store.SessionState, bool) { return nil, true }, true)
	ownedBy(t, n3, "x", "n2")
	res, _, err := connectAcquire(n3, "x")
	if err != nil || res.PrevOwner != "n2" || res.Empty.Reason != "" {
		t.Fatalf("retry on n3: %+v, %v; want a takeover from n2 that resumes the session held here", res, err)
	}
	if got := n2.svc.LivenessStats().TakeoverAnsweredPendingTaking; got < 1 {
		t.Fatalf("n2 answered pending from a takeover's note %d; want n3 named, at least 1", got)
	}
	if !n3.sessions.has("default", "x") || n3.svc.LivenessStats().TakeoverHeld != 1 {
		t.Fatalf("n3 holds the session %v, takeovers that resumed it %d; want it kept and 1", n3.sessions.has("default", "x"), n3.svc.LivenessStats().TakeoverHeld)
	}
}

// TestANoteIsReadBeforeTheFirstMark: when a CONNECT that found a lease left a
// note, its owner is the one fetched from, not the first acquire's.
func TestANoteIsReadBeforeTheFirstMark(t *testing.T) {
	var asked []string
	var mu sync.Mutex
	fixtureFallback = func(_ context.Context, _, _, prev string) (*store.SessionState, bool) {
		mu.Lock()
		asked = append(asked, prev)
		mu.Unlock()
		return replicaOf("x"), true
	}
	t.Cleanup(func() { fixtureFallback = nil })
	holdWorkerBack(t)
	c := newCluster(t, []string{"n1"}, 2*time.Second, nil)
	n := c.nodes["n1"]
	res, err := acquire(t, n, "x", nil, 300, false)
	if err != nil {
		t.Fatal(err)
	}
	k := key{"default", "x"}
	n.svc.abandoned.note(k, "n8", n.svc.steady(), n.svc.o.TTL)
	st, _, err := n.svc.fetchAbandoned(context.Background(), k, res.Epoch, false, nil, EmptyEvidence{}, "n9")
	if err != nil || st == nil {
		t.Fatalf("fetch: state %v, err %v; want the replica's copy", st != nil, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(asked) != 1 || asked[0] != "n8" {
		t.Fatalf("the fallback was asked for previous owner %v; want the note's n8 only", asked)
	}
}
