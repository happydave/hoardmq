package lease

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/store"
)

// silence makes a node a previous owner whose takeover pass does not start
// answering until released: its pass blocks in Take. The release also runs at
// cleanup, before the cluster stops.
func silence(t *testing.T, n *tnode) (release func()) {
	t.Helper()
	ch := make(chan struct{})
	release = sync.OnceFunc(func() { close(ch) })
	n.sessions.setOnTake(func(string, string) { <-ch })
	t.Cleanup(release)
	return release
}

// connectAcquire acquires the way the broker's CONNECT does: on a context of
// the takeover timeout, which an unanswered pull spends.
func connectAcquire(n *tnode, id string) (AcquireResult, time.Duration, error) {
	ctx, cancel := context.WithTimeout(context.Background(), n.svc.o.TakeoverTimeout)
	defer cancel()
	began := time.Now()
	res, err := n.svc.Acquire(ctx, "default", id, nil, 300, false)
	return res, time.Since(began), err
}

// peerFetch answers like Replicator.Fetch for a copy held on another node: on
// a context that has ended no request reaches the peer, and the fetch reports
// unanswered.
func peerFetch(st *store.SessionState) func(context.Context, string, string, string) (*store.SessionState, bool) {
	return func(ctx context.Context, _, id, _ string) (*store.SessionState, bool) {
		if id != st.ClientID {
			return nil, true
		}
		if ctx.Err() != nil {
			return nil, false
		}
		return st, true
	}
}

func replicaOf(id string) *store.SessionState {
	st := sessionFor(id)
	st.Subscriptions = []store.SubscriptionRecord{{Filter: "t/replica", QoS: 1}}
	return st
}

// silentOwnerCluster is three nodes whose fallback is fb; n1 owns x and holds
// its live session, and its takeover pass is held back. The adoption worker
// is held back too, so a CONNECT is always the fetcher.
func silentOwnerCluster(t *testing.T, fb func(context.Context, string, string, string) (*store.SessionState, bool)) (a, b *tnode, release func()) {
	t.Helper()
	holdWorkerBack(t)
	fixtureFallback = fb
	t.Cleanup(func() { fixtureFallback = nil })
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	a, b = c.nodes["n1"], c.nodes["n2"]
	if _, err := acquire(t, a, "x", nil, 300, true); err != nil {
		t.Fatal(err)
	}
	live := sessionFor("x")
	live.Subscriptions = []store.SubscriptionRecord{{Filter: "t/live", QoS: 1}}
	a.sessions.put("default", "x", live)
	ownedBy(t, b, "x", "n1")
	return a, b, silence(t, a)
}

// TestSilentOwnersTakeoverIsServedFromTheReplica: the previous owner does not
// answer the pull within the CONNECT's budget, and the replica is on a peer.
// The fallback runs on a budget of its own and serves the replica's copy.
func TestSilentOwnersTakeoverIsServedFromTheReplica(t *testing.T) {
	replica := replicaOf("x")
	_, b, _ := silentOwnerCluster(t, peerFetch(replica))
	res, took, err := connectAcquire(b, "x")
	if err != nil || res.PrevOwner != "n1" || res.State != replica {
		t.Fatalf("takeover from a silent owner: %+v, err %v; want the replica's state", res, err)
	}
	if budget := b.svc.o.TakeoverTimeout + b.svc.o.FallbackTimeout + 500*time.Millisecond; took > budget {
		t.Fatalf("the CONNECT took %s, over the takeover budget plus the fallback's (%s)", took, budget)
	}
	if st := b.svc.LivenessStats(); st.TakeoverOwnerSilent != 1 || st.TakeoverOwnerSilentServed != 1 || st.TakeoverUnanswered != 0 || st.TakeoverEmpty != 0 {
		t.Fatalf("counters: silent %d, served %d, unanswered %d, empty %d; want 1, 1, 0, 0", st.TakeoverOwnerSilent, st.TakeoverOwnerSilentServed, st.TakeoverUnanswered, st.TakeoverEmpty)
	}
}

// TestSilentOwnersFallbackIsBounded: a fallback that never answers ends at its
// own deadline, and the CONNECT is refused within the takeover budget plus that
// bound.
func TestSilentOwnersFallbackIsBounded(t *testing.T) {
	var mu sync.Mutex
	var ended error
	fb := func(ctx context.Context, _, id, _ string) (*store.SessionState, bool) {
		if id != "x" {
			return nil, true
		}
		<-ctx.Done()
		mu.Lock()
		ended = ctx.Err()
		mu.Unlock()
		return nil, false
	}
	_, b, _ := silentOwnerCluster(t, fb)
	budget := b.svc.o.TakeoverTimeout + b.svc.o.FallbackTimeout + 500*time.Millisecond
	type outcome struct {
		err  error
		took time.Duration
	}
	done := make(chan outcome, 1)
	go func() {
		_, took, err := connectAcquire(b, "x")
		done <- outcome{err, took}
	}()
	select {
	case o := <-done:
		if !refusedBusy(o.err) || o.took > budget {
			t.Fatalf("refused %v after %s; want Server Busy within %s", o.err, o.took, budget)
		}
	case <-time.After(budget + time.Second):
		t.Fatalf("the CONNECT had not returned after %s: the fallback is not bounded", budget+time.Second)
	}
	mu.Lock()
	defer mu.Unlock()
	if !errors.Is(ended, context.DeadlineExceeded) {
		t.Fatalf("the fallback's context ended with %v; want its own deadline", ended)
	}
}

// TestSilentOwnerAndNothingFoundIsRefusedNotServedFresh: the replicas answer
// that they hold nothing, but the previous owner has not answered and may
// still hand the session over. The CONNECT is refused and the adoption kept,
// and the retry after the owner's late answer resumes the live session.
func TestSilentOwnerAndNothingFoundIsRefusedNotServedFresh(t *testing.T) {
	_, b, release := silentOwnerCluster(t, func(context.Context, string, string, string) (*store.SessionState, bool) { return nil, true })
	_, _, err := connectAcquire(b, "x")
	if !refusedBusy(err) {
		t.Fatalf("a silent owner with nothing found elsewhere: %v, want Server Busy", err)
	}
	k := key{"default", "x"}
	b.svc.adopting.mu.Lock()
	ad := b.svc.adopting.m[k]
	b.svc.adopting.mu.Unlock()
	if ad == nil || !ad.job.takeover || ad.job.prev != "n1" {
		t.Fatalf("adoption %+v; want one recorded for the takeover from n1", ad)
	}
	if st := b.svc.LivenessStats(); st.TakeoverOwnerSilent != 1 || st.TakeoverOwnerSilentServed != 0 || st.TakeoverUnanswered != 1 || st.TakeoverEmpty != 0 {
		t.Fatalf("counters: silent %d, served %d, unanswered %d, empty %d; want 1, 0, 1, 0", st.TakeoverOwnerSilent, st.TakeoverOwnerSilentServed, st.TakeoverUnanswered, st.TakeoverEmpty)
	}
	release()
	waitFor(t, "the previous owner's late answer to reach the adoption", 5*time.Second, func() bool {
		b.svc.adopting.mu.Lock()
		defer b.svc.adopting.mu.Unlock()
		ad := b.svc.adopting.m[k]
		return ad != nil && ad.late != nil
	})
	ownedBy(t, b, "x", "n2")
	res, err := acquire(t, b, "x", nil, 300, false)
	if err != nil || res.State == nil || len(res.State.Subscriptions) != 1 || res.State.Subscriptions[0].Filter != "t/live" {
		t.Fatalf("the retry: %+v, err %v; want the live session the previous owner handed over", res, err)
	}
}

// TestRetryAfterASilentOwnerIsServedFromTheReplica: the refused client's
// retry fetches its adoption, and the previous owner is still silent. The
// retry's fallback has its own budget too.
func TestRetryAfterASilentOwnerIsServedFromTheReplica(t *testing.T) {
	replica := replicaOf("x")
	var mu sync.Mutex
	calls := 0
	fetch := peerFetch(replica)
	fb := func(ctx context.Context, ns, id, prev string) (*store.SessionState, bool) {
		if id != "x" {
			return nil, true
		}
		mu.Lock()
		calls++
		first := calls == 1
		mu.Unlock()
		if first {
			return nil, true
		}
		return fetch(ctx, ns, id, prev)
	}
	_, b, _ := silentOwnerCluster(t, fb)
	if _, _, err := connectAcquire(b, "x"); !refusedBusy(err) {
		t.Fatalf("the takeover: %v, want Server Busy (silent owner, nothing found)", err)
	}
	ownedBy(t, b, "x", "n2")
	res, took, err := connectAcquire(b, "x")
	if err != nil || res.State != replica {
		t.Fatalf("the retry with the owner still silent: %+v, err %v; want the replica's state", res, err)
	}
	if budget := b.svc.o.TakeoverTimeout + b.svc.o.FallbackTimeout + 500*time.Millisecond; took > budget {
		t.Fatalf("the retry took %s, over %s", took, budget)
	}
}
