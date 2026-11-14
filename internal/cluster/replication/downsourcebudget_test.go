package replication

import (
	"context"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/store"
)

// downSourceFixture: the takeover fetch of a client whose owner died, run by a
// node whose map still holds the dead owner. n2 owned X and replicated it to
// n1; n1 then took the lease and has not written X, so n2's copy is still in
// n1's replica store. n3 takes the lease from n1 under the three-node map,
// where X's one source for previous owner n1 is n2, which is down. n3 is not
// a holder of X's shard, so its kept-copy pass looks in its own store first and
// then asks n1. n3 fetches.
func downSourceFixture(t *testing.T) (h *harness, id string) {
	t.Helper()
	h = newHarnessWith(t, []string{"n1", "n2", "n3"}, 1, 2, func(o *Options) { o.AckTimeout = time.Second })
	m := shardmap.Map{Version: 1, Basis: []string{"n1", "n2", "n3"}, Replicas: 2, Shards: []shardmap.Shard{{Leader: "n1", Followers: []string{"n2"}}}}
	h.applyMap(m)
	waitSettled(h, 1)
	id = clientIn(1, 0, "x")
	if got := m.Targets(0, "n1"); len(got) != 1 || got[0] != "n2" {
		t.Fatalf("precondition: X's sources for n1 are %v, want [n2]", got)
	}
	if m.Holds(0, "n3") {
		t.Fatal("precondition: n3 holds X's shard")
	}
	h.setLease("default", id, "n1", 5)
	f := h.nodes["n1"].rep.follower
	f.mu.Lock()
	f.state.Sessions = append(f.state.Sessions, &store.SessionState{
		SessionMeta:   store.SessionMeta{Namespace: "default", ClientID: id, Epoch: 2, Version: 5, ExpiryInterval: 3600},
		Subscriptions: []store.SubscriptionRecord{{Filter: "b/x", QoS: 1}}})
	f.mu.Unlock()
	h.nodes["n2"].rep.Stop()
	h.nodes["n2"].tr.Stop()
	n3 := h.nodes["n3"].rep
	h.waitFor("n3 to read n2 down and n1 up", 5*time.Second, func() bool {
		return fetchReach(n3.peerInfo("n2")) == reachDown && fetchReach(n3.peerInfo("n1")) == reachAsk
	})
	return h, id
}

// holdFollowerFor holds node's follower mutex for d, as a checkpoint of a
// large replica store does, and returns a channel closed once it is released.
func holdFollowerFor(h *harness, node string, d time.Duration) <-chan struct{} {
	f := h.nodes[node].rep.follower
	f.mu.Lock()
	released := make(chan struct{})
	go func() {
		time.Sleep(d)
		f.mu.Unlock()
		close(released)
	}()
	return released
}

// TestDownSourceFetchInsideItsBudgetFindsTheCopy is the control: with nothing
// holding n3's store, the kept-copy pass asks n1 and returns its copy.
func TestDownSourceFetchInsideItsBudgetFindsTheCopy(t *testing.T) {
	h, id := downSourceFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	got := h.nodes["n3"].rep.FetchKept(ctx, "default", id, "n1")
	t.Logf("state %v, answered %v, down %v; n1 answered %d fetches", got.State != nil, got.Answered, got.Down, h.nodes["n1"].rep.FetchStats().Answered)
	if got.State == nil || !got.Answered || len(got.State.Subscriptions) != 1 {
		t.Fatalf("the fetch returned %v (answered %v), want n1's copy with its subscription", got.State, got.Answered)
	}
}

// TestDownSourceFetchWhoseOwnStoreOutlastsItsBudgetIsNotAnswered: the same
// fetch while n3's own store is held past the budget. The source is down, so
// the kept-copy pass runs at once and waits on n3's store; when it returns the
// budget is spent and n1 is never asked. A fetch that never asked the node
// holding the copy has not shown it is held nowhere: it must return n1's copy
// or read unanswered, never "answered, nothing held", which a takeover serves
// as a fresh session.
func TestDownSourceFetchWhoseOwnStoreOutlastsItsBudgetIsNotAnswered(t *testing.T) {
	h, id := downSourceFixture(t)
	released := holdFollowerFor(h, "n3", 600*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	began := time.Now()
	got := h.nodes["n3"].rep.FetchKept(ctx, "default", id, "n1")
	took := time.Since(began)
	<-released
	asked := h.nodes["n1"].rep.FetchStats().Answered
	_, held := h.nodes["n1"].rep.follower.find("default", id)
	t.Logf("state %v, answered %v, down %v, took %s; n1 answered %d fetches; n1 holds X %v", got.State != nil, got.Answered, got.Down, took.Round(time.Millisecond), asked, held)
	if got.State == nil && got.Answered {
		t.Fatalf("the fetch read \"answered, nothing held\" while n1 holds X with a subscription and was asked %d times: its source was down, and its budget ran out in n3's own store before the kept-copy pass asked n1", asked)
	}
}
