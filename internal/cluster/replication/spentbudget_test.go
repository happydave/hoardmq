package replication

import (
	"context"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/store"
)

// spentBudgetFixture: n1 owns X and has not written it yet, so the copy an
// earlier owner fed is still in n1's replica store. Under the map X's shard is
// held by n1 and n3, so a fetch for previous owner n1 has n3 alone as its
// source: n3 looks in its own store, finds nothing, and n1's copy is reached
// only by the kept-copy pass. n3 fetches.
func spentBudgetFixture(t *testing.T) (h *harness, id string, sinks map[string]*counters) {
	t.Helper()
	h, sinks = withCountersAnd(t, []string{"n1", "n3"}, 2, 2, func(o *Options) { o.AckTimeout = time.Second })
	m := shardmap.Map{Version: 1, Basis: []string{"n1", "n3"}, Replicas: 2, Shards: []shardmap.Shard{
		{Leader: "n1", Followers: []string{"n3"}}, {Leader: "n3", Followers: []string{"n1"}}}}
	h.applyMap(m)
	waitSettled(h, 1)
	id = clientIn(2, 0)
	if got := m.Targets(0, "n1"); len(got) != 1 || got[0] != "n3" {
		t.Fatalf("precondition: X's sources for n1 are %v, want [n3]", got)
	}
	h.setLease("default", id, "n1", 5)
	f := h.nodes["n1"].rep.follower
	f.mu.Lock()
	f.state.Sessions = append(f.state.Sessions, &store.SessionState{
		SessionMeta:   store.SessionMeta{Namespace: "default", ClientID: id, Epoch: 2, Version: 5, ExpiryInterval: 300},
		Subscriptions: []store.SubscriptionRecord{{Filter: "b/x", QoS: 1}},
		Entries:       []store.Entry{queued(1, "queued")}})
	f.mu.Unlock()
	return h, id, sinks
}

// holdFollower holds node's follower mutex for d, as a checkpoint of a large
// replica store does, and returns a channel closed once it is released.
func holdFollower(h *harness, node string, d time.Duration) <-chan struct{} {
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

// TestFetchInsideItsBudgetFindsTheFormerHoldersCopy is the control: with no
// wait on n3's store, the kept-copy pass asks n1 and returns its copy.
func TestFetchInsideItsBudgetFindsTheFormerHoldersCopy(t *testing.T) {
	h, id, _ := spentBudgetFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	got := h.nodes["n3"].rep.FetchKept(ctx, "default", id, "n1")
	t.Logf("state %v, answered %v; n1 answered %d fetches", got.State != nil, got.Answered, h.nodes["n1"].rep.FetchStats().Answered)
	if got.State == nil || !got.Answered || len(got.State.Subscriptions) != 1 || len(got.State.Entries) != 1 {
		t.Fatalf("the fetch returned %v (answered %v), want n1's copy with its subscription and entry", got.State, got.Answered)
	}
}

// TestFetchWhoseOwnFindOutlastsItsBudgetIsNotAnswered: n3's own store is held
// past the fetch's budget, as a checkpoint holds it. When n3's find returns
// the budget is spent, so the kept-copy pass asks nobody. A fetch that asked
// nobody holding the copy has not shown it is held nowhere: it must return
// n1's copy or read unanswered, never "answered, nothing held", which a
// takeover serves as a fresh session.
func TestFetchWhoseOwnFindOutlastsItsBudgetIsNotAnswered(t *testing.T) {
	h, id, sinks := spentBudgetFixture(t)
	n3 := h.nodes["n3"]
	released := holdFollower(h, "n3", 600*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	began := time.Now()
	got := n3.rep.FetchKept(ctx, "default", id, "n1")
	took := time.Since(began)
	<-released
	asked := h.nodes["n1"].rep.FetchStats().Answered
	_, held := h.nodes["n1"].rep.follower.find("default", id)
	t.Logf("state %v, answered %v, took %s; n1 answered %d fetches; n1 holds X %v", got.State != nil, got.Answered, took.Round(time.Millisecond), asked, held)
	if got.State == nil && got.Answered {
		t.Fatalf("the fetch read \"answered, nothing held\" while n1 holds X with a subscription and was asked %d times: its budget ran out in n3's own find, and the kept-copy pass asked nobody", asked)
	}
	if spent := n3.rep.FetchStats().NoneBudgetSpent; spent != 1 {
		t.Fatalf("NoneBudgetSpent = %d, want 1: the fetch ended on its budget without being counted", spent)
	}
	if got := sinks["n3"].get("hoardmq_replication_fetches_total", map[string]string{"outcome": "none-budget-spent"}); got != 1 {
		t.Fatalf("hoardmq_replication_fetches_total{outcome=\"none-budget-spent\"} = %v on n3, want 1", got)
	}
}

// TestFetchWhoseKeptPassFindOutlastsItsBudgetIsNotAnswered: n3, X's source,
// answers "not found" at once, and n4 keeps a copy. The fetching node, n2, is
// not a source, so its kept-copy pass looks in its own store first; that store
// is held past the budget, and n4 is never asked.
func TestFetchWhoseKeptPassFindOutlastsItsBudgetIsNotAnswered(t *testing.T) {
	h, id := keptCopyFixture(t)
	n2 := h.nodes["n2"]
	released := holdFollower(h, "n2", 600*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	got := n2.rep.FetchKept(ctx, "default", id, "n1")
	<-released
	asked := h.nodes["n4"].rep.FetchStats().Answered
	t.Logf("state %v, answered %v; n3 answered %d fetches, n4 %d", got.State != nil, got.Answered, h.nodes["n3"].rep.FetchStats().Answered, asked)
	if got.State == nil && got.Answered {
		t.Fatalf("the fetch read \"answered, nothing held\" while n4 keeps X and was asked %d times: its budget ran out in n2's own find, and the kept-copy pass asked nobody", asked)
	}
	if spent := n2.rep.FetchStats().NoneBudgetSpent; spent != 1 {
		t.Fatalf("NoneBudgetSpent = %d, want 1: the fetch ended on its budget without being counted", spent)
	}
}

// TestFetchAnswered: a fetch that found no copy has shown none exists only
// when no request ended unanswered and its kept-copy pass did not end on a
// spent budget.
func TestFetchAnswered(t *testing.T) {
	for _, c := range []struct {
		unanswered, budgetSpent, want bool
	}{
		{false, false, true},
		{true, false, false},
		{false, true, false},
		{true, true, false},
	} {
		if got := fetchAnswered(c.unanswered, c.budgetSpent); got != c.want {
			t.Errorf("fetchAnswered(unanswered %v, budgetSpent %v) = %v, want %v", c.unanswered, c.budgetSpent, got, c.want)
		}
	}
}

// TestFetchPastADownPeerNamesIt: X's previous owner n1 is still in the map, and
// its source n3 holds nothing. n4, which keeps a copy, is down, so the
// kept-copy pass cannot ask it. A fetch that could not ask a node that may
// hold the copy has not shown it is held nowhere: it must name n4 among the
// down nodes, for the caller to judge by the log, or read unanswered.
func TestFetchPastADownPeerNamesIt(t *testing.T) {
	h := newHarnessWith(t, []string{"n1", "n3", "n4"}, 2, 2, func(o *Options) { o.AckTimeout = time.Second })
	m := shardmap.Map{Version: 1, Basis: []string{"n1", "n3", "n4"}, Replicas: 2, Shards: []shardmap.Shard{
		{Leader: "n1", Followers: []string{"n3"}}, {Leader: "n4", Followers: []string{"n1"}}}}
	h.applyMap(m)
	waitSettled(h, 1)
	id := clientIn(2, 0)
	if got := m.Targets(0, "n1"); len(got) != 1 || got[0] != "n3" {
		t.Fatalf("precondition: X's sources for n1 are %v, want [n3]", got)
	}
	h.setLease("default", id, "n1", 5)
	holdCopy(h, "n4", id, queued(1, "queued"))
	h.nodes["n4"].rep.Stop()
	h.nodes["n4"].tr.Stop()
	h.waitFor("n3 to see n4 down", 5*time.Second, func() bool { return !h.nodes["n3"].rep.up("n4") })
	got := h.nodes["n3"].rep.FetchKept(context.Background(), "default", id, "n1")
	t.Logf("state %v, answered %v, down %v", got.State != nil, got.Answered, got.Down)
	if got.State == nil && got.Answered && !contains(got.Down, "n4") {
		t.Fatalf("the fetch read \"answered, nothing held\" and named no down node while n4, which keeps X, could not be asked")
	}
}
