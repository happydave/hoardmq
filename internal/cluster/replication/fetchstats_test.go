package replication

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/message"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
)

// fetchFixture is three nodes with one session of n1's replicated to n2.
func fetchFixture(t *testing.T, mutate func(*Options)) (*harness, string) {
	t.Helper()
	h := newHarnessWith(t, []string{"n1", "n2", "n3"}, 3, 2, mutate)
	h.applyMap(shardmap.Compute(1, h.order, 2, 3))
	id := clientIn(3, 0) // leader n1, follower n2
	h.writeSession("n1", id, 1)
	if err := h.nodes["n1"].barrier(context.Background()); err != nil {
		t.Fatal(err)
	}
	return h, id
}

// TestFetchStagesAreCountedOnTheSideThatRunsThem: a remote fetch is timed as
// a request where it is asked and as queue, find and reply where it is
// answered; a local one as a find.
func TestFetchStagesAreCountedOnTheSideThatRunsThem(t *testing.T) {
	h, id := fetchFixture(t, nil)
	n2, n3 := h.nodes["n2"].rep, h.nodes["n3"].rep
	before2, before3 := n2.FetchStats(), n3.FetchStats()

	if ss, _, _ := n3.Fetch(context.Background(), "default", id, "n1"); ss == nil {
		t.Fatal("remote fetch found nothing")
	}
	asked, answered := n3.FetchStats(), n2.FetchStats()
	if asked.Remote-before3.Remote != 1 || asked.Requests-before3.Requests != 1 || asked.RequestsEnded != before3.RequestsEnded {
		t.Fatalf("requester counts: %+v", asked)
	}
	if asked.Local != before3.Local || asked.Answered != before3.Answered {
		t.Fatalf("the requester counted a local fetch or an answer: %+v", asked)
	}
	if answered.Answered-before2.Answered != 1 || answered.Finds-before2.Finds != 1 || answered.Pooled-before2.Pooled != 1 {
		t.Fatalf("holder counts: %+v", answered)
	}
	request := asked.RequestMs - before3.RequestMs
	held := (answered.QueueMs - before2.QueueMs) + (answered.FindMs - before2.FindMs) + (answered.ReplyMs - before2.ReplyMs)
	if request <= 0 || asked.SendMs-before3.SendMs <= 0 || answered.FindMs-before2.FindMs <= 0 || answered.ReplyMs-before2.ReplyMs <= 0 {
		t.Fatalf("a stage took no time: asked %+v answered %+v", asked, answered)
	}
	if held > request {
		t.Fatalf("the holder's stages (%.3f ms) exceed the request they are part of (%.3f ms)", held, request)
	}

	mid := n2.FetchStats()
	if ss, _, _ := n2.Fetch(context.Background(), "default", id, "n1"); ss == nil {
		t.Fatal("local fetch found nothing")
	}
	local := n2.FetchStats()
	if local.Local-mid.Local != 1 || local.LocalFinds-mid.LocalFinds != 1 || local.Requests != mid.Requests || local.LocalFindMs-mid.LocalFindMs <= 0 || local.Finds-mid.Finds != 1 {
		t.Fatalf("local fetch: %+v after %+v", local, mid)
	}

	if ss, _, _ := n3.Fetch(context.Background(), "default", "nobody", "n1"); ss != nil {
		t.Fatal("unknown session fetched")
	}
	if got := n3.FetchStats(); got.None-asked.None != 1 {
		t.Fatalf("a fetch that found nothing: %+v", got)
	}
}

// TestFetchQueueWaitIsNotFindTime: a fetch that waits for the peer's worker
// is charged to the queue, which is what lets a run say whether the worker
// or the follower's lock is the limit. Fetches are put on the worker, where
// one can be made to wait behind another.
func TestFetchQueueWaitIsNotFindTime(t *testing.T) {
	h, id := fetchFixture(t, func(o *Options) { o.FetchOnWorker = true })
	n2, n3 := h.nodes["n2"].rep, h.nodes["n3"].rep
	const held = 150 * time.Millisecond

	// A fetch ahead of the real one occupies n3's worker on n2 for as long as
	// the follower's lock is held. It carries no queue time of its own.
	n2.follower.mu.Lock()
	n2.follower.enqueue("n3", frame{Kind: kindFetch, Namespace: "default", ClientID: id})
	h.waitFor("the worker to take the first fetch", 5*time.Second, func() bool { return n2.FetchStats().Answered == 1 })

	got := make(chan bool, 1)
	go func() {
		ss, _, _ := n3.Fetch(context.Background(), "default", id, "n1")
		got <- ss != nil
	}()
	time.Sleep(held)
	n2.follower.mu.Unlock()

	select {
	case ok := <-got:
		if !ok {
			t.Fatal("the queued fetch found nothing")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the queued fetch never returned")
	}
	st := n2.FetchStats()
	if st.Answered != 2 {
		t.Fatalf("answered %d fetches, want 2", st.Answered)
	}
	if st.QueueMs < float64(held.Milliseconds())/2 {
		t.Fatalf("queue wait %.1f ms for a fetch that waited about %v behind another", st.QueueMs, held)
	}
}

// blockWorker occupies holder's worker for peer until the returned func is
// called: the worker takes a retained write from peer and stops on the
// session manager's lock, which no fetch touches.
func blockWorker(t *testing.T, h *harness, holder, peer string) (release func()) {
	t.Helper()
	n := h.nodes[holder]
	n.sessions.mu.Lock()
	release = sync.OnceFunc(n.sessions.mu.Unlock)
	t.Cleanup(release)
	n.rep.follower.enqueue(peer, frame{Kind: kindRetainedWrite, Msg: &message.Message{Namespace: "default", Topic: "blocker", Payload: []byte("x"), Retain: true}})
	return release
}

// TestFetchIsAnsweredWhileThePeersWorkerIsBusy: after a node loss the asking
// peer's worker is seconds deep in the storm's appends, and a CONNECT is
// holding a handshake slot for the answer.
func TestFetchIsAnsweredWhileThePeersWorkerIsBusy(t *testing.T) {
	h, id := fetchFixture(t, nil)
	n2, n3 := h.nodes["n2"].rep, h.nodes["n3"].rep
	release := blockWorker(t, h, "n2", "n3")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ss, _, _ := n3.Fetch(ctx, "default", id, "n1")
	if ss == nil || len(ss.Entries) != 1 {
		t.Fatalf("fetch with the peer's worker blocked: %+v", ss)
	}
	st := n2.FetchStats()
	if st.Pooled != 1 || st.Overflowed != 0 || st.Answered != 1 {
		t.Fatalf("holder counts %+v, want the one fetch answered by the pool", st)
	}
	release()
}

// TestFetchOverflowsToTheWorkerWhenThePoolIsFull: the reader never blocks
// and a fetch is never dropped, so a full queue sends it the old way.
func TestFetchOverflowsToTheWorkerWhenThePoolIsFull(t *testing.T) {
	h, id := fetchFixture(t, func(o *Options) { o.FetchAnswerers, o.FetchQueue = 1, 1 })
	n2, n3 := h.nodes["n2"].rep, h.nodes["n3"].rep

	// The follower's lock stops the one answerer inside its find, then one
	// fetch fills the queue, and the next has nowhere to go but the worker.
	n2.follower.mu.Lock()
	unlock := sync.OnceFunc(n2.follower.mu.Unlock)
	t.Cleanup(unlock)
	const fetches = 4
	got := make(chan bool, fetches)
	for i := 0; i < fetches; i++ {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			ss, _, _ := n3.Fetch(ctx, "default", id, "n1")
			got <- ss != nil
		}()
	}
	h.waitFor("a fetch to overflow", 5*time.Second, func() bool { return n2.FetchStats().Overflowed >= 1 })
	unlock()
	for i := 0; i < fetches; i++ {
		select {
		case ok := <-got:
			if !ok {
				t.Fatal("a fetch found nothing")
			}
		case <-time.After(10 * time.Second):
			t.Fatal("a fetch never returned")
		}
	}
	st := n2.FetchStats()
	if st.Answered != fetches || st.Pooled+st.Overflowed != fetches || st.Overflowed < 1 {
		t.Fatalf("holder counts %+v, want %d answered between the pool and the worker", st, fetches)
	}
}

// TestNoneIsCountedApartWhenNobodyAnswered: a fetch that timed out has not
// learned that the session is held nowhere, and says so; one whose source is
// down asked nobody it could have waited for.
func TestNoneIsCountedApartWhenNobodyAnswered(t *testing.T) {
	h, id := fetchFixture(t, func(o *Options) { o.FetchOnWorker = true })
	n3 := h.nodes["n3"].rep
	if ss, answered, down := n3.Fetch(context.Background(), "default", "nobody", "n1"); ss != nil || !answered || len(down) != 0 {
		t.Fatalf("unknown session: state %v, answered %v, down %v, want none, answered and none down", ss != nil, answered, down)
	}
	if st := n3.FetchStats(); st.None != 1 || st.NoneUnanswered != 0 || st.NoneSourceDown != 0 {
		t.Fatalf("an answered not-found: %+v", st)
	}
	release := blockWorker(t, h, "n2", "n3")
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if ss, answered, _ := n3.Fetch(ctx, "default", id, "n1"); ss != nil || answered {
		t.Fatalf("through a blocked worker: state %v, answered %v, want none and unanswered", ss != nil, answered)
	}
	release()
	if st := n3.FetchStats(); st.None != 2 || st.NoneUnanswered != 1 || st.NoneSourceDown != 0 {
		t.Fatalf("an unanswered fetch: %+v", st)
	}
	h.nodes["n2"].tr.Stop()
	h.waitFor("n3 to see n2 down", 5*time.Second, func() bool { return !n3.up("n2") })
	ss, answered, down := n3.Fetch(context.Background(), "default", id, "n1")
	if ss != nil || !answered {
		t.Fatalf("with the source down: state %v, answered %v, want none and answered", ss != nil, answered)
	}
	if len(down) != 1 || down[0] != "n2" {
		t.Fatalf("with the source down the fetch names %v as down, want [n2]", down)
	}
	if st := n3.FetchStats(); st.None != 3 || st.NoneUnanswered != 1 || st.NoneSourceDown != 1 {
		t.Fatalf("a fetch whose source is down: %+v", st)
	}
}
