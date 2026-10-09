package replication

import (
	"context"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/store"
)

// keptCopyFixture: n1 owned X and is gone. Under the map, X's shard is held
// by n3 and n2, so X's source for previous owner n1 is n3; n4 holds no part
// of the shard and keeps a copy of X, which n3 does not hold. n2 fetches.
func keptCopyFixture(t *testing.T) (h *harness, id string) {
	t.Helper()
	h = newHarnessWith(t, []string{"n1", "n2", "n3", "n4"}, 2, 2, func(o *Options) { o.AckTimeout = time.Second })
	m := shardmap.Map{Version: 1, Basis: []string{"n2", "n3", "n4"}, Replicas: 2, Shards: []shardmap.Shard{
		{Leader: "n3", Followers: []string{"n2"}}, {Leader: "n4", Followers: []string{"n2"}}}}
	h.applyMap(m)
	waitSettled(h, 1)
	id = clientIn(2, 0)
	if got := m.Targets(0, "n1"); len(got) != 1 || got[0] != "n3" {
		t.Fatalf("precondition: X's sources for n1 are %v, want [n3]", got)
	}
	h.setLease("default", id, "n1", 1)
	holdCopy(h, "n4", id, queued(1, "queued"))
	h.nodes["n1"].rep.Stop()
	h.nodes["n1"].tr.Stop()
	h.waitFor("n2 to see n1 down", 5*time.Second, func() bool { return !h.nodes["n2"].rep.up("n1") })
	return h, id
}

// TestFetchFindsAKeptCopyBehindASlowSource: n3 answers "not found", but only
// after most of the fetch's budget, and n4 answers in a fraction of it. Every
// source answered, so n4's kept copy is the fetch's to return.
func TestFetchFindsAKeptCopyBehindASlowSource(t *testing.T) {
	h, id := keptCopyFixture(t)
	n2 := h.nodes["n2"]
	n2.tr.SetFault("n3", &transport.Fault{Delay: 900 * time.Millisecond, Classes: []transport.Class{transport.ReplicationAppend}})
	n2.tr.SetFault("n4", &transport.Fault{Delay: 200 * time.Millisecond, Classes: []transport.Class{transport.ReplicationAppend}})
	ss, answered, _ := n2.rep.Fetch(context.Background(), "default", id, "n1")
	if ss == nil {
		t.Fatalf("the fetch returned no copy (answered %v): n3 answered \"not found\" late, and n4's kept copy was never read", answered)
	}
	if len(ss.Entries) != 1 {
		t.Fatalf("the fetch read %d entries, want n4's one", len(ss.Entries))
	}
	n2.tr.SetFault("n3", &transport.Fault{Delay: 900 * time.Millisecond, Classes: []transport.Class{transport.ReplicationAppend}})
	if f := n2.rep.FetchKept(context.Background(), "default", id, "n1"); f.State == nil || !f.Answered || f.Kept != nil || f.KeptFrom != "" {
		t.Fatalf("FetchKept: state %v, answered %v, kept %v from %q; want n4's copy as the answer and nothing unconfirmed", f.State != nil, f.Answered, f.Kept != nil, f.KeptFrom)
	}
}

// TestFetchDoesNotTakeAKeptCopyOverASilentSource: n3 never answers, and n4
// keeps a copy. n3 may hold a newer copy the owner fed, so the fetch reads
// unanswered rather than return n4's.
func TestFetchDoesNotTakeAKeptCopyOverASilentSource(t *testing.T) {
	h, id := keptCopyFixture(t)
	n2 := h.nodes["n2"]
	dropAll(h, "n2", "n3", 1)
	began := time.Now()
	ss, answered, _ := n2.rep.Fetch(context.Background(), "default", id, "n1")
	if ss != nil || answered {
		t.Fatalf("the fetch returned a copy (%v) or read as answered (%v) while its source never answered", ss != nil, answered)
	}
	if took := time.Since(began); took > 3*time.Second {
		t.Fatalf("the fetch took %s", took)
	}
	if got := n2.rep.FetchStats().NoneKeptUnconfirmed; got != 1 {
		t.Fatalf("NoneKeptUnconfirmed = %d, want 1: the fetch should have found n4's copy and not taken it", got)
	}
	f := n2.rep.FetchKept(context.Background(), "default", id, "n1")
	if f.State != nil || f.Answered {
		t.Fatalf("FetchKept returned a copy (%v) or read as answered (%v) while its source never answered", f.State != nil, f.Answered)
	}
	if f.Kept == nil || f.KeptFrom != "n4" || len(f.Kept.Entries) != 1 {
		t.Fatalf("FetchKept's unconfirmed copy: %v from %q; want n4's, with its one entry", f.Kept, f.KeptFrom)
	}
}

// holdCopy puts a copy of X with the given entries in node's replica store.
func holdCopy(h *harness, node, id string, entries ...store.Entry) {
	f := h.nodes[node].rep.follower
	f.mu.Lock()
	f.state.Sessions = append(f.state.Sessions, &store.SessionState{SessionMeta: store.SessionMeta{Namespace: "default", ClientID: id, Epoch: 1, Version: 5, ExpiryInterval: 300},
		Entries: entries})
	f.mu.Unlock()
}

// TestFetchTakesASourcesCopyOverAKeptCopyThatAnsweredFirst: n3 holds X with
// two messages and answers after 700 ms; n4's kept copy, one message, answers
// soon after half the budget. The source's copy is the one the owner fed.
func TestFetchTakesASourcesCopyOverAKeptCopyThatAnsweredFirst(t *testing.T) {
	h, id := keptCopyFixture(t)
	holdCopy(h, "n3", id, queued(1, "queued"), queued(2, "later"))
	n2 := h.nodes["n2"]
	n2.tr.SetFault("n3", &transport.Fault{Delay: 700 * time.Millisecond, Classes: []transport.Class{transport.ReplicationAppend}})
	ss, answered, _ := n2.rep.Fetch(context.Background(), "default", id, "n1")
	if ss == nil || !answered || len(ss.Entries) != 2 {
		t.Fatalf("the fetch returned %v (answered %v), want n3's copy with two entries", ss, answered)
	}
}

// TestFetchAsksItsSourcesAtOnce: with three copies X has two sources, n3 and
// n4. n3 never answers and n4 holds X; n4's copy is the answer.
func TestFetchAsksItsSourcesAtOnce(t *testing.T) {
	h := newHarnessWith(t, []string{"n1", "n2", "n3", "n4"}, 2, 3, func(o *Options) { o.AckTimeout = time.Second })
	m := shardmap.Map{Version: 1, Basis: []string{"n2", "n3", "n4"}, Replicas: 3, Shards: []shardmap.Shard{
		{Leader: "n3", Followers: []string{"n4", "n2"}}, {Leader: "n4", Followers: []string{"n2", "n3"}}}}
	h.applyMap(m)
	waitSettled(h, 1)
	id := clientIn(2, 0)
	if got := m.Targets(0, "n1"); len(got) != 2 || got[0] != "n3" || got[1] != "n4" {
		t.Fatalf("precondition: X's sources for n1 are %v, want [n3 n4]", got)
	}
	h.setLease("default", id, "n1", 1)
	holdCopy(h, "n4", id, queued(1, "queued"))
	h.nodes["n1"].rep.Stop()
	h.nodes["n1"].tr.Stop()
	h.waitFor("n2 to see n1 down", 5*time.Second, func() bool { return !h.nodes["n2"].rep.up("n1") })
	dropAll(h, "n2", "n3", 1)
	ss, answered, _ := h.nodes["n2"].rep.Fetch(context.Background(), "default", id, "n1")
	if ss == nil || !answered {
		t.Fatalf("the fetch returned %v (answered %v), want n4's copy: a silent n3 was asked first and n4 not in time", ss, answered)
	}
}

// TestFetchAsksNoFormerHolderWhenASourceAnswers: n3 holds X and answers at
// once; n4, which keeps a copy, is never asked.
func TestFetchAsksNoFormerHolderWhenASourceAnswers(t *testing.T) {
	h, id := keptCopyFixture(t)
	holdCopy(h, "n3", id, queued(1, "queued"), queued(2, "later"))
	ss, _, _ := h.nodes["n2"].rep.Fetch(context.Background(), "default", id, "n1")
	if ss == nil || len(ss.Entries) != 2 {
		t.Fatalf("the fetch returned %v, want n3's copy", ss)
	}
	if got := h.nodes["n4"].rep.FetchStats().Answered; got != 0 {
		t.Fatalf("n4 answered %d fetches, want 0: a former holder was asked though a source had the copy", got)
	}
}
