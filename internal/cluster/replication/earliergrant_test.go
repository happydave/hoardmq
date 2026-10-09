package replication

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/store"
)

// earlierGrantMaps: under v1 shard 0 is held by n1 and n2, and X's owner n1
// replicates it to n2. Under v2, the map after n1's declaration, the shard is
// held by n3 and n2: X's new owner n4 holds none of it and replicates to n3,
// and n2 holds the shard without being fed.
func earlierGrantMaps() (v1, v2 shardmap.Map) {
	v1 = shardmap.Map{Version: 1, Basis: []string{"n1", "n2", "n3", "n4"}, Replicas: 2, Shards: []shardmap.Shard{
		{Leader: "n1", Followers: []string{"n2"}}, {Leader: "n3", Followers: []string{"n4"}}}}
	v2 = shardmap.Map{Version: 2, Basis: []string{"n2", "n3", "n4"}, Replicas: 2, Shards: []shardmap.Shard{
		{Leader: "n3", Followers: []string{"n2"}}, {Leader: "n4", Followers: []string{"n2"}}}}
	return v1, v2
}

// earlierGrantFixture: n2 holds X's copy from n1's grant (epoch 1). n1 is
// gone, and X's lease is n4's at grant 5 before v2 applies, so n2 keeps the
// copy for its new owner. n4 then writes the session at its grant; with lose,
// every replication frame from n4 to n3, its one target, is lost.
func earlierGrantFixture(t *testing.T, lose bool, mutate func(*Options)) (h *harness, sinks map[string]*counters, id string) {
	t.Helper()
	h, sinks = withCountersAnd(t, []string{"n1", "n2", "n3", "n4"}, 2, 2, func(o *Options) {
		quickAck(o)
		if mutate != nil {
			mutate(o)
		}
	})
	v1, v2 := earlierGrantMaps()
	h.applyMap(v1)
	waitSettled(h, 1)
	id = clientIn(2, 0)
	h.writeSession("n1", id, 1)
	alive(h, "n1")
	if err := h.nodes["n1"].barrier(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !holdsCopy(h, "n2", id) {
		t.Fatal("precondition: n2 does not hold n1's copy under v1")
	}
	notAlive(h, "n1")
	h.setLease("default", id, "n4", 5)
	alive(h, "n4")
	if lose {
		dropAll(h, "n4", "n3", 1)
	}
	h.applyMap(v2)
	waitSettled(h, 2)
	if !holdsCopy(h, "n2", id) || keptUnwritten(sinks, "n2") < 1 {
		t.Fatalf("precondition: after v2 n2 holds %v, kept{owner-unwritten} %v; want the copy kept for n4", holdsCopy(h, "n2", id), keptUnwritten(sinks, "n2"))
	}
	feedSessionAt(h.nodes["n4"], id, 5, queued(1, "queued"))
	h.writeSession("n4", id, 5)
	return h, sinks, id
}

// feedSessionAt is feedSession at a given epoch: the state the node's session
// manager hands a repair or a resync.
func feedSessionAt(n *tnode, id string, epoch uint64, entries ...store.Entry) {
	feedSession(n, id, entries...)
	n.sessions.mu.Lock()
	n.sessions.sessions[0].Epoch = epoch
	n.sessions.mu.Unlock()
}

func unwrittenChecks(sinks map[string]*counters, node, outcome string) float64 {
	return sinks[node].get("hoardmq_replicas_unwritten_checks_total", map[string]string{"outcome": outcome})
}

// TestCopyFromAnotherGrantOutlivesAnUndeclaredOwnersLostWrite: n2 keeps X's copy from an earlier grant for its new owner n4; n4's
// write to its one target is lost; n4 stops before it confirms, and is still
// alive by the log after several check intervals; it is declared only after.
// A takeover then fetches X from the survivors. n2's copy is the only one
// there is, so it must still be there.
func TestCopyFromAnotherGrantOutlivesAnUndeclaredOwnersLostWrite(t *testing.T) {
	const every = 400 * time.Millisecond
	h, sinks, id := earlierGrantFixture(t, true, func(o *Options) { o.UnwrittenCheck = every })
	h.nodes["n4"].rep.Stop()
	h.nodes["n4"].tr.Stop()
	time.Sleep(3 * every)
	if holdsCopy(h, "n3", id) {
		t.Fatal("precondition: n3 holds X though every frame n4 sent it was lost")
	}
	notAlive(h, "n4")
	ss, answered, _ := h.nodes["n3"].rep.Fetch(context.Background(), "default", id, "n4")
	if ss == nil {
		t.Fatalf("after n4's declaration X is held nowhere (answered %v): n2 dropped the only copy, dropped{not-a-target-held} %v", answered, dropped(sinks, "n2", "not-a-target-held"))
	}
	if len(ss.Entries) != 1 {
		t.Fatalf("the takeover read %d entries, want n1's queued one", len(ss.Entries))
	}
}

// TestCopyFromAnotherGrantGoesOnceATargetHoldsIt: n4's write reaches n3 and n4
// never confirms. At its check n2 sees n3 holding X at n4's grant, so its copy
// is not the only one, and goes.
func TestCopyFromAnotherGrantGoesOnceATargetHoldsIt(t *testing.T) {
	const every = 300 * time.Millisecond
	h, sinks, id := earlierGrantFixture(t, false, func(o *Options) { o.UnwrittenCheck = every })
	h.waitFor("n3 to hold X at n4's grant", 3*time.Second, func() bool {
		ss := h.nodes["n3"].rep.ReplicaState().Find("default", id)
		return ss != nil && ss.Epoch == 5
	})
	if !holdsCopy(h, "n2", id) {
		t.Fatal("n2 dropped its copy before its first check")
	}
	h.waitFor("n2 to drop the copy at its check", 5*time.Second, func() bool { return !holdsCopy(h, "n2", id) })
	if got, held := dropped(sinks, "n2", "not-a-target-held"), unwrittenChecks(sinks, "n2", "held"); got != 1 || held < 1 {
		t.Fatalf("dropped{not-a-target-held} %v, checks{held} %v; want 1 and at least 1", got, held)
	}
}

// TestCopyFromAnotherGrantStaysWhileATargetHoldsAnOlderOne: n3 holds a copy of
// X from before n4's grant, and n4's own write to n3 is lost. An older copy at
// a target is not the owner's, so n2 keeps its own through two checks.
func TestCopyFromAnotherGrantStaysWhileATargetHoldsAnOlderOne(t *testing.T) {
	const every = 300 * time.Millisecond
	h, sinks, id := earlierGrantFixture(t, true, func(o *Options) { o.UnwrittenCheck = every })
	old := h.nodes["n3"].rep.follower
	old.mu.Lock()
	old.state.Sessions = append(old.state.Sessions, &store.SessionState{SessionMeta: store.SessionMeta{Namespace: "default", ClientID: id, Epoch: 3, Version: 5, ExpiryInterval: 300}})
	old.mu.Unlock()
	h.waitFor("n2's second check, or its copy gone", 5*time.Second, func() bool {
		return unwrittenChecks(sinks, "n2", "not-held") >= 2 || !holdsCopy(h, "n2", id)
	})
	if !holdsCopy(h, "n2", id) {
		t.Fatalf("n2 dropped its copy on a target's older one (dropped{not-a-target-held} %v)", dropped(sinks, "n2", "not-a-target-held"))
	}
}

// TestCopyFromAnotherGrantIsCheckedAgain: n3 holds nothing of n4's at the
// first check. The loss is then lifted and n4's resync gives n3 the session,
// so a later check sees it and n2's copy goes.
func TestCopyFromAnotherGrantIsCheckedAgain(t *testing.T) {
	const every = 300 * time.Millisecond
	h, sinks, id := earlierGrantFixture(t, true, func(o *Options) { o.UnwrittenCheck = every })
	h.waitFor("n2's first check", 5*time.Second, func() bool { return unwrittenChecks(sinks, "n2", "not-held") >= 1 })
	if !holdsCopy(h, "n2", id) {
		t.Fatal("n2 dropped its copy though n3 holds nothing of n4's")
	}
	h.nodes["n4"].tr.SetFault("n3", nil)
	h.writeSession("n4", id, 5)
	h.waitFor("n2 to drop the copy at a later check", 10*time.Second, func() bool { return !holdsCopy(h, "n2", id) })
	if got := dropped(sinks, "n2", "not-a-target-held"); got != 1 {
		t.Fatalf("dropped{not-a-target-held} = %v, want 1", got)
	}
}

// TestCheckDoesNotHoldUpTheReconcileWorker: n2's requests to n3 are delayed
// past what a map change takes, so a check is in flight when the next map
// arrives. n2 reconciles the map without waiting for the check.
func TestCheckDoesNotHoldUpTheReconcileWorker(t *testing.T) {
	const every = 300 * time.Millisecond
	h, sinks, id := earlierGrantFixture(t, true, func(o *Options) {
		o.UnwrittenCheck = every
		o.AckTimeout = 4 * time.Second
	})
	h.nodes["n2"].tr.SetFault("n3", &transport.Fault{Delay: 3 * time.Second, Classes: []transport.Class{transport.ReplicationAppend}})
	h.waitFor("a check under way on n2", 5*time.Second, func() bool { return unwrittenChecks(sinks, "n2", "started") >= 1 })
	_, v2 := earlierGrantMaps()
	v3 := v2
	v3.Version = 3
	v3.Shards = []shardmap.Shard{v2.Shards[0], {Leader: "n2", Followers: []string{"n3"}}}
	h.applyMap(v3)
	began := time.Now()
	h.waitFor("n2 to reconcile v3", 5*time.Second, func() bool { return h.nodes["n2"].rep.Reconciled() == 3 })
	if took := time.Since(began); took > time.Second {
		t.Fatalf("n2 took %s to reconcile a map while a check waited on n3", took)
	}
	if !holdsCopy(h, "n2", id) {
		t.Fatal("n2 dropped its copy while its check had no answer")
	}
}

// TestChecksInFlightAreBounded: twelve copies are kept for n4 and fall due
// together, and n2's requests to n3 are delayed. No more than
// unwrittenChecksAtOnce of the checks ask n3 at once.
func TestChecksInFlightAreBounded(t *testing.T) {
	h, sinks := withCountersAnd(t, []string{"n1", "n2", "n3", "n4"}, 2, 2, func(o *Options) {
		quickAck(o)
		o.UnwrittenCheck = 300 * time.Millisecond
		o.AckTimeout = 4 * time.Second
	})
	v1, v2 := earlierGrantMaps()
	h.applyMap(v1)
	waitSettled(h, 1)
	ids := make([]string, 12)
	for i := range ids {
		ids[i] = clientIn(2, 0, fmt.Sprintf("b%d-", i))
		h.writeSession("n1", ids[i], 1)
	}
	alive(h, "n1")
	if err := h.nodes["n1"].barrier(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		if !holdsCopy(h, "n2", id) {
			t.Fatalf("precondition: n2 does not hold n1's copy of %s", id)
		}
	}
	notAlive(h, "n1")
	for _, id := range ids {
		h.setLease("default", id, "n4", 5)
	}
	alive(h, "n4")
	h.nodes["n2"].tr.SetFault("n3", &transport.Fault{Delay: 3 * time.Second, Classes: []transport.Class{transport.ReplicationAppend}})
	h.applyMap(v2)
	waitSettled(h, 2)
	h.waitFor("n2's checks under way", 5*time.Second, func() bool { return unwrittenChecks(sinks, "n2", "started") >= unwrittenChecksAtOnce })
	time.Sleep(500 * time.Millisecond)
	if got := unwrittenChecks(sinks, "n2", "started"); got != unwrittenChecksAtOnce {
		t.Fatalf("%v checks asked n3 at once, want %d", got, unwrittenChecksAtOnce)
	}
}
