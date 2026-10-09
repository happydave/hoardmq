package replication

import (
	"context"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/lease"
	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

// unfedMaps: under v1 shard 0 is held by n1 and n2, so a session owned by n1
// is replicated to n2; under v2 it is held by n3 and n2, so the owner
// replicates to n3 and n2 receives nothing while still holding the shard.
func unfedMaps(order []string) (v1, v2 shardmap.Map) {
	v1 = shardmap.Compute(1, order, 2, 2)
	v2 = shardmap.Map{Version: 2, Basis: order, Replicas: 2, Shards: []shardmap.Shard{{Leader: "n3", Followers: []string{"n2"}}, {Leader: "n2", Followers: []string{"n3"}}}}
	return v1, v2
}

func alive(h *harness, node string) {
	for _, n := range h.nodes {
		n.leases.mu.Lock()
		n.leases.nodes[node] = lease.NodeInfo{Alive: time.Now()}
		n.leases.mu.Unlock()
	}
}

func notAlive(h *harness, node string) {
	for _, n := range h.nodes {
		n.leases.mu.Lock()
		delete(n.leases.nodes, node)
		n.leases.mu.Unlock()
	}
}

// queued is the entry writeSession enqueues, for feeding the owner's session
// manager so its rebalance and its repairs carry the same state the store has.
func queued(seq uint64, payload string) store.Entry {
	return store.Entry{Seq: seq, Message: &message.Message{Namespace: "default", Topic: "a/b", Payload: []byte(payload)}, QoS: 1}
}

func holdsCopy(h *harness, node, id string) bool {
	return h.nodes[node].rep.ReplicaState().Find("default", id) != nil
}

// TestCopyOfASessionThisNodeIsNotATargetForIsDropped: n2 holds a copy of X
// because it was X's target; the map moves the target to n3 while n2 goes on
// holding the shard, and the copy — which nothing will maintain — goes once
// the owner confirms n3 holds the session.
func TestCopyOfASessionThisNodeIsNotATargetForIsDropped(t *testing.T) {
	h, sinks := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, quickAck)
	v1, v2 := unfedMaps(h.order)
	h.applyMap(v1)
	waitSettled(h, 1)
	id := clientIn(2, 0)
	h.writeSession("n1", id, 1)
	feedSession(h.nodes["n1"], id, queued(1, "queued"))
	alive(h, "n1")
	if err := h.nodes["n1"].barrier(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !holdsCopy(h, "n2", id) {
		t.Fatal("precondition: n2 does not hold the copy under v1")
	}
	h.applyMap(v2)
	waitSettled(h, 2)
	h.waitFor("n2 to drop the copy it is no longer a target for", 2*time.Second, func() bool {
		return !holdsCopy(h, "n2", id)
	})
	if !holdsCopy(h, "n3", id) {
		t.Fatal("n3, the session's target under v2, does not hold the copy")
	}
	if got := dropped(sinks, "n2", "not-a-target-confirmed"); got != 1 {
		t.Fatalf("dropped{not-a-target-confirmed} = %v, want 1", got)
	}
}

// TestUnfedCopyIsKeptWhileTheOwnerIsGone: the same map change with the owner
// not alive. No target is being fed either, so the copy may be the last one
// and stays.
func TestUnfedCopyIsKeptWhileTheOwnerIsGone(t *testing.T) {
	h, sinks := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, quickAck)
	v1, v2 := unfedMaps(h.order)
	h.applyMap(v1)
	waitSettled(h, 1)
	id := clientIn(2, 0)
	h.writeSession("n1", id, 1)
	feedSession(h.nodes["n1"], id, queued(1, "queued"))
	alive(h, "n1")
	if err := h.nodes["n1"].barrier(context.Background()); err != nil {
		t.Fatal(err)
	}
	notAlive(h, "n1")
	h.applyMap(v2)
	waitSettled(h, 2)
	time.Sleep(300 * time.Millisecond)
	if !holdsCopy(h, "n2", id) {
		t.Fatal("n2 dropped a copy of a session whose owner is gone")
	}
	if got := sinks["n2"].get("hoardmq_replicas_kept_total", map[string]string{"reason": "owner-not-alive"}); got < 1 {
		t.Fatalf("kept{owner-not-alive} = %v, want at least 1", got)
	}
}

// TestHandOverToANodeThatIsNotATargetIsRefused: n2 stops holding the shard
// and hands its copy to the new holders. Only one of them is the session's
// target; the other would hold a copy nothing maintains, and refuses it.
func TestHandOverToANodeThatIsNotATargetIsRefused(t *testing.T) {
	h, _ := withCountersAnd(t, []string{"n1", "n2", "n3", "n4"}, 2, 2, quickAck)
	v1 := shardmap.Compute(1, h.order, 2, 2)
	v2 := shardmap.Map{Version: 2, Basis: h.order, Replicas: 2, Shards: []shardmap.Shard{{Leader: "n3", Followers: []string{"n4"}}, {Leader: "n2", Followers: []string{"n3"}}}}
	h.applyMap(v1)
	waitSettled(h, 1)
	id := clientIn(2, 0)
	h.writeSession("n1", id, 1)
	feedSession(h.nodes["n1"], id, queued(1, "queued"))
	alive(h, "n1")
	if err := h.nodes["n1"].barrier(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !holdsCopy(h, "n2", id) {
		t.Fatal("precondition: n2 does not hold the copy under v1")
	}
	h.applyMap(v2)
	waitSettled(h, 2)
	h.waitFor("the hand-over to reach n3, the session's target", 3*time.Second, func() bool {
		return holdsCopy(h, "n3", id)
	})
	time.Sleep(300 * time.Millisecond)
	if holdsCopy(h, "n4", id) {
		t.Fatal("n4 took a handed-over copy of a session it is not a target for")
	}
}

// TestTakeoverReadsTheMaintainedCopy: the shape the work item reports. n2 is
// X's target, then the map makes n3 the target and the owner writes again;
// the owner dies and the map makes n2 the fetch's source. What the fetch
// returns is the copy the owner's stream maintained, not the one n2 held
// from before the map moved.
func TestTakeoverReadsTheMaintainedCopy(t *testing.T) {
	for _, c := range []struct {
		name    string
		defect  bool
		entries int
	}{
		{"sound", false, 2},
		{"unfed copies kept", true, 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			h, _ := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, func(o *Options) {
				quickAck(o)
				o.KeepUnfedCopies = c.defect
			})
			v1, v2 := unfedMaps(h.order)
			h.applyMap(v1)
			waitSettled(h, 1)
			id := clientIn(2, 0)
			h.writeSession("n1", id, 1)
			feedSession(h.nodes["n1"], id, queued(1, "queued"))
			alive(h, "n1")
			if err := h.nodes["n1"].barrier(context.Background()); err != nil {
				t.Fatal(err)
			}
			h.applyMap(v2)
			waitSettled(h, 2)
			// The owner writes once more: the record reaches n3, its target
			// under v2, and nothing else.
			key := store.Key{Namespace: "default", ClientID: id, Epoch: 1}
			if err := h.nodes["n1"].own.Enqueue(key, queued(2, "since")); err != nil {
				t.Fatal(err)
			}
			if err := h.nodes["n1"].own.Sync(); err != nil {
				t.Fatal(err)
			}
			feedSession(h.nodes["n1"], id, queued(1, "queued"), queued(2, "since"))
			h.waitFor("the record to reach n3", 3*time.Second, func() bool {
				ss := h.nodes["n3"].rep.ReplicaState().Find("default", id)
				return ss != nil && len(ss.Entries) == 2
			})
			// The owner dies: its liveness lapses and nothing more of its
			// leaves the node, so the map change below cannot be answered by
			// a fresh copy from it. The map then makes n2 the source a
			// takeover reads — v3 holds the shard on n2 and n3, in that
			// order.
			notAlive(h, "n1")
			h.nodes["n1"].rep.Stop()
			h.nodes["n1"].tr.Stop()
			h.waitFor("n2 to see the owner down", 3*time.Second, func() bool {
				return !h.nodes["n2"].rep.up("n1")
			})
			v3 := shardmap.Map{Version: 3, Basis: h.order, Replicas: 2, Shards: []shardmap.Shard{{Leader: "n2", Followers: []string{"n3"}}, {Leader: "n2", Followers: []string{"n3"}}}}
			h.applyMap(v3)
			h.waitFor("the survivors to reconcile the map without the owner", 3*time.Second, func() bool {
				return h.nodes["n2"].rep.Reconciled() == 3 && h.nodes["n3"].rep.Reconciled() == 3
			})
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			got, _, _ := h.nodes["n2"].rep.Fetch(ctx, "default", id, "n1")
			if got == nil {
				t.Fatal("the takeover found no copy at all")
			}
			if len(got.Entries) != c.entries {
				t.Fatalf("the takeover read %d queued messages, want %d", len(got.Entries), c.entries)
			}
		})
	}
}
