package replication

import (
	"context"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/lease"
	"github.com/happydave/hoardmq/internal/cluster/shardmap"
)

func TestOwnerAliveByLog(t *testing.T) {
	for _, c := range []struct{ live, inBasis, want bool }{
		{true, true, true},
		{true, false, false},
		{false, true, false},
		{false, false, false},
	} {
		if got := ownerAliveByLog(c.live, c.inBasis); got != c.want {
			t.Errorf("ownerAliveByLog(%v, %v) = %v, want %v", c.live, c.inBasis, got, c.want)
		}
	}
}

// TestCopyKeptWhenTheMapHasDroppedAnOwnerNobodyHasDeclared: the owner died,
// the leader left it out of the next shard map, and the reassignment that
// declares it has not applied yet, so every lease table still reads it alive.
// The holder that stops holding the shard hands its copy on, is acknowledged,
// and still keeps it: the owner is feeding nobody, and this may be the last
// copy.
func TestCopyKeptWhenTheMapHasDroppedAnOwnerNobodyHasDeclared(t *testing.T) {
	h, sinks := withCountersAnd(t, []string{"n1", "n2", "n3", "n4"}, 2, 2, quickAck)
	v1 := shardmap.Map{Version: 1, Basis: h.order, Replicas: 2, Shards: []shardmap.Shard{{Leader: "n1", Followers: []string{"n2"}}, {Leader: "n3", Followers: []string{"n4"}}}}
	v2 := shardmap.Map{Version: 2, Basis: []string{"n2", "n3", "n4"}, Replicas: 2, Shards: []shardmap.Shard{{Leader: "n3"}, {Leader: "n3", Followers: []string{"n4"}}}}
	h.applyMap(v1)
	waitSettled(h, 1)
	id := clientIn(2, 0)
	h.writeSession("n1", id, 1)
	if err := h.nodes["n1"].barrier(context.Background()); err != nil {
		t.Fatal(err)
	}
	if h.nodes["n2"].rep.ReplicaState().Find("default", id) == nil {
		t.Fatal("n2 does not hold the copy under v1")
	}
	for _, n := range h.nodes {
		n.leases.mu.Lock()
		n.leases.nodes["n1"] = lease.NodeInfo{Alive: time.Now(), AliveIndex: 1}
		n.leases.mu.Unlock()
	}
	h.applyMap(v2)
	waitSettled(h, 2)
	if h.nodes["n2"].rep.ReplicaState().Find("default", id) == nil {
		t.Fatal("n2 dropped its copy of a session whose owner the shard map no longer names")
	}
	if got := sinks["n2"].get("hoardmq_replicas_kept_total", map[string]string{"reason": "owner-not-alive"}); got < 1 {
		t.Fatalf("kept for owner-not-alive counted %v, want at least 1", got)
	}
}
