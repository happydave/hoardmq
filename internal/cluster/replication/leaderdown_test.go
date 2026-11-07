package replication

import (
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
)

// TestRetainedWriteWhileTheShardLeaderIsDown: n1 leads shard 0 and n2
// follows it. n1 stops, and before any map drops it n3, which holds nothing of
// the shard, takes a retained write to it. The write must either be refused,
// served by no later map, n3 leading the shard among them, or reach the
// shard's holders: once the map drops n1 and n2 leads the shard, n2 must serve
// it.
func TestRetainedWriteWhileTheShardLeaderIsDown(t *testing.T) {
	h, sinks := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, quickAck)
	allAlive(h)
	h.applyMap(moveMap(h, 1, sh("n1", "n2"), sh("n2", "n3")))
	waitSettled(h, 1)
	tp := topicIn(2, 0)
	h.nodes["n1"].rep.Stop()
	h.nodes["n1"].tr.Stop()
	h.waitFor("n2 and n3 to see n1 down", 5*time.Second, func() bool {
		return !h.nodes["n2"].rep.up("n1") && !h.nodes["n3"].rep.up("n1")
	})
	err := writeRetained(h, "n3", tp, "x")
	down := sinks["n3"].get("hoardmq_retained_forwards_total", map[string]string{"outcome": "leader-down"})
	seen := h.nodes["n2"].rep.QueryRetained("default", tp, time.Now())
	t.Logf("n3's write with the leader down: err=%v reason=%q; n3 forwards{leader-down} %v; n3 own %q, n3 replica %q, n2 own %q, n2 replica %q; a query at n2 finds %d",
		err, barrierReason(err), down, ownHolds(h, "n3", tp), replicaHolds(h, "n3", tp), ownHolds(h, "n2", tp), replicaHolds(h, "n2", tp), len(seen))
	if err != nil {
		// Refused: no map serves it, once n1 is dropped (n2 leads) nor once
		// n3 leads the shard.
		declare(h, "n1")
		for _, v := range []shardmap.Map{
			{Version: 2, Basis: []string{"n2", "n3"}, Replicas: 2, Shards: []shardmap.Shard{sh("n2", "n3"), sh("n2", "n3")}},
			{Version: 3, Basis: []string{"n2", "n3"}, Replicas: 2, Shards: []shardmap.Shard{sh("n3", "n2"), sh("n2", "n3")}},
		} {
			h.applyMap(v)
			h.waitFor("n2 and n3 to reconcile", 5*time.Second, func() bool {
				return h.nodes["n2"].rep.Reconciled() == v.Version && h.nodes["n3"].rep.Reconciled() == v.Version
			})
			time.Sleep(500 * time.Millisecond)
			found := len(h.nodes["n2"].rep.QueryRetained("default", tp, time.Now()))
			t.Logf("refused write, under v%d (%s leads): n2 own %q, n2 replica %q, n3 own %q, n3 replica %q; a query at n2 finds %d",
				v.Version, v.Shards[0].Leader, ownHolds(h, "n2", tp), replicaHolds(h, "n2", tp), ownHolds(h, "n3", tp), replicaHolds(h, "n3", tp), found)
			if found != 0 {
				t.Fatalf("under v%d a subscriber is sent the refused write", v.Version)
			}
		}
		return
	}
	declare(h, "n1")
	h.applyMap(shardmap.Map{Version: 2, Basis: []string{"n2", "n3"}, Replicas: 2, Shards: []shardmap.Shard{sh("n2", "n3"), sh("n2", "n3")}})
	h.waitFor("n2 and n3 to reconcile v2", 5*time.Second, func() bool {
		return h.nodes["n2"].rep.Reconciled() == 2 && h.nodes["n3"].rep.Reconciled() == 2
	})
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && ownHolds(h, "n2", tp) != "x" {
		time.Sleep(20 * time.Millisecond)
	}
	seen = h.nodes["n3"].rep.QueryRetained("default", tp, time.Now())
	t.Logf("under v2 (n2 leads): n2 own %q, n2 replica %q, n3 own %q, n3 replica %q; a query at n3 finds %d",
		ownHolds(h, "n2", tp), replicaHolds(h, "n2", tp), ownHolds(h, "n3", tp), replicaHolds(h, "n3", tp), len(seen))
	if ownHolds(h, "n2", tp) != "x" {
		t.Fatal("an acknowledged retained write never reached the shard's leader")
	}
}

// TestRefusedWriteWhileTheLeaderIsDownThenANewerOne: as above, n3's write
// of x is refused with n1 down, and its copy stays in n3's own store. Under
// v2 n2 leads and acknowledges a client's write of y; under v3 n3 leads and n2
// hands y over. A subscriber must be sent y, the acknowledged write.
func TestRefusedWriteWhileTheLeaderIsDownThenANewerOne(t *testing.T) {
	h, _ := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, quickAck)
	allAlive(h)
	h.applyMap(moveMap(h, 1, sh("n1", "n2"), sh("n2", "n3")))
	waitSettled(h, 1)
	tp := topicIn(2, 0)
	h.nodes["n1"].rep.Stop()
	h.nodes["n1"].tr.Stop()
	h.waitFor("n3 to see n1 down", 5*time.Second, func() bool { return !h.nodes["n3"].rep.up("n1") })
	if err := writeRetained(h, "n3", tp, "x"); err == nil {
		t.Fatal("precondition: the write with the shard's leader down was acknowledged")
	}
	declare(h, "n1")
	reconcile := func(v shardmap.Map) {
		h.applyMap(v)
		h.waitFor("n2 and n3 to reconcile", 5*time.Second, func() bool {
			return h.nodes["n2"].rep.Reconciled() == v.Version && h.nodes["n3"].rep.Reconciled() == v.Version &&
				h.nodes["n2"].rep.RetainedMovesPending() == 0 && h.nodes["n3"].rep.RetainedMovesPending() == 0
		})
	}
	reconcile(shardmap.Map{Version: 2, Basis: []string{"n2", "n3"}, Replicas: 2, Shards: []shardmap.Shard{sh("n2", "n3"), sh("n2", "n3")}})
	if err := writeRetained(h, "n2", tp, "y"); err != nil {
		t.Fatalf("precondition: n2's write under v2: %v", err)
	}
	reconcile(shardmap.Map{Version: 3, Basis: []string{"n2", "n3"}, Replicas: 2, Shards: []shardmap.Shard{sh("n3", "n2"), sh("n2", "n3")}})
	time.Sleep(500 * time.Millisecond)
	got := h.nodes["n2"].rep.QueryRetained("default", tp, time.Now())
	payload := ""
	if len(got) == 1 {
		payload = string(got[0].Payload)
	}
	t.Logf("under v3 (n3 leads): n3 own %q, n2 replica %q, n3 replica %q; a query at n2 finds %d (%q)",
		ownHolds(h, "n3", tp), replicaHolds(h, "n2", tp), replicaHolds(h, "n3", tp), len(got), payload)
	if payload != "y" {
		t.Fatalf("a subscriber is sent %q, want y, the acknowledged write", payload)
	}
}
