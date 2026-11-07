package replication

import (
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
)

// TestRefusedWriteLeavesNothingToMaskALaterOne: four brokers. n1 leads
// shard 0 and n2 follows; n1 stops, and n3, which holds nothing of the shard,
// takes a retained write of x, refused since the leader is down. Under v2 n2
// leads with n4, n3 holding nothing of the shard, and n2 acknowledges a
// client's write of y. Under v3 n3 leads, and n2 hands y over. A subscriber
// must be sent y, the acknowledged write: nothing the refused write left on
// n3 may keep it out, and it leaves nothing there.
func TestRefusedWriteLeavesNothingToMaskALaterOne(t *testing.T) {
	h, _ := withCountersAnd(t, []string{"n1", "n2", "n3", "n4"}, 2, 2, quickAck)
	allAlive(h)
	h.applyMap(moveMap(h, 1, sh("n1", "n2"), sh("n3", "n4")))
	waitSettled(h, 1)
	tp := topicIn(2, 0)
	h.nodes["n1"].rep.Stop()
	h.nodes["n1"].tr.Stop()
	h.waitFor("n3 to see n1 down", 5*time.Second, func() bool { return !h.nodes["n3"].rep.up("n1") })
	if err := writeRetained(h, "n3", tp, "x"); err == nil {
		t.Fatal("precondition: the write with the shard's leader down was acknowledged")
	}
	leftover := ownHolds(h, "n3", tp)
	t.Logf("after the refused write: n3 own %q", leftover)
	declare(h, "n1")
	live := []string{"n2", "n3", "n4"}
	reconcile := func(v shardmap.Map) {
		h.applyMap(v)
		h.waitFor("the live nodes to reconcile with no move pending", 5*time.Second, func() bool {
			for _, n := range live {
				if h.nodes[n].rep.Reconciled() != v.Version || h.nodes[n].rep.RetainedMovesPending() != 0 {
					return false
				}
			}
			return true
		})
	}
	basis := []string{"n2", "n3", "n4"}
	reconcile(shardmap.Map{Version: 2, Basis: basis, Replicas: 2, Shards: []shardmap.Shard{sh("n2", "n4"), sh("n3", "n4")}})
	if err := writeRetained(h, "n2", tp, "y"); err != nil {
		t.Fatalf("precondition: n2's write under v2: %v", err)
	}
	reconcile(shardmap.Map{Version: 3, Basis: basis, Replicas: 2, Shards: []shardmap.Shard{sh("n3", "n2"), sh("n3", "n4")}})
	time.Sleep(500 * time.Millisecond)
	got := h.nodes["n2"].rep.QueryRetained("default", tp, time.Now())
	payload := ""
	if len(got) == 1 {
		payload = string(got[0].Payload)
	}
	t.Logf("under v3 (n3 leads): n3 own %q, n2 replica %q, n2 own %q; a query at n2 finds %d (%q)",
		ownHolds(h, "n3", tp), replicaHolds(h, "n2", tp), ownHolds(h, "n2", tp), len(got), payload)
	if payload != "y" {
		t.Fatalf("a subscriber is sent %q, want y, the acknowledged write", payload)
	}
	if leftover != "" {
		t.Fatalf("the refused write left %q in n3's own store", leftover)
	}
}
