package replication

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/lease"
	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/store"
)

func levelRefusals(sinks map[string]*counters, node string) float64 {
	return sinks[node].get("hoardmq_cluster_feature_level_refusals_total", map[string]string{"where": "frame"})
}

// TestFrameAboveTheReceiversBuildIsRefused: a frame carries the feature level
// its sender read, and a follower whose build does not understand that level
// refuses it — the key is not held there and a barrier over it fails — while
// a frame at a level it understands is applied.
func TestFrameAboveTheReceiversBuildIsRefused(t *testing.T) {
	var cluster atomic.Uint64 // the level n1 reads and stamps
	cluster.Store(2)
	h, sinks := withCountersAnd(t, []string{"n1", "n2"}, 2, 2, func(o *Options) {
		if o.Node == "n1" {
			// A sender whose build understands every level it reads.
			o.Level, o.BuildLevel = cluster.Load, func() uint64 { return 2 }
		}
		// n2 has no BuildLevel: a build that understands level 1.
	})
	h.applyMap(shardmap.Compute(1, h.order, 2, 2))
	waitSettled(h, 1)
	n1 := h.nodes["n1"]

	above := clientIn(2, 0)
	h.setLease("default", above, "n1", 1)
	if err := n1.own.SessionUpsert(store.SessionMeta{Namespace: "default", ClientID: above, Epoch: 1}); err != nil {
		t.Fatal(err)
	}
	h.waitFor("n2 to refuse the frame stamped level 2", 5*time.Second, func() bool {
		return levelRefusals(sinks, "n2") >= 1
	})
	if ss := h.nodes["n2"].rep.ReplicaState().Find("default", above); ss != nil {
		t.Fatalf("n2 holds a session whose frame it does not understand: %+v", ss)
	}
	err := n1.rep.Barrier(context.Background(), []store.Scope{{Namespace: "default", ClientID: above}})
	if !errors.Is(err, ErrReplicaUnavailable) || barrierReason(err) != "not-held" {
		t.Fatalf("barrier over the refused session: %v (reason %q), want not-held", err, barrierReason(err))
	}

	cluster.Store(1)
	refusedBefore := levelRefusals(sinks, "n2")
	within := clientIn(2, 0, "w")
	h.setLease("default", within, "n1", 1)
	if err := n1.own.SessionUpsert(store.SessionMeta{Namespace: "default", ClientID: within, Epoch: 1}); err != nil {
		t.Fatal(err)
	}
	h.waitFor("n2 to hold the session sent at level 1", 5*time.Second, func() bool {
		return h.nodes["n2"].rep.ReplicaState().Find("default", within) != nil
	})
	if err := n1.rep.Barrier(context.Background(), []store.Scope{{Namespace: "default", ClientID: within}}); err != nil {
		t.Fatalf("barrier over a session n2 understands: %v", err)
	}
	if got := levelRefusals(sinks, "n2"); got != refusedBefore {
		t.Fatalf("a frame at level 1 was counted as a level refusal (%v → %v)", refusedBefore, got)
	}
}

// TestBelowLevelMemberIsLeftOutOfTheBasis: a member whose latest alive
// advertises a level below the cluster's is not placed, whatever that does to
// the copy count; it is still alive, so the owners' confirmations reach it.
func TestBelowLevelMemberIsLeftOutOfTheBasis(t *testing.T) {
	h := newHarness(t, []string{"n1", "n2", "n3"}, 2, 3)
	n1 := h.nodes["n1"]
	for _, name := range h.order {
		n1.leases.nodes[name] = lease.NodeInfo{AliveIndex: 1}
	}
	n1.leases.below = map[string]bool{"n3": true}
	alive, leaving, below := n1.rep.placementFacts()
	if !slices.Equal(alive, []string{"n1", "n2", "n3"}) || len(leaving) != 0 || !slices.Equal(below, []string{"n3"}) {
		t.Fatalf("facts: alive %v leaving %v below %v", alive, leaving, below)
	}
	if got := placementBasis(placeable(alive, below), leaving, 3); !slices.Equal(got, []string{"n1", "n2"}) {
		t.Fatalf("basis %v with n3 below the level and three copies, want [n1 n2]", got)
	}
}

// TestBelowLevelSenderStampsItsBuild: a sender whose build is below the
// level its table reads stamps its build's level, the semantics it applied;
// a receiver of that build accepts the frame.
func TestBelowLevelSenderStampsItsBuild(t *testing.T) {
	h, sinks := withCountersAnd(t, []string{"n1", "n2"}, 2, 2, func(o *Options) {
		o.Level = func() uint64 { return 3 }
		o.BuildLevel = func() uint64 { return 2 }
	})
	h.applyMap(shardmap.Compute(1, h.order, 2, 2))
	waitSettled(h, 1)
	n1 := h.nodes["n1"]
	id := clientIn(2, 0)
	h.setLease("default", id, "n1", 1)
	if err := n1.own.SessionUpsert(store.SessionMeta{Namespace: "default", ClientID: id, Epoch: 1}); err != nil {
		t.Fatal(err)
	}
	h.waitFor("n2 to hold the session", 5*time.Second, func() bool {
		return h.nodes["n2"].rep.ReplicaState().Find("default", id) != nil
	})
	if got := levelRefusals(sinks, "n2"); got != 0 {
		t.Fatalf("n2 refused %v frames for their level", got)
	}
}
