package replication

import (
	"context"
	"testing"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

// TestLeaderRefusesAForwardedRetainedWritePastItsLimit: a retained write
// published on n1 and forwarded to its leader n2, whose namespace is at its
// retained-topic limit, fails n1's barrier over the topic, and n2 stores
// nothing.
func TestLeaderRefusesAForwardedRetainedWritePastItsLimit(t *testing.T) {
	h := newHarness(t, []string{"n1", "n2", "n3"}, 3, 2)
	h.applyMap(shardmap.Compute(1, h.order, 2, 3)) // shard i: leader n(i+1), follower n(i+2 mod 3)
	waitSettled(h, 1)
	topic := topicIn(3, 1)
	scope := store.Scope{Namespace: "default", Topic: topic}
	n1, n2 := h.nodes["n1"], h.nodes["n2"]
	n2.sessions.full.Store(true)
	if n1.rep.StoreRetained(&message.Message{Namespace: "default", Topic: topic, Payload: []byte("a"), Retain: true}) {
		t.Fatal("precondition: n1 stored a write n2 leads")
	}
	if err := n1.barrier(context.Background(), scope); err == nil {
		t.Fatal("n1's barrier passed over a write n2's limit refused")
	}
	if _, has := n2.sessions.has("default", topic); has {
		t.Fatal("n2 stored a write its limit refused")
	}
}

// TestRetainedHandOverIsNotDecidedByTheLimit: a shard's new leader installs
// the hand-over of a message the cluster already holds even while its
// namespace is at its retained-topic limit.
func TestRetainedHandOverIsNotDecidedByTheLimit(t *testing.T) {
	h, sinks := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, quickAck)
	_, n3Holds, _ := retainedHandOver(t, h, sinks, func() { h.nodes["n3"].sessions.full.Store(true) })
	if !n3Holds {
		t.Fatal("the new leader at its limit did not install the hand-over")
	}
}

// TestRejectClassRetainedLimit: the leader's refusal of a write past its
// retained-topic limit has its own class.
func TestRejectClassRetainedLimit(t *testing.T) {
	if got := rejectClass(retainedLimitReason); got != "retained-limit" {
		t.Fatalf("rejectClass(%q) = %q, want retained-limit", retainedLimitReason, got)
	}
}
