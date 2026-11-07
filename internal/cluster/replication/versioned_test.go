package replication

import (
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

// replicaTombstone reports whether a node's replica store holds a tombstone
// for a topic.
func replicaTombstone(h *harness, node, topic string) bool {
	_, held, tomb := h.nodes[node].rep.ReplicaState().RetainedHeld("default", topic)
	return held && tomb
}

// TestCopyDropIsNotFannedOut: a copy drop at the shard's leader (a delete
// with no version) removes only its own copy; the follower keeps what it
// holds. The control fans it out, as before copy drops and clears were told
// apart, and the follower's copy goes.
func TestCopyDropIsNotFannedOut(t *testing.T) {
	for _, c := range []struct {
		name    string
		control bool
	}{{"kept", false}, {"control: fanned out", true}} {
		t.Run(c.name, func(t *testing.T) {
			h, _ := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, func(o *Options) {
				quickAck(o)
				o.FanOutCopyDrops = c.control
			})
			allAlive(h)
			h.applyMap(moveMap(h, 1, sh("n1", "n2"), sh("n2", "n3")))
			waitSettled(h, 1)
			tp := topicIn(2, 0)
			if err := writeRetained(h, "n1", tp, "x"); err != nil {
				t.Fatal(err)
			}
			h.waitFor("n2 to hold the copy", 5*time.Second, func() bool { return replicaHolds(h, "n2", tp) == "x" })
			h.nodes["n1"].sessions.InjectRetained(&message.Message{Namespace: "default", Topic: tp})
			time.Sleep(300 * time.Millisecond)
			got := replicaHolds(h, "n2", tp)
			if c.control {
				if got != "" {
					t.Fatalf("control: n2 holds %q after a fanned-out copy drop", got)
				}
				return
			}
			if got != "x" {
				t.Fatalf("n2 holds %q after the leader's copy drop, want x: a copy drop is the node's alone", got)
			}
		})
	}
}

// TestPushAndResyncCarryTombstones: a clear at the leader is a tombstone that
// a new follower's whole push and a resync both carry, so the follower holds
// it too.
func TestPushAndResyncCarryTombstones(t *testing.T) {
	h, sinks := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, quickAck)
	allAlive(h)
	h.applyMap(moveMap(h, 1, sh("n1", "n2"), sh("n2", "n3")))
	waitSettled(h, 1)
	tp := topicIn(2, 0)
	if err := writeRetained(h, "n1", tp, "x"); err != nil {
		t.Fatal(err)
	}
	if err := writeRetained(h, "n1", tp, ""); err != nil {
		t.Fatal(err)
	}
	h.waitFor("n2 to hold the tombstone", 5*time.Second, func() bool { return replicaTombstone(h, "n2", tp) })
	h.applyMap(moveMap(h, 2, sh("n1", "n3"), sh("n2", "n3")))
	waitSettled(h, 2)
	h.waitFor("n3, the new follower, to hold the tombstone from the push", 5*time.Second, func() bool { return replicaTombstone(h, "n3", tp) })
	// A resync replaces nothing it carries with less: drop n3's tombstone by
	// a copy drop, then resync it from n1.
	if _, err := h.nodes["n3"].rep.follower.apply(store.RetainRecord(&message.Message{Namespace: "default", Topic: tp})); err != nil {
		t.Fatal(err)
	}
	h.nodes["n1"].rep.resyncPeer("n3")
	h.waitFor("n3 to hold the tombstone from the resync", 5*time.Second, func() bool { return replicaTombstone(h, "n3", tp) })
	// Pushes and resyncs arrive as appends, and are counted on that path.
	for node, least := range map[string]float64{"n2": 2, "n3": 2} {
		if got := sinks[node].get("hoardmq_retained_merges_total", map[string]string{"path": "append", "outcome": "newer"}); got < least {
			t.Errorf("%s counted %v newer appends, want at least %v", node, got, least)
		}
	}
}

// TestHandOverToAPromotedFollowerAfterItsClear: n1 leads shard 0 and n2
// follows, holding "old". v2 makes n2 the leader with n1 following, so n1, the
// old leader, hands its copy over. BeforeRetainedHandOver holds that hand-over
// until n2 has acknowledged a client's clear of the topic: the clear stays.
func TestHandOverToAPromotedFollowerAfterItsClear(t *testing.T) {
	reached := make(chan struct{}, 1)
	release := make(chan struct{})
	var once sync.Once
	open := func() { once.Do(func() { close(release) }) }
	h, _ := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, func(o *Options) {
		quickAck(o)
		if o.Node == "n1" {
			o.BeforeRetainedHandOver = func(leader string) {
				if leader != "n2" {
					return
				}
				select {
				case reached <- struct{}{}:
				default:
				}
				<-release
			}
		}
	})
	t.Cleanup(open)
	allAlive(h)
	h.applyMap(moveMap(h, 1, sh("n1", "n2"), sh("n2", "n3")))
	waitSettled(h, 1)
	tp := topicIn(2, 0)
	if err := writeRetained(h, "n1", tp, "old"); err != nil {
		t.Fatal(err)
	}
	h.waitFor("n2 to hold the copy", 5*time.Second, func() bool { return replicaHolds(h, "n2", tp) == "old" })
	h.applyMap(moveMap(h, 2, sh("n2", "n1"), sh("n2", "n3")))
	select {
	case <-reached:
	case <-time.After(5 * time.Second):
		t.Fatal("precondition: n1 never began its hand-over to n2")
	}
	h.waitFor("n2 to reconcile v2 with no move pending", 5*time.Second, func() bool {
		return h.nodes["n2"].rep.Reconciled() == 2 && h.nodes["n2"].rep.RetainedMovesPending() == 0
	})
	if err := writeRetained(h, "n2", tp, ""); err != nil {
		t.Fatalf("n2 refused the client's clear while the hand-over was owed: %v", err)
	}
	open()
	time.Sleep(time.Second)
	if got := ownHolds(h, "n2", tp); got != "" {
		t.Fatalf("n2 holds %q after n1's hand-over; the client's acknowledged clear was undone", got)
	}
}
