package replication

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

// TestAResyncKeepsARetainedWriteMadeAfterItsRead: n1 leads shard 0 and n2
// follows it. n1's resync of n2 has read the retained messages when a write of
// the same topic is stored and tapped; its barrier runs once the resync is
// queued, as a publisher's does after its journal's sync. The write must reach
// n2 behind the resync's older copy, and n2, leading the shard once n1 is lost,
// must serve it. The resync runs directly and as a broken stream's repair,
// during which no barrier to n2 passes. Without the push lock (the control) n2
// serves the older message.
func TestAResyncKeepsARetainedWriteMadeAfterItsRead(t *testing.T) {
	type tc struct {
		name      string
		later     string
		broken    bool
		unordered bool
	}
	var cases []tc
	for _, unordered := range []bool{false, true} {
		for _, c := range []tc{
			{"a newer message", "newer", false, false},
			{"a clear", "", false, false},
			{"a newer message, a broken stream's resync", "newer", true, false},
			{"a clear, a broken stream's resync", "", true, false},
		} {
			if unordered {
				c.name, c.unordered = "control: unordered, "+c.name, true
			}
			cases = append(cases, c)
		}
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := newHarnessWith(t, []string{"n1", "n2", "n3"}, 2, 2, func(o *Options) {
				quickAck(o)
				o.UnorderedRetainedPush = c.unordered
			})
			allAlive(h)
			h.applyMap(moveMap(h, 1, sh("n1", "n2"), sh("n2", "n3")))
			waitSettled(h, 1)
			tp := topicIn(2, 0)
			if err := writeRetained(h, "n1", tp, "old"); err != nil {
				t.Fatal(err)
			}
			h.waitFor("n2 to hold the old message", 5*time.Second, func() bool { return replicaHolds(h, "n2", tp) == "old" })
			n1 := h.nodes["n1"]
			reached, release := make(chan struct{}), make(chan struct{})
			first := sync.OnceFunc(func() { close(reached) })
			unblock := sync.OnceFunc(func() { close(release) })
			t.Cleanup(unblock)
			n1.rep.resyncRetainedGate = func() { first(); <-release }
			done := make(chan struct{})
			if c.broken {
				p := n1.rep.outboundFor("n2")
				p.broken()
				n1.rep.resync("n2")
				go func() {
					defer close(done)
					for deadline := time.Now().Add(10 * time.Second); p.holed() && time.Now().Before(deadline); {
						time.Sleep(5 * time.Millisecond)
					}
				}()
			} else {
				go func() {
					defer close(done)
					n1.rep.resyncPeer("n2")
				}()
			}
			waitClosed(t, "n1's resync of n2 to have read the retained messages", reached)
			stored := make(chan struct{})
			go func() {
				defer close(stored)
				m := &message.Message{Namespace: "default", Topic: tp, Payload: []byte(c.later), Retain: true}
				if n1.rep.StoreRetained(m) {
					n1.sessions.InjectRetained(m)
				}
			}()
			var during bool
			select {
			case <-stored:
				during = true
			case <-time.After(300 * time.Millisecond):
			}
			unblock()
			waitClosed(t, "the resync's pushes to be queued", done)
			waitClosed(t, "the later retained write to be stored", stored)
			if err := n1.rep.Barrier(context.Background(), []store.Scope{{Namespace: "default", Topic: tp}}); err != nil {
				t.Fatalf("the later retained write's barrier: %v", err)
			}
			replica := replicaHolds(h, "n2", tp)
			for _, peer := range []string{"n2", "n3"} {
				dropAll(h, "n1", peer, 21)
			}
			declare(h, "n1")
			h.applyMap(shardmap.Map{Version: 2, Basis: []string{"n2", "n3"}, Replicas: 2, Shards: []shardmap.Shard{sh("n2", "n3"), sh("n3", "n2")}})
			waitSettled(h, 2)
			served := ownHolds(h, "n2", tp)
			if c.unordered {
				if replica != "old" || served != "old" || !during {
					t.Fatalf("control: n2 holds %q and serves %q (stored during the hold: %v), want the older message in both, stored during the hold", replica, served, during)
				}
				return
			}
			if replica != c.later || served != c.later {
				t.Fatalf("n2 holds %q after the resync and serves %q as the shard's next leader, want %q (the write was stored while the resync was held: %v)",
					replica, served, c.later, during)
			}
			if during {
				t.Fatal("the later write was stored while the resync was held: the resync's retained push does not hold the push lock")
			}
		})
	}
}

// TestAResyncPushesOnlyTheShardsThePeerFollowsFromThisNode: n1 leads shard 0
// (n2 follows) and n2 leads shard 1 (n3 follows). n1 also holds its own copy of
// a shard-1 topic, as an old leader keeps one. n1's resync of n3 must push
// neither a message of shard 0, which n3 does not follow, nor its copy of
// shard 1, which n1 does not lead and which would replace the leader's.
func TestAResyncPushesOnlyTheShardsThePeerFollowsFromThisNode(t *testing.T) {
	h := newHarnessWith(t, []string{"n1", "n2", "n3"}, 2, 2, quickAck)
	allAlive(h)
	h.applyMap(moveMap(h, 1, sh("n1", "n2"), sh("n2", "n3")))
	waitSettled(h, 1)
	tp0, tp1 := topicIn(2, 0), topicIn(2, 1)
	if err := writeRetained(h, "n1", tp0, "n1's"); err != nil {
		t.Fatal(err)
	}
	if err := writeRetained(h, "n2", tp1, "leader's"); err != nil {
		t.Fatal(err)
	}
	h.waitFor("n3 to hold shard 1's message", 5*time.Second, func() bool { return replicaHolds(h, "n3", tp1) == "leader's" })
	n1 := h.nodes["n1"]
	n1.sessions.InjectRetained(&message.Message{Namespace: "default", Topic: tp1, Payload: []byte("n1's own"), Retain: true})
	if ownHolds(h, "n1", tp1) != "n1's own" {
		t.Fatal("precondition: n1 does not hold its own copy of the shard-1 topic")
	}
	n1.rep.resyncPeer("n3")
	if err := n1.rep.outboundFor("n3").wait(context.Background(), n1.rep.up, nil); err != nil {
		t.Fatalf("waiting for n3 to acknowledge the resync: %v", err)
	}
	if got := replicaHolds(h, "n3", tp0); got != "" {
		t.Fatalf("n3 holds %q for a topic of shard 0, which it does not follow", got)
	}
	if got := replicaHolds(h, "n3", tp1); got != "leader's" {
		t.Fatalf("n3 holds %q for shard 1's topic, want the leader's message: n1 pushed a shard it does not lead", got)
	}
}
