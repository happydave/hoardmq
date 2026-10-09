package replication

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/lease"
	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

func TestRetainedMovePredicates(t *testing.T) {
	for _, c := range []struct {
		name                   string
		leads, holds, orphaned bool
		release, mapV          uint64
		want                   bool
	}{
		{"a leader keeps its own store", true, true, false, 5, 5, false},
		{"given the shard back by a later map", false, true, false, 5, 7, false},
		{"a new follower's release before its push", false, true, false, 5, 5, true},
		{"not yet at the release's map", false, true, false, 5, 4, true},
		{"an old holder", false, false, false, 5, 5, true},
		{"kept for a releaser that is gone", false, false, true, 5, 5, false},
	} {
		if got := releaseDrops(c.leads, c.holds, c.orphaned, false, c.release, c.mapV); got != c.want {
			t.Errorf("releaseDrops %s = %v, want %v", c.name, got, c.want)
		}
	}
	if releaseDrops(false, true, false, true, 5, 5) || releaseDrops(false, false, false, true, 5, 5) {
		t.Error("releaseDrops: a copy owed to the release's sender is kept")
	}
	for _, c := range []struct {
		name                string
		note                bool
		acted, release      uint64
		heldThen, senderLed bool
		want                bool
	}{
		{"the note owes the sender", true, 5, 5, false, false, true},
		{"held, the sender did not, map not acted on", false, 4, 5, true, false, true},
		{"the sender led it", false, 4, 5, true, true, false},
		{"the sender only followed it", false, 4, 5, true, false, true},
		{"this node did not hold it", false, 4, 5, false, false, false},
		{"the release's map acted on, no note", false, 5, 5, true, false, false},
		{"no map acted on", false, 0, 5, false, false, true},
	} {
		if got := owesHandOver(c.note, c.acted, c.release, c.heldThen, c.senderLed); got != c.want {
			t.Errorf("owesHandOver %s = %v, want %v", c.name, got, c.want)
		}
	}
	for _, c := range []struct {
		received, note    uint64
		leads, holds      bool
		wantAct, wantDrop bool
	}{
		{5, 5, false, false, true, true},
		{6, 5, false, false, true, true},
		{4, 5, false, false, false, false},
		{0, 5, false, false, false, false},
		{0, 0, false, false, false, false},
		{5, 5, true, false, true, false},
		{5, 5, false, true, true, false},
	} {
		act, drop := releasedEarlier(c.received, c.note, c.leads, c.holds)
		if act != c.wantAct || drop != c.wantDrop {
			t.Errorf("releasedEarlier(%d, %d, leads %v, holds %v) = %v, %v; want %v, %v", c.received, c.note, c.leads, c.holds, act, drop, c.wantAct, c.wantDrop)
		}
	}
	for _, c := range []struct {
		pending       bool
		cur, rec      uint64
		changed, want bool
	}{
		{true, 3, 3, false, true},
		{false, 4, 3, true, true},
		{false, 4, 3, false, false},
		{false, 3, 3, true, false},
	} {
		if got := movingShard(c.pending, c.cur, c.rec, c.changed); got != c.want {
			t.Errorf("movingShard(%v, %d, %d, %v) = %v, want %v", c.pending, c.cur, c.rec, c.changed, got, c.want)
		}
	}
	for _, c := range []struct {
		alive, leads, leaderAlive bool
		want                      keptVerdict
	}{
		{true, true, false, promoteRetained},
		{false, true, false, promoteRetained},
		{false, false, true, handOnRetained},
		{false, false, false, keepRetained}, // the map still names the gone releaser
		{true, false, true, keepRetained},
	} {
		if got := judgeKept(c.alive, c.leads, c.leaderAlive); got != c.want {
			t.Errorf("judgeKept(%v, %v, %v) = %v, want %v", c.alive, c.leads, c.leaderAlive, got, c.want)
		}
	}
	if !installHandOver(false) || installHandOver(true) {
		t.Error("installHandOver: a hand-over installs only where the leader holds nothing")
	}
	for _, c := range []struct {
		held, in bool
		out      int
		want     bool
	}{
		{true, true, 0, true},
		{false, true, 0, false},
		{true, false, 0, false},
		{true, true, 1, false},
	} {
		if got := moveDone(c.held, c.in, c.out); got != c.want {
			t.Errorf("moveDone(%v, %v, %d) = %v, want %v", c.held, c.in, c.out, got, c.want)
		}
	}
}

// moveMap is a map over the three nodes with the given shards.
func moveMap(h *harness, version uint64, shards ...shardmap.Shard) shardmap.Map {
	return shardmap.Map{Version: version, Basis: h.order, Replicas: 2, Shards: shards}
}

func sh(leader string, followers ...string) shardmap.Shard {
	return shardmap.Shard{Leader: leader, Followers: followers}
}

// writeRetained writes a retained message from a node, stored there when it
// leads the topic's shard and forwarded to the leader otherwise, and waits as a
// publish does.
func writeRetained(h *harness, node, topic, payload string) error {
	n := h.nodes[node]
	m := &message.Message{Namespace: "default", Topic: topic, Payload: []byte(payload), Retain: true}
	if n.rep.StoreRetained(m) {
		n.sessions.InjectRetained(m)
	}
	return n.rep.Barrier(context.Background(), []store.Scope{{Namespace: "default", Topic: topic}})
}

// ownHolds reports what a node's own retained store holds for a topic.
func ownHolds(h *harness, node, topic string) string {
	m, ok := h.nodes[node].sessions.has("default", topic)
	if !ok {
		return ""
	}
	return string(m.Payload)
}

// replicaHolds reports what a node's replica store holds for a topic.
func replicaHolds(h *harness, node, topic string) string {
	for _, m := range h.nodes[node].rep.ReplicaState().Retained {
		if m.Namespace == "default" && m.Topic == topic {
			return string(m.Payload)
		}
	}
	return ""
}

// declare marks a node declared in every node's lease table.
func declare(h *harness, name string) {
	for _, n := range h.nodes {
		n.leases.mu.Lock()
		info := n.leases.nodes[name]
		info.DeadAt = 1
		n.leases.nodes[name] = info
		n.leases.mu.Unlock()
	}
}

// revive clears a node's declaration in every node's lease table.
func revive(h *harness, name string) {
	for _, n := range h.nodes {
		n.leases.mu.Lock()
		n.leases.nodes[name] = lease.NodeInfo{Alive: time.Now()}
		n.leases.mu.Unlock()
	}
}

// keptMovedRetained is how many shards a node counted as kept for a move.
func keptMovedRetained(sinks map[string]*counters, node string) float64 {
	return sinks[node].get("hoardmq_replicas_kept_total", map[string]string{"reason": "retained-moved"})
}

// followerMoves runs the leader-kept move: under v1 n1 leads shard 0 and n2
// follows, a retained message is written and n2 holds it; then every frame
// n1 sends n3 is lost and v2 moves shard 0's follower to n3. It returns the
// topic.
func followerMoves(t *testing.T, h *harness) string {
	t.Helper()
	allAlive(h)
	h.applyMap(moveMap(h, 1, sh("n1", "n2"), sh("n2", "n3")))
	waitSettled(h, 1)
	tp := topicIn(2, 0)
	if err := writeRetained(h, "n1", tp, "kept"); err != nil {
		t.Fatalf("precondition: the retained write under v1: %v", err)
	}
	h.waitFor("n2 to hold the copy under v1", 5*time.Second, func() bool { return replicaHolds(h, "n2", tp) == "kept" })
	dropAll(h, "n1", "n3", 11)
	h.applyMap(moveMap(h, 2, sh("n1", "n3"), sh("n2", "n3")))
	waitSettled(h, 2)
	return tp
}

// TestOldFollowerKeepsUntilReleased: the leader keeps the shard and its push
// to the new follower is lost. The old follower keeps its copy, the leader
// refuses retained writes on the shard while the move is pending (and not on a
// shard that did not move), and once the push lands the old follower is
// released.
func TestOldFollowerKeepsUntilReleased(t *testing.T) {
	h, sinks := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, quickAck)
	tp := followerMoves(t, h)
	if replicaHolds(h, "n2", tp) != "kept" {
		t.Fatal("n2 dropped its copy before n3 held the shard")
	}
	if got := keptMovedRetained(sinks, "n2"); got != 1 || h.nodes["n2"].rep.KeptRetained() != 1 {
		t.Fatalf("n2 kept{retained-moved} %v, KeptRetained %d; want 1 and 1", got, h.nodes["n2"].rep.KeptRetained())
	}
	if h.nodes["n1"].rep.RetainedMovesPending() != 1 {
		t.Fatalf("n1 has %d moves pending, want 1", h.nodes["n1"].rep.RetainedMovesPending())
	}
	err := writeRetained(h, "n1", topicIn(2, 0), "during")
	if barrierReason(err) != "moving" {
		t.Fatalf("a retained write on the moving shard: %v (reason %q), want moving", err, barrierReason(err))
	}
	if err := writeRetained(h, "n2", topicIn(2, 1), "still"); err != nil {
		t.Fatalf("a retained write on a shard that did not move: %v", err)
	}
	h.nodes["n1"].tr.SetFault("n3", nil)
	h.waitFor("n3 to hold the shard and n2 to be released", 10*time.Second, func() bool {
		return replicaHolds(h, "n3", tp) != "" && replicaHolds(h, "n2", tp) == "" && h.nodes["n1"].rep.RetainedMovesPending() == 0
	})
	if got := dropped(sinks, "n2", "retained-released"); got < 1 {
		t.Fatalf("n2 dropped{retained-released} %v, want at least 1", got)
	}
	if err := writeRetained(h, "n1", tp, "after"); err != nil {
		t.Fatalf("a retained write once the move completed: %v", err)
	}
}

// TestKeptRetainedCopyReachesTheNextLeader: the leader dies with the move
// pending; under the next map n3 leads the shard. n2 hands its kept copy to
// n3. The control keeps it on n2, and n3 serves nothing.
func TestKeptRetainedCopyReachesTheNextLeader(t *testing.T) {
	for _, c := range []struct {
		name    string
		control bool
	}{{"handed on", false}, {"control: not handed on", true}} {
		t.Run(c.name, func(t *testing.T) {
			h, sinks := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, func(o *Options) {
				quickAck(o)
				o.KeepRetainedNotHandedOn = c.control
			})
			tp := followerMoves(t, h)
			for _, peer := range []string{"n2", "n3"} {
				dropAll(h, "n1", peer, 12)
			}
			declare(h, "n1")
			h.applyMap(shardmap.Map{Version: 3, Basis: []string{"n2", "n3"}, Replicas: 2, Shards: []shardmap.Shard{sh("n3", "n2"), sh("n2", "n3")}})
			waitSettled(h, 3)
			if c.control {
				time.Sleep(time.Second)
				if ownHolds(h, "n3", tp) != "" {
					t.Fatal("control: n3 holds the message although nothing handed it on")
				}
				return
			}
			h.waitFor("n3 to hold the message", 5*time.Second, func() bool { return ownHolds(h, "n3", tp) == "kept" })
			if got := dropped(sinks, "n2", "retained-handed-on"); got != 0 {
				t.Fatalf("n2, a follower of the shard again, dropped{retained-handed-on} %v, want 0", got)
			}
			h.waitFor("n2 to hold the message as n3's follower", 5*time.Second, func() bool { return replicaHolds(h, "n2", tp) == "kept" })
			if h.nodes["n2"].rep.KeptRetained() != 0 {
				t.Fatalf("n2 still keeps %d shards", h.nodes["n2"].rep.KeptRetained())
			}
		})
	}
}

// TestHandOnDoesNotReplaceANewerMessage: as above, but n3 holds a newer
// message for the topic before n2's hand-on arrives, and nothing for a second
// topic n2 kept: the newer message stays, the missing one is installed.
func TestHandOnDoesNotReplaceANewerMessage(t *testing.T) {
	h, _ := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, quickAck)
	allAlive(h)
	h.applyMap(moveMap(h, 1, sh("n1", "n2"), sh("n2", "n3")))
	waitSettled(h, 1)
	tp, other := topicIn(2, 0), topicIn2(2, 0, 1)
	for _, w := range []string{tp, other} {
		if err := writeRetained(h, "n1", w, "old"); err != nil {
			t.Fatal(err)
		}
	}
	h.waitFor("n2 to hold both copies", 5*time.Second, func() bool {
		return replicaHolds(h, "n2", tp) == "old" && replicaHolds(h, "n2", other) == "old"
	})
	dropAll(h, "n1", "n3", 13)
	h.applyMap(moveMap(h, 2, sh("n1", "n3"), sh("n2", "n3")))
	waitSettled(h, 2)
	// n3 is given a newer message for tp before it leads, as a write the
	// former leader made would reach it.
	h.nodes["n3"].sessions.InjectRetained(&message.Message{Namespace: "default", Topic: tp, Payload: []byte("newer"), Retain: true})
	for _, peer := range []string{"n2", "n3"} {
		dropAll(h, "n1", peer, 14)
	}
	declare(h, "n1")
	h.applyMap(shardmap.Map{Version: 3, Basis: []string{"n2", "n3"}, Replicas: 2, Shards: []shardmap.Shard{sh("n3", "n2"), sh("n2", "n3")}})
	waitSettled(h, 3)
	h.waitFor("n3 to hold the topic it lacked", 5*time.Second, func() bool { return ownHolds(h, "n3", other) == "old" })
	if got := ownHolds(h, "n3", tp); got != "newer" {
		t.Fatalf("n3 holds %q for the topic it had a newer message for, want newer", got)
	}
}

// TestOldLeaderKeepsItsOwnCopyUntilReleased: a promotion. Under v2 n2, n1's
// follower, leads and n3 follows; n1 no longer holds the shard. n2's push to
// n3 is lost, so n1 keeps its own copy until n2 releases it.
func TestOldLeaderKeepsItsOwnCopyUntilReleased(t *testing.T) {
	h, sinks := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, quickAck)
	allAlive(h)
	h.applyMap(moveMap(h, 1, sh("n1", "n2"), sh("n2", "n3")))
	waitSettled(h, 1)
	tp := topicIn(2, 0)
	if err := writeRetained(h, "n1", tp, "kept"); err != nil {
		t.Fatal(err)
	}
	h.waitFor("n2 to hold the copy", 5*time.Second, func() bool { return replicaHolds(h, "n2", tp) == "kept" })
	dropAll(h, "n2", "n3", 15)
	h.applyMap(moveMap(h, 2, sh("n2", "n3"), sh("n2", "n3")))
	waitSettled(h, 2)
	if ownHolds(h, "n1", tp) != "kept" || keptMovedRetained(sinks, "n1") != 1 {
		t.Fatalf("n1 holds %q, kept{retained-moved} %v; want its own copy kept", ownHolds(h, "n1", tp), keptMovedRetained(sinks, "n1"))
	}
	h.nodes["n2"].tr.SetFault("n3", nil)
	h.waitFor("n3 to hold the shard and n1 to be released", 10*time.Second, func() bool {
		return replicaHolds(h, "n3", tp) == "kept" && ownHolds(h, "n1", tp) == ""
	})
}

// TestOldFollowerKeepsThroughALeaderMove: under v2 n3, which held nothing,
// leads and n1 follows; n2 no longer holds the shard. n2 keeps its copy until
// n3 has n1's hand-over and n1, its follower, holds it.
func TestOldFollowerKeepsThroughALeaderMove(t *testing.T) {
	h, sinks := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, quickAck)
	allAlive(h)
	h.applyMap(moveMap(h, 1, sh("n1", "n2"), sh("n2", "n3")))
	waitSettled(h, 1)
	tp := topicIn(2, 0)
	if err := writeRetained(h, "n1", tp, "kept"); err != nil {
		t.Fatal(err)
	}
	h.waitFor("n2 to hold the copy", 5*time.Second, func() bool { return replicaHolds(h, "n2", tp) == "kept" })
	dropAll(h, "n1", "n3", 16)
	h.applyMap(moveMap(h, 2, sh("n3", "n1"), sh("n2", "n3")))
	waitSettled(h, 2)
	if replicaHolds(h, "n2", tp) != "kept" || keptMovedRetained(sinks, "n2") != 1 {
		t.Fatalf("n2 holds %q, kept{retained-moved} %v; want its copy kept while n1's hand-over is lost", replicaHolds(h, "n2", tp), keptMovedRetained(sinks, "n2"))
	}
	h.nodes["n1"].tr.SetFault("n3", nil)
	h.waitFor("n3 to lead the message and n2 to be released", 10*time.Second, func() bool {
		return ownHolds(h, "n3", tp) == "kept" && replicaHolds(h, "n2", tp) == "" && replicaHolds(h, "n1", tp) == "kept"
	})
}

// TestReleaseBeforePushClearsAKeptCopy: n2 keeps topic a from a move whose
// release to it is lost; the leader deletes a; a later map makes n2 a
// follower again. Its copy then is the leader's: no a.
func TestReleaseBeforePushClearsAKeptCopy(t *testing.T) {
	h, _ := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, quickAck)
	allAlive(h)
	h.applyMap(moveMap(h, 1, sh("n1", "n2"), sh("n2", "n3")))
	waitSettled(h, 1)
	a := topicIn(2, 0)
	if err := writeRetained(h, "n1", a, "kept"); err != nil {
		t.Fatal(err)
	}
	h.waitFor("n2 to hold a", 5*time.Second, func() bool { return replicaHolds(h, "n2", a) == "kept" })
	dropAll(h, "n1", "n2", 17)
	h.applyMap(moveMap(h, 2, sh("n1", "n3"), sh("n2", "n3")))
	waitSettled(h, 2)
	// The release to n2 is lost; n2 is passed over by declaring it, so the
	// move completes and the leader takes writes. n2 stays declared, so no
	// release owed to a passed-over node reaches it: only the next map's
	// release-then-push can clear its copy.
	declare(h, "n2")
	h.waitFor("n1's move to complete without n2", 5*time.Second, func() bool { return h.nodes["n1"].rep.RetainedMovesPending() == 0 })
	if err := writeRetained(h, "n1", a, ""); err != nil {
		t.Fatalf("the delete of a: %v", err)
	}
	if replicaHolds(h, "n2", a) != "kept" {
		t.Fatal("precondition: n2 should still keep a")
	}
	h.nodes["n1"].tr.SetFault("n2", nil)
	time.Sleep(300 * time.Millisecond)
	if replicaHolds(h, "n2", a) != "kept" {
		t.Fatal("precondition: n2 should keep a until the next map")
	}
	h.applyMap(moveMap(h, 3, sh("n1", "n2"), sh("n2", "n3")))
	waitSettled(h, 3)
	h.waitFor("n2 to hold the leader's copy of the shard, without a", 10*time.Second, func() bool {
		return replicaHolds(h, "n2", a) == "" && h.nodes["n1"].rep.RetainedMovesPending() == 0
	})
}

// TestPushIsOrderedAgainstTaps: a retained write runs while the push to a new
// follower holds its lock, between the push's read and its enqueue. The new
// follower ends with the newer message. Without the lock (the control) the
// older copy is pushed after the newer record and replaces it.
func TestPushIsOrderedAgainstTaps(t *testing.T) {
	for _, c := range []struct {
		name      string
		unordered bool
		want      string
	}{{"ordered", false, "newer"}, {"control: unordered", true, "old"}} {
		t.Run(c.name, func(t *testing.T) {
			h, _ := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, func(o *Options) {
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
			n1 := h.nodes["n1"]
			var once sync.Once
			wrote := make(chan struct{})
			n1.rep.pushGate = func() {
				once.Do(func() {
					go func() {
						defer close(wrote)
						m := &message.Message{Namespace: "default", Topic: tp, Payload: []byte("newer"), Retain: true}
						n1.sessions.InjectRetained(m)
					}()
					if c.unordered {
						// Without the lock the write's tap runs now, before
						// the push is enqueued.
						<-wrote
					} else {
						// With it the write's tap waits for the push.
						time.Sleep(100 * time.Millisecond)
					}
				})
			}
			h.applyMap(moveMap(h, 2, sh("n1", "n3"), sh("n2", "n3")))
			waitSettled(h, 2)
			<-wrote
			h.waitFor("n3 to settle", 5*time.Second, func() bool { return replicaHolds(h, "n3", tp) != "" && n1.rep.RetainedMovesPending() == 0 })
			time.Sleep(200 * time.Millisecond)
			if got := replicaHolds(h, "n3", tp); got != c.want {
				t.Fatalf("n3 holds %q, want %q (n1 holds %q)", got, c.want, ownHolds(h, "n1", tp))
			}
		})
	}
}

// TestRestartKeepsANotedRetainedCopy: n2 keeps a copy; its notes are read
// back from their file and a rebalance from no map runs, as at a restart. It
// still keeps the copy. With the file gone first (the control), it drops it,
// as a restart did before notes existed.
func TestRestartKeepsANotedRetainedCopy(t *testing.T) {
	for _, c := range []struct {
		name     string
		lostNote bool
	}{{"noted", false}, {"control: note lost", true}} {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			h, _ := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, func(o *Options) {
				quickAck(o)
				if o.Node == "n2" {
					o.ReplicaDir = dir
				}
			})
			tp := followerMoves(t, h)
			r := h.nodes["n2"].rep
			if c.lostNote {
				if err := os.Remove(filepath.Join(dir, keptRetainedFile)); err != nil {
					t.Fatal(err)
				}
			}
			r.rmoves.mu.Lock()
			r.rmoves.kept = r.readKeptRetained()
			r.rmoves.mu.Unlock()
			r.rebalance(shardmap.Map{}, r.Map().Current())
			got := replicaHolds(h, "n2", tp)
			if c.lostNote && got != "" {
				t.Fatalf("control: n2 holds %q with no note after the restart, want nothing", got)
			}
			if !c.lostNote && got != "kept" {
				t.Fatalf("n2 holds %q after the restart, want its kept copy", got)
			}
		})
	}
}

// TestRestartedLeaderReleasesEveryone: n1 forgets its pending move, as a
// restart does, and reconciles from no map. Once n3 holds the shard, n1
// releases every other member, n2's kept copy with it.
func TestRestartedLeaderReleasesEveryone(t *testing.T) {
	h, _ := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, quickAck)
	tp := followerMoves(t, h)
	r := h.nodes["n1"].rep
	r.rmoves.mu.Lock()
	r.rmoves.pending = map[int]*pendingMove{}
	r.rmoves.mu.Unlock()
	h.nodes["n1"].tr.SetFault("n3", nil)
	time.Sleep(200 * time.Millisecond)
	if replicaHolds(h, "n2", tp) != "kept" {
		t.Fatal("precondition: n2 should still keep its copy once n1 has forgotten the move")
	}
	r.rebalance(shardmap.Map{}, r.Map().Current())
	// The rebalance ran here, not on the reconcile worker, which waits for a
	// wake to look at the move it recorded.
	select {
	case r.wake <- struct{}{}:
	default:
	}
	h.waitFor("n2 to be released", 10*time.Second, func() bool { return replicaHolds(h, "n2", tp) == "" })
	if replicaHolds(h, "n3", tp) != "kept" {
		t.Fatal("n3 does not hold the shard")
	}
}

// TestPassedOverHolderIsReleasedOnReturn: n2 is declared while the move is
// pending, so n1 completes it without n2's release; n2 keeps. Once n2 is
// alive again n1 releases it.
func TestPassedOverHolderIsReleasedOnReturn(t *testing.T) {
	h, _ := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, quickAck)
	allAlive(h)
	h.applyMap(moveMap(h, 1, sh("n1", "n2"), sh("n2", "n3")))
	waitSettled(h, 1)
	tp := topicIn(2, 0)
	if err := writeRetained(h, "n1", tp, "kept"); err != nil {
		t.Fatal(err)
	}
	h.waitFor("n2 to hold the copy", 5*time.Second, func() bool { return replicaHolds(h, "n2", tp) == "kept" })
	dropAll(h, "n1", "n2", 18)
	declare(h, "n2")
	h.applyMap(moveMap(h, 2, sh("n1", "n3"), sh("n2", "n3")))
	waitSettled(h, 2)
	h.waitFor("n1's move to complete without n2", 5*time.Second, func() bool { return h.nodes["n1"].rep.RetainedMovesPending() == 0 })
	if replicaHolds(h, "n2", tp) != "kept" {
		t.Fatal("precondition: n2 should keep its copy while passed over")
	}
	h.nodes["n1"].tr.SetFault("n2", nil)
	revive(h, "n2")
	h.waitFor("n2 to be released on its return", 10*time.Second, func() bool { return replicaHolds(h, "n2", tp) == "" })
}

// TestOlderReceiverOfTheRelease: n2 runs the older build. It drops its copy
// at its rebalance, as before, and acknowledges n1's release unapplied, so
// n1's move completes.
func TestOlderReceiverOfTheRelease(t *testing.T) {
	h, sinks := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, func(o *Options) {
		quickAck(o)
		o.PreRetainedMoves = o.Node == "n2"
	})
	allAlive(h)
	h.applyMap(moveMap(h, 1, sh("n1", "n2"), sh("n2", "n3")))
	waitSettled(h, 1)
	tp := topicIn(2, 0)
	if err := writeRetained(h, "n1", tp, "kept"); err != nil {
		t.Fatal(err)
	}
	h.waitFor("n2 to hold the copy", 5*time.Second, func() bool { return replicaHolds(h, "n2", tp) == "kept" })
	h.applyMap(moveMap(h, 2, sh("n1", "n3"), sh("n2", "n3")))
	waitSettled(h, 2)
	h.waitFor("n1's move to complete with n2's acknowledgement", 5*time.Second, func() bool { return h.nodes["n1"].rep.RetainedMovesPending() == 0 })
	if replicaHolds(h, "n2", tp) != "" || keptMovedRetained(sinks, "n2") != 0 {
		t.Fatal("the older n2 kept its copy; an older build drops it at its rebalance")
	}
	if sinks["n1"].get("hoardmq_retained_releases_total", map[string]string{"direction": "out"}) < 1 {
		t.Fatal("n1 sent n2 no release")
	}
}

// TestOlderReceiverOfTheHandOverMark: n3, the new leader, runs the older
// build. n1's hand-over, marked, is applied as a write and acknowledged, so n3
// holds the message. n1 keeps its own copy, since an older leader sends no
// release: the stale keep a mixed pair is known to leave.
func TestOlderReceiverOfTheHandOverMark(t *testing.T) {
	h, sinks := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, func(o *Options) {
		quickAck(o)
		o.PreRetainedMoves = o.Node == "n3"
	})
	allAlive(h)
	h.applyMap(moveMap(h, 1, sh("n1", "n2"), sh("n2", "n3")))
	waitSettled(h, 1)
	tp := topicIn(2, 0)
	if err := writeRetained(h, "n1", tp, "kept"); err != nil {
		t.Fatal(err)
	}
	h.applyMap(moveMap(h, 2, sh("n3", "n2"), sh("n2", "n3")))
	waitSettled(h, 2)
	h.waitFor("n3 to hold the handed-over message", 5*time.Second, func() bool { return ownHolds(h, "n3", tp) == "kept" })
	if got := sinks["n1"].get("hoardmq_replicas_kept_total", map[string]string{"reason": "handover-unacknowledged"}); got != 0 {
		t.Fatalf("n1's hand-over to the older n3 went unacknowledged %v times", got)
	}
	time.Sleep(500 * time.Millisecond)
	if ownHolds(h, "n1", tp) != "kept" {
		t.Fatal("n1 dropped its own copy although an older leader sends no release")
	}
}

// topicIn2 is the nth topic (from 0) whose shard is the given one.
func topicIn2(shards, want, nth int) string {
	for i := 0; ; i++ {
		t := fmt.Sprintf("t/%d", i)
		if shardmap.ShardOf("default", t, shards) == want {
			if nth == 0 {
				return t
			}
			nth--
		}
	}
}

// TestMoveWaitsForTheOldHoldersRelease: n3 holds the shard but n1's release
// to n2, alive, is lost. The move stays pending, and the shard refuses writes,
// until n2 acknowledges its release: a write taken before would be missing
// from the copy n2 still keeps.
func TestMoveWaitsForTheOldHoldersRelease(t *testing.T) {
	h, _ := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, quickAck)
	allAlive(h)
	h.applyMap(moveMap(h, 1, sh("n1", "n2"), sh("n2", "n3")))
	waitSettled(h, 1)
	tp := topicIn(2, 0)
	if err := writeRetained(h, "n1", tp, "kept"); err != nil {
		t.Fatal(err)
	}
	h.waitFor("n2 to hold the copy", 5*time.Second, func() bool { return replicaHolds(h, "n2", tp) == "kept" })
	dropAll(h, "n1", "n2", 19)
	h.applyMap(moveMap(h, 2, sh("n1", "n3"), sh("n2", "n3")))
	waitSettled(h, 2)
	h.waitFor("n3 to hold the shard", 5*time.Second, func() bool { return replicaHolds(h, "n3", tp) == "kept" })
	time.Sleep(300 * time.Millisecond)
	if h.nodes["n1"].rep.RetainedMovesPending() != 1 || replicaHolds(h, "n2", tp) != "kept" {
		t.Fatalf("n1 has %d moves pending and n2 holds %q; want the move pending on n2's release", h.nodes["n1"].rep.RetainedMovesPending(), replicaHolds(h, "n2", tp))
	}
	if err := writeRetained(h, "n1", tp, "during"); barrierReason(err) != "moving" {
		t.Fatalf("a write while n2's release is outstanding: %v (reason %q), want moving", err, barrierReason(err))
	}
	h.nodes["n1"].tr.SetFault("n2", nil)
	h.waitFor("n2 to be released and the move to complete", 10*time.Second, func() bool {
		return replicaHolds(h, "n2", tp) == "" && h.nodes["n1"].rep.RetainedMovesPending() == 0
	})
}

// TestStartReadsTheKeptRetainedNotes: a replicator started on a replica
// directory holding notes keeps them, as a restarted node does before its
// first reconcile.
func TestStartReadsTheKeptRetainedNotes(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, keptRetainedFile), []byte(`{"0":{"releaser":"n1","version":2,"since":"2026-10-08T00:00:00Z"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	h, _ := withCountersAnd(t, []string{"n1", "n2"}, 2, 2, func(o *Options) {
		if o.Node == "n2" {
			o.ReplicaDir = dir
		}
	})
	note, ok := h.nodes["n2"].rep.keptNote(0)
	if !ok || note.Releaser != "n1" || note.Version != 2 || h.nodes["n2"].rep.KeptRetained() != 1 {
		t.Fatalf("n2's note %+v (%v), kept %d; want the note read at start", note, ok, h.nodes["n2"].rep.KeptRetained())
	}
}

// TestLostHandOnIsMadeAgain: the leader dies mid-move and n2's hand-on to n3,
// the next leader, is lost. n2 keeps its note and makes the hand-on again once
// its frames reach n3.
func TestLostHandOnIsMadeAgain(t *testing.T) {
	h, _ := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, quickAck)
	tp := followerMoves(t, h)
	for _, peer := range []string{"n2", "n3"} {
		dropAll(h, "n1", peer, 20)
	}
	dropAll(h, "n2", "n3", 21)
	declare(h, "n1")
	h.applyMap(shardmap.Map{Version: 3, Basis: []string{"n2", "n3"}, Replicas: 2, Shards: []shardmap.Shard{sh("n3", "n2"), sh("n2", "n3")}})
	waitSettled(h, 3)
	time.Sleep(time.Second)
	if ownHolds(h, "n3", tp) != "" || h.nodes["n2"].rep.KeptRetained() != 1 {
		t.Fatalf("precondition: n3 holds %q, n2 keeps %d; want nothing handed on yet and the note kept", ownHolds(h, "n3", tp), h.nodes["n2"].rep.KeptRetained())
	}
	h.nodes["n2"].tr.SetFault("n3", nil)
	h.waitFor("n3 to hold the message once the hand-on is made again", 10*time.Second, func() bool { return ownHolds(h, "n3", tp) == "kept" })
}

// TestDeliveredNeedsAnAcknowledgementAndNoFailure: a frame counts as held only
// when the peer's acknowledgement covers it and no send at or above it failed;
// a break marks every frame sent so far failed, though it also moves the
// acknowledged mark past them.
func TestDeliveredNeedsAnAcknowledgementAndNoFailure(t *testing.T) {
	p := &outbound{name: "peer"}
	p.cond = sync.NewCond(&p.mu)
	for i := 0; i < 5; i++ {
		p.push(frame{Kind: kindResyncMark})
	}
	if done, failed := p.delivered(3); done || failed {
		t.Fatalf("before any acknowledgement: done %v failed %v, want neither", done, failed)
	}
	p.acked(3)
	if done, failed := p.delivered(3); !done || failed {
		t.Fatalf("acknowledged: done %v failed %v, want done", done, failed)
	}
	if done, _ := p.delivered(4); done {
		t.Fatal("a frame past the acknowledgement counted as held")
	}
	p.broken()
	if done, failed := p.delivered(5); done || !failed {
		t.Fatalf("after a break: done %v failed %v, want failed", done, failed)
	}
	if done, failed := p.delivered(0); done || failed {
		t.Fatal("sequence 0 names no frame")
	}
}
