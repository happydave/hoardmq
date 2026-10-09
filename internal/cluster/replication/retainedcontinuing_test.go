package replication

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

// lose cuts a node off as a stopped one is: every replication frame it sends
// vanishes, and every node's lease table declares it.
func lose(h *harness, name string, seed int64) {
	for _, peer := range h.order {
		if peer != name {
			dropAll(h, name, peer, seed)
		}
	}
	declare(h, name)
}

// releasesIn is how many releases a node has received.
func releasesIn(sinks map[string]*counters, node string) float64 {
	return sinks[node].get("hoardmq_retained_releases_total", map[string]string{"direction": "in"})
}

// TestFollowerCopyReachesALeaderThatHeldNothing: under v1 n1 leads shard 0 and
// n2 follows, and n2 holds an acknowledged retained message. n1 is lost; under
// v2 over n2 and n3, n3, which held nothing of the shard, leads it and n2 still
// follows. n2's copy is the only one, so it must reach n3, whichever of n3's
// release and n2's own rebalance comes first.
func TestFollowerCopyReachesALeaderThatHeldNothing(t *testing.T) {
	for _, c := range []struct {
		name string
		// releaseFirst holds n2's rebalance until n3's release has reached
		// n2; otherwise n3's rebalance is held until n2's hand-over is in.
		releaseFirst bool
	}{{"the new leader's release first", true}, {"the follower's hand-over first", false}} {
		t.Run(c.name, func(t *testing.T) {
			h, sinks := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, quickAck)
			allAlive(h)
			h.applyMap(moveMap(h, 1, sh("n1", "n2"), sh("n2", "n3")))
			waitSettled(h, 1)
			tp := topicIn(2, 0)
			if err := writeRetained(h, "n1", tp, "kept"); err != nil {
				t.Fatalf("precondition: the retained write under v1: %v", err)
			}
			h.waitFor("n2 to hold the copy under v1", 5*time.Second, func() bool { return replicaHolds(h, "n2", tp) == "kept" })
			lose(h, "n1", 21)
			release := make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			t.Cleanup(unblock)
			held := "n3"
			if c.releaseFirst {
				held = "n2"
			}
			h.nodes[held].rep.fanOutGate = func() { <-release }
			before := releasesIn(sinks, "n2")
			h.applyMap(shardmap.Map{Version: 2, Basis: []string{"n2", "n3"}, Replicas: 2, Shards: []shardmap.Shard{sh("n3", "n2"), sh("n2", "n3")}})
			if c.releaseFirst {
				h.waitFor("n3's release to reach n2", 5*time.Second, func() bool { return releasesIn(sinks, "n2") > before })
				t.Logf("n2's copy after n3's release, before n2's rebalance: %q", replicaHolds(h, "n2", tp))
			} else {
				h.waitFor("n2 to reconcile v2", 5*time.Second, func() bool { return h.nodes["n2"].rep.Reconciled() == 2 })
				t.Logf("n3's own copy after n2's rebalance, before n3's: %q", ownHolds(h, "n3", tp))
			}
			unblock()
			h.waitFor("n2 and n3 to reconcile v2", 5*time.Second, func() bool {
				return h.nodes["n2"].rep.Reconciled() == 2 && h.nodes["n3"].rep.Reconciled() == 2
			})
			deadline := time.Now().Add(3 * time.Second)
			for time.Now().Before(deadline) && (ownHolds(h, "n3", tp) != "kept" || replicaHolds(h, "n2", tp) != "kept") {
				time.Sleep(20 * time.Millisecond)
			}
			if ownHolds(h, "n3", tp) != "kept" || replicaHolds(h, "n2", tp) != "kept" {
				t.Fatalf("after v2: n3 leads holding %q, n2 follows holding %q; want the acknowledged message on both", ownHolds(h, "n3", tp), replicaHolds(h, "n2", tp))
			}
		})
	}
}

// TestFollowerCopyThroughAPromotion: four nodes, three copies. Under v1 n1
// leads shard 0 and n2 and n3 follow. n1 is lost; under v2 n2 leads, n3 still
// follows and n4 is new. n3 stays a holder throughout, and the message is held
// by every holder after the move.
func TestFollowerCopyThroughAPromotion(t *testing.T) {
	h, _ := withCountersAnd(t, []string{"n1", "n2", "n3", "n4"}, 2, 3, quickAck)
	allAlive(h)
	h.applyMap(shardmap.Map{Version: 1, Basis: h.order, Replicas: 3, Shards: []shardmap.Shard{sh("n1", "n2", "n3"), sh("n2", "n3", "n4")}})
	waitSettled(h, 1)
	tp := topicIn(2, 0)
	if err := writeRetained(h, "n1", tp, "kept"); err != nil {
		t.Fatalf("precondition: the retained write under v1: %v", err)
	}
	h.waitFor("n2 and n3 to hold the copy under v1", 5*time.Second, func() bool {
		return replicaHolds(h, "n2", tp) == "kept" && replicaHolds(h, "n3", tp) == "kept"
	})
	lose(h, "n1", 22)
	h.applyMap(shardmap.Map{Version: 2, Basis: []string{"n2", "n3", "n4"}, Replicas: 3, Shards: []shardmap.Shard{sh("n2", "n3", "n4"), sh("n3", "n4", "n2")}})
	h.waitFor("the survivors to reconcile v2", 5*time.Second, func() bool {
		for _, n := range []string{"n2", "n3", "n4"} {
			if h.nodes[n].rep.Reconciled() != 2 {
				return false
			}
		}
		return true
	})
	h.waitFor("every holder to hold the message", 5*time.Second, func() bool {
		return ownHolds(h, "n2", tp) == "kept" && replicaHolds(h, "n3", tp) == "kept" && replicaHolds(h, "n4", tp) == "kept"
	})
}

// TestFollowerCopyWhileTheLeaderStays: four nodes, three copies. Under v1 n1
// leads shard 0 and n2 and n3 follow. n3 is lost; under v2 n1 still leads, n2
// still follows and n4 is new. The leader is whole, so its release and push
// leave every holder with the message.
func TestFollowerCopyWhileTheLeaderStays(t *testing.T) {
	h, _ := withCountersAnd(t, []string{"n1", "n2", "n3", "n4"}, 2, 3, quickAck)
	allAlive(h)
	h.applyMap(shardmap.Map{Version: 1, Basis: h.order, Replicas: 3, Shards: []shardmap.Shard{sh("n1", "n2", "n3"), sh("n2", "n3", "n4")}})
	waitSettled(h, 1)
	tp := topicIn(2, 0)
	if err := writeRetained(h, "n1", tp, "kept"); err != nil {
		t.Fatalf("precondition: the retained write under v1: %v", err)
	}
	h.waitFor("n2 and n3 to hold the copy under v1", 5*time.Second, func() bool {
		return replicaHolds(h, "n2", tp) == "kept" && replicaHolds(h, "n3", tp) == "kept"
	})
	lose(h, "n3", 23)
	h.applyMap(shardmap.Map{Version: 2, Basis: []string{"n1", "n2", "n4"}, Replicas: 3, Shards: []shardmap.Shard{sh("n1", "n2", "n4"), sh("n2", "n4", "n1")}})
	h.waitFor("the survivors to reconcile v2", 5*time.Second, func() bool {
		for _, n := range []string{"n1", "n2", "n4"} {
			if h.nodes[n].rep.Reconciled() != 2 {
				return false
			}
		}
		return true
	})
	h.waitFor("every holder to hold the message", 5*time.Second, func() bool {
		return ownHolds(h, "n1", tp) == "kept" && replicaHolds(h, "n2", tp) == "kept" && replicaHolds(h, "n4", tp) == "kept"
	})
}

// TestLeavingFollowerCopyReachesALeaderThatHeldNothing: four nodes, two
// copies. Under v1 n1 leads shard 0 and n2 follows. n1 is lost; under v2 n3,
// which held nothing of the shard, leads it and n4 follows: n2 holds the only
// copy and no longer holds the shard. n2's rebalance is held until n3's
// release reaches it; the message must still reach n3.
func TestLeavingFollowerCopyReachesALeaderThatHeldNothing(t *testing.T) {
	h, _ := withCountersAnd(t, []string{"n1", "n2", "n3", "n4"}, 2, 2, quickAck)
	allAlive(h)
	h.applyMap(shardmap.Map{Version: 1, Basis: h.order, Replicas: 2, Shards: []shardmap.Shard{sh("n1", "n2"), sh("n3", "n4")}})
	waitSettled(h, 1)
	tp := topicIn(2, 0)
	if err := writeRetained(h, "n1", tp, "kept"); err != nil {
		t.Fatalf("precondition: the retained write under v1: %v", err)
	}
	h.waitFor("n2 to hold the copy under v1", 5*time.Second, func() bool { return replicaHolds(h, "n2", tp) == "kept" })
	lose(h, "n1", 24)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	h.nodes["n2"].rep.fanOutGate = func() { <-release }
	h.applyMap(shardmap.Map{Version: 2, Basis: []string{"n2", "n3", "n4"}, Replicas: 2, Shards: []shardmap.Shard{sh("n3", "n4"), sh("n4", "n2")}})
	// n3's move completes once n2, an old holder, acknowledged its release.
	reached := true
	deadline := time.Now().Add(3 * time.Second)
	for h.nodes["n3"].rep.Reconciled() != 2 || h.nodes["n3"].rep.RetainedMovesPending() != 0 {
		if time.Now().After(deadline) {
			reached = false
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Logf("n3's release reached n2 before n2's rebalance: %v; n2's copy then: %q", reached, replicaHolds(h, "n2", tp))
	unblock()
	h.waitFor("the survivors to reconcile v2", 5*time.Second, func() bool {
		for _, n := range []string{"n2", "n3", "n4"} {
			if h.nodes[n].rep.Reconciled() != 2 {
				return false
			}
		}
		return true
	})
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && ownHolds(h, "n3", tp) != "kept" {
		time.Sleep(20 * time.Millisecond)
	}
	if ownHolds(h, "n3", tp) != "kept" {
		t.Fatalf("after v2: n3 leads holding %q, n4 holds %q, n2 holds %q; want the acknowledged message on n3", ownHolds(h, "n3", tp), replicaHolds(h, "n4", tp), replicaHolds(h, "n2", tp))
	}
	// n3's release came while n2 still owed it the hand-over; once the
	// hand-over is in, it is acted on.
	h.waitFor("n2, no longer a holder, to drop its copy on n3's earlier release", 3*time.Second, func() bool {
		return replicaHolds(h, "n2", tp) == "" && h.nodes["n2"].rep.KeptRetained() == 0
	})
}

// owedHandOver runs the three-node shape with n2's hand-over to n3 lost: under
// v1 n1 leads shard 0 and n2 follows; n1 is lost; under v2 n3 leads and n2
// follows, and every replication frame n2 sends n3 vanishes. It returns the
// topic once n2 has reconciled v2.
func owedHandOver(t *testing.T, h *harness) string {
	t.Helper()
	allAlive(h)
	h.applyMap(moveMap(h, 1, sh("n1", "n2"), sh("n2", "n3")))
	waitSettled(h, 1)
	tp := topicIn(2, 0)
	if err := writeRetained(h, "n1", tp, "kept"); err != nil {
		t.Fatalf("precondition: the retained write under v1: %v", err)
	}
	h.waitFor("n2 to hold the copy under v1", 5*time.Second, func() bool { return replicaHolds(h, "n2", tp) == "kept" })
	lose(h, "n1", 25)
	dropAll(h, "n2", "n3", 26)
	h.applyMap(shardmap.Map{Version: 2, Basis: []string{"n2", "n3"}, Replicas: 2, Shards: []shardmap.Shard{sh("n3", "n2"), sh("n2", "n3")}})
	h.waitFor("n2 and n3 to reconcile v2", 5*time.Second, func() bool {
		return h.nodes["n2"].rep.Reconciled() == 2 && h.nodes["n3"].rep.Reconciled() == 2
	})
	return tp
}

// TestOwedHandOverIsMadeAgain: n2's hand-over to n3, which held nothing of
// the shard, is lost. n2 keeps its copy through n3's release, owing n3 the
// hand-over, and makes it again once its frames get through.
func TestOwedHandOverIsMadeAgain(t *testing.T) {
	h, sinks := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, quickAck)
	tp := owedHandOver(t, h)
	time.Sleep(time.Second)
	if ownHolds(h, "n3", tp) != "" || replicaHolds(h, "n2", tp) != "kept" || h.nodes["n2"].rep.KeptRetained() != 1 {
		t.Fatalf("with n2's hand-over lost: n3 holds %q, n2 holds %q and keeps %d shards; want nothing, kept, 1", ownHolds(h, "n3", tp), replicaHolds(h, "n2", tp), h.nodes["n2"].rep.KeptRetained())
	}
	// n2 has acted on v2, so only its note says it owes n3: a further
	// release from n3 must not drop the copy n3 does not hold.
	before := releasesIn(sinks, "n2")
	h.nodes["n3"].rep.pushRelease("n2", 2, 0)
	h.waitFor("n3's second release to reach n2", 3*time.Second, func() bool { return releasesIn(sinks, "n2") > before })
	if got := replicaHolds(h, "n2", tp); got != "kept" {
		t.Fatalf("n2 holds %q after a release from n3, which it still owes the hand-over; want kept", got)
	}
	h.nodes["n2"].tr.SetFault("n3", nil)
	h.waitFor("n3 to hold the message once n2's hand-over gets through", 3*time.Second, func() bool { return ownHolds(h, "n3", tp) == "kept" })
	h.waitFor("n2 to owe nothing, its copy a follower's", 3*time.Second, func() bool {
		return h.nodes["n2"].rep.KeptRetained() == 0 && replicaHolds(h, "n2", tp) == "kept"
	})
}

// TestOwedHandOverSurvivesARestart: as above, and n2's notes are read back
// from their file before its frames get through, as at a restart. The hand-over
// is still made. With the file gone first (the control), nothing owes n3 the
// hand-over and n3 never holds the message.
func TestOwedHandOverSurvivesARestart(t *testing.T) {
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
			tp := owedHandOver(t, h)
			r := h.nodes["n2"].rep
			if c.lostNote {
				if err := os.Remove(filepath.Join(dir, keptRetainedFile)); err != nil {
					t.Fatal(err)
				}
			}
			r.rmoves.mu.Lock()
			r.rmoves.kept = r.readKeptRetained()
			r.rmoves.mu.Unlock()
			h.nodes["n2"].tr.SetFault("n3", nil)
			if c.lostNote {
				time.Sleep(2 * time.Second)
				if got := ownHolds(h, "n3", tp); got != "" {
					t.Fatalf("control: n3 holds %q with n2's note lost, want nothing", got)
				}
				return
			}
			h.waitFor("n3 to hold the message after n2's notes were read back", 3*time.Second, func() bool { return ownHolds(h, "n3", tp) == "kept" })
		})
	}
}

// TestFollowerCopyThroughALeaderWhosePushWasLost: under v1 over n1 and n2 (three
// copies configured), n1 leads shard 0 and n2 follows. n3 joins at v2 as a new
// follower, and n1's push to it is lost. n1 is lost inside the move; under v3
// over n2 and n3, n3, which held the shard under v2 by the map but never
// received it, leads, and n2 still follows. n2's copy is the only one.
func TestFollowerCopyThroughALeaderWhosePushWasLost(t *testing.T) {
	h, sinks := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 3, quickAck)
	allAlive(h)
	h.applyMap(shardmap.Map{Version: 1, Basis: []string{"n1", "n2"}, Replicas: 3, Shards: []shardmap.Shard{sh("n1", "n2"), sh("n2", "n1")}})
	waitSettled(h, 1)
	tp := topicIn(2, 0)
	if err := writeRetained(h, "n1", tp, "kept"); err != nil {
		t.Fatalf("precondition: the retained write under v1: %v", err)
	}
	h.waitFor("n2 to hold the copy under v1", 5*time.Second, func() bool { return replicaHolds(h, "n2", tp) == "kept" })
	dropAll(h, "n1", "n3", 27)
	h.applyMap(shardmap.Map{Version: 2, Basis: []string{"n1", "n2", "n3"}, Replicas: 3, Shards: []shardmap.Shard{sh("n1", "n2", "n3"), sh("n2", "n3", "n1")}})
	waitSettled(h, 2)
	if replicaHolds(h, "n3", tp) != "" {
		t.Fatal("precondition: n3 holds the copy although n1's push to it was lost")
	}
	lose(h, "n1", 28)
	// n2's rebalance is held until n3's release has reached it, so the
	// release meets n2's copy before n2 hands it over.
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	h.nodes["n2"].rep.fanOutGate = func() { <-release }
	before := releasesIn(sinks, "n2")
	h.applyMap(shardmap.Map{Version: 3, Basis: []string{"n2", "n3"}, Replicas: 3, Shards: []shardmap.Shard{sh("n3", "n2"), sh("n2", "n3")}})
	h.waitFor("n3's release to reach n2", 5*time.Second, func() bool { return releasesIn(sinks, "n2") > before })
	unblock()
	h.waitFor("n2 and n3 to reconcile v3", 5*time.Second, func() bool {
		return h.nodes["n2"].rep.Reconciled() == 3 && h.nodes["n3"].rep.Reconciled() == 3
	})
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && (ownHolds(h, "n3", tp) != "kept" || replicaHolds(h, "n2", tp) != "kept") {
		time.Sleep(20 * time.Millisecond)
	}
	if ownHolds(h, "n3", tp) != "kept" || replicaHolds(h, "n2", tp) != "kept" {
		t.Fatalf("after v3: n3 leads holding %q, n2 follows holding %q; want the acknowledged message on both", ownHolds(h, "n3", tp), replicaHolds(h, "n2", tp))
	}
}

func TestKeptJudgementPredicates(t *testing.T) {
	if ownRetainedMayGo(true) {
		t.Error("ownRetainedMayGo: a node's own copies of a shard it leads now must not go")
	}
	if !ownRetainedMayGo(false) {
		t.Error("ownRetainedMayGo: a node that does not lead the shard may drop its own copies")
	}
	at := time.Unix(100, 0)
	base := retainedNote{Releaser: "n3", Version: 2, Own: true, Since: at}
	for _, c := range []struct {
		name string
		b    retainedNote
		want bool
	}{
		{"the same keep", base, true},
		{"another releaser", retainedNote{Releaser: "n2", Version: 2, Own: true, Since: at}, false},
		{"another map", retainedNote{Releaser: "n3", Version: 3, Own: true, Since: at}, false},
		{"own or not", retainedNote{Releaser: "n3", Version: 2, Since: at}, false},
		{"a hand-over owed", retainedNote{Releaser: "n3", Version: 2, Own: true, HandOver: true, Since: at}, false},
		{"noted again", retainedNote{Releaser: "n3", Version: 2, Own: true, Since: at.Add(time.Millisecond)}, false},
	} {
		if got := sameNote(base, c.b); got != c.want {
			t.Errorf("sameNote %s = %v, want %v", c.name, got, c.want)
		}
	}
}

// TestStaleJudgementKeepsANewLeadersCopies: n2 keeps replica copies of shard 0
// under a note whose releaser's release it has already received, so a judging
// pass drops them. The pass is held at the shard while v2 makes n2 the shard's
// leader. Released, it must not act on its snapshot and v1: n2's rebalance to
// v2 promotes the copy, and n2 leads holding it. With the stale judgement
// restored (the control), the copy is dropped first and n2 leads holding
// nothing.
func TestStaleJudgementKeepsANewLeadersCopies(t *testing.T) {
	for _, c := range []struct {
		name  string
		stale bool
	}{{"judged again", false}, {"control: stale judgement", true}} {
		t.Run(c.name, func(t *testing.T) {
			h, _ := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, func(o *Options) {
				quickAck(o)
				o.StaleKeptJudgement = c.stale
			})
			allAlive(h)
			h.applyMap(moveMap(h, 1, sh("n1", "n3"), sh("n3", "n1")))
			waitSettled(h, 1)
			tp := topicIn(2, 0)
			r := h.nodes["n2"].rep
			if _, err := r.follower.apply(store.RetainRecord(&message.Message{Namespace: "default", Topic: tp, Payload: []byte("kept"), Retain: true})); err != nil {
				t.Fatal(err)
			}
			held := make(chan struct{})
			release := make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			t.Cleanup(unblock)
			once := sync.Once{}
			r.judgeGate = func(shard int) {
				if shard == 0 {
					once.Do(func() { close(held); <-release })
				}
			}
			r.rmoves.mu.Lock()
			r.rmoves.received[0] = map[string]uint64{"n3": 1}
			r.rmoves.mu.Unlock()
			r.keepRetained(0, "n3", 1, false, false)
			// Wake the reconcile worker, which looks at kept shards only once
			// it knows one exists.
			select {
			case r.wake <- struct{}{}:
			default:
			}
			select {
			case <-held:
			case <-time.After(5 * time.Second):
				t.Fatal("precondition: no judging pass reached the kept shard")
			}
			h.applyMap(moveMap(h, 2, sh("n2", "n3"), sh("n3", "n1")))
			unblock()
			h.waitFor("n2 to reconcile v2", 5*time.Second, func() bool { return r.Reconciled() == 2 })
			time.Sleep(500 * time.Millisecond)
			got := ownHolds(h, "n2", tp)
			if c.stale {
				if got != "" {
					t.Fatalf("control: n2 leads holding %q with the stale judgement, want nothing", got)
				}
				return
			}
			if got != "kept" {
				t.Fatalf("n2, the shard's leader under v2, holds %q; want its kept copy promoted", got)
			}
		})
	}
}

// TestHandOnJudgedAgainAfterItsWait: n2 keeps replica copies of shard 0 for a
// releaser that is gone, so a judging pass hands them on to the shard's leader
// n3 and drops them once acknowledged. n2's frames to n3 are slowed, and while
// the pass waits for the acknowledgement n2 applies v2, under which it leads
// the shard. The pass must not drop on its old map: n2's rebalance promotes the
// copy, and n2 leads holding it. With the stale judgement (the control), the
// copy is dropped after the wait and n2 leads holding nothing.
func TestHandOnJudgedAgainAfterItsWait(t *testing.T) {
	for _, c := range []struct {
		name  string
		stale bool
	}{{"judged again", false}, {"control: stale judgement", true}} {
		t.Run(c.name, func(t *testing.T) {
			h, _ := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, func(o *Options) {
				o.AckTimeout = 2 * time.Second
				o.StaleKeptJudgement = c.stale
			})
			allAlive(h)
			h.applyMap(moveMap(h, 1, sh("n3", "n1"), sh("n1", "n3")))
			waitSettled(h, 1)
			tp := topicIn(2, 0)
			r := h.nodes["n2"].rep
			if _, err := r.follower.apply(store.RetainRecord(&message.Message{Namespace: "default", Topic: tp, Payload: []byte("kept"), Retain: true})); err != nil {
				t.Fatal(err)
			}
			declare(h, "n1")
			h.nodes["n2"].tr.SetFault("n3", &transport.Fault{Delay: 600 * time.Millisecond, Classes: []transport.Class{transport.ReplicationAppend}})
			entered := make(chan struct{})
			once := sync.Once{}
			r.judgeGate = func(shard int) {
				if shard == 0 {
					once.Do(func() { close(entered) })
				}
			}
			r.keepRetained(0, "n1", 1, false, false)
			select {
			case r.wake <- struct{}{}:
			default:
			}
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("precondition: no judging pass reached the kept shard")
			}
			time.Sleep(150 * time.Millisecond)
			if _, err := h.nodes["n2"].table.Apply(0, shardmap.Encode(moveMap(h, 2, sh("n2", "n3"), sh("n1", "n3")))); err != nil {
				t.Fatal(err)
			}
			h.waitFor("n2 to reconcile v2", 10*time.Second, func() bool { return r.Reconciled() == 2 })
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) && ownHolds(h, "n2", tp) != "kept" {
				time.Sleep(20 * time.Millisecond)
			}
			got := ownHolds(h, "n2", tp)
			if c.stale {
				if got != "" {
					t.Fatalf("control: n2 leads holding %q with the stale judgement, want nothing", got)
				}
				return
			}
			if got != "kept" {
				t.Fatalf("n2, the shard's leader under v2, holds %q; want its kept copy promoted", got)
			}
		})
	}
}

// TestOwedHandOverOwesEverySender: n3 led shard 0, n1 followed and is lost. At
// v2 n4 leads and n3's hand-over to it is lost, so n3 owes it. At v3 n2 leads,
// with n3's frames to n2 lost too, and n2's release reaches n3 while n3's
// hand-over is still unacknowledged. The release must not drop n3's copy, the
// only one; once n3's frames get through, n2 leads holding it.
func TestOwedHandOverOwesEverySender(t *testing.T) {
	h, _ := withCountersAnd(t, []string{"n1", "n2", "n3", "n4"}, 2, 2, quickAck)
	allAlive(h)
	h.applyMap(shardmap.Map{Version: 1, Basis: h.order, Replicas: 2, Shards: []shardmap.Shard{sh("n3", "n1"), sh("n1", "n2")}})
	waitSettled(h, 1)
	tp := topicIn(2, 0)
	if err := writeRetained(h, "n3", tp, "kept"); err != nil {
		t.Fatalf("precondition: the retained write under v1: %v", err)
	}
	h.waitFor("n1 to hold the copy under v1", 5*time.Second, func() bool { return replicaHolds(h, "n1", tp) == "kept" })
	lose(h, "n1", 29)
	dropAll(h, "n3", "n4", 30)
	h.applyMap(shardmap.Map{Version: 2, Basis: []string{"n2", "n3", "n4"}, Replicas: 2, Shards: []shardmap.Shard{sh("n4", "n2"), sh("n2", "n3")}})
	h.waitFor("n3 to owe n4 its hand-over", 5*time.Second, func() bool {
		n, ok := h.nodes["n3"].rep.keptNote(0)
		return ok && n.HandOver
	})
	dropAll(h, "n3", "n2", 31)
	h.applyMap(shardmap.Map{Version: 3, Basis: []string{"n2", "n3", "n4"}, Replicas: 2, Shards: []shardmap.Shard{sh("n2", "n3"), sh("n3", "n4")}})
	h.waitFor("n2 and n3 to reconcile v3", 5*time.Second, func() bool {
		return h.nodes["n2"].rep.Reconciled() == 3 && h.nodes["n3"].rep.Reconciled() == 3
	})
	time.Sleep(time.Second)
	if got := ownHolds(h, "n3", tp); got != "kept" {
		t.Fatalf("n3 holds %q after n2's release while its hand-over is unacknowledged; want kept", got)
	}
	h.nodes["n3"].tr.SetFault("n2", nil)
	h.nodes["n3"].tr.SetFault("n4", nil)
	h.waitFor("n2 to lead holding the message", 5*time.Second, func() bool { return ownHolds(h, "n2", tp) == "kept" })
}
