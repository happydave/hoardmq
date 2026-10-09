package replication

import (
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

// applyTo installs a map on the named nodes only: a lost node stays on the map
// it had, as one cut off from the log does.
func applyTo(h *harness, m shardmap.Map, nodes ...string) {
	h.t.Helper()
	for _, n := range nodes {
		if _, err := h.nodes[n].table.Apply(0, shardmap.Encode(m)); err != nil {
			h.t.Fatal(err)
		}
	}
}

// waitReconciled waits for the named nodes to have acted on a map version.
func waitReconciled(h *harness, version uint64, nodes ...string) {
	h.t.Helper()
	h.waitFor("the nodes to reconcile the map", 5*time.Second, func() bool {
		for _, n := range nodes {
			if h.nodes[n].rep.Reconciled() < version {
				return false
			}
		}
		return true
	})
}

// waitMovesDone waits for the named nodes to have no retained move pending,
// so a write is taken again.
func waitMovesDone(h *harness, nodes ...string) {
	h.t.Helper()
	h.waitFor("the retained moves to complete", 5*time.Second, func() bool {
		for _, n := range nodes {
			if h.nodes[n].rep.RetainedMovesPending() != 0 {
				return false
			}
		}
		return true
	})
}

// heldBy names, for each node, what its own (L) and replica (R) stores hold of
// a topic.
func heldBy(h *harness, topic string, nodes ...string) string {
	out := ""
	for _, n := range nodes {
		out += n + ":" + ownHolds(h, n, topic) + "/" + replicaHolds(h, n, topic) + " "
	}
	return out
}

// TestOldLeaderThatStaysAFollowerKeepsItsCopy: under v1 n1 leads shard 0 and n2
// follows. Under v2 n2 leads and n1 follows, and every frame n2 sends n1 is
// lost, so its release and whole push never reach n1. n1's hand-over to n2 is
// acknowledged, as n2 holds the shard already. n1 must still hold the message
// after v2: n2 is lost next, and under v3 n1 leads with n3 following.
func TestOldLeaderThatStaysAFollowerKeepsItsCopy(t *testing.T) {
	h, _ := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, quickAck)
	allAlive(h)
	applyTo(h, shardmap.Map{Version: 1, Basis: []string{"n1", "n2"}, Replicas: 2, Shards: []shardmap.Shard{sh("n1", "n2"), sh("n2", "n1")}}, "n1", "n2", "n3")
	waitReconciled(h, 1, "n1", "n2", "n3")
	waitMovesDone(h, "n1", "n2", "n3")
	tp := topicIn(2, 0)
	if err := writeRetained(h, "n1", tp, "kept"); err != nil {
		t.Fatalf("precondition: the retained write under v1: %v", err)
	}
	h.waitFor("n2 to hold the copy under v1", 5*time.Second, func() bool { return replicaHolds(h, "n2", tp) == "kept" })
	dropAll(h, "n2", "n1", 31)
	applyTo(h, shardmap.Map{Version: 2, Basis: []string{"n1", "n2"}, Replicas: 2, Shards: []shardmap.Shard{sh("n2", "n1"), sh("n1", "n2")}}, "n1", "n2", "n3")
	waitReconciled(h, 2, "n1", "n2", "n3")
	time.Sleep(500 * time.Millisecond)
	t.Logf("after v2, n2's frames to n1 lost: %s", heldBy(h, tp, "n1", "n2", "n3"))
	if ownHolds(h, "n1", tp) != "kept" && replicaHolds(h, "n1", tp) != "kept" {
		t.Errorf("after v2 n1 holds nothing of the acknowledged message; only n2 does (%s)", heldBy(h, tp, "n1", "n2", "n3"))
	}
	lose(h, "n2", 32)
	applyTo(h, shardmap.Map{Version: 3, Basis: []string{"n1", "n3"}, Replicas: 2, Shards: []shardmap.Shard{sh("n1", "n3"), sh("n3", "n1")}}, "n1", "n3")
	waitReconciled(h, 3, "n1", "n3")
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && (ownHolds(h, "n1", tp) != "kept" || replicaHolds(h, "n3", tp) != "kept") {
		time.Sleep(20 * time.Millisecond)
	}
	if ownHolds(h, "n1", tp) != "kept" || replicaHolds(h, "n3", tp) != "kept" {
		t.Fatalf("after v3 with n2 lost: %s; want n1 leading and n3 following, each holding the acknowledged message", heldBy(h, tp, "n1", "n3"))
	}
}

// TestOwedHandOverOutlivesALaterKeep: under v1 n1 leads shard 0 and n3 follows.
// n1 is lost; under v2 n2, which held nothing, leads and n3 follows, and every
// frame n3 sends n2 is lost, so n3 owes n2 its hand-over. Under v3 n2 still
// leads and n4 follows, so n3 stops holding the shard while it still owes the
// hand-over. n3 must keep its copy, through n2's release, until n2 has it.
// With restart, n3's notes are read back from their file after v3, as at a
// restart, and must still owe the hand-over.
func TestOwedHandOverOutlivesALaterKeep(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(map[bool]string{false: "kept", true: "read back as at a restart"}[restart], func(t *testing.T) {
			owedHandOverAtALaterKeep(t, restart)
		})
	}
}

func owedHandOverAtALaterKeep(t *testing.T, restart bool) {
	dir := t.TempDir()
	h, _ := withCountersAnd(t, []string{"n1", "n2", "n3", "n4"}, 2, 2, func(o *Options) {
		quickAck(o)
		if o.Node == "n3" {
			o.ReplicaDir = dir
		}
	})
	allAlive(h)
	all := []string{"n1", "n2", "n3", "n4"}
	applyTo(h, shardmap.Map{Version: 1, Basis: all, Replicas: 2, Shards: []shardmap.Shard{sh("n1", "n3"), sh("n2", "n4")}}, all...)
	waitReconciled(h, 1, all...)
	waitMovesDone(h, all...)
	tp := topicIn(2, 0)
	if err := writeRetained(h, "n1", tp, "kept"); err != nil {
		t.Fatalf("precondition: the retained write under v1: %v", err)
	}
	h.waitFor("n3 to hold the copy under v1", 5*time.Second, func() bool { return replicaHolds(h, "n3", tp) == "kept" })
	lose(h, "n1", 33)
	dropAll(h, "n3", "n2", 34)
	live := []string{"n2", "n3", "n4"}
	applyTo(h, shardmap.Map{Version: 2, Basis: live, Replicas: 2, Shards: []shardmap.Shard{sh("n2", "n3"), sh("n4", "n2")}}, live...)
	waitReconciled(h, 2, live...)
	h.waitFor("n3 to note the hand-over it owes n2", 3*time.Second, func() bool { return h.nodes["n3"].rep.KeptRetained() == 1 })
	if n, _ := h.nodes["n3"].rep.keptNote(0); !n.HandOver {
		t.Fatalf("precondition: n3's note after v2 is %+v; want an owed hand-over", n)
	}
	applyTo(h, shardmap.Map{Version: 3, Basis: live, Replicas: 2, Shards: []shardmap.Shard{sh("n2", "n4"), sh("n4", "n2")}}, live...)
	waitReconciled(h, 3, live...)
	time.Sleep(time.Second)
	if restart {
		r := h.nodes["n3"].rep
		r.rmoves.mu.Lock()
		r.rmoves.kept = r.readKeptRetained()
		r.rmoves.mu.Unlock()
		if n, _ := r.keptNote(0); !n.HandOver {
			t.Fatalf("n3's note read back after v3 is %+v; want the hand-over still owed", n)
		}
	}
	t.Logf("after v3, n3's frames to n2 still lost: %s; n3's note %+v", heldBy(h, tp, live...), func() retainedNote { n, _ := h.nodes["n3"].rep.keptNote(0); return n }())
	if replicaHolds(h, "n3", tp) != "kept" {
		t.Errorf("after v3 n3 holds %q while n2 has not been shown to hold the message (%s); want kept", replicaHolds(h, "n3", tp), heldBy(h, tp, live...))
	}
	h.nodes["n3"].tr.SetFault("n2", nil)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && (ownHolds(h, "n2", tp) != "kept" || replicaHolds(h, "n4", tp) != "kept") {
		time.Sleep(20 * time.Millisecond)
	}
	if ownHolds(h, "n2", tp) != "kept" || replicaHolds(h, "n4", tp) != "kept" {
		t.Fatalf("after n3's frames get through: %s; want n2 leading and n4 following, each holding the acknowledged message", heldBy(h, tp, live...))
	}
	h.waitFor("n3, no longer a holder, to drop its copy once n2 has it", 3*time.Second, func() bool {
		return replicaHolds(h, "n3", tp) == "" && h.nodes["n3"].rep.KeptRetained() == 0
	})
}

// TestOldLeadersReleaseAheadOfTheMapKeepsTheNewLeadersCopies: under v1 n3 leads
// shard 0 and n1 follows. Under v2 n1 leads and n2 follows; n3, the old
// leader, keeps its own copies until n1 releases them, and n1's release reaches
// it while it still owes n1 its hand-over, so the keep waits for n3's next
// judging, which the test holds. Under v3 n3 leads again and n1 follows. n1
// applies v3 first and hands its copies to n3, which has not applied v3 and
// still keeps its own for n1's release. n3 must not answer for the shard's
// leader: once n1 lets its copies go, n3's are the only ones.
func TestOldLeadersReleaseAheadOfTheMapKeepsTheNewLeadersCopies(t *testing.T) {
	h, sinks := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, quickAck)
	allAlive(h)
	all := []string{"n1", "n2", "n3"}
	applyTo(h, shardmap.Map{Version: 1, Basis: all, Replicas: 2, Shards: []shardmap.Shard{sh("n3", "n1"), sh("n2", "n3")}}, all...)
	waitReconciled(h, 1, all...)
	waitMovesDone(h, all...)
	tp := topicIn(2, 0)
	if err := writeRetained(h, "n3", tp, "kept"); err != nil {
		t.Fatalf("precondition: the retained write under v1: %v", err)
	}
	h.waitFor("n1 to hold the copy under v1", 5*time.Second, func() bool { return replicaHolds(h, "n1", tp) == "kept" })

	rebalance := make(chan struct{})
	letRebalance := sync.OnceFunc(func() { close(rebalance) })
	t.Cleanup(letRebalance)
	h.nodes["n3"].rep.fanOutGate = func() { <-rebalance }
	judge := make(chan struct{})
	letJudge := sync.OnceFunc(func() { close(judge) })
	t.Cleanup(letJudge)
	before := releasesIn(sinks, "n3")
	applyTo(h, shardmap.Map{Version: 2, Basis: all, Replicas: 2, Shards: []shardmap.Shard{sh("n1", "n2"), sh("n2", "n3")}}, all...)
	h.waitFor("n1's release to reach n3 before n3's rebalance", 5*time.Second, func() bool { return releasesIn(sinks, "n3") > before })
	h.nodes["n3"].rep.judgeGate = func(int) { <-judge }
	letRebalance()
	waitReconciled(h, 2, all...)
	h.waitFor("n3 to keep its own copies for n1's release", 3*time.Second, func() bool { return h.nodes["n3"].rep.KeptRetained() == 1 })
	if ownHolds(h, "n3", tp) != "kept" {
		t.Fatalf("precondition: n3's own copy after v2 is %q; want kept", ownHolds(h, "n3", tp))
	}

	v3 := shardmap.Map{Version: 3, Basis: all, Replicas: 2, Shards: []shardmap.Shard{sh("n3", "n1"), sh("n2", "n3")}}
	before = releasesIn(sinks, "n3")
	applyTo(h, v3, "n1", "n2")
	// n1's hand-over is either answered, and its release then sent, or
	// refused, and n1 then owes it.
	h.waitFor("n1's hand-over to be answered or refused", 5*time.Second, func() bool {
		n, _ := h.nodes["n1"].rep.keptNote(0)
		return releasesIn(sinks, "n3") > before || n.HandOver
	})
	time.Sleep(200 * time.Millisecond)
	t.Logf("after n1's hand-over, n3 still on v2: %s", heldBy(h, tp, all...))
	applyTo(h, v3, "n3")
	letJudge()
	waitReconciled(h, 3, all...)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && (ownHolds(h, "n3", tp) != "kept" || replicaHolds(h, "n1", tp) != "kept") {
		time.Sleep(20 * time.Millisecond)
	}
	if ownHolds(h, "n3", tp) != "kept" || replicaHolds(h, "n1", tp) != "kept" {
		t.Fatalf("after v3: %s; want n3 leading and n1 following, each holding the acknowledged message", heldBy(h, tp, all...))
	}
}

// TestRetainedHandOverPredicates: the verdict on a hand-over, and the choice
// for a former leader's own copies. The dangerous arguments are a non-leader
// that holds the topic or keeps its own copies (it must refuse, not answer for
// the leader), and a follower that has not had the new leader's release (its
// own copies must be kept as a follower's, not deleted).
func TestRetainedHandOverPredicates(t *testing.T) {
	for _, c := range []struct {
		name                        string
		leads, holdsTopic, keepsOwn bool
		want                        handOverVerdict
	}{
		{"a non-leader holding the topic", false, true, false, handOverRefuse},
		{"a non-leader keeping its own copies", false, false, true, handOverRefuse},
		{"a non-leader with both", false, true, true, handOverRefuse},
		{"a non-leader with neither", false, false, false, handOverInstall},
		{"the leader holding the topic", true, true, false, handOverHeld},
		{"the leader holding the topic, own copies noted", true, true, true, handOverHeld},
		{"the leader holding nothing", true, false, false, handOverInstall},
	} {
		if got := judgeHandOver(c.leads, c.holdsTopic, c.keepsOwn); got != c.want {
			t.Errorf("judgeHandOver, %s: %d, want %d", c.name, got, c.want)
		}
	}
	for _, c := range []struct {
		name            string
		holds, released bool
		want            bool
	}{
		{"a follower without the new leader's release", true, false, true},
		{"a follower with the release, the push behind it", true, true, false},
		{"no longer a holder", false, false, false},
		{"no longer a holder, released", false, true, false},
	} {
		if got := moveOwnToReplica(c.holds, c.released); got != c.want {
			t.Errorf("moveOwnToReplica, %s: %v, want %v", c.name, got, c.want)
		}
	}
}

// TestOwnCopiesKeptAsAFollowersAfterAHandOver: a former leader that follows
// the shard and has not had the new leader's release keeps its own copies as
// replica copies, without replacing a topic its replica store holds; with the
// release in, or no longer holding the shard, its own copies go as before.
func TestOwnCopiesKeptAsAFollowersAfterAHandOver(t *testing.T) {
	for _, c := range []struct {
		name               string
		follows, released  bool
		wantA, wantB, ownB string
	}{
		{"a follower without the release", true, false, "new", "oldB", ""},
		{"a follower with the release", true, true, "new", "", ""},
		{"no longer a holder", false, false, "new", "", ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			h, _ := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, quickAck)
			allAlive(h)
			follower := "n2"
			if !c.follows {
				follower = "n3"
			}
			m := shardmap.Map{Version: 1, Basis: h.order, Replicas: 2, Shards: []shardmap.Shard{sh("n1", follower), sh("n1", follower)}}
			applyTo(h, m, h.order...)
			waitReconciled(h, 1, h.order...)
			// n1's first moves release every other member, n2 among them;
			// they must be done before n2's copies are written.
			waitMovesDone(h, h.order...)
			r := h.nodes["n2"].rep
			a, b := topicIn2(2, 0, 0), topicIn2(2, 0, 1)
			h.nodes["n2"].sessions.InjectRetained(&message.Message{Namespace: "default", Topic: a, Payload: []byte("oldA"), Retain: true})
			h.nodes["n2"].sessions.InjectRetained(&message.Message{Namespace: "default", Topic: b, Payload: []byte("oldB"), Retain: true})
			if _, err := r.follower.apply(store.RetainRecord(&message.Message{Namespace: "default", Topic: a, Payload: []byte("new"), Retain: true})); err != nil {
				t.Fatal(err)
			}
			// The hand-over is acknowledged at map version 2: n1's release at
			// v1, which a first reconcile sends, is not that map's.
			cur := m
			cur.Version = 2
			if c.released {
				r.rmoves.mu.Lock()
				r.rmoves.received[0] = map[string]uint64{"n1": 2}
				r.rmoves.mu.Unlock()
			}
			r.endOwnAfterHandOver(cur, 0, "n1")
			if got := replicaHolds(h, "n2", a); got != c.wantA {
				t.Errorf("n2's replica copy of %s is %q, want %q: a topic the replica store holds keeps its copy", a, got, c.wantA)
			}
			if got := replicaHolds(h, "n2", b); got != c.wantB {
				t.Errorf("n2's replica copy of %s is %q, want %q", b, got, c.wantB)
			}
			if got := ownHolds(h, "n2", b); got != c.ownB {
				t.Errorf("n2's own copy of %s is %q, want %q", b, got, c.ownB)
			}
		})
	}
}

// TestOwedHandOverHandsEveryKeptCopy: n3's note owes n2 a hand-over and also
// keeps own copies (a note made again over an older one). Its retry hands n2
// its replica copies as well as its own, so n2 holds the message once n3's
// frames get through.
func TestOwedHandOverHandsEveryKeptCopy(t *testing.T) {
	h, _ := withCountersAnd(t, []string{"n1", "n2", "n3", "n4"}, 2, 2, quickAck)
	allAlive(h)
	all := []string{"n1", "n2", "n3", "n4"}
	applyTo(h, shardmap.Map{Version: 1, Basis: all, Replicas: 2, Shards: []shardmap.Shard{sh("n1", "n3"), sh("n2", "n4")}}, all...)
	waitReconciled(h, 1, all...)
	waitMovesDone(h, all...)
	tp := topicIn(2, 0)
	if err := writeRetained(h, "n1", tp, "kept"); err != nil {
		t.Fatalf("precondition: the retained write under v1: %v", err)
	}
	h.waitFor("n3 to hold the copy under v1", 5*time.Second, func() bool { return replicaHolds(h, "n3", tp) == "kept" })
	lose(h, "n1", 35)
	dropAll(h, "n3", "n2", 36)
	live := []string{"n2", "n3", "n4"}
	applyTo(h, shardmap.Map{Version: 2, Basis: live, Replicas: 2, Shards: []shardmap.Shard{sh("n2", "n4"), sh("n4", "n2")}}, live...)
	waitReconciled(h, 2, live...)
	h.waitFor("n3 to note the hand-over it owes n2", 3*time.Second, func() bool { n, _ := h.nodes["n3"].rep.keptNote(0); return n.HandOver })
	h.nodes["n3"].rep.keepRetained(0, "n2", 2, true, false)
	if n, _ := h.nodes["n3"].rep.keptNote(0); !n.HandOver || !n.Own {
		t.Fatalf("precondition: n3's note is %+v; want own copies kept and the hand-over still owed", n)
	}
	h.nodes["n3"].tr.SetFault("n2", nil)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && ownHolds(h, "n2", tp) != "kept" {
		time.Sleep(20 * time.Millisecond)
	}
	if ownHolds(h, "n2", tp) != "kept" {
		t.Fatalf("after n3's frames get through: %s; want n2 leading and holding the acknowledged message", heldBy(h, tp, live...))
	}
	// n3 holds the shard no longer: its copies are kept for n2's release, and
	// go with it.
	h.waitFor("n3 to drop its copies on n2's release", 3*time.Second, func() bool {
		return replicaHolds(h, "n3", tp) == "" && h.nodes["n3"].rep.KeptRetained() == 0
	})
}

// TestHandOverRefusedByANodeKeepingOwnCopiesOfTheShard: as the release-ahead
// test, but the topic n1 hands over is one n3 does not hold: n1 cleared n3's
// topic and took a new one under v2. n3, not yet on v3, still keeps its own
// copies of the shard for n1's release, so it must refuse the hand-over rather
// than install a copy that release would then delete with the rest.
func TestHandOverRefusedByANodeKeepingOwnCopiesOfTheShard(t *testing.T) {
	h, sinks := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, quickAck)
	allAlive(h)
	all := []string{"n1", "n2", "n3"}
	applyTo(h, shardmap.Map{Version: 1, Basis: all, Replicas: 2, Shards: []shardmap.Shard{sh("n3", "n1"), sh("n2", "n3")}}, all...)
	waitReconciled(h, 1, all...)
	waitMovesDone(h, all...)
	a, c := topicIn2(2, 0, 0), topicIn2(2, 0, 1)
	if err := writeRetained(h, "n3", a, "first"); err != nil {
		t.Fatalf("precondition: the retained write under v1: %v", err)
	}
	h.waitFor("n1 to hold the copy under v1", 5*time.Second, func() bool { return replicaHolds(h, "n1", a) == "first" })

	rebalance := make(chan struct{})
	letRebalance := sync.OnceFunc(func() { close(rebalance) })
	t.Cleanup(letRebalance)
	h.nodes["n3"].rep.fanOutGate = func() { <-rebalance }
	judge := make(chan struct{})
	letJudge := sync.OnceFunc(func() { close(judge) })
	t.Cleanup(letJudge)
	before := releasesIn(sinks, "n3")
	applyTo(h, shardmap.Map{Version: 2, Basis: all, Replicas: 2, Shards: []shardmap.Shard{sh("n1", "n2"), sh("n2", "n3")}}, all...)
	h.waitFor("n1's release to reach n3 before n3's rebalance", 5*time.Second, func() bool { return releasesIn(sinks, "n3") > before })
	h.nodes["n3"].rep.judgeGate = func(int) { <-judge }
	letRebalance()
	waitReconciled(h, 2, all...)
	h.waitFor("n3 to keep its own copies for n1's release", 3*time.Second, func() bool { return h.nodes["n3"].rep.KeptRetained() == 1 })
	waitMovesDone(h, "n1", "n2")
	if err := writeRetained(h, "n1", a, ""); err != nil {
		t.Fatalf("precondition: the clear under v2: %v", err)
	}
	if err := writeRetained(h, "n1", c, "later"); err != nil {
		t.Fatalf("precondition: the retained write under v2: %v", err)
	}
	if ownHolds(h, "n3", c) != "" || ownHolds(h, "n3", a) != "first" {
		t.Fatalf("precondition: n3 keeps %q of %s and %q of %s; want its old copy only", ownHolds(h, "n3", a), a, ownHolds(h, "n3", c), c)
	}

	v3 := shardmap.Map{Version: 3, Basis: all, Replicas: 2, Shards: []shardmap.Shard{sh("n3", "n1"), sh("n2", "n3")}}
	before = releasesIn(sinks, "n3")
	applyTo(h, v3, "n1", "n2")
	h.waitFor("n1's hand-over to be answered or refused", 5*time.Second, func() bool {
		n, _ := h.nodes["n1"].rep.keptNote(0)
		return releasesIn(sinks, "n3") > before || n.HandOver
	})
	time.Sleep(200 * time.Millisecond)
	t.Logf("after n1's hand-over, n3 still on v2: %s", heldBy(h, c, all...))
	applyTo(h, v3, "n3")
	letJudge()
	waitReconciled(h, 3, all...)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && (ownHolds(h, "n3", c) != "later" || replicaHolds(h, "n1", c) != "later") {
		time.Sleep(20 * time.Millisecond)
	}
	if ownHolds(h, "n3", c) != "later" || replicaHolds(h, "n1", c) != "later" {
		t.Fatalf("after v3: %s; want n3 leading and n1 following, each holding the acknowledged message", heldBy(h, c, all...))
	}
}
