package replication

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/store"
)

// movedAgainShape is a sequence of four maps over which X, owned by n1, is
// replicated to holder under v1 and not under v2, the owner's whole write to
// its new target under v2 is lost, and v3 moves X's shard while holder holds
// it without being fed. v4 is the map without n1.
type movedAgainShape struct {
	name           string
	nodes          []string
	shard          int
	v1, v2, v3, v4 shardmap.Map
	holder         string
	// lostV2 and lostV3 are the owner's new targets under v2 and v3 whose
	// replication frames from n1 are lost from that map on.
	lostV2, lostV3 string
	fetcher        string
	// restart replaces v3 with the holder's restart under v2: its notes are
	// gone and its first reconcile starts from no map.
	restart bool
}

func movedAgainShapes() []movedAgainShape {
	return []movedAgainShape{
		{
			// A rolling restart over three pods with two copies: n2 away (v1),
			// n2 back and n3 leaving (v2), n3 back (v3), every map computed.
			// Under v2 n3 holds nothing of X's shard; under v3 it holds it
			// again, and n1 still replicates X to n2 only.
			name:   "rolling restart",
			nodes:  []string{"n1", "n2", "n3"},
			shard:  1,
			v1:     shardmap.Compute(1, []string{"n1", "n3"}, 2, 2),
			v2:     shardmap.Compute(2, []string{"n1", "n2"}, 2, 2),
			v3:     shardmap.Compute(3, []string{"n1", "n2", "n3"}, 2, 2),
			v4:     shardmap.Compute(4, []string{"n2", "n3"}, 2, 2),
			holder: "n3", lostV2: "n2", fetcher: "n2",
		},
		{
			// n2 holds X's shard under v2 and v3, and is X's target under
			// neither.
			name:  "held throughout",
			nodes: []string{"n1", "n2", "n3", "n4"},
			shard: 0,
			v1: shardmap.Map{Version: 1, Basis: []string{"n1", "n2", "n3", "n4"}, Replicas: 2, Shards: []shardmap.Shard{
				{Leader: "n1", Followers: []string{"n2"}}, {Leader: "n3", Followers: []string{"n4"}}}},
			v2: shardmap.Map{Version: 2, Basis: []string{"n1", "n2", "n3", "n4"}, Replicas: 2, Shards: []shardmap.Shard{
				{Leader: "n3", Followers: []string{"n2"}}, {Leader: "n4", Followers: []string{"n1"}}}},
			v3: shardmap.Map{Version: 3, Basis: []string{"n1", "n2", "n3", "n4"}, Replicas: 2, Shards: []shardmap.Shard{
				{Leader: "n4", Followers: []string{"n2"}}, {Leader: "n3", Followers: []string{"n1"}}}},
			v4: shardmap.Map{Version: 4, Basis: []string{"n2", "n3", "n4"}, Replicas: 2, Shards: []shardmap.Shard{
				{Leader: "n2", Followers: []string{"n3"}}, {Leader: "n4", Followers: []string{"n3"}}}},
			holder: "n2", lostV2: "n3", lostV3: "n4", fetcher: "n3",
		},
	}
}

// movedAgainCases are the shapes, and the held-throughout shape's holder
// restarting under v2 in place of v3.
func movedAgainCases() []movedAgainShape {
	shapes := movedAgainShapes()
	restarted := shapes[1]
	restarted.name, restarted.restart, restarted.lostV3 = "holder restarts", true, ""
	return append(shapes, restarted)
}

// check asserts the shape's own claims about its maps.
func (s movedAgainShape) check(t *testing.T) {
	t.Helper()
	targets := func(m shardmap.Map) []string { return m.Targets(s.shard, "n1") }
	if !slices.Contains(targets(s.v1), s.holder) {
		t.Fatalf("precondition: under v1 n1's targets for X are %v, without %s", targets(s.v1), s.holder)
	}
	if slices.Contains(targets(s.v2), s.holder) || !slices.Contains(targets(s.v2), s.lostV2) {
		t.Fatalf("precondition: under v2 n1's targets for X are %v; want %s and not %s", targets(s.v2), s.lostV2, s.holder)
	}
	if s.restart {
		if !s.v2.Holds(s.shard, s.holder) {
			t.Fatalf("precondition: %s does not hold X's shard under v2 (%v)", s.holder, s.v2.Holders(s.shard))
		}
		return
	}
	if slices.Equal(s.v2.Holders(s.shard), s.v3.Holders(s.shard)) || !s.v3.Holds(s.shard, s.holder) || slices.Contains(targets(s.v3), s.holder) {
		t.Fatalf("precondition: v3 must move X's shard (%v → %v) with %s holding it and not a target (%v)", s.v2.Holders(s.shard), s.v3.Holders(s.shard), s.holder, targets(s.v3))
	}
	if s.v3.Holds(s.shard, "n1") {
		t.Fatalf("precondition: n1 holds X's shard under v3 (%v)", s.v3.Holders(s.shard))
	}
}

// movedAgainFixture runs a case to its second event with n1 alive: X written
// at grant 1 and held by the holder under v1, v2 moving X's target away from
// the holder with n1's frames to its new target lost, then v3 or the holder's
// restart. mutate adjusts every node's options.
func movedAgainFixture(t *testing.T, s movedAgainShape, mutate func(*Options)) (h *harness, sinks map[string]*counters, id string) {
	t.Helper()
	s.check(t)
	h, sinks = withCountersAnd(t, s.nodes, 2, 2, func(o *Options) {
		quickAck(o)
		if mutate != nil {
			mutate(o)
		}
	})
	id = clientIn(2, s.shard)
	n1 := h.nodes["n1"]
	h.applyMap(s.v1)
	waitSettled(h, 1)
	h.writeSession("n1", id, 1)
	feedSession(n1, id, queued(1, "queued"))
	alive(h, "n1")
	if err := n1.barrier(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !holdsCopy(h, s.holder, id) {
		t.Fatalf("precondition: %s does not hold X under v1", s.holder)
	}
	dropAll(h, "n1", s.lostV2, 1)
	h.applyMap(s.v2)
	waitSettled(h, 2)
	time.Sleep(500 * time.Millisecond) // n1's confirmation barrier fails at the ack timeout
	if !holdsCopy(h, s.holder, id) || keptMovedCount(sinks, s.holder) < 1 {
		t.Fatalf("precondition: after v2 %s holds %v, kept{moved-target} %v; want the moved copy kept", s.holder, holdsCopy(h, s.holder, id), keptMovedCount(sinks, s.holder))
	}
	if s.restart {
		r := h.nodes[s.holder].rep
		r.unwritten.forget(store.Scope{Namespace: "default", ClientID: id})
		r.rebalance(shardmap.Map{}, s.v2)
		return h, sinks, id
	}
	if s.lostV3 != "" {
		dropAll(h, "n1", s.lostV3, 2)
	}
	h.applyMap(s.v3)
	waitSettled(h, 3)
	return h, sinks, id
}

// TestMovedCopyOutlivesALaterMapChange: holder keeps X's copy after v2 moves
// X's target away from it, and n1's whole write to the new target is lost, so
// no confirmation comes. v3 moves X's shard again while holder holds it, or
// holder restarts. n1, alive throughout, then dies before any target holds X.
// The takeover must still find the copy holder kept: it is the only other one.
func TestMovedCopyOutlivesALaterMapChange(t *testing.T) {
	for _, s := range movedAgainCases() {
		t.Run(s.name, func(t *testing.T) {
			h, sinks, id := movedAgainFixture(t, s, nil)
			n1 := h.nodes["n1"]
			time.Sleep(500 * time.Millisecond)
			keptAfter, keptMoved := holdsCopy(h, s.holder, id), h.nodes[s.holder].rep.KeptMoved()
			n1.rep.Stop()
			n1.tr.Stop()
			notAlive(h, "n1")
			var survivors []string
			for _, n := range s.nodes {
				if n != "n1" {
					survivors = append(survivors, n)
				}
			}
			for _, n := range survivors {
				if holdsCopy(h, n, id) && n != s.holder {
					t.Fatalf("precondition: %s holds X though every frame n1 sent it after v1 was lost", n)
				}
				if _, err := h.nodes[n].table.Apply(0, shardmap.Encode(s.v4)); err != nil {
					t.Fatal(err)
				}
			}
			h.waitFor("the survivors to reconcile v4", 5*time.Second, func() bool {
				for _, n := range survivors {
					if h.nodes[n].rep.Reconciled() != 4 {
						return false
					}
				}
				return true
			})
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			ss, answered, _ := h.nodes[s.fetcher].rep.Fetch(ctx, "default", id, "n1")
			if ss == nil {
				t.Fatalf("X is held nowhere after n1's death (answered %v): %s held its copy after v3 or its restart %v, kept{moved-target} %v",
					answered, s.holder, keptAfter, keptMovedCount(sinks, s.holder))
			}
			if len(ss.Entries) != 1 {
				t.Fatalf("the takeover read %d entries, want n1's queued one", len(ss.Entries))
			}
			if got := keptMovedCount(sinks, s.holder); got < 2 || keptMoved != 1 {
				t.Fatalf("after v3 or the restart %s counted kept{moved-target} %v and KeptMoved %d; want at least 2 and 1: the copy is a moved copy at the second judging too", s.holder, got, keptMoved)
			}
		})
	}
}

// TestMovedCopyAfterALaterMapChangeGoesOnTheOwnersWord: the held-throughout
// case to v3 with n1 alive. A confirmation at X's grant from a node that is
// not its owner, and n1's at a lower grant, leave n2's copy; n1's at the grant
// releases it.
func TestMovedCopyAfterALaterMapChangeGoesOnTheOwnersWord(t *testing.T) {
	s := movedAgainShapes()[1]
	h, sinks, id := movedAgainFixture(t, s, nil)
	n2 := h.nodes[s.holder].rep
	n2.NoteWritten("default", id, "n3", 1)
	n2.NoteWritten("default", id, "n1", 0)
	time.Sleep(300 * time.Millisecond)
	if !holdsCopy(h, s.holder, id) {
		t.Fatal("a confirmation from a node that is not the owner, or below the lease's grant, dropped the copy")
	}
	n2.NoteWritten("default", id, "n1", 1)
	h.waitFor("n2 to drop the copy on n1's confirmation", 3*time.Second, func() bool { return !holdsCopy(h, s.holder, id) })
	if got := dropped(sinks, s.holder, "not-a-target-confirmed"); got != 1 {
		t.Fatalf("dropped{not-a-target-confirmed} = %v, want 1", got)
	}
	if n := n2.KeptMoved(); n != 0 {
		t.Fatalf("KeptMoved = %d after the copy went, want 0", n)
	}
}

// TestMovedCopyAfterALaterMapChangeGoesOnceATargetHoldsIt: the held-throughout
// case to v3 with n1 alive and checks every 300 ms. While n4, n1's target
// under v3, holds nothing of X, n2's checks find no target holding it and n2
// keeps its copy through four of them; once n1's frames reach n4 again and n4
// holds X at its grant, the next check releases it.
func TestMovedCopyAfterALaterMapChangeGoesOnceATargetHoldsIt(t *testing.T) {
	const every = 300 * time.Millisecond
	s := movedAgainShapes()[1]
	h, sinks, id := movedAgainFixture(t, s, func(o *Options) { o.UnwrittenCheck = every })
	base := unwrittenChecks(sinks, s.holder, "not-held")
	h.waitFor("four checks of n4 after v3", 5*time.Second, func() bool {
		return unwrittenChecks(sinks, s.holder, "not-held") >= base+4 || !holdsCopy(h, s.holder, id)
	})
	if !holdsCopy(h, s.holder, id) {
		t.Fatalf("n2 dropped its copy with no target holding X (dropped{not-a-target-held} %v)", dropped(sinks, s.holder, "not-a-target-held"))
	}
	n1 := h.nodes["n1"]
	n1.tr.SetFault(s.lostV3, nil)
	later := queued(2, "later")
	feedSession(n1, id, queued(1, "queued"), later)
	if err := n1.own.Enqueue(store.Key{Namespace: "default", ClientID: id, Epoch: 1}, later); err != nil {
		t.Fatal(err)
	}
	if err := n1.own.Sync(); err != nil {
		t.Fatal(err)
	}
	key := []store.Scope{{Namespace: "default", ClientID: id}}
	h.waitFor("n1's barrier over X to pass at n4", 10*time.Second, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		return n1.rep.Barrier(ctx, key) == nil
	})
	if ss := h.nodes[s.lostV3].rep.ReplicaState().Find("default", id); ss == nil || ss.Epoch != 1 {
		t.Fatalf("precondition: n4 does not hold X at grant 1: %+v", ss)
	}
	h.waitFor("n2 to drop its copy once n4 holds X", 5*time.Second, func() bool { return !holdsCopy(h, s.holder, id) })
	if got := dropped(sinks, s.holder, "not-a-target-held"); got != 1 {
		t.Fatalf("dropped{not-a-target-held} = %v, want 1", got)
	}
}

// TestTargetsCopyIsNotJudgedAtAMapChange: v2 moves X's shard, n1 still owns X
// and n2 is still its target. n2's copy is fed, so the rebalance neither keeps
// it as a moved copy nor checks n1's targets for it.
func TestTargetsCopyIsNotJudgedAtAMapChange(t *testing.T) {
	h, sinks := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, func(o *Options) {
		quickAck(o)
		o.UnwrittenCheck = 300 * time.Millisecond
	})
	v1 := shardmap.Map{Version: 1, Basis: h.order, Replicas: 2, Shards: []shardmap.Shard{{Leader: "n1", Followers: []string{"n2"}}, {Leader: "n2", Followers: []string{"n3"}}}}
	v2 := shardmap.Map{Version: 2, Basis: h.order, Replicas: 2, Shards: []shardmap.Shard{{Leader: "n2", Followers: []string{"n1"}}, {Leader: "n3", Followers: []string{"n2"}}}}
	id := clientIn(2, 0)
	if !slices.Equal(v1.Targets(0, "n1"), []string{"n2"}) || !slices.Equal(v2.Targets(0, "n1"), []string{"n2"}) {
		t.Fatalf("precondition: n1's targets for X are %v under v1 and %v under v2, want [n2] in both", v1.Targets(0, "n1"), v2.Targets(0, "n1"))
	}
	h.applyMap(v1)
	waitSettled(h, 1)
	h.writeSession("n1", id, 1)
	feedSession(h.nodes["n1"], id, queued(1, "queued"))
	alive(h, "n1")
	if err := h.nodes["n1"].barrier(context.Background()); err != nil {
		t.Fatal(err)
	}
	h.applyMap(v2)
	waitSettled(h, 2)
	time.Sleep(4 * 300 * time.Millisecond)
	if !holdsCopy(h, "n2", id) {
		t.Fatalf("n2, n1's target under both maps, dropped its copy (dropped{not-a-target-held} %v)", dropped(sinks, "n2", "not-a-target-held"))
	}
	if kept, n := keptMovedCount(sinks, "n2"), unwrittenChecks(sinks, "n2", "started"); kept != 0 || n != 0 {
		t.Fatalf("n2 kept{moved-target} %v and started %v checks for a copy its owner feeds; want 0 and 0", kept, n)
	}
}
