package replication

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/store"
)

// handoverRefusedMaps: X on shard 0 of two, owned by n1. Under v1 n1
// replicates X to n3; under v2 to n2, and n3 holds nothing of shard 0; v3
// moves shard 0 between n1 and n2, n3 still holding none of it; v4 is the map
// without n1, under which n3 leads shard 0.
func handoverRefusedMaps() (v1, v2, v3, v4 shardmap.Map) {
	all := []string{"n1", "n2", "n3"}
	v1 = shardmap.Map{Version: 1, Basis: all, Replicas: 2, Shards: []shardmap.Shard{
		{Leader: "n1", Followers: []string{"n3"}}, {Leader: "n2", Followers: []string{"n1"}}}}
	v2 = shardmap.Map{Version: 2, Basis: all, Replicas: 2, Shards: []shardmap.Shard{
		{Leader: "n1", Followers: []string{"n2"}}, {Leader: "n3", Followers: []string{"n2"}}}}
	v3 = shardmap.Map{Version: 3, Basis: all, Replicas: 2, Shards: []shardmap.Shard{
		{Leader: "n2", Followers: []string{"n1"}}, {Leader: "n3", Followers: []string{"n1"}}}}
	v4 = shardmap.Map{Version: 4, Basis: []string{"n2", "n3"}, Replicas: 2, Shards: []shardmap.Shard{
		{Leader: "n3", Followers: []string{"n2"}}, {Leader: "n2", Followers: []string{"n3"}}}}
	return v1, v2, v3, v4
}

// handoverRefusedCase is one way n3 comes to judge its copy of X with no
// moved note.
type handoverRefusedCase struct {
	name string
	// restart: n3 is down while v2 applies and restarts under it, its first
	// reconcile from no map. laterMap: v3 follows.
	restart, laterMap bool
}

func handoverRefusedCases() []handoverRefusedCase {
	return []handoverRefusedCase{
		{name: "holder restarts", restart: true},
		{name: "holder restarts, then a later map change", restart: true, laterMap: true},
		{name: "no restart"},
	}
}

// handoverRefusedFixture runs a case to its event with n1 alive: X written at
// grant 1 and held by n3 under v1, v2 moving X's target to n2 and n3 off X's
// shard with n1's whole write landing at n2 and its confirmation to n3 lost,
// then n3's restart, v3, or neither. A restarted n3 was down while v2
// applied, so it holds no note and no timer from before. every is the check
// interval.
func handoverRefusedFixture(t *testing.T, c handoverRefusedCase, every time.Duration) (h *harness, sinks map[string]*counters, id string) {
	t.Helper()
	v1, v2, v3, v4 := handoverRefusedMaps()
	if !slices.Equal(v1.Targets(0, "n1"), []string{"n3"}) || !slices.Equal(v2.Targets(0, "n1"), []string{"n2"}) ||
		v2.Holds(0, "n3") || v3.Holds(0, "n3") || !slices.Equal(v3.Targets(0, "n1"), []string{"n2"}) ||
		!slices.Equal(v4.Targets(0, "n1"), []string{"n3"}) {
		t.Fatalf("precondition: the maps do not place X as described")
	}
	h, sinks = withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, func(o *Options) {
		quickAck(o)
		o.UnwrittenCheck = every
		if o.Node == "n1" {
			o.ConfirmMoved = nil
		}
	})
	id = clientIn(2, 0)
	n1, n3 := h.nodes["n1"], h.nodes["n3"].rep
	h.applyMap(v1)
	waitSettled(h, 1)
	h.writeSession("n1", id, 1)
	feedSession(n1, id, queued(1, "queued"))
	alive(h, "n1")
	if err := n1.barrier(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !holdsCopy(h, "n3", id) {
		t.Fatal("precondition: n3 does not hold X under v1")
	}
	if c.restart {
		// n3 is down while v2 applies: n1 and n2 act on it alone.
		for _, n := range []string{"n1", "n2"} {
			if _, err := h.nodes[n].table.Apply(0, shardmap.Encode(v2)); err != nil {
				t.Fatal(err)
			}
		}
		h.waitFor("n1 and n2 to reconcile v2", 5*time.Second, func() bool {
			return n1.rep.Reconciled() == 2 && h.nodes["n2"].rep.Reconciled() == 2
		})
	} else {
		h.applyMap(v2)
		waitSettled(h, 2)
	}
	if err := n1.rep.Barrier(context.Background(), []store.Scope{{Namespace: "default", ClientID: id}}); err != nil {
		t.Fatalf("n1's barrier over X at n2 under v2: %v", err)
	}
	if ss := h.nodes["n2"].rep.ReplicaState().Find("default", id); ss == nil || ss.Epoch != 1 || len(ss.Entries) != 1 {
		t.Fatalf("precondition: n2 does not hold X whole at grant 1 under v2: %+v", ss)
	}
	if c.restart {
		// n3's restart: it has acted on no map, and its first reconcile goes
		// from none to v2, as shardmap.Recovered starts it.
		n3.recMu.Lock()
		n3.reconciled = shardmap.Map{}
		n3.recMu.Unlock()
		if _, err := h.nodes["n3"].table.Apply(0, shardmap.Encode(v2)); err != nil {
			t.Fatal(err)
		}
		h.waitFor("n3 to reconcile v2", 5*time.Second, func() bool { return n3.Reconciled() == 2 })
	}
	if !holdsCopy(h, "n3", id) {
		t.Fatalf("precondition: n3 dropped X before n1 wrote it again (dropped{not-a-target-held} %v)", dropped(sinks, "n3", "not-a-target-held"))
	}
	if c.laterMap {
		h.applyMap(v3)
		waitSettled(h, 3)
	}
	return h, sinks, id
}

// TestHandoverRefusedCopyDoesNotOutliveTheOwnersLaterWork: n3 holds X, fed by
// n1 under v1. v2 moves X's target to n2 and n3 off X's shard; n1's whole
// write lands at n2 and its confirmation to n3 is lost. n3, down while v2
// applied, restarts: with no moved note it hands its copy to n2, which refuses
// it, holding X already. n1, alive throughout, writes X again, and dies some
// check intervals later; the map without it names n3 first among X's sources.
// The takeover must read n1's latest X, and n3's copy must have gone on a
// check that found n2 holding X at its grant.
func TestHandoverRefusedCopyDoesNotOutliveTheOwnersLaterWork(t *testing.T) {
	const every = 600 * time.Millisecond
	_, _, _, v4 := handoverRefusedMaps()
	for _, c := range handoverRefusedCases() {
		t.Run(c.name, func(t *testing.T) {
			h, sinks, id := handoverRefusedFixture(t, c, every)
			key := []store.Scope{{Namespace: "default", ClientID: id}}
			n1, n3 := h.nodes["n1"], h.nodes["n3"].rep
			startedBefore := unwrittenChecks(sinks, "n3", "started")
			later := queued(2, "later")
			feedSession(n1, id, queued(1, "queued"), later)
			if err := n1.own.Enqueue(store.Key{Namespace: "default", ClientID: id, Epoch: 1}, later); err != nil {
				t.Fatal(err)
			}
			if err := n1.own.Sync(); err != nil {
				t.Fatal(err)
			}
			if err := n1.rep.Barrier(context.Background(), key); err != nil {
				t.Fatalf("n1's barrier over its second write: %v", err)
			}
			time.Sleep(4 * every)
			n3Copy := n3.ReplicaState().Find("default", id)
			n3Entries := -1
			if n3Copy != nil {
				n3Entries = len(n3Copy.Entries)
			}
			started := unwrittenChecks(sinks, "n3", "started") - startedBefore
			n1.rep.Stop()
			n1.tr.Stop()
			notAlive(h, "n1")
			for _, n := range []string{"n2", "n3"} {
				if _, err := h.nodes[n].table.Apply(0, shardmap.Encode(v4)); err != nil {
					t.Fatal(err)
				}
			}
			h.waitFor("the survivors to reconcile v4", 5*time.Second, func() bool {
				return h.nodes["n2"].rep.Reconciled() == 4 && n3.Reconciled() == 4
			})
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			ss, answered, _ := h.nodes["n2"].rep.Fetch(ctx, "default", id, "n1")
			if ss == nil || len(ss.Entries) != 2 {
				got := -1
				if ss != nil {
					got = len(ss.Entries)
				}
				t.Fatalf("the takeover read %d entries (answered %v) where n1 acknowledged 2: n3 held %d entries before n1 died, "+
					"kept{handover-unacknowledged} %v, kept{moved-target} %v, checks started %v",
					got, answered, n3Entries, kept(sinks, "n3"), keptMovedCount(sinks, "n3"), started)
			}
			if n3Entries != -1 || dropped(sinks, "n3", "not-a-target-held") != 1 || started < 1 {
				t.Fatalf("before n1 died n3 held %d entries, dropped{not-a-target-held} %v, checks started %v; want none, 1, at least 1",
					n3Entries, dropped(sinks, "n3", "not-a-target-held"), started)
			}
		})
	}
}

// TestHandoverRefusedCopyGoesOnTheOwnersWord: n3 restarted and its hand-over
// was refused, with checks too far apart to run. n1's confirmation at a lower
// grant, then one at X's grant from a node that is not its owner, leave n3's
// copy; n1's at the grant releases it.
func TestHandoverRefusedCopyGoesOnTheOwnersWord(t *testing.T) {
	h, sinks, id := handoverRefusedFixture(t, handoverRefusedCases()[0], time.Minute)
	n3 := h.nodes["n3"].rep
	n3.NoteWritten("default", id, "n1", 0)
	n3.NoteWritten("default", id, "n2", 1)
	time.Sleep(300 * time.Millisecond)
	if !holdsCopy(h, "n3", id) {
		t.Fatal("a confirmation below X's grant, or from a node that is not its owner, dropped n3's copy")
	}
	n3.NoteWritten("default", id, "n1", 1)
	h.waitFor("n3 to drop its copy on n1's confirmation", 3*time.Second, func() bool { return !holdsCopy(h, "n3", id) })
	if got := dropped(sinks, "n3", "not-a-target-confirmed"); got != 1 {
		t.Fatalf("dropped{not-a-target-confirmed} = %v, want 1", got)
	}
}

// earlierGrantHandoverFixture runs the earlier grant's shape to v2 with n2
// alive: n3 holds X at grant 1, fed by n1 under v1; X moves to n2 at grant 2,
// which replicates it to n1; v2 moves n3 off X's shard, and its hand-over to
// n1 is refused. lose drops every replication frame from n2 to n1 from before
// X moves, so n1 holds no grant-2 copy. every is the check interval.
func earlierGrantHandoverFixture(t *testing.T, every time.Duration, lose bool) (h *harness, sinks map[string]*counters, id string) {
	t.Helper()
	v1, v2, _, _ := handoverRefusedMaps()
	if !slices.Equal(v1.Targets(0, "n2"), []string{"n1"}) || !slices.Equal(v2.Targets(0, "n2"), []string{"n1"}) || v2.Holds(0, "n3") {
		t.Fatalf("precondition: the maps do not place X as described")
	}
	h, sinks = withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, func(o *Options) {
		quickAck(o)
		o.UnwrittenCheck = every
	})
	id = clientIn(2, 0)
	n2 := h.nodes["n2"]
	h.applyMap(v1)
	waitSettled(h, 1)
	h.writeSession("n1", id, 1)
	feedSession(h.nodes["n1"], id, queued(1, "queued"))
	alive(h, "n1")
	if err := h.nodes["n1"].barrier(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !holdsCopy(h, "n3", id) {
		t.Fatal("precondition: n3 does not hold X under v1")
	}
	if lose {
		dropAll(h, "n2", "n1", 3)
	}
	h.nodes["n1"].sessions.setSessions()
	feedSessionAt(n2, id, 2, queued(1, "queued"))
	h.writeSession("n2", id, 2)
	alive(h, "n2")
	if !lose {
		if err := n2.rep.Barrier(context.Background(), []store.Scope{{Namespace: "default", ClientID: id}}); err != nil {
			t.Fatalf("n2's barrier over X at n1: %v", err)
		}
	}
	h.applyMap(v2)
	waitSettled(h, 2)
	if ss := h.nodes["n3"].rep.ReplicaState().Find("default", id); ss == nil || ss.Epoch != 1 {
		t.Fatalf("precondition: n3 does not hold X at grant 1 after v2: %+v", ss)
	}
	return h, sinks, id
}

// TestHandoverRefusedEarlierGrantCopyDoesNotOutliveTheNewOwnersWork: the
// earlier grant's shape. n2, alive throughout, writes X again, and dies some
// check intervals later; the map without it names n3 first among X's sources.
// The takeover must read n2's latest X, and n3's copy must have gone on a
// check that found n1 holding X at grant 2.
func TestHandoverRefusedEarlierGrantCopyDoesNotOutliveTheNewOwnersWork(t *testing.T) {
	const every = 600 * time.Millisecond
	gone := shardmap.Map{Version: 3, Basis: []string{"n1", "n3"}, Replicas: 2, Shards: []shardmap.Shard{
		{Leader: "n3", Followers: []string{"n1"}}, {Leader: "n1", Followers: []string{"n3"}}}}
	if !slices.Equal(gone.Targets(0, "n2"), []string{"n3"}) {
		t.Fatalf("precondition: the map without n2 does not name n3 first")
	}
	h, sinks, id := earlierGrantHandoverFixture(t, every, false)
	key := []store.Scope{{Namespace: "default", ClientID: id}}
	n2, n3 := h.nodes["n2"], h.nodes["n3"].rep
	startedBefore := unwrittenChecks(sinks, "n3", "started")
	later := queued(2, "later")
	feedSessionAt(n2, id, 2, queued(1, "queued"), later)
	if err := n2.own.Enqueue(store.Key{Namespace: "default", ClientID: id, Epoch: 2}, later); err != nil {
		t.Fatal(err)
	}
	if err := n2.own.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := n2.rep.Barrier(context.Background(), key); err != nil {
		t.Fatalf("n2's barrier over its second write: %v", err)
	}
	time.Sleep(4 * every)
	n3Held := n3.ReplicaState().Find("default", id) != nil
	started := unwrittenChecks(sinks, "n3", "started") - startedBefore
	n2.rep.Stop()
	n2.tr.Stop()
	notAlive(h, "n2")
	for _, n := range []string{"n1", "n3"} {
		if _, err := h.nodes[n].table.Apply(0, shardmap.Encode(gone)); err != nil {
			t.Fatal(err)
		}
	}
	h.waitFor("the survivors to reconcile the map without n2", 5*time.Second, func() bool {
		return h.nodes["n1"].rep.Reconciled() == 3 && n3.Reconciled() == 3
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ss, answered, _ := h.nodes["n1"].rep.Fetch(ctx, "default", id, "n2")
	if ss == nil || ss.Epoch != 2 || len(ss.Entries) != 2 {
		grant, got := uint64(0), -1
		if ss != nil {
			grant, got = ss.Epoch, len(ss.Entries)
		}
		t.Fatalf("the takeover read %d entries at grant %d (answered %v) where n2 acknowledged 2 at grant 2: n3 held its copy before n2 died %v, "+
			"kept{handover-unacknowledged} %v, kept{owner-unwritten} %v, checks started after v2 %v",
			got, grant, answered, n3Held, kept(sinks, "n3"), keptUnwritten(sinks, "n3"), started)
	}
	if n3Held || keptUnwritten(sinks, "n3") < 1 || dropped(sinks, "n3", "not-a-target-held") != 1 {
		t.Fatalf("before n2 died n3 held its copy %v, kept{owner-unwritten} %v, dropped{not-a-target-held} %v; want false, at least 1, 1",
			n3Held, keptUnwritten(sinks, "n3"), dropped(sinks, "n3", "not-a-target-held"))
	}
}

// TestHandoverRefusedEarlierGrantCopyGoesOnceATargetHoldsIt: the earlier
// grant's shape with n2's writes to n1 lost, checks every 300 ms. While n1
// holds nothing at grant 2, n3's checks find no target holding X and n3 keeps
// its copy through four of them; once n2's frames reach n1 and n1 holds X at
// grant 2, a check releases it.
func TestHandoverRefusedEarlierGrantCopyGoesOnceATargetHoldsIt(t *testing.T) {
	const every = 300 * time.Millisecond
	h, sinks, id := earlierGrantHandoverFixture(t, every, true)
	base := unwrittenChecks(sinks, "n3", "not-held")
	h.waitFor("four checks of n1 after v2", 5*time.Second, func() bool {
		return unwrittenChecks(sinks, "n3", "not-held") >= base+4 || !holdsCopy(h, "n3", id)
	})
	if !holdsCopy(h, "n3", id) {
		t.Fatalf("n3 dropped its copy with no target holding X (dropped{not-a-target-held} %v)", dropped(sinks, "n3", "not-a-target-held"))
	}
	n2 := h.nodes["n2"]
	n2.tr.SetFault("n1", nil)
	later := queued(2, "later")
	feedSessionAt(n2, id, 2, queued(1, "queued"), later)
	if err := n2.own.Enqueue(store.Key{Namespace: "default", ClientID: id, Epoch: 2}, later); err != nil {
		t.Fatal(err)
	}
	if err := n2.own.Sync(); err != nil {
		t.Fatal(err)
	}
	key := []store.Scope{{Namespace: "default", ClientID: id}}
	h.waitFor("n2's barrier over X to pass at n1", 10*time.Second, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		return n2.rep.Barrier(ctx, key) == nil
	})
	if ss := h.nodes["n1"].rep.ReplicaState().Find("default", id); ss == nil || ss.Epoch != 2 {
		t.Fatalf("precondition: n1 does not hold X at grant 2: %+v", ss)
	}
	h.waitFor("n3 to drop its copy once n1 holds X", 5*time.Second, func() bool { return !holdsCopy(h, "n3", id) })
	if got := dropped(sinks, "n3", "not-a-target-held"); got != 1 {
		t.Fatalf("dropped{not-a-target-held} = %v, want 1", got)
	}
}

// TestTargetsCopyIsNotJudgedByAPendingRecheck: the earlier grant's shape with
// n2's writes to n1 lost, checks every 300 ms, so n3 keeps its copy and its
// rechecks keep coming. v3 makes n3 n2's target: n2 writes X whole to n3, and
// the copy is n2's own, fed. n2's confirmation at grant 2, sent while n3 was
// not fed, arrives late, and a recheck pending from before runs: it must leave
// the copy, not judge it and drop it on that confirmation or on n3 itself
// holding the session.
func TestTargetsCopyIsNotJudgedByAPendingRecheck(t *testing.T) {
	const every = 300 * time.Millisecond
	h, sinks, id := earlierGrantHandoverFixture(t, every, true)
	h.waitFor("a check of n1 after v2", 5*time.Second, func() bool { return unwrittenChecks(sinks, "n3", "not-held") >= 1 })
	v3 := shardmap.Map{Version: 3, Basis: []string{"n1", "n2", "n3"}, Replicas: 2, Shards: []shardmap.Shard{
		{Leader: "n2", Followers: []string{"n3"}}, {Leader: "n1", Followers: []string{"n2"}}}}
	if !slices.Equal(v3.Targets(0, "n2"), []string{"n3"}) {
		t.Fatalf("precondition: n2's targets for X under v3 are %v, want [n3]", v3.Targets(0, "n2"))
	}
	h.applyMap(v3)
	waitSettled(h, 3)
	h.waitFor("n3 to hold n2's copy at grant 2", 5*time.Second, func() bool {
		ss := h.nodes["n3"].rep.ReplicaState().Find("default", id)
		return ss != nil && ss.Epoch == 2
	})
	n3 := h.nodes["n3"].rep
	n3.NoteWritten("default", id, "n2", 2)
	n3.queueRecheck(store.Scope{Namespace: "default", ClientID: id})
	time.Sleep(5 * every)
	if ss := n3.ReplicaState().Find("default", id); ss == nil || ss.Epoch != 2 {
		t.Fatalf("n3, n2's target under v3, no longer holds X at grant 2 (%+v): dropped{not-a-target-held} %v, dropped{not-a-target-confirmed} %v",
			ss, dropped(sinks, "n3", "not-a-target-held"), dropped(sinks, "n3", "not-a-target-confirmed"))
	}
}
