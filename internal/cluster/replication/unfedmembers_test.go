package replication

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
)

func TestUnfedOf(t *testing.T) {
	for _, c := range []struct {
		name    string
		alive   []string
		leaving []string
		targets []string
		want    []string
	}{
		{"every other live member but the targets", []string{"n1", "n2", "n3", "n4"}, nil, []string{"n3"}, []string{"n2", "n4"}},
		{"a leaving member that is no target", []string{"n3", "n1", "n2"}, []string{"n3"}, []string{"n2"}, []string{"n3"}},
		{"a leaving member that is still a target", []string{"n1", "n2", "n3"}, []string{"n3"}, []string{"n3"}, []string{"n2", "n3"}},
		{"no targets: every other member", []string{"n1", "n2", "n3"}, nil, nil, []string{"n2", "n3"}},
		{"a member not alive is not named", []string{"n1", "n3"}, nil, nil, []string{"n3"}},
		{"duplicates once, sorted", []string{"n4", "n1", "n2", "n4"}, nil, nil, []string{"n2", "n4"}},
		{"alone, leaving or not", []string{"n1"}, []string{"n1"}, nil, nil},
	} {
		if got := unfedOf(c.alive, c.leaving, "n1", c.targets); !slices.Equal(got, c.want) {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}

// TestUnfedNamesEveryLiveMemberButTheTargets: under v2 shard 0 is held by n3
// and n2. As X's owner n3 replicates to n2; n1 holds nothing of the shard, so
// no holder is unfed, yet n1 may keep a seal or a drain's wait for X and is
// named. A member not alive by the log is not.
func TestUnfedNamesEveryLiveMemberButTheTargets(t *testing.T) {
	h, _ := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, quickAck)
	id := clientIn(2, 0)
	for _, n := range h.order {
		alive(h, n)
	}
	if got := h.nodes["n3"].rep.Unfed("default", id); got != nil {
		t.Fatalf("before any map: %v, want none", got)
	}
	_, v2 := unfedMaps(h.order)
	v2.Version = 1
	h.applyMap(v2)
	waitSettled(h, 1)
	if got := h.nodes["n3"].rep.UnfedHolders("default", id); len(got) != 0 {
		t.Fatalf("precondition: n3's unfed holders %v, want none", got)
	}
	if got := h.nodes["n3"].rep.Unfed("default", id); !slices.Equal(got, []string{"n1"}) {
		t.Fatalf("n3 tells %v, want [n1]", got)
	}
	if got := h.nodes["n1"].rep.Unfed("default", id); !slices.Equal(got, []string{"n2"}) {
		t.Fatalf("n1 tells %v, want [n2] (its target is n3)", got)
	}
	notAlive(h, "n1")
	if got := h.nodes["n3"].rep.Unfed("default", id); len(got) != 0 {
		t.Fatalf("with n1 not alive n3 tells %v, want none", got)
	}
}

func TestRejudgeSoon(t *testing.T) {
	every := time.Minute
	for _, c := range []struct {
		name  string
		noted uint64
		age   time.Duration
		lease uint64
		want  bool
	}{
		{"a confirmation ahead of the table", 9, time.Second, 5, true},
		{"the table at the confirmed grant", 9, time.Second, 9, false},
		{"the table past it", 9, time.Second, 12, false},
		{"ahead, but older than one interval", 9, every, 5, false},
	} {
		if got := rejudgeSoon(c.noted, c.age, c.lease, every); got != c.want {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}

// TestMovedCopyGoesOnANewOwnersConfirmationAheadOfTheTable: n2 keeps X's copy
// when v2 moves X's target from n2 to n3 and n1's whole write to n3 is lost.
// The session then changes hands: n3 takes it at epoch 2 and confirms. n2
// notes the confirmation before its own table shows n3's grant, so the
// lease it reads still names n1; once its table applies the grant the copy
// goes, well inside the minute after which a check of n3's targets would run.
func TestMovedCopyGoesOnANewOwnersConfirmationAheadOfTheTable(t *testing.T) {
	h, sinks := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, func(o *Options) {
		quickAck(o)
		o.UnwrittenCheck = time.Minute
	})
	v1, _ := unfedMaps(h.order)
	v2 := shardmap.Map{Version: 2, Basis: h.order, Replicas: 2, Shards: []shardmap.Shard{{Leader: "n3", Followers: []string{"n1"}}, {Leader: "n2", Followers: []string{"n3"}}}}
	h.applyMap(v1)
	waitSettled(h, 1)
	id := clientIn(2, 0)
	h.writeSession("n1", id, 1)
	feedSession(h.nodes["n1"], id, queued(1, "queued"))
	alive(h, "n1")
	alive(h, "n3")
	if err := h.nodes["n1"].barrier(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !holdsCopy(h, "n2", id) {
		t.Fatal("precondition: n2 does not hold the copy under v1")
	}
	dropAll(h, "n1", "n3", 1)
	h.applyMap(v2)
	waitSettled(h, 2)
	if slices.Contains(v2.Targets(0, "n3"), "n2") || !holdsCopy(h, "n2", id) || keptMovedCount(sinks, "n2") < 1 {
		t.Fatalf("precondition: n2 a target of n3 %v, holds %v, kept{moved-target} %v; want false, true, at least 1",
			slices.Contains(v2.Targets(0, "n3"), "n2"), holdsCopy(h, "n2", id), keptMovedCount(sinks, "n2"))
	}
	for _, n := range []string{"n1", "n3"} {
		h.nodes[n].leases.set("default", id, "n3", 2)
	}
	n2 := h.nodes["n2"]
	n2.rep.NoteWritten("default", id, "n3", 2)
	time.Sleep(200 * time.Millisecond)
	if !holdsCopy(h, "n2", id) {
		t.Fatal("the copy went while n2's table still named n1 at epoch 1")
	}
	n2.leases.set("default", id, "n3", 2)
	h.waitFor("n2 to drop the copy once its table shows n3's grant", 2*time.Second, func() bool { return !holdsCopy(h, "n2", id) })
	if got := dropped(sinks, "n2", "not-a-target-confirmed"); got != 1 {
		t.Fatalf("dropped{not-a-target-confirmed} = %v, want 1", got)
	}
}

// TestUnfedCopyGoesOnANewOwnersConfirmationAheadOfTheTable is the same for a
// copy from an earlier grant: on four nodes n2 keeps X's copy, written under
// n1's first grant, when v2 leaves n2 a holder of X's shard but not n1's
// target and the lease is n1's at a later grant. n4, which holds nothing of
// the shard, then takes X and confirms before n2's table shows its grant.
func TestUnfedCopyGoesOnANewOwnersConfirmationAheadOfTheTable(t *testing.T) {
	h, sinks := withCountersAnd(t, []string{"n1", "n2", "n3", "n4"}, 2, 2, func(o *Options) {
		quickAck(o)
		o.UnwrittenCheck = time.Minute
	})
	shards := func(version uint64, zero []string) shardmap.Map {
		return shardmap.Map{Version: version, Basis: h.order, Replicas: 2, Shards: []shardmap.Shard{
			{Leader: zero[0], Followers: zero[1:]}, {Leader: "n3", Followers: []string{"n4"}}}}
	}
	h.applyMap(shards(1, []string{"n1", "n2"}))
	waitSettled(h, 1)
	id := clientIn(2, 0)
	h.writeSession("n1", id, 1)
	feedSession(h.nodes["n1"], id, queued(1, "queued"))
	for _, n := range h.order {
		alive(h, n)
	}
	if err := h.nodes["n1"].barrier(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !holdsCopy(h, "n2", id) {
		t.Fatal("precondition: n2 does not hold the copy under v1")
	}
	h.setLease("default", id, "n1", 5)
	v2 := shards(2, []string{"n3", "n2"})
	h.applyMap(v2)
	waitSettled(h, 2)
	if !holdsCopy(h, "n2", id) || keptUnwritten(sinks, "n2") < 1 || slices.Contains(v2.Targets(0, "n4"), "n2") {
		t.Fatalf("precondition: n2 holds %v, kept{owner-unwritten} %v, a target of n4 %v; want true, at least 1, false",
			holdsCopy(h, "n2", id), keptUnwritten(sinks, "n2"), slices.Contains(v2.Targets(0, "n4"), "n2"))
	}
	for _, n := range []string{"n1", "n3", "n4"} {
		h.nodes[n].leases.set("default", id, "n4", 6)
	}
	n2 := h.nodes["n2"]
	n2.rep.NoteWritten("default", id, "n4", 6)
	time.Sleep(200 * time.Millisecond)
	if !holdsCopy(h, "n2", id) {
		t.Fatal("the copy went while n2's table still named n1 at epoch 5")
	}
	n2.leases.set("default", id, "n4", 6)
	h.waitFor("n2 to drop the copy once its table shows n4's grant", 2*time.Second, func() bool { return !holdsCopy(h, "n2", id) })
	if got := dropped(sinks, "n2", "not-a-target-confirmed"); got != 1 {
		t.Fatalf("dropped{not-a-target-confirmed} = %v, want 1", got)
	}
}

func TestCountsAsMoved(t *testing.T) {
	for _, c := range []struct {
		name         string
		held, known  bool
		owner        string
		ownerTargets []string
		want         bool
	}{
		{"held, its owner replicates elsewhere", true, true, "n3", []string{"n2"}, true},
		{"no longer held", false, true, "n3", []string{"n2"}, false},
		{"the owner now replicates here", true, true, "n3", []string{"n1"}, false},
		{"this node owns it", true, true, "n1", nil, false},
		{"lease unknown", true, false, "", nil, true},
	} {
		if got := countsAsMoved(c.held, c.known, c.owner, "n1", c.ownerTargets); got != c.want {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}

// TestKeptMovedLeavesOutACopyItsNewOwnerFeeds: n2 keeps X's copy when v2
// moves X's target to n3. The session then moves to n3, whose target under v2
// is n2: the copy is n3's to feed now, and is not counted as kept for a moved
// target, before any recheck has forgotten its note.
func TestKeptMovedLeavesOutACopyItsNewOwnerFeeds(t *testing.T) {
	h, sinks, id := movedFixture(t, true, func(o *Options) { o.UnwrittenCheck = time.Minute })
	n2 := h.nodes["n2"].rep
	if !holdsCopy(h, "n2", id) || keptMovedCount(sinks, "n2") < 1 || n2.KeptMoved() != 1 {
		t.Fatalf("precondition: n2 holds %v, kept{moved-target} %v, KeptMoved %d; want true, at least 1, 1",
			holdsCopy(h, "n2", id), keptMovedCount(sinks, "n2"), n2.KeptMoved())
	}
	_, v2 := unfedMaps(h.order)
	if !slices.Contains(v2.Targets(0, "n3"), "n2") {
		t.Fatal("precondition: n2 is not n3's target under v2")
	}
	alive(h, "n3")
	h.setLease("default", id, "n3", 2)
	if got := n2.KeptMoved(); got != 0 {
		t.Fatalf("KeptMoved %d with the copy now n3's to feed, want 0", got)
	}
	if got := n2.KeptMovedSample(5); len(got) != 0 {
		t.Fatalf("KeptMovedSample %v, want none", got)
	}
}
