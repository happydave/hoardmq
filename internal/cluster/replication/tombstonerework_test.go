package replication

import (
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

// clearedAt clears topic at node, the shard's leader, and returns the
// tombstone's version once follower holds it.
func clearedAt(t *testing.T, h *harness, node, follower, tp string) store.Version {
	t.Helper()
	if err := writeRetained(h, node, tp, ""); err != nil {
		t.Fatal(err)
	}
	e, ok := h.nodes[node].sessions.HeldRetained("default", tp)
	if !ok || !e.Tombstone {
		t.Fatalf("precondition: %s holds no tombstone for %s", node, tp)
	}
	h.waitFor(follower+" to hold the tombstone", 5*time.Second, func() bool { return replicaTombstone(h, follower, tp) })
	return e.RV
}

// putReplicaTombstone installs a tombstone in node's replica store directly.
func putReplicaTombstone(t *testing.T, h *harness, node, tp string, v store.Version) {
	t.Helper()
	if _, err := h.nodes[node].rep.follower.apply(store.RetainRecordVersioned(&message.Message{Namespace: "default", Topic: tp}, v)); err != nil {
		t.Fatal(err)
	}
}

// TestTombstoneSampleTakenAgainAfterABreak: a follower's stream breaks after
// the leader sampled what it must acknowledge to hold a tombstone. The sample
// is failed for good, so it is taken again once the resync, which carries the
// tombstone, is queued; then the tombstone goes on its conditions.
func TestTombstoneSampleTakenAgainAfterABreak(t *testing.T) {
	h, _ := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, func(o *Options) {
		quickAck(o)
		o.RetainedTombstoneHold = 400 * time.Millisecond
	})
	allAlive(h)
	h.applyMap(moveMap(h, 1, sh("n1", "n2"), sh("n2", "n3")))
	waitSettled(h, 1)
	tp := topicIn(2, 0)
	clearedAt(t, h, "n1", "n2", tp)
	n1 := h.nodes["n1"].rep
	n1.judgeTombstones(h.nodes["n1"].table.Current())
	n1.tombs.mu.Lock()
	_, sampled := n1.tombs.tracked[0][[2]string{"default", tp}].sentAt["n2"]
	n1.tombs.mu.Unlock()
	if !sampled {
		t.Fatal("precondition: no sample of n2's stream for the tombstone")
	}
	n1.outboundFor("n2").broken()
	n1.resync("n2")
	h.waitFor("the tombstone to go at n1 and n2 after the stream's resync", 5*time.Second, func() bool {
		return !ownTombstone(h, "n1", tp) && !replicaTombstone(h, "n2", tp)
	})
}

// TestKind11LostInABreakIsSentAgain: a kind-11 frame lost with its stream is
// owed to the follower again, not settled by the acknowledgement after the
// resync, and the resync sends it, so the follower drops the tombstone. A
// resync alone drops nothing it does not carry.
func TestKind11LostInABreakIsSentAgain(t *testing.T) {
	h, _ := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, quickAck)
	allAlive(h)
	h.applyMap(moveMap(h, 1, sh("n1", "n2"), sh("n2", "n3")))
	waitSettled(h, 1)
	tp := topicIn(2, 0)
	v := clearedAt(t, h, "n1", "n2", tp)
	n1 := h.nodes["n1"].rep
	p := n1.outboundFor("n2")
	dropAll(h, "n1", "n2", 11)
	n1.dropTombstones([]string{"n2"}, 0, []tombDrop{{Namespace: "default", Topic: tp, RV: v}})
	if ownTombstone(h, "n1", tp) {
		t.Fatal("precondition: n1 kept the tombstone it dropped")
	}
	time.Sleep(100 * time.Millisecond)
	h.nodes["n1"].tr.SetFault("n2", nil)
	p.broken()
	n1.resync("n2")
	h.waitFor("n2 to drop the tombstone the lost frame named", 5*time.Second, func() bool { return !replicaTombstone(h, "n2", tp) })
}

// TestAResyncDropsNothingItDoesNotCarry: a tombstone the follower holds and
// the leader lacks stays through the leader's resync.
func TestAResyncDropsNothingItDoesNotCarry(t *testing.T) {
	h, _ := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, quickAck)
	allAlive(h)
	h.applyMap(moveMap(h, 1, sh("n1", "n2"), sh("n2", "n3")))
	waitSettled(h, 1)
	tp := topicIn(2, 0)
	putReplicaTombstone(t, h, "n2", tp, store.Version{E: 1, N: 5})
	if err := writeRetained(h, "n1", topicIn2(2, 0, 1), "x"); err != nil {
		t.Fatal(err)
	}
	n1 := h.nodes["n1"].rep
	n1.resyncPeer("n2")
	p := n1.outboundFor("n2")
	last := p.lastSent()
	h.waitFor("n2 to acknowledge the resync", 5*time.Second, func() bool { d, _ := p.delivered(last); return d })
	if replicaHolds(h, "n2", topicIn2(2, 0, 1)) != "x" {
		t.Fatal("precondition: the resync did not reach n2")
	}
	if !replicaTombstone(h, "n2", tp) {
		t.Fatal("the resync dropped a tombstone it did not carry")
	}
}

// TestKind11FenceAndShard: a follower applies kind 11 only from its shard's
// leader, not while it owes a hand-over of the shard (whoever its note names
// as releaser: the hand-over is owed to whichever node leads now), and only
// to entries of the shard the frame names. The dropped-through version it is
// told is kept for its own use once it leads.
func TestKind11FenceAndShard(t *testing.T) {
	h, sinks := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, quickAck)
	allAlive(h)
	h.applyMap(moveMap(h, 1, sh("n1", "n2"), sh("n2", "n3")))
	waitSettled(h, 1)
	a, other := topicIn(2, 0), topicIn(2, 1)
	v := store.Version{E: 1, N: 9}
	putReplicaTombstone(t, h, "n2", a, v)
	putReplicaTombstone(t, h, "n2", other, v)
	n2 := h.nodes["n2"].rep
	w := &worker{f: n2.follower, peer: "n1"}
	fr := frame{Kind: kindRetainedTombstonesDropped, Shards: []int{0}, Epoch: 1, Drops: []tombDrop{{"default", a, v}, {"default", other, v}}}
	if (&worker{f: n2.follower, peer: "n3"}).applyTombstoneDrops(fr) {
		t.Fatal("kind 11 from a node that does not lead the shard was applied")
	}
	n2.rmoves.mu.Lock()
	n2.rmoves.kept[0] = retainedNote{Releaser: "n3", Version: 1, Own: true, HandOver: true}
	n2.rmoves.mu.Unlock()
	if w.applyTombstoneDrops(fr) {
		t.Fatal("kind 11 was applied while n2 owes a hand-over of the shard to its leader")
	}
	if !replicaTombstone(h, "n2", a) {
		t.Fatal("an unapplied frame dropped a tombstone")
	}
	n2.forgetKept(0)
	if !w.applyTombstoneDrops(fr) {
		t.Fatal("kind 11 from the shard's leader, nothing owed, was not applied")
	}
	if replicaTombstone(h, "n2", a) {
		t.Fatal("the named tombstone of the frame's shard stayed")
	}
	if !replicaTombstone(h, "n2", other) {
		t.Fatal("an entry of another shard was dropped")
	}
	if got := sinks["n2"].get("hoardmq_retained_tombstone_drops_total", map[string]string{"outcome": "unapplied"}); got != 2 {
		t.Fatalf("unapplied frames counted %v, want 2", got)
	}
	n2.tombs.mu.Lock()
	x := n2.tombs.dropped[0]
	n2.tombs.mu.Unlock()
	if x != v {
		t.Fatalf("n2 was told dropped through %v, want %v", x, v)
	}
	n2.belowDropped(0, "handover", "n3", &message.Message{Namespace: "default", Topic: a}, store.Version{E: 1, N: 3}, true)
	if got := sinks["n2"].get("hoardmq_retained_merges_total", map[string]string{"path": "handover", "outcome": "below-dropped"}); got != 1 {
		t.Fatalf("below-dropped counted %v at the follower told the version, want 1", got)
	}
}

// TestUnappliedKind11IsOwedEndToEnd: a follower that acknowledges kind 11
// unapplied leaves the entries owed at the leader, and keeps its tombstone.
func TestUnappliedKind11IsOwedEndToEnd(t *testing.T) {
	h, _ := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, quickAck)
	allAlive(h)
	h.applyMap(moveMap(h, 1, sh("n1", "n2"), sh("n2", "n3")))
	waitSettled(h, 1)
	tp := topicIn(2, 0)
	v := clearedAt(t, h, "n1", "n2", tp)
	n2 := h.nodes["n2"]
	n2.leases.mu.Lock()
	info := n2.leases.nodes["n1"]
	info.DeadAt = 1
	n2.leases.nodes["n1"] = info
	n2.leases.mu.Unlock()
	n2.rep.rmoves.mu.Lock()
	n2.rep.rmoves.kept[0] = retainedNote{Releaser: "n3", Version: 1, HandOver: true}
	n2.rep.rmoves.mu.Unlock()
	n1 := h.nodes["n1"].rep
	n1.dropTombstones([]string{"n2"}, 0, []tombDrop{{Namespace: "default", Topic: tp, RV: v}})
	h.waitFor("n1 to hold the entry owed to n2", 5*time.Second, func() bool {
		n1.tombs.mu.Lock()
		defer n1.tombs.mu.Unlock()
		return len(n1.tombs.owed["n2"][0]) == 1 && len(n1.tombs.sent["n2"]) == 0
	})
	if !replicaTombstone(h, "n2", tp) {
		t.Fatal("n2 dropped the tombstone while it owes a hand-over")
	}
}

// TestOwedDropsGoOnceTheHandOverIsDone: what a peer is owed goes again once
// its hand-over of the shard has gone quiet for the acknowledgement timeout,
// not with each hand-over frame, and at once on its release. Owed and sent
// entries of a shard this node no longer leads, or of a peer that no longer
// follows it, are forgotten.
func TestOwedDropsGoOnceTheHandOverIsDone(t *testing.T) {
	h, _ := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, quickAck)
	allAlive(h)
	h.applyMap(moveMap(h, 1, sh("n1", "n2"), sh("n2", "n3")))
	waitSettled(h, 1)
	n1 := h.nodes["n1"].rep
	cur := h.nodes["n1"].table.Current()
	d := tombDrop{"default", topicIn(2, 0), store.Version{E: 1, N: 4}}
	owe := func() {
		n1.tombs.mu.Lock()
		n1.tombs.owed["n2"] = map[int][]tombDrop{0: {d}}
		n1.tombs.mu.Unlock()
	}
	owedNow := func() int {
		n1.tombs.mu.Lock()
		defer n1.tombs.mu.Unlock()
		return len(n1.tombs.owed["n2"][0])
	}
	owe()
	n1.noteHandOverIn("n2", 0)
	n1.judgeTombstones(cur)
	if owedNow() != 1 {
		t.Fatal("the owed entries went while the hand-over was not yet quiet")
	}
	time.Sleep(350 * time.Millisecond)
	n1.judgeTombstones(cur)
	if owedNow() != 0 {
		t.Fatal("the owed entries did not go once the hand-over was quiet")
	}
	owe()
	n1.handOverReleased("n2", 0)
	if owedNow() != 0 {
		t.Fatal("the owed entries did not go on the peer's release")
	}
	n1.tombs.mu.Lock()
	n1.tombs.owed["n3"] = map[int][]tombDrop{0: {d}}
	n1.tombs.owed["n2"] = map[int][]tombDrop{1: {d}}
	n1.tombs.sent["n3"] = []sentDrops{{seq: 1, shard: 0, drops: []tombDrop{d}}}
	n1.tombs.mu.Unlock()
	n1.judgeTombstones(cur)
	n1.tombs.mu.Lock()
	left := len(n1.tombs.owed["n3"]) + len(n1.tombs.owed["n2"]) + len(n1.tombs.sent["n3"])
	n1.tombs.mu.Unlock()
	if left != 0 {
		t.Fatalf("%d owed or sent entries left for a peer that does not follow the shard or a shard not led, want none", left)
	}
}

// TestKind11BatchIsBounded: a drop of more than 1,024 entries goes out in
// frames of at most 1,024 each.
func TestKind11BatchIsBounded(t *testing.T) {
	h, _ := withCountersAnd(t, []string{"n1", "n2"}, 1, 2, quickAck)
	r := h.nodes["n1"].rep
	ds := make([]tombDrop, maxTombstoneDropBatch+1)
	for i := range ds {
		ds[i] = tombDrop{"default", "t", store.Version{E: 1, N: uint64(i + 1)}}
	}
	r.sendTombstoneDrops("n2", 0, ds)
	r.tombs.mu.Lock()
	sent := r.tombs.sent["n2"]
	r.tombs.mu.Unlock()
	if len(sent) != 2 || len(sent[0].drops) != maxTombstoneDropBatch || len(sent[1].drops) != 1 {
		var sizes []int
		for _, s := range sent {
			sizes = append(sizes, len(s.drops))
		}
		t.Fatalf("frames of %v entries, want [%d 1]", sizes, maxTombstoneDropBatch)
	}
}

// TestFollowerHoldsReadsTheTapAndRefusals: a tombstone judged before its tap
// ran is held only once the follower acknowledged the tap's frame, and a
// follower that refused the topic's record does not hold it.
func TestFollowerHoldsReadsTheTapAndRefusals(t *testing.T) {
	h, _ := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, func(o *Options) {
		quickAck(o)
		o.RetainedTombstoneHold = time.Nanosecond
	})
	allAlive(h)
	h.applyMap(moveMap(h, 1, sh("n1", "n2"), sh("n2", "n3")))
	waitSettled(h, 1)
	n1 := h.nodes["n1"]
	cur := n1.table.Current()
	if err := writeRetained(h, "n1", topicIn2(2, 0, 1), "x"); err != nil {
		t.Fatal(err)
	}
	p := n1.rep.outboundFor("n2")
	last := p.lastSent()
	h.waitFor("n2 to acknowledge", 5*time.Second, func() bool { d, _ := p.delivered(last); return d })
	tp := topicIn(2, 0)
	v := store.Version{E: 1, N: 1 << 40}
	n1.sessions.mu.Lock()
	n1.sessions.retained["default/"+tp] = &message.Message{Namespace: "default", Topic: tp, Retain: true}
	n1.sessions.vers["default/"+tp] = v
	n1.sessions.mu.Unlock()
	n1.rep.judgeTombstones(cur)
	n1.rep.noteTapped("default", tp, v, map[string]uint64{"n2": last + 1000})
	n1.rep.judgeTombstones(cur)
	if !ownTombstone(h, "n1", tp) {
		t.Fatal("the tombstone was dropped before n2 acknowledged its tap's frame")
	}
	n1.rep.tombs.mu.Lock()
	delete(n1.rep.tombs.tapped, [2]string{"default", tp})
	n1.rep.tombs.tracked = map[int]map[[2]string]*tombState{}
	n1.rep.tombs.mu.Unlock()
	p.mu.Lock()
	if p.refused == nil {
		p.refused = map[store.Scope]refusal{}
	}
	p.refused[store.Scope{Namespace: "default", Topic: tp}] = refusal{epoch: 1, class: "apply"}
	p.mu.Unlock()
	n1.rep.judgeTombstones(cur)
	n1.rep.judgeTombstones(cur)
	if !ownTombstone(h, "n1", tp) {
		t.Fatal("the tombstone was dropped while n2 had refused its topic")
	}
	p.mu.Lock()
	delete(p.refused, store.Scope{Namespace: "default", Topic: tp})
	p.mu.Unlock()
	n1.rep.judgeTombstones(cur)
	if ownTombstone(h, "n1", tp) {
		t.Fatal("precondition: the tombstone did not go with every condition met")
	}
}

// TestBelowDroppedOnTheAppendPath: a follower's append that installs into an
// empty topic at or below the shard's dropped-through version is counted
// below-dropped; one over a held entry is not.
func TestBelowDroppedOnTheAppendPath(t *testing.T) {
	h, sinks := withCountersAnd(t, []string{"n1", "n2"}, 1, 2, quickAck)
	n2 := h.nodes["n2"].rep
	n2.noteDropped(0, []tombDrop{{"default", "gone", store.Version{E: 2, N: 10}}})
	apply := func(tp string, v store.Version) {
		rec := store.RetainRecordVersioned(&message.Message{Namespace: "default", Topic: tp, Payload: []byte("x")}, v)
		if _, err := n2.follower.applyTimed(rec, nil, holdApplyOther, "append", "n1"); err != nil {
			t.Fatal(err)
		}
	}
	apply("empty", store.Version{E: 2, N: 3})
	apply("held", store.Version{E: 2, N: 1})
	apply("held", store.Version{E: 2, N: 2})
	if got := sinks["n2"].get("hoardmq_retained_merges_total", map[string]string{"path": "append", "outcome": "below-dropped"}); got != 2 {
		t.Fatalf("below-dropped on append counted %v, want 2 (the two installs into an empty topic)", got)
	}
}

// TestZeroVersionTombstoneIsNotCarried: a tombstone at the zero Version, which
// would read as a copy drop, is left out of every collector.
func TestZeroVersionTombstoneIsNotCarried(t *testing.T) {
	st := store.NewState()
	st.RetainedTombstones = append(st.RetainedTombstones, store.Tombstone{Namespace: "default", Topic: "z"}, store.Tombstone{Namespace: "default", Topic: "s", RV: store.Version{E: 1, N: 1}})
	es := replicaEntries(st)
	if len(es) != 1 || es[0].Msg.Topic != "s" {
		t.Fatalf("replica entries %+v, want only the stamped tombstone", es)
	}
	h, _ := withCountersAnd(t, []string{"n1", "n2"}, 1, 2, quickAck)
	n1 := h.nodes["n1"]
	n1.sessions.mu.Lock()
	n1.sessions.retained["default/z"] = &message.Message{Namespace: "default", Topic: "z", Retain: true}
	n1.sessions.retained["default/m"] = &message.Message{Namespace: "default", Topic: "m", Payload: []byte("x"), Retain: true}
	n1.sessions.mu.Unlock()
	local := n1.rep.localEntries()
	if len(local) != 1 || local[0].Msg.Topic != "m" {
		t.Fatalf("local entries %+v, want only the message", local)
	}
}

// TestOwnTombstoneMovesToTheReplicaStore: a former leader's own tombstone,
// once its hand-over is acknowledged while it still follows the shard, moves
// into its replica store with its version, so its follower's copy keeps
// outranking an older one.
func TestOwnTombstoneMovesToTheReplicaStore(t *testing.T) {
	h, _ := withCountersAnd(t, []string{"n1", "n2"}, 1, 2, quickAck)
	allAlive(h)
	h.applyMap(moveMap(h, 1, sh("n2", "n1")))
	waitSettled(h, 1)
	n1 := h.nodes["n1"]
	v := store.Version{E: 1, N: 5}
	n1.sessions.mu.Lock()
	n1.sessions.retained["default/t"] = &message.Message{Namespace: "default", Topic: "t", Retain: true}
	n1.sessions.vers["default/t"] = v
	n1.sessions.mu.Unlock()
	// The move's window: the leader's release of the shard, which the push
	// to n1 as a new follower carried, not yet received.
	n1.rep.rmoves.mu.Lock()
	delete(n1.rep.rmoves.received, 0)
	n1.rep.rmoves.mu.Unlock()
	n1.rep.endOwnAfterHandOver(n1.table.Current(), 0, "n2")
	if got, held, tomb := n1.rep.ReplicaState().RetainedHeld("default", "t"); !held || !tomb || got != v {
		t.Fatalf("n1's replica store holds %v (held %v, tombstone %v), want the tombstone at %v", got, held, tomb, v)
	}
	if _, ok := n1.sessions.HeldRetained("default", "t"); ok {
		t.Fatal("n1 kept its own tombstone after the move")
	}
}

// TestUnversionedHandOverOntoUnversionedIsHeld: on the hand-over path an
// unversioned copy does not replace an unversioned message the leader holds.
func TestUnversionedHandOverOntoUnversionedIsHeld(t *testing.T) {
	h, sinks := withCountersAnd(t, []string{"n1", "n2"}, 1, 2, quickAck)
	allAlive(h)
	h.applyMap(moveMap(h, 1, sh("n1", "n2")))
	waitSettled(h, 1)
	if durable, _ := h.nodes["n1"].sessions.InjectRetained(&message.Message{Namespace: "default", Topic: "t", Payload: []byte("held"), Retain: true}); !durable {
		t.Fatal("precondition")
	}
	if !h.nodes["n2"].rep.handOverRetained("n1", []store.RetainedEntry{{Msg: &message.Message{Namespace: "default", Topic: "t", Payload: []byte("kept"), Retain: true}}}, 1) {
		t.Fatal("the hand-over was not acknowledged")
	}
	if got := ownHolds(h, "n1", "t"); got != "held" {
		t.Fatalf("n1 holds %q, want the held message", got)
	}
	if got := sinks["n1"].get("hoardmq_retained_merges_total", map[string]string{"path": "handover", "outcome": "equal"}); got != 1 {
		t.Fatalf("the hand-over counted equal %v, want 1", got)
	}
}
