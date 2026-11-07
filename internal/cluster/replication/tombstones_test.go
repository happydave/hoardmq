package replication

import (
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

func ownTombstone(h *harness, node, topic string) bool {
	for _, e := range h.nodes[node].sessions.RetainedEntries() {
		if e.Msg.Topic == topic && e.Tombstone {
			return true
		}
	}
	return false
}

// TestTombstoneGoesOnlyOnTheLeadersWord: a clear's tombstone is kept at the
// leader and its follower for the hold; once it has passed, the follower holds
// it and no move is pending, the leader drops it and names it in kind 11, and
// the follower drops it then. Each tombstone goes on its own conditions: one
// cleared later stays until its own hold has passed. The leader's tombstone
// gauge for the namespace follows them.
func TestTombstoneGoesOnlyOnTheLeadersWord(t *testing.T) {
	hold := 1500 * time.Millisecond
	h, sinks := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, func(o *Options) {
		quickAck(o)
		o.RetainedTombstoneHold = hold
	})
	allAlive(h)
	h.applyMap(moveMap(h, 1, sh("n1", "n2"), sh("n2", "n3")))
	waitSettled(h, 1)
	a, b := topicIn2(2, 0, 0), topicIn2(2, 0, 1)
	for _, tp := range []string{a, b} {
		if err := writeRetained(h, "n1", tp, "x"); err != nil {
			t.Fatal(err)
		}
	}
	if err := writeRetained(h, "n1", a, ""); err != nil {
		t.Fatal(err)
	}
	h.waitFor("n2 to hold a's tombstone", 5*time.Second, func() bool { return replicaTombstone(h, "n2", a) })
	time.Sleep(hold / 2)
	if err := writeRetained(h, "n1", b, ""); err != nil {
		t.Fatal(err)
	}
	gauge := func() float64 {
		return sinks["n1"].get("hoardmq_retained_tombstones", map[string]string{"namespace": "default"})
	}
	h.waitFor("n1's tombstone gauge to read both", time.Second, func() bool { return gauge() == 2 })
	if !ownTombstone(h, "n1", a) || !replicaTombstone(h, "n2", a) {
		t.Fatal("a's tombstone went before its hold")
	}
	h.waitFor("a's tombstone to go at n1 and at n2", 5*time.Second, func() bool {
		return !ownTombstone(h, "n1", a) && !replicaTombstone(h, "n2", a)
	})
	if !ownTombstone(h, "n1", b) || !replicaTombstone(h, "n2", b) {
		t.Fatal("b's tombstone went with a's, before its own hold")
	}
	h.waitFor("b's tombstone to go too", 5*time.Second, func() bool {
		return !ownTombstone(h, "n1", b) && !replicaTombstone(h, "n2", b)
	})
	h.waitFor("n1's tombstone gauge to read none for the namespace", 5*time.Second, func() bool { return gauge() == 0 })
	if got := sinks["n2"].get("hoardmq_retained_tombstone_drops_total", map[string]string{"outcome": "applied"}); got < 2 {
		t.Fatalf("n2 applied %v kind-11 frames, want at least 2", got)
	}
}

// TestFollowerTombstoneDropIsACompareAndDelete: a kind-11 entry drops a
// follower's tombstone only while it is still the named one: a message, or a
// tombstone at another version, written over it stays.
func TestFollowerTombstoneDropIsACompareAndDelete(t *testing.T) {
	h, _ := withCountersAnd(t, []string{"n1", "n2"}, 1, 2, quickAck)
	f := h.nodes["n2"].rep.follower
	v := store.Version{E: 3, N: 7}
	for _, tp := range []string{"m", "t", "other"} {
		if _, err := f.apply(store.RetainRecordVersioned(&message.Message{Namespace: "default", Topic: tp}, v)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.apply(store.RetainRecordVersioned(&message.Message{Namespace: "default", Topic: "m", Payload: []byte("newer")}, store.Version{E: 3, N: 8})); err != nil {
		t.Fatal(err)
	}
	if _, err := f.apply(store.RetainRecordVersioned(&message.Message{Namespace: "default", Topic: "other"}, store.Version{E: 3, N: 9})); err != nil {
		t.Fatal(err)
	}
	n := f.dropTombstones([]tombDrop{{"default", "m", v}, {"default", "t", v}, {"default", "other", v}})
	if n != 1 {
		t.Fatalf("dropped %d, want 1 (t only)", n)
	}
	if got := replicaHolds(h, "n2", "m"); got != "newer" {
		t.Fatalf("m holds %q, want the message written over the tombstone", got)
	}
	if replicaTombstone(h, "n2", "t") {
		t.Fatal("t's tombstone stayed")
	}
	if !replicaTombstone(h, "n2", "other") {
		t.Fatal("a tombstone at another version went")
	}
}

// TestUnappliedDropsAreSentAgain: a kind-11 frame a follower acknowledges
// unapplied leaves its entries owed to it; they go out again on the next send
// for the shard, at a resync, or once its owed hand-over is in
// (resendOwedDrops), and an applied acknowledgement settles them.
func TestUnappliedDropsAreSentAgain(t *testing.T) {
	h, _ := withCountersAnd(t, []string{"n1", "n2"}, 1, 2, quickAck)
	r := h.nodes["n1"].rep
	d := tombDrop{"default", "t", store.Version{E: 2, N: 4}}
	r.sendTombstoneDrops("n2", 0, []tombDrop{d})
	r.tombs.mu.Lock()
	sent := r.tombs.sent["n2"]
	r.tombs.mu.Unlock()
	if len(sent) != 1 {
		t.Fatalf("%d kind-11 frames recorded sent, want 1", len(sent))
	}
	r.tombstoneDropsAcked("n2", sent[0].seq, true)
	r.tombs.mu.Lock()
	owed := len(r.tombs.owed["n2"][0])
	r.tombs.mu.Unlock()
	if owed != 1 {
		t.Fatalf("%d entries owed after an unapplied acknowledgement, want 1", owed)
	}
	r.resendOwedDrops("n2", 0)
	r.tombs.mu.Lock()
	owed, resent := len(r.tombs.owed["n2"][0]), r.tombs.sent["n2"]
	r.tombs.mu.Unlock()
	if owed != 0 || len(resent) != 1 || len(resent[0].drops) != 1 {
		t.Fatalf("after the resend: %d owed, frames %v; want none owed and the entry sent again", owed, resent)
	}
	r.tombstoneDropsAcked("n2", resent[0].seq, false)
	r.tombs.mu.Lock()
	left := len(r.tombs.sent["n2"]) + len(r.tombs.owed["n2"][0])
	r.tombs.mu.Unlock()
	if left != 0 {
		t.Fatalf("%d left after an applied acknowledgement, want none", left)
	}
}

// TestBelowDroppedIsCounted: after a shard's tombstones were dropped through a
// version, a hand-over installed into an empty topic at or below it is
// counted; one above it, or one that replaced what was held, is not.
func TestBelowDroppedIsCounted(t *testing.T) {
	h, sinks := withCountersAnd(t, []string{"n1", "n2"}, 1, 2, quickAck)
	r := h.nodes["n1"].rep
	r.noteDropped(0, []tombDrop{{"default", "t", store.Version{E: 4, N: 10}}})
	m := &message.Message{Namespace: "default", Topic: "u", Payload: []byte("x")}
	r.belowDropped(0, "handover", "n2", m, store.Version{E: 4, N: 9}, true)
	r.belowDropped(0, "handover", "n2", m, store.Version{E: 4, N: 8}, false)
	r.belowDropped(0, "handover", "n2", m, store.Version{E: 5, N: 1}, true)
	if got := sinks["n1"].get("hoardmq_retained_merges_total", map[string]string{"path": "handover", "outcome": "below-dropped"}); got != 1 {
		t.Fatalf("below-dropped counted %v, want 1: the install into an empty topic at or below the version", got)
	}
}

// TestTombstonePredicates: the drop and the follower's apply, each refusing
// row included.
func TestTombstonePredicates(t *testing.T) {
	hold := time.Hour
	for _, c := range []struct {
		name                 string
		held, moving, passed bool
		age                  time.Duration
		want                 bool
	}{
		{"goes", true, false, false, hold, true},
		{"followers do not hold it", false, false, false, hold, false},
		{"a move is pending", true, true, false, hold, false},
		{"an old holder is passed over", true, false, true, hold, false},
		{"younger than the hold", true, false, false, hold - time.Second, false},
	} {
		if got := tombstoneGoes(c.held, c.moving, c.passed, c.age, hold); got != c.want {
			t.Errorf("%s: tombstoneGoes = %v, want %v", c.name, got, c.want)
		}
	}
	for _, c := range []struct {
		name                     string
		senderLeads, leads, owes bool
		want                     bool
	}{
		{"applies", true, false, false, true},
		{"the sender does not lead the shard", false, false, false, false},
		{"this node leads it", true, true, false, false},
		{"this node owes the sender a hand-over", true, false, true, false},
	} {
		if got := kind11Applies(c.senderLeads, c.leads, c.owes); got != c.want {
			t.Errorf("%s: kind11Applies = %v, want %v", c.name, got, c.want)
		}
	}
}
