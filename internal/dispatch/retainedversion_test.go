package dispatch

import (
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

func rmsg(tp, payload string) *message.Message {
	return &message.Message{Namespace: "default", Topic: tp, Payload: []byte(payload), Retain: true}
}

// TestExpiredVersionedEntryIsLeftToTheSweep: a lookup that meets an expired
// stamped message serves nothing and removes nothing, so an older copy handed
// over before the sweep stays below it; the sweep at the shard's leader then
// leaves a tombstone stamped above it, and at another node removes it. An
// expired unversioned message the lookup removes.
func TestExpiredVersionedEntryIsLeftToTheSweep(t *testing.T) {
	r := New(RoundRobin)
	now := time.Unix(1000, 0)
	m := rmsg("a/v", "stamped")
	m.Expiry = now.Add(time.Second)
	v, ok := r.RetainStamped(m, 4, 0)
	if !ok {
		t.Fatal("the stamped write was not installed")
	}
	u := rmsg("a/u", "unversioned")
	u.Expiry = now.Add(time.Second)
	r.Retain(u)
	later := now.Add(2 * time.Second)
	for _, filter := range []string{"a/v", "a/+", "#"} {
		if got := r.RetainedFor("default", filter, later); len(got) != 0 {
			t.Fatalf("%s served %d expired messages", filter, len(got))
		}
	}
	if e, ok := r.HeldRetained("default", "a/v"); !ok || e.RV != v {
		t.Fatalf("the expired stamped entry: held %v at %v, want kept at %v for the sweep", ok, e.RV, v)
	}
	if _, ok := r.HeldRetained("default", "a/u"); ok {
		t.Fatal("the expired unversioned entry was not removed by the lookup")
	}
	if installed, _ := r.MergeRetained(rmsg("a/v", "older"), store.Version{E: 4, N: v.N - 1}, true); installed {
		t.Fatal("an older copy installed over the expired stamped entry")
	}
	follower := New(RoundRobin)
	if _, ok := follower.RetainStamped(m, 4, 0); !ok {
		t.Fatal("precondition")
	}
	if got := follower.SweepLeading(later, func(string, string) (uint64, bool) { return 0, false }); len(got) != 0 {
		t.Fatalf("a non-leader's sweep stamped %d tombstones", len(got))
	}
	if _, ok := follower.HeldRetained("default", "a/v"); ok {
		t.Fatal("a non-leader's sweep kept the expired entry")
	}
	got := r.SweepLeading(later, func(string, string) (uint64, bool) { return 5, true })
	if len(got) != 1 || !got[0].Tombstone || !v.Less(got[0].RV) || got[0].RV.E != 5 {
		t.Fatalf("the leader's sweep returned %+v, want one tombstone at epoch 5 above %v", got, v)
	}
	if e, _ := r.HeldRetained("default", "a/v"); !e.Tombstone || e.RV != got[0].RV {
		t.Fatalf("the leader holds %+v, want the sweep's tombstone", e)
	}
}

// TestTrieStampMergeAndTombstones: the trie's stamp rises, merge installs only
// a higher version (an unversioned copy into an empty topic, the later of two
// unversioned off a hand-over path), a tombstone is held and counted for the
// limit but never served through any filter, and a tombstone goes only by a
// compare-and-delete at its own version.
func TestTrieStampMergeAndTombstones(t *testing.T) {
	r := New(RoundRobin)
	v1, _ := r.RetainStamped(rmsg("s/a", "1"), 2, 0)
	v2, _ := r.RetainStamped(rmsg("s/a", "2"), 2, 0)
	if !v1.Less(v2) || v1.E != 2 {
		t.Fatalf("stamps %v then %v, want rising in epoch 2", v1, v2)
	}
	if installed, held := r.MergeRetained(rmsg("s/a", "old"), v1, true); installed || held.RV != v2 {
		t.Fatalf("a lower copy: installed %v, held %v", installed, held.RV)
	}
	if installed, _ := r.MergeRetained(rmsg("s/a", "same"), v2, false); installed {
		t.Fatal("an equal version installed")
	}
	hi := store.Version{E: 3, N: 1000}
	if installed, held := r.MergeRetained(rmsg("s/a", "new"), hi, true); !installed || held.RV != hi || string(held.Msg.Payload) != "new" {
		t.Fatalf("a higher copy: installed %v, held %+v", installed, held)
	}
	if installed, _ := r.MergeRetained(rmsg("s/b", "u1"), store.Version{}, true); !installed {
		t.Fatal("an unversioned copy into an empty topic was not installed")
	}
	if installed, _ := r.MergeRetained(rmsg("s/b", "u2"), store.Version{}, true); installed {
		t.Fatal("an unversioned hand-over replaced an unversioned message")
	}
	if installed, _ := r.MergeRetained(rmsg("s/b", "u3"), store.Version{}, false); !installed {
		t.Fatal("an unversioned stream copy did not replace an unversioned message")
	}
	if v4, _ := r.RetainStamped(rmsg("s/c", "x"), 3, 0); !hi.Less(v4) {
		t.Fatalf("the stamp after a merged %v is %v, want above it", hi, v4)
	}
	tv, ok := r.RetainStamped(&message.Message{Namespace: "default", Topic: "s/c", Retain: true}, 3, 0)
	if !ok {
		t.Fatal("the clear was not taken")
	}
	for _, filter := range []string{"s/c", "s/+", "#", "s/#"} {
		for _, m := range r.RetainedFor("default", filter, time.Unix(0, 0)) {
			if m.Topic == "s/c" {
				t.Fatalf("%s served the tombstone", filter)
			}
		}
	}
	if r.RetainedCount() != 2 {
		t.Fatalf("RetainedCount %d, want the two messages", r.RetainedCount())
	}
	if _, ok := r.RetainStamped(rmsg("s/d", "x"), 3, 3); ok {
		t.Fatal("a new topic was taken at a limit of 3 with two messages and a tombstone held")
	}
	if _, ok := r.RetainStamped(&message.Message{Namespace: "default", Topic: "s/c", Retain: true}, 3, 3); !ok {
		t.Fatal("a clear of a tombstoned topic at the limit was refused")
	}
	e, _ := r.HeldRetained("default", "s/c")
	if r.DropTombstoneIf("default", "s/c", tv) {
		t.Fatal("a compare-and-delete at a replaced tombstone's version took the newer one")
	}
	if r.DropTombstoneIf("default", "s/a", hi) {
		t.Fatal("a compare-and-delete took a message")
	}
	if !r.DropTombstoneIf("default", "s/c", e.RV) {
		t.Fatal("the compare-and-delete at the tombstone's version did not take it")
	}
	if _, ok := r.HeldRetained("default", "s/c"); ok {
		t.Fatal("the dropped tombstone is still held")
	}
}

// TestMarkAppendedNamesTheEntry: the append count is recorded only on the
// entry that was appended: a later write at the same version, as two
// unversioned writes share one, is not marked by the earlier one's append.
func TestMarkAppendedNamesTheEntry(t *testing.T) {
	r := New(RoundRobin)
	first := rmsg("m", "first")
	r.Retain(first)
	second := rmsg("m", "second")
	r.Retain(second)
	r.MarkRetainedAppended(first, store.Version{}, 7)
	if e, _ := r.HeldRetained("default", "m"); e.Appended != 0 {
		t.Fatalf("the later unversioned entry is marked appended at %d by the earlier one's append", e.Appended)
	}
	r.MarkRetainedAppended(second, store.Version{}, 8)
	if e, _ := r.HeldRetained("default", "m"); e.Appended != 8 {
		t.Fatalf("the entry is marked at %d, want 8", e.Appended)
	}
	v, _ := r.RetainStamped(rmsg("n", "x"), 2, 0)
	r.MarkRetainedAppended(rmsg("n", "x"), v, 9)
	if e, _ := r.HeldRetained("default", "n"); e.Appended != 0 {
		t.Fatalf("another message at the entry's version marked it appended at %d", e.Appended)
	}
}

// TestTrieRefusesUnversionedOverVersioned: the trie stores no unversioned
// write, message or delete, over a stamped message or tombstone, and says so;
// over an unversioned entry or into an empty topic it stores the write.
func TestTrieRefusesUnversionedOverVersioned(t *testing.T) {
	r := New(RoundRobin)
	r.RetainStamped(rmsg("v", "stamped"), 2, 0)
	r.RetainStamped(&message.Message{Namespace: "default", Topic: "c", Retain: true}, 2, 0)
	for _, m := range []*message.Message{rmsg("v", "later"), {Namespace: "default", Topic: "v", Retain: true}, rmsg("c", "later")} {
		if got := r.RetainUnder(m, 0); got != store.RetainVersionedHeld {
			t.Fatalf("an unversioned write to %s over a stamped entry: %v, want refused", m.Topic, got)
		}
	}
	if e, _ := r.HeldRetained("default", "v"); string(e.Msg.Payload) != "stamped" {
		t.Fatalf("v holds %q, want the stamped write", e.Msg.Payload)
	}
	if got := r.RetainUnder(rmsg("u", "1"), 0); got != store.RetainStored {
		t.Fatalf("an unversioned write into an empty topic: %v", got)
	}
	if got := r.RetainUnder(rmsg("u", "2"), 0); got != store.RetainStored {
		t.Fatalf("an unversioned write over an unversioned entry: %v", got)
	}
}
