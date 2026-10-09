package lease

import (
	"fmt"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

func apply(t *testing.T, tb *Table, ops ...Op) []Result {
	t.Helper()
	r, err := tb.Apply(1, Encode(ops))
	if err != nil {
		t.Fatal(err)
	}
	return r.([]Result)
}

func TestLeaseLifecycle(t *testing.T) {
	tb := NewTable()
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	will := &store.WillRecord{Message: &message.Message{Topic: "w"}, Delay: 5}
	var changes []change
	tb.onChange = func(before, after *Lease) { changes = append(changes, change{before, after}) }

	r := apply(t, tb, Op{Kind: OpAcquire, Namespace: "d", ClientID: "x", Node: "n1", Will: will, ExpiryInterval: 60, Now: now})
	if !r[0].OK || r[0].Epoch != 1 || r[0].PrevOwner != "" {
		t.Fatalf("first acquire %+v", r[0])
	}
	// Same owner again: a renewal, epoch unchanged.
	r = apply(t, tb, Op{Kind: OpAcquire, Namespace: "d", ClientID: "x", Node: "n1", Now: now})
	if !r[0].OK || r[0].Epoch != 1 || r[0].PrevOwner != "" {
		t.Fatalf("renew %+v", r[0])
	}
	// Another node: a takeover with a higher epoch and the previous owner.
	r = apply(t, tb, Op{Kind: OpAcquire, Namespace: "d", ClientID: "x", Node: "n2", Now: now})
	if !r[0].OK || r[0].Epoch != 2 || r[0].PrevOwner != "n1" {
		t.Fatalf("takeover %+v", r[0])
	}
	if !tb.Valid("d", "x", "n2", 2) || tb.Valid("d", "x", "n1", 1) || tb.Valid("d", "x", "n2", 1) {
		t.Fatal("epoch fencing")
	}
	// A stale disconnect is refused; the current one records expiry.
	exp := now.Add(time.Minute)
	r = apply(t, tb,
		Op{Kind: OpDisconnected, Namespace: "d", ClientID: "x", Node: "n1", Epoch: 1, ExpiresAt: exp, Now: now},
		Op{Kind: OpDisconnected, Namespace: "d", ClientID: "x", Node: "n2", Epoch: 2, ExpiresAt: exp, WillArmed: true, Now: now})
	if r[0].OK || !r[1].OK {
		t.Fatalf("disconnected %+v", r)
	}
	l, _ := tb.Lookup("d", "x")
	if l.Online || !l.ExpiresAt.Equal(exp) || !l.DisconnectedAt.Equal(now) {
		t.Fatalf("offline lease %+v", l)
	}
	// Release with a stale epoch is a no-op; with the current one removes.
	r = apply(t, tb, Op{Kind: OpRelease, Namespace: "d", ClientID: "x", Node: "n2", Epoch: 1}, Op{Kind: OpRelease, Namespace: "d", ClientID: "x", Node: "n2", Epoch: 2})
	if r[0].OK || !r[1].OK || tb.Count() != 0 {
		t.Fatalf("release %+v count %d", r, tb.Count())
	}
	if len(changes) < 4 {
		t.Fatalf("change hook saw %d changes", len(changes))
	}
}

func TestAliveAndReassign(t *testing.T) {
	tb := NewTable()
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	will := &store.WillRecord{Message: &message.Message{Topic: "w"}, Delay: 5}
	apply(t, tb,
		Op{Kind: OpAlive, Node: "n1", Advertise: "n1:1883", Now: now},
		Op{Kind: OpAcquire, Namespace: "d", ClientID: "online", Node: "n1", Will: will, ExpiryInterval: 30, Now: now},
		Op{Kind: OpAcquire, Namespace: "d", ClientID: "offline", Node: "n1", Will: will, ExpiryInterval: 30, Now: now},
		Op{Kind: OpDisconnected, Namespace: "d", ClientID: "offline", Node: "n1", Epoch: 1, ExpiresAt: now.Add(20 * time.Second), WillArmed: true, Now: now},
		Op{Kind: OpAcquire, Namespace: "d", ClientID: "other", Node: "n2", Now: now})
	if n, ok := tb.Node("n1"); !ok || n.Advertise != "n1:1883" || !n.Alive.Equal(now) {
		t.Fatalf("node info %+v %v", n, ok)
	}
	// An older alive does not move the clock back.
	apply(t, tb, Op{Kind: OpAlive, Node: "n1", Now: now.Add(-time.Hour)})
	if n, _ := tb.Node("n1"); !n.Alive.Equal(now) {
		t.Fatal("alive moved backwards")
	}
	// Reassign refused when an alive record reached the log after the leader's
	// look: the record above applied at index 1, the look saw none.
	r := apply(t, tb, Op{Kind: OpReassign, Node: "n1", To: "n2", SeenAliveIndex: 0, Now: now.Add(time.Minute)})
	if r[0].OK {
		t.Fatal("reassign of a live owner accepted")
	}
	// Reassign with the observed alive time moves both leases, treating the
	// online one's death as its disconnect.
	later := now.Add(time.Minute)
	r = apply(t, tb, Op{Kind: OpReassign, Node: "n1", To: "n2", SeenAliveIndex: 1, Now: later})
	if !r[0].OK || r[0].Moved != 2 {
		t.Fatalf("reassign %+v", r[0])
	}
	on, _ := tb.Lookup("d", "online")
	if on.Owner != "n2" || on.Epoch != 2 || on.Online || !on.DisconnectedAt.Equal(later) || !on.ExpiresAt.Equal(later.Add(30*time.Second)) || on.Will == nil || !on.Will.Armed {
		t.Fatalf("adopted online lease %+v", on)
	}
	off, _ := tb.Lookup("d", "offline")
	if off.Owner != "n2" || off.Epoch != 2 || !off.ExpiresAt.Equal(now.Add(20*time.Second)) {
		t.Fatalf("adopted offline lease %+v", off)
	}
	if got := tb.OwnedBy("n2"); len(got) != 3 {
		t.Fatalf("n2 owns %d", len(got))
	}
	if owners := tb.Owners(); len(owners) != 1 || owners[0] != "n2" {
		t.Fatalf("owners %v", owners)
	}
	// Snapshot round trip.
	snap, err := tb.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	tb2 := NewTable()
	if err := tb2.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if tb2.Count() != 3 || !tb2.Valid("d", "online", "n2", 2) {
		t.Fatal("restore lost leases")
	}
	if n, ok := tb2.Node("n1"); !ok || n.Advertise != "n1:1883" {
		t.Fatal("restore lost node info")
	}
}

func TestHandoffMovesEveryLeaseWithoutAnAliveCheck(t *testing.T) {
	tb := NewTable()
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	will := &store.WillRecord{Message: &message.Message{Topic: "w"}, Delay: 5}
	apply(t, tb,
		Op{Kind: OpAlive, Node: "n1", Now: now},
		Op{Kind: OpAcquire, Namespace: "d", ClientID: "online", Node: "n1", Will: will, ExpiryInterval: 30, Now: now},
		Op{Kind: OpAcquire, Namespace: "d", ClientID: "offline", Node: "n1", ExpiryInterval: 30, Now: now},
		Op{Kind: OpDisconnected, Namespace: "d", ClientID: "offline", Node: "n1", Epoch: 1, ExpiresAt: now.Add(20 * time.Second), Now: now},
		Op{Kind: OpAcquire, Namespace: "d", ClientID: "other", Node: "n2", Now: now})
	// The owner was alive a moment ago; a reassign would be refused, a
	// hand-off is the owner's own decision.
	if r := apply(t, tb, Op{Kind: OpReassign, Node: "n1", To: "n2", SeenAliveIndex: 0, Now: now}); r[0].OK {
		t.Fatal("reassign of a live owner accepted")
	}
	if r := apply(t, tb, Op{Kind: OpHandoff, Node: "n1", To: "n1", Now: now}); r[0].OK {
		t.Fatal("hand-off to self accepted")
	}
	later := now.Add(time.Second)
	r := apply(t, tb, Op{Kind: OpHandoff, Node: "n1", To: "n2", Now: later})
	if !r[0].OK || r[0].Moved != 2 {
		t.Fatalf("hand-off %+v", r[0])
	}
	on, _ := tb.Lookup("d", "online")
	if on.Owner != "n2" || on.Epoch != 2 || on.Online || !on.DisconnectedAt.Equal(later) || on.Will == nil || !on.Will.Armed {
		t.Fatalf("handed-off online lease %+v", on)
	}
	off, _ := tb.Lookup("d", "offline")
	if off.Owner != "n2" || off.Epoch != 2 || !off.ExpiresAt.Equal(now.Add(20*time.Second)) {
		t.Fatalf("handed-off offline lease %+v", off)
	}
	if len(tb.OwnedBy("n1")) != 0 || len(tb.OwnedBy("n2")) != 3 {
		t.Fatalf("ownership after hand-off: n1 %d n2 %d", len(tb.OwnedBy("n1")), len(tb.OwnedBy("n2")))
	}
}

// TestSnapshotSizeTenThousandLeases is the measurement WI 1269's plan asked
// for (non-functional constraint): the serialised size of a 10,000-lease
// table, which 1268 predicted would become the largest snapshot section. It
// records rather than judges — the size is logged, and the only assertion
// is that the snapshot restores whole.
func TestSnapshotSizeTenThousandLeases(t *testing.T) {
	tb := NewTable()
	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	nodes := []string{"hoardmq-0", "hoardmq-1", "hoardmq-2"}
	for _, n := range nodes {
		apply(t, tb, Op{Kind: OpAlive, Node: n, Advertise: n + ".hoardmq-headless:1883", Now: now})
	}
	const leases = 10_000
	for start := 0; start < leases; start += 500 {
		ops := make([]Op, 0, 500)
		for i := start; i < start+500; i++ {
			// A realistic record: a 23-character client identifier, a third
			// of the fleet disconnected with an expiry deadline, one in ten
			// carrying an armed Will with a small payload.
			op := Op{Kind: OpAcquire, Namespace: "default",
				ClientID: fmt.Sprintf("device-%05d-aaaaaaaaaa", i),
				Node:     nodes[i%len(nodes)], ExpiryInterval: 3600, Now: now}
			if i%10 == 0 {
				op.Will = &store.WillRecord{Message: &message.Message{Topic: "state/" + op.ClientID, Payload: []byte(`{"online":false}`)}, Delay: 30, Armed: true}
			}
			ops = append(ops, op)
			if i%3 == 0 {
				ops = append(ops, Op{Kind: OpDisconnected, Namespace: "default", ClientID: op.ClientID, Node: op.Node, ExpiresAt: now.Add(time.Hour), Now: now})
			}
		}
		apply(t, tb, ops...)
	}
	snap, err := tb.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("snapshot of %d leases (3 nodes, 1/3 disconnected, 1/10 with a Will): %d bytes, %.0f bytes per lease", leases, len(snap), float64(len(snap))/leases)
	restored := NewTable()
	if err := restored.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if restored.Count() != leases {
		t.Fatalf("restored %d of %d leases", restored.Count(), leases)
	}
}

// TestRenewalNeverTakes: an acquire that carries an epoch renews the lease its
// node holds at that epoch and does nothing to any other; without one it
// takes the lease, as a takeover must.
func TestRenewalNeverTakes(t *testing.T) {
	now := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	tb := NewTable()
	var changes int
	tb.onChange = func(_, _ *Lease) { changes++ }
	acquire := func(node string, epoch uint64) Result {
		return apply(t, tb, Op{Kind: OpAcquire, Namespace: "d", ClientID: "x", Node: node, Epoch: epoch, ExpiryInterval: 60, Now: now})[0]
	}

	if r := acquire("a", 1); r.OK || r.Reason != "stale" || tb.Count() != 0 {
		t.Fatalf("a renewal created a lease that did not exist: %+v, %d leases", r, tb.Count())
	}
	if r := acquire("a", 0); !r.OK || r.Epoch != 1 {
		t.Fatalf("first acquire %+v", r)
	}
	apply(t, tb, Op{Kind: OpDisconnected, Namespace: "d", ClientID: "x", Node: "a", Epoch: 1, Now: now})
	if r := acquire("a", 1); !r.OK || r.Epoch != 1 || r.PrevOwner != "" {
		t.Fatalf("a renewal of a lease its node holds: %+v", r)
	}
	if l, _ := tb.Lookup("d", "x"); !l.Online {
		t.Fatal("the renewal did not mark the lease online")
	}

	if r := apply(t, tb, Op{Kind: OpReassign, Node: "a", To: "b", Now: now})[0]; !r.OK || r.Moved != 1 {
		t.Fatalf("reassign %+v", r)
	}
	changes = 0
	if r := acquire("a", 1); r.OK || r.Reason != "stale" {
		t.Fatalf("a late renewal from the old owner was applied: %+v", r)
	}
	if l, _ := tb.Lookup("d", "x"); l.Owner != "b" || l.Epoch != 2 || l.Online || changes != 0 {
		t.Fatalf("the lease is no longer b's at epoch 2, offline, untouched: %+v, %d changes", l, changes)
	}
	if r := acquire("b", 1); r.OK {
		t.Fatalf("a renewal at an old epoch was applied for the current owner: %+v", r)
	}

	if r := acquire("a", 0); !r.OK || r.Epoch != 3 || r.PrevOwner != "b" {
		t.Fatalf("a takeover %+v", r)
	}
}

// TestHookedIndexTrailsTheHooks: the index a reader pairs with what the
// change hooks produce is published only once they have returned. Published
// at apply, a reader could see the reassignment's index with none of the
// adoptions it queues, and take the node for idle.
func TestHookedIndexTrailsTheHooks(t *testing.T) {
	tb := NewTable()
	entered, release := make(chan struct{}), make(chan struct{})
	tb.onChange = func(_, _ *Lease) {
		close(entered)
		<-release
	}
	if _, err := tb.Apply(6, Encode([]Op{{Kind: OpAlive, Node: "n1", Now: time.Now()}})); err != nil {
		t.Fatal(err)
	}
	if got := tb.HookedIndex(); got != 6 {
		t.Fatalf("an operation with no lease change: hooked index %d, want 6", got)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = tb.Apply(7, Encode([]Op{{Kind: OpAcquire, Namespace: "default", ClientID: "c", Node: "n1", Now: time.Now()}}))
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the change hook never ran")
	}
	if _, ok := tb.Lookup("default", "c"); !ok {
		t.Fatal("the lease is not in the table while its hook runs; the test proves nothing")
	}
	if got := tb.HookedIndex(); got != 6 {
		t.Fatalf("hooked index %d while index 7's hook is still running, want 6", got)
	}
	close(release)
	<-done
	if got := tb.HookedIndex(); got != 7 {
		t.Fatalf("hooked index %d after the hook returned, want 7", got)
	}
}

// TestLostAtIsTheIndexANodeLastLostALeaseAt: taken from the log alone, so it
// is the same on every node, and it survives a snapshot.
func TestLostAtIsTheIndexANodeLastLostALeaseAt(t *testing.T) {
	tb := NewTable()
	now := time.Now()
	apply := func(index uint64, ops ...Op) {
		t.Helper()
		if _, err := tb.Apply(index, Encode(ops)); err != nil {
			t.Fatal(err)
		}
	}
	apply(3, Op{Kind: OpAcquire, Namespace: "default", ClientID: "a", Node: "n1", Now: now},
		Op{Kind: OpAcquire, Namespace: "default", ClientID: "b", Node: "n1", Now: now},
		Op{Kind: OpAcquire, Namespace: "default", ClientID: "c", Node: "n1", Now: now})
	if got := tb.LostAt("n1"); got != 0 {
		t.Fatalf("n1 has lost nothing: LostAt %d", got)
	}
	apply(5, Op{Kind: OpAcquire, Namespace: "default", ClientID: "a", Node: "n2", Now: now}) // a takeover
	if got := tb.LostAt("n1"); got != 5 {
		t.Fatalf("after a takeover at 5: LostAt(n1) %d", got)
	}
	apply(6, Op{Kind: OpDisconnected, Namespace: "default", ClientID: "b", Node: "n1", Epoch: 3, Now: now}) // changes no owner
	if got := tb.LostAt("n1"); got != 5 {
		t.Fatalf("a disconnect moved LostAt(n1) to %d", got)
	}
	apply(8, Op{Kind: OpRelease, Namespace: "default", ClientID: "b", Node: "n1", Epoch: 3, Now: now}) // b was granted at index 3
	if got := tb.LostAt("n1"); got != 8 {
		t.Fatalf("after a release at 8: LostAt(n1) %d", got)
	}
	apply(11, Op{Kind: OpReassign, Node: "n1", To: "n2", Now: now.Add(time.Hour)})
	if got, owns := tb.LostAt("n1"), tb.CountOwnedBy("n1"); got != 11 || owns != 0 {
		t.Fatalf("after the reassignment at 11: LostAt(n1) %d, n1 owns %d", got, owns)
	}
	if got := tb.LostAt("n2"); got != 0 {
		t.Fatalf("n2 only gained: LostAt %d", got)
	}
	data, err := tb.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	restored := NewTable()
	if err := restored.Restore(data); err != nil {
		t.Fatal(err)
	}
	if got := restored.LostAt("n1"); got != 11 {
		t.Fatalf("restored LostAt(n1) %d, want 11", got)
	}
}

// TestEpochIsTheGrantsLogIndex: a lease's epoch is the log index of the
// command that last changed its owner, so it never goes backwards — not even
// across a release and a fresh acquire, where a counter would restart at one
// and a follower would take the old owner's straggler for the newer record.
func TestEpochIsTheGrantsLogIndex(t *testing.T) {
	tb := NewTable()
	now := time.Now()
	apply := func(index uint64, ops ...Op) []Result {
		t.Helper()
		out, err := tb.Apply(index, Encode(ops))
		if err != nil {
			t.Fatal(err)
		}
		return out.([]Result)
	}
	epoch := func(id string) uint64 {
		l, _ := tb.Lookup("default", id)
		return l.Epoch
	}
	acquire := func(id, node string) Op {
		return Op{Kind: OpAcquire, Namespace: "default", ClientID: id, Node: node, Now: now}
	}
	if res := apply(40, acquire("a", "n1"), acquire("b", "n1")); res[0].Epoch != 40 || epoch("b") != 40 {
		t.Fatalf("granted at index 40: result %d, table %d", res[0].Epoch, epoch("b"))
	}
	if res := apply(55, acquire("a", "n2")); res[0].Epoch != 55 || res[0].PrevOwner != "n1" {
		t.Fatalf("taken over at index 55: %+v", res[0])
	}
	renew := acquire("a", "n2")
	renew.Epoch = 55
	if res := apply(58, renew); !res[0].OK || epoch("a") != 55 {
		t.Fatalf("a same-owner renewal at 58 moved the token to %d", epoch("a"))
	}
	apply(60, Op{Kind: OpRelease, Namespace: "default", ClientID: "a", Node: "n2", Epoch: 55, Now: now})
	if res := apply(70, acquire("a", "n1")); res[0].Epoch != 70 {
		t.Fatalf("released at 60 and acquired again at 70: epoch %d; a counter would say 1", res[0].Epoch)
	}
	apply(90, Op{Kind: OpReassign, Node: "n1", To: "n3", Now: now.Add(time.Hour)})
	if epoch("a") != 90 || epoch("b") != 90 {
		t.Fatalf("a reassignment at 90 moved the leases to %d and %d", epoch("a"), epoch("b"))
	}
	// Where Apply is driven with no index the epoch still only rises.
	plain := NewTable()
	for i, node := range []string{"n1", "n2", "n1"} {
		out, _ := plain.Apply(0, Encode([]Op{acquire("c", node)}))
		if got := out.([]Result)[0].Epoch; got != uint64(i+1) {
			t.Fatalf("grant %d with no log index: epoch %d", i+1, got)
		}
	}
}

// TestOneCommandGrantsAClientToOneNode: two owners at one index would hold
// the same token. A command carries one node's operations, so it cannot
// happen; the table refuses it all the same.
func TestOneCommandGrantsAClientToOneNode(t *testing.T) {
	tb := NewTable()
	now := time.Now()
	out, err := tb.Apply(12, Encode([]Op{
		{Kind: OpAcquire, Namespace: "default", ClientID: "c", Node: "n1", Now: now},
		{Kind: OpAcquire, Namespace: "default", ClientID: "c", Node: "n1", Epoch: 12, Now: now}, // its own renewal: fine
		{Kind: OpAcquire, Namespace: "default", ClientID: "c", Node: "n2", Now: now},
	}))
	if err != nil {
		t.Fatal(err)
	}
	res := out.([]Result)
	if !res[0].OK || !res[1].OK {
		t.Fatalf("the grant and its own renewal: %+v", res[:2])
	}
	if res[2].OK || res[2].Reason != doubleGrant {
		t.Fatalf("a second owner in the same command: %+v, want it refused", res[2])
	}
	if l, _ := tb.Lookup("default", "c"); l.Owner != "n1" || l.Epoch != 12 {
		t.Fatalf("the lease after the refused grant: %+v", l)
	}
	// The next command may move it.
	out, _ = tb.Apply(13, Encode([]Op{{Kind: OpAcquire, Namespace: "default", ClientID: "c", Node: "n2", Now: now}}))
	if got := out.([]Result)[0]; !got.OK || got.Epoch != 13 {
		t.Fatalf("the next command's grant: %+v", got)
	}
}
