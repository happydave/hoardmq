package replication

import (
	"context"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/lease"
	"github.com/happydave/hoardmq/internal/store"
)

// withTokens is a three-node harness whose nodes each keep the highest grant
// token seen per client, as a broker's do.
func withTokens(t *testing.T) (*harness, map[string]*lease.Tokens) {
	t.Helper()
	tokens := map[string]*lease.Tokens{}
	h := newHarnessWith(t, []string{"n1", "n2", "n3"}, 2, 2, func(o *Options) {
		quickAck(o)
		tokens[o.Node] = lease.NewTokens()
		o.Tokens = tokens[o.Node]
	})
	return h, tokens
}

// setLeaseOn sets one node's lease table entry for a client.
func setLeaseOn(h *harness, node, id, owner string, epoch uint64) {
	h.nodes[node].leases.set("default", id, owner, epoch)
}

// writeOwned journals a session on owner at epoch with the given entries, so
// its tap replicates each record at that epoch, and gives the fake session
// manager the same state.
func writeOwned(t *testing.T, h *harness, owner, id string, epoch uint64, entries ...store.Entry) {
	t.Helper()
	n := h.nodes[owner]
	k := store.Key{Namespace: "default", ClientID: id, Epoch: epoch}
	if err := n.own.SessionUpsert(store.SessionMeta{Namespace: "default", ClientID: id, Epoch: epoch, Version: 5, ExpiryInterval: 300}); err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if err := n.own.Enqueue(k, e); err != nil {
			t.Fatal(err)
		}
	}
	if err := n.own.Sync(); err != nil {
		t.Fatal(err)
	}
	feedSessionAt(n, id, epoch, entries...)
}

// copyEpochOf is the epoch of a node's replica copy of id, 0 if none.
func copyEpochOf(h *harness, node, id string) uint64 {
	ss := h.nodes[node].rep.ReplicaState().Find("default", id)
	if ss == nil {
		return 0
	}
	return ss.Epoch
}

// TestAPreviousOwnersWholeWriteDoesNotReplaceTheTakersCopy: n2 owned X at
// token 44 and n3 holds its copy. n1 takes X at 63; n1 and n2 have seen the
// grant, n3's table has not applied it. n1's import of X, with an entry n2
// never had, reaches n3 and its barrier passes. n2, still serving its old
// copy, then resyncs n3. Its whole write must not replace n1's copy at n3.
func TestAPreviousOwnersWholeWriteDoesNotReplaceTheTakersCopy(t *testing.T) {
	h, tokens := withTokens(t)
	allAlive(h)
	h.applyMap(moveMap(h, 1, sh("n3", "n1"), sh("n2", "n3")))
	waitSettled(h, 1)
	id := clientIn(2, 0)
	h.setLease("default", id, "n2", 44)
	writeOwned(t, h, "n2", id, 44, queued(1, "queued"))
	h.waitFor("n3 to hold n2's copy", 5*time.Second, func() bool { return holdsEntry(h, "n3", id, 1) })
	for _, n := range []string{"n1", "n2"} {
		setLeaseOn(h, n, id, "n1", 63)
		tokens[n].Note("default", id, 63)
	}
	writeOwned(t, h, "n1", id, 63, queued(1, "queued"), queued(2, "taker's"))
	ctx := context.Background()
	if err := h.nodes["n1"].rep.Barrier(ctx, []store.Scope{{Namespace: "default", ClientID: id, Epoch: 63}}); err != nil {
		t.Fatalf("the taker's barrier: %v", err)
	}
	if !holdsEntry(h, "n3", id, 2) || copyEpochOf(h, "n3", id) != 63 {
		t.Fatalf("precondition: n3 should hold the taker's copy at 63 with entry 2 (epoch %d)", copyEpochOf(h, "n3", id))
	}
	if l, _ := h.nodes["n3"].leases.Lookup("default", id); l.Epoch != 44 {
		t.Fatalf("precondition: n3's table should lag at 44, it has %+v", l)
	}
	n2 := h.nodes["n2"]
	n2.rep.resyncPeer("n3")
	if err := n2.rep.outboundFor("n3").wait(ctx, n2.rep.up, nil); err != nil {
		t.Fatalf("waiting for n3 to answer n2's resync: %v", err)
	}
	if !holdsEntry(h, "n3", id, 2) {
		t.Fatalf("n3's copy lost the taker's entry: n2's whole write of its own copy (held at 44) replaced it, at epoch %d", copyEpochOf(h, "n3", id))
	}
}

// TestATakersWholeWriteIsTakenWhileItsTableLags: n2 has taken X over at token
// 12725 and serves it at that epoch, but its own table still names n1 at 94.
// n3's table has the grant. n2's whole write of X to n3 must be accepted, and
// a barrier over X at its token pass.
func TestATakersWholeWriteIsTakenWhileItsTableLags(t *testing.T) {
	h, tokens := withTokens(t)
	allAlive(h)
	h.applyMap(moveMap(h, 1, sh("n3", "n2"), sh("n2", "n3")))
	waitSettled(h, 1)
	id := clientIn(2, 0)
	h.setLease("default", id, "n1", 94)
	setLeaseOn(h, "n3", id, "n2", 12725)
	tokens["n3"].Note("default", id, 12725)
	tokens["n2"].Note("default", id, 12725)
	feedSessionAt(h.nodes["n2"], id, 12725, queued(1, "queued"))
	n2 := h.nodes["n2"]
	n2.rep.resyncPeer("n3")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := n2.rep.Barrier(ctx, []store.Scope{{Namespace: "default", ClientID: id, Epoch: 12725}}); err != nil {
		t.Fatalf("a barrier over X at the taker's token after its whole write: %v (n3 holds a copy: %v)", err, holdsCopy(h, "n3", id))
	}
	if !holdsEntry(h, "n3", id, 1) || copyEpochOf(h, "n3", id) != 12725 {
		t.Fatalf("n3 holds no copy of X at 12725 (epoch %d)", copyEpochOf(h, "n3", id))
	}
}

// TestAWholeWriteOfASessionWithNoEpochCarriesTheTablesToken: n2 serves X with
// no epoch, and every table names n2 at token 7. n2's whole write to n3 must
// carry 7, as before a whole copy carried its session's epoch: n3 notes the
// token of a copy it takes only when the copy carries one.
func TestAWholeWriteOfASessionWithNoEpochCarriesTheTablesToken(t *testing.T) {
	h, tokens := withTokens(t)
	allAlive(h)
	h.applyMap(moveMap(h, 1, sh("n3", "n2"), sh("n2", "n3")))
	waitSettled(h, 1)
	id := clientIn(2, 0)
	h.setLease("default", id, "n2", 7)
	feedSessionAt(h.nodes["n2"], id, 0, queued(1, "queued"))
	if seen := tokens["n3"].Seen("default", id); seen != 0 {
		t.Fatalf("precondition: n3 has seen token %d for X, want none", seen)
	}
	n2 := h.nodes["n2"]
	n2.rep.resyncPeer("n3")
	if err := n2.rep.outboundFor("n3").wait(context.Background(), n2.rep.up, nil); err != nil {
		t.Fatalf("waiting for n3 to answer n2's resync: %v", err)
	}
	if !holdsEntry(h, "n3", id, 1) {
		t.Fatal("n3 did not take n2's whole write")
	}
	if seen := tokens["n3"].Seen("default", id); seen != 7 {
		t.Fatalf("n3 saw token %d from n2's whole write of a session with no epoch, want the table's 7", seen)
	}
}
