package replication

import (
	"context"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/lease"
	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

// atLevel2 stamps every frame at level 2 and gives each node grant tokens,
// returning them by node.
func atLevel2(tokens map[string]*lease.Tokens, extra func(*Options)) func(*Options) {
	return func(o *Options) {
		tokens[o.Node] = lease.NewTokens()
		o.Tokens = tokens[o.Node]
		o.Level = func() uint64 { return 2 }
		o.BuildLevel = func() uint64 { return 2 }
		if extra != nil {
			extra(o)
		}
	}
}

// tornWholeWrite sets up a follower whose copy an owner's whole write, made
// in records as an older owner makes it, tears: n1 owns the client at 5 and
// n2 holds its copy with one subscription. n1 writes the session whole at 7,
// the delete first; n2 applies the delete; then its own acquire at 9 is
// granted, so the token noted is 9 while its table still names n1 at 7; then
// the rest of the write arrives at 7 and is refused as behind the grant seen.
func tornWholeWrite(t *testing.T, extra func(*Options)) (*harness, map[string]*counters, map[string]*lease.Tokens, string) {
	t.Helper()
	tokens := map[string]*lease.Tokens{}
	h, sinks := withCountersAnd(t, []string{"n1", "n2", "n3"}, 1, 3, atLevel2(tokens, extra))
	h.applyMap(shardmap.Compute(1, h.order, 3, 1))
	waitSettled(h, 1)
	id := clientIn(1, 0)
	n1, n2 := h.nodes["n1"], h.nodes["n2"]
	writeSessionWhole(t, h, "n1", id, 5)
	h.waitFor("n2 to hold n1's copy with its subscription", 5*time.Second, func() bool {
		ss := n2.rep.ReplicaState().Find("default", id)
		return ss != nil && len(ss.Subscriptions) == 1
	})

	h.setLease("default", id, "n1", 7)
	k := store.Key{Namespace: "default", ClientID: id, Epoch: 7}
	if err := n1.own.SessionDelete(k); err != nil {
		t.Fatal(err)
	}
	if err := n1.own.Sync(); err != nil {
		t.Fatal(err)
	}
	h.waitFor("n2 to apply the whole write's delete", 5*time.Second, func() bool { return !holdsCopy(h, "n2", id) })

	tokens["n2"].Note("default", id, 9)
	if err := n1.own.SessionUpsert(store.SessionMeta{Namespace: "default", ClientID: id, Epoch: 7, Version: 5, ExpiryInterval: 300}); err != nil {
		t.Fatal(err)
	}
	if err := n1.own.Subscribe(k, store.SubscriptionRecord{Filter: "a/b", QoS: 1}); err != nil {
		t.Fatal(err)
	}
	if err := n1.own.Sync(); err != nil {
		t.Fatal(err)
	}
	h.waitFor("n2 to refuse the rest of the whole write", 5*time.Second, func() bool { return staleTokens(sinks, "n2") >= 1 })
	return h, sinks, tokens, id
}

// TestARecordsFormWholeWriteTornByAGrantLeavesNoCopy is the records form's
// control: an older owner's whole write, torn by a grant seen between its
// records, leaves n2 without a copy, which a copy whole or absent allows.
func TestARecordsFormWholeWriteTornByAGrantLeavesNoCopy(t *testing.T) {
	h, _, _, id := tornWholeWrite(t, nil)
	if holdsCopy(h, "n2", id) {
		t.Fatalf("n2 holds a copy after a records-form whole write whose delete it applied and whose rest it refused; want none")
	}
}

// importWhole writes a session whole at n1 as an import does: through an open
// whole write, so its records go to the followers as one snapshot frame at
// close. grant runs before the frame is sent.
func importWhole(t *testing.T, h *harness, id string, epoch uint64, grant func()) {
	t.Helper()
	n1 := h.nodes["n1"]
	h.setLease("default", id, "n1", epoch)
	k := store.Key{Namespace: "default", ClientID: id, Epoch: epoch}
	st := &store.SessionState{SessionMeta: store.SessionMeta{Namespace: "default", ClientID: id, Epoch: epoch, Version: 5, ExpiryInterval: 300},
		Subscriptions: []store.SubscriptionRecord{{Filter: "a/b", QoS: 1}}, InboundQoS2: []uint16{}, Entries: []store.Entry{}}
	closeWhole := n1.rep.OpenWhole("default", id, causeImport)
	if closeWhole == nil {
		t.Fatal("the replicator sends whole writes as records")
	}
	for _, write := range []func() error{
		func() error { return n1.own.SessionDelete(k) },
		func() error { return n1.own.SessionUpsert(st.SessionMeta) },
		func() error { return n1.own.Subscribe(k, st.Subscriptions[0]) },
	} {
		if err := write(); err != nil {
			t.Fatal(err)
		}
	}
	if grant != nil {
		grant()
	}
	closeWhole(st, true)
	if err := n1.own.Sync(); err != nil {
		t.Fatal(err)
	}
}

// TestAWholeWriteIsNotTornByAGrantSeenBetweenItsRecords: an import's whole
// write reaches n2 as one frame, judged once. A grant noted before it arrives
// refuses it whole, and n2 keeps the copy it had; one noted after it was
// applied leaves n1's session. Either way n2 holds the session with its
// subscription.
func TestAWholeWriteIsNotTornByAGrantSeenBetweenItsRecords(t *testing.T) {
	t.Run("grant before the frame", func(t *testing.T) {
		tokens := map[string]*lease.Tokens{}
		h, sinks := withCountersAnd(t, []string{"n1", "n2", "n3"}, 1, 3, atLevel2(tokens, nil))
		h.applyMap(shardmap.Compute(1, h.order, 3, 1))
		waitSettled(h, 1)
		id := clientIn(1, 0)
		writeSessionWhole(t, h, "n1", id, 5)
		h.waitFor("n2 to hold n1's copy", 5*time.Second, func() bool { return copyEpochOf(h, "n2", id) == 5 })
		importWhole(t, h, id, 7, func() { tokens["n2"].Note("default", id, 9) })
		h.waitFor("n2 to refuse the whole write", 5*time.Second, func() bool { return staleTokens(sinks, "n2") >= 1 })
		ss := h.nodes["n2"].rep.ReplicaState().Find("default", id)
		if ss == nil || len(ss.Subscriptions) != 1 || ss.Epoch != 5 {
			t.Fatalf("n2's copy after a whole write refused for a grant seen before it: held %v, subscriptions %d; want the copy it had at 5 with its subscription", ss != nil, subsIn(ss))
		}
	})
	t.Run("grant after the frame", func(t *testing.T) {
		tokens := map[string]*lease.Tokens{}
		h, _ := withCountersAnd(t, []string{"n1", "n2", "n3"}, 1, 3, atLevel2(tokens, nil))
		h.applyMap(shardmap.Compute(1, h.order, 3, 1))
		waitSettled(h, 1)
		id := clientIn(1, 0)
		writeSessionWhole(t, h, "n1", id, 5)
		h.waitFor("n2 to hold n1's copy", 5*time.Second, func() bool { return copyEpochOf(h, "n2", id) == 5 })
		toN2 := h.nodes["n1"].rep.outboundFor("n2")
		before := toN2.lastSent()
		importWhole(t, h, id, 7, nil)
		if sent := toN2.lastSent() - before; sent != 1 {
			t.Fatalf("the whole write went to n2 as %d frames; want one snapshot", sent)
		}
		h.waitFor("n2 to apply the whole write", 5*time.Second, func() bool { return copyEpochOf(h, "n2", id) == 7 })
		tokens["n2"].Note("default", id, 9)
		ss := h.nodes["n2"].rep.ReplicaState().Find("default", id)
		if ss == nil || len(ss.Subscriptions) != 1 {
			t.Fatalf("n2's copy after a whole write applied before a grant: held %v, subscriptions %d; want n1's session with its subscription", ss != nil, subsIn(ss))
		}
	})
}

// servedAt12 makes n1's session manager serve the session at 12 with its
// subscription, as the real manager holds it when the client reconnects.
func servedAt12(n *tnode, id string) {
	n.sessions.mu.Lock()
	n.sessions.sessions = []*store.SessionState{{
		SessionMeta:   store.SessionMeta{Namespace: "default", ClientID: id, Epoch: 12, Version: 5, ExpiryInterval: 300},
		Subscriptions: []store.SubscriptionRecord{{Filter: "a/b", QoS: 1}}, InboundQoS2: []uint16{}, Entries: []store.Entry{},
	}}
	n.sessions.mu.Unlock()
}

// resumeAt12 is the client reconnecting to n1, which holds the session, at
// grant 12: the connect writes the session's meta record alone.
func resumeAt12(t *testing.T, h *harness, id string) {
	t.Helper()
	n1 := h.nodes["n1"]
	h.setLease("default", id, "n1", 12)
	if err := n1.own.SessionUpsert(store.SessionMeta{Namespace: "default", ClientID: id, Epoch: 12, Version: 5, ExpiryInterval: 300}); err != nil {
		t.Fatal(err)
	}
	if err := n1.own.Sync(); err != nil {
		t.Fatal(err)
	}
}

// TestAMetaRecordDoesNotMakeAHollowCopy: after the torn write, the client
// reconnects to n1 at grant 12 and the connect writes the meta record alone.
// n2, holding no copy, refuses it rather than make a session with no
// subscriptions; n1 repairs the key whole, so a fetch from n2 hands a
// takeover the session as its owner holds it.
func TestAMetaRecordDoesNotMakeAHollowCopy(t *testing.T) {
	h, _, _, id := tornWholeWrite(t, nil)
	if holdsCopy(h, "n2", id) {
		t.Skip("the torn write did not leave n2 without a copy")
	}
	n2 := h.nodes["n2"]
	servedAt12(h.nodes["n1"], id)
	resumeAt12(t, h, id)
	h.waitFor("n2 to apply the connect's meta record", 5*time.Second, func() bool { return copyEpochOf(h, "n2", id) == 12 })
	st, answered, _ := n2.rep.Fetch(t.Context(), "default", id, "n1")
	if st != nil && len(st.Subscriptions) != 1 {
		t.Fatalf("n2's fetch, answered %v, hands a takeover a copy at %d with %d subscriptions while its owner n1 holds the session with 1", answered, st.Epoch, len(st.Subscriptions))
	}
}

// TestAMetaRecordFindingNoCopyIsRepairedByADelete: as above, but n1 no longer
// serves the session, so its repair is the session's delete: n2 ends holding
// nothing, and a fetch from it finds nothing.
func TestAMetaRecordFindingNoCopyIsRepairedByADelete(t *testing.T) {
	h, sinks, _, id := tornWholeWrite(t, nil)
	resumeAt12(t, h, id)
	h.waitFor("n1 to repair the key with its delete", 5*time.Second, func() bool { return repairsAny(sinks, "n1", "delete") >= 1 })
	// n2's own stream: n3, which applied the records-form write's delete and
	// refused its meta, is repaired on its own schedule.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	up := func(string) bool { return true }
	if err := h.nodes["n1"].rep.outboundFor("n2").wait(ctx, up, []store.Scope{{Namespace: "default", ClientID: id, Epoch: 12}}); err != nil {
		t.Fatalf("the delete repair was not acknowledged by n2: %v; refused at n1 %v", err, h.nodes["n1"].rep.RefusedKeys())
	}
	if holdsCopy(h, "n2", id) {
		t.Fatal("n2 holds a copy after a meta record it refused and a delete repair; want none")
	}
	if st, _, _ := h.nodes["n2"].rep.Fetch(t.Context(), "default", id, "n1"); st != nil {
		t.Fatalf("n2's fetch hands back a copy at %d with %d subscriptions; want nothing", st.Epoch, len(st.Subscriptions))
	}
}

// TestDefectLoneMetaMakesCopy is R23's defect seam: with whole writes sent as
// records and a lone meta record applied whatever its level, the meta
// reproduction above leaves n2 serving a copy without its subscription.
func TestDefectLoneMetaMakesCopy(t *testing.T) {
	h, _, _, id := tornWholeWrite(t, func(o *Options) { o.LoneMetaMakesCopy = true })
	servedAt12(h.nodes["n1"], id)
	resumeAt12(t, h, id)
	h.waitFor("n2 to apply the connect's meta record", 5*time.Second, func() bool { return copyEpochOf(h, "n2", id) == 12 })
	st, _, _ := h.nodes["n2"].rep.Fetch(t.Context(), "default", id, "n1")
	if st == nil || len(st.Subscriptions) != 0 {
		t.Fatalf("under the defect n2's fetch hands back %v with %d subscriptions; want the hollow copy (0) the defect makes", st != nil, subsIn(st))
	}
	if h.nodes["n1"].rep.OpenWhole("default", id, causeImport) != nil {
		t.Fatal("under the defect the owner opens a whole write; want records")
	}
}

// repairsAny counts a node's repairs with an outcome, whatever their cause.
func repairsAny(sinks map[string]*counters, node, outcome string) float64 {
	var n float64
	for _, cause := range []string{repairUnapplied, repairEarlierGrant} {
		n += sinks[node].get("hoardmq_replication_repairs_total", map[string]string{"cause": cause, "outcome": outcome})
	}
	return n
}

// writeSessionWhole writes a session as a brand-new one reaches its targets at
// level 2: its records (meta, subscription and a queued entry, as
// writeSession writes them) inside one whole write, sent as one snapshot; as
// records where the replicator sends records.
func writeSessionWhole(t *testing.T, h *harness, owner, id string, epoch uint64) {
	t.Helper()
	st := &store.SessionState{SessionMeta: store.SessionMeta{Namespace: "default", ClientID: id, Epoch: epoch, Version: 5, ExpiryInterval: 300},
		Subscriptions: []store.SubscriptionRecord{{Filter: "a/b", QoS: 1}}, InboundQoS2: []uint16{},
		Entries: []store.Entry{{Seq: 1, Message: &message.Message{Namespace: "default", Topic: "a/b", Payload: []byte("queued")}, QoS: 1}}}
	closeWhole := h.nodes[owner].rep.OpenWhole("default", id, causeCreate)
	h.writeSession(owner, id, epoch)
	if closeWhole != nil {
		closeWhole(st, true)
	}
}

func subsIn(ss *store.SessionState) int {
	if ss == nil {
		return -1
	}
	return len(ss.Subscriptions)
}
