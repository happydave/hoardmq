package replication

import (
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/lease"
	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/store"
)

func staleTokens(sinks map[string]*counters, node string) float64 {
	return sinks[node].get("hoardmq_lease_stale_token_total", map[string]string{"where": "replication"})
}

// TestRecordBelowTheHighestTokenSeenIsRefused is the window a lagging lease
// table leaves open. n2's table still says n1 owns the client at epoch 5; n2
// has been told of a grant at 9 — as it is when its own acquire succeeds, or
// when the new owner's first frame arrives — and has not applied it. The old
// owner's next record, at 5, looks current to the table and is refused
// anyway; the new owner's, at 9, is applied though the table has not heard
// of it.
func TestRecordBelowTheHighestTokenSeenIsRefused(t *testing.T) {
	tokens := map[string]*lease.Tokens{}
	h, sinks := withCountersAnd(t, []string{"n1", "n2", "n3"}, 1, 3, func(o *Options) {
		tokens[o.Node] = lease.NewTokens()
		o.Tokens = tokens[o.Node]
	})
	h.applyMap(shardmap.Compute(1, h.order, 3, 1))
	waitSettled(h, 1)
	id := clientIn(1, 0)
	n1, n2, n3 := h.nodes["n1"], h.nodes["n2"], h.nodes["n3"]
	h.writeSession("n1", id, 5)
	h.waitFor("n2 to hold n1's copy", 5*time.Second, func() bool { return n2.rep.ReplicaState().Find("default", id) != nil })
	if got := tokens["n2"].Seen("default", id); got != 5 {
		t.Fatalf("n2 accepted records at epoch 5 and noted %d", got)
	}

	tokens["n2"].Note("default", id, 9)
	if err := n1.own.Subscribe(store.Key{Namespace: "default", ClientID: id, Epoch: 5}, store.SubscriptionRecord{Filter: "stale/write", QoS: 1}); err != nil {
		t.Fatal(err)
	}
	if err := n1.own.Sync(); err != nil {
		t.Fatal(err)
	}
	// At least one: the old owner is refused again for anything more it sends.
	h.waitFor("n2 to refuse the old owner's record", 5*time.Second, func() bool { return staleTokens(sinks, "n2") >= 1 })
	for _, sub := range n2.rep.ReplicaState().Find("default", id).Subscriptions {
		if sub.Filter == "stale/write" {
			t.Fatal("n2 applied a record written under a grant it knows has been superseded")
		}
	}
	if l, _ := n2.leases.Lookup("default", id); l.Owner != "n1" || l.Epoch != 5 {
		t.Fatalf("the test's premise: n2's table should still name n1 at 5, not %+v", l)
	}

	// The new owner writes the session whole at 9, as an import does.
	if err := n3.own.SessionUpsert(store.SessionMeta{Namespace: "default", ClientID: id, Epoch: 9, Version: 5, ExpiryInterval: 300}); err != nil {
		t.Fatal(err)
	}
	if err := n3.own.Sync(); err != nil {
		t.Fatal(err)
	}
	h.waitFor("n2 to apply the new owner's record", 5*time.Second, func() bool {
		ss := n2.rep.ReplicaState().Find("default", id)
		return ss != nil && ss.Epoch == 9
	})
	// The old owner may be refused again meanwhile — a snapshot it sends is as
	// stale as its records — so the count says nothing about n3. What n3 was
	// refused does.
	if keys := n3.rep.RefusedKeys()["n2"]; len(keys) != 0 {
		t.Fatalf("n2 refused the new owner's records: %v", keys)
	}
}
