package replication

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

func unstamped(sinks map[string]*counters, node, typ string) float64 {
	return sinks[node].get("hoardmq_replication_unstamped_total", map[string]string{"type": typ})
}

// TestSessionRecordWithoutALeaseEpochIsReported: a session record written
// with no epoch, while this node's lease table holds one for the client, is
// counted and named — the anomaly behind a follower's stale-epoch refusal of
// a record its own owner wrote (WI 1570).
func TestSessionRecordWithoutALeaseEpochIsReported(t *testing.T) {
	h, sinks := withCounters(t, []string{"n1", "n2"}, 2, 2)
	h.applyMap(shardmap.Compute(1, h.order, 2, 2))
	waitSettled(h, 1)
	id := clientIn(2, 0)
	h.setLease("default", id, "n1", 1)
	n1 := h.nodes["n1"]
	if err := n1.own.SessionUpsert(store.SessionMeta{Namespace: "default", ClientID: id, Epoch: 0, Version: 5, ExpiryInterval: 300}); err != nil {
		t.Fatal(err)
	}
	if err := n1.own.Sync(); err != nil {
		t.Fatal(err)
	}
	h.waitFor("the unstamped record to be counted", 2*time.Second, func() bool {
		return unstamped(sinks, "n1", "session-upsert") == 1
	})
}

// TestStampedSessionRecordCostsNoLeaseLookup: the report above is the only
// reason the fan-out reads the lease table, and it reads it for an unstamped
// record alone — the lookup is on the path every write takes, and its lock is
// the one a takeover storm starves (WI 1292).
func TestStampedSessionRecordCostsNoLeaseLookup(t *testing.T) {
	h, _ := withCounters(t, []string{"n1", "n2"}, 2, 2)
	h.applyMap(shardmap.Compute(1, h.order, 2, 2))
	waitSettled(h, 1)
	id := clientIn(2, 0)
	h.setLease("default", id, "n1", 1)
	n1 := h.nodes["n1"]
	var mu sync.Mutex
	looked := 0
	n1.leases.onLookup = func(ns, client string) {
		if client == id {
			mu.Lock()
			looked++
			mu.Unlock()
		}
	}
	defer func() { n1.leases.onLookup = nil }()
	if err := n1.own.SessionUpsert(store.SessionMeta{Namespace: "default", ClientID: id, Epoch: 1, Version: 5, ExpiryInterval: 300}); err != nil {
		t.Fatal(err)
	}
	if err := n1.own.Sync(); err != nil {
		t.Fatal(err)
	}
	// Long enough for the tap, which runs inside the store's write, and for a
	// reconcile tick behind it.
	time.Sleep(300 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if looked != 0 {
		t.Fatalf("the lease table was read %d times for a record that carried an epoch", looked)
	}
}

// TestStaleJudgesAnUnstampedRecordByItsOwner: the predicate behind the
// refusal, driven directly. A record with no epoch cannot be older or newer
// than the table's generation; what decides it is whether the node that sent
// it is the one the table names as owner.
func TestStaleJudgesAnUnstampedRecordByItsOwner(t *testing.T) {
	h := newHarness(t, []string{"n1", "n2"}, 2, 2)
	f := h.nodes["n2"].rep.follower
	id := clientIn(2, 0)
	for _, c := range []struct {
		name       string
		lease      bool
		owner      string
		leaseEpoch uint64
		epoch      uint64
		peer       string
		want       bool
	}{
		{"unstamped from the owner", true, "n1", 1, 0, "n1", false},
		{"unstamped from another node", true, "n1", 1, 0, "n3", true},
		{"unstamped with no lease known", false, "", 0, 0, "n1", false},
		{"behind the lease epoch", true, "n1", 2, 1, "n1", true},
		{"at the lease epoch, from the owner", true, "n1", 1, 1, "n1", false},
		{"at the lease epoch, from another node", true, "n1", 1, 1, "n3", true},
	} {
		t.Run(c.name, func(t *testing.T) {
			h.nodes["n2"].leases.mu.Lock()
			delete(h.nodes["n2"].leases.leases, "default/"+id)
			h.nodes["n2"].leases.mu.Unlock()
			if c.lease {
				h.nodes["n2"].leases.set("default", id, c.owner, c.leaseEpoch)
			}
			got, why := f.stale(c.peer, "default", id, c.epoch)
			if got != c.want {
				t.Fatalf("stale(peer=%s, epoch=%d) = %v (%q), want %v", c.peer, c.epoch, got, why, c.want)
			}
		})
	}
}

// TestUnstampedRecordFromTheOwnerReachesItsFollower: the same rule through
// the stream. The owner writes a session with no epoch while the lease table
// holds one; the follower applies it and the barrier over it passes, where
// before the record was refused and the publish behind it failed.
func TestUnstampedRecordFromTheOwnerReachesItsFollower(t *testing.T) {
	h, sinks := withCounters(t, []string{"n1", "n2"}, 2, 2)
	h.applyMap(shardmap.Compute(1, h.order, 2, 2))
	waitSettled(h, 1)
	id := clientIn(2, 0)
	h.setLease("default", id, "n1", 1)
	n1 := h.nodes["n1"]
	if err := n1.own.SessionUpsert(store.SessionMeta{Namespace: "default", ClientID: id, Epoch: 0, Version: 5, ExpiryInterval: 300}); err != nil {
		t.Fatal(err)
	}
	if err := n1.own.Enqueue(store.Key{Namespace: "default", ClientID: id}, store.Entry{Seq: 1, Message: &message.Message{Namespace: "default", Topic: "a/b", Payload: []byte("queued")}, QoS: 1}); err != nil {
		t.Fatal(err)
	}
	if err := n1.own.Sync(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := n1.barrier(ctx); err != nil {
		t.Fatalf("the barrier over an unstamped record from the owner did not pass: %v", err)
	}
	ss := h.nodes["n2"].rep.ReplicaState().Find("default", id)
	if ss == nil || len(ss.Entries) != 1 {
		t.Fatalf("the follower holds %v for a session its owner wrote without an epoch", ss)
	}
	if got := unstamped(sinks, "n1", "session-upsert"); got != 1 {
		t.Fatalf("unstamped{session-upsert} = %v, want 1: the write was not reported", got)
	}
}
