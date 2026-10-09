package replication

import (
	"context"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

// TestMovedCopyDoesNotOutliveItsOwnersLaterWork: n2 keeps X's copy after the
// map moves X's target from n2 to n3, and n1's confirmation never comes: its
// whole write to n3 was lost while n3 was unreachable. n3 then recovers and
// holds X, and n1 acknowledges a second message there. Time passes with n1
// alive. n1 then dies, and under the map without it X's sources name n2
// first. A takeover must read the copy that holds the second message.
func TestMovedCopyDoesNotOutliveItsOwnersLaterWork(t *testing.T) {
	const every = 300 * time.Millisecond
	h, sinks, id := movedFixture(t, true, func(o *Options) { o.UnwrittenCheck = every })
	n1 := h.nodes["n1"]
	time.Sleep(500 * time.Millisecond) // n1's confirmation barrier fails at the ack timeout
	if !holdsCopy(h, "n2", id) || keptMovedCount(sinks, "n2") < 1 {
		t.Fatalf("precondition: n2 holds %v, kept{moved-target} %v; want the moved copy kept", holdsCopy(h, "n2", id), keptMovedCount(sinks, "n2"))
	}
	n1.tr.SetFault("n3", nil)
	later := store.Entry{Seq: 2, Message: &message.Message{Namespace: "default", Topic: "a/b", Payload: []byte("later")}, QoS: 1}
	feedSession(n1, id, queued(1, "queued"), later)
	if err := n1.own.Enqueue(store.Key{Namespace: "default", ClientID: id, Epoch: 1}, later); err != nil {
		t.Fatal(err)
	}
	if err := n1.own.Sync(); err != nil {
		t.Fatal(err)
	}
	key := []store.Scope{{Namespace: "default", ClientID: id}}
	h.waitFor("n1's barrier over the second message to pass at n3", 10*time.Second, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		return n1.rep.Barrier(ctx, key) == nil
	})
	if ss := h.nodes["n3"].rep.ReplicaState().Find("default", id); ss == nil || len(ss.Entries) != 2 {
		t.Fatalf("precondition: n3 does not hold both messages: %+v", ss)
	}
	time.Sleep(4 * every) // n1 alive by the log throughout
	n1.rep.Stop()
	n1.tr.Stop()
	notAlive(h, "n1")
	v3 := shardmap.Compute(3, []string{"n2", "n3"}, 2, 2)
	if got := v3.Targets(0, "n1"); len(got) == 0 || got[0] != "n2" {
		t.Fatalf("precondition: under v3 X's sources are %v, want n2 first", got)
	}
	for _, n := range []string{"n2", "n3"} {
		if _, err := h.nodes[n].table.Apply(0, shardmap.Encode(v3)); err != nil {
			t.Fatal(err)
		}
	}
	h.waitFor("n2 and n3 to reconcile v3", 5*time.Second, func() bool {
		return h.nodes["n2"].rep.Reconciled() == 3 && h.nodes["n3"].rep.Reconciled() == 3
	})
	ss, answered, _ := h.nodes["n3"].rep.Fetch(context.Background(), "default", id, "n1")
	if ss == nil {
		t.Fatalf("the takeover found no copy (answered %v)", answered)
	}
	if len(ss.Entries) != 2 {
		t.Fatalf("the takeover read %d entries, want both: n2's moved copy, kept since before n1's second message, answered first (n2 holds %v, dropped{not-a-target-held} %v)",
			len(ss.Entries), holdsCopy(h, "n2", id), dropped(sinks, "n2", "not-a-target-held"))
	}
	if got := dropped(sinks, "n2", "not-a-target-held"); got != 1 {
		t.Fatalf("dropped{not-a-target-held} = %v, want 1: n2's copy should go once n3 was seen holding X", got)
	}
}
