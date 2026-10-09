package replication

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/lease"
	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/store"
)

func TestKeySuperseded(t *testing.T) {
	for _, c := range []struct {
		epoch, seen uint64
		want        bool
	}{
		{5, 7, true},
		{7, 7, false},
		{8, 7, false},
		{0, 9, false},
		{5, 0, false},
	} {
		if got := keySuperseded(c.epoch, c.seen); got != c.want {
			t.Errorf("keySuperseded(%d, %d) = %v, want %v", c.epoch, c.seen, got, c.want)
		}
	}
}

// TestBarrierRefusesAWriteIntoACopyALaterGrantReplaced: n1 wrote a session at
// epoch 5 and its target holds it. Once n1 has seen a grant at 7 for the
// client, a barrier naming the copy fails as superseded, though the target's
// table, which the test never moves, would still take n1's records; a
// retained key and an epoch-0 key beside it do not fail it.
func TestBarrierRefusesAWriteIntoACopyALaterGrantReplaced(t *testing.T) {
	tokens := map[string]*lease.Tokens{}
	h, sinks := withCountersAnd(t, []string{"n1", "n2"}, 1, 2, func(o *Options) {
		tokens[o.Node] = lease.NewTokens()
		o.Tokens = tokens[o.Node]
	})
	h.applyMap(shardmap.Compute(1, h.order, 2, 1))
	waitSettled(h, 1)
	id := clientIn(1, 0)
	n1 := h.nodes["n1"]
	h.writeSession("n1", id, 5)
	tokens["n1"].Note("default", id, 5)
	key := store.Scope{Namespace: "default", ClientID: id, Epoch: 5}
	others := []store.Scope{{Namespace: "default", Topic: "r/x"}, {Namespace: "default", ClientID: id + "-noepoch"}}
	barrier := func(keys ...store.Scope) error {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		return n1.rep.Barrier(ctx, keys)
	}
	if err := barrier(append([]store.Scope{key}, others...)...); err != nil {
		t.Fatalf("the copy at the token seen: barrier %v, want it to pass", err)
	}

	tokens["n1"].Note("default", id, 7)
	err := barrier(append([]store.Scope{key}, others...)...)
	if err == nil {
		t.Fatal("a later grant seen: the barrier over the old copy passed")
	}
	if got := barrierReason(err); got != "superseded" || !errors.Is(err, ErrReplicaUnavailable) {
		t.Fatalf("reason %q (%v), want superseded wrapping ErrReplicaUnavailable", got, err)
	}
	if n := sinks["n1"].get("hoardmq_replication_barrier_failures_total", map[string]string{"peer": "-", "reason": "superseded"}); n != 1 {
		t.Fatalf("superseded failures counted %v, want 1", n)
	}
	if err := barrier(others...); err != nil {
		t.Fatalf("the retained and epoch-0 keys alone: barrier %v, want it to pass", err)
	}
	if l, _ := h.nodes["n2"].leases.Lookup("default", id); l.Epoch > 5 {
		t.Fatalf("the test's premise: n2's table should not know the later grant, it has %+v", l)
	}
}
