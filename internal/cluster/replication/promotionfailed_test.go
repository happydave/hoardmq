package replication

import (
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
)

// failedPromotion runs the promotion with n2's installs not durable: under
// v1 n1 leads shard 0 and n2 follows, holding a replica copy of an
// acknowledged retained message; n1 dies and v2 makes n2 the shard's leader.
// It returns the topic.
func failedPromotion(t *testing.T, h *harness) string {
	t.Helper()
	allAlive(h)
	h.applyMap(moveMap(h, 1, sh("n1", "n2"), sh("n2", "n3")))
	waitSettled(h, 1)
	tp := topicIn(2, 0)
	if err := writeRetained(h, "n1", tp, "acked"); err != nil {
		t.Fatalf("precondition: the write under v1: %v", err)
	}
	h.waitFor("n2 to hold the replica copy", 5*time.Second, func() bool { return replicaHolds(h, "n2", tp) == "acked" })
	h.nodes["n2"].sessions.unjournaled.Store(true)
	for _, peer := range []string{"n2", "n3"} {
		dropAll(h, "n1", peer, 21)
	}
	declare(h, "n1")
	h.applyMap(shardmap.Map{Version: 2, Basis: []string{"n2", "n3"}, Replicas: 2, Shards: []shardmap.Shard{sh("n2", "n3"), sh("n2", "n3")}})
	h.waitFor("n2 and n3 to reconcile v2", 5*time.Second, func() bool {
		return h.nodes["n2"].rep.Reconciled() == 2 && h.nodes["n3"].rep.Reconciled() == 2
	})
	return tp
}

// TestPromotionKeepsAReplicaCopyWhoseInstallFailed: n2's install of its
// promoted copy is not durable. The replica copy is kept, counted, and the
// shard refuses client writes; once installs are durable again the retry
// installs it and drops the replica copy.
func TestPromotionKeepsAReplicaCopyWhoseInstallFailed(t *testing.T) {
	h, sinks := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, quickAck)
	tp := failedPromotion(t, h)
	if replicaHolds(h, "n2", tp) != "acked" {
		t.Fatal("n2 dropped its replica copy although installing it in its own journal failed")
	}
	if got := sinks["n2"].get("hoardmq_replicas_kept_total", map[string]string{"reason": "install-failed"}); got != 1 {
		t.Fatalf("n2 kept{install-failed} %v, want 1", got)
	}
	// The move itself completes (n1 is declared, n3 holds the shard), so only
	// the kept copy can make the shard refuse.
	h.waitFor("n2's move to complete", 5*time.Second, func() bool { return h.nodes["n2"].rep.RetainedMovesPending() == 0 })
	if err := writeRetained(h, "n2", tp, "during"); barrierReason(err) != "moving" {
		t.Fatalf("a client write while the copy is kept: %v (reason %q), want moving", err, barrierReason(err))
	}
	h.nodes["n2"].sessions.unjournaled.Store(false)
	// The refused write was applied before its barrier refused it, as a
	// write on a moving shard is, and is stamped above the replica copy, so
	// the retry's merge
	// keeps it: the retry replaces nothing, and drops the replica copy once
	// what n2 holds is durable.
	h.waitFor("the retry to drop the replica copy over a durable entry", 5*time.Second, func() bool {
		return replicaHolds(h, "n2", tp) == "" && ownHolds(h, "n2", tp) == "during"
	})
	if err := writeRetained(h, "n2", tp, "after"); err != nil {
		t.Fatalf("a client write once the install is durable: %v", err)
	}
}

// TestPromotedCopySurvivesAPowerCutBeforeTheRepair: end to end. After the
// failed promotion n2 loses its power before its journal is repaired: its
// in-memory store loses the copy, which the journal never took
// (session.TestRetainedInstallWhoseSyncFailedAndACrash shows that half), and
// it reconciles from no map, as a restart does. Its kept replica copy is
// promoted again. The control drops the copy at the failed promotion, as
// before, and n2 holds nothing.
func TestPromotedCopySurvivesAPowerCutBeforeTheRepair(t *testing.T) {
	for _, c := range []struct {
		name    string
		control bool
	}{{"kept", false}, {"control: dropped", true}} {
		t.Run(c.name, func(t *testing.T) {
			h, _ := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, func(o *Options) {
				quickAck(o)
				o.DropFailedPromotion = c.control
			})
			tp := failedPromotion(t, h)
			n2 := h.nodes["n2"]
			n2.sessions.mu.Lock()
			delete(n2.sessions.retained, "default/"+tp)
			n2.sessions.mu.Unlock()
			n2.sessions.unjournaled.Store(false)
			n2.rep.rebalance(shardmap.Map{}, n2.rep.Map().Current())
			got := ownHolds(h, "n2", tp)
			t.Logf("after the restart: n2 own %q, n2 replica %q", got, replicaHolds(h, "n2", tp))
			if c.control {
				if got != "" {
					t.Fatalf("control: n2 holds %q, want nothing", got)
				}
				return
			}
			if got != "acked" {
				t.Fatalf("n2, the shard's leader, holds %q after the power cut and restart, want acked", got)
			}
		})
	}
}

// TestPromotedCopyGoes: the decision at a retry, each refusal included.
func TestPromotedCopyGoes(t *testing.T) {
	for _, c := range []struct {
		leads, installed bool
		drop, forget     bool
	}{
		{leads: true, installed: true, drop: true, forget: true},
		{leads: true, installed: false, drop: false, forget: false},
		{leads: false, installed: false, drop: false, forget: true},
		{leads: false, installed: true, drop: false, forget: true},
	} {
		drop, forget := promotedCopyGoes(c.leads, c.installed)
		if drop != c.drop || forget != c.forget {
			t.Errorf("promotedCopyGoes(leads=%v, installed=%v) = drop %v forget %v, want %v %v", c.leads, c.installed, drop, forget, c.drop, c.forget)
		}
	}
	for _, c := range []struct{ leads, held, want bool }{
		{false, false, true}, {true, false, false}, {false, true, false}, {true, true, false},
	} {
		if got := retryOwnDelete(c.leads, c.held); got != c.want {
			t.Errorf("retryOwnDelete(leads=%v, held=%v) = %v, want %v", c.leads, c.held, got, c.want)
		}
	}
}

// TestFailedOwnDeleteIsMadeAgain: n1 leads shard 0 and v2 moves the shard
// to n2 and n3, so n1, which no longer holds it, hands its own copy over and
// deletes it once released. The delete is not durable: it is counted, noted
// and made again, and once durable the note goes.
func TestFailedOwnDeleteIsMadeAgain(t *testing.T) {
	h, sinks := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, quickAck)
	allAlive(h)
	h.applyMap(moveMap(h, 1, sh("n1", "n2"), sh("n2", "n3")))
	waitSettled(h, 1)
	tp := topicIn(2, 0)
	if err := writeRetained(h, "n1", tp, "own"); err != nil {
		t.Fatalf("precondition: the write under v1: %v", err)
	}
	n1 := h.nodes["n1"]
	n1.sessions.unjournaled.Store(true)
	h.applyMap(moveMap(h, 2, sh("n2", "n3"), sh("n2", "n3")))
	h.waitFor("n1's delete of its own copy to fail", 5*time.Second, func() bool {
		return sinks["n1"].get("hoardmq_retained_drop_failures_total", nil) >= 1
	})
	noted := func() int {
		n1.rep.rfailed.mu.Lock()
		defer n1.rep.rfailed.mu.Unlock()
		return len(n1.rep.rfailed.deletes)
	}
	if noted() != 1 {
		t.Fatalf("%d failed deletes noted, want 1", noted())
	}
	n1.sessions.unjournaled.Store(false)
	h.waitFor("the delete to be made again and the note to go", 5*time.Second, func() bool { return noted() == 0 })
	if got := ownHolds(h, "n1", tp); got != "" {
		t.Fatalf("n1 holds %q of a shard it no longer holds", got)
	}
}
