package replication

import (
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
)

// TestHandOnAfterTheNewLeadersWrite: n1 leads shard 0 and dies inside a move
// whose release to n2 was lost, so n2 keeps its copy of tp ("old") and hands
// it on to n3, the shard's next leader. BeforeRetainedHandOver holds n2's
// hand-on until n3 has acknowledged a client's write to tp. A newer message
// must stay; a clear must stay cleared, also when n3's retained store is
// rebuilt from its journal in between, as a restart rebuilds it.
func TestHandOnAfterTheNewLeadersWrite(t *testing.T) {
	for _, c := range []struct {
		name, write string
		restart     bool
	}{
		{"a newer message", "newer", false},
		{"a clear", "", false},
		{"a clear, then the leader restarts", "", true},
	} {
		t.Run(c.name, func(t *testing.T) {
			reached := make(chan struct{}, 1)
			release := make(chan struct{})
			var once sync.Once
			open := func() { once.Do(func() { close(release) }) }
			h, sinks := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, func(o *Options) {
				quickAck(o)
				if o.Node == "n2" {
					o.BeforeRetainedHandOver = func(leader string) {
						if leader != "n3" {
							return
						}
						select {
						case reached <- struct{}{}:
						default:
						}
						<-release
					}
				}
			})
			t.Cleanup(open)
			allAlive(h)
			h.applyMap(moveMap(h, 1, sh("n1", "n2"), sh("n2", "n3")))
			waitSettled(h, 1)
			tp := topicIn(2, 0)
			if err := writeRetained(h, "n1", tp, "old"); err != nil {
				t.Fatalf("precondition: the write under v1: %v", err)
			}
			h.waitFor("n2 to hold the copy", 5*time.Second, func() bool { return replicaHolds(h, "n2", tp) == "old" })
			dropAll(h, "n1", "n3", 13)
			h.applyMap(moveMap(h, 2, sh("n1", "n3"), sh("n2", "n3")))
			waitSettled(h, 2)
			if replicaHolds(h, "n2", tp) != "old" || h.nodes["n2"].rep.KeptRetained() != 1 {
				t.Fatalf("precondition: n2 holds %q and keeps %d shards, want its copy kept", replicaHolds(h, "n2", tp), h.nodes["n2"].rep.KeptRetained())
			}
			for _, peer := range []string{"n2", "n3"} {
				dropAll(h, "n1", peer, 14)
			}
			declare(h, "n1")
			h.applyMap(shardmap.Map{Version: 3, Basis: []string{"n2", "n3"}, Replicas: 2, Shards: []shardmap.Shard{sh("n3", "n2"), sh("n2", "n3")}})
			select {
			case <-reached:
			case <-time.After(5 * time.Second):
				t.Fatal("precondition: n2 never began its hand-on to n3")
			}
			h.waitFor("n3 to reconcile v3 with no move pending", 5*time.Second, func() bool {
				return h.nodes["n3"].rep.Reconciled() == 3 && h.nodes["n3"].rep.RetainedMovesPending() == 0
			})
			err := writeRetained(h, "n3", tp, c.write)
			t.Logf("n3's write of %q before the hand-on: err=%v reason=%q; n3 holds %q", c.write, err, barrierReason(err), ownHolds(h, "n3", tp))
			if err != nil {
				t.Fatalf("n3 refused the client's write while the hand-on was owed: %v", err)
			}
			if c.restart {
				if err := h.nodes["n3"].sessions.reload(); err != nil {
					t.Fatal(err)
				}
				if !ownTombstone(h, "n3", tp) {
					t.Fatalf("n3 holds no tombstone for %s after its retained store was rebuilt from its journal", tp)
				}
			}
			open()
			h.waitFor("n2's hand-on to be answered", 5*time.Second, func() bool { return h.nodes["n2"].rep.KeptRetained() == 0 })
			held := sinks["n3"].get("hoardmq_retained_handovers_total", map[string]string{"outcome": "held"})
			got := ownHolds(h, "n3", tp)
			t.Logf("after the hand-on: n3 holds %q, n3 handovers{held} %v, n2 holds replica %q", got, held, replicaHolds(h, "n2", tp))
			if got != c.write {
				t.Fatalf("n3 holds %q after n2's hand-on; the client's acknowledged write was %q", got, c.write)
			}
			if older := sinks["n3"].get("hoardmq_retained_merges_total", map[string]string{"path": "handover", "outcome": "older"}); older < 1 {
				t.Fatalf("n2's older hand-on counted %v older at n3, want at least 1", older)
			}
		})
	}
}
