package broker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
	"github.com/happydave/hoardmq/internal/config"
)

// TestBehindVoterIsNotReadyOnItsProbes: a broker whose consensus log ends
// below the leader's first index, and which cannot receive the snapshot it
// needs (its data connections held down), has a leader and hears it on its
// control connections. /readyz and /servingz must answer 503 on it, and 200
// again once the snapshot reaches it.
func TestBehindVoterIsNotReadyOnItsProbes(t *testing.T) {
	nodes := startTrio(t, func(name string, d *config.Document) {
		d.Cluster.Raft.SnapshotEntries = 20
		d.Cluster.Raft.SnapshotTrailing = 10
		metricsOn(d)
	})
	leaderName := func(skip string) string {
		for n, f := range nodes {
			if n != skip && f.srv.Consensus().Status().Role == "leader" {
				return n
			}
		}
		return ""
	}
	propose := func(skip, key string) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for {
			l := leaderName(skip)
			if l != "" {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				_, err := nodes[l].srv.Consensus().Propose(ctx, consensus.KindConfig, consensus.ConfigCommand(key, "v")[1:])
				cancel()
				if err == nil {
					return
				}
			}
			if time.Now().After(deadline) {
				t.Fatalf("no proposal of %s committed", key)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	for i := 0; i < 30; i++ {
		propose("", fmt.Sprint("before", i))
	}
	lead := leaderName("")
	victim := ""
	for n := range nodes {
		if n != lead {
			victim = n
			break
		}
	}
	for _, path := range []string{"/readyz", "/servingz"} {
		if code, body := scrape(t, nodes[victim], path); code != 200 {
			t.Fatalf("precondition: %s on %s answered %d %q before the cut", path, victim, code, body)
		}
	}

	cut := func(on bool) {
		for n, f := range nodes {
			if n != victim {
				f.srv.Cluster().Isolate(victim, on)
				nodes[victim].srv.Cluster().Isolate(n, on)
			}
		}
	}
	hold := func(on bool) {
		for n, f := range nodes {
			if n != victim {
				f.srv.Cluster().HoldData(victim, on)
				nodes[victim].srv.Cluster().HoldData(n, on)
			}
		}
	}
	cut(true)
	hold(true)
	t.Cleanup(func() { hold(false) })
	for i := 0; i < 45; i++ {
		propose(victim, fmt.Sprint("behind", i))
	}
	cut(false)
	lead = leaderName(victim)
	vc, lc := nodes[victim].srv.Consensus(), nodes[lead].srv.Consensus()
	waitUntil(t, victim+" to know the leader again", 10*time.Second, func() bool {
		l, ok := vc.Leader()
		return ok && l == lead
	})
	if st, ls := vc.Status(), lc.Status(); st.Applied >= ls.SnapshotIndex {
		t.Fatalf("precondition not reached: %s applied %d, at or past the leader's snapshot %d", victim, st.Applied, ls.SnapshotIndex)
	}

	// The broker's stall timeout at the harness's 10 ms tick is its 10 s
	// floor.
	for _, path := range []string{"/readyz", "/servingz"} {
		if code, body, ok := waitProbe(t, nodes[victim], path, 503, 15*time.Second); !ok {
			ready, reason := nodes[victim].srv.Readiness()
			st, ls := vc.Status(), lc.Status()
			t.Errorf("%s on %s answered %d %q for 15 s while it cannot apply past the leader's snapshot (readiness %v %q; applied %d, the leader's snapshot %d)",
				path, victim, code, body, ready, reason, st.Applied, ls.SnapshotIndex)
		}
	}
	if code, body := scrape(t, nodes[victim], "/servingz"); code != 503 || body != "not ready: behind\n" {
		t.Errorf("/servingz on %s answered %d %q, want 503 naming the reason behind", victim, code, body)
	}
	if st := vc.Status(); st.Applied >= lc.Status().SnapshotIndex {
		t.Fatalf("precondition lost: %s applied %d, past the leader's snapshot", victim, st.Applied)
	}

	hold(false)
	for _, path := range []string{"/readyz", "/servingz"} {
		if code, body, ok := waitProbe(t, nodes[victim], path, 200, 15*time.Second); !ok {
			t.Errorf("%s on %s answered %d %q once its snapshot could reach it", path, victim, code, body)
		}
	}
}
