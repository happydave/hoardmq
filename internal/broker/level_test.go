package broker

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

// TestNodeBelowTheClusterLevelDoesNotServe: a cluster that has raised its
// feature level, and one member put back on a build that does not understand
// it. That member stays in the group and reports unready with the reason, and
// a CONNECT to it is redirected to the leader; the others serve.
func TestNodeBelowTheClusterLevelDoesNotServe(t *testing.T) {
	nodes := startTrio(t, func(_ string, d *config.Document) {
		d.Cluster.Readiness.StableMs = 0
		d.Observability.Address = "127.0.0.1:0"
		d.Modules["metrics.prometheus"] = json.RawMessage(`{}`)
	})
	waitMap(t, nodes, 3)
	for _, f := range nodes {
		f.srv.Leases().SetBuildLevel(2)
	}
	waitLevel := func(want uint64) {
		t.Helper()
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			ok := true
			for _, f := range nodes {
				if f.srv.Leases().Table().Level() != want {
					ok = false
				}
			}
			if ok {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("the cluster did not reach level %d", want)
	}
	waitLevel(2)

	wantMetrics := func(node string, lines ...string) {
		t.Helper()
		_, body := scrape(t, nodes[node], "/metrics")
		for _, l := range lines {
			if !strings.Contains(body, l+"\n") {
				t.Errorf("%s /metrics lacks %q", node, l)
			}
		}
	}
	wantMetrics("n1", "hoardmq_cluster_feature_level 2", "hoardmq_cluster_feature_level_build 2", "hoardmq_cluster_feature_level_min_advertised 2")

	// The member rolled back is one that does not lead, so its refusal can
	// name the leader.
	leader, _ := nodes["n1"].srv.Consensus().Leader()
	old := "n1"
	if leader == old {
		old = "n2"
	}
	if ready, reason := nodes[old].srv.Readiness(); !ready {
		t.Fatalf("%s is unready (%s) before its build was put back", old, reason)
	}
	nodes[old].srv.Leases().SetBuildLevel(1)

	if ready, reason := nodes[old].srv.Readiness(); ready || reason != "level" {
		t.Fatalf("%s below the cluster's level: ready=%v reason=%q, want unready for \"level\"", old, ready, reason)
	}
	_, ack := nodes[old].connect("tcp", packet.V5, &packet.Connect{ClientID: "c-old", CleanStart: true}, false)
	ref := ""
	if ack.Properties.ServerReference != nil {
		ref = *ack.Properties.ServerReference
	}
	if ack.ReasonCode != packet.UseAnotherServer || ref != "client."+leader+":1883" {
		t.Fatalf("CONNECT to %s: %s, reference %q; want Use Another Server naming %s", old, ack.ReasonCode, ref, leader)
	}
	if !nodes[old].srv.Consensus().Ready() {
		t.Fatalf("%s left the consensus group; it must stay a member to receive the log", old)
	}
	_, ack = nodes[leader].connect("tcp", packet.V5, &packet.Connect{ClientID: "c-new", CleanStart: true}, false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("CONNECT to %s, which understands the level: %s", leader, ack.ReasonCode)
	}

	wantMetrics(old, "hoardmq_cluster_feature_level 2", "hoardmq_cluster_feature_level_build 1",
		`hoardmq_cluster_feature_level_refusals_total{where="connect"} 1`)

	// Upgraded again, it serves.
	nodes[old].srv.Leases().SetBuildLevel(2)
	if ready, reason := nodes[old].srv.Readiness(); !ready {
		t.Fatalf("%s upgraded again is unready (%s)", old, reason)
	}
	_, ack = nodes[old].connect("tcp", packet.V5, &packet.Connect{ClientID: "c-old", CleanStart: true}, false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("CONNECT to %s upgraded again: %s", old, ack.ReasonCode)
	}
}
