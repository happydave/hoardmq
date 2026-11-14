package broker

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

// TestABrandNewSessionReachesItsTargetWhole: at level 2 a client's first
// persistent session, which no earlier session can have copies of, reaches
// its target as one snapshot, making the copy with its meta; nothing is
// refused for want of a copy.
func TestABrandNewSessionReachesItsTargetWhole(t *testing.T) {
	nodes := startTrio(t, func(_ string, d *config.Document) {
		d.Cluster.Shards = 8
		d.Cluster.Replicas = 2
		d.Observability.Address = "127.0.0.1:0"
		d.Modules["metrics.prometheus"] = json.RawMessage(`{}`)
	})
	m := waitMap(t, nodes, 3)
	waitUntil(t, "the cluster at level 2", 10*time.Second, func() bool {
		for _, n := range nodes {
			if n.srv.Leases().Table().Level() != 2 {
				return false
			}
		}
		return true
	})
	id := "brandnew"
	target := m.Targets(shardmap.ShardOf("default", id, 8), "n1")
	if len(target) != 1 {
		t.Fatalf("targets %v; want one", target)
	}
	exp := uint32(600)
	x, ack := nodes["n1"].connect("tcp", packet.V5, &packet.Connect{ClientID: id, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	if ack.ReasonCode != packet.Success || ack.SessionPresent {
		t.Fatalf("connect: %s, session present %v", ack.ReasonCode, ack.SessionPresent)
	}
	defer func() { _ = x.c.Close() }()
	waitUntil(t, target[0]+" to hold the copy", 5*time.Second, func() bool {
		return nodes[target[0]].srv.Replication().ReplicaState().Find("default", id) != nil
	})
	if v, _ := scrapeValue(mustScrape(t, nodes["n1"]), `hoardmq_replication_whole_writes_total{cause="create",form="snapshot"}`); v < 1 {
		t.Fatalf("n1's create whole writes %v; want at least 1", v)
	}
	if v, _ := scrapeValue(mustScrape(t, nodes["n1"]), `hoardmq_session_creations_total{form="snapshot"}`); v < 1 {
		t.Fatalf("n1's creations sent whole %v; want at least 1", v)
	}
	for name, n := range nodes {
		if v, _ := scrapeValue(mustScrape(t, n), `hoardmq_replication_unapplied_total{reason="no-session",type="session-upsert"}`); v != 0 {
			t.Fatalf("%s refused %v lone meta records; want 0", name, v)
		}
	}
}
