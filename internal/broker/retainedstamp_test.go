package broker

import (
	"testing"

	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

// TestLeadersRetainedWriteIsStamped: on a cluster, a client's retained
// publish to the node leading its shard is held there at a stamped version,
// and the admin path's replication names the epoch too: the session manager's
// replication layer passes the replicator's stamp through.
func TestLeadersRetainedWriteIsStamped(t *testing.T) {
	nodes := startTrio(t, func(_ string, d *config.Document) {
		d.Cluster.Shards = 8
		d.Cluster.Readiness.StableMs = 0
	})
	m := waitMap(t, nodes, 3)
	const topic = "stamped/r"
	leader := m.Shards[nodes["n1"].srv.Replication().Map().ShardOf("default", topic)].Leader
	cl, ack := nodes[leader].connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("connect: %v", ack.ReasonCode)
	}
	cl.send(&packet.Publish{Topic: topic, QoS: 1, PacketID: 1, Retain: true, Payload: []byte("m")})
	if pa, ok := cl.read().(*packet.Puback); !ok || pa.ReasonCode.IsError() {
		t.Fatalf("the retained publish: %+v", pa)
	}
	e, ok := nodes[leader].srv.router.HeldRetained("default", topic)
	if !ok || e.RV.Zero() || e.RV.E < m.Version {
		t.Fatalf("%s holds %v (held %v), want the write stamped at map version %d or later", leader, e.RV, ok, m.Version)
	}
}
