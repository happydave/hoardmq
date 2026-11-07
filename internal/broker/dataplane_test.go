package broker

import (
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/message"
)

// TestModulesRetainedWriteMisroutedIsForwardedAgain: in a three-node broker, a
// module's retained write that n3 forwards to the leader of its map, refused
// there as misrouted because that node applied a newer map, is held at n3 and
// forwarded once more when n3's map reaches the newer one, so the new leader
// holds it.
func TestModulesRetainedWriteMisroutedIsForwardedAgain(t *testing.T) {
	nodes := startTrio(t, func(_ string, d *config.Document) {
		d.Cluster.Shards = 8
		d.Cluster.Readiness.StableMs = 0
	})
	cur := waitMap(t, nodes, 3)
	table := nodes["n1"].srv.Replication().Map()
	topic := ""
	for i := 0; topic == "" && i < 1000; i++ {
		tp := "misrouted/" + string(rune('a'+i%26)) + string(rune('a'+i/26))
		if cur.Shards[table.ShardOf("default", tp)].Leader == "n1" {
			topic = tp
		}
	}
	if topic == "" {
		t.Fatal("precondition: no topic of a shard n1 leads")
	}
	next := cur
	next.Version = cur.Version + 1
	next.Shards = append([]shardmap.Shard(nil), cur.Shards...)
	next.Shards[table.ShardOf("default", topic)] = shardmap.Shard{Leader: "n2", Followers: []string{"n1"}}
	for _, n := range []string{"n1", "n2"} {
		if _, err := nodes[n].srv.Replication().Map().Apply(0, shardmap.Encode(next)); err != nil {
			t.Fatal(err)
		}
	}
	nodes["n3"].srv.sessions.Inject(&message.Message{Topic: topic, Payload: []byte("module"), Retain: true})
	time.Sleep(300 * time.Millisecond)
	if _, err := nodes["n3"].srv.Replication().Map().Apply(0, shardmap.Encode(next)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if e, ok := nodes["n2"].srv.router.HeldRetained("default", topic); ok && string(e.Msg.Payload) == "module" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("n2, the newer map's leader, never held the module's write refused as misrouted at n1")
}
