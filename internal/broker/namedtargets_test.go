package broker

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

// TestPublishWaitsOnlyForItsRecordsTargets (WI 1631): with n3 down and still
// named by the shard map, a QoS 1 publish on n1 whose records have no replica
// target on n3 is acknowledged, and one whose publisher's or subscriber's
// records do is refused.
func TestPublishWaitsOnlyForItsRecordsTargets(t *testing.T) {
	const shards = 8
	nodes := startTrio(t, func(_ string, d *config.Document) {
		d.Cluster.Shards = shards
		d.Cluster.Replicas = 2
		// The map drops a stopped node once its lease record lapses; this case
		// needs n3 stopped and still named for its whole length.
		d.Cluster.Leases.TTLMs = 15000
	})
	m := waitMap(t, nodes, 3)
	onN3 := func(id string) bool {
		return slices.Contains(m.Targets(shardmap.ShardOf("default", id, shards), "n1"), "n3")
	}
	pick := func(prefix string, want bool) string {
		for i := 0; ; i++ {
			if id := fmt.Sprintf("%s%d", prefix, i); onN3(id) == want {
				return id
			}
		}
	}
	exp := uint32(600)
	subscribeOn := func(id, topic string) *client {
		c, ack := nodes["n1"].connect("tcp", packet.V5, &packet.Connect{ClientID: id, CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
		if ack.ReasonCode != packet.Success {
			t.Fatalf("connect %s: %s", id, ack.ReasonCode)
		}
		c.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: topic, QoS: 1}}})
		if _, ok := c.read().(*packet.Suback); !ok {
			t.Fatalf("no SUBACK for %s", id)
		}
		return c
	}
	publisher := func(id string) *client {
		c, ack := nodes["n1"].connect("tcp", packet.V5, &packet.Connect{ClientID: id, CleanStart: true}, false)
		if ack.ReasonCode != packet.Success {
			t.Fatalf("connect %s: %s", id, ack.ReasonCode)
		}
		return c
	}
	var id uint16
	publish := func(c *client, topic string) packet.ReasonCode {
		t.Helper()
		id++
		c.send(&packet.Publish{Topic: topic, QoS: 1, PacketID: id, Payload: []byte("m")})
		a, ok := c.read().(*packet.Puback)
		if !ok || a.PacketID != id {
			t.Fatalf("PUBACK %d: %+v", id, a)
		}
		return a.ReasonCode
	}

	subscribeOn(pick("sub", false), "clear")
	subscribeOn(pick("sub", true), "via")
	pubClear, pubVia := publisher(pick("pub", false)), publisher(pick("pub", true))
	publishUntilAccepted(t, pubClear, "clear", "m")
	publishUntilAccepted(t, pubVia, "clear", "m")
	publishUntilAccepted(t, pubClear, "via", "m")

	nodes["n3"].stop()
	deadline := time.Now().Add(10 * time.Second)
	for down := false; !down; {
		for _, p := range nodes["n1"].srv.Cluster().Peers() {
			if p.Name == "n3" && p.State != transport.Up {
				down = true
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("precondition: n1 never saw n3 down")
		}
		time.Sleep(20 * time.Millisecond)
	}

	for _, c := range []struct {
		name  string
		pub   *client
		topic string
		want  packet.ReasonCode
	}{
		{"no record targets n3", pubClear, "clear", packet.Success},
		{"the publisher's record targets n3", pubVia, "clear", packet.ImplementationSpecificError},
		{"the subscriber's record targets n3", pubClear, "via", packet.ImplementationSpecificError},
	} {
		if got := publish(c.pub, c.topic); got != c.want {
			t.Errorf("%s: PUBACK %s, want %s", c.name, got, c.want)
		}
		if cur := nodes["n1"].srv.Replication().Map().Current(); cur.Version != m.Version || !slices.Contains(cur.Basis, "n3") {
			t.Fatalf("precondition: the map moved to version %d, basis %v; the case needs n3 down and still named", cur.Version, cur.Basis)
		}
	}
}
