package broker

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

// TestNewOwnersConfirmationReachesTheHolderItDoesNotFeed: x's shard is held by
// its previous owner and a third node and not by the new owner, so as x's owner
// the new owner replicates to one of them. The client is placed so that the one
// it does not replicate to is the third node, not the previous owner, which is
// sent the confirmation as the giver anyway. After the takeover, that node has
// the new owner's confirmation at its grant: the lease service sends it there
// and hands it to the replicator there.
func TestNewOwnersConfirmationReachesTheHolderItDoesNotFeed(t *testing.T) {
	nodes := startTrio(t, func(_ string, d *config.Document) {
		d.Cluster.Shards = 8
		d.Cluster.Replicas = 2
	})
	m := waitMap(t, nodes, 3)
	id, prev, next, unfed := "", "", "", ""
	for i := 0; id == "" && i < 1000; i++ {
		c := fmt.Sprint("x", i)
		shard := shardmap.ShardOf("default", c, 8)
		holders := m.Holders(shard)
		for _, n := range []string{"n1", "n2", "n3"} {
			if slices.Contains(holders, n) {
				continue
			}
			for _, h := range holders {
				if slices.Contains(m.Targets(shard, n), h) {
					continue
				}
				for _, p := range holders {
					if p != h {
						id, prev, next, unfed = c, p, n, h
					}
				}
			}
		}
	}
	if id == "" {
		t.Fatal("precondition not reached: no client whose shard has a holder its new owner would not feed, other than its previous owner")
	}
	exp := uint32(600)
	x, ack := nodes[prev].connect("tcp", packet.V5, &packet.Connect{ClientID: id, CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("connect on %s: %s", prev, ack.ReasonCode)
	}
	x.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "t/x", QoS: 1}}})
	if _, ok := x.read().(*packet.Suback); !ok {
		t.Fatal("no SUBACK")
	}
	var a *packet.Connack
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, a = nodes[next].connect("tcp", packet.V5, &packet.Connect{ClientID: id, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false); a.ReasonCode == packet.Success {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if a.ReasonCode != packet.Success || !a.SessionPresent {
		t.Fatalf("takeover on %s: %s, session present %v", next, a.ReasonCode, a.SessionPresent)
	}
	l, ok := nodes[next].srv.Leases().Table().Lookup("default", id)
	if !ok || l.Owner != next {
		t.Fatalf("%s's lease for %s: %+v", next, id, l)
	}
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if owner, token, ok := nodes[unfed].srv.Replication().WrittenFor("default", id); ok && owner == next && token >= l.Epoch {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	owner, token, ok := nodes[unfed].srv.Replication().WrittenFor("default", id)
	t.Fatalf("%s, the holder %s does not feed, holds confirmation %q at %d (%v); want %s at %d or above", unfed, next, owner, token, ok, next, l.Epoch)
}
