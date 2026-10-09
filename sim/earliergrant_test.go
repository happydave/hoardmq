package sim

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/cluster/transport"
)

// earlierGrantRoles names a placement for TestEarlierGrantCopyOutlivesAnUndeclaredOwner:
// the subscriber's owner p replicates it to h; once p is gone the leader l
// adopts it, holds none of its shard and replicates to t, while h still holds
// the shard without being fed.
type earlierGrantRoles struct {
	id      string
	p, h, t string
}

// findEarlierGrantRoles searches the non-leaders and client identifiers for
// that placement, under the maps computed with every node and without p.
func findEarlierGrantRoles(names []string, leader string, replicas, shards int) (earlierGrantRoles, bool) {
	all := shardmap.Compute(0, names, replicas, shards)
	for _, p := range names {
		if p == leader {
			continue
		}
		without := shardmap.Compute(0, slices.DeleteFunc(slices.Clone(names), func(n string) bool { return n == p }), replicas, shards)
		for i := 0; i < 512; i++ {
			id := fmt.Sprintf("sub-earlier-%d", i)
			s := shardmap.ShardOf("default", id, shards)
			before := all.Targets(s, p)
			if len(before) != 1 || before[0] == leader {
				continue
			}
			h := before[0]
			// The shard's holders must move when p goes, or h's copy is not
			// looked at again.
			after := without.Targets(s, leader)
			if slices.Equal(all.Holders(s), without.Holders(s)) || without.Holds(s, leader) || !without.Holds(s, h) || len(after) != 1 || after[0] == h {
				continue
			}
			return earlierGrantRoles{id: id, p: p, h: h, t: after[0]}, true
		}
	}
	return earlierGrantRoles{}, false
}

// TestEarlierGrantCopyOutlivesAnUndeclaredOwner, on five brokers: the
// subscriber's owner p dies with three acknowledged messages queued,
// replicated to h. The leader l adopts the session; under the map without p it
// replicates to t, and h, still holding the shard, keeps its copy from p's
// grant for l. Every replication frame from l to t is lost: l's fetch
// asks t first and never completes, so l writes nothing anywhere and confirms
// nothing. l stays alive past twice the ttl, then dies, and is declared only
// after that. The survivors must still hold the three messages.
func TestEarlierGrantCopyOutlivesAnUndeclaredOwner(t *testing.T) {
	// The adoption worker is held back so the adopter writes the session only
	// under the map without p: written before it, to a node that is then no
	// longer a target, the copy would be one the map moved, kept on rules of
	// its own.
	c := New(t, Options{Nodes: 5, AdoptionDelay: 3 * time.Second, Log: halfLog(t, "earliergrant")})
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	leaderName, ok := agreedLeader(leaderViews(c, nil))
	if !ok {
		t.Fatal("no agreed leader")
	}
	names := make([]string, 0, len(c.Nodes))
	for _, n := range c.Nodes {
		names = append(names, n.Name)
	}
	rep := c.Node(leaderName).Server().Replication()
	roles, found := findEarlierGrantRoles(names, leaderName, rep.Map().Current().Replicas, rep.Map().Count())
	if !found {
		t.Fatalf("no placement under leader %s: the owner's one target outside the leader, then the leader outside the shard with a target other than that holder", leaderName)
	}
	l, p, h, tn := c.Node(leaderName), c.Node(roles.p), c.Node(roles.h), c.Node(roles.t)
	t.Logf("client %s: owner %s fed %s; adopter %s will feed %s", roles.id, p.Name, h.Name, l.Name, tn.Name)

	ledger := &Ledger{}
	sub := NewClient(roles.id, []string{p.TCP}, 600, 1)
	if err := sub.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := sub.Subscribe(ctx, "earlier/t", 1); err != nil {
		t.Fatal(err)
	}
	ledger.Subscriptions = []Subscription{{Client: sub.ID, Filter: "earlier/t", QoS: 1, Since: time.Now()}}
	sub.Close()
	waitOffline(t, p, roles.id)
	pub := NewClient("pub-earlier", []string{p.TCP}, 0, 2)
	if err := pub.Run(ctx); err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 3; i++ {
		pctx, pcancel := context.WithTimeout(ctx, 5*time.Second)
		code, err := pub.Publish(pctx, "earlier/t", 1, i)
		pcancel()
		if err != nil {
			ledger.addErr(PublishFailure{Client: pub.ID, Node: nodeNameAt(c, pub), Seq: i, Code: code, Err: err.Error()})
			continue
		}
		ledger.addPub(Publication{Publisher: pub.ID, Topic: "earlier/t", QoS: 1, Seq: i, Code: code, AckedAt: time.Now()})
	}
	pub.Close()
	waitReplicaCopy(t, h, roles.id, 3, 15*time.Second)

	lost := &transport.Fault{Drop: 1, Classes: []transport.Class{transport.ReplicationAppend}, Rand: rand.New(rand.NewSource(7))} //nolint:gosec // seeded simulation
	c.Fault(l, tn, lost)
	c.Crash(p)
	waitBasisWithout(t, c, p.Name, 30*time.Second)
	if ss := replicaCopy(h, roles.id); ss == nil {
		t.Fatalf("%s dropped its copy at the map change, before its bound", h.Name)
	}
	// The adopter stays alive, renewing, for more than twice the ttl.
	time.Sleep(2*c.o.TTL + 2*time.Second)
	t.Logf("before %s dies: %s holds a copy %v, %s holds a copy %v", l.Name, h.Name, replicaCopy(h, roles.id) != nil, tn.Name, replicaCopy(tn, roles.id) != nil)
	c.Crash(l)
	c.Fault(l, tn, nil)
	waitBasisWithout(t, c, l.Name, 30*time.Second)

	var survivors []string
	for _, n := range c.Running() {
		survivors = append(survivors, n.TCP)
	}
	sctx, scancel := context.WithTimeout(ctx, 30*time.Second)
	sub2 := NewClient(roles.id, survivors, 600, 3)
	err := sub2.Run(sctx)
	scancel()
	if err != nil {
		t.Fatalf("the subscriber did not come back on a survivor: %v", err)
	}
	drainInto(ledger, sub2, 3, 8*time.Second)
	sub2.Close()
	if vs := Check(Evidence{Ledger: ledger}); hasViolation(vs, PropNoAckedLoss) {
		t.Fatalf("acknowledged messages lost: %v", vs)
	}
}
