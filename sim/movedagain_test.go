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

// movedAgainRoles names a placement for TestMovedCopyOutlivesALaterMapChange:
// the subscriber's owner o replicates it to x under the map over every node;
// once d drains, x is not o's target; once e drains too, x holds the
// session's shard again without being o's target, and o holds none of it.
type movedAgainRoles struct {
	id         string
	o, x, d, e string
}

// findMovedAgainRoles searches the non-leaders and client identifiers for that
// placement, under the maps computed with every node, without d, and without
// d and e.
func findMovedAgainRoles(names []string, leader string, replicas, shards int) (movedAgainRoles, bool) {
	without := func(out ...string) shardmap.Map {
		return shardmap.Compute(0, slices.DeleteFunc(slices.Clone(names), func(n string) bool { return slices.Contains(out, n) }), replicas, shards)
	}
	all := without()
	for _, d := range names {
		for _, e := range names {
			for _, o := range names {
				if d == e || slices.Contains([]string{d, e}, o) || slices.Contains([]string{d, e, o}, leader) {
					continue
				}
				v2, v3 := without(d), without(d, e)
				for i := 0; i < 512; i++ {
					id := fmt.Sprintf("sub-moved-%d", i)
					s := shardmap.ShardOf("default", id, shards)
					before := all.Targets(s, o)
					if len(before) != 1 || before[0] == d || before[0] == e {
						continue
					}
					x := before[0]
					if slices.Contains(v2.Targets(s, o), x) || !v3.Holds(s, x) || v3.Holds(s, o) || slices.Contains(v3.Targets(s, o), x) {
						continue
					}
					if slices.Equal(v2.Holders(s), v3.Holders(s)) {
						continue
					}
					return movedAgainRoles{id: id, o: o, x: x, d: d, e: e}, true
				}
			}
		}
	}
	return movedAgainRoles{}, false
}

// waitReconciledWithout waits until every running node's map leaves the named
// node out and the node has reconciled it.
func waitReconciledWithout(t *testing.T, c *Cluster, name string, bound time.Duration) {
	t.Helper()
	waitBasisWithout(t, c, name, bound)
	deadline := time.Now().Add(bound)
	for {
		all := true
		for _, n := range c.Running() {
			if r := n.Server().Replication(); r.Reconciled() < r.Map().Current().Version {
				all = false
			}
		}
		if all {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the map without %s is not reconciled everywhere after %s", name, bound)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestMovedCopyOutlivesALaterMapChange, on five brokers: the subscriber's
// owner o replicates its session, three acknowledged messages queued, to x.
// From then on every replication frame o sends is lost. d drains and the map
// moves the session's target from x to another node; x keeps its copy for o.
// e drains and the map moves the shard again, x holding it without being o's
// target. o, alive throughout, then dies before any target holds the session.
// The survivors must still hold the three messages.
func TestMovedCopyOutlivesALaterMapChange(t *testing.T) {
	c := New(t, Options{Nodes: 5, Log: halfLog(t, "movedagain")})
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
	roles, found := findMovedAgainRoles(names, leaderName, rep.Map().Current().Replicas, rep.Map().Count())
	if !found {
		t.Fatalf("no placement under leader %s", leaderName)
	}
	o, x, d, e := c.Node(roles.o), c.Node(roles.x), c.Node(roles.d), c.Node(roles.e)
	t.Logf("client %s: owner %s fed %s; %s then %s drain", roles.id, o.Name, x.Name, d.Name, e.Name)

	ledger := &Ledger{}
	sub := NewClient(roles.id, []string{o.TCP}, 600, 1)
	if err := sub.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := sub.Subscribe(ctx, "moved/t", 1); err != nil {
		t.Fatal(err)
	}
	ledger.Subscriptions = []Subscription{{Client: sub.ID, Filter: "moved/t", QoS: 1, Since: time.Now()}}
	sub.Close()
	waitOffline(t, o, roles.id)
	pub := NewClient("pub-moved", []string{o.TCP}, 0, 2)
	if err := pub.Run(ctx); err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 3; i++ {
		pctx, pcancel := context.WithTimeout(ctx, 5*time.Second)
		code, err := pub.Publish(pctx, "moved/t", 1, i)
		pcancel()
		if err != nil {
			ledger.addErr(PublishFailure{Client: pub.ID, Node: nodeNameAt(c, pub), Seq: i, Code: code, Err: err.Error()})
			continue
		}
		ledger.addPub(Publication{Publisher: pub.ID, Topic: "moved/t", QoS: 1, Seq: i, Code: code, AckedAt: time.Now()})
	}
	pub.Close()
	waitReplicaCopy(t, x, roles.id, 3, 15*time.Second)

	for _, n := range c.Nodes {
		if n != o {
			c.Fault(o, n, &transport.Fault{Drop: 1, Classes: []transport.Class{transport.ReplicationAppend}, Rand: rand.New(rand.NewSource(7))}) //nolint:gosec // seeded simulation
		}
	}
	drain := func(n *Node) {
		t.Helper()
		srv := n.Server()
		srv.Drain()
		select {
		case <-srv.Drained():
		case <-time.After(30 * time.Second):
			t.Fatalf("%s's drain did not complete", n.Name)
		}
	}
	drain(d)
	waitReconciledWithout(t, c, d.Name, 30*time.Second)
	if replicaCopy(x, roles.id) == nil || x.Server().Replication().KeptMoved() < 1 {
		t.Fatalf("precondition: after %s left, %s holds %v and keeps %d moved copies; want its copy kept for %s", d.Name, x.Name, replicaCopy(x, roles.id) != nil, x.Server().Replication().KeptMoved(), o.Name)
	}
	drain(e)
	waitReconciledWithout(t, c, e.Name, 30*time.Second)
	time.Sleep(time.Second)
	t.Logf("before %s dies: %s holds a copy %v", o.Name, x.Name, replicaCopy(x, roles.id) != nil)
	c.Crash(o)
	waitBasisWithout(t, c, o.Name, 30*time.Second)

	var survivors []string
	for _, n := range c.Running() {
		if n != d && n != e {
			survivors = append(survivors, n.TCP)
		}
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
