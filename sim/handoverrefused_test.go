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

// handoverRefusedRoles names a placement for the hand-over scenarios below: x
// holds the subscriber's session for its first owner o1, the session's owner
// is o (o1 itself, or the node it moved to), and with d out x holds none of
// the session's shard and o replicates it to t; with d and o out, x is the
// first of o's targets a takeover asks.
type handoverRefusedRoles struct {
	id             string
	o1, o, x, d, t string
}

// findHandoverRefusedRoles searches the nodes and client identifiers for that
// placement, with the leader neither o, x, d nor t. moved: o is not o1 and
// does not replicate the session to x under the map over every node.
func findHandoverRefusedRoles(names []string, leader string, replicas, shards int, moved bool) (handoverRefusedRoles, bool) {
	without := func(out ...string) shardmap.Map {
		return shardmap.Compute(0, slices.DeleteFunc(slices.Clone(names), func(n string) bool { return slices.Contains(out, n) }), replicas, shards)
	}
	all := without()
	for _, o1 := range names {
		for _, o := range names {
			if moved == (o == o1) {
				continue
			}
			for _, x := range names {
				for _, d := range names {
					if slices.Contains([]string{o, x, d}, leader) || x == o1 || x == o || d == o || d == x {
						continue
					}
					back, gone := without(d), without(d, o)
					for i := 0; i < 512; i++ {
						id := fmt.Sprintf("sub-refused-%d", i)
						s := shardmap.ShardOf("default", id, shards)
						if !slices.Equal(all.Targets(s, o1), []string{x}) || (moved && slices.Contains(all.Targets(s, o), x)) ||
							back.Holds(s, x) || !slices.Equal(gone.Targets(s, o), []string{x}) {
							continue
						}
						if t := back.Targets(s, o); len(t) == 1 && t[0] != leader {
							return handoverRefusedRoles{id: id, o1: o1, o: o, x: x, d: d, t: t[0]}, true
						}
					}
				}
			}
		}
	}
	return handoverRefusedRoles{}, false
}

// handoverRefused is one run of a hand-over scenario on five brokers.
type handoverRefused struct {
	t      *testing.T
	c      *Cluster
	ctx    context.Context
	ledger *Ledger
	roles  handoverRefusedRoles
	shard  int
}

func newHandoverRefused(t *testing.T, moved bool, half string) (*handoverRefused, context.CancelFunc) {
	t.Helper()
	c := New(t, Options{Nodes: 5, Log: halfLog(t, half)})
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	leaderName, ok := agreedLeader(leaderViews(c, nil))
	if !ok {
		t.Fatal("no agreed leader")
	}
	names := make([]string, 0, len(c.Nodes))
	for _, n := range c.Nodes {
		names = append(names, n.Name)
	}
	rep := c.Node(leaderName).Server().Replication()
	shards := rep.Map().Count()
	roles, found := findHandoverRefusedRoles(names, leaderName, rep.Map().Current().Replicas, shards, moved)
	if !found {
		t.Fatalf("no placement under leader %s", leaderName)
	}
	s := &handoverRefused{t: t, c: c, ctx: ctx, ledger: &Ledger{}, roles: roles, shard: shardmap.ShardOf("default", roles.id, shards)}
	t.Logf("client %s (shard %d): %s fed %s; owner %s, then fed %s with %s out", roles.id, s.shard, roles.o1, roles.x, roles.o, roles.t, roles.d)
	return s, cancel
}

func (s *handoverRefused) node(name string) *Node { return s.c.Node(name) }

// connect connects the subscriber on a node, the first time with its
// subscription, records what it is delivered, and leaves.
func (s *handoverRefused) connect(on string, subscribe bool) {
	s.t.Helper()
	sub := NewClient(s.roles.id, []string{s.node(on).TCP}, 600, 1)
	if err := sub.Run(s.ctx); err != nil {
		s.t.Fatal(err)
	}
	if subscribe {
		if err := sub.Subscribe(s.ctx, "refused/t", 1); err != nil {
			s.t.Fatal(err)
		}
		s.ledger.Subscriptions = []Subscription{{Client: sub.ID, Filter: "refused/t", QoS: 1, Since: time.Now()}}
	} else {
		drainInto(s.ledger, sub, 3, 8*time.Second)
	}
	sub.Close()
	waitOffline(s.t, s.node(on), s.roles.id)
}

// publish publishes seq from..to at QoS 1 on a node.
func (s *handoverRefused) publish(on string, from, to uint64) {
	s.t.Helper()
	pub := NewClient("pub-refused", []string{s.node(on).TCP}, 0, 2)
	if err := pub.Run(s.ctx); err != nil {
		s.t.Fatal(err)
	}
	defer pub.Close()
	for i := from; i <= to; i++ {
		code, err := publishWithRetry(s.ctx, pub, "refused/t", 1, i, 5)
		if err != nil || code.IsError() {
			s.ledger.addErr(PublishFailure{Client: pub.ID, Node: on, Seq: i, Code: code, Err: fmt.Sprint(err)})
			continue
		}
		s.ledger.addPub(Publication{Publisher: pub.ID, Topic: "refused/t", QoS: 1, Seq: i, Code: code, AckedAt: time.Now()})
	}
}

// waitMap waits until every running node's map satisfies ok and is
// reconciled there.
func (s *handoverRefused) waitMap(what string, ok func(shardmap.Map) bool) {
	s.t.Helper()
	waitUntil(s.t, what, 30*time.Second, func() bool {
		for _, n := range s.c.Running() {
			r := n.Server().Replication()
			if m := r.Map().Current(); !ok(m) || r.Reconciled() < m.Version {
				return false
			}
		}
		return true
	})
}

// checkBack asserts that with d out x holds none of the shard and o
// replicates the session to t.
func (s *handoverRefused) checkBack() {
	s.t.Helper()
	m := s.node(s.roles.x).Server().Replication().Map().Current()
	if m.Holds(s.shard, s.roles.x) || !slices.Equal(m.Targets(s.shard, s.roles.o), []string{s.roles.t}) {
		s.t.Fatalf("precondition: shard %d is held by %v and %s's targets are %v", s.shard, m.Holders(s.shard), s.roles.o, m.Targets(s.shard, s.roles.o))
	}
}

// ownerDies lets some check intervals pass, then crashes o. Until the map
// without o is applied every frame to t is lost, so the takeover's fetch reads
// that map's sources, as a fetch made after the map change does. The
// subscriber then comes back on a survivor, and every acknowledged message
// must reach it.
func (s *handoverRefused) ownerDies(want int) {
	s.t.Helper()
	time.Sleep(3 * 2 * s.c.timings().TTL)
	x, o, tn := s.node(s.roles.x), s.node(s.roles.o), s.node(s.roles.t)
	held := -1
	if ss := replicaCopy(x, s.roles.id); ss != nil {
		held = len(ss.Entries)
	}
	s.t.Logf("before %s dies: %s holds a copy of %d entries", o.Name, x.Name, held)
	for _, n := range s.c.Running() {
		if n != tn {
			s.c.Fault(n, tn, &transport.Fault{Drop: 1, Classes: []transport.Class{transport.ReplicationAppend}, Rand: rand.New(rand.NewSource(11))}) //nolint:gosec // seeded simulation
		}
	}
	s.c.Crash(o)
	s.waitMap("the map without "+o.Name, func(m shardmap.Map) bool { return !slices.Contains(m.Basis, o.Name) })
	for _, n := range s.c.Running() {
		s.c.Fault(n, tn, nil)
	}
	var survivors []string
	for _, n := range s.c.Running() {
		survivors = append(survivors, n.TCP)
	}
	sctx, scancel := context.WithTimeout(s.ctx, 30*time.Second)
	sub := NewClient(s.roles.id, survivors, 600, 3)
	err := sub.Run(sctx)
	scancel()
	if err != nil {
		s.t.Fatalf("the subscriber did not come back on a survivor: %v", err)
	}
	drainInto(s.ledger, sub, want, 8*time.Second)
	sub.Close()
	if vs := Check(Evidence{Ledger: s.ledger}); hasViolation(vs, PropNoAckedLoss) {
		s.t.Fatalf("acknowledged messages lost: %v", vs)
	}
}

// TestHandoverRefusedCopyDoesNotOutliveTheOwnersLaterWork, on five brokers:
// the subscriber's owner o replicates its session, three acknowledged messages
// queued, to x. Every confirmation o sends x is lost. x drains and stops,
// keeping its copy; d drains and stops; x restarts, and with d out it holds
// none of the session's shard, so it hands its copy to o's target t, which
// already holds the session and refuses it. o, alive throughout, takes three
// more acknowledged messages, then dies. The survivors must hold all six.
func TestHandoverRefusedCopyDoesNotOutliveTheOwnersLaterWork(t *testing.T) {
	s, cancel := newHandoverRefused(t, false, "handoverrefused")
	defer cancel()
	o, x, d := s.node(s.roles.o), s.node(s.roles.x), s.node(s.roles.d)
	s.connect(o.Name, true)
	s.publish(o.Name, 1, 3)
	waitReplicaCopy(t, x, s.roles.id, 3, 15*time.Second)

	s.c.Fault(o, x, &transport.Fault{Drop: 1, Classes: []transport.Class{transport.TakeoverRequest}, Rand: rand.New(rand.NewSource(7))}) //nolint:gosec // seeded simulation
	s.c.Drain(x)
	waitReconciledWithout(t, s.c, x.Name, 30*time.Second)
	s.c.Drain(d)
	waitReconciledWithout(t, s.c, d.Name, 30*time.Second)
	s.c.Start(x)
	s.waitMap(x.Name+" back in the map with "+d.Name+" out", func(m shardmap.Map) bool {
		return slices.Contains(m.Basis, x.Name) && !slices.Contains(m.Basis, d.Name)
	})
	s.checkBack()
	if replicaCopy(x, s.roles.id) == nil {
		t.Fatalf("precondition: %s holds no copy after it returned", x.Name)
	}
	s.publish(o.Name, 4, 6)
	s.ownerDies(6)
}

// TestHandoverRefusedEarlierGrantCopyDoesNotOutliveTheNewOwnersWork, on five
// brokers: o1 replicates the subscriber's session, three acknowledged
// messages queued, to x. The subscriber moves to o, which does not replicate
// it to x, and leaves. d drains and stops; with d out x holds none of the
// session's shard, so it hands its copy, from o1's grant, to o's target t,
// which refuses a copy from an earlier grant than the lease's. o, alive
// throughout, takes three more acknowledged messages, then dies. The
// survivors must hold the three.
func TestHandoverRefusedEarlierGrantCopyDoesNotOutliveTheNewOwnersWork(t *testing.T) {
	s, cancel := newHandoverRefused(t, true, "handoverrefusedgrant")
	defer cancel()
	o1, o, x, d := s.node(s.roles.o1), s.node(s.roles.o), s.node(s.roles.x), s.node(s.roles.d)
	s.connect(o1.Name, true)
	s.publish(o1.Name, 1, 3)
	waitReplicaCopy(t, x, s.roles.id, 3, 15*time.Second)
	s.connect(o.Name, false)

	s.c.Drain(d)
	waitReconciledWithout(t, s.c, d.Name, 30*time.Second)
	s.checkBack()
	s.publish(o.Name, 4, 6)
	s.ownerDies(3)
}
