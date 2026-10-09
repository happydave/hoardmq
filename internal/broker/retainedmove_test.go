package broker

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/packet"
)

// retainedMoveRoles is a placement in which a drain moves a retained shard's
// follower and leaves its leader: under the map over all three nodes the
// shard of topic is led by leader and followed by leaving; under the map
// without leaving it is led by leader and followed by gained.
type retainedMoveRoles struct {
	leader, leaving, gained string
	shard                   int
	topic                   string
}

// findRetainedMoveRoles searches for that placement with gained the
// consensus leader, so the drain and the leader's death each leave the
// consensus core where it is.
func findRetainedMoveRoles(m shardmap.Map, consensusLeader string) (retainedMoveRoles, bool) {
	names := m.Basis
	for _, leaving := range names {
		for _, leader := range names {
			if leader == leaving || leader == consensusLeader || leaving == consensusLeader {
				continue
			}
			without := shardmap.Compute(0, slices.DeleteFunc(slices.Clone(names), func(n string) bool { return n == leaving }), m.Replicas, len(m.Shards))
			for s := range m.Shards {
				before, after := m.Shards[s], without.Shards[s]
				if before.Leader != leader || !slices.Contains(before.Followers, leaving) {
					continue
				}
				if after.Leader != leader || !slices.Contains(after.Followers, consensusLeader) {
					continue
				}
				for i := 0; i < 4096; i++ {
					tp := fmt.Sprintf("r/move/%d", i)
					if shardmap.ShardOf("default", tp, len(m.Shards)) == s {
						return retainedMoveRoles{leader: leader, leaving: leaving, gained: consensusLeader, shard: s, topic: tp}, true
					}
				}
			}
		}
	}
	return retainedMoveRoles{}, false
}

// replicaRetained reports whether the node's replica store holds a retained
// message for the topic.
func replicaRetained(f *fixture, topic string) bool {
	st := f.srv.Replication().ReplicaState()
	if st == nil {
		return false
	}
	for _, m := range st.Retained {
		if m.Namespace == "default" && m.Topic == topic {
			return true
		}
	}
	return false
}

// localRetained reports whether the node's own retained store holds the topic.
func localRetained(f *fixture, topic string) bool {
	return len(f.srv.Sessions().Router().RetainedFor("default", topic, time.Now())) > 0
}

// retainedMoveOutcome is what the scenario saw once the leader was gone.
type retainedMoveOutcome struct {
	roles retainedMoveRoles
	// heldBy names every running node holding the topic, own or replica store.
	heldBy []string
	// served is whether a subscriber on the gained node was sent the message.
	served bool
	// leavingDropped is whether the leaving node had dropped its replica copy
	// before the leader died, and gainedHeld whether the gained node held one.
	leavingDropped, gainedHeld bool
}

// retainedMove runs the scenario: a retained message acknowledged on its
// shard's leader and held at its follower; the follower drains and the map
// moves the shard's follower to the third node; the leader then dies. With
// loseThePush, every replication frame from the leader to the third node is
// dropped from just before the drain, which holds open the window between the
// old follower's drop and the new follower's holding the shard.
func retainedMove(t *testing.T, loseThePush bool) retainedMoveOutcome {
	t.Helper()
	nodes, m := leavingTrio(t, 2)
	lead := leaderOf(nodes)
	if lead == "" {
		t.Fatal("precondition not reached: no consensus leader in the settled trio")
	}
	roles, ok := findRetainedMoveRoles(m, lead)
	if !ok {
		t.Fatalf("precondition not reached: no placement under consensus leader %s in map %+v", lead, m)
	}
	leader, leaving, gained := nodes[roles.leader], nodes[roles.leaving], nodes[roles.gained]
	t.Logf("topic %s (shard %d): led by %s, followed by %s; %s drains and %s follows", roles.topic, roles.shard, roles.leader, roles.leaving, roles.leaving, roles.gained)

	pub, _ := leader.connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	pub.send(&packet.Publish{Topic: roles.topic, QoS: 1, Retain: true, PacketID: 1, Payload: []byte("kept")})
	if a, ok := pub.read().(*packet.Puback); !ok || (a.ReasonCode != packet.Success && a.ReasonCode != packet.NoMatchingSubscribers) {
		t.Fatalf("precondition not reached: the retained publish was not acknowledged: %+v", a)
	}
	waitUntil(t, roles.leaving+" to hold the retained copy", 5*time.Second, func() bool { return replicaRetained(leaving, roles.topic) })

	if loseThePush {
		leader.srv.Cluster().SetFault(roles.gained, &transport.Fault{Drop: 1, Classes: []transport.Class{transport.ReplicationAppend}, Rand: rand.New(rand.NewSource(1))}) //nolint:gosec // seeded test fault
		t.Cleanup(func() { leader.srv.Cluster().SetFault(roles.gained, nil) })
	}
	leaving.srv.Drain()
	waitUntil(t, "the leader and the leaving node to reconcile the map without "+roles.leaving, 10*time.Second, func() bool {
		for _, f := range []*fixture{leader, leaving} {
			r := f.srv.Replication()
			cur := r.Map().Current()
			if slices.Contains(cur.Basis, roles.leaving) || r.Reconciled() < cur.Version {
				return false
			}
		}
		return true
	})
	// The leader's push to the gained node has had the time it takes.
	time.Sleep(500 * time.Millisecond)
	out := retainedMoveOutcome{roles: roles, leavingDropped: !replicaRetained(leaving, roles.topic), gainedHeld: replicaRetained(gained, roles.topic)}
	t.Logf("before %s dies: %s holds its copy %v, %s holds one %v", roles.leader, roles.leaving, !out.leavingDropped, roles.gained, out.gainedHeld)

	leader.srv.Crash()
	leader.stopped = true
	waitUntil(t, "the map without "+roles.leader+" on "+roles.gained, 20*time.Second, func() bool {
		cur := gained.srv.Replication().Map().Current()
		return !slices.Contains(cur.Basis, roles.leader) && gained.srv.Replication().Reconciled() >= cur.Version
	})
	// The new leader promotes what it holds; give the rebalance its moment.
	time.Sleep(time.Second)
	for _, name := range []string{"n1", "n2", "n3"} {
		f := nodes[name]
		if f.stopped {
			continue
		}
		if localRetained(f, roles.topic) || replicaRetained(f, roles.topic) {
			out.heldBy = append(out.heldBy, name)
		}
	}
	sub, ack := gained.connect("tcp", packet.V5, &packet.Connect{ClientID: "sub", CleanStart: true}, false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("subscriber on %s: %s", roles.gained, ack.ReasonCode)
	}
	sub.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: roles.topic, QoS: 1}}})
	if _, ok := sub.read().(*packet.Suback); !ok {
		t.Fatal("no SUBACK")
	}
	_ = sub.c.SetReadDeadline(time.Now().Add(2 * time.Second))
	if p, err := sub.dec.Read(); err == nil {
		if got, ok := p.(*packet.Publish); ok && got.Topic == roles.topic && string(got.Payload) == "kept" && got.Retain {
			out.served = true
		}
	}
	cur := gained.srv.Replication().Map().Current()
	t.Logf("after %s died: map v%d basis %v, shard %d led by %s; held by %v; served %v", roles.leader, cur.Version, cur.Basis, roles.shard, cur.Shards[roles.shard].Leader, out.heldBy, out.served)
	return out
}

// TestRetainedMessageOutlivesItsLeaderAfterAFollowerLeaves: a retained
// message acknowledged on its shard's leader and held at its follower. The
// follower drains, so the map moves the shard's follower to the third node,
// and the leader's push to that node is lost. The leader then dies. The
// message was accepted and must still be served.
func TestRetainedMessageOutlivesItsLeaderAfterAFollowerLeaves(t *testing.T) {
	o := retainedMove(t, true)
	if !o.served || len(o.heldBy) == 0 {
		t.Fatalf("the retained message on %s is lost after %s died: held by %v, served %v; %s dropped its copy before the leader died: %v; %s held one: %v",
			o.roles.topic, o.roles.leader, o.heldBy, o.served, o.roles.leaving, o.leavingDropped, o.roles.gained, o.gainedHeld)
	}
}

// TestRetainedMoveScenarioServesTheMessageWhenNothingIsLost is the control on
// the scenario: with the leader's push to the new follower delivered, the
// message survives the leader's death and is served.
func TestRetainedMoveScenarioServesTheMessageWhenNothingIsLost(t *testing.T) {
	o := retainedMove(t, false)
	if !o.gainedHeld {
		t.Fatalf("precondition not reached: %s did not receive the leader's push", o.roles.gained)
	}
	if !o.served {
		t.Fatalf("the retained message on %s was not served after %s died: held by %v", o.roles.topic, o.roles.leader, o.heldBy)
	}
}

// TestDefectDropMovedRetainedLosesTheMessage is the control on the old
// holder's keep: with the drop at the rebalance restored, the
// reproduction loses the message again.
func TestDefectDropMovedRetainedLosesTheMessage(t *testing.T) {
	fixtureHooks = Hooks{Defects: Defects{DropMovedRetained: true}}
	t.Cleanup(func() { fixtureHooks = Hooks{} })
	o := retainedMove(t, true)
	if o.served || len(o.heldBy) > 0 {
		t.Fatalf("with the drop restored the message survived: held by %v, served %v", o.heldBy, o.served)
	}
}

// TestDefectKeepRetainedNotHandedOnLosesTheMessage is the control on the
// hand-on: the drained node keeps its copy, but with nothing handing it
// to the shard's next leader the message is not served.
func TestDefectKeepRetainedNotHandedOnLosesTheMessage(t *testing.T) {
	fixtureHooks = Hooks{Defects: Defects{KeepRetainedNotHandedOn: true}}
	t.Cleanup(func() { fixtureHooks = Hooks{} })
	o := retainedMove(t, true)
	if o.served {
		t.Fatalf("with no hand-on the message was served: held by %v", o.heldBy)
	}
	if o.leavingDropped {
		t.Fatal("precondition: the drained node should have kept its copy")
	}
}

// TestDrainWaitsForItsRetainedRelease: the leaving node keeps a retained shard
// whose leader's push to the new follower is lost; its drain's leaving phase
// waits for the release and expires with the shard counted kept. With the push
// delivered, the release comes and the phase ends left with nothing kept.
func TestDrainWaitsForItsRetainedRelease(t *testing.T) {
	for _, c := range []struct {
		name     string
		lose     bool
		outcome  string
		retained int
	}{{"push lost", true, "copies-expired", 1}, {"push delivered", false, "left", 0}} {
		t.Run(c.name, func(t *testing.T) {
			nodes, m := leavingTrio(t, 2)
			lead := leaderOf(nodes)
			roles, ok := findRetainedMoveRoles(m, lead)
			if !ok {
				t.Fatalf("precondition not reached: no placement under consensus leader %s", lead)
			}
			leader, leaving := nodes[roles.leader], nodes[roles.leaving]
			pub, _ := leader.connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
			pub.send(&packet.Publish{Topic: roles.topic, QoS: 1, Retain: true, PacketID: 1, Payload: []byte("kept")})
			if a, ok := pub.read().(*packet.Puback); !ok || a.ReasonCode.IsError() {
				t.Fatalf("precondition not reached: the retained publish was not acknowledged: %+v", a)
			}
			waitUntil(t, roles.leaving+" to hold the retained copy", 5*time.Second, func() bool { return replicaRetained(leaving, roles.topic) })
			if c.lose {
				leader.srv.Cluster().SetFault(roles.gained, &transport.Fault{Drop: 1, Classes: []transport.Class{transport.ReplicationAppend}, Rand: rand.New(rand.NewSource(1))}) //nolint:gosec // seeded test fault
				t.Cleanup(func() { leader.srv.Cluster().SetFault(roles.gained, nil) })
			}
			leaving.srv.Drain()
			select {
			case <-leaving.srv.Drained():
			case <-time.After(15 * time.Second):
				t.Fatal("the drain did not complete")
			}
			if got := metricSum(t, leaving, "hoardmq_drain_placement_total", map[string]string{"outcome": c.outcome}); got != 1 {
				t.Fatalf("placement{%s} = %v, want 1", c.outcome, got)
			}
			if st := leaving.srv.DrainStatus(); st.RetainedShardsKept != c.retained {
				t.Fatalf("retained shards kept at the drain's end %d, want %d", st.RetainedShardsKept, c.retained)
			}
		})
	}
}
