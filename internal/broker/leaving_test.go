package broker

import (
	"fmt"
	"math/rand"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

// leavingTrio is three brokers, eight shards, with the given copy count and a
// stable interval of a second, so readiness reads the topology.
func leavingTrio(t *testing.T, replicas int) (map[string]*fixture, shardmap.Map) {
	t.Helper()
	nodes := startTrio(t, func(_ string, d *config.Document) {
		d.Cluster.Shards = 8
		d.Cluster.Replicas = replicas
		d.Cluster.Readiness.StableMs = 1000
		d.Cluster.Drain.ConfirmTimeoutMs = 4000
	})
	return nodes, waitMap(t, nodes, 3)
}

// clientTargeting finds a client identifier whose session, owned by owner,
// has node among its replica targets under m.
func clientTargeting(m shardmap.Map, owner, node string) string {
	for i := 0; ; i++ {
		id := fmt.Sprintf("x%d", i)
		if slices.Contains(m.Targets(shardmap.ShardOf("default", id, len(m.Shards)), owner), node) {
			return id
		}
	}
}

// durableSubscriber connects id on f with a session that outlives the
// connection and subscribes it to topic.
func durableSubscriber(t *testing.T, f *fixture, id, topic string) *client {
	t.Helper()
	exp := uint32(600)
	c, ack := f.connect("tcp", packet.V5, &packet.Connect{ClientID: id, CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("connect %s: %s", id, ack.ReasonCode)
	}
	c.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: topic, QoS: 1}}})
	if _, ok := c.read().(*packet.Suback); !ok {
		t.Fatal("no SUBACK")
	}
	return c
}

// publishCode publishes one QoS 1 message and returns the PUBACK's reason.
func publishCode(t *testing.T, pub *client, id uint16, topic string) packet.ReasonCode {
	t.Helper()
	pub.send(&packet.Publish{Topic: topic, QoS: 1, PacketID: id, Payload: []byte("m")})
	a, ok := pub.read().(*packet.Puback)
	if !ok || a.PacketID != id {
		t.Fatalf("PUBACK %d: %+v", id, a)
	}
	return a.ReasonCode
}

// publishAccepted publishes until one is acknowledged Success, the
// precondition of a test that then judges a later publish: a just-formed
// cluster can refuse its first ones while its streams and routes settle. It
// returns the next packet identifier.
func publishAccepted(t *testing.T, pub *client, topic string) uint16 {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for id := uint16(1); ; id++ {
		if publishCode(t, pub, id, topic) == packet.Success {
			return id + 1
		}
		if !time.Now().Before(deadline) {
			t.Fatal("precondition: no publish was accepted before the drain")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func waitReadyAll(t *testing.T, nodes map[string]*fixture, names ...string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, n := range names {
			if r, _ := nodes[n].srv.Readiness(); !r {
				ok = false
			}
		}
		if ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%v not ready", names)
}

// TestDrainedNodeLeavesTheShardMap: n3 drains with no client of its own. Its
// hand-off moves nothing and marks it leaving; the map leaves it out on every
// node; the copy n3 held of x, whose target it was, goes once n1 confirms x
// held at its new target; the survivors stay ready through the map change;
// and with n3 stopped a publish into x is acknowledged, x's target now a live
// node.
func TestDrainedNodeLeavesTheShardMap(t *testing.T) {
	nodes, m := leavingTrio(t, 2)
	id := clientTargeting(m, "n1", "n3")
	durableSubscriber(t, nodes["n1"], id, "t/leave")
	pub, _ := nodes["n1"].connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	next := publishAccepted(t, pub, "t/leave")
	waitFor := func(what string, d time.Duration, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(d)
		for time.Now().Before(deadline) {
			if cond() {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for %s", what)
	}
	waitFor("n3 to hold x's copy", 5*time.Second, func() bool {
		return nodes["n3"].srv.Replication().ReplicaState().Find("default", id) != nil
	})
	waitReadyAll(t, nodes, "n1", "n2")
	nodes["n3"].srv.Drain()
	waitFor("a map without n3 on n1 and n2", 10*time.Second, func() bool {
		for _, n := range []string{"n1", "n2"} {
			if b := nodes[n].srv.Replication().Map().Current().Basis; !slices.Equal(b, []string{"n1", "n2"}) {
				return false
			}
		}
		return true
	})
	if !nodes["n1"].srv.Leases().Table().Leaving("n3") {
		t.Fatal("n3 is not marked leaving on n1")
	}
	// The hand-off woke the leader's driver: the map that left n3 out was
	// decided on the wake, not on the driver's periodic check.
	excluded := nodes["n1"].srv.Replication().Map().Current().Version
	decidedOnWake := false
	for _, f := range nodes {
		for _, e := range f.srv.Replication().MapTimeline().Events {
			if e.Version == excluded && e.Cause == "declaration" {
				decidedOnWake = true
			}
		}
	}
	if !decidedOnWake {
		t.Fatalf("the map without n3 (version %d) was not decided on the hand-off's wake", excluded)
	}
	// Within the stable interval after the map change, and before n3 could be
	// declared dead: the survivors' topology has not changed.
	until := time.Now().Add(700 * time.Millisecond)
	for time.Now().Before(until) {
		for _, n := range []string{"n1", "n2"} {
			if r, why := nodes[n].srv.Readiness(); !r {
				t.Fatalf("%s not ready after the map left n3 out: %s", n, why)
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	select {
	case <-nodes["n3"].srv.Drained():
	case <-time.After(15 * time.Second):
		t.Fatal("n3's drain did not complete")
	}
	if got := metricSum(t, nodes["n3"], "hoardmq_drain_placement_total", map[string]string{"outcome": "left"}); got != 1 {
		t.Fatalf("placement{left} on n3 = %v, want 1", got)
	}
	if got := metricSum(t, nodes["n3"], "hoardmq_replicas_dropped_total", map[string]string{"reason": "not-a-target-confirmed"}); got < 1 {
		t.Fatalf("dropped{not-a-target-confirmed} on n3 = %v, want at least 1", got)
	}
	if nodes["n3"].srv.Replication().ReplicaState().Find("default", id) != nil {
		t.Fatal("n3 still holds x's copy after its drain")
	}
	nodes["n3"].stop()
	if rc := publishCode(t, pub, next, "t/leave"); rc != packet.Success {
		t.Fatalf("publish with n3 down: %s; x's target should be a live node", rc)
	}
}

// TestDrainedNodeComesBackIntoTheMap: once the drained node is heard alive
// again — here its renewals resumed, standing for its next incarnation's first
// record — the driver's next check puts it back.
func TestDrainedNodeComesBackIntoTheMap(t *testing.T) {
	nodes, _ := leavingTrio(t, 2)
	nodes["n3"].srv.Drain()
	select {
	case <-nodes["n3"].srv.Drained():
	case <-time.After(15 * time.Second):
		t.Fatal("n3's drain did not complete")
	}
	waitMap(t, nodes, 2)
	nodes["n3"].srv.Leases().ResumeRenewals()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if b := nodes["n1"].srv.Replication().Map().Current().Basis; len(b) == 3 {
			if nodes["n1"].srv.Leases().Table().Leaving("n3") {
				t.Fatal("n3 is back in the map while still marked leaving")
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("n3 did not come back into the map")
}

// TestDrainKeepsTheCopyCount: with three copies on three brokers a draining
// node stays in the map, its drain ends with placement{placed}, and a publish
// into a session whose copy it holds is refused while it is down.
func TestDrainKeepsTheCopyCount(t *testing.T) {
	nodes, _ := leavingTrio(t, 3)
	id := "x3"
	durableSubscriber(t, nodes["n1"], id, "t/floor")
	pub, _ := nodes["n1"].connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	next := publishAccepted(t, pub, "t/floor")
	nodes["n3"].srv.Drain()
	select {
	case <-nodes["n3"].srv.Drained():
	case <-time.After(15 * time.Second):
		t.Fatal("n3's drain did not complete")
	}
	if got := metricSum(t, nodes["n3"], "hoardmq_drain_placement_total", map[string]string{"outcome": "placed"}); got != 1 {
		t.Fatalf("placement{placed} on n3 = %v, want 1", got)
	}
	if b := nodes["n1"].srv.Replication().Map().Current().Basis; len(b) != 3 {
		t.Fatalf("basis %v; the copy count should keep n3 in it", b)
	}
	nodes["n3"].stop()
	if rc := publishCode(t, pub, next, "t/floor"); rc == packet.Success {
		t.Fatal("a publish needing the down node's copy was acknowledged with fewer copies than configured")
	}
}

// TestLateRenewalClearsTheMarkAndTheDrainMarksAgain: a renewal applied after
// the hand-off clears n3's mark; the drain hands off again and leaves.
func TestLateRenewalClearsTheMarkAndTheDrainMarksAgain(t *testing.T) {
	var n3 *fixture
	fixtureHooks = Hooks{DrainHandedOff: func() {
		if n3 == nil {
			return
		}
		l := n3.srv.Leases()
		deadline := time.Now().Add(5 * time.Second)
		for !l.Table().Leaving("n3") && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		l.ResumeRenewals()
		for l.Table().Leaving("n3") && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		l.PauseRenewals(time.Second)
	}}
	t.Cleanup(func() { fixtureHooks = Hooks{} })
	nodes, _ := leavingTrio(t, 2)
	n3 = nodes["n3"]
	n3.srv.Drain()
	select {
	case <-n3.srv.Drained():
	case <-time.After(15 * time.Second):
		t.Fatal("n3's drain did not complete")
	}
	if got := metricSum(t, n3, "hoardmq_drain_placement_total", map[string]string{"outcome": "left"}); got != 1 {
		t.Fatalf("placement{left} on n3 = %v, want 1", got)
	}
	if !nodes["n1"].srv.Leases().Table().Leaving("n3") {
		t.Fatal("n3 is not marked leaving after its drain")
	}
}

// TestDrainWaitsForItsMovedCopies: n1's whole write of x to its new target is
// slow, so its confirmation to n3 comes late; n3's drain does not complete
// until that confirmation has released the copy n3 held as x's old target.
func TestDrainWaitsForItsMovedCopies(t *testing.T) {
	nodes, m := leavingTrio(t, 2)
	id := clientTargeting(m, "n1", "n3")
	durableSubscriber(t, nodes["n1"], id, "t/slow")
	pub, _ := nodes["n1"].connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	publishAccepted(t, pub, "t/slow")
	deadline := time.Now().Add(5 * time.Second)
	for nodes["n3"].srv.Replication().ReplicaState().Find("default", id) == nil {
		if !time.Now().Before(deadline) {
			t.Fatal("precondition: n3 does not hold x's copy")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// The delay is per frame, and the confirmation's durability wait (one
	// acknowledgement timeout, 1 s here) covers every frame n1 pushed n2 before
	// it: x's whole write and the release of the retained shards n2 newly
	// follows. Two frames must fit in the wait.
	nodes["n1"].srv.Cluster().SetFault("n2", &transport.Fault{Delay: 400 * time.Millisecond, Classes: []transport.Class{transport.ReplicationAppend}})
	t.Cleanup(func() { nodes["n1"].srv.Cluster().SetFault("n2", nil) })
	nodes["n3"].srv.Drain()
	select {
	case <-nodes["n3"].srv.Drained():
	case <-time.After(15 * time.Second):
		t.Fatal("n3's drain did not complete")
	}
	if nodes["n3"].srv.Replication().ReplicaState().Find("default", id) != nil {
		t.Fatal("n3's drain completed while it still held x's copy, before n1 confirmed x held at its new target")
	}
	if got := metricSum(t, nodes["n3"], "hoardmq_drain_placement_total", map[string]string{"outcome": "left"}); got != 1 {
		t.Fatalf("placement{left} on n3 = %v, want 1", got)
	}
}

// TestALeavingNodeTakesNoHandOff: the consensus leader drains first and stays
// running, marked leaving. A second drain hands its offline session to the
// third node, not to the leader, which a drain would otherwise prefer.
func TestALeavingNodeTakesNoHandOff(t *testing.T) {
	nodes, _ := leavingTrio(t, 2)
	leader := ""
	for name, f := range nodes {
		if f.srv.Consensus().Status().Role == "leader" {
			leader = name
		}
	}
	if leader == "" {
		t.Fatal("precondition: no leader")
	}
	var second, third string
	for _, n := range []string{"n1", "n2", "n3"} {
		switch {
		case n == leader:
		case second == "":
			second = n
		default:
			third = n
		}
	}
	nodes[leader].srv.Drain()
	select {
	case <-nodes[leader].srv.Drained():
	case <-time.After(15 * time.Second):
		t.Fatal("the leader's drain did not complete")
	}
	if !nodes[second].srv.Leases().Table().Leaving(leader) {
		t.Fatal("precondition: the leader is not marked leaving")
	}
	c := durableSubscriber(t, nodes[second], "off", "t/off")
	c.send(&packet.Disconnect{ReasonCode: packet.NormalDisconnection})
	_ = c.c.Close()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if rec, ok := nodes[second].srv.Sessions().Record("off"); ok && !rec.Online {
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatal("precondition: the session did not go offline")
		}
		time.Sleep(20 * time.Millisecond)
	}
	nodes[second].srv.Drain()
	select {
	case <-nodes[second].srv.Drained():
	case <-time.After(15 * time.Second):
		t.Fatal("the second drain did not complete")
	}
	l, ok := nodes[third].srv.Leases().Table().Lookup("default", "off")
	if !ok || l.Owner != third {
		t.Fatalf("the offline session's lease after the second drain: %+v, %v; want it on %s, not the leaving leader %s", l, ok, third, leader)
	}
}

// TestDrainCopyWaitIsBounded: n1's whole write of x to its new target never
// lands, so no confirmation comes; n3's drain gives the copy up to its bound,
// counts placement{copies-expired} and completes, still holding the copy.
func TestDrainCopyWaitIsBounded(t *testing.T) {
	nodes, m := leavingTrio(t, 2)
	id := clientTargeting(m, "n1", "n3")
	durableSubscriber(t, nodes["n1"], id, "t/lost")
	pub, _ := nodes["n1"].connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	publishAccepted(t, pub, "t/lost")
	deadline := time.Now().Add(5 * time.Second)
	for nodes["n3"].srv.Replication().ReplicaState().Find("default", id) == nil {
		if !time.Now().Before(deadline) {
			t.Fatal("precondition: n3 does not hold x's copy")
		}
		time.Sleep(10 * time.Millisecond)
	}
	nodes["n1"].srv.Cluster().SetFault("n2", &transport.Fault{Drop: 1, Classes: []transport.Class{transport.ReplicationAppend}, Rand: rand.New(rand.NewSource(1))}) //nolint:gosec // seeded test fault
	t.Cleanup(func() { nodes["n1"].srv.Cluster().SetFault("n2", nil) })
	nodes["n3"].srv.Drain()
	select {
	case <-nodes["n3"].srv.Drained():
	case <-time.After(20 * time.Second):
		t.Fatal("n3's drain did not complete")
	}
	if got := metricSum(t, nodes["n3"], "hoardmq_drain_placement_total", map[string]string{"outcome": "copies-expired"}); got != 1 {
		t.Fatalf("placement{copies-expired} on n3 = %v, want 1", got)
	}
	if nodes["n3"].srv.Replication().ReplicaState().Find("default", id) == nil {
		t.Fatal("n3 dropped x's copy with no confirmation")
	}
}

// handoffTrio is leavingTrio's three brokers with two copies and a ttl of 4 s,
// so a node whose renewals a drain paused is not declared dead inside these
// tests' few seconds; mutate adjusts each node further.
func handoffTrio(t *testing.T, mutate func(d *config.Document)) map[string]*fixture {
	t.Helper()
	nodes := startTrio(t, func(_ string, d *config.Document) {
		d.Cluster.Shards = 8
		d.Cluster.Replicas = 2
		d.Cluster.Readiness.StableMs = 1000
		d.Cluster.Drain.ConfirmTimeoutMs = 4000
		d.Cluster.Leases.TTLMs = 4000
		if mutate != nil {
			mutate(d)
		}
	})
	waitMap(t, nodes, 3)
	return nodes
}

// leaderOf names the node that leads, or "" when none does.
func leaderOf(nodes map[string]*fixture) string {
	for name, f := range nodes {
		if f.srv.Consensus().Status().Role == "leader" {
			return name
		}
	}
	return ""
}

// TestDrainLeavesTheMapWhenItsHandOffMeetsAnElection: n3 drains with nothing
// to hand, and its first hand-off meets an election. From just before that
// attempt the leader's consensus frames are dropped until another node leads,
// so the attempt cannot apply. The drain tries again, the new leader applies
// it, and n3 is marked leaving and leaves the map.
func TestDrainLeavesTheMapWhenItsHandOffMeetsAnElection(t *testing.T) {
	var nodes map[string]*fixture
	var attempts atomic.Int32
	moved := make(chan string, 1)
	setDrop := func(lead string, on bool) {
		for name := range nodes {
			if name == lead {
				continue
			}
			var f *transport.Fault
			if on {
				f = &transport.Fault{Drop: 1, Classes: []transport.Class{transport.Consensus}, Rand: rand.New(rand.NewSource(1))} //nolint:gosec // a fault's coin, not a secret
			}
			nodes[lead].srv.Cluster().SetFault(name, f)
		}
	}
	fixtureHooks = Hooks{DrainHandoff: func(attempt int) {
		attempts.Store(int32(attempt))
		if attempt != 1 {
			return
		}
		lead := leaderOf(nodes)
		setDrop(lead, true)
		go func() {
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				if l := leaderOf(nodes); l != "" && l != lead {
					setDrop(lead, false)
					moved <- lead + " to " + l
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
			setDrop(lead, false)
			moved <- lead + " to nobody"
		}()
	}}
	t.Cleanup(func() { fixtureHooks = Hooks{} })
	nodes = handoffTrio(t, nil)
	nodes["n3"].srv.Drain()
	select {
	case m := <-moved:
		t.Logf("the lead moved from %s during the first hand-off", m)
	case <-time.After(10 * time.Second):
		t.Fatal("precondition not reached: n3's drain made no hand-off attempt")
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		left := true
		for _, n := range []string{"n1", "n2"} {
			if b := nodes[n].srv.Replication().Map().Current().Basis; !slices.Equal(b, []string{"n1", "n2"}) {
				left = false
			}
		}
		if left {
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("timed out waiting for a map without n3 on n1 and n2 (hand-off attempts %d)", attempts.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !nodes["n1"].srv.Leases().Table().Leaving("n3") {
		t.Fatal("n3 is not marked leaving on n1")
	}
	select {
	case <-nodes["n3"].srv.Drained():
	case <-time.After(15 * time.Second):
		t.Fatal("n3's drain did not complete")
	}
	// The first attempt failed: the pass above is the retry's.
	if a := attempts.Load(); a < 2 {
		t.Fatalf("hand-off attempts on n3 = %d, want at least 2", a)
	}
	if got := metricSum(t, nodes["n3"], "hoardmq_drain_handoff_retries_total", nil); got < 1 {
		t.Fatalf("handoff_retries on n3 = %v, want at least 1", got)
	}
	if got := metricSum(t, nodes["n3"], "hoardmq_drain_placement_total", map[string]string{"outcome": "left"}); got != 1 {
		t.Fatalf("placement{left} on n3 = %v, want 1", got)
	}
	if got := metricSum(t, nodes["n3"], "hoardmq_drain_placement_total", map[string]string{"outcome": "not-handed-off"}); got != 0 {
		t.Fatalf("placement{not-handed-off} on n3 = %v, want 0", got)
	}
}

// TestDrainThatCannotHandOffStopsAtItsDeadline: a draining node whose lease
// frames reach no peer tries its hand-off again until the drain's deadline,
// then completes, counted not-handed-off, unmarked and still in the map.
func TestDrainThatCannotHandOffStopsAtItsDeadline(t *testing.T) {
	var attempts atomic.Int32
	fixtureHooks = Hooks{DrainHandoff: func(attempt int) { attempts.Store(int32(attempt)) }}
	t.Cleanup(func() { fixtureHooks = Hooks{} })
	const timeout = 2500 * time.Millisecond
	nodes := handoffTrio(t, func(d *config.Document) { d.Cluster.Drain.TimeoutMs = int(timeout / time.Millisecond) })
	lead := leaderOf(nodes)
	var d string
	for _, n := range []string{"n1", "n2", "n3"} {
		if n != lead {
			d = n
			break
		}
	}
	for name := range nodes {
		if name != d {
			nodes[d].srv.Cluster().SetFault(name, &transport.Fault{Drop: 1, Classes: []transport.Class{transport.LeaseNotification}, Rand: rand.New(rand.NewSource(1))}) //nolint:gosec // a fault's coin, not a secret
		}
	}
	nodes[d].srv.Drain()
	select {
	case <-nodes[d].srv.Drained():
	case <-time.After(timeout + 2500*time.Millisecond):
		t.Fatalf("%s's drain did not complete within its deadline and 2.5 s (attempts %d, role %s)", d, attempts.Load(), nodes[d].srv.Consensus().Status().Role)
	}
	if a := attempts.Load(); a < 2 {
		t.Fatalf("hand-off attempts on %s = %d, want at least 2 (role %s)", d, a, nodes[d].srv.Consensus().Status().Role)
	}
	if got := metricSum(t, nodes[d], "hoardmq_drain_handoff_retries_total", nil); got < 1 {
		t.Fatalf("handoff_retries on %s = %v, want at least 1", d, got)
	}
	if got := metricSum(t, nodes[d], "hoardmq_drain_placement_total", map[string]string{"outcome": "not-handed-off"}); got != 1 {
		t.Fatalf("placement{not-handed-off} on %s = %v, want 1", d, got)
	}
	if got := metricSum(t, nodes[d], "hoardmq_drain_placement_total", map[string]string{"outcome": "left"}); got != 0 {
		t.Fatalf("placement{left} on %s = %v, want 0", d, got)
	}
	other := lead
	if nodes[other].srv.Leases().Table().Leaving(d) {
		t.Fatalf("%s is marked leaving on %s with no hand-off applied", d, other)
	}
	if b := nodes[other].srv.Replication().Map().Current().Basis; !slices.Contains(b, d) {
		t.Fatalf("basis on %s is %v; %s, never handed off, should still be in it", other, b, d)
	}
}
