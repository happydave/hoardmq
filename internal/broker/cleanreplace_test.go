package broker

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

// cleanTakeover leaves x's persistent session on n1 with the subscription t/x
// and three queued messages, waits for the replica on holder, then connects x
// to n2 with Clean Start and the given expiry (0 for none). pick chooses the
// client identifier from the map. It returns the identifier, n2's client and
// the token n2 serves x at.
func cleanTakeover(t *testing.T, nodes map[string]*fixture, m shardmap.Map, holder string, pick func(shard int) bool, expiry uint32) (string, *client, uint64) {
	t.Helper()
	id := ""
	for i := 0; id == ""; i++ {
		if i > 1000 {
			t.Fatal("no client identifier fits the placement")
		}
		if c := fmt.Sprint("x", i); pick(shardmap.ShardOf("default", c, 8)) {
			id = c
		}
	}
	exp := uint32(600)
	x, ack := nodes["n1"].connect("tcp", packet.V5, &packet.Connect{ClientID: id, CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("connect on n1: %s", ack.ReasonCode)
	}
	x.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "t/x", QoS: 1}}})
	if _, ok := x.read().(*packet.Suback); !ok {
		t.Fatal("no SUBACK")
	}
	x.send(&packet.Disconnect{ReasonCode: packet.NormalDisconnection})
	_ = x.c.Close()
	waitUntil(t, "n1 to record x offline", 5*time.Second, func() bool {
		rec, ok := nodes["n1"].srv.Sessions().Record(id)
		return ok && !rec.Online
	})
	pub, _ := nodes["n1"].connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	// A freshly formed cluster can refuse a first QoS 1 publish while its
	// replication links settle; a refused one is sent again.
	pid := uint16(0)
	for i := 1; i <= 3; i++ {
		for accepted := false; !accepted; {
			if pid++; pid > 40 {
				t.Fatalf("precondition not reached: message %d never accepted", i)
			}
			pub.send(&packet.Publish{Topic: "t/x", QoS: 1, PacketID: pid, Payload: []byte(fmt.Sprint("m", i))})
			a, ok := pub.read().(*packet.Puback)
			if accepted = ok && a.ReasonCode == packet.Success; !accepted {
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
	waitUntil(t, holder+" to hold x's replica with its messages", 5*time.Second, func() bool {
		ss := nodes[holder].srv.Replication().ReplicaState().Find("default", id)
		return ss != nil && len(ss.Entries) >= 3 && len(ss.Subscriptions) == 1
	})
	// The previous owner's own delete, written as it loses the lease, is
	// accepted by a holder that has not yet applied the new grant. Held back
	// three seconds it arrives after the grant, as it usually does, so the
	// copy can go only by the new owner's write.
	for _, peer := range []string{"n2", "n3"} {
		nodes["n1"].srv.Cluster().SetFault(peer, &transport.Fault{Delay: 3 * time.Second, Classes: []transport.Class{transport.ReplicationAppend}})
	}
	var ne *uint32
	if expiry != 0 {
		ne = &expiry
	}
	x2, ack := nodes["n2"].connect("tcp", packet.V5, &packet.Connect{ClientID: id, CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: ne}}, false)
	if ack.ReasonCode != packet.Success || ack.SessionPresent {
		t.Fatalf("clean connect on n2: %s, session present %v", ack.ReasonCode, ack.SessionPresent)
	}
	var token uint64
	waitUntil(t, "n2's table to show x on n2", 5*time.Second, func() bool {
		l, ok := nodes["n2"].srv.Leases().Table().Lookup("default", id)
		token = l.Epoch
		return ok && l.Owner == "n2"
	})
	return id, x2, token
}

func filters(ss *store.SessionState) []string {
	out := []string{}
	for _, s := range ss.Subscriptions {
		out = append(out, s.Filter)
	}
	return out
}

// TestCleanStartTakeoverLeavesNoOldStateOnTheFollower: a follower holding the
// previous owner's copy, which is also the new owner's target, ends with the
// new session only: nothing of the old subscriptions or queue under the new
// owner's metadata.
func TestCleanStartTakeoverLeavesNoOldStateOnTheFollower(t *testing.T) {
	nodes, m := startReplicatedTrio(t)
	id, x2, token := cleanTakeover(t, nodes, m, "n3", func(sh int) bool {
		return slices.Equal(m.Targets(sh, "n1"), []string{"n3"}) && slices.Equal(m.Targets(sh, "n2"), []string{"n3"})
	}, 600)
	onN3 := func() *store.SessionState { return nodes["n3"].srv.Replication().ReplicaState().Find("default", id) }
	waitUntil(t, "n3's copy at n2's token", 5*time.Second, func() bool {
		ss := onN3()
		return ss != nil && ss.Epoch == token
	})
	if ss := onN3(); len(ss.Subscriptions) != 0 || len(ss.Entries) != 0 {
		t.Fatalf("n3's copy at n2's token holds subscriptions %v and %d entries from the session x replaced", filters(ss), len(ss.Entries))
	}
	x2.send(&packet.Subscribe{PacketID: 2, Subscriptions: []packet.Subscription{{Filter: "t/new", QoS: 1}}})
	if _, ok := x2.read().(*packet.Suback); !ok {
		t.Fatal("no SUBACK on n2")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := nodes["n2"].srv.Sessions().Durable(ctx, []store.Scope{{Namespace: "default", ClientID: id}}); err != nil {
		t.Fatalf("n2's records for x not acknowledged by n3: %v", err)
	}
	if got := filters(onN3()); !slices.Equal(got, []string{"t/new"}) {
		t.Fatalf("n3's copy holds %v, want [t/new]", got)
	}
}

// within5s fails the test with what was not reached within five seconds.
func within5s(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("not within 5s: %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestCleanStartTakeoverWithNoExpiryLeavesNoCopy: a new session that is not
// persistent journals nothing of its own; the follower's copy of the previous
// session still goes.
func TestCleanStartTakeoverWithNoExpiryLeavesNoCopy(t *testing.T) {
	nodes, m := startReplicatedTrio(t)
	id, _, _ := cleanTakeover(t, nodes, m, "n3", func(sh int) bool {
		return slices.Equal(m.Targets(sh, "n1"), []string{"n3"}) && slices.Equal(m.Targets(sh, "n2"), []string{"n3"})
	}, 0)
	within5s(t, "n3 dropped the previous session's copy", func() bool {
		return nodes["n3"].srv.Replication().ReplicaState().Find("default", id) == nil
	})
}

// TestCleanStartTakeoverDropsTheNewOwnersOwnCopy: the new owner, which held
// the previous owner's copy as its target, keeps none of it, though its new
// session journals nothing of its own.
func TestCleanStartTakeoverDropsTheNewOwnersOwnCopy(t *testing.T) {
	nodes, m := startReplicatedTrio(t)
	id, _, _ := cleanTakeover(t, nodes, m, "n2", func(sh int) bool {
		return slices.Equal(m.Targets(sh, "n1"), []string{"n2"})
	}, 0)
	within5s(t, "n2 dropped the previous owner's copy", func() bool {
		return nodes["n2"].srv.Replication().ReplicaState().Find("default", id) == nil
	})
}
