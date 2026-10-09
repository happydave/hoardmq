package broker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
	"github.com/happydave/hoardmq/internal/cluster/lease"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

// TestDrainRedirectsAndHandsOff: a draining node redirects its connected
// MQTT 5 clients to live nodes with a Server Reference, waits for their
// takeover, hands its remaining leases to a live node and reports complete;
// an offline session's queued messages are delivered from the new owner.
func TestDrainRedirectsAndHandsOff(t *testing.T) {
	nodes := startTrio(t, func(_ string, d *config.Document) {
		d.Cluster.Shards = 8
		d.Cluster.Drain.RatePerSecond = 1000
		d.Cluster.Drain.TimeoutMs = 10000
		d.Cluster.Readiness.StableMs = 0 // stability is TestReadinessTracksTopologyAndDrain's subject
	})
	waitMap(t, nodes, 3)
	byAdvertise := map[string]string{}
	for name, f := range nodes {
		byAdvertise[f.srv.config().Cluster.Advertise] = name
	}
	exp := uint32(600)
	// A connected durable subscriber on n1.
	live, ack := nodes["n1"].connect("tcp", packet.V5, &packet.Connect{ClientID: "live", CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("connect: %s", ack.ReasonCode)
	}
	live.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "d/live", QoS: 1}}})
	live.read()
	// An offline durable session on n1 with queued QoS 1 messages.
	off, _ := nodes["n1"].connect("tcp", packet.V5, &packet.Connect{ClientID: "off", CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	off.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "d/off", QoS: 1}}})
	off.read()
	off.send(&packet.Disconnect{ReasonCode: packet.NormalDisconnection})
	_ = off.c.Close()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if rec, ok := nodes["n1"].srv.Sessions().Record("off"); ok && !rec.Online {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	pub, _ := nodes["n2"].connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	for i := 1; i <= 2; i++ {
		pub.send(&packet.Publish{Topic: "d/off", QoS: 1, PacketID: uint16(i), Payload: []byte(fmt.Sprint("q", i))})
		pub.read()
	}
	// Wait for the queued entries to reach a follower (forwards are async).
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		held := false
		for _, n := range []string{"n2", "n3"} {
			if ss := nodes[n].srv.Replication().ReplicaState().Find("default", "off"); ss != nil && len(ss.Entries) == 2 {
				held = true
			}
		}
		if held {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if ready, _ := nodes["n1"].srv.Readiness(); !ready {
		t.Fatal("n1 not ready before the drain")
	}

	nodes["n1"].srv.Drain()
	if ready, reason := nodes["n1"].srv.Readiness(); ready || reason != "draining" {
		t.Fatalf("readiness during drain: %v %q", ready, reason)
	}
	nodes["n1"].srv.Drain() // idempotent
	// The live client is told where to go.
	d, ok := live.read().(*packet.Disconnect)
	if !ok || d.ReasonCode != packet.UseAnotherServer || d.Properties.ServerReference == nil {
		t.Fatalf("redirect: %+v", d)
	}
	target, known := byAdvertise[*d.Properties.ServerReference]
	if !known || target == "n1" {
		t.Fatalf("server reference %q is not another node's address", *d.Properties.ServerReference)
	}
	// It reconnects there with its session.
	live2, ack := nodes[target].connect("tcp", packet.V5, &packet.Connect{ClientID: "live", CleanStart: false, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	if ack.ReasonCode != packet.Success || !ack.SessionPresent {
		t.Fatalf("reconnect on %s: %+v", target, ack)
	}
	pub.send(&packet.Publish{Topic: "d/live", QoS: 1, PacketID: 9, Payload: []byte("moved")})
	pub.read()
	if m, ok := live2.read().(*packet.Publish); !ok || string(m.Payload) != "moved" {
		t.Fatalf("delivery after redirect: %+v", m)
	}
	// The drain completes with the offline lease handed off.
	select {
	case <-nodes["n1"].srv.Drained():
	case <-time.After(20 * time.Second):
		t.Fatalf("drain did not complete: %+v", nodes["n1"].srv.DrainStatus())
	}
	st := nodes["n1"].srv.DrainStatus()
	if !st.Complete || st.LeasesRemaining != 0 {
		t.Fatalf("drain status %+v", st)
	}
	// The offline session was handed off with its lease, and the drain waited
	// for its receiver to confirm it.
	if st.SessionsConfirmed < 1 || st.SessionsUnconfirmed != 0 || st.SessionsAwaiting != 0 {
		t.Fatalf("drain ended without its handed-off session confirmed: %+v", st)
	}
	// Drained fires when n1 has applied the handoff; n2 applies the same
	// entry on its own schedule, so its table is read with a bounded wait.
	var l lease.Lease
	ok = false
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if l, ok = nodes["n2"].srv.Leases().Table().Lookup("default", "off"); ok && l.Owner != "n1" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !ok || l.Owner == "n1" {
		t.Fatalf("offline lease after drain: %+v %v", l, ok)
	}
	// The new owner imported the replica; connecting there (or elsewhere)
	// delivers the queued messages.
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if rec, ok := nodes[l.Owner].srv.Sessions().Record("off"); ok && rec.Queued == 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	off2, ack := nodes[l.Owner].connect("tcp", packet.V5, &packet.Connect{ClientID: "off", CleanStart: false, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	if ack.ReasonCode != packet.Success || !ack.SessionPresent {
		t.Fatalf("offline client on %s: %+v", l.Owner, ack)
	}
	for i := 1; i <= 2; i++ {
		m, ok := off2.read().(*packet.Publish)
		if !ok || string(m.Payload) != fmt.Sprint("q", i) {
			t.Fatalf("queued %d after drain: %+v", i, m)
		}
		off2.send(&packet.Puback{Ack: packet.Ack{PacketID: m.PacketID}})
	}
	nodes["n1"].stop()
}

// TestDrainAloneClosesClients: with no other live node, a drain closes
// clients with Server Busy and completes.
func TestDrainAloneClosesClients(t *testing.T) {
	f := start(t, func(d *config.Document) {
		d.Cluster.Drain.TimeoutMs = 1000
		d.Cluster.Readiness.StableMs = 0
	})
	c, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "solo", CleanStart: true}, false)
	if st := f.srv.DrainStatus(); st.Draining {
		t.Fatal("draining before drain")
	}
	f.srv.Drain()
	d, ok := c.read().(*packet.Disconnect)
	if !ok || d.ReasonCode != packet.ServerBusy {
		t.Fatalf("lone drain disconnect: %+v", d)
	}
	select {
	case <-f.srv.Drained():
	case <-time.After(10 * time.Second):
		t.Fatal("lone drain did not complete")
	}
	if st := f.srv.DrainStatus(); st.SessionsAwaiting != 0 || st.SessionsUnconfirmed != 0 {
		t.Fatalf("a drain with no node to hand to waited on sessions: %+v", st)
	}
	// The listener is closed: a new client is refused.
	var addr string
	for _, l := range f.srv.listeners {
		if l.Config.Name == "tcp" {
			addr = l.Addr().String()
		}
	}
	if nc, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
		_ = nc.Close()
		t.Fatal("listener still accepting after drain")
	}
}

// TestReadinessTracksTopologyAndDrain (AC 2): readiness is false while the
// topology changed within the stable interval and during a drain, true once
// the topology has settled.
func TestReadinessTracksTopologyAndDrain(t *testing.T) {
	nodes := startTrio(t, func(_ string, d *config.Document) {
		d.Cluster.Readiness.StableMs = 1500
	})
	waitMap(t, nodes, 3)
	waitReady := func(name string, want bool, within time.Duration) string {
		deadline := time.Now().Add(within)
		var reason string
		for time.Now().Before(deadline) {
			var ready bool
			ready, reason = nodes[name].srv.Readiness()
			if ready == want {
				return reason
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("%s readiness did not become %v; last reason %q", name, want, reason)
		return ""
	}
	waitReady("n2", true, 10*time.Second)
	// n3 leaves: the alive set and the shard map change; n2 turns not ready
	// for the stable interval, then ready again.
	nodes["n3"].stop()
	reason := waitReady("n2", false, 10*time.Second)
	if reason != "topology" {
		t.Fatalf("not-ready reason after a topology change: %q", reason)
	}
	waitReady("n2", true, 15*time.Second)
	nodes["n2"].srv.Drain()
	if ready, reason := nodes["n2"].srv.Readiness(); ready || reason != "draining" {
		t.Fatalf("readiness during drain: %v %q", ready, reason)
	}
	<-nodes["n2"].srv.Drained()
}

// TestTopologyWatcher: the stable clock resets on a fingerprint change.
func TestTopologyWatcher(t *testing.T) {
	fp := "a"
	changes := 0
	w := newTopology(func() string { return fp }, 300*time.Millisecond, func() { changes++ })
	if w.Stable() {
		t.Fatal("stable before any sample")
	}
	w.sample()
	if w.Stable() {
		t.Fatal("stable immediately after the first sample")
	}
	time.Sleep(350 * time.Millisecond)
	if !w.Stable() {
		t.Fatal("not stable after the interval")
	}
	fp = "b"
	w.sample()
	if w.Stable() || changes != 1 {
		t.Fatalf("change not detected: stable=%v changes=%d", w.Stable(), changes)
	}
	// A zero interval disables the condition.
	zero := newTopology(func() string { return "x" }, 0, nil)
	if !zero.Stable() {
		t.Fatal("zero interval not stable")
	}
	zero.sample()
	if !zero.Stable() {
		t.Fatal("zero interval not stable after a sample")
	}
}

// TestDrainKeySettled: a session a drain handed off is settled only by a
// confirmation for a later grant than the one this node held it at, or by its
// lease ending; anything else keeps the process from exiting with the only
// copy.
func TestDrainKeySettled(t *testing.T) {
	for _, tc := range []struct {
		name              string
		held, confirmed   uint64
		leaseExists, want bool
	}{
		{"no confirmation", 40, 0, true, false},
		{"a confirmation of this node's own grant", 40, 40, true, false},
		{"a confirmation of an earlier grant", 40, 12, true, false},
		{"a confirmation of a later grant", 40, 41, true, true},
		{"the lease ended", 40, 0, false, true},
		{"the lease ended after a confirmation", 40, 41, false, true},
	} {
		if got := drainKeySettled(tc.held, tc.confirmed, tc.leaseExists); got != tc.want {
			t.Errorf("%s: drainKeySettled(%d, %d, %v) = %v, want %v", tc.name, tc.held, tc.confirmed, tc.leaseExists, got, tc.want)
		}
	}
}

// TestDrainMayEnd: the wait ends when nothing is unconfirmed, or at the
// deadline with the unconfirmed sessions reported as an expiry; never before
// the deadline while one is unconfirmed.
func TestDrainMayEnd(t *testing.T) {
	deadline := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name             string
		unconfirmed      int
		now              time.Time
		end, wantExpired bool
	}{
		{"one unconfirmed before the deadline", 1, deadline.Add(-time.Millisecond), false, false},
		{"many unconfirmed before the deadline", 500, deadline.Add(-time.Minute), false, false},
		{"none unconfirmed before the deadline", 0, deadline.Add(-time.Minute), true, false},
		{"one unconfirmed at the deadline", 1, deadline, true, true},
		{"one unconfirmed past the deadline", 1, deadline.Add(time.Second), true, true},
		{"none unconfirmed past the deadline", 0, deadline.Add(time.Second), true, false},
	} {
		end, expired := drainMayEnd(tc.unconfirmed, tc.now, deadline)
		if end != tc.end || expired != tc.wantExpired {
			t.Errorf("%s: drainMayEnd = (%v, %v), want (%v, %v)", tc.name, end, expired, tc.end, tc.wantExpired)
		}
	}
}

// TestHandoffRetries: a drain proposes a failed hand-off again before its
// deadline unless the table refused it or this node's core has stopped or
// left the cluster; at and past the deadline it never does.
func TestHandoffRetries(t *testing.T) {
	now := time.Unix(1000, 0)
	ahead := now.Add(time.Second)
	cases := []struct {
		name     string
		err      error
		deadline time.Time
		want     bool
	}{
		{"the table refused it", fmt.Errorf("%w: no target", lease.ErrHandoffRefused), ahead, false},
		{"the core stopped", consensus.ErrStopped, ahead, false},
		{"this node was removed", consensus.ErrRemoved, ahead, false},
		{"the drain's wait ended", context.DeadlineExceeded, ahead, true},
		{"the forward found no leader", fmt.Errorf("%w: forward timed out", lease.ErrLeaderUnreachable), ahead, true},
		{"the proposer lost the lead", &consensus.NotLeaderError{}, ahead, true},
		{"the proposal was dropped", fmt.Errorf("%w: lost", consensus.ErrDropped), ahead, true},
		{"the leader's own wait ended, forwarded as text", errors.New("context deadline exceeded"), ahead, true},
		{"at the deadline", context.DeadlineExceeded, now, false},
		{"past the deadline", context.DeadlineExceeded, now.Add(-time.Millisecond), false},
	}
	for _, c := range cases {
		if got := handoffRetries(c.err, now, c.deadline); got != c.want {
			t.Errorf("%s: handoffRetries(%v) = %v, want %v", c.name, c.err, got, c.want)
		}
	}
}
