package sim

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/broker"
	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

// The negative controls: each property is paired with a injected defect
// that disables the mechanism protecting it. The scenario runs twice — with
// the switch, the property must report a violation; without it, none.

func hasViolation(vs []Violation, prop string) bool {
	for _, v := range vs {
		if v.Property == prop {
			return true
		}
	}
	return false
}

func contrast(t *testing.T, prop string, sab broker.Defects, scenario func(t *testing.T, c *Cluster) Evidence) {
	t.Helper()
	contrastWith(t, prop, sab, broker.Defects{}, scenario)
}

// contrastWith is contrast with a defect the sound half carries too: the
// injected source of a fault whose detection the defective half lacks.
func contrastWith(t *testing.T, prop string, defective, sound broker.Defects, scenario func(t *testing.T, c *Cluster) Evidence) {
	t.Helper()
	contrastOpts(t, prop, defective, sound, Options{}, scenario)
}

// contrastOpts is contrastWith over a cluster of a given shape, for a
// scenario that needs more than the default three nodes.
func contrastOpts(t *testing.T, prop string, defective, sound broker.Defects, opts Options, scenario func(t *testing.T, c *Cluster) Evidence) {
	t.Helper()
	t.Run("defective", func(t *testing.T) {
		o := opts
		o.Defects, o.Log = defective, halfLog(t, "defective")
		vs := Check(scenario(t, New(t, o)))
		if !hasViolation(vs, prop) {
			t.Fatalf("the defective build did not violate %s; violations: %v", prop, vs)
		}
		t.Logf("caught: %v", vs)
	})
	t.Run("sound", func(t *testing.T) {
		o := opts
		o.Defects, o.Log = sound, halfLog(t, "sound")
		vs := Check(scenario(t, New(t, o)))
		if hasViolation(vs, prop) {
			t.Fatalf("the sound build violated %s: %v", prop, vs)
		}
	})
}

// halfLog opens a node-log file for one half of a contrast when
// HOARDMQ_SIM_LOG names a path; the half's name is appended to it.
func halfLog(t *testing.T, half string) io.Writer {
	t.Helper()
	path := os.Getenv("HOARDMQ_SIM_LOG")
	if path == "" {
		return nil
	}
	f, err := os.Create(path + "." + half) //nolint:gosec // operator-chosen path
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

// TestDefectMinorityServes: a fenced node that keeps granting leases
// answers a new connect with Success during its partition.
func TestDefectMinorityServes(t *testing.T) {
	contrast(t, PropMinority, broker.Defects{MinorityServes: true}, func(t *testing.T, c *Cluster) Evidence {
		n3 := c.Node("n3")
		for _, o := range c.Nodes {
			if o != n3 {
				c.Partition(n3, o)
			}
		}
		defer c.HealAll()
		// Probe once the node knows its leases have lapsed, not after a
		// guessed sleep: the fence follows a failed renewal, which takes a
		// proposal timeout of its own.
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			if srv := n3.Server(); srv != nil && srv.Leases() != nil && srv.Leases().Fenced() {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if srv := n3.Server(); srv == nil || srv.Leases() == nil || !srv.Leases().Fenced() {
			t.Fatal("the partitioned node did not fence within the deadline")
		}
		code, ref, err := ProbeConnect(n3.TCP, "probe-minority")
		return Evidence{Ledger: &Ledger{}, Probes: []Probe{{At: time.Now(), Node: n3.Name, Code: code, Ref: ref, Err: err}}}
	})
}

// TestDefectForgetInbound: a QoS 2 PUBLISH re-sent with DUP before the
// exchange completes must not be routed twice.
func TestDefectForgetInbound(t *testing.T) {
	contrast(t, PropNoDuplicate, broker.Defects{ForgetInbound: true}, forgetInboundScenario(nil, nil))
}

// TestForgetInboundScenarioUnderLoad: when the broker cannot confirm a publish
// in time and answers an error PUBREC, the scenario must not retransmit into an
// exchange that has ended. Forward acknowledgements from the subscriber's node
// are held past the acknowledgement timeout for two seconds, so the first
// attempts fail and a later one succeeds; the sound build reports no duplicate
// (WI 1486).
func TestForgetInboundScenarioUnderLoad(t *testing.T) {
	c := New(t, Options{})
	delayForwardAcks := func(c *Cluster) {
		c.Fault(c.Node("n2"), c.Node("n1"), &transport.Fault{Delay: c.AckTimeout + 500*time.Millisecond, Classes: []transport.Class{transport.ForwardedPublish}})
		time.AfterFunc(2*time.Second, func() { c.Fault(c.Node("n2"), c.Node("n1"), nil) })
	}
	var errored []string
	ev := forgetInboundScenario(delayForwardAcks, &errored)(t, c)
	if len(errored) == 0 {
		t.Fatal("no attempt saw an error PUBREC, so the case tested nothing")
	}
	if vs := Check(ev); hasViolation(vs, PropNoDuplicate) {
		t.Fatalf("the sound build under a late forward acknowledgement violated %s: %v", PropNoDuplicate, vs)
	}
}

// forgetInboundScenario publishes a QoS 2 message from a raw client on n1 to a
// subscriber on n2, retransmits it with DUP after a successful PUBREC (as after
// a lost PUBREC), then completes the exchange. afterRoute, when set, runs once
// the route has reached n1; errored, when set, receives the attempts that
// ended with an error PUBREC.
//
// An error PUBREC ends the exchange (MQTT 5 §4.3.3), and the broker may already
// have routed the message it could not confirm. Retransmitting into that ended
// exchange is a new publish, and its second delivery is the protocol working,
// not the defect: so an errored attempt stops there and the scenario tries
// again with a new identifier and sequence number (WI 1486).
func forgetInboundScenario(afterRoute func(*Cluster), erroredOut *[]string) func(t *testing.T, c *Cluster) Evidence {
	return func(t *testing.T, c *Cluster) Evidence {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		sub := NewClient("sub-q2", []string{c.Node("n2").TCP}, 600, 1)
		if err := sub.Run(ctx); err != nil {
			t.Fatal(err)
		}
		defer sub.Close()
		if err := sub.Subscribe(ctx, "q2/t", 2); err != nil {
			t.Fatal(err)
		}
		ledger := &Ledger{}
		nc, err := net.DialTimeout("tcp", c.Node("n1").TCP, 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = nc.Close() }()
		enc := &packet.Encoder{Version: packet.V5}
		dec := packet.NewDecoder(nc)
		dec.Version = packet.V5
		_ = nc.SetDeadline(time.Now().Add(40 * time.Second))
		must := func(p packet.Packet) {
			if err := enc.Encode(nc, p); err != nil {
				t.Fatal(err)
			}
		}
		read := func() packet.Packet {
			p, err := dec.Read()
			if err != nil {
				t.Fatal(err)
			}
			return p
		}
		must(&packet.Connect{ProtocolLevel: packet.V5, ClientID: "raw-pub", CleanStart: true})
		if a, ok := read().(*packet.Connack); !ok || a.ReasonCode != packet.Success {
			t.Fatalf("connack %+v", a)
		}
		// Wait for the route to reach n1.
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if nodes, _ := c.Node("n1").Server().Routes().Table().Match("default", "q2/t"); len(nodes) == 1 {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if afterRoute != nil {
			afterRoute(c)
		}
		// Attempts continue for a time budget rather than a count: under the
		// whole-module gate's contention the harness's 1 s barrier has been
		// seen to fail five times running (WI 1487).
		budget := time.Now().Add(20 * time.Second)
		var errored []string
		accepted := false
		for i := 1; time.Now().Before(budget) && !accepted; i++ {
			pub := &packet.Publish{Topic: "q2/t", QoS: 2, PacketID: uint16(6 + i), Payload: payload("raw-pub", uint64(i))}
			must(pub)
			rec, ok := read().(*packet.Pubrec)
			if !ok {
				t.Fatal("no PUBREC")
			}
			if rec.ReasonCode.IsError() {
				errored = append(errored, fmt.Sprintf("attempt %d: %s", i, rec.ReasonCode))
				t.Logf("PUBREC %s ended attempt %d; not retransmitting into an ended exchange", rec.ReasonCode, i)
				continue
			}
			accepted = true
			pub.Dup = true
			must(pub)
			if rec, ok := read().(*packet.Pubrec); !ok || rec.ReasonCode.IsError() {
				t.Fatalf("the retransmission's PUBREC: %+v", rec)
			}
			must(&packet.Pubrel{Ack: packet.Ack{PacketID: pub.PacketID}})
			if comp, ok := read().(*packet.Pubcomp); !ok || comp.ReasonCode.IsError() {
				t.Fatalf("PUBCOMP: %+v", comp)
			}
		}
		if erroredOut != nil {
			*erroredOut = errored
		}
		if !accepted {
			t.Fatalf("the barrier failed on every attempt, so the run says nothing about the property: %v", errored)
		}
		time.Sleep(time.Second)
		ledger.Deliveries = sub.TakeDeliveries()
		return Evidence{Ledger: ledger}
	}
}

// TestDefectReorderForwards: swapped frames between nodes break the
// per-publisher order at a remote subscriber.
func TestDefectReorderForwards(t *testing.T) {
	contrast(t, PropOrdering, broker.Defects{ReorderForwards: true}, func(t *testing.T, c *Cluster) Evidence {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		sub := NewClient("sub-ord", []string{c.Node("n2").TCP}, 600, 1)
		if err := sub.Run(ctx); err != nil {
			t.Fatal(err)
		}
		defer sub.Close()
		if err := sub.Subscribe(ctx, "ord/t", 1); err != nil {
			t.Fatal(err)
		}
		pub := NewClient("pub-ord", []string{c.Node("n1").TCP}, 600, 2)
		if err := pub.Run(ctx); err != nil {
			t.Fatal(err)
		}
		defer pub.Close()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if nodes, _ := c.Node("n1").Server().Routes().Table().Match("default", "ord/t"); len(nodes) == 1 {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		ledger := &Ledger{}
		// QoS 0 publishes in a burst so adjacent frames queue on the link.
		for i := uint64(1); i <= 400; i++ {
			if _, err := pub.Publish(ctx, "ord/t", 0, i); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(2 * time.Second)
		ledger.Deliveries = sub.TakeDeliveries()
		return Evidence{Ledger: ledger}
	})
}

// TestDefectNoBarrier: acknowledging after the local sync only loses
// acknowledged messages when the owner dies before its followers hold them.
func TestDefectNoBarrier(t *testing.T) {
	contrast(t, PropNoAckedLoss, broker.Defects{NoBarrier: true}, func(t *testing.T, c *Cluster) Evidence {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		n1 := c.Node("n1")
		others := []string{c.Node("n2").TCP, c.Node("n3").TCP}
		sub := NewClient("sub-loss", append([]string{n1.TCP}, others...), 600, 1)
		if err := sub.Run(ctx); err != nil {
			t.Fatal(err)
		}
		if err := sub.Subscribe(ctx, "loss/t", 1); err != nil {
			t.Fatal(err)
		}
		ledger := &Ledger{}
		ledger.Subscriptions = []Subscription{{Client: sub.ID, Filter: "loss/t", QoS: 1, Since: time.Now()}}
		// The subscriber goes offline so the messages queue on n1.
		sub.Close()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if rec, ok := n1.Server().Sessions().Record("sub-loss"); ok && !rec.Online {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		// Every replication frame n1 writes is dropped, so its followers
		// never hold what it acknowledges. Only that class is faulted:
		// heartbeats, lease proposals and route updates still flow, so n1
		// stays a member and keeps serving its clients.
		lost := &transport.Fault{Drop: 1, Classes: []transport.Class{transport.ReplicationAppend}, Rand: rand.New(rand.NewSource(7))} //nolint:gosec // seeded simulation
		for _, o := range c.Nodes {
			if o != n1 {
				c.Fault(n1, o, lost)
			}
		}
		pub := NewClient("pub-loss", []string{n1.TCP}, 600, 3)
		if err := pub.Run(ctx); err != nil {
			t.Fatal(err)
		}
		acked := 0
		for i := uint64(1); i <= 8; i++ {
			pctx, pcancel := context.WithTimeout(ctx, 5*time.Second)
			code, err := pub.Publish(pctx, "loss/t", 1, i)
			pcancel()
			if err != nil {
				ledger.addErr(PublishFailure{Client: pub.ID, Node: nodeNameAt(c, pub), Seq: i, Code: code, Err: err.Error()})
				continue
			}
			acked++
			ledger.addPub(Publication{Publisher: pub.ID, Topic: "loss/t", QoS: 1, Seq: i, Code: code, AckedAt: time.Now()})
		}
		pub.Close()
		c.Crash(n1)
		t.Logf("acknowledged %d of 8 before the crash", acked)
		// The leader adopts the lease and imports what the followers hold;
		// the subscriber comes back elsewhere.
		time.Sleep(c.o.TTL + time.Second)
		sctx, scancel := context.WithTimeout(ctx, 30*time.Second)
		sub2 := NewClient("sub-loss", others, 600, 4)
		err := sub2.Run(sctx)
		scancel()
		if err != nil {
			t.Fatalf("the subscriber did not come back on a survivor: %v", err)
		}
		time.Sleep(3 * time.Second)
		ledger.Deliveries = sub2.TakeDeliveries()
		sub2.Close()
		c.Start(n1)
		return Evidence{Ledger: ledger}
	})
}

// TestDefectIgnoreChecksum: a node must refuse to start over a corrupted
// record.
func TestDefectIgnoreChecksum(t *testing.T) {
	contrast(t, PropFailClosed, broker.Defects{IgnoreChecksum: true}, func(t *testing.T, c *Cluster) Evidence {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		n1 := c.Node("n1")
		cl := NewClient("writer", []string{n1.TCP}, 600, 5)
		if err := cl.Run(ctx); err != nil {
			t.Fatal(err)
		}
		if err := cl.Subscribe(ctx, "w/t", 1); err != nil {
			t.Fatal(err)
		}
		for i := uint64(1); i <= 5; i++ {
			if _, err := cl.Publish(ctx, "w/t", 1, i); err != nil {
				t.Fatal(err)
			}
		}
		cl.Close()
		c.Crash(n1)
		if err := c.CorruptRecord(n1, StateLog); err != nil {
			t.Fatal(err)
		}
		err := c.TryStart(n1)
		t.Logf("restart over the corrupted state log: %v", err)
		fc := FailClosed{Node: n1.Name, Log: StateLog, Refused: err != nil, Err: err}
		if err != nil {
			_ = c.TruncateTail(n1, StateLog, 9)
			c.Start(n1)
		}
		return Evidence{Ledger: &Ledger{}, FailClosed: []FailClosed{fc}}
	})
}

// TestDefectPubrelUnreplicated: exactly-once across an owner loss. The
// owner's replication is cut, so it cannot tell its followers that the
// subscriber has the messages. Sound, it holds the PUBREL, the client never
// completes, and the rebuilt session re-sends under the identifier the
// replica already carried, so the subscriber sees one delivery. With the defect,
// the PUBREL goes out anyway, the client completes and releases the
// identifier, and the rebuild hands the application the message again.
func TestDefectPubrelUnreplicated(t *testing.T) {
	contrast(t, PropNoDuplicate, broker.Defects{PubrelUnreplicated: true}, func(t *testing.T, c *Cluster) Evidence {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		n1 := c.Node("n1")
		// The subscription is made and then left offline, so the messages
		// queue on n1 and replicate while the cluster is still healthy.
		setup := NewClient("sub-x2", []string{n1.TCP}, 600, 11)
		if err := setup.Run(ctx); err != nil {
			t.Fatal(err)
		}
		if err := setup.Subscribe(ctx, "x2/t", 2); err != nil {
			t.Fatal(err)
		}
		setup.Close()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if rec, ok := n1.Server().Sessions().Record("sub-x2"); ok && !rec.Online {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		ledger := &Ledger{}
		pub := NewClient("pub-x2", []string{n1.TCP}, 600, 12)
		if err := pub.Run(ctx); err != nil {
			t.Fatal(err)
		}
		for i := uint64(1); i <= 4; i++ {
			pctx, pcancel := context.WithTimeout(ctx, 10*time.Second)
			if _, err := pub.Publish(pctx, "x2/t", 2, i); err != nil {
				t.Fatalf("publish %d: %v", i, err)
			}
			pcancel()
		}
		pub.Close()
		// From here n1 can tell its followers nothing.
		lost := &transport.Fault{Drop: 1, Classes: []transport.Class{transport.ReplicationAppend}, Rand: rand.New(rand.NewSource(12))} //nolint:gosec // seeded simulation
		for _, o := range c.Nodes {
			if o != n1 {
				c.Fault(n1, o, lost)
			}
		}
		// One client for the rest of the scenario: it drains the queue on
		// n1, and when n1 dies it re-homes itself, carrying the exchange
		// state a real client would carry.
		sub := NewClient("sub-x2", c.Addresses(), 600, 13)
		if err := sub.Run(ctx); err != nil {
			t.Fatal(err)
		}
		defer sub.Close()
		time.Sleep(3 * time.Second)
		c.Crash(n1)
		time.Sleep(c.o.TTL + 6*time.Second)
		online := time.Now().Add(30 * time.Second)
		for time.Now().Before(online) && !sub.Online() {
			time.Sleep(100 * time.Millisecond)
		}
		time.Sleep(4 * time.Second)
		ledger.Deliveries = sub.TakeDeliveries()
		c.Start(n1)
		return Evidence{Ledger: ledger}
	})
}

// TestDefectForwardGap: a forwarded publish frame lost with the link up must
// fail the publish that produced it. The receiver keeps the sender's sequence
// and reports a gap before acknowledging the frame that revealed it; with
// that check off, the sender's cumulative acknowledgement covers the lost
// frame and its publish is acknowledged as delivered nowhere.
func TestDefectForwardGap(t *testing.T) {
	contrast(t, PropNoAckedLoss, broker.Defects{IgnoreForwardGaps: true}, func(t *testing.T, c *Cluster) Evidence {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		n1, n2 := c.Node("n1"), c.Node("n2")
		sub := NewClient("sub-gap", []string{n2.TCP}, 600, 1)
		if err := sub.Run(ctx); err != nil {
			t.Fatal(err)
		}
		defer sub.Close()
		if err := sub.Subscribe(ctx, "gap/t", 1); err != nil {
			t.Fatal(err)
		}
		ledger := &Ledger{}
		ledger.Subscriptions = []Subscription{{Client: sub.ID, Filter: "gap/t", QoS: 1, Since: time.Now()}}
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if nodes, _ := n1.Server().Routes().Table().Match("default", "gap/t"); len(nodes) == 1 {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		// A third of the forwarded frames from n1 to n2 vanish on the wire;
		// every other class still flows, so the link stays up. Several
		// publishers at once, so their frames interleave on the stream: a
		// lost frame is then covered by a later publisher's acknowledgement,
		// which is the hole a cumulative acknowledgement has.
		c.Fault(n1, n2, &transport.Fault{Drop: 0.3, Classes: []transport.Class{transport.ForwardedPublish}, Rand: rand.New(rand.NewSource(9))}) //nolint:gosec // seeded simulation
		var wg sync.WaitGroup
		for p := 0; p < 6; p++ {
			pub := NewClient(fmt.Sprintf("pub-gap-%d", p), []string{n1.TCP}, 600, int64(2+p))
			if err := pub.Run(ctx); err != nil {
				t.Fatal(err)
			}
			defer pub.Close()
			wg.Add(1)
			go func(pub *Client) {
				defer wg.Done()
				for i := uint64(1); i <= 15; i++ {
					pctx, pcancel := context.WithTimeout(ctx, 5*time.Second)
					code, err := pub.Publish(pctx, "gap/t", 1, i)
					pcancel()
					if err != nil {
						ledger.addErr(PublishFailure{Client: pub.ID, Node: nodeNameAt(c, pub), Seq: i, Code: code, Err: err.Error()})
						continue
					}
					ledger.addPub(Publication{Publisher: pub.ID, Topic: "gap/t", QoS: 1, Seq: i, Code: code, AckedAt: time.Now()})
				}
			}(pub)
		}
		wg.Wait()
		c.Fault(n1, n2, nil)
		time.Sleep(2 * time.Second)
		ledger.Deliveries = sub.TakeDeliveries()
		return Evidence{Ledger: ledger}
	})
}

// TestDefectReplicationGap is TestDefectNoBarrier's scenario with half the
// owner's replication frames lost on the wire instead of all of them: the
// follower reports the gaps and the owner resyncs or refuses; with the check
// off, the owner acknowledges publishes its followers never held, and the
// takeover after its crash loses them.
func TestDefectReplicationGap(t *testing.T) {
	contrast(t, PropNoAckedLoss, broker.Defects{IgnoreReplicationGaps: true}, func(t *testing.T, c *Cluster) Evidence {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		n1 := c.Node("n1")
		others := []string{c.Node("n2").TCP, c.Node("n3").TCP}
		sub := NewClient("sub-rgap", append([]string{n1.TCP}, others...), 600, 1)
		if err := sub.Run(ctx); err != nil {
			t.Fatal(err)
		}
		if err := sub.Subscribe(ctx, "rgap/t", 1); err != nil {
			t.Fatal(err)
		}
		ledger := &Ledger{}
		ledger.Subscriptions = []Subscription{{Client: sub.ID, Filter: "rgap/t", QoS: 1, Since: time.Now()}}
		sub.Close()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if rec, ok := n1.Server().Sessions().Record("sub-rgap"); ok && !rec.Online {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		lost := &transport.Fault{Drop: 0.5, Classes: []transport.Class{transport.ReplicationAppend}, Rand: rand.New(rand.NewSource(11))} //nolint:gosec // seeded simulation
		for _, o := range c.Nodes {
			if o != n1 {
				c.Fault(n1, o, lost)
			}
		}
		pub := NewClient("pub-rgap", []string{n1.TCP}, 600, 3)
		if err := pub.Run(ctx); err != nil {
			t.Fatal(err)
		}
		acked := 0
		for i := uint64(1); i <= 24; i++ {
			pctx, pcancel := context.WithTimeout(ctx, 5*time.Second)
			code, err := pub.Publish(pctx, "rgap/t", 1, i)
			pcancel()
			if err != nil {
				ledger.addErr(PublishFailure{Client: pub.ID, Node: nodeNameAt(c, pub), Seq: i, Code: code, Err: err.Error()})
				continue
			}
			acked++
			ledger.addPub(Publication{Publisher: pub.ID, Topic: "rgap/t", QoS: 1, Seq: i, Code: code, AckedAt: time.Now()})
		}
		pub.Close()
		// The injected loss is the drop, not a link that stays lossy: the
		// fault is lifted and the repairs the gap reports started are given
		// time to complete before the owner dies. A resync is a delete then
		// records; under a drop that never lifts, a resync's own frames are
		// lost and a follower can end up holding less than it did.
		for _, o := range c.Nodes {
			if o != n1 {
				c.Fault(n1, o, nil)
			}
		}
		time.Sleep(3 * time.Second)
		c.Crash(n1)
		t.Logf("acknowledged %d of 24 before the crash", acked)
		time.Sleep(c.o.TTL + time.Second)
		sctx, scancel := context.WithTimeout(ctx, 30*time.Second)
		sub2 := NewClient("sub-rgap", others, 600, 4)
		err := sub2.Run(sctx)
		scancel()
		if err != nil {
			t.Fatalf("the subscriber did not come back on a survivor: %v", err)
		}
		time.Sleep(3 * time.Second)
		ledger.Deliveries = sub2.TakeDeliveries()
		sub2.Close()
		c.Start(n1)
		return Evidence{Ledger: ledger}
	})
}

// TestDefectHandoverDropped: a node that stops holding a session's replica on
// a map change hands its copy to the new holder and lets go only once the new
// holder acknowledged it. With the hand-over frames lost and the owner dead
// before it writes again, the copy the old holder kept is what a takeover
// finds; with the seam on it deleted the copy, and the session is lost.
func TestDefectHandoverDropped(t *testing.T) {
	contrast(t, PropNoAckedLoss, broker.Defects{DeleteWithoutHandoverAck: true}, func(t *testing.T, c *Cluster) Evidence {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		n1, n2, n3 := c.Node("n1"), c.Node("n2"), c.Node("n3")
		sub := NewClient("sub-ho", []string{n1.TCP}, 600, 1)
		if err := sub.Run(ctx); err != nil {
			t.Fatal(err)
		}
		if err := sub.Subscribe(ctx, "ho/t", 1); err != nil {
			t.Fatal(err)
		}
		ledger := &Ledger{}
		ledger.Subscriptions = []Subscription{{Client: sub.ID, Filter: "ho/t", QoS: 1, Since: time.Now()}}
		sub.Close()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if rec, ok := n1.Server().Sessions().Record("sub-ho"); ok && !rec.Online {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		// Messages queue on n1 and reach its follower(s).
		pub := NewClient("pub-ho", []string{n1.TCP}, 600, 2)
		if err := pub.Run(ctx); err != nil {
			t.Fatal(err)
		}
		for i := uint64(1); i <= 6; i++ {
			pctx, pcancel := context.WithTimeout(ctx, 5*time.Second)
			code, err := pub.Publish(pctx, "ho/t", 1, i)
			pcancel()
			if err != nil {
				ledger.addErr(PublishFailure{Client: pub.ID, Node: nodeNameAt(c, pub), Seq: i, Code: code, Err: err.Error()})
				continue
			}
			ledger.addPub(Publication{Publisher: pub.ID, Topic: "ho/t", QoS: 1, Seq: i, Code: code, AckedAt: time.Now()})
		}
		pub.Close()
		// The session's replica lives on the holder of its shard that is not
		// the owner (shard leadership is placement, not ownership). That
		// holder leaves and returns: while it is gone the map puts the copy
		// on the third node, and on its return the third node hands the copy
		// back to it and lets go. Every replication frame to the returning
		// node is lost on the wire, so no hand-over — the owner's or the old
		// holder's — is acknowledged.
		rep := n1.Server().Replication()
		shard := rep.Map().ShardOf("default", "sub-ho")
		leaving, other := n3, n2
		for _, h := range rep.Map().Current().Holders(shard) {
			if h == n2.Name {
				leaving, other = n2, n3
			}
		}
		t.Logf("shard %d holders %v: %s leaves and returns, %s hands over", shard, rep.Map().Current().Holders(shard), leaving.Name, other.Name)
		c.Crash(leaving)
		time.Sleep(c.o.TTL + 2*time.Second)
		lost := &transport.Fault{Drop: 1, Classes: []transport.Class{transport.ReplicationAppend}, Rand: rand.New(rand.NewSource(5))} //nolint:gosec // seeded simulation
		// The returning node comes back with its replica log gone, so the
		// copy it held before it left is not there to be found: what it
		// holds after this is what the hand-over gave it.
		if err := os.RemoveAll(leaving.logDir(ReplicaLog)); err != nil {
			t.Fatal(err)
		}
		c.Start(leaving)
		c.Fault(n1, leaving, lost)
		c.Fault(other, leaving, lost)
		// The hand-over happens when the map puts the returning node back on
		// the shard, and that waits on its peers redialling it — up to their
		// dialer's backoff after several seconds down. The scenario means
		// nothing unless the map moved while the owner still lived, so the
		// return is awaited, not slept for, and the hand-over is then given
		// its acknowledgement timeout to fail.
		waitHolds(t, c, shard, leaving, 20*time.Second)
		time.Sleep(2*c.AckTimeout + 500*time.Millisecond)
		c.Fault(n1, leaving, nil)
		c.Fault(other, leaving, nil)
		// The owner dies before it writes the session again.
		c.Crash(n1)
		time.Sleep(c.o.TTL + time.Second)
		sctx, scancel := context.WithTimeout(ctx, 30*time.Second)
		sub2 := NewClient("sub-ho", []string{leaving.TCP, other.TCP}, 600, 4)
		err := sub2.Run(sctx)
		scancel()
		if err != nil {
			t.Fatalf("the subscriber did not come back on a survivor: %v", err)
		}
		time.Sleep(3 * time.Second)
		ledger.Deliveries = sub2.TakeDeliveries()
		sub2.Close()
		c.Start(n1)
		return Evidence{Ledger: ledger}
	})
}

// waitHolds waits until every running node has applied and reconciled a map
// under which node holds the shard, failing with each node's view when the
// bound passes.
func waitHolds(t *testing.T, c *Cluster, shard int, node *Node, bound time.Duration) {
	t.Helper()
	deadline := time.Now().Add(bound)
	for {
		views := ""
		holds := true
		for _, n := range c.Nodes {
			srv := n.Server()
			if srv == nil {
				continue
			}
			rep := srv.Replication()
			m := rep.Map().Current()
			recovering := srv.Consensus().Recovering()
			views += fmt.Sprintf(" %s: v%d holders %v reconciled v%d recovering %v;", n.Name, m.Version, m.Holders(shard), rep.Reconciled(), recovering)
			if recovering || !m.Holds(shard, node.Name) || rep.Reconciled() != m.Version {
				holds = false
			}
		}
		if holds {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s does not hold shard %d on every node within %s:%s", node.Name, shard, bound, views)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestDefectHandOverOwnedCopies: an owner that took a session over keeps a
// replica copy of it from the previous owner. With the seam on, a later map
// change hands that copy over as the owner's own whole write, replacing the
// copy the holder had been fed with one from before the takeover: after the
// owner dies the takeover resends QoS 2 messages the subscriber completed
// long ago, and loses what was queued since.
func TestDefectHandOverOwnedCopies(t *testing.T) {
	contrastOpts(t, PropNoDuplicate, broker.Defects{HandOverOwnedCopies: true}, broker.Defects{}, Options{Nodes: 4}, staleCopyScenario(false))
}

// TestDefectUnappliedAcknowledged: the same stale copy in both halves; the
// subscriber's reconnect then sends what was queued since, and the records
// for it change nothing on the stale copy. The sound build refuses the first
// of them and the owner repairs the key from its live session, so nothing
// stale survives the owner's death; the defective build acknowledges them
// and the stale copy stands.
func TestDefectUnappliedAcknowledged(t *testing.T) {
	contrastOpts(t, PropNoDuplicate, broker.Defects{HandOverOwnedCopies: true, AcknowledgeUnapplied: true}, broker.Defects{HandOverOwnedCopies: true}, Options{Nodes: 4}, staleCopyScenario(true))
}

// staleCopyScenario: n1 owns a durable subscriber whose shard n2 holds a
// copy of; the subscriber reconnects on n2 (a live takeover), which — with
// the seam — keeps the copy it replicated; more messages are queued while it
// is away; the map then moves the shard off n2, so its stale copy is handed
// over as the owner's own write and replaces the copy the holder was being
// fed. With touch, the subscriber reconnects once more before n2 dies, so
// the owner sends the queued messages and the records for them reach a copy
// that does not hold them. Finally n2 dies and the subscriber returns on a
// survivor, which rebuilds the session from the copy.
//
// The cluster has four nodes so that n4 — which holds none of the client's
// shard in any of the maps below — is the one that comes and goes. A holder
// that restarts comes back with a gap in its stream, and the resync that
// repairs a gap repairs the stale copy with it, which is what made the
// earlier three-node form of this scenario decide differently from run to
// run.
func staleCopyScenario(touch bool) func(t *testing.T, c *Cluster) Evidence {
	return func(t *testing.T, c *Cluster) Evidence {
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()
		n1, n2, n3, n4 := c.Node("n1"), c.Node("n2"), c.Node("n3"), c.Node("n4")
		rep := n1.Server().Replication()
		// The two maps the scenario passes through, computed here so the
		// client can be chosen by what they say: n4 away and n4 back.
		shards, replicas := rep.Map().Count(), rep.Map().Current().Replicas
		away := shardmap.Compute(0, []string{n1.Name, n2.Name, n3.Name}, replicas, shards)
		back := shardmap.Compute(0, []string{n1.Name, n2.Name, n3.Name, n4.Name}, replicas, shards)
		id := ""
		for i := 0; i < 256 && id == ""; i++ {
			cand := fmt.Sprintf("sub-stale-%d", i)
			s := rep.Map().ShardOf("default", cand)
			// While n4 is away the shard is held by n2 and n3, and its
			// owner n1 holds none of it, so the copy n2 takes is the one the
			// session has.
			if h := away.Holders(s); len(h) != 2 || h[0] != n2.Name || h[1] != n3.Name {
				continue
			}
			// When n4 returns n2 stops holding the shard, so its copy is
			// handed over. Which holder the takeover then reads no longer
			// decides the outcome: a holder the owner replicates nothing to
			// has no copy to be read (WI 1569), so the screen that pinned
			// this scenario to the fed holder is gone.
			if back.Holds(s, n2.Name) {
				continue
			}
			id = cand
		}
		if id == "" {
			t.Fatal("no client id maps to a shard held by n2 and n3 while n4 is away and fed through n3 after it returns")
		}
		shard := rep.Map().ShardOf("default", id)
		c.Crash(n4)
		waitHolds(t, c, shard, n2, 20*time.Second)
		ledger := &Ledger{}
		sub := NewClient(id, []string{n1.TCP}, 600, 1)
		if err := sub.Run(ctx); err != nil {
			t.Fatal(err)
		}
		if err := sub.Subscribe(ctx, "stale/t", 2); err != nil {
			t.Fatal(err)
		}
		ledger.Subscriptions = []Subscription{{Client: sub.ID, Filter: "stale/t", QoS: 2, Since: time.Now()}}
		sub.Close()
		waitOffline(t, n1, id)
		pub := NewClient("pub-stale", []string{n1.TCP}, 600, 2)
		if err := pub.Run(ctx); err != nil {
			t.Fatal(err)
		}
		publish := func(from, to uint64, qos byte) {
			for i := from; i <= to; i++ {
				pctx, pcancel := context.WithTimeout(ctx, 5*time.Second)
				code, err := pub.Publish(pctx, "stale/t", qos, i)
				pcancel()
				if err != nil {
					ledger.addErr(PublishFailure{Client: pub.ID, Node: nodeNameAt(c, pub), Seq: i, Code: code, Err: err.Error()})
					continue
				}
				ledger.addPub(Publication{Publisher: pub.ID, Topic: "stale/t", QoS: qos, Seq: i, Code: code, AckedAt: time.Now()})
			}
		}
		// Queued on n1, copied to n2: what n2 holds at the takeover, and
		// what its stale copy will still hold at the end. The copy is the
		// scenario's premise, so it is waited for rather than assumed.
		publish(1, 3, 2)
		waitReplicaCopy(t, n2, id, 3, 15*time.Second)
		// The takeover: n2 owns the session, delivers the three, and — with
		// the seam — keeps the copy it replicated.
		sub2 := NewClient(id, []string{n2.TCP}, 600, 3)
		if err := sub2.Run(ctx); err != nil {
			t.Fatal(err)
		}
		// The three reach the subscriber before the scenario moves on: they
		// are what the stale copy will later hand out a second time. (They do
		// not always complete — the takeover holds PUBREL until the PUBREC
		// state is on the replicas — and the scenario does not need them to.)
		if got := drainInto(ledger, sub2, 3, 30*time.Second); got < 3 {
			t.Fatalf("the takeover delivered %d of the 3 queued messages", got)
		}
		sub2.Close()
		waitOffline(t, n2, id)
		// The copy n2 kept is the premise of everything below, and keeping it
		// is a race the harness cannot pin: the old owner deletes its session
		// at the takeover, and that delete is applied here when n2's own
		// replication lease view is still at the old epoch (traced: a session
		// delete from n1 at epoch 1, a millisecond after the takeover). The
		// copy is then gone and there is nothing for the map change to hand
		// over, so the run cannot exercise what this control is for.
		if c.o.Defects.HandOverOwnedCopies && replicaCopy(n2, id) == nil {
			t.Skipf("the copy n2 kept of %s was deleted by the old owner at the takeover; this run cannot exercise the hand-over", id)
		}
		// Queued on n2 while the subscriber is away; n3's copy holds them,
		// the copy n2 kept does not.
		publish(4, 6, 1)
		// n4 returns: the map moves the shard to n3 and n4, n2 stops holding
		// it, and with the seam hands its stale copy over as its own write.
		c.Start(n4)
		waitHolds(t, c, shard, n4, 20*time.Second)
		// The copy is handed over, or dropped in its place; either way the
		// receivers are given the acknowledgement timeout to apply what n2
		// sent before the scenario reads the result.
		settleHandOver(n2, id, 15*time.Second)
		time.Sleep(2*c.AckTimeout + 500*time.Millisecond)
		if touch {
			// The subscriber's reconnect makes the owner send the queued
			// messages: records for entries the stale copy does not hold.
			sub3 := NewClient(id, []string{n2.TCP}, 600, 4)
			if err := sub3.Run(ctx); err != nil {
				t.Fatal(err)
			}
			if got := drainInto(ledger, sub3, 3, 30*time.Second); got < 3 {
				t.Fatalf("the reconnect was sent %d of the 3 messages queued since", got)
			}
			waitQueue(t, n2, id, 0, 30*time.Second)
			sub3.Close()
			waitOffline(t, n2, id)
			time.Sleep(2 * c.AckTimeout)
		}
		pub.Close()
		c.Crash(n2)
		time.Sleep(c.o.TTL + time.Second)
		sctx, scancel := context.WithTimeout(ctx, 30*time.Second)
		sub4 := NewClient(id, []string{n3.TCP, n1.TCP}, 600, 5)
		err := sub4.Run(sctx)
		scancel()
		if err != nil {
			t.Fatalf("the subscriber did not come back on a survivor: %v", err)
		}
		// The duplicates the stale copy holds arrive on this connection; the
		// wait ends early when they do and runs out when there are none.
		drainInto(ledger, sub4, 3, 8*time.Second)
		sub4.Close()
		c.Start(n2)
		return Evidence{Ledger: ledger}
	}
}

// TestDefectKeepUnfedCopies: a holder the owner replicates nothing to keeps
// the copy it last held, and a takeover after the owner's death reads it —
// handing the subscriber messages it completed before the map moved. With
// the rule in, that holder has no copy and the takeover reads the one the
// owner's stream maintained.
func TestDefectKeepUnfedCopies(t *testing.T) {
	// The adoption worker is held back so the takeover this control is about
	// is the one the returning subscriber drives, after the map has dropped
	// the dead owner: an adoption that ran first would rebuild the session
	// from wherever the map pointed while the owner was still in its basis.
	contrastOpts(t, PropNoDuplicate, broker.Defects{KeepUnfedCopies: true}, broker.Defects{}, Options{Nodes: 4, AdoptionDelay: 20 * time.Second}, unfedCopyScenario())
}

// unfedCopyScenario: the subscriber's owner is n3, which holds none of its
// shard, so exactly one holder is replicated to. While n4 is away that holder
// is n1, which takes the three queued messages; when n4 returns the map makes
// it the target instead, and n1 — still a holder — is fed nothing more. The
// subscriber then comes back on n3 and completes the three, so only the
// maintained copy knows they are gone. n3 dies, the map puts the shard's
// first holder at n1, and the takeover reads whatever n1 has.
func unfedCopyScenario() func(t *testing.T, c *Cluster) Evidence {
	return func(t *testing.T, c *Cluster) Evidence {
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()
		n1, n2, n3, n4 := c.Node("n1"), c.Node("n2"), c.Node("n3"), c.Node("n4")
		rep := n1.Server().Replication()
		shards, replicas := rep.Map().Count(), rep.Map().Current().Replicas
		away := shardmap.Compute(0, []string{n1.Name, n2.Name, n3.Name}, replicas, shards)
		back := shardmap.Compute(0, []string{n1.Name, n2.Name, n3.Name, n4.Name}, replicas, shards)
		gone := shardmap.Compute(0, []string{n1.Name, n2.Name, n4.Name}, replicas, shards)
		fedBy := func(m shardmap.Map, s int) string {
			t := m.Targets(s, n3.Name)
			if len(t) != 1 {
				return ""
			}
			return t[0]
		}
		id := ""
		for i := 0; i < 256 && id == ""; i++ {
			cand := fmt.Sprintf("sub-unfed-%d", i)
			s := rep.Map().ShardOf("default", cand)
			// n3 owns the session and holds none of its shard, so one holder
			// is fed and the other is not, throughout.
			if away.Holds(s, n3.Name) || back.Holds(s, n3.Name) {
				continue
			}
			// n1 is fed while n4 is away, stops being fed when it returns
			// without ceasing to hold the shard, and is what the takeover
			// reads once n3 is gone.
			if fedBy(away, s) != n1.Name || !back.Holds(s, n1.Name) || fedBy(back, s) == n1.Name || fedBy(gone, s) != n1.Name {
				continue
			}
			id = cand
		}
		if id == "" {
			t.Fatal("no client id maps to a shard fed through n1 while n4 is away, held but unfed when it returns, and read from n1 once n3 is gone")
		}
		shard := rep.Map().ShardOf("default", id)
		c.Crash(n4)
		waitHolds(t, c, shard, n1, 20*time.Second)
		ledger := &Ledger{}
		sub := NewClient(id, []string{n3.TCP}, 600, 1)
		if err := sub.Run(ctx); err != nil {
			t.Fatal(err)
		}
		if err := sub.Subscribe(ctx, "unfed/t", 2); err != nil {
			t.Fatal(err)
		}
		ledger.Subscriptions = []Subscription{{Client: sub.ID, Filter: "unfed/t", QoS: 2, Since: time.Now()}}
		sub.Close()
		waitOffline(t, n3, id)
		pub := NewClient("pub-unfed", []string{n3.TCP}, 600, 2)
		if err := pub.Run(ctx); err != nil {
			t.Fatal(err)
		}
		for i := uint64(1); i <= 3; i++ {
			pctx, pcancel := context.WithTimeout(ctx, 5*time.Second)
			code, err := pub.Publish(pctx, "unfed/t", 2, i)
			pcancel()
			if err != nil {
				ledger.addErr(PublishFailure{Client: pub.ID, Node: nodeNameAt(c, pub), Seq: i, Code: code, Err: err.Error()})
				continue
			}
			ledger.addPub(Publication{Publisher: pub.ID, Topic: "unfed/t", QoS: 2, Seq: i, Code: code, AckedAt: time.Now()})
		}
		// n1 is the holder the owner feeds, and the copy it takes here is the
		// one that will go stale: the scenario's premise, waited for.
		waitReplicaCopy(t, n1, id, 3, 15*time.Second)
		// n4 returns and becomes the target; n1 keeps holding the shard and
		// is sent nothing more.
		c.Start(n4)
		waitHolds(t, c, shard, n4, 20*time.Second)
		time.Sleep(2*c.AckTimeout + 500*time.Millisecond)
		// What the halves differ in, before anything else happens: the holder
		// the owner no longer feeds either keeps the copy it last held or has
		// none. The scenario waits for that rather than assuming it.
		if c.o.Defects.KeepUnfedCopies {
			waitReplicaCopy(t, n1, id, 3, 15*time.Second)
		} else {
			settleHandOver(n1, id, 15*time.Second)
			if ss := replicaCopy(n1, id); ss != nil {
				t.Fatalf("n1 still holds %d entries for %s though the owner feeds n4", len(ss.Entries), id)
			}
		}
		// The subscriber takes the three and completes them, on its owner.
		sub2 := NewClient(id, []string{n3.TCP}, 600, 3)
		if err := sub2.Run(ctx); err != nil {
			t.Fatal(err)
		}
		if got := drainInto(ledger, sub2, 3, 30*time.Second); got < 3 {
			t.Fatalf("the subscriber was given %d of the 3 queued messages", got)
		}
		waitQueue(t, n3, id, 0, 30*time.Second)
		sub2.Close()
		waitOffline(t, n3, id)
		pub.Close()
		time.Sleep(2 * c.AckTimeout)
		// The owner dies; the map's first holder for the shard is n1.
		c.Crash(n3)
		// Two things must be true when the subscriber returns, and they are
		// true in this order for about a second: the map has dropped the dead
		// owner, so the takeover looks at n1 rather than at the holder the
		// owner was feeding; and the lease has not yet expired, so no
		// adoption has claimed it for n1 — which would drop the copy this
		// control is about, since an owner holds no copy of its own session
		// (WI 1293). So the subscriber returns as soon as the map has moved,
		// without waiting out the lease.
		waitBasisWithout(t, c, n3.Name, 30*time.Second)
		sctx, scancel := context.WithTimeout(ctx, 30*time.Second)
		// The subscriber returns on n1 — the holder whose copy this is about,
		// and the node the lease table hands the dead owner's lease to.
		sub3 := NewClient(id, []string{n1.TCP}, 600, 4)
		err := sub3.Run(sctx)
		scancel()
		if err != nil {
			t.Fatalf("the subscriber did not come back on a survivor: %v", err)
		}
		drainInto(ledger, sub3, 3, 8*time.Second)
		sub3.Close()
		c.Start(n3)
		return Evidence{Ledger: ledger}
	}
}

// waitBasisWithout waits until every running node's shard map is computed
// from a basis that no longer contains the named node — which is what decides
// where a takeover looks for the session of an owner that has just died.
func waitBasisWithout(t *testing.T, c *Cluster, name string, bound time.Duration) {
	t.Helper()
	deadline := time.Now().Add(bound)
	for {
		seen, all := "", true
		for _, n := range c.Running() {
			srv := n.Server()
			if srv == nil {
				continue
			}
			m := srv.Replication().Map().Current()
			seen += fmt.Sprintf(" %s: v%d %v;", n.Name, m.Version, m.Basis)
			for _, b := range m.Basis {
				if b == name {
					all = false
				}
			}
		}
		if all {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the map still names %s in its basis after %s:%s", name, bound, seen)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// replicaCopy is the node's replica copy of a client's session, or nil when
// it holds none.
func replicaCopy(n *Node, id string) *store.SessionState {
	srv := n.Server()
	if srv == nil || srv.Replication() == nil {
		return nil
	}
	st := srv.Replication().ReplicaState()
	if st == nil {
		return nil
	}
	for _, ss := range st.Sessions {
		if ss.Namespace == "default" && ss.ClientID == id {
			return ss
		}
	}
	return nil
}

// waitReplicaCopy waits until the node's replica copy of the session holds at
// least that many entries. A scenario whose premise is a copy that goes stale
// fails here rather than running on without one.
func waitReplicaCopy(t *testing.T, n *Node, id string, entries int, bound time.Duration) {
	t.Helper()
	deadline := time.Now().Add(bound)
	held := -1
	for {
		if ss := replicaCopy(n, id); ss != nil {
			held = len(ss.Entries)
			if held >= entries {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s does not hold a replica copy of %s with %d entries within %s: held %d", n.Name, id, entries, bound, held)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// settleHandOver gives a map change's hand-over, or the drop that replaces it,
// time to finish: it returns as soon as the node holds no copy of the session,
// and otherwise waits out the bound. It does not require the copy to be gone —
// a hand-over its holders do not acknowledge in time is kept by design
// (WI 1289) — only that the scenario does not race it.
func settleHandOver(n *Node, id string, bound time.Duration) {
	deadline := time.Now().Add(bound)
	for replicaCopy(n, id) != nil && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
}

// ownerQueue is how many entries the node's own session holds for the client,
// or -1 when it holds no session for it.
func ownerQueue(n *Node, id string) int {
	srv := n.Server()
	if srv == nil {
		return -1
	}
	ss, ok := srv.Sessions().Export("default", id)
	if !ok || ss == nil {
		return -1
	}
	return len(ss.Entries)
}

// waitQueue waits until the owner's session for the client holds that many
// entries: how a scenario sees that a subscriber finished with what was queued
// for it, rather than sleeping for as long as that usually takes.
func waitQueue(t *testing.T, n *Node, id string, want int, bound time.Duration) {
	t.Helper()
	deadline := time.Now().Add(bound)
	for {
		held := ownerQueue(n, id)
		if held == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s holds %d queued messages for %s after %s, want %d", n.Name, held, id, bound, want)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// drainInto moves a client's deliveries and events into the ledger and reports
// how many deliveries it took, returning as soon as want of them have arrived
// and otherwise waiting out the bound — so a half that expects duplicates does
// not read before they land, and one that expects none is not read early
// enough to miss them.
func drainInto(ledger *Ledger, c *Client, want int, bound time.Duration) int {
	deadline := time.Now().Add(bound)
	got := 0
	for {
		ds := c.TakeDeliveries()
		got += len(ds)
		ledger.Deliveries = append(ledger.Deliveries, ds...)
		ledger.Events = append(ledger.Events, c.TakeEvents()...)
		if got >= want || time.Now().After(deadline) {
			return got
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// waitOffline waits until the node records the client's session offline.
func waitOffline(t *testing.T, n *Node, id string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if srv := n.Server(); srv != nil {
			if rec, ok := srv.Sessions().Record(id); ok && !rec.Online {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%s did not record %s offline", n.Name, id)
}
