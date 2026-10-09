package broker

import (
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/lease"
	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

// nodeImportGate holds one node's import of one client, on the CONNECT's
// takeover path or the adoption worker's, between its fetch and its import.
type nodeImportGate struct {
	node, id atomic.Value // string
	entered  chan struct{}
	release  chan struct{}
	once     sync.Once
}

func (g *nodeImportGate) hook(node string) func(ns, id string) {
	return func(_, id string) {
		want, _ := g.id.Load().(string)
		at, _ := g.node.Load().(string)
		if want == "" || id != want || node != at {
			return
		}
		g.once.Do(func() { close(g.entered) })
		<-g.release
	}
}

// servedBeforeImport is what a publish to a lost node's client met while
// that client's session was between its adopter and the node it reconnected
// to, and what the client was given once its import finished.
type servedBeforeImport struct {
	forgotten bool              // the lost node's routes were forgotten while the import was held
	publish   packet.ReasonCode // the publish's acknowledgement, made while the import was held
	delivered bool              // the client received the publish after its import
	ack       *packet.Connack
	// After the import: whether both survivors then forgot the lost node's
	// routes, and whether a publish made on the adopter reached the client.
	forgottenAfter bool
	later          bool
	// What each survivor's entries for sessions in transit ended as.
	adopterTransit, takerTransit lease.TransitStats
}

// runServedBeforeImport: node victim holds client x's offline session; the
// leader adopts it when victim crashes, with its adoption worker held back.
// x reconnects to the third node, whose CONNECT takes the lease from the
// adopter and is held between its fetch and its import. The adopter's worker
// then finds the lease moved and ends its adoption. While the import is held,
// a QoS 1 publish to x's filter is made on the adopter (WI 1833). With
// takerFactFirst the third node's served fact for the node loss is in the log
// before x reconnects; otherwise it can only come later.
//
// The adopter is the node that leads when victim is declared dead, and the
// scenario plans for the node leading at its start. When the lead has moved in
// between, another node adopts x and the scenario starts again on a new trio.
func runServedBeforeImport(t *testing.T, takerFactFirst, laterHeld bool) servedBeforeImport {
	t.Helper()
	for attempt := 1; ; attempt++ {
		out, planned, adoptedBy := runServedBeforeImportOnce(t, takerFactFirst, laterHeld)
		if adoptedBy == "" {
			return out
		}
		if attempt == adopterAttempts {
			t.Fatalf("precondition not reached: in %d attempts the lead moved before the declaration and another node adopted", attempt)
		}
		t.Logf("attempt %d: %s adopted, not %s, the leader at the start; starting again on a new trio", attempt, adoptedBy, planned)
	}
}

// runServedBeforeImportOnce runs the scenario on one trio. It returns the
// planned adopter, and the node that adopted instead, which it has stopped the
// trio for, or "" when the scenario ran.
func runServedBeforeImportOnce(t *testing.T, takerFactFirst, laterHeld bool) (servedBeforeImport, string, string) {
	t.Helper()
	g := &nodeImportGate{entered: make(chan struct{}), release: make(chan struct{})}
	release := sync.OnceFunc(func() { close(g.release) })
	t.Cleanup(func() { fixtureHooks = Hooks{} })
	nodes := startTrio(t, func(name string, d *config.Document) {
		d.Cluster.Shards = 8
		d.Cluster.Replicas = 2
		delay := 1500 * time.Millisecond
		if takerFactFirst {
			delay = 2500 * time.Millisecond
		}
		fixtureHooks = Hooks{AdoptionDelay: delay, ImportGate: g.hook(name)}
	})
	fixtureHooks = Hooks{}
	t.Cleanup(release)
	m := waitMap(t, nodes, 3)
	adopter := ""
	for name, f := range nodes {
		if f.srv.Consensus().Status().Role == "leader" {
			adopter = name
		}
	}
	if adopter == "" {
		t.Fatal("precondition not reached: no leader")
	}
	var others []string
	for _, n := range []string{"n1", "n2", "n3"} {
		if n != adopter {
			others = append(others, n)
		}
	}
	victim, second := others[0], others[1]
	// x's replica is on the adopter, so the second node's takeover fetches it
	// from there.
	id, later := "", ""
	for i := 0; id == "" || later == ""; i++ {
		if c := fmt.Sprint("x", i); slices.Equal(m.Targets(shardmap.ShardOf("default", c, 8), victim), []string{adopter}) {
			if id == "" {
				id = c
			} else {
				later = c
			}
		}
	}
	exp := uint32(600)
	x, ack := nodes[victim].connect("tcp", packet.V5, &packet.Connect{ClientID: id, CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("connect on %s: %s", victim, ack.ReasonCode)
	}
	x.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "t/x", QoS: 1}}})
	if _, ok := x.read().(*packet.Suback); !ok {
		t.Fatal("no SUBACK")
	}
	x.send(&packet.Disconnect{ReasonCode: packet.NormalDisconnection})
	_ = x.c.Close()
	waitUntil(t, id+" offline on "+victim, 5*time.Second, func() bool {
		rec, ok := nodes[victim].srv.Sessions().Record(id)
		return ok && !rec.Online
	})
	if laterHeld {
		w, ack := nodes[victim].connect("tcp", packet.V5, &packet.Connect{ClientID: later, CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
		if ack.ReasonCode != packet.Success {
			t.Fatalf("connect %s on %s: %s", later, victim, ack.ReasonCode)
		}
		w.send(&packet.Disconnect{ReasonCode: packet.NormalDisconnection})
		_ = w.c.Close()
		waitUntil(t, later+" offline on "+victim, 5*time.Second, func() bool {
			rec, ok := nodes[victim].srv.Sessions().Record(later)
			return ok && !rec.Online
		})
	}
	// Each survivor owns a lease of its own, so each is a member whose fact
	// the forget waits for.
	for _, n := range []string{adopter, second} {
		if _, a := nodes[n].connect("tcp", packet.V5, &packet.Connect{ClientID: "own-" + n, CleanStart: true}, false); a.ReasonCode != packet.Success {
			t.Fatalf("connect on %s: %s", n, a.ReasonCode)
		}
	}
	waitUntil(t, "x's route on the adopter and the second node", 5*time.Second, func() bool {
		for _, n := range []string{adopter, second} {
			if owners, _ := nodes[n].srv.Routes().Table().Match("default", "t/x"); !slices.Contains(owners, victim) {
				return false
			}
		}
		return true
	})
	waitUntil(t, adopter+" holding x's replica", 5*time.Second, func() bool {
		return nodes[adopter].srv.Replication().ReplicaState().Find("default", id) != nil
	})

	g.id.Store(id)
	g.node.Store(second)
	nodes[victim].srv.Crash()
	nodes[victim].stopped = true
	if by := reassignedOwner(t, nodes, second, id, victim); by != adopter {
		release()
		for _, f := range nodes {
			f.stop()
		}
		return servedBeforeImport{}, adopter, by
	}
	if takerFactFirst {
		waitUntil(t, second+"'s served fact for "+victim+"'s loss in the log", 5*time.Second, func() bool {
			fact, _ := nodes[second].srv.Leases().ServedFact(second)
			return fact >= nodes[second].srv.Leases().LostAt(victim)
		})
	}
	if _, unfinished := nodes[adopter].srv.Leases().AdoptionState(); unfinished == 0 {
		t.Fatal("precondition not reached: the adopter finished its adoption before the reconnect")
	}

	nc := nodes[second].dial("tcp", false)
	t.Cleanup(func() { _ = nc.Close() })
	x2 := &client{t: t, c: nc, enc: &packet.Encoder{Version: packet.V5}, dec: packet.NewDecoder(nc)}
	x2.dec.Version = packet.V5
	x2.send(&packet.Connect{ProtocolLevel: packet.V5, ClientID: id, Properties: packet.Properties{SessionExpiryInterval: &exp}})
	select {
	case <-g.entered:
	case <-time.After(10 * time.Second):
		t.Fatalf("precondition not reached: %s's CONNECT for %s never reached its import", second, id)
	}
	if laterHeld {
		// The second takeover's grant is above the first's, and its import is
		// not held: the taker holds it while the first still imports.
		w, ack := nodes[second].connect("tcp", packet.V5, &packet.Connect{ClientID: later, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
		if ack.ReasonCode != packet.Success {
			t.Fatalf("precondition not reached: %s on %s: %s", later, second, ack.ReasonCode)
		}
		t.Cleanup(func() { _ = w.c.Close() })
		waitUntil(t, second+" holding "+later+" taken in transit", 10*time.Second, func() bool {
			return nodes[second].srv.Leases().TransitStats().TakenHeld >= 1
		})
	}
	waitUntil(t, adopter+"'s adoption of "+id+" to find its lease moved", 10*time.Second, func() bool {
		want := int64(1)
		if laterHeld {
			want = 2
		}
		return nodes[adopter].srv.Leases().LivenessStats().AdoptWorkerMoved >= want
	})
	if _, held := nodes[adopter].srv.Sessions().Record(id); held {
		t.Fatalf("precondition not reached: %s imported %s", adopter, id)
	}

	out := servedBeforeImport{}
	// The forget is what the window needs. Without a fix it follows the
	// adoption's end within milliseconds; the wait stays well inside the
	// bound on the entries that hold it (twice the ttl, 4 s here), so a
	// forget seen here is not one a bound let through.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if len(nodes[adopter].srv.Routes().Table().HiddenOwners()) == 0 {
			out.forgotten = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Logf("%s: connecting the publisher", time.Now().Format("15:04:05.000"))
	pub, _ := nodes[adopter].connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	t.Logf("%s: publishing", time.Now().Format("15:04:05.000"))
	pub.send(&packet.Publish{Topic: "t/x", QoS: 1, PacketID: 7, Payload: []byte("during")})
	a, ok := pub.read().(*packet.Puback)
	if !ok {
		t.Fatalf("no PUBACK for the publish made during the import: %+v", a)
	}
	out.publish = a.ReasonCode

	release()
	_ = nc.SetReadDeadline(time.Now().Add(10 * time.Second))
	p, err := x2.dec.Read()
	if err != nil {
		t.Fatalf("no CONNACK from %s: %v", second, err)
	}
	if out.ack, ok = p.(*packet.Connack); !ok {
		t.Fatalf("no CONNACK from %s: %+v", second, p)
	}
	if out.ack.ReasonCode != packet.Success {
		return out, adopter, ""
	}
	// Anything queued for x is sent right after the CONNACK.
	_ = nc.SetReadDeadline(time.Now().Add(3 * time.Second))
	for {
		p, err := x2.dec.Read()
		if err != nil {
			break
		}
		if m, ok := p.(*packet.Publish); ok && string(m.Payload) == "during" {
			out.delivered = true
			break
		}
	}
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if len(nodes[adopter].srv.Routes().Table().HiddenOwners()) == 0 && len(nodes[second].srv.Routes().Table().HiddenOwners()) == 0 {
			out.forgottenAfter = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	out.adopterTransit = nodes[adopter].srv.Leases().TransitStats()
	out.takerTransit = nodes[second].srv.Leases().TransitStats()
	if out.forgottenAfter {
		// A refusal here is the cluster settling (a barrier on the lost node
		// until the shard map drops it), which a client retries.
		publishUntilAccepted(t, pub, "t/x", "later")
		_ = nc.SetReadDeadline(time.Now().Add(5 * time.Second))
		for {
			p, err := x2.dec.Read()
			if err != nil {
				break
			}
			if m, ok := p.(*packet.Publish); ok && string(m.Payload) == "later" {
				out.later = true
				break
			}
		}
	}
	_ = nc.SetReadDeadline(time.Time{})
	return out, adopter, ""
}

// reassignedOwner waits for observer's table to name an owner of id other
// than victim, the crashed node whose leases the declaring leader reassigns to
// itself, and returns it.
func reassignedOwner(t *testing.T, nodes map[string]*fixture, observer, id, victim string) string {
	t.Helper()
	owner := ""
	waitUntil(t, observer+"'s table naming a new owner of "+id, 20*time.Second, func() bool {
		if l, ok := nodes[observer].srv.Leases().Table().Lookup("default", id); ok && l.Owner != victim {
			owner = l.Owner
			return true
		}
		return false
	})
	return owner
}

// TestPublishDuringATakeoverFromTheAdopterIsNotAcknowledgedUnheld: a QoS 1
// publish to a lost node's client, made while that client's session is
// between the adopter and the node that took its lease from the adopter, is
// either refused or reaches the session: never acknowledged while nobody
// holds it (WI 1833).
func TestPublishDuringATakeoverFromTheAdopterIsNotAcknowledgedUnheld(t *testing.T) {
	for _, first := range []bool{false, true} {
		name := map[bool]string{false: "the taker's fact after the takeover", true: "the taker's fact already in the log"}[first]
		t.Run(name, func(t *testing.T) { checkServedBeforeImport(t, runServedBeforeImport(t, first, false)) })
	}
	// The taker, its fact already in the log, holds a second session it took
	// later, at a higher grant, while the first is still importing: its cover
	// must not claim the first.
	t.Run("a later takeover held first", func(t *testing.T) { checkServedBeforeImport(t, runServedBeforeImport(t, true, true)) })
}

func checkServedBeforeImport(t *testing.T, got servedBeforeImport) {
	t.Helper()
	t.Logf("forgotten during the import %v, publish %s, CONNACK %s, delivered %v; forgotten after %v, a later publish delivered %v", got.forgotten, got.publish, got.ack.ReasonCode, got.delivered, got.forgottenAfter, got.later)
	if !got.publish.IsError() && !got.delivered {
		t.Fatalf("the publish was acknowledged (%s) and the client never received it; the lost node's routes forgotten during the import: %v", got.publish, got.forgotten)
	}
	if got.ack.ReasonCode != packet.Success {
		t.Fatalf("precondition: the client was not served on the taker: %s", got.ack.ReasonCode)
	}
	if !got.forgottenAfter || !got.later {
		t.Fatalf("after the import the lost node's routes were forgotten on both survivors: %v, and a publish reached the client: %v", got.forgottenAfter, got.later)
	}
	// The forget came on the taker's newer fact, not on a bound.
	if a, k := got.adopterTransit, got.takerTransit; a.HandedServed < 1 || a.HandedExpired != 0 || k.TakenHeld < 1 || k.TakenExpired != 0 {
		t.Fatalf("the adopter's entry ended %+v and the taker's %+v; want each ended on its condition, none on the bound", a, k)
	}
}
