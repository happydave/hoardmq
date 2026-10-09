package broker

import (
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

// importGate holds one client's import between its fetch and its import, on
// every node given its hook, and records the first node to reach it.
type importGate struct {
	id      atomic.Value // string
	by      atomic.Value // string: the node whose import entered first
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *importGate) hook(node string) func(ns, id string) {
	return func(_, id string) {
		if want, _ := g.id.Load().(string); want == "" || id != want {
			return
		}
		g.once.Do(func() {
			g.by.Store(node)
			close(g.entered)
		})
		<-g.release
	}
}

// heldOn is the node whose import the gate holds, once entered is closed.
func (g *importGate) heldOn() string {
	by, _ := g.by.Load().(string)
	return by
}

// importRaceOutcome is what the client is served on the third node, and the
// three nodes' roles.
type importRaceOutcome struct {
	ack                     *packet.Connack
	x                       *client
	nodes                   map[string]*fixture
	victim, adopter, second string
	id                      string
}

// adopterAttempts bounds how many trios a scenario that plans its adopter
// starts when the lead moves before the crashed node is declared dead.
const adopterAttempts = 3

// importAfterEmptyAnswer: the adopter's worker has fetched x's session from
// its own replica copy and is held before the import. The client connects to
// the third node, which takes the lease from the adopter; the adopter's pass
// finds no session and answers with no state, and that answer is held in
// flight. The worker's import is then let go, and it completes before the
// third node reads the answer and falls back to the replicas.
//
// The adopter is the node that leads when the crashed node is declared dead,
// and the scenario plans for the node leading at its start. When the lead has
// moved in between, another node adopts x and the scenario cannot happen, so
// it starts again on a new trio.
func importAfterEmptyAnswer(t *testing.T, defect bool) importRaceOutcome {
	t.Helper()
	for attempt := 1; ; attempt++ {
		o, adoptedBy := importAfterEmptyAnswerOnce(t, defect)
		if adoptedBy == "" {
			return o
		}
		if attempt == adopterAttempts {
			t.Fatalf("precondition not reached: in %d attempts the lead moved before the declaration and another node adopted", attempt)
		}
		t.Logf("attempt %d: %s adopted %s, not %s, the leader at the start; starting again on a new trio", attempt, adoptedBy, o.id, o.adopter)
	}
}

// importAfterEmptyAnswerOnce runs the scenario on one trio. When a node other
// than the planned adopter adopted x, it stops the trio and returns that
// node's name with the planned roles; otherwise the outcome and "".
func importAfterEmptyAnswerOnce(t *testing.T, defect bool) (importRaceOutcome, string) {
	t.Helper()
	g := &importGate{entered: make(chan struct{}), release: make(chan struct{})}
	release := sync.OnceFunc(func() { close(g.release) })
	t.Cleanup(func() { fixtureHooks = Hooks{} })
	nodes := startTrio(t, func(name string, d *config.Document) {
		d.Cluster.Shards = 8
		d.Cluster.Replicas = 2
		fixtureHooks = Hooks{ImportGate: g.hook(name), Defects: Defects{ImportPastLaterGrant: defect}}
	})
	fixtureHooks = Hooks{}
	// Registered after the nodes' shutdowns, so it runs before them: a worker
	// held at the gate would otherwise hold its node's shutdown.
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
	id := ""
	for i := 0; id == ""; i++ {
		if c := fmt.Sprint("x", i); slices.Equal(m.Targets(shardmap.ShardOf("default", c, 8), victim), []string{adopter}) {
			id = c
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
	waitUntil(t, "x offline on "+victim, 5*time.Second, func() bool {
		rec, ok := nodes[victim].srv.Sessions().Record(id)
		return ok && !rec.Online
	})
	pub, _ := nodes[second].connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	// A freshly formed cluster can refuse the first QoS 1 publish forwarded
	// to a peer (WI 1572): publish until three are accepted.
	accepted := 0
	for pid := uint16(1); pid < 60 && accepted < 3; pid++ {
		pub.send(&packet.Publish{Topic: "t/x", QoS: 1, PacketID: pid, Payload: []byte(fmt.Sprint("m", accepted+1))})
		if a, ok := pub.read().(*packet.Puback); ok && a.ReasonCode == packet.Success {
			accepted++
		} else {
			time.Sleep(100 * time.Millisecond)
		}
	}
	if accepted < 3 {
		t.Fatalf("precondition not reached: %d of 3 publishes to %s accepted", accepted, id)
	}
	waitUntil(t, adopter+" holding x's replica with three messages", 5*time.Second, func() bool {
		ss := nodes[adopter].srv.Replication().ReplicaState().Find("default", id)
		return ss != nil && len(ss.Entries) == 3
	})
	if ss := nodes[second].srv.Replication().ReplicaState().Find("default", id); ss != nil {
		t.Fatalf("precondition not reached: %s holds a copy of %s itself", second, id)
	}

	g.id.Store(id)
	// The adopter's answer to the second node's pull is held in flight long
	// enough for the released import to finish first; the pull's own budget
	// (1 s) outlasts it.
	nodes[adopter].srv.Cluster().SetFault(second, &transport.Fault{Delay: 600 * time.Millisecond, Classes: []transport.Class{transport.TakeoverAck}})
	nodes[victim].srv.Crash()
	nodes[victim].stopped = true
	select {
	case <-g.entered:
	case <-time.After(20 * time.Second):
		t.Fatalf("precondition not reached: %s's worker never fetched %s", adopter, id)
	}
	if by := g.heldOn(); by != adopter {
		release()
		for _, f := range nodes {
			f.stop()
		}
		return importRaceOutcome{nodes: nodes, victim: victim, adopter: adopter, second: second, id: id}, by
	}
	waitUntil(t, second+"'s table naming "+adopter+" as "+id+"'s owner", 5*time.Second, func() bool {
		l, ok := nodes[second].srv.Leases().Table().Lookup("default", id)
		return ok && l.Owner == adopter
	})

	nc := nodes[second].dial("tcp", false)
	t.Cleanup(func() { _ = nc.Close() })
	x2 := &client{t: t, c: nc, enc: &packet.Encoder{Version: packet.V5}, dec: packet.NewDecoder(nc)}
	x2.dec.Version = packet.V5
	x2.send(&packet.Connect{ProtocolLevel: packet.V5, ClientID: id, Properties: packet.Properties{SessionExpiryInterval: &exp}})
	answered := make(chan packet.Packet, 1)
	go func() {
		_ = nc.SetReadDeadline(time.Now().Add(10 * time.Second))
		p, err := x2.dec.Read()
		if err != nil {
			p = nil
		}
		answered <- p
	}()
	waitUntil(t, adopter+"'s pass to answer "+id+" with no state", 5*time.Second, func() bool {
		return nodes[adopter].srv.Leases().LivenessStats().TakeoverAnsweredPending >= 1
	})
	release()
	// The crashed node held no other client's lease, so the adopter's only
	// adoption is x's.
	waitUntil(t, adopter+"'s worker to finish with "+id, 5*time.Second, func() bool {
		_, unfinished := nodes[adopter].srv.Leases().AdoptionState()
		return unfinished == 0
	})
	select {
	case p := <-answered:
		t.Fatalf("precondition not reached: %s answered %+v before the import finished", second, p)
	default:
	}
	p := <-answered
	a, ok := p.(*packet.Connack)
	if !ok {
		t.Fatalf("no CONNACK from %s: %+v", second, p)
	}
	_ = nc.SetReadDeadline(time.Time{})
	return importRaceOutcome{ack: a, x: x2, nodes: nodes, victim: victim, adopter: adopter, second: second, id: id}, ""
}

func waitUntil(t *testing.T, what string, within time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("precondition not reached: %s", what)
}

// TestImportAfterAnEmptyAnswerKeepsTheSession: the adopter's pass answered
// the third node with no state before the adopter's worker installed the
// session it had fetched. The install is refused, since the pass's request
// carried a later grant, so the adopter's replica copy stays for the third
// node's fallback: the client is served its session and its queued messages.
func TestImportAfterAnEmptyAnswerKeepsTheSession(t *testing.T) {
	o := importAfterEmptyAnswer(t, false)
	if o.ack.ReasonCode != packet.Success || !o.ack.SessionPresent {
		t.Fatalf("CONNACK on %s: %s, Session Present %v; want the session served", o.second, o.ack.ReasonCode, o.ack.SessionPresent)
	}
	for i := 1; i <= 3; i++ {
		msg := o.x.readPublish()
		if string(msg.Payload) != fmt.Sprint("m", i) {
			t.Fatalf("queued message %d: %q", i, msg.Payload)
		}
		o.x.send(&packet.Puback{Ack: packet.Ack{PacketID: msg.PacketID}})
	}
	waitUntil(t, "the adopter to receive the confirmation", 10*time.Second, func() bool {
		return o.nodes[o.adopter].srv.Leases().LivenessStats().ConfirmsReceived >= 1
	})
	ad, sec := o.nodes[o.adopter].srv.Leases().LivenessStats(), o.nodes[o.second].srv.Leases().LivenessStats()
	if ad.AdoptImportRefused != 1 || ad.AfterEmptyAnswer != 0 || ad.ReclaimedOnConfirm != 0 || sec.TakeoverEmpty != 0 {
		t.Fatalf("%s: import refused %d, imported after an empty answer %d, reclaimed on confirm %d; %s: takeover empty %d; want 1, 0, 0, 0",
			o.adopter, ad.AdoptImportRefused, ad.AfterEmptyAnswer, ad.ReclaimedOnConfirm, o.second, sec.TakeoverEmpty)
	}
}

// TestImportPastALaterGrantLosesTheSession is the permanent control: with the
// install admitted whatever grant was seen, the adopter installs the session
// after answering "no state", its write drops its replica copy, and the third
// node serves the client an empty session.
func TestImportPastALaterGrantLosesTheSession(t *testing.T) {
	o := importAfterEmptyAnswer(t, true)
	if o.ack.ReasonCode != packet.Success {
		t.Fatalf("CONNACK on %s: %s", o.second, o.ack.ReasonCode)
	}
	if o.ack.SessionPresent {
		t.Fatal("served its session with the install admitted past a later grant: the scenario no longer reaches the loss it is the control for")
	}
	if st := o.nodes[o.adopter].srv.Leases().LivenessStats(); st.AfterEmptyAnswer != 1 {
		t.Fatalf("%s's imports after an empty answer: %d, want 1", o.adopter, st.AfterEmptyAnswer)
	}
}

// connectImportAfterALaterGrant: x's session is on a, offline, with three
// queued messages replicated to n, the third node. x's CONNECT on r takes the
// lease over and is held after its pull, before its import. x then connects
// on a again, taking the lease back; r's pass finds no session and answers
// with no state. r's CONNECT is then let go. It returns both CONNACKs: r's
// held one, then a's.
func connectImportAfterALaterGrant(t *testing.T, defect bool) (held, again *packet.Connack, xa *client, nodes map[string]*fixture, r, id string) {
	t.Helper()
	g := &importGate{entered: make(chan struct{}), release: make(chan struct{})}
	release := sync.OnceFunc(func() { close(g.release) })
	t.Cleanup(func() { fixtureHooks = Hooks{} })
	a, n := "n1", "n3"
	r = "n2"
	nodes = startTrio(t, func(name string, d *config.Document) {
		d.Cluster.Shards = 8
		d.Cluster.Replicas = 2
		fixtureHooks = Hooks{Defects: Defects{ImportPastLaterGrant: defect}}
		if name == r {
			// Only r's install is held: a's own CONNECT installs through
			// the same hook.
			fixtureHooks.ImportGate = g.hook(name)
		}
	})
	fixtureHooks = Hooks{}
	t.Cleanup(release)
	m := waitMap(t, nodes, 3)
	for i := 0; id == ""; i++ {
		if c := fmt.Sprint("x", i); slices.Equal(m.Targets(shardmap.ShardOf("default", c, 8), a), []string{n}) {
			id = c
		}
	}
	exp := uint32(600)
	x, ack := nodes[a].connect("tcp", packet.V5, &packet.Connect{ClientID: id, CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("connect on %s: %s", a, ack.ReasonCode)
	}
	x.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "t/x", QoS: 1}}})
	if _, ok := x.read().(*packet.Suback); !ok {
		t.Fatal("no SUBACK")
	}
	x.send(&packet.Disconnect{ReasonCode: packet.NormalDisconnection})
	_ = x.c.Close()
	waitUntil(t, "x offline on "+a, 5*time.Second, func() bool {
		rec, ok := nodes[a].srv.Sessions().Record(id)
		return ok && !rec.Online
	})
	pub, _ := nodes[n].connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	accepted := 0
	for pid := uint16(1); pid < 60 && accepted < 3; pid++ {
		pub.send(&packet.Publish{Topic: "t/x", QoS: 1, PacketID: pid, Payload: []byte(fmt.Sprint("m", accepted+1))})
		if p, ok := pub.read().(*packet.Puback); ok && p.ReasonCode == packet.Success {
			accepted++
		} else {
			time.Sleep(100 * time.Millisecond)
		}
	}
	if accepted < 3 {
		t.Fatalf("precondition not reached: %d of 3 publishes to %s accepted", accepted, id)
	}
	waitUntil(t, n+" holding x's replica with three messages", 5*time.Second, func() bool {
		ss := nodes[n].srv.Replication().ReplicaState().Find("default", id)
		return ss != nil && len(ss.Entries) == 3
	})

	g.id.Store(id)
	nc := nodes[r].dial("tcp", false)
	t.Cleanup(func() { _ = nc.Close() })
	xr := &client{t: t, c: nc, enc: &packet.Encoder{Version: packet.V5}, dec: packet.NewDecoder(nc)}
	xr.dec.Version = packet.V5
	xr.send(&packet.Connect{ProtocolLevel: packet.V5, ClientID: id, Properties: packet.Properties{SessionExpiryInterval: &exp}})
	answered := make(chan packet.Packet, 1)
	go func() {
		_ = nc.SetReadDeadline(time.Now().Add(10 * time.Second))
		p, err := xr.dec.Read()
		if err != nil {
			p = nil
		}
		answered <- p
	}()
	select {
	case <-g.entered:
	case <-time.After(10 * time.Second):
		t.Fatalf("precondition not reached: %s's CONNECT for %s never pulled the state", r, id)
	}
	waitUntil(t, a+"'s table naming "+r+" as "+id+"'s owner", 5*time.Second, func() bool {
		l, ok := nodes[a].srv.Leases().Table().Lookup("default", id)
		return ok && l.Owner == r
	})
	xa, again = nodes[a].connect("tcp", packet.V5, &packet.Connect{ClientID: id, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	if st := nodes[r].srv.Leases().LivenessStats(); st.TakeoversServed < 1 {
		t.Fatalf("precondition not reached: %s served no pass for %s's return to %s", r, id, a)
	}
	release()
	p := <-answered
	h, ok := p.(*packet.Connack)
	if !ok {
		t.Fatalf("no CONNACK from %s: %+v", r, p)
	}
	return h, again, xa, nodes, r, id
}

// TestConnectImportAfterALaterGrantIsRefused: r's CONNECT pulled x's state for
// its grant, and before r installed it the lease went back to a, whose pull r
// answered "no state". r installs nothing and answers its client Server Busy;
// a serves x its session.
func TestConnectImportAfterALaterGrantIsRefused(t *testing.T) {
	held, again, xa, nodes, r, id := connectImportAfterALaterGrant(t, false)
	if again.ReasonCode != packet.Success || !again.SessionPresent {
		t.Fatalf("x back on n1: %s, Session Present %v; want its session", again.ReasonCode, again.SessionPresent)
	}
	for i := 1; i <= 3; i++ {
		if msg := xa.readPublish(); string(msg.Payload) != fmt.Sprint("m", i) {
			t.Fatalf("queued message %d on n1: %q", i, msg.Payload)
		}
	}
	if held.ReasonCode != packet.ServerBusy {
		t.Fatalf("the CONNECT held on %s: %s; want Server Busy, its lease having moved on", r, held.ReasonCode)
	}
	if _, ok := nodes[r].srv.Sessions().Record(id); ok {
		t.Fatalf("%s holds a session for %s, whose lease is n1's", r, id)
	}
	if st := nodes[r].srv.Leases().LivenessStats(); st.ConnectImportRefused != 1 {
		t.Fatalf("%s's refused CONNECT installs: %d, want 1", r, st.ConnectImportRefused)
	}
}

// TestConnectImportPastALaterGrantServesWhereTheLeaseIsNot is its control: with
// the install admitted whatever grant was seen, r installs the session and
// accepts its client while the lease is n1's.
func TestConnectImportPastALaterGrantServesWhereTheLeaseIsNot(t *testing.T) {
	held, _, _, nodes, r, id := connectImportAfterALaterGrant(t, true)
	if held.ReasonCode != packet.Success {
		t.Fatalf("the CONNECT held on %s: %s; with the check off it is accepted, or the scenario no longer reaches what it controls", r, held.ReasonCode)
	}
	if _, ok := nodes[r].srv.Sessions().Record(id); !ok {
		t.Fatalf("%s holds no session for %s with the check off", r, id)
	}
}
