package broker

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

// With two configured copies, x's session is on its owner and fed to one
// holder. The owner crashes, the leader declares it dead and adopts x's
// lease, and its adoption worker is held back long enough for the holder to
// be cut off from the adopter first. The adopter's fetch then finds the
// holder down; the log says the holder is alive, so the adoption waits on it.

// downHolderRoles are the nodes of one scenario: the adopter (the leader at
// the declaration), the owner that crashes, the holder of x's copy, and the
// rest.
type downHolderRoles struct {
	adopter, owner, holder string
	others                 []string
	id                     string
}

// downHolderShards is the shard count of these scenarios.
const downHolderShards = 8

// pickDownHolder finds an owner, a holder and a client among the nodes other
// than leader such that the owner's one target under m is the holder. Under
// the map computed without the owner, its one target is the holder too when
// sameAfter, so the adopter's fetch asks the holder whichever map it reads;
// otherwise it is the adopter, so a fetch under that map does not ask the
// holder at all.
func pickDownHolder(m shardmap.Map, names []string, leader string, sameAfter bool) (downHolderRoles, bool) {
	var rest []string
	for _, n := range names {
		if n != leader {
			rest = append(rest, n)
		}
	}
	slices.Sort(rest)
	for i := 0; i < 1000; i++ {
		id := fmt.Sprint("x", i)
		shard := shardmap.ShardOf("default", id, downHolderShards)
		for _, owner := range rest {
			var basis []string
			for _, n := range names {
				if n != owner {
					basis = append(basis, n)
				}
			}
			after := shardmap.Compute(1, basis, m.Replicas, downHolderShards)
			before := m.Targets(shard, owner)
			if len(before) != 1 || before[0] == leader {
				continue
			}
			if want := []string{leader}; sameAfter {
				if !slices.Equal(after.Targets(shard, owner), before) {
					continue
				}
			} else if !slices.Equal(after.Targets(shard, owner), want) {
				continue
			}
			r := downHolderRoles{adopter: leader, owner: owner, holder: before[0], id: id}
			for _, n := range rest {
				if n != owner && n != r.holder {
					r.others = append(r.others, n)
				}
			}
			return r, true
		}
	}
	return downHolderRoles{}, false
}

// downHolderAttempts bounds how many clusters a scenario starts when the lead
// moves before the owner is declared dead and another node adopts x.
const downHolderAttempts = 3

// downHolder runs the scenario on the named nodes: x's session with three
// acknowledged messages is on the owner and its copy on the holder, the owner
// crashes, the leader adopts x, and the holder is cut off from the adopter
// before the adopter's worker fetches. It returns the nodes, the roles and
// when the cut began.
func downHolder(t *testing.T, names []string) (map[string]*fixture, downHolderRoles, time.Time) {
	t.Helper()
	return downHolderPlaced(t, names, true)
}

// downHolderPlaced is downHolder with the client's placement under the map
// without the owner chosen as pickDownHolder's sameAfter.
func downHolderPlaced(t *testing.T, names []string, sameAfter bool) (map[string]*fixture, downHolderRoles, time.Time) {
	t.Helper()
	for attempt := 1; ; attempt++ {
		nodes, r, cut, adoptedBy := downHolderOnce(t, names, sameAfter)
		if adoptedBy == "" {
			return nodes, r, cut
		}
		if attempt == downHolderAttempts {
			t.Fatalf("precondition not reached: in %d attempts the lead moved before the declaration and another node adopted", attempt)
		}
		t.Logf("attempt %d: %s adopted %s, not %s, the leader at the start; starting again", attempt, adoptedBy, r.id, r.adopter)
	}
}

// downHolderOnce runs the scenario on one cluster. When a node other than the
// planned adopter adopted x, it stops the cluster and returns that node.
func downHolderOnce(t *testing.T, names []string, sameAfter bool) (map[string]*fixture, downHolderRoles, time.Time, string) {
	t.Helper()
	t.Cleanup(func() { fixtureHooks = Hooks{} })
	nodes := startSettled(t, names, func(_ string, d *config.Document) {
		d.Cluster.Shards = downHolderShards
		d.Cluster.Replicas = 2
		// Long enough for the cut to come after the reassignment applies and
		// before the adopter's worker first fetches.
		fixtureHooks = Hooks{AdoptionDelay: 2 * time.Second}
	})
	m := waitMap(t, nodes, len(names))
	leader := ""
	waitUntil(t, "a leader", 5*time.Second, func() bool {
		leader = leaderOf(nodes)
		return leader != ""
	})
	r, ok := pickDownHolder(m, names, leader, sameAfter)
	if !ok {
		t.Fatalf("precondition not reached: no client placed as the scenario needs (leader %s, same target under both maps %v)", leader, sameAfter)
	}
	exp := uint32(600)
	o := nodes[r.owner]
	x, ack := o.connect("tcp", packet.V5, &packet.Connect{ClientID: r.id, CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("connect %s on %s: %s", r.id, r.owner, ack.ReasonCode)
	}
	x.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "t/x", QoS: 1}}})
	if _, ok := x.read().(*packet.Suback); !ok {
		t.Fatal("no SUBACK")
	}
	x.send(&packet.Disconnect{ReasonCode: packet.NormalDisconnection})
	_ = x.c.Close()
	waitOffline(t, o, r.id)
	pub, _ := o.connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	// A just-formed cluster can refuse the first publish whose records go to
	// a peer: publish until three are accepted.
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
		t.Fatalf("precondition not reached: %d of 3 publishes to %s accepted", accepted, r.id)
	}
	// pub's session ends with its connection, and its lease with it, so x is
	// the only lease the crash leaves to adopt and the counts read are x's.
	pub.send(&packet.Disconnect{ReasonCode: packet.NormalDisconnection})
	_ = pub.c.Close()
	waitUntil(t, "pub's lease released", 5*time.Second, func() bool {
		for _, n := range names {
			if _, ok := nodes[n].srv.Leases().Table().Lookup("default", "pub"); ok {
				return false
			}
		}
		return true
	})
	waitUntil(t, r.holder+" to hold x's copy with three messages", 5*time.Second, func() bool {
		ss := nodes[r.holder].srv.Replication().ReplicaState().Find("default", r.id)
		return ss != nil && len(ss.Entries) == 3
	})
	for _, n := range names {
		if n != r.holder && n != r.owner && nodes[n].srv.Replication().ReplicaState().Find("default", r.id) != nil {
			t.Fatalf("precondition not reached: %s also holds a copy of %s", n, r.id)
		}
	}

	nodes[r.owner].srv.Crash()
	nodes[r.owner].stopped = true
	a := nodes[r.adopter]
	adoptedBy := ""
	waitUntil(t, "the owner's declaration and x's reassignment", 20*time.Second, func() bool {
		for _, n := range names {
			if n == r.owner {
				continue
			}
			tbl := nodes[n].srv.Leases().Table()
			if l, ok := tbl.Lookup("default", r.id); ok && l.Owner == n && !tbl.Live(r.owner) {
				adoptedBy = n
				return true
			}
		}
		return false
	})
	if adoptedBy != r.adopter {
		for _, f := range nodes {
			f.stop()
		}
		return nodes, r, time.Time{}, adoptedBy
	}
	if !sameAfter {
		// The placement differs from the holder only under the map without the
		// owner, so the cut waits for the adopter to apply it.
		waitUntil(t, r.adopter+" to apply the map without "+r.owner, 10*time.Second, func() bool {
			return !slices.Contains(a.srv.Replication().Map().Current().Basis, r.owner)
		})
	}
	cutPair(nodes, r.adopter, r.holder, true)
	cut := time.Now()
	t.Cleanup(func() { cutPair(nodes, r.adopter, r.holder, false) })
	if st := a.srv.Leases().LivenessStats(); st.AdoptWorkerUnanswered != 0 || st.AdoptWorkerEmpty != 0 || st.AdoptedByWorker != 0 {
		t.Fatalf("precondition not reached: %s's worker fetched before the cut (%+v)", r.adopter, st)
	}
	t.Logf("owner %s crashed; %s adopted %s; holder %s cut off from it", r.owner, r.adopter, r.id, r.holder)
	return nodes, r, cut, ""
}

// tryConnect is fixture.connect that reports a missing CONNACK instead of
// failing the test, for a node that may be unable to answer in time.
func tryConnect(f *fixture, id string, exp *uint32) (*client, *packet.Connack) {
	f.t.Helper()
	nc := f.dial("tcp", false)
	f.t.Cleanup(func() { _ = nc.Close() })
	cl := &client{t: f.t, c: nc, enc: &packet.Encoder{Version: packet.V5}, dec: packet.NewDecoder(nc)}
	cl.dec.Version = packet.V5
	cl.send(&packet.Connect{ProtocolLevel: packet.V5, ClientID: id, Properties: packet.Properties{SessionExpiryInterval: exp}})
	_ = nc.SetReadDeadline(time.Now().Add(10 * time.Second))
	p, err := cl.dec.Read()
	if err != nil {
		_ = nc.Close()
		return nil, nil
	}
	ack, _ := p.(*packet.Connack)
	return cl, ack
}

// servedOn connects id to f until it is accepted, for at most within, and
// returns the accepted client and its CONNACK, or nils, with the refusals
// seen before.
func servedOn(f *fixture, id string, exp *uint32, within time.Duration) (*client, *packet.Connack, []packet.ReasonCode) {
	f.t.Helper()
	var codes []packet.ReasonCode
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		c, ack := tryConnect(f, id, exp)
		if ack != nil && ack.ReasonCode == packet.Success {
			return c, ack, codes
		}
		if ack != nil {
			codes = append(codes, ack.ReasonCode)
		}
		if c != nil {
			_ = c.c.Close()
		}
		time.Sleep(200 * time.Millisecond)
	}
	return nil, nil, codes
}

// wantSession requires the three messages acknowledged before the crash.
func wantSession(t *testing.T, x *client) {
	t.Helper()
	for i := 1; i <= 3; i++ {
		msg := x.readPublish()
		if string(msg.Payload) != fmt.Sprint("m", i) {
			t.Fatalf("queued message %d: %q", i, msg.Payload)
		}
		x.send(&packet.Puback{Ack: packet.Ack{PacketID: msg.PacketID}})
	}
}

// holderPrecondition requires, past the wait, that the adopter owns x's lease,
// reads the holder as alive, and holds nothing for x, and that the holder
// still has x's copy.
func holderPrecondition(t *testing.T, nodes map[string]*fixture, r downHolderRoles) {
	t.Helper()
	a := nodes[r.adopter]
	l, ok := a.srv.Leases().Table().Lookup("default", r.id)
	if !ok || l.Owner != r.adopter || !a.srv.Leases().Table().Live(r.holder) {
		t.Fatalf("precondition not reached: x's lease on %s's table %+v (found %v), %s alive %v", r.adopter, l, ok, r.holder, a.srv.Leases().Table().Live(r.holder))
	}
	if ss := nodes[r.holder].srv.Replication().ReplicaState().Find("default", r.id); ss == nil || len(ss.Entries) != 3 {
		t.Fatalf("precondition not reached: %s no longer holds x's copy with three messages (%+v)", r.holder, ss)
	}
	if _, held := a.srv.Sessions().Record(r.id); held {
		t.Fatalf("precondition not reached: %s holds x's session although it could not reach %s", r.adopter, r.holder)
	}
}

// TestAdoptionWaitsForADownHolderAliveByTheLog is the double fault on three
// brokers: with the owner gone and the holder cut off from the adopter,
// neither survivor has a quorum, so nobody can declare the holder and the log
// says it is alive. The cut lasts twice the
// adoption's old bound (SealTimeout, twice the ttl). Once it heals, x must be
// served its session on the adopter, with its three messages.
func TestAdoptionWaitsForADownHolderAliveByTheLog(t *testing.T) {
	nodes, r, cut := downHolder(t, []string{"n1", "n2", "n3"})
	a := nodes[r.adopter]
	bound := 2 * 2 * time.Second // SealTimeout: twice the trio's ttl
	time.Sleep(time.Until(cut.Add(2*bound + time.Second)))
	holderPrecondition(t, nodes, r)
	if l := leaderOf(nodes); l != "" {
		t.Fatalf("precondition not reached: %s leads with the owner gone and the holder cut off", l)
	}
	st := a.srv.Leases().LivenessStats()
	t.Logf("past twice the bound: %s holder waits %d, gave up %d, waits past the bound %d, waiting now %d", r.adopter, st.AdoptHolderUndeclared, st.AdoptGaveUp, st.AdoptWaitsPastBound, st.AdoptWaitingNow)

	cutPair(nodes, r.adopter, r.holder, false)
	waitUntil(t, "a leader once the cut heals", 20*time.Second, func() bool { return leaderOf(nodes) != "" })
	exp := uint32(600)
	x, ack, codes := servedOn(a, r.id, &exp, 30*time.Second)
	if ack == nil {
		t.Fatalf("x was never accepted on %s once %s was reachable again (refusals %v)", r.adopter, r.holder, codes)
	}
	if !ack.SessionPresent {
		t.Fatalf("x was served a fresh session on %s once %s, which held its copy, was reachable again (gave up %d; refusals before %v)",
			r.adopter, r.holder, a.srv.Leases().LivenessStats().AdoptGaveUp, codes)
	}
	wantSession(t, x)
}

// TestAdoptionWaitsForADownHolderTheLeaderHears is the asymmetric form, on
// four brokers: the holder is cut off from the adopter only. The adopter
// loses its quorum and the fourth node leads, hearing the holder, so the log
// says the holder is alive. Past twice the adoption's old bound, x is not to
// be served fresh on the adopter; once the cut heals, it must be served its
// session there.
func TestAdoptionWaitsForADownHolderTheLeaderHears(t *testing.T) {
	nodes, r, cut := downHolder(t, []string{"n1", "n2", "n3", "n4"})
	a := nodes[r.adopter]
	waitUntil(t, "the fourth node to lead", 20*time.Second, func() bool { return leaderOf(nodes) == r.others[0] })
	bound := 2 * 2 * time.Second
	time.Sleep(time.Until(cut.Add(2*bound + time.Second)))
	holderPrecondition(t, nodes, r)
	st := a.srv.Leases().LivenessStats()
	t.Logf("past twice the bound: %s holder waits %d, gave up %d, waits past the bound %d, waiting now %d", r.adopter, st.AdoptHolderUndeclared, st.AdoptGaveUp, st.AdoptWaitsPastBound, st.AdoptWaitingNow)

	exp := uint32(600)
	c, ack := tryConnect(a, r.id, &exp)
	if ack != nil && ack.ReasonCode == packet.Success {
		t.Fatalf("x was served on %s (session present %v) while %s, alive by the log and heard by the leader %s, held its copy (gave up %d)",
			r.adopter, ack.SessionPresent, r.holder, r.others[0], a.srv.Leases().LivenessStats().AdoptGaveUp)
	}
	if c != nil {
		_ = c.c.Close()
	}
	if ack != nil {
		t.Logf("x refused on %s while cut off from %s: %s", r.adopter, r.holder, ack.ReasonCode)
	}

	cutPair(nodes, r.adopter, r.holder, false)
	x, ack, codes := servedOn(a, r.id, &exp, 30*time.Second)
	if ack == nil || !ack.SessionPresent {
		t.Fatalf("x on %s once %s was reachable again: %+v (refusals before %v)", r.adopter, r.holder, ack, codes)
	}
	wantSession(t, x)
}

// TestRefusedClientIsSentToTheLeaderPastADownHoldersBound: in the four-broker
// form, past twice the adoption's old bound the adopter sends x to the
// leader, which hears the holder; x connects there, with the cut in place,
// and is served its session with its three messages.
func TestRefusedClientIsSentToTheLeaderPastADownHoldersBound(t *testing.T) {
	nodes, r, cut := downHolder(t, []string{"n1", "n2", "n3", "n4"})
	a, leader := nodes[r.adopter], r.others[0]
	waitUntil(t, "the fourth node to lead", 20*time.Second, func() bool { return leaderOf(nodes) == leader })
	waitUntil(t, r.adopter+" to know the leader", 10*time.Second, func() bool { return a.srv.Consensus().Status().Leader == leader })
	time.Sleep(time.Until(cut.Add(2*2*2*time.Second + time.Second))) // twice SealTimeout, twice the brokers' ttl
	exp := uint32(600)
	c, ack := tryConnect(a, r.id, &exp)
	if c != nil {
		_ = c.c.Close()
	}
	want := "client." + leader + ":1883"
	if ack == nil || ack.ReasonCode != packet.UseAnotherServer || ack.Properties.ServerReference == nil || *ack.Properties.ServerReference != want {
		t.Fatalf("x on %s past the bound: %+v; want Use Another Server naming %q", r.adopter, ack, want)
	}
	x, ack, codes := servedOn(nodes[leader], r.id, &exp, 20*time.Second)
	t.Logf("leader %s: refusals before service %v", leader, codes)
	if ack == nil || !ack.SessionPresent {
		t.Fatalf("x on the leader %s with the cut in place: %+v", leader, ack)
	}
	wantSession(t, x)
	if st := a.srv.Leases().LivenessStats(); st.AdoptRedirected < 1 || st.AdoptWaitsPastBound != 1 {
		t.Fatalf("%s: redirected %d, waits past the bound %d; want at least 1, and 1", r.adopter, st.AdoptRedirected, st.AdoptWaitsPastBound)
	}
}

// TestAdoptionWaitsForADownHolderOutsideTheMapsSources: the three-broker
// double fault, with x placed so that under the map without the owner its one
// target is the adopter itself. A fetch under that map asks no other source,
// so the holder is not one; down and alive by the log, it is still waited
// for. Once the cut heals, x must be served its session on the adopter.
func TestAdoptionWaitsForADownHolderOutsideTheMapsSources(t *testing.T) {
	nodes, r, cut := downHolderPlaced(t, []string{"n1", "n2", "n3"}, false)
	a := nodes[r.adopter]
	bound := 2 * 2 * time.Second
	time.Sleep(time.Until(cut.Add(2*bound + time.Second)))
	holderPrecondition(t, nodes, r)
	st := a.srv.Leases().LivenessStats()
	m := a.srv.Replication().Map().Current()
	t.Logf("past twice the bound: %s map version %d basis %v; holder waits %d, worker empty %d, gave up %d, waits past the bound %d",
		r.adopter, m.Version, m.Basis, st.AdoptHolderUndeclared, st.AdoptWorkerEmpty, st.AdoptGaveUp, st.AdoptWaitsPastBound)
	if st.AdoptWorkerEmpty != 0 || st.AdoptHolderUndeclared < 1 {
		t.Fatalf("%s ended %d adoptions empty and waited on %s %d times; want none ended and at least one wait", r.adopter, st.AdoptWorkerEmpty, r.holder, st.AdoptHolderUndeclared)
	}
	cutPair(nodes, r.adopter, r.holder, false)
	waitUntil(t, "a leader once the cut heals", 20*time.Second, func() bool { return leaderOf(nodes) != "" })
	exp := uint32(600)
	x, ack, codes := servedOn(a, r.id, &exp, 30*time.Second)
	if ack == nil || !ack.SessionPresent {
		t.Fatalf("x on %s once %s, which held its copy, was reachable again: %+v (refusals before %v; worker empty %d)",
			r.adopter, r.holder, ack, codes, a.srv.Leases().LivenessStats().AdoptWorkerEmpty)
	}
	wantSession(t, x)
}

// TestRefusedClientIsSentToTheLeaderPastADownHolderOutsideTheMapsSources: the
// four-broker form of the same placement. The fourth node leads and hears the
// holder; past twice the adoption's old bound the adopter sends x there, and x
// is served its session with its three messages, with the cut in place.
func TestRefusedClientIsSentToTheLeaderPastADownHolderOutsideTheMapsSources(t *testing.T) {
	nodes, r, cut := downHolderPlaced(t, []string{"n1", "n2", "n3", "n4"}, false)
	a, leader := nodes[r.adopter], r.others[0]
	waitUntil(t, "the fourth node to lead", 20*time.Second, func() bool { return leaderOf(nodes) == leader })
	waitUntil(t, r.adopter+" to know the leader", 10*time.Second, func() bool { return a.srv.Consensus().Status().Leader == leader })
	time.Sleep(time.Until(cut.Add(2*2*2*time.Second + time.Second))) // twice SealTimeout, twice the brokers' ttl
	exp := uint32(600)
	c, ack := tryConnect(a, r.id, &exp)
	if c != nil {
		_ = c.c.Close()
	}
	want := "client." + leader + ":1883"
	if ack == nil || ack.ReasonCode != packet.UseAnotherServer || ack.Properties.ServerReference == nil || *ack.Properties.ServerReference != want {
		t.Fatalf("x on %s past the bound: %+v; want Use Another Server naming %q", r.adopter, ack, want)
	}
	x, ack, codes := servedOn(nodes[leader], r.id, &exp, 20*time.Second)
	t.Logf("leader %s: refusals before service %v", leader, codes)
	if ack == nil || !ack.SessionPresent {
		t.Fatalf("x on the leader %s with the cut in place: %+v", leader, ack)
	}
	wantSession(t, x)
	if st := a.srv.Leases().LivenessStats(); st.AdoptRedirected < 1 || st.AdoptWaitsPastBound != 1 {
		t.Fatalf("%s: redirected %d, waits past the bound %d; want at least 1, and 1", r.adopter, st.AdoptRedirected, st.AdoptWaitsPastBound)
	}
}
