package sim

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/broker"
	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/packet"
)

// A drain cut at the termination grace (WI 2259): a follower of the consensus
// core, called n1 below, is stopped within 2 s, as the kubelet stops a pod at
// the end of its grace, while it still owns the leases of the clients
// connected to it, and restarted at once, well inside the lease ttl, so
// nobody declares it dead. Pairs publish through the cut and the restart.
// Half the pairs' subscribers stay on n1 to the cut, a third of those with a
// second subscriber to the same topic on a live node; the other half moved
// from n1 to another node just before it, as a drain's redirect moves them,
// so n1's journal still holds their sessions.

// cutPairs is the pairs a cut-restart run uses, CUTRESTART_PAIRS or 30.
func cutPairs() int {
	if n, err := strconv.Atoi(os.Getenv("CUTRESTART_PAIRS")); err == nil && n > 1 {
		return n
	}
	return 30
}

// cutHold is where a run holds the restarted n1, and for how long: before it
// loads its store (load), or after the load and before its consensus core,
// lease service and replication open (start): the two windows a large store
// keeps open long enough to be met.
type cutHold struct {
	load, start time.Duration
}

func (h cutHold) String() string { return fmt.Sprintf("load%v-start%v", h.load, h.start) }

// cutLoss is one publish answered Success whose subscriber never got it, or
// answered No Matching Subscribers while its subscriber's session existed.
type cutLoss struct {
	pair   int
	seq    uint64
	node   string
	noSubs bool
	atSec  float64
}

// runCutRestart runs the scenario and returns the losses, with the run's
// zero at the cut.
func runCutRestart(t *testing.T, hold cutHold, defects broker.Defects) []cutLoss {
	t.Helper()
	const ttl = 10 * time.Second
	var armed atomic.Bool
	var victimName atomic.Value
	victimName.Store("")
	gate := func(d time.Duration) func(string) {
		return func(node string) {
			if d > 0 && armed.Load() && node == victimName.Load().(string) {
				time.Sleep(d)
			}
		}
	}
	c := New(t, Options{TTL: ttl, LoadGate: gate(hold.load), StartGate: gate(hold.start), Defects: defects,
		Log: halfLog(t, "cutrestart-"+hold.String())})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	// A cut leader's survivors first elect, and redirect its clients to it
	// until they have, so its sessions wait for it.
	leader, ok := agreedLeader(leaderViews(c, nil))
	if !ok {
		t.Fatal("no agreed leader")
	}
	var n1 *Node
	var others []*Node
	for _, n := range c.Nodes {
		if n1 == nil && n.Name != leader {
			n1 = n
			continue
		}
		others = append(others, n)
	}
	victimName.Store(n1.Name)
	n2, n3 := others[0], others[1]
	pairs := cutPairs()
	topic := func(i int) string { return fmt.Sprintf("cut/%02d", i) }

	subs := make([]*Client, pairs)
	pubs := make([]*Client, pairs)
	for i := range pairs {
		subs[i] = NewClient(fmt.Sprintf("cut-s%02d", i), []string{n1.TCP, n2.TCP, n3.TCP}, 600, int64(100+i))
		if err := subs[i].Run(ctx); err != nil {
			t.Fatal(err)
		}
		defer subs[i].Close()
		if err := subs[i].Subscribe(ctx, topic(i), 1); err != nil {
			t.Fatal(err)
		}
	}
	for i := pairs / 2; i < pairs; i++ {
		to := others[i%2]
		subs[i].Close()
		subs[i] = NewClient(subs[i].ID, []string{to.TCP, n1.TCP}, 600, int64(200+i))
		if err := subs[i].Run(ctx); err != nil {
			t.Fatal(err)
		}
		defer subs[i].Close()
	}
	// A second subscriber on a live node for every third pair that stays: a
	// publish to its topic matches it whatever n1's routes say.
	for i := 0; i < pairs/2; i += 3 {
		o := NewClient(fmt.Sprintf("cut-o%02d", i), []string{others[i%2].TCP}, 600, int64(400+i))
		if err := o.Run(ctx); err != nil {
			t.Fatal(err)
		}
		defer o.Close()
		if err := o.Subscribe(ctx, topic(i), 1); err != nil {
			t.Fatal(err)
		}
	}
	for i := range pairs {
		from := others[(i+1)%2]
		pubs[i] = NewClient(fmt.Sprintf("cut-p%02d", i), []string{from.TCP, n1.TCP}, 600, int64(300+i))
		if err := pubs[i].Run(ctx); err != nil {
			t.Fatal(err)
		}
		defer pubs[i].Close()
	}
	waitUntil(t, "every pair's route on the publishers' nodes", 10*time.Second, func() bool {
		for i := range pairs {
			owner, pub := n1, others[(i+1)%2]
			if i >= pairs/2 {
				owner = others[i%2]
			}
			if owner == pub {
				continue
			}
			if nodes, _ := pub.Server().Routes().Table().Match("default", topic(i)); !containsName(nodes, owner.Name) {
				return false
			}
		}
		return true
	})

	type mark struct {
		at     time.Time
		node   string
		noSubs bool
	}
	var mu sync.Mutex
	acked := make([]map[uint64]mark, pairs)
	for i := range acked {
		acked[i] = map[uint64]mark{}
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := range pairs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tick := time.NewTicker(50 * time.Millisecond)
			defer tick.Stop()
			for seq := uint64(1); ; seq++ {
				select {
				case <-stop:
					return
				case <-tick.C:
				}
				pctx, pcancel := context.WithTimeout(ctx, 5*time.Second)
				code, err := pubs[i].Publish(pctx, topic(i), 1, seq)
				pcancel()
				if err == nil && (code == packet.Success || code == packet.NoMatchingSubscribers) {
					mu.Lock()
					acked[i][seq] = mark{at: time.Now(), node: c.NodeAt(pubs[i].Node()).Name, noSubs: code == packet.NoMatchingSubscribers}
					mu.Unlock()
				}
			}
		}(i)
	}
	time.Sleep(time.Second)
	cutAt := time.Now()
	c.StopWithin(n1, 2*time.Second)
	armed.Store(true)
	if err := c.start(n1); err != nil {
		t.Fatal(err)
	}
	t.Logf("%s (leader %s) stopped and restarted in %v", n1.Name, leader, time.Since(cutAt).Round(time.Millisecond))
	waitUntil(t, "every subscriber back", 60*time.Second, func() bool {
		for _, s := range subs {
			if !s.Online() {
				return false
			}
		}
		return true
	})
	time.Sleep(10 * time.Second)
	close(stop)
	wg.Wait()
	time.Sleep(5 * time.Second)

	// The scenario proves nothing over pairs that stopped being acknowledged:
	// every pair must have publishes answered Success after the cut.
	mu.Lock()
	for i := range pairs {
		after := 0
		for _, m := range acked[i] {
			if !m.noSubs && m.at.After(cutAt) {
				after++
			}
		}
		if after == 0 {
			t.Errorf("pair %02d: no publish answered Success after the cut", i)
		}
	}
	mu.Unlock()
	var losses []cutLoss
	total := 0
	for i, s := range subs {
		got := map[uint64]bool{}
		for _, d := range s.TakeDeliveries() {
			got[d.Seq] = true
		}
		mu.Lock()
		for seq, m := range acked[i] {
			total++
			if m.noSubs || !got[seq] {
				losses = append(losses, cutLoss{pair: i, seq: seq, node: m.node, noSubs: m.noSubs, atSec: m.at.Sub(cutAt).Seconds()})
			}
		}
		mu.Unlock()
	}
	sort.Slice(losses, func(a, b int) bool {
		if losses[a].pair != losses[b].pair {
			return losses[a].pair < losses[b].pair
		}
		return losses[a].seq < losses[b].seq
	})
	noSubs := 0
	for _, l := range losses {
		if l.noSubs {
			noSubs++
		}
	}
	t.Logf("%d publishes acknowledged across %d pairs: %d never delivered, %d of them answered No Matching Subscribers",
		total, pairs, len(losses), noSubs)
	return losses
}

func reportCutLosses(t *testing.T, losses []cutLoss) {
	t.Helper()
	for _, l := range losses {
		answer := "Success"
		if l.noSubs {
			answer = "No Matching Subscribers"
		}
		t.Errorf("pair %02d seq %d: answered %s by %s at %+.3f s from the cut, never delivered", l.pair, l.seq, answer, l.node, l.atSec)
	}
}

// TestACutNodeRestartedAtOnceLosesNoAcknowledgedPublish: every publish
// acknowledged through the cut and the restart reaches its subscriber, with
// the restart held before the store's load and after it.
func TestACutNodeRestartedAtOnceLosesNoAcknowledgedPublish(t *testing.T) {
	for _, hold := range []cutHold{{load: 3 * time.Second}, {start: 3 * time.Second}} {
		t.Run(hold.String(), func(t *testing.T) {
			reportCutLosses(t, runCutRestart(t, hold, broker.Defects{}))
		})
	}
}

// TestDefectSyncBeforeLoad: a restarted node that sends its route set
// before its store is loaded withdraws its sessions' routes at its peers,
// and a publish to them is acknowledged and never delivered.
func TestDefectSyncBeforeLoad(t *testing.T) {
	losses := runCutRestart(t, cutHold{load: 3 * time.Second}, broker.Defects{SyncBeforeLoad: true})
	if len(losses) == 0 {
		t.Fatal("the defective build lost no acknowledged publish")
	}
	t.Logf("caught: %d", len(losses))
}

// TestDefectForwardsBeforeServing: a restarted node that takes forwarded
// deliveries before its replication is attached acknowledges publishes that
// the session its takers fetch from a replica never holds.
func TestDefectForwardsBeforeServing(t *testing.T) {
	losses := runCutRestart(t, cutHold{start: 3 * time.Second}, broker.Defects{ForwardsBeforeServing: true})
	if len(losses) == 0 {
		t.Fatal("the defective build lost no acknowledged publish")
	}
	t.Logf("caught: %d", len(losses))
}

// TestARecoveringNodeTakesNoForwardedDelivery: a restarted node whose
// consensus core has not caught up refuses forwarded QoS 1 deliveries into
// its sessions, though its replication is attached; once it has recovered it
// takes them. Its peers drop every consensus frame to it, so it never learns
// the leader's commit while the fault holds.
func TestARecoveringNodeTakesNoForwardedDelivery(t *testing.T) {
	c := New(t, Options{TTL: 10 * time.Second, Log: halfLog(t, "recovering")})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	leader, ok := agreedLeader(leaderViews(c, nil))
	if !ok {
		t.Fatal("no agreed leader")
	}
	var victim *Node
	var others []*Node
	for _, n := range c.Nodes {
		if victim == nil && n.Name != leader {
			victim = n
			continue
		}
		others = append(others, n)
	}
	const topic = "rec/x"
	setup := NewClient("rec-s", []string{victim.TCP}, 600, 1)
	if err := setup.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := setup.Subscribe(ctx, topic, 1); err != nil {
		t.Fatal(err)
	}
	setup.Close()
	waitUntil(t, "the subscriber offline on its node", 5*time.Second, func() bool {
		rec, ok := victim.Server().Sessions().Record("rec-s")
		return ok && !rec.Online
	})
	pubNode := others[0]
	// The publisher's own session replicates to the other survivor, so its
	// barrier waits on nothing the restarting node holds and only the
	// forward to the restarting node can refuse it.
	pubID := ""
	for i := 0; i < 64 && pubID == ""; i++ {
		id := fmt.Sprintf("rec-p%02d", i)
		m := pubNode.Server().Replication().Map()
		if !containsName(m.Current().Targets(m.ShardOf("default", id), pubNode.Name), victim.Name) {
			pubID = id
		}
	}
	if pubID == "" {
		t.Fatal("precondition not reached: no publisher identifier whose session avoids the restarting node")
	}
	pub := NewClient(pubID, []string{pubNode.TCP}, 600, 2)
	if err := pub.Run(ctx); err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	routed := func() bool {
		nodes, _ := pubNode.Server().Routes().Table().Match("default", topic)
		return containsName(nodes, victim.Name)
	}
	waitUntil(t, "the subscriber's route on the publisher's node", 5*time.Second, routed)

	for _, o := range others {
		c.Fault(o, victim, &transport.Fault{Drop: 1, Classes: []transport.Class{transport.Consensus}, Rand: rand.New(rand.NewSource(1))}) //nolint:gosec // seeded simulation
	}
	c.StopWithin(victim, 2*time.Second)
	c.Start(victim)
	waitUntil(t, "the restarted node attached, still recovering, and routed again", 10*time.Second, func() bool {
		srv := victim.Server()
		return srv.Replication() != nil && srv.Consensus() != nil && srv.Consensus().Recovering() && routed()
	})
	notServing := func() float64 {
		return rollSnapshot(c)[victim.Name][`hoardmq_forward_failures_total{reason="not-serving"}`]
	}
	acked := 0
	for seq := uint64(1); seq <= 10; seq++ {
		pctx, pcancel := context.WithTimeout(ctx, 5*time.Second)
		code, err := pub.Publish(pctx, topic, 1, seq)
		pcancel()
		if err == nil && !code.IsError() {
			acked++
		}
	}
	if !victim.Server().Consensus().Recovering() {
		t.Fatal("precondition lost: the node recovered while the fault held")
	}
	if acked != 0 {
		t.Fatalf("%d of 10 publishes acknowledged into a recovering node's session", acked)
	}
	if n := notServing(); n < 1 {
		t.Fatalf("not-serving refusals %v on the recovering node, want at least 1", n)
	}

	for _, o := range others {
		c.Fault(o, victim, nil)
	}
	waitUntil(t, "the node recovered", 15*time.Second, func() bool { return !victim.Server().Consensus().Recovering() })
	code, err := publishWithRetry(ctx, pub, topic, 1, 11, 20)
	if err != nil || code.IsError() {
		t.Fatalf("a publish once recovered: %s %v", code, err)
	}
	sub := NewClient("rec-s", []string{victim.TCP}, 600, 3)
	if err := sub.Run(ctx); err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	delivered := false
	waitUntil(t, "the publish once recovered delivered", 10*time.Second, func() bool {
		for _, d := range sub.TakeDeliveries() {
			delivered = delivered || (d.Publisher == pubID && d.Seq == 11)
		}
		return delivered
	})
}
