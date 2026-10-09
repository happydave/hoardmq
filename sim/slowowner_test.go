package sim

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/packet"
)

func envMs(name string, def time.Duration) time.Duration {
	if n, err := strconv.Atoi(os.Getenv(name)); err == nil && n >= 0 {
		return time.Duration(n) * time.Millisecond
	}
	return def
}

// TestAPreviousOwnerAcknowledgesNothingIntoASupersededCopy: a follower n1
// holding offline durable sessions is cut and restarted at once, as a drain cut
// at the grace restarts a pod. Their subscribers then connect to the consensus
// leader L, which takes the leases from n1 and pulls the sessions. n1 is slow
// to answer (its takeover passes held past the pull's timeout), so L serves
// each session from its replica on T, the third node. T's lease table lags L's
// grants (L's consensus frames to T are dropped for a while), so T would take
// n1's writes at n1's token. L's imports are held after their fetch, as an
// import waits under load. Publishers on T publish throughout; every publish
// answered Success must reach its subscriber, so n1 must refuse what it would
// write into a copy L now owns.
func TestAPreviousOwnerAcknowledgesNothingIntoASupersededCopy(t *testing.T) {
	delay := envMs("SLOWOWNER_DELAY_MS", 4*time.Second)
	lag := envMs("SLOWOWNER_LAG_MS", 6*time.Second)
	hold := envMs("SLOWOWNER_IMPORT_MS", 2*time.Second)
	pairs := 12
	if n, err := strconv.Atoi(os.Getenv("SLOWOWNER_PAIRS")); err == nil && n > 0 {
		pairs = n
	}
	const ttl = 10 * time.Second
	var armed atomic.Bool
	var victimName, leaderName atomic.Value
	victimName.Store("")
	leaderName.Store("")
	c := New(t, Options{TTL: ttl, Log: halfLog(t, "slowowner"),
		TakeoverDelay: func(node string) time.Duration {
			if armed.Load() && node == victimName.Load().(string) {
				return delay
			}
			return 0
		},
		ImportGate: func(node, _, id string) {
			if armed.Load() && hold > 0 && node == leaderName.Load().(string) && strings.HasPrefix(id, "so-s") {
				time.Sleep(hold)
			}
		}})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	leader, ok := agreedLeader(leaderViews(c, nil))
	if !ok {
		t.Fatal("no agreed leader")
	}
	var n1, lead, third *Node
	for _, n := range c.Nodes {
		switch {
		case n.Name == leader:
			lead = n
		case n1 == nil:
			n1 = n
		default:
			third = n
		}
	}
	victimName.Store(n1.Name)
	leaderName.Store(lead.Name)
	m := n1.Server().Replication().Map()
	var ids []string
	for i := 0; len(ids) < pairs && i < 10*pairs+100; i++ {
		id := fmt.Sprintf("so-s%03d", i)
		tg := m.Current().Targets(m.ShardOf("default", id), n1.Name)
		if containsName(tg, third.Name) && !containsName(tg, lead.Name) {
			ids = append(ids, id)
		}
	}
	if len(ids) < pairs {
		t.Fatalf("precondition not reached: %d of %d subscriber identifiers replicate from %s to %s alone", len(ids), pairs, n1.Name, third.Name)
	}
	topic := func(i int) string { return fmt.Sprintf("so/%02d", i) }
	subs := make([]*Client, pairs)
	pubs := make([]*Client, pairs)
	for i := range pairs {
		setup := NewClient(ids[i], []string{n1.TCP}, 600, int64(100+i))
		if err := setup.Run(ctx); err != nil {
			t.Fatal(err)
		}
		if err := setup.Subscribe(ctx, topic(i), 1); err != nil {
			t.Fatal(err)
		}
		setup.Close()
		pubs[i] = NewClient(fmt.Sprintf("so-p%03d", i), []string{third.TCP}, 600, int64(300+i))
		if err := pubs[i].Run(ctx); err != nil {
			t.Fatal(err)
		}
		defer pubs[i].Close()
	}
	waitUntil(t, "every subscriber offline on n1 and routed on the publishers' node", 10*time.Second, func() bool {
		for i := range pairs {
			if rec, ok := n1.Server().Sessions().Record(ids[i]); !ok || rec.Online {
				return false
			}
			if nodes, _ := third.Server().Routes().Table().Match("default", topic(i)); !containsName(nodes, n1.Name) {
				return false
			}
		}
		return true
	})
	type mark struct {
		at   time.Time
		node string
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
				if err == nil && code == packet.Success {
					mu.Lock()
					acked[i][seq] = mark{at: time.Now(), node: c.NodeAt(pubs[i].Node()).Name}
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
	waitUntil(t, "the restarted node recovered", 20*time.Second, func() bool {
		srv := n1.Server()
		return srv.Consensus() != nil && !srv.Consensus().Recovering()
	})
	if lag > 0 {
		c.Fault(lead, third, &transport.Fault{Drop: 1, Classes: []transport.Class{transport.Consensus}, Rand: rand.New(rand.NewSource(1))}) //nolint:gosec // seeded simulation
	}
	t.Logf("%s (leader %s, third %s) stopped, restarted and recovered in %v; takeover delay %v, %s's lag %v, import hold %v", n1.Name, lead.Name, third.Name, time.Since(cutAt).Round(time.Millisecond), delay, third.Name, lag, hold)
	for i := range pairs {
		subs[i] = NewClient(ids[i], []string{lead.TCP}, 600, int64(200+i))
		go func(s *Client) { _ = s.Run(ctx) }(subs[i])
		defer subs[i].Close()
	}
	if lag > 0 {
		time.Sleep(lag)
		c.Fault(lead, third, nil)
	}
	waitUntil(t, "every subscriber back", 60*time.Second, func() bool {
		for _, s := range subs {
			if !s.Online() {
				return false
			}
		}
		return true
	})
	time.Sleep(3 * time.Second)
	close(stop)
	wg.Wait()
	time.Sleep(5 * time.Second)

	type loss struct {
		pair int
		seq  uint64
		node string
		at   float64
	}
	var losses []loss
	total, after := 0, 0
	for i, s := range subs {
		got := map[uint64]bool{}
		for _, d := range s.TakeDeliveries() {
			got[d.Seq] = true
		}
		mu.Lock()
		for seq, mk := range acked[i] {
			total++
			if mk.at.After(cutAt) {
				after++
			}
			if !got[seq] {
				losses = append(losses, loss{i, seq, mk.node, mk.at.Sub(cutAt).Seconds()})
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
	t.Logf("%d publishes answered Success across %d pairs (%d after the cut): %d never delivered", total, pairs, after, len(losses))
	if after == 0 {
		t.Fatal("no publish answered Success after the cut")
	}
	for _, l := range losses {
		t.Errorf("pair %02d (%s) seq %d: answered Success by %s at %+.3f s from the cut, never delivered", l.pair, ids[l.pair], l.seq, l.node, l.at)
	}
	// The scenario reached the path: the restarted node refused a write into
	// a copy it had seen another node granted.
	if n := rollSnapshot(c)[n1.Name][`hoardmq_replication_barrier_failures_total{peer="-",reason="superseded"}`]; n < 1 {
		t.Errorf("the restarted node refused %v writes into superseded copies, want at least 1", n)
	}
}
