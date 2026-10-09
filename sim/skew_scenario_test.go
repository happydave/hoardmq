package sim

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
)

// skewPlan says whose clock a skew scenario moves and whether it then takes
// the leader away.
type skewPlan struct {
	leader     bool // skew the leader; otherwise every follower
	stopLeader bool // stop the leader once the skew is on
}

// leaseMove is one observed change of a lease's owner.
type leaseMove struct {
	client, from, to, seenOn string
	offsets                  string // every node's clock offset when it was seen
}

// skewOutcome is what a skew scenario saw of the clients whose owners stayed
// up throughout.
type skewOutcome struct {
	clients       int // watched: connected to a node that was never stopped
	nodesWatched  int // nodes with at least one watched client
	moves         []leaseMove
	takenOver     []string // watched clients dropped with Session Taken Over
	dropped       []string // watched clients that lost their connection at all
	leaderChanged bool
}

// runSkewScenario connects two durable clients to every node, steps the
// chosen clocks ahead, three ttls at a time, over two and a half ttls — long
// enough for several adoption ticks and for any wait on a lapse to run out —
// and reports every lease of a node that stayed up that changed owner, and
// every such client that lost its connection.
func runSkewScenario(t *testing.T, plan skewPlan) skewOutcome {
	t.Helper()
	c := New(t, Options{Log: halfLog(t, "skew-"+t.Name())})
	ttl := c.o.TTL
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	leaderName, ok := agreedLeader(leaderViews(c, nil))
	if !ok {
		t.Fatal("no agreed leader")
	}
	leader := c.Node(leaderName)

	owner := map[string]string{}
	clients := map[string]*Client{}
	for i, n := range c.Nodes {
		for j := 0; j < 2; j++ {
			id := fmt.Sprintf("skew-%s-%d", n.Name, j)
			cl := NewClient(id, []string{n.TCP}, 600, int64(10*i+j+1))
			if err := cl.Run(ctx); err != nil {
				t.Fatal(err)
			}
			defer cl.Close()
			if err := cl.Subscribe(ctx, "skew/"+id, 1); err != nil {
				t.Fatal(err)
			}
			clients[id], owner[id] = cl, n.Name
		}
	}
	for _, cl := range clients {
		cl.TakeEvents()
	}

	var skewed []*Node
	for _, n := range c.Nodes {
		if (n == leader) == plan.leader {
			skewed = append(skewed, n)
		}
	}
	offsets := func() string {
		s := ""
		for _, n := range c.Nodes {
			s += fmt.Sprintf("%s=%s ", n.Name, time.Duration(n.skew.Load()))
		}
		return s
	}
	// Only the first adoption tick after a step reads records stamped on the
	// clock from before it, and whether a renewal lands first depends on where
	// in the tick interval the step falls. So the clock is stepped five times,
	// three ttls further each time, at intervals that walk the step across the
	// tick.
	steps := 0
	step := func() {
		steps++
		for _, n := range skewed {
			c.Skew(n, time.Duration(steps)*3*ttl)
		}
	}
	step()
	stopped := map[string]bool{}
	if plan.stopLeader {
		c.Stop(leader)
		stopped[leader.Name] = true
	}

	out := skewOutcome{}
	seenMove := map[string]bool{}
	began := time.Now()
	end := began.Add(5 * ttl / 2)
	stride := ttl/4 + ttl/20
	for time.Now().Before(end) {
		if steps < 5 && time.Since(began) >= time.Duration(steps)*stride {
			step()
		}
		for _, n := range c.Running() {
			srv := n.Server()
			if srv == nil || srv.Leases() == nil {
				continue
			}
			for id, was := range owner {
				if stopped[was] {
					continue
				}
				l, ok := srv.Leases().Table().Lookup("default", id)
				if ok && l.Owner != was && !seenMove[id] {
					seenMove[id] = true
					out.moves = append(out.moves, leaseMove{client: id, from: was, to: l.Owner, seenOn: n.Name, offsets: offsets()})
				}
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	for _, n := range skewed {
		c.Skew(n, 0)
	}

	watchedNodes := map[string]bool{}
	for id, cl := range clients {
		if stopped[owner[id]] {
			continue
		}
		out.clients++
		watchedNodes[owner[id]] = true
		for _, e := range cl.TakeEvents() {
			if e.Kind != "disconnect" && e.Kind != "dropped" {
				continue
			}
			out.dropped = append(out.dropped, id)
			if e.Code == packet.SessionTakenOver {
				out.takenOver = append(out.takenOver, id)
			}
		}
	}
	out.nodesWatched = len(watchedNodes)
	if now, ok := agreedLeader(leaderViews(c, nil)); ok && now != leaderName {
		out.leaderChanged = true
	}
	return out
}

// requireNoChurn fails on any lease of a node that stayed up that moved, and
// on any of its clients that was taken over, and on a run that watched
// nothing.
func requireNoChurn(t *testing.T, got skewOutcome, wantNodes int) {
	t.Helper()
	if got.nodesWatched != wantNodes || got.clients != 2*wantNodes {
		t.Fatalf("watched %d clients on %d nodes, want %d on %d: the property would be vacuous", got.clients, got.nodesWatched, 2*wantNodes, wantNodes)
	}
	for _, m := range got.moves {
		t.Errorf("lease of %s moved from %s, which stayed up, to %s (seen on %s; clock offsets %s)", m.client, m.from, m.to, m.seenOn, m.offsets)
	}
	if len(got.takenOver) != 0 {
		t.Errorf("clients of nodes that stayed up were dropped with Session Taken Over: %v", got.takenOver)
	}
	if len(got.dropped) != 0 {
		t.Errorf("clients of nodes that stayed up lost their connection: %v", got.dropped)
	}
}

// TestSkewedFollowersTakeNoLease: followers whose clocks run three ttls fast
// take nothing from the nodes they read as lapsed.
func TestSkewedFollowersTakeNoLease(t *testing.T) {
	requireNoChurn(t, runSkewScenario(t, skewPlan{}), 3)
}

// TestSkewedLeaderAdoptsNoLiveOwner: a leader whose clock steps three ttls
// ahead reads every other node's alive record as lapsed, and must adopt none
// of them.
func TestSkewedLeaderAdoptsNoLiveOwner(t *testing.T) {
	requireNoChurn(t, runSkewScenario(t, skewPlan{leader: true}), 3)
}

// TestLeaderChangeToASkewedNodeAdoptsNoLiveOwner: the new leader's clock is
// three ttls ahead of the clock every alive record was stamped on.
func TestLeaderChangeToASkewedNodeAdoptsNoLiveOwner(t *testing.T) {
	got := runSkewScenario(t, skewPlan{stopLeader: true})
	if !got.leaderChanged {
		t.Fatal("the leader did not change: the scenario tested nothing")
	}
	requireNoChurn(t, got, 2)
}
