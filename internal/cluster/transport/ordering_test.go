package transport

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/config"
)

// orderingRun sends forwarded publishes from senders goroutines and returns
// the admitted order recorded under the admission lock and the arrival order
// at the peer. One class: the order the design guarantees (invariant 4) is
// within the forwarded-publish class per peer; since WI 1340 each class is
// dispatched by its own drainer, so order across classes is not a property.
func orderingRun(t *testing.T, reorder bool, senders int) (admitted, arrived []string) {
	t.Helper()
	c := newCluster(t, "secret", []string{"n1", "n2"}, nil)
	var mu sync.Mutex
	c.nodes["n1"].t.admitted = func(peer string, payload []byte) {
		if len(payload) > 1 { // heartbeats carry one byte
			mu.Lock()
			admitted = append(admitted, string(payload))
			mu.Unlock()
		}
	}
	c.nodes["n1"].t.reorder = reorder
	var amu sync.Mutex
	c.nodes["n2"].t.Handle(ForwardedPublish, func(peer string, payload []byte) {
		amu.Lock()
		arrived = append(arrived, string(payload))
		amu.Unlock()
	})
	c.start(t)
	c.nodes["n1"].waitUp(t, "n2")
	const each = 1000
	var wg sync.WaitGroup
	for s := 0; s < senders; s++ {
		wg.Add(1)
		go func(s int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				payload := fmt.Sprintf("s%d:%d", s, i)
				if err := c.nodes["n1"].t.Send(context.Background(), "n2", ForwardedPublish, []byte(payload)); err != nil {
					t.Error(err)
					return
				}
			}
		}(s)
	}
	wg.Wait()
	waitFor(t, "all frames", func() bool {
		amu.Lock()
		defer amu.Unlock()
		return len(arrived) == senders*each
	})
	mu.Lock()
	defer mu.Unlock()
	amu.Lock()
	defer amu.Unlock()
	return append([]string(nil), admitted...), append([]string(nil), arrived...)
}

// checkOrder reports the first position where arrival differs from
// admission, and any sender whose own sequence is not monotone.
func checkOrder(admitted, arrived []string) error {
	if len(admitted) != len(arrived) {
		return fmt.Errorf("admitted %d, arrived %d", len(admitted), len(arrived))
	}
	for i := range admitted {
		if admitted[i] != arrived[i] {
			return fmt.Errorf("position %d: admitted %s, arrived %s", i, admitted[i], arrived[i])
		}
	}
	last := map[string]int{}
	for _, p := range arrived {
		sender, seq, _ := strings.Cut(p, ":")
		n, _ := strconv.Atoi(seq)
		if prev, ok := last[sender]; ok && n != prev+1 {
			return fmt.Errorf("sender %s: %d after %d", sender, n, prev)
		}
		last[sender] = n
	}
	return nil
}

func TestOrderingUnderConcurrentSenders(t *testing.T) {
	admitted, arrived := orderingRun(t, false, 8)
	if err := checkOrder(admitted, arrived); err != nil {
		t.Fatal(err)
	}
}

// TestOrderingCheckDetectsReordering is the negative control: with the
// writer made to swap adjacent frames, the same check must fail. One sender
// makes the control certain rather than likely — with several, a swap of two
// frames from different senders leaves every sender's own order intact, and
// the check rightly sees nothing.
func TestOrderingCheckDetectsReordering(t *testing.T) {
	admitted, arrived := orderingRun(t, true, 1)
	if err := checkOrder(admitted, arrived); err == nil {
		t.Fatal("the ordering check did not detect a reordering writer")
	}
}

func TestSingleConnectionWhenBothDial(t *testing.T) {
	c := newCluster(t, "secret", []string{"n1", "n2"}, nil)
	for _, n := range c.nodes {
		n.t.dialAll = true
	}
	a := received(c.nodes["n1"], Consensus)
	b := received(c.nodes["n2"], Consensus)
	c.start(t)
	c.nodes["n1"].waitUp(t, "n2")
	c.nodes["n2"].waitUp(t, "n1")
	// Let any duplicate resolve, then both directions must work on the one
	// surviving connection, dialed by the lower-named node.
	time.Sleep(300 * time.Millisecond)
	for _, n := range c.nodes {
		l := n.t.peers[otherOf(n.t.o.Node)].current(RoleControl)
		if l == nil {
			t.Fatalf("%s has no link", n.t.o.Node)
		}
		if l.dialedBy != "n1" {
			t.Fatalf("%s kept the connection dialed by %s", n.t.o.Node, l.dialedBy)
		}
	}
	if err := c.nodes["n1"].t.Send(context.Background(), "n2", Consensus, []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := c.nodes["n2"].t.Send(context.Background(), "n1", Consensus, []byte("b")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "both directions", func() bool { return a.n() == 1 && b.n() == 1 })
	// Exactly one connection: the loser was closed, so each side reports
	// one peer up and at most one "replaced" or "superseded" down event.
	for _, n := range c.nodes {
		if ps := n.t.Peers(); len(ps) != 1 || ps[0].State != Up {
			t.Fatalf("%s peers %+v", n.t.o.Node, ps)
		}
	}
}

func otherOf(name string) string {
	if name == "n1" {
		return "n2"
	}
	return "n1"
}

func TestHeartbeatDetectsSilence(t *testing.T) {
	c := newCluster(t, "secret", []string{"n1", "n2"}, nil)
	c.start(t)
	c.nodes["n1"].waitUp(t, "n2")
	c.nodes["n2"].waitUp(t, "n1")
	// Stress level travels in heartbeats.
	c.nodes["n1"].t.SetStress(2)
	waitFor(t, "stress level", func() bool { return c.nodes["n2"].t.Peers()[0].Stress == 2 })

	// Silence n1: its writer stops; n2 must declare it down after the
	// misses elapse (3 × 100 ms), within a tolerance.
	start := time.Now()
	c.nodes["n1"].t.pauseWrites.Store(true)
	c.nodes["n2"].waitDown(t, "n1")
	if el := time.Since(start); el < 250*time.Millisecond || el > 2*time.Second {
		t.Fatalf("down after %s", el)
	}
	if c.nodes["n2"].ev.count("hoardmq_cluster_heartbeat_misses_total", "n1") < 1 {
		t.Fatal("miss not counted")
	}
	// Release: the dialer (n1) reconnects.
	c.nodes["n1"].t.pauseWrites.Store(false)
	c.nodes["n2"].waitUp(t, "n1")
	c.nodes["n1"].waitUp(t, "n2")
}

func TestFramingErrorsCloseTheConnection(t *testing.T) {
	cases := []struct {
		name string
		raw  []byte
	}{
		{"oversize", []byte{0x7F, 0xFF, 0xFF, 0xFF, byte(Consensus), 0}},
		{"unknown class", []byte{0, 0, 0, 2, 0xEE, 0}},
		{"handshake class after handshake", []byte{0, 0, 0, 2, byte(classHello), 0}},
		{"flags", []byte{0, 0, 0, 2, byte(Consensus), 0x80}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newCluster(t, "secret", []string{"n1", "n2"}, nil)
			c.start(t)
			c.nodes["n1"].waitUp(t, "n2")
			c.nodes["n2"].waitUp(t, "n1")
			l := c.nodes["n1"].t.peers["n2"].current(RoleControl)
			// Bypass the writer: raw bytes straight to the socket.
			c.nodes["n1"].t.pauseWrites.Store(true)
			if _, err := l.c.Write(tc.raw); err != nil {
				t.Fatal(err)
			}
			c.nodes["n2"].waitDown(t, "n1")
			if !c.nodes["n2"].ev.has("cluster.peer.protocol", "closed") {
				t.Fatal("no protocol audit event")
			}
		})
	}
}

func TestHandlerPanicClosesOnlyThatConnection(t *testing.T) {
	c := newCluster(t, "secret", []string{"n1", "n2"}, nil)
	c.nodes["n2"].t.Handle(Consensus, func(peer string, payload []byte) { panic("bug") })
	c.start(t)
	c.nodes["n1"].waitUp(t, "n2")
	c.nodes["n2"].waitUp(t, "n1")
	if err := c.nodes["n1"].t.Send(context.Background(), "n2", Consensus, []byte("x")); err != nil {
		t.Fatal(err)
	}
	c.nodes["n2"].waitDown(t, "n1")
	// The pair recovers.
	c.nodes["n2"].waitUp(t, "n1")
}

func TestSetPeersAddsAndRemoves(t *testing.T) {
	c := newCluster(t, "secret", []string{"n1", "n2", "n3"}, nil)
	c.start(t)
	for _, n := range c.nodes {
		for _, other := range []string{"n1", "n2", "n3"} {
			if other != n.t.o.Node {
				n.waitUp(t, other)
			}
		}
	}
	// Remove n3 from n1's view: n1 drops the connection and refuses n3.
	n1 := c.nodes["n1"].t
	n1.SetPeers([]config.Peer{{Name: "n2", Address: c.nodes["n2"].t.Addr().String()}})
	c.nodes["n3"].waitDown(t, "n1")
	waitFor(t, "n1 forgets n3", func() bool { return len(n1.Peers()) == 1 })
	if err := n1.Send(context.Background(), "n3", Consensus, nil); err == nil {
		t.Fatal("send to a removed peer succeeded")
	}
}
