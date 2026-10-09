package sim

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// n1 owns an offline durable subscriber, s, on t/x; another subscriber, o,
// holds t/x on n3. n1 is cut off from both peers and n2 crashes, so n3 has no
// quorum. n2 restarts inside the partition: its new routing plane has never
// had n1's routes. Its data connection to n3 is held until n2's lease table
// shows n1's leases moved to whichever of n2 and n3 leads, and every node's
// adoption worker is held back, so s's adoption is unfinished when n3's
// routes first reach n2. A publisher on n2 then publishes to t/x until the
// adoption is finished. A node that reads n1's routes as known once n1 owns
// nothing acknowledges publishes that match o and never reach s.

// TestARestartedNodeAcknowledgesNothingForALostOwnerItsSessionsAreNotYetServed:
// every publish n2 acknowledges while s's adoption is unfinished must reach s
// once its session is served.
func TestARestartedNodeAcknowledgesNothingForALostOwnerItsSessionsAreNotYetServed(t *testing.T) {
	const delay = 4 * time.Second
	c := New(t, Options{AdoptionDelay: delay, Log: halfLog(t, "restartinpartition")})
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	n1, n2, n3 := c.Node("n1"), c.Node("n2"), c.Node("n3")

	setup := NewClient("s", []string{n1.TCP}, 600, 1)
	if err := setup.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := setup.Subscribe(ctx, "t/x", 1); err != nil {
		t.Fatal(err)
	}
	setup.Close()
	waitUntil(t, "s recorded offline on n1", 5*time.Second, func() bool {
		rec, ok := n1.Server().Sessions().Record("s")
		return ok && !rec.Online
	})
	o := NewClient("o", []string{n3.TCP}, 600, 2)
	if err := o.Run(ctx); err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	if err := o.Subscribe(ctx, "t/x", 1); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "n2 to route t/x to n1 and n3", 5*time.Second, func() bool {
		nodes, _ := n2.Server().Routes().Table().Match("default", "t/x")
		return len(nodes) == 2
	})

	c.Partition(n1, n2)
	c.Partition(n1, n3)
	c.Crash(n2)
	time.Sleep(3 * time.Second) // past the ttl of 2 s: n1's alive record has lapsed
	// n3 refuses n2 until n2's data dial to n3 is held, so the restarted n2
	// can reach n3 on its control connection only.
	n3.Server().Cluster().Isolate("n2", true)
	c.Start(n2)
	n2.Server().Cluster().HoldData("n3", true)
	n2.Server().Cluster().Isolate("n1", true)
	t.Cleanup(func() {
		if s := n2.Server(); s != nil {
			s.Cluster().HoldData("n3", false)
		}
	})
	n3.Server().Cluster().Isolate("n2", false)

	adopter := ""
	waitUntil(t, "n2's table to show s moved from n1", 30*time.Second, func() bool {
		l, ok := n2.Server().Leases().Table().Lookup("default", "s")
		if ok && l.Owner != "n1" {
			adopter = l.Owner
			return true
		}
		return false
	})
	if adopter != "n2" && adopter != "n3" {
		t.Fatalf("precondition not reached: s is owned by %q", adopter)
	}
	if nodes, _ := n2.Server().Routes().Table().Match("default", "t/x"); len(nodes) != 0 {
		t.Fatalf("precondition not reached: n2 already holds a route for t/x on %v before n3's routes reach it", nodes)
	}
	adoptedAt := time.Now()
	n2.Server().Cluster().HoldData("n3", false)
	t.Logf("%s adopts s; n2's data connection to n3 let up", adopter)

	pub := NewClient("p", []string{n2.TCP}, 600, 3)
	if err := pub.Run(ctx); err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	served := func() bool {
		srv := c.Node(adopter).Server()
		_, held := srv.Sessions().Record("s")
		_, unfinished := srv.Leases().AdoptionState()
		return held && unfinished == 0
	}
	var acked []uint64
	refused, beforeServed := 0, 0
	for seq := uint64(1); time.Since(adoptedAt) < delay+5*time.Second; seq++ {
		wasServed := served()
		pctx, pcancel := context.WithTimeout(ctx, 5*time.Second)
		code, err := pub.Publish(pctx, "t/x", 1, seq)
		pcancel()
		switch {
		case err != nil || code.IsError():
			refused++
		default:
			acked = append(acked, seq)
			if !wasServed {
				beforeServed++
			}
		}
		if wasServed && len(acked) > 0 && acked[len(acked)-1] == seq {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Logf("publishes on n2: %d acknowledged (%d before s's adoption was finished on %s), %d refused; n2 knows every owner's routes: %v",
		len(acked), beforeServed, adopter, refused, n2.Server().Routes().OwnersKnown())
	if len(acked) == 0 {
		t.Fatal("no publish on n2 was acknowledged after s's adoption was finished: the refusal never ended")
	}

	sub := NewClient("s", []string{c.Node(adopter).TCP}, 600, 4)
	if err := sub.Run(ctx); err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	got := map[uint64]bool{}
	missing := func() []uint64 {
		var out []uint64
		for _, seq := range acked {
			if !got[seq] {
				out = append(out, seq)
			}
		}
		return out
	}
	deadline := time.Now().Add(10 * time.Second)
	for len(missing()) > 0 && time.Now().Before(deadline) {
		for _, d := range sub.TakeDeliveries() {
			if d.Publisher == "p" {
				got[d.Seq] = true
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if m := missing(); len(m) > 0 {
		t.Fatalf("%d of %d publishes n2 acknowledged never reached s (%v); %d were acknowledged before s's adoption on %s was finished",
			len(m), len(acked), fmt.Sprint(m), beforeServed, adopter)
	}
}
