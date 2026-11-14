package sim

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
)

// heldAdoptionCase runs one client x against three brokers. x's session, a
// subscription and three acknowledged QoS 1 messages queued, is owned by a
// follower v whose only replica target is the leader l. v crashes, l adopts
// the lease, and l's adoption worker is held back, so x's CONNECT on l fetches
// the adopted session itself, from l's own replica store; that CONNECT's
// import is then held. x gives the connection up and sends a second CONNECT on
// l, which must not be answered with a fresh session while the session is in
// the first CONNECT's hands.
//
// With refuse, x then gives the second connection up too and connects to the
// third node t, which takes the lease from l, before the first CONNECT's
// import is let go; that import is refused on the later grant. Otherwise the
// import is let go at once. Either way x must, once it is connected again,
// hold its subscription and receive the three messages.
func heldAdoptionCase(t *testing.T, refuse bool) {
	const topic = "ha/x"
	const delay = 90 * time.Second
	var armedImport atomic.Bool
	var target atomic.Value
	target.Store("")
	importHeld, importGo := make(chan struct{}, 1), make(chan struct{})
	releaseImport := sync.OnceFunc(func() { close(importGo) })
	t.Cleanup(releaseImport)
	c := New(t, Options{AdoptionDelay: delay, Log: halfLog(t, "heldadoption-"+t.Name()),
		ImportGate: func(node, _, cid string) {
			if cid == target.Load().(string) && armedImport.CompareAndSwap(true, false) {
				importHeld <- struct{}{}
				<-importGo
			}
		}})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	leaderName, ok := agreedLeader(leaderViews(c, nil))
	if !ok {
		t.Fatal("no agreed leader")
	}
	l := c.Node(leaderName)
	var v, tn *Node
	for _, n := range c.Nodes {
		switch {
		case n == l:
		case v == nil:
			v = n
		default:
			tn = n
		}
	}
	m := l.Server().Replication().Map()
	id := ""
	for i := 0; i < 512 && id == ""; i++ {
		cand := fmt.Sprintf("ha-x-%d", i)
		if slices.Equal(m.Current().Targets(m.ShardOf("default", cand), v.Name), []string{l.Name}) {
			id = cand
		}
	}
	if id == "" {
		t.Fatalf("no client id whose owner %s replicates only to the leader %s", v.Name, l.Name)
	}
	target.Store(id)
	t.Logf("client %s: owner %s, replica on the leader %s; third node %s", id, v.Name, l.Name, tn.Name)

	setup := NewClient(id, []string{v.TCP}, 600, 1)
	if err := setup.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := setup.Subscribe(ctx, topic, 1); err != nil {
		t.Fatal(err)
	}
	setup.Close()
	waitOffline(t, v, id)
	pub := NewClient("ha-pub", []string{tn.TCP}, 600, 2)
	if err := pub.Run(ctx); err != nil {
		t.Fatal(err)
	}
	for seq := uint64(1); seq <= 3; seq++ {
		code, err := publishWithRetry(ctx, pub, topic, 1, seq, 20)
		if err != nil || code != packet.Success {
			t.Fatalf("publish %d: %s %v", seq, code, err)
		}
	}
	pub.Close()
	waitReplicaCopy(t, l, id, 3, 15*time.Second)

	c.Crash(v)
	waitUntil(t, "the leader adopting x's lease", 30*time.Second, func() bool {
		lo, ok := l.Server().Leases().Table().Lookup("default", id)
		return ok && lo.Owner == l.Name && !lo.Online
	})
	waitBasisWithout(t, c, v.Name, 30*time.Second)
	inlineBefore := l.Server().Leases().LivenessStats().AdoptedInline

	// The first CONNECT fetches the adopted session and its import is held.
	armedImport.Store(true)
	first, err := sendConnect(l.TCP, id)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-importHeld:
	case <-time.After(10 * time.Second):
		t.Fatal("precondition not reached: the first CONNECT's import was not held")
	}
	if got := l.Server().Leases().LivenessStats().AdoptedInline; got <= inlineBefore {
		t.Fatalf("precondition not reached: no adoption settled inside a CONNECT on %s (%d, %d before)", l.Name, got, inlineBefore)
	}
	if replicaCopy(l, id) == nil {
		t.Fatalf("precondition not reached: %s holds no replica copy of x while the first CONNECT's import is held", l.Name)
	}
	// x gives the first connection up and connects again on the same node.
	_ = first.Close()
	emptyBefore := rollSnapshot(c)[l.Name]
	second, err := sendConnect(l.TCP, id)
	if err != nil {
		t.Fatal(err)
	}
	type answer struct {
		ack *packet.Connack
		err error
	}
	answered := make(chan answer, 1)
	sent := time.Now()
	go func() { ack, err := readConnack(second); answered <- answer{ack, err} }()
	var a answer
	early := false
	select {
	case a = <-answered:
		early = true
		code := "no CONNACK"
		if a.ack != nil {
			code = a.ack.ReasonCode.String()
		}
		t.Logf("x's second CONNECT answered after %s: %s, %v", time.Since(sent).Round(time.Millisecond), code, a.err)
	case <-time.After(2 * time.Second):
		t.Logf("x's second CONNECT unanswered after %s", time.Since(sent).Round(time.Millisecond))
	}
	servedEmpty := early && a.err == nil && a.ack != nil && a.ack.ReasonCode == packet.Success && !a.ack.SessionPresent
	if servedEmpty {
		t.Errorf("x's second CONNECT on %s was served Session Present false while the first CONNECT held its adopted session", l.Name)
		for k, n := range rollSnapshot(c)[l.Name] {
			if d := n - emptyBefore[k]; d > 0 && strings.HasPrefix(k, "hoardmq_sessions_rebuilt_empty_total") {
				t.Logf("%s counted %s +%v", l.Name, k, d)
			}
		}
		// The fresh session's write drops the replica copy the first CONNECT
		// fetched from.
		deadline := time.Now().Add(3 * time.Second)
		for replicaCopy(l, id) != nil && time.Now().Before(deadline) {
			time.Sleep(20 * time.Millisecond)
		}
		if replicaCopy(l, id) == nil {
			t.Errorf("%s's replica copy of x was dropped while the first CONNECT's import was still held", l.Name)
		}
	} else if replicaCopy(l, id) == nil {
		t.Errorf("%s's replica copy of x was dropped while the first CONNECT's import was still held", l.Name)
	}

	_ = second.Close()
	if refuse {
		t.Logf("%s holds a replica copy of x as x connects to %s: %v", l.Name, tn.Name, replicaCopy(l, id) != nil)
		// x connects to the third node, which takes the lease from l.
		conn, ack, err := connectRaw(tn.TCP, id)
		for try := 0; try < 20 && (err != nil || ack == nil || ack.ReasonCode != packet.Success); try++ {
			if conn != nil {
				_ = conn.Close()
			}
			time.Sleep(250 * time.Millisecond)
			conn, ack, err = connectRaw(tn.TCP, id)
		}
		if err != nil || ack == nil || ack.ReasonCode != packet.Success {
			t.Fatalf("x's CONNECT on %s: %+v, %v", tn.Name, ack, err)
		}
		t.Logf("x's CONNECT on %s while l's import was held: Session Present %v", tn.Name, ack.SessionPresent)
		_ = conn.Close()
		waitOffline(t, tn, id)
	}
	refusedBefore := l.Server().Leases().LivenessStats().ConnectImportRefused
	releaseImport()
	time.Sleep(time.Second)
	t.Logf("after the first CONNECT's import was let go: %s refused %d CONNECT imports (%d before)", l.Name, l.Server().Leases().LivenessStats().ConnectImportRefused, refusedBefore)

	home := l
	if refuse {
		home = tn
	}
	if ss, ok := home.Server().Sessions().Export("default", id); ok {
		t.Logf("x's session on %s: %d subscriptions, %d entries, epoch %d", home.Name, len(ss.Subscriptions), len(ss.Entries), ss.Epoch)
	}
	resume := NewClient(id, []string{home.TCP}, 600, 3)
	rctx, rcancel := context.WithTimeout(ctx, 30*time.Second)
	err = resume.Run(rctx)
	rcancel()
	if err != nil {
		t.Fatalf("x did not come back on %s: %v", home.Name, err)
	}
	defer resume.Close()
	time.Sleep(3 * time.Second)
	got := map[uint64]bool{}
	for _, d := range resume.TakeDeliveries() {
		if d.Publisher == "ha-pub" {
			got[d.Seq] = true
		}
	}
	for _, e := range resume.TakeEvents() {
		if e.Kind == "connack" {
			t.Logf("x's resume on %s: %s, Session Present %v", home.Name, e.Code, e.Present)
		}
	}
	for seq := uint64(1); seq <= 3; seq++ {
		if !got[seq] {
			t.Errorf("acknowledged message %d queued for x never delivered after x resumed on %s", seq, home.Name)
		}
	}
}

// TestSecondConnectWhileAnAdoptedSessionsImportIsHeld: the first CONNECT's
// import is let go after the second CONNECT.
func TestSecondConnectWhileAnAdoptedSessionsImportIsHeld(t *testing.T) {
	heldAdoptionCase(t, false)
}

// TestSecondConnectWhileAnAdoptedSessionsImportIsHeldThenRefused: a later
// grant on another node makes the first CONNECT's import refused.
func TestSecondConnectWhileAnAdoptedSessionsImportIsHeldThenRefused(t *testing.T) {
	heldAdoptionCase(t, true)
}
