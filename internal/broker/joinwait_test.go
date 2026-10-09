package broker

import (
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
)

// TestAWaitingNodeAdmitsNoClient (WI 1735): a node whose data is gone,
// restarted while the group holds its admission, is not ready and refuses a
// client for as long as it waits, and its wait gauge counts the time; once
// admitted it becomes ready and the gauge reads zero.
func TestAWaitingNodeAdmitsNoClient(t *testing.T) {
	var hold atomic.Bool
	fixtureHooks = Hooks{HoldAdmissions: hold.Load}
	t.Cleanup(func() { fixtureHooks = Hooks{} })
	nodes := startTrio(t, nil)

	doc := *nodes["n1"].srv.config()
	hold.Store(true)
	nodes["n1"].stop()
	if err := os.RemoveAll(doc.Node.DataDir); err != nil {
		t.Fatal(err)
	}
	srv, err := NewWithHooks(doc, nil, fixtureHooks)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	n1 := &fixture{t: t, srv: srv, ca: nodes["n1"].ca, dir: nodes["n1"].dir, client: nodes["n1"].client}
	t.Cleanup(n1.stop)

	waitUntil(t, "n1's join window to pass with a group known", 15*time.Second, func() bool {
		return metric(n1, "hoardmq_raft_join_window_passed_total", nil) >= 1
	})
	if wait := metric(n1, "hoardmq_raft_join_wait_seconds", nil); wait <= 0 {
		t.Fatalf("the join wait gauge reads %v while n1 waits", wait)
	}
	if ready, reason := srv.Readiness(); ready {
		t.Fatal("a node waiting to be admitted reports ready")
	} else {
		t.Logf("waiting node's readiness: %s", reason)
	}
	// A refusal may be a CONNACK with an error or the connection closed; only
	// a successful CONNACK admits the client.
	nc := n1.dial("tcp", false)
	defer func() { _ = nc.Close() }()
	cl := &client{t: t, c: nc, enc: &packet.Encoder{Version: packet.V5}, dec: packet.NewDecoder(nc)}
	cl.dec.Version = packet.V5
	cl.send(&packet.Connect{ProtocolLevel: packet.V5, ClientID: "w", CleanStart: true})
	_ = nc.SetReadDeadline(time.Now().Add(10 * time.Second))
	answer, err := cl.dec.Read()
	if ack, ok := answer.(*packet.Connack); err == nil && ok && ack.ReasonCode == packet.Success {
		t.Fatal("a node waiting to be admitted accepted a client")
	}
	if ne, ok := err.(interface{ Timeout() bool }); ok && ne.Timeout() {
		t.Fatal("the waiting node gave the CONNECT no answer within 10 s")
	}
	t.Logf("CONNECT to the waiting node: %v %v", answer, err)
	if ready, _ := srv.Readiness(); ready {
		t.Fatal("n1 became ready while admissions were held")
	}

	hold.Store(false)
	waitUntil(t, "n1 admitted and ready", 20*time.Second, func() bool {
		ready, _ := srv.Readiness()
		return ready
	})
	if wait := metric(n1, "hoardmq_raft_join_wait_seconds", nil); wait != 0 {
		t.Fatalf("the join wait gauge reads %v once admitted", wait)
	}
}
