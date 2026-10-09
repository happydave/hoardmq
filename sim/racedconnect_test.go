package sim

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
)

// connectRaw sends one CONNECT that resumes a session (no clean start, a
// session expiry) and returns the connection and the broker's CONNACK, or the
// read error.
func connectRaw(addr, id string) (net.Conn, *packet.Connack, error) {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, nil, err
	}
	_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
	enc := &packet.Encoder{Version: packet.V5}
	dec := packet.NewDecoder(conn)
	dec.Version = packet.V5
	exp := uint32(600)
	if err := enc.Encode(conn, withCredentials(&packet.Connect{ProtocolLevel: packet.V5, ClientID: id, Properties: packet.Properties{SessionExpiryInterval: &exp}})); err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	p, err := dec.Read()
	if err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	ack, _ := p.(*packet.Connack)
	return conn, ack, nil
}

type rawOutcome struct {
	conn net.Conn
	ack  *packet.Connack
	err  error
}

// TestRacedPathSecondConnectOnTheSameNodeAtTheBroker:
// x's session, with queued messages, is on n1. A second CONNECT of x on n2
// looks the lease up (n1) and is held; a first CONNECT on n2 then takes the
// lease from n1 and pulls the session, its import held; the second is let go,
// is served raced (Session Present false), and the first's import follows:
// the client resumes the whole session, every acknowledged message with it.
func TestRacedPathSecondConnectOnTheSameNodeAtTheBroker(t *testing.T) {
	const id = "rc-x"
	var armedAcquire, armedImport atomic.Bool
	acquireHeld, acquireGo := make(chan struct{}, 1), make(chan struct{})
	importHeld, importGo := make(chan struct{}, 1), make(chan struct{})
	var once1, once2 sync.Once
	t.Cleanup(func() { once1.Do(func() { close(acquireGo) }); once2.Do(func() { close(importGo) }) })
	c := New(t, Options{TTL: 10 * time.Second, Log: halfLog(t, "racedconnect"),
		AcquireGate: func(node, _, cid string, granted bool) {
			if node == "n2" && cid == id && !granted && armedAcquire.CompareAndSwap(true, false) {
				acquireHeld <- struct{}{}
				<-acquireGo
			}
		},
		ImportGate: func(node, _, cid string) {
			if node == "n2" && cid == id && armedImport.CompareAndSwap(true, false) {
				importHeld <- struct{}{}
				<-importGo
			}
		}})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	n1, n2, n3 := c.Node("n1"), c.Node("n2"), c.Node("n3")
	setup := NewClient(id, []string{n1.TCP}, 600, 1)
	if err := setup.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := setup.Subscribe(ctx, "rc/x", 1); err != nil {
		t.Fatal(err)
	}
	setup.Close()
	waitUntil(t, "x offline on n1", 5*time.Second, func() bool {
		rec, ok := n1.Server().Sessions().Record(id)
		return ok && !rec.Online
	})
	pub := NewClient("rc-p", []string{n3.TCP}, 600, 2)
	if err := pub.Run(ctx); err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	acked := map[uint64]bool{}
	for seq := uint64(1); seq <= 3; seq++ {
		code, err := publishWithRetry(ctx, pub, "rc/x", 1, seq, 20)
		if err != nil || code != packet.Success {
			t.Fatalf("publish %d: %s %v", seq, code, err)
		}
		acked[seq] = true
	}

	armedAcquire.Store(true)
	second := make(chan rawOutcome, 1)
	go func() { conn, ack, err := connectRaw(n2.TCP, id); second <- rawOutcome{conn, ack, err} }()
	select {
	case <-acquireHeld:
	case <-time.After(10 * time.Second):
		t.Fatal("precondition not reached: the second CONNECT's acquire was not held")
	}
	armedImport.Store(true)
	first := make(chan rawOutcome, 1)
	go func() { conn, ack, err := connectRaw(n2.TCP, id); first <- rawOutcome{conn, ack, err} }()
	select {
	case <-importHeld:
	case <-time.After(10 * time.Second):
		t.Fatal("precondition not reached: the first CONNECT's import was not held")
	}
	once1.Do(func() { close(acquireGo) })
	s := <-second
	if s.err != nil {
		t.Fatalf("second CONNECT: %v", s.err)
	}
	if s.ack == nil || s.ack.ReasonCode != packet.Success || s.ack.SessionPresent {
		t.Fatalf("second CONNECT: %+v, want Success with no session present (served raced)", s.ack)
	}
	once2.Do(func() { close(importGo) })
	f := <-first
	if f.err != nil || f.ack == nil || f.ack.ReasonCode != packet.Success || !f.ack.SessionPresent {
		t.Fatalf("first CONNECT: %+v %v, want Success with the session present", f.ack, f.err)
	}
	if s.conn != nil {
		_ = s.conn.Close()
	}
	if n := rollSnapshot(c)["n2"][`hoardmq_sessions_rebuilt_empty_total{reason="raced"}`]; n != 1 {
		t.Fatalf("n2 counted %v sessions served empty raced, want 1", n)
	}
	_ = f.conn.Close()
	resume := NewClient(id, []string{n2.TCP}, 600, 3)
	if err := resume.Run(ctx); err != nil {
		t.Fatal(err)
	}
	defer resume.Close()
	time.Sleep(3 * time.Second)
	got := map[uint64]bool{}
	for _, d := range resume.TakeDeliveries() {
		got[d.Seq] = true
	}
	for seq := range acked {
		if !got[seq] {
			t.Errorf("acknowledged seq %d never delivered to x", seq)
		}
	}
}
