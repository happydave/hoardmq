package sim

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
)

// sendConnect dials addr and sends one CONNECT that resumes x's session (no
// clean start, a session expiry), leaving the CONNACK unread.
func sendConnect(addr, id string) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, err
	}
	_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
	exp := uint32(600)
	enc := &packet.Encoder{Version: packet.V5}
	if err := enc.Encode(conn, withCredentials(&packet.Connect{ProtocolLevel: packet.V5, ClientID: id, Properties: packet.Properties{SessionExpiryInterval: &exp}})); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

// readConnack reads the CONNACK of a connection sendConnect opened.
func readConnack(conn net.Conn) (*packet.Connack, error) {
	dec := packet.NewDecoder(conn)
	dec.Version = packet.V5
	p, err := dec.Read()
	if err != nil {
		return nil, err
	}
	ack, _ := p.(*packet.Connack)
	return ack, nil
}

// scForm is how x's later CONNECTs meet its first CONNECT's takeover on n2.
type scForm int

const (
	// scRaced: the second looked the lease up under n1, and its acquire
	// applied after the first's grant.
	scRaced scForm = iota
	// scOwned: the second looked the lease up once n2's table showed the
	// first's grant.
	scOwned
	// scFirstOpen: as scOwned, with the first connection left open.
	scFirstOpen
	// scThird: as scOwned; x gives the waiting second up and sends a third
	// the moment the import is released.
	scThird
	// scOlderOpen: as scThird, with the second left open while the third is
	// sent beside it.
	scOlderOpen
)

// secondConnectCase runs one client x that keeps its session state as MQTT 5
// says, against three brokers: x holds an open QoS 2 exchange on n1 (PUBREC
// received, PUBREL not sent) and is offline. Its first CONNECT on n2 takes
// the lease from n1, and its import of n1's session is held. x gives that
// connection up (not in scFirstOpen) and sends a second CONNECT on n2, which
// must not be answered while the import is held.
//
// On Session Present x sends the PUBREL it owes; on Session Present false it
// discards its session state, so identifier 1 is free again. It then
// publishes a second QoS 2 message under identifier 1, the lowest free one. A
// completed exchange (PUBCOMP) must be delivered to the subscriber.
func secondConnectCase(t *testing.T, form scForm) {
	const topic = "sc/t"
	id := [...]string{"sc-raced", "sc-owned", "sc-open", "sc-third", "sc-older"}[form]
	third := form == scThird || form == scOlderOpen
	raced := form == scRaced
	var armedBefore atomic.Int32
	var armedImport atomic.Bool
	beforeHeld := make(chan struct{}, 2)
	firstGo, secondGo := make(chan struct{}), make(chan struct{})
	importHeld, importGo := make(chan struct{}, 1), make(chan struct{})
	releaseFirst := sync.OnceFunc(func() { close(firstGo) })
	releaseSecond := sync.OnceFunc(func() { close(secondGo) })
	releaseImport := sync.OnceFunc(func() { close(importGo) })
	t.Cleanup(releaseFirst)
	t.Cleanup(releaseSecond)
	t.Cleanup(releaseImport)
	c := New(t, Options{TTL: 10 * time.Second, Log: halfLog(t, "secondconnect"),
		AcquireGate: func(node, _, cid string, granted bool) {
			if node != "n2" || cid != id || granted {
				return
			}
			// The first acquire held is the first CONNECT's, the second the
			// second's.
			switch armedBefore.Add(-1) {
			case 1:
				beforeHeld <- struct{}{}
				<-firstGo
			case 0:
				beforeHeld <- struct{}{}
				<-secondGo
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

	sub := NewClient(id+"-s", []string{n3.TCP}, 600, 1)
	if err := sub.Run(ctx); err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if err := sub.Subscribe(ctx, topic, 2); err != nil {
		t.Fatal(err)
	}
	got := map[uint64]int{}
	delivered := func(seq uint64) bool {
		for _, d := range sub.TakeDeliveries() {
			if d.Publisher == id {
				got[d.Seq]++
			}
		}
		return got[seq] > 0
	}

	// The open exchange: accepted on n1, PUBREL not sent.
	var open uint16
	waitUntil(t, "x's first QoS 2 publish accepted on n1", 30*time.Second, func() bool {
		conn, ack, err := connectRaw(n1.TCP, id)
		if err != nil || ack == nil || ack.ReasonCode != packet.Success {
			if conn != nil {
				_ = conn.Close()
			}
			return false
		}
		defer func() { _ = conn.Close() }()
		code, err := newSpecConn(conn).publish2(1, topic, payload(id, 1))
		if err == nil && code == packet.Success {
			open = 1
			return true
		}
		return false
	})
	waitUntil(t, "x offline on n1", 5*time.Second, func() bool {
		rec, ok := n1.Server().Sessions().Record(id)
		return ok && !rec.Online
	})
	waitUntil(t, "the first message delivered", 10*time.Second, func() bool { return delivered(1) })
	waitUntil(t, "n2's table showing x on n1", 5*time.Second, func() bool {
		l, ok := n2.Server().Leases().Table().Lookup("default", id)
		return ok && l.Owner == "n1"
	})

	n2OwnsX := func() bool {
		l, ok := n2.Server().Leases().Table().Lookup("default", id)
		return ok && l.Owner == "n2"
	}
	armedImport.Store(true)
	if raced {
		armedBefore.Store(2)
	}
	first, err := sendConnect(n2.TCP, id)
	if err != nil {
		t.Fatal(err)
	}
	if raced {
		select {
		case <-beforeHeld:
		case <-time.After(10 * time.Second):
			t.Fatal("precondition not reached: the first CONNECT's acquire was not held before its proposal")
		}
	} else {
		select {
		case <-importHeld:
		case <-time.After(10 * time.Second):
			t.Fatal("precondition not reached: the first CONNECT's import was not held")
		}
		waitUntil(t, "n2's table showing the first CONNECT's grant", 5*time.Second, n2OwnsX)
	}
	// x gives the first connection up and connects again.
	if form != scFirstOpen {
		_ = first.Close()
	} else {
		defer func() { _ = first.Close() }()
	}
	second, err := sendConnect(n2.TCP, id)
	if err != nil {
		t.Fatal(err)
	}
	if raced {
		select {
		case <-beforeHeld:
		case <-time.After(10 * time.Second):
			t.Fatal("precondition not reached: the second CONNECT's acquire was not held before its proposal")
		}
		releaseFirst()
		select {
		case <-importHeld:
		case <-time.After(10 * time.Second):
			t.Fatal("precondition not reached: the first CONNECT's import was not held")
		}
		waitUntil(t, "n2's table showing the first CONNECT's grant", 5*time.Second, n2OwnsX)
		releaseSecond()
	}

	type answer struct {
		ack *packet.Connack
		err error
	}
	answered := make(chan answer, 1)
	go func() { ack, err := readConnack(second); answered <- answer{ack, err} }()
	var a answer
	early := false
	select {
	case a = <-answered:
		early = true
	case <-time.After(300 * time.Millisecond):
	}
	if early {
		t.Errorf("x's second CONNECT was answered while the first's import was held: %+v, %v", a.ack, a.err)
	}
	conn := second
	switch {
	case third && !early:
		// x gives the waiting second up (or, in scOlderOpen, sends the third
		// beside it, waiting too); the third goes out as the import is
		// released, or before.
		if form == scThird {
			_ = second.Close()
			releaseImport()
		}
		thirdConn, err := sendConnect(n2.TCP, id)
		if err != nil {
			t.Fatal(err)
		}
		if form == scOlderOpen {
			waitUntil(t, "the third CONNECT waiting beside the second", 5*time.Second, func() bool {
				return n2.Server().Leases().LivenessStats().TakeoverWaiting >= 2
			})
			releaseImport()
		}
		conn = thirdConn
		a.ack, a.err = readConnack(thirdConn)
		if form == scOlderOpen {
			older := <-answered
			if older.err != nil || older.ack == nil || older.ack.ReasonCode != packet.ServerBusy {
				t.Errorf("x's older, overtaken CONNECT: %+v, %v; want CONNACK Server Busy", older.ack, older.err)
			}
			_ = second.Close()
		}
	case !early:
		releaseImport()
		a = <-answered
	default:
		releaseImport()
	}
	// A refused CONNECT is retried, as a client does.
	for try := 0; try < 10 && a.err == nil && a.ack != nil && a.ack.ReasonCode != packet.Success; try++ {
		t.Logf("x's second CONNECT refused: %s; retrying", a.ack.ReasonCode)
		_ = conn.Close()
		time.Sleep(200 * time.Millisecond)
		var ack *packet.Connack
		conn, ack, a.err = connectRaw(n2.TCP, id)
		a.ack = ack
	}
	if a.err != nil || a.ack == nil || a.ack.ReasonCode != packet.Success {
		t.Fatalf("x's second CONNECT: %+v, %v", a.ack, a.err)
	}
	served := a.ack.SessionPresent
	t.Logf("x's CONNECT after the first's import was released: Session Present %v", served)
	if !served {
		t.Errorf("x's CONNECT after the first was served Session Present false for the session its first CONNECT was taking over")
	}
	if form == scFirstOpen {
		firstConnectionEnds(t, first)
	}
	sc := newSpecConn(conn)
	if form == scThird && served {
		// Nothing displaces the third once served, the abandoned second
		// included.
		_ = sc.conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		_, err := sc.next()
		var ne net.Error
		switch {
		case errors.As(err, &ne) && ne.Timeout():
			sc = newSpecConn(conn)
		case err != nil:
			t.Logf("x's third connection ended: %v; reconnecting", err)
			_ = conn.Close()
			again, ack, err := connectRaw(n2.TCP, id)
			if err != nil || ack == nil || ack.ReasonCode != packet.Success || !ack.SessionPresent {
				t.Fatalf("x's reconnect after the third: %+v, %v; want its session", ack, err)
			}
			sc = newSpecConn(again)
		default:
			t.Fatal("x's third connection got a packet it did not ask for")
		}
	}
	if !served {
		// The client discards its session state, the open exchange with it.
		open = 0
		// The first CONNECT's import ends this connection; the client
		// reconnects.
		_ = sc.conn.SetDeadline(time.Now().Add(5 * time.Second))
		for {
			if _, err := sc.next(); err != nil {
				t.Logf("x's second connection ended: %v", err)
				break
			}
		}
		_ = sc.conn.Close()
		// The client's reconnect backoff.
		time.Sleep(500 * time.Millisecond)
		conn, ack, err := connectRaw(n2.TCP, id)
		if err != nil || ack == nil || ack.ReasonCode != packet.Success {
			t.Fatalf("x's reconnect: %+v, %v", ack, err)
		}
		t.Logf("x's reconnect: Session Present %v", ack.SessionPresent)
		sc = newSpecConn(conn)
	}
	defer func() { _ = sc.conn.Close() }()
	_ = sc.conn.SetDeadline(time.Now().Add(20 * time.Second))
	if open != 0 {
		if code, err := sc.release(open); err != nil || code != packet.Success {
			t.Fatalf("x's PUBREL for the open exchange: %s, %v", code, err)
		}
	}
	code, err := sc.publish2(1, topic, payload(id, 2))
	if err != nil || code != packet.Success {
		t.Fatalf("x's second QoS 2 publish: %s, %v", code, err)
	}
	if code, err := sc.release(1); err != nil || code != packet.Success {
		t.Fatalf("x's PUBREL for the second publish: %s, %v", code, err)
	}
	waitFor := time.Now().Add(5 * time.Second)
	for !delivered(2) && time.Now().Before(waitFor) {
		time.Sleep(50 * time.Millisecond)
	}
	if !delivered(2) {
		t.Errorf("x's second QoS 2 message completed (PUBCOMP) and was never delivered (x's second CONNECT was served Session Present %v)", served)
	}
	if got[1] != 1 {
		t.Errorf("the first message was delivered %d times, want once", got[1])
	}
}

// firstConnectionEnds reads x's first connection, left open, to its end: a
// Success CONNACK and then DISCONNECT Session Taken Over, or a close with no
// CONNACK when the newer connection displaced it before it was established.
func firstConnectionEnds(t *testing.T, first net.Conn) {
	t.Helper()
	_ = first.SetDeadline(time.Now().Add(5 * time.Second))
	fc := newSpecConn(first)
	p, err := fc.dec.Read()
	if err != nil {
		t.Logf("x's first connection closed with no CONNACK: %v", err)
		return
	}
	if ack, ok := p.(*packet.Connack); !ok || ack.ReasonCode != packet.Success {
		t.Fatalf("x's first connection: %T %+v, want its CONNACK", p, p)
	}
	for {
		if _, err := fc.next(); err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				t.Fatal("x's first connection stayed connected beside the newer one")
			}
			t.Logf("x's first connection: CONNACK, then %v", err)
			return
		}
	}
}

// TestSecondConnectRacedDuringTheFirstsTakeoverKeepsAClientsState: the
// second path to a session served empty as raced.
func TestSecondConnectRacedDuringTheFirstsTakeoverKeepsAClientsState(t *testing.T) {
	secondConnectCase(t, scRaced)
}

// TestSecondConnectOnTheOwnedLeaseDuringTheFirstsTakeoverKeepsAClientsState:
// the second CONNECT finds the first's grant in n2's table.
func TestSecondConnectOnTheOwnedLeaseDuringTheFirstsTakeoverKeepsAClientsState(t *testing.T) {
	secondConnectCase(t, scOwned)
}

// TestSecondConnectDisplacesTheFirstLeftOpen: the newer connection is served
// the session and the first, left open, is ended (MQTT-3.1.4-3).
func TestSecondConnectDisplacesTheFirstLeftOpen(t *testing.T) {
	secondConnectCase(t, scFirstOpen)
}

// TestThirdConnectAsTheImportEndsKeepsAClientsState: a CONNECT sent the moment
// the first's import is released is served the session.
func TestThirdConnectAsTheImportEndsKeepsAClientsState(t *testing.T) {
	secondConnectCase(t, scThird)
}

// TestOlderWaitingConnectLeftOpenIsRefusedForTheNewer: two CONNECTs wait on
// the first's takeover, the older left open; the newer is served the session
// and the older is refused.
func TestOlderWaitingConnectLeftOpenIsRefusedForTheNewer(t *testing.T) {
	secondConnectCase(t, scOlderOpen)
}
