package sim

import (
	"context"
	"errors"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/lease"
	"github.com/happydave/hoardmq/internal/packet"
)

// specConn is one MQTT 5 connection driven packet by packet, for a client
// that keeps its own session state as the specification says.
type specConn struct {
	conn net.Conn
	enc  *packet.Encoder
	dec  *packet.Decoder
}

func newSpecConn(conn net.Conn) *specConn {
	dec := packet.NewDecoder(conn)
	dec.Version = packet.V5
	return &specConn{conn: conn, enc: &packet.Encoder{Version: packet.V5}, dec: dec}
}

// next reads the next packet that is not a DISCONNECT; a DISCONNECT, or the
// connection ending, is returned as an error.
func (s *specConn) next() (packet.Packet, error) {
	p, err := s.dec.Read()
	if err != nil {
		return nil, err
	}
	if d, ok := p.(*packet.Disconnect); ok {
		return nil, errors.New("disconnected: " + d.ReasonCode.String())
	}
	return p, nil
}

// publish2 sends a QoS 2 PUBLISH and returns its PUBREC's reason code.
func (s *specConn) publish2(id uint16, topic string, body []byte) (packet.ReasonCode, error) {
	if err := s.enc.Encode(s.conn, &packet.Publish{QoS: 2, Topic: topic, PacketID: id, Payload: body}); err != nil {
		return 0, err
	}
	p, err := s.next()
	if err != nil {
		return 0, err
	}
	r, ok := p.(*packet.Pubrec)
	if !ok || r.PacketID != id {
		return 0, errors.New("expected the PUBREC")
	}
	return r.ReasonCode, nil
}

// release sends the PUBREL and returns the PUBCOMP's reason code.
func (s *specConn) release(id uint16) (packet.ReasonCode, error) {
	if err := s.enc.Encode(s.conn, &packet.Pubrel{Ack: packet.Ack{PacketID: id}}); err != nil {
		return 0, err
	}
	p, err := s.next()
	if err != nil {
		return 0, err
	}
	c, ok := p.(*packet.Pubcomp)
	if !ok || c.PacketID != id {
		return 0, errors.New("expected the PUBCOMP")
	}
	return c.ReasonCode, nil
}

// TestHandedOffSessionIsNotServedEmptyToAClientThatKeepsItsState:
// x holds an open QoS 2 exchange on the leader (PUBREC received, PUBREL not
// yet sent) and is offline. x's CONNECT on the follower the leader's drain
// hands its leases to looks the lease up under the leader and forwards its
// acquire, which the leader holds; the drain hands off, and the follower's
// hook for the hand-off is held, as an apply that trails the leader's. The
// acquire is then let go: it applies after the hand-off, and the leader
// answers the follower before the follower has recorded the adoption.
//
// x behaves as the specification says (MQTT 5 §3.2.2.1.2, §4.4): on Session
// Present it sends the PUBREL it owes; on Session Present false it discards
// its session state, so identifier 1 is free again. It then publishes a second
// QoS 2 message under identifier 1, the lowest free one. A completed exchange
// (PUBCOMP) must be delivered to the subscriber.
func TestHandedOffSessionIsNotServedEmptyToAClientThatKeepsItsState(t *testing.T) {
	const id, topic = "hr-x", "hr/t"
	var leadName, recvName atomic.Value
	leadName.Store("")
	recvName.Store("")
	var armedForward, armedAdopt atomic.Bool
	forwardHeld, forwardGo := make(chan struct{}, 1), make(chan struct{})
	adoptHeld, adoptGo := make(chan struct{}, 1), make(chan struct{})
	releaseForward := sync.OnceFunc(func() { close(forwardGo) })
	releaseAdopt := sync.OnceFunc(func() { close(adoptGo) })
	t.Cleanup(releaseForward)
	t.Cleanup(releaseAdopt)
	c := New(t, Options{TTL: 10 * time.Second, Log: halfLog(t, "handoffraced"),
		LeaseForwardGate: func(node, peer string, ops []lease.Op) {
			if node != leadName.Load().(string) || peer != recvName.Load().(string) {
				return
			}
			for _, op := range ops {
				if op.Kind == lease.OpAcquire && op.ClientID == id && armedForward.CompareAndSwap(true, false) {
					forwardHeld <- struct{}{}
					<-forwardGo
					return
				}
			}
		},
		AdoptGate: func(node, _, cid string) {
			if node == recvName.Load().(string) && cid == id && armedAdopt.CompareAndSwap(true, false) {
				adoptHeld <- struct{}{}
				<-adoptGo
			}
		}})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	var leader string
	waitUntil(t, "an agreed leader", 20*time.Second, func() bool {
		l, ok := agreedLeader(leaderViews(c, nil))
		leader = l
		return ok
	})
	var others []string
	for _, n := range c.Nodes {
		if n.Name != leader {
			others = append(others, n.Name)
		}
	}
	sort.Strings(others)
	// A draining leader hands its leases to the first other live node.
	lead, recv, third := c.Node(leader), c.Node(others[0]), c.Node(others[1])
	leadName.Store(lead.Name)
	recvName.Store(recv.Name)

	sub := NewClient("hr-s", []string{third.TCP}, 600, 1)
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

	// The open exchange: accepted on the leader, PUBREL not sent.
	var open uint16
	waitUntil(t, "x's first QoS 2 publish accepted on the leader", 30*time.Second, func() bool {
		conn, ack, err := connectRaw(lead.TCP, id)
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
	waitUntil(t, "x offline on the leader", 5*time.Second, func() bool {
		rec, ok := lead.Server().Sessions().Record(id)
		return ok && !rec.Online
	})
	waitUntil(t, "the first message delivered", 10*time.Second, func() bool { return delivered(1) })
	if l, ok := recv.Server().Leases().Table().Lookup("default", id); !ok || l.Owner != lead.Name {
		t.Fatalf("precondition not reached: the receiver's table has x at %+v (found %v), want the leader's", l, ok)
	}

	armedForward.Store(true)
	armedAdopt.Store(true)
	first := make(chan rawOutcome, 1)
	go func() { conn, ack, err := connectRaw(recv.TCP, id); first <- rawOutcome{conn, ack, err} }()
	select {
	case <-forwardHeld:
	case <-time.After(10 * time.Second):
		t.Fatal("precondition not reached: x's acquire, forwarded by the receiver, was not held at the leader")
	}
	lead.Server().Drain()
	select {
	case <-adoptHeld:
	case <-time.After(10 * time.Second):
		t.Fatal("precondition not reached: the receiver's hook for the hand-off was not held")
	}
	releaseForward()

	var o rawOutcome
	early := false
	select {
	case o = <-first:
		early = true
	case <-time.After(time.Second):
	}
	releaseAdopt()
	if !early {
		o = <-first
	}
	// A refused CONNECT is retried, as a client does.
	for try := 0; try < 10 && o.err == nil && o.ack != nil && o.ack.ReasonCode != packet.Success; try++ {
		t.Logf("x's CONNECT on the receiver refused: %s; retrying", o.ack.ReasonCode)
		_ = o.conn.Close()
		time.Sleep(200 * time.Millisecond)
		o.conn, o.ack, o.err = connectRaw(recv.TCP, id)
	}
	if o.err != nil || o.ack == nil || o.ack.ReasonCode != packet.Success {
		t.Fatalf("x's CONNECT on the receiver: %+v, %v", o.ack, o.err)
	}
	t.Logf("x's CONNECT on the receiver answered while its hook was held: %v; served Session Present %v", early, o.ack.SessionPresent)
	if !o.ack.SessionPresent {
		t.Errorf("x's first successful CONNECT on the receiver was served Session Present false for the session it was handed")
	}
	sc := newSpecConn(o.conn)
	if !o.ack.SessionPresent {
		// The client discards its session state, the open exchange with it.
		open = 0
		// The import that follows ends this connection; the client reconnects.
		_ = sc.conn.SetDeadline(time.Now().Add(5 * time.Second))
		for {
			if _, err := sc.next(); err != nil {
				t.Logf("x's first connection on the receiver ended: %v", err)
				break
			}
		}
		_ = sc.conn.Close()
		conn, ack, err := connectRaw(recv.TCP, id)
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
		t.Errorf("x's second QoS 2 message completed (PUBCOMP) and was never delivered (x was first served Session Present %v)", o.ack.SessionPresent)
	}
	if got[1] != 1 {
		t.Errorf("the first message was delivered %d times, want once", got[1])
	}
}
