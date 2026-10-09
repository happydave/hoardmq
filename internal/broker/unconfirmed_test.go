package broker

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

// TestRetransmissionWhoseOpeningOutranItsDispatchIsRefused: a QoS 2 publish's
// opening reaches the publisher's replica while its forward to the offline
// subscriber's node is held, and the publisher's node dies before the PUBREC.
// On the survivor the client's retransmission is refused rather than answered
// as a duplicate of a message no subscriber had, and the client's next publish
// of it reaches the subscriber once.
func TestRetransmissionWhoseOpeningOutranItsDispatchIsRefused(t *testing.T) {
	var armed atomic.Bool
	hold := make(chan struct{})
	release := sync.OnceFunc(func() { close(hold) })
	fixtureHooks = Hooks{ForwardApplyGate: func(node, peer string) {
		if node == "n3" && peer == "n1" && armed.Load() {
			<-hold
		}
	}}
	t.Cleanup(func() { fixtureHooks = Hooks{} })
	nodes := startTrio(t, func(_ string, d *config.Document) {
		d.Cluster.Shards = 8
		d.Cluster.Replicas = 2
	})
	// Registered after the nodes, so it runs before their shutdowns.
	t.Cleanup(release)
	waitMap(t, nodes, 3)
	exp := uint32(600)

	sub, _ := nodes["n3"].connect("tcp", packet.V5, &packet.Connect{ClientID: "s", CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	sub.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "t/x", QoS: 2}}})
	if _, ok := sub.read().(*packet.Suback); !ok {
		t.Fatal("precondition: no SUBACK")
	}
	sub.send(&packet.Disconnect{ReasonCode: packet.NormalDisconnection})
	_ = sub.c.Close()
	waitOffline(t, nodes["n3"], "s")

	pub, _ := nodes["n1"].connect("tcp", packet.V5, &packet.Connect{ClientID: "p", CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	// A freshly formed cluster can refuse the first publish forwarded to a
	// peer (WI 1572): wait until one reaches the subscriber's node.
	accepted := false
	for pid := uint16(100); pid < 150 && !accepted; pid++ {
		pub.send(&packet.Publish{Topic: "t/x", QoS: 1, PacketID: pid, Payload: []byte("warm")})
		a, ok := pub.read().(*packet.Puback)
		accepted = ok && a.ReasonCode == packet.Success
		if !accepted {
			time.Sleep(100 * time.Millisecond)
		}
	}
	if !accepted {
		t.Fatal("precondition: no publish from n1 to the subscriber on n3 was accepted")
	}

	armed.Store(true)
	pub.send(&packet.Publish{Topic: "t/x", QoS: 2, PacketID: 9, Payload: []byte("m")})
	holder := ""
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && holder == "" {
		for _, n := range []string{"n2", "n3"} {
			if ss := nodes[n].srv.Replication().ReplicaState().Find("default", "p"); ss != nil {
				for _, id := range ss.InboundQoS2 {
					if id == 9 {
						holder = n
					}
				}
			}
		}
		time.Sleep(time.Millisecond)
	}
	if holder == "" {
		t.Fatal("precondition: the opening of 9 reached no replica")
	}
	nodes["n1"].srv.Crash()
	nodes["n1"].stopped = true

	var p2 *client
	deadline = time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) && p2 == nil {
		c, a := nodes[holder].connect("tcp", packet.V5, &packet.Connect{ClientID: "p", Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
		if a.ReasonCode == packet.Success {
			if !a.SessionPresent {
				t.Fatalf("precondition: p was served a fresh session on %s", holder)
			}
			p2 = c
		} else {
			_ = c.c.Close()
			time.Sleep(200 * time.Millisecond)
		}
	}
	if p2 == nil {
		t.Fatalf("precondition: p was never accepted on %s after n1 died", holder)
	}
	p2.send(&packet.Publish{Topic: "t/x", QoS: 2, PacketID: 9, Dup: true, Payload: []byte("m")})
	if r, ok := p2.read().(*packet.Pubrec); !ok || r.PacketID != 9 || r.ReasonCode != packet.ImplementationSpecificError {
		t.Fatalf("the retransmission under 9 was answered %+v; want PUBREC 9 with %v", r, packet.ImplementationSpecificError)
	}
	p2.send(&packet.Publish{Topic: "t/x", QoS: 2, PacketID: 10, Payload: []byte("m")})
	if r, ok := p2.read().(*packet.Pubrec); !ok || r.PacketID != 10 || r.ReasonCode.IsError() {
		t.Fatalf("the client's next publish of m was answered %+v", r)
	}
	p2.send(&packet.Pubrel{Ack: packet.Ack{PacketID: 10}})
	if c, ok := p2.read().(*packet.Pubcomp); !ok || c.ReasonCode.IsError() {
		t.Fatalf("PUBCOMP for 10: %+v", c)
	}

	s2, a := nodes["n3"].connect("tcp", packet.V5, &packet.Connect{ClientID: "s", Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	if !a.SessionPresent {
		t.Fatal("precondition: s's session is not present on n3")
	}
	got := map[string]int{}
	for {
		_ = s2.c.SetReadDeadline(time.Now().Add(1500 * time.Millisecond))
		p, err := s2.dec.Read()
		if err != nil {
			break
		}
		if m, ok := p.(*packet.Publish); ok {
			got[string(m.Payload)]++
			switch m.QoS {
			case 1:
				s2.send(&packet.Puback{Ack: packet.Ack{PacketID: m.PacketID}})
			case 2:
				s2.send(&packet.Pubrec{Ack: packet.Ack{PacketID: m.PacketID}})
			}
		}
	}
	if got["warm"] < 1 {
		t.Fatalf("precondition: s received %v, without the warm-up message", got)
	}
	if got["m"] != 1 {
		t.Fatalf("s received m %d times, want once (all: %v)", got["m"], got)
	}
}
