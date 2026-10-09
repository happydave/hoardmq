package broker

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

// startTrio brings up three brokers with the shared-secret transport,
// statically configured peers, a fast raft clock and short leases, and
// waits until every node is ready and every peer up.
func startTrio(t *testing.T, mutate func(name string, d *config.Document)) map[string]*fixture {
	t.Helper()
	return startSettled(t, []string{"n1", "n2", "n3"}, mutate)
}

// startSettled is startTrio for the named nodes.
func startSettled(t *testing.T, names []string, mutate func(name string, d *config.Document)) map[string]*fixture {
	t.Helper()
	dir := t.TempDir()
	secret := filepath.Join(dir, "secret")
	if err := os.WriteFile(secret, []byte("broker-cluster-test-secret-bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	nodes := startCluster(t, names, func(name string, d *config.Document) {
		d.Cluster.HeartbeatMs = 100
		d.Cluster.Auth = config.ClusterAuth{Mode: "secret", SecretFile: secret}
		d.Cluster.Raft.TickMs = 10
		d.Cluster.Raft.JoinTimeoutMs = 1000
		d.Cluster.Leases.TTLMs = 2000
		d.Cluster.Leases.TakeoverTimeoutMs = 1000
		d.Cluster.Replication.AckTimeoutMs = 1000
		d.Cluster.Advertise = "client." + name + ":1883"
		if mutate != nil {
			mutate(name, d)
		}
	})
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, f := range nodes {
			if !f.srv.Consensus().Ready() || f.srv.Leases().Fenced() {
				ok = false
			}
			for _, p := range f.srv.Cluster().Peers() {
				if p.State != transport.Up {
					ok = false
				}
			}
			for _, other := range names {
				if _, known := f.srv.Leases().Table().Node(other); !known {
					ok = false
				}
			}
		}
		if ok {
			return nodes
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("the cluster did not settle")
	return nil
}

// TestLiveTakeoverAcrossBrokers: a client with a subscription connects to a
// second broker with Clean Start 0; the first broker's connection ends with
// Session Taken Over, the second reports Session Present, and a publish on
// the second broker reaches the client through the moved subscription.
func TestLiveTakeoverAcrossBrokers(t *testing.T) {
	nodes := startTrio(t, nil)
	exp := uint32(300)
	x1, ack := nodes["n1"].connect("tcp", packet.V5, &packet.Connect{ClientID: "x", CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("connect on n1: %s", ack.ReasonCode)
	}
	x1.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "t/x", QoS: 1}}})
	if _, ok := x1.read().(*packet.Suback); !ok {
		t.Fatal("no SUBACK")
	}
	first, ok := nodes["n1"].srv.Leases().Table().Lookup("default", "x")
	if !ok || first.Owner != "n1" || first.Epoch == 0 {
		t.Fatalf("lease after connect %+v %v", first, ok)
	}
	x2, ack := nodes["n2"].connect("tcp", packet.V5, &packet.Connect{ClientID: "x", CleanStart: false, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	if ack.ReasonCode != packet.Success || !ack.SessionPresent {
		t.Fatalf("connect on n2: %+v", ack)
	}
	// The first connection was told the session moved.
	if d, ok := x1.read().(*packet.Disconnect); !ok || d.ReasonCode != packet.SessionTakenOver {
		t.Fatalf("n1 connection: %+v", d)
	}
	if l, _ := nodes["n1"].srv.Leases().Table().Lookup("default", "x"); l.Owner != "n2" || l.Epoch <= first.Epoch {
		t.Fatalf("lease after takeover %+v", l)
	}
	// The subscription travelled: a publish on n2 is delivered to x on n2.
	pub, _ := nodes["n2"].connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	pub.send(&packet.Publish{Topic: "t/x", QoS: 1, PacketID: 5, Payload: []byte("moved")})
	pub.read()
	if m, ok := x2.read().(*packet.Publish); !ok || string(m.Payload) != "moved" {
		t.Fatalf("delivery after takeover: %+v", m)
	}
	// n1 no longer holds the session.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := nodes["n1"].srv.Sessions().Record("x"); !ok {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, ok := nodes["n1"].srv.Sessions().Record("x"); ok {
		t.Fatal("n1 still holds x")
	}
}

// TestMinorityRefusesNewClients: a broker cut off from the other two fences
// its clients within the ttl and refuses a new client with a redirect.
func TestMinorityRefusesNewClients(t *testing.T) {
	nodes := startTrio(t, nil)
	cc := nodes["n3"]
	exp := uint32(60)
	x, ack := cc.connect("tcp", packet.V5, &packet.Connect{ClientID: "x", CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("connect on n3: %s", ack.ReasonCode)
	}
	for _, other := range []string{"n1", "n2"} {
		nodes[other].srv.Cluster().Isolate("n3", true)
		cc.srv.Cluster().Isolate(other, true)
	}
	// Within the ttl the minority node fences itself and disconnects its
	// clients (Use Another Server when a majority node is known, else Server
	// Busy; 0x88 is a CONNACK-only code).
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && !cc.srv.Leases().Fenced() {
		time.Sleep(50 * time.Millisecond)
	}
	if !cc.srv.Leases().Fenced() {
		t.Fatal("minority node did not fence itself")
	}
	if d, ok := x.read().(*packet.Disconnect); !ok || (d.ReasonCode != packet.UseAnotherServer && d.ReasonCode != packet.ServerBusy) {
		t.Fatalf("fenced client: %+v", d)
	}
	// A new client on the minority node is refused with a redirect to a
	// majority-side node's advertised address, or unavailable.
	_, ack = cc.connect("tcp", packet.V5, &packet.Connect{ClientID: "y", CleanStart: true}, false)
	switch ack.ReasonCode {
	case packet.UseAnotherServer:
		if ack.Properties.ServerReference == nil || *ack.Properties.ServerReference == "" {
			t.Fatal("redirect without a Server Reference")
		}
	case packet.ServerUnavailable:
	default:
		t.Fatalf("minority connect: %s", ack.ReasonCode)
	}
	// The majority takes the client over once the lease lapsed, without a
	// second live session in between (n3 fenced first).
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		_, ack = nodes["n1"].connect("tcp", packet.V5, &packet.Connect{ClientID: "x", CleanStart: false, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
		if ack.ReasonCode == packet.Success {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if ack.ReasonCode != packet.Success {
		t.Fatalf("majority takeover: %s", ack.ReasonCode)
	}
	// Heal: n3 renews and serves again.
	for _, other := range []string{"n1", "n2"} {
		nodes[other].srv.Cluster().Isolate("n3", false)
		cc.srv.Cluster().Isolate(other, false)
	}
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && cc.srv.Leases().Fenced() {
		time.Sleep(50 * time.Millisecond)
	}
	if _, ack = cc.connect("tcp", packet.V5, &packet.Connect{ClientID: "z", CleanStart: true}, false); ack.ReasonCode != packet.Success {
		t.Fatalf("connect after heal: %s", ack.ReasonCode)
	}
}
