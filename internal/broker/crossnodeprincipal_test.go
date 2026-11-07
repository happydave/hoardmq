package broker

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

// startPrincipalTrio starts n1–n3 with password users alice and mallory, and
// metrics on each node's observability listener.
func startPrincipalTrio(t *testing.T) map[string]*fixture {
	t.Helper()
	alice, _ := auth.HashPassword([]byte("pa"))
	mallory, _ := auth.HashPassword([]byte("pm"))
	users := filepath.Join(t.TempDir(), "users.json")
	if err := os.WriteFile(users, []byte(`{"users":[{"username":"alice","password":"`+alice+`"},{"username":"mallory","password":"`+mallory+`"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	return startTrio(t, func(_ string, d *config.Document) {
		d.Auth.Anonymous = false
		d.Modules["auth.file"] = json.RawMessage(`{"path":"` + users + `"}`)
		d.Modules["metrics.prometheus"] = json.RawMessage(`{}`)
		d.Observability.Address = "127.0.0.1:0"
	})
}

// TestRefusedCrossNodeAttemptsLeaveTheOwnerConnected: alice holds c1 on n1;
// mallory CONNECTs as c1 on n2 twenty times. Each attempt is refused (Not
// authorized) from n2's lease table, before any takeover, so alice's
// connection is never closed, and n2 counts each refusal. Alice then moves
// to n2 herself: the same principal takes her session over, queue and all.
func TestRefusedCrossNodeAttemptsLeaveTheOwnerConnected(t *testing.T) {
	nodes := startPrincipalTrio(t)
	a, ack := nodes["n1"].connect("tcp", packet.V5, asUser("alice", true), false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("alice on n1: %s", ack.ReasonCode)
	}
	a.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "t/a", QoS: 1}}})
	if s, ok := a.read().(*packet.Suback); !ok || s.ReasonCodes[0] != 1 {
		t.Fatalf("subscribe: %+v", s)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if l, ok := nodes["n2"].srv.Leases().Table().Lookup("default", "c1"); ok && l.Owner == "n1" && l.Principal != 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("precondition: n2's table holds alice's lease with its digest")
		}
		time.Sleep(10 * time.Millisecond)
	}
	const attempts = 20
	began := time.Now()
	for i := 0; i < attempts; i++ {
		if _, ack := nodes["n2"].connect("tcp", packet.V5, asUser("mallory", false), false); ack.ReasonCode != packet.NotAuthorized || ack.SessionPresent {
			t.Fatalf("mallory's attempt %d on n2: %s present=%v", i, ack.ReasonCode, ack.SessionPresent)
		}
	}
	window := time.Since(began)
	a.send(&packet.Pingreq{})
	if p := a.read(); p.Type() != packet.PINGRESP {
		t.Fatalf("alice's n1 connection after mallory's attempts: %T %+v", p, p)
	}
	if l, ok := nodes["n1"].srv.Leases().Table().Lookup("default", "c1"); !ok || l.Owner != "n1" || !l.Online {
		t.Fatalf("alice's lease after mallory's attempts: %+v %v, want n1's and online", l, ok)
	}
	_, body := scrape(t, nodes["n2"], "/metrics")
	for _, series := range []string{`hoardmq_connect_refusals_total{reason="principal"}`, `hoardmq_lease_ops_total{op="acquire",outcome="principal"}`} {
		if v, _ := metricValue(t, body, series); v != attempts {
			t.Errorf("n2's %s = %v, want %d", series, v, attempts)
		}
	}
	t.Logf("REGRESSION %d refused attempts on n2 in %v; alice's n1 connection closed 0 times", attempts, window.Round(time.Millisecond))

	pub, _ := nodes["n1"].connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true, HasUsername: true, Username: "alice", HasPassword: true, Password: []byte("pa")}, false)
	a.send(&packet.Disconnect{})
	waitOffline(t, nodes["n1"], "c1")
	pub.send(&packet.Publish{Topic: "t/a", QoS: 1, PacketID: 1, Payload: []byte("for alice")})
	if p, ok := pub.read().(*packet.Puback); !ok || p.ReasonCode != packet.Success {
		t.Fatalf("publish: %+v", p)
	}
	a2, ack := nodes["n2"].connect("tcp", packet.V5, asUser("alice", false), false)
	if ack.ReasonCode != packet.Success || !ack.SessionPresent {
		t.Fatalf("alice moving to n2: %s present=%v", ack.ReasonCode, ack.SessionPresent)
	}
	if p, ok := a2.read().(*packet.Publish); !ok || string(p.Payload) != "for alice" {
		t.Fatalf("alice's queued message on n2: %+v", p)
	}
}
