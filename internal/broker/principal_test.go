package broker

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/bans"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

// startPrincipals starts a node with password users alice and mallory, both
// allowed every topic by the fixture's ACL; mutate runs after.
func startPrincipals(t *testing.T, mutate func(*config.Document)) *fixture {
	t.Helper()
	alice, _ := auth.HashPassword([]byte("pa"))
	mallory, _ := auth.HashPassword([]byte("pm"))
	users := filepath.Join(t.TempDir(), "users.json")
	if err := os.WriteFile(users, []byte(`{"users":[{"username":"alice","password":"`+alice+`"},{"username":"mallory","password":"`+mallory+`"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	return start(t, func(d *config.Document) {
		d.Auth.Anonymous = false
		d.Modules["auth.file"] = json.RawMessage(`{"path":"` + users + `"}`)
		if mutate != nil {
			mutate(d)
		}
	})
}

func asUser(user string, clean bool) *packet.Connect {
	expiry := uint32(3600)
	pw := map[string]string{"alice": "pa", "mallory": "pm"}[user]
	return &packet.Connect{ClientID: "c1", CleanStart: clean, KeepAlive: 30, HasUsername: true, Username: user,
		HasPassword: true, Password: []byte(pw), Properties: packet.Properties{SessionExpiryInterval: &expiry}}
}

// queueForAlice leaves alice's persistent session for c1 offline with one
// QoS 1 message queued on t/a.
func queueForAlice(t *testing.T, f *fixture) {
	t.Helper()
	cl, ack := f.connect("tcp", packet.V5, asUser("alice", true), false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("alice: %s", ack.ReasonCode)
	}
	cl.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "t/a", QoS: 1}}})
	if s, ok := cl.read().(*packet.Suback); !ok || s.ReasonCodes[0] != 1 {
		t.Fatalf("subscribe: %+v", s)
	}
	cl.send(&packet.Disconnect{})
	waitOffline(t, f, "c1")
	pub, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true, HasUsername: true, Username: "alice", HasPassword: true, Password: []byte("pa")}, false)
	pub.send(&packet.Publish{Topic: "t/a", QoS: 1, PacketID: 1, Payload: []byte("for alice")})
	if a, ok := pub.read().(*packet.Puback); !ok || a.ReasonCode != packet.Success {
		t.Fatalf("publish: %+v", a)
	}
}

// aliceGetsHerSession reconnects alice as c1 and expects the session and the
// queued message.
func aliceGetsHerSession(t *testing.T, f *fixture) {
	t.Helper()
	cl, ack := f.connect("tcp", packet.V5, asUser("alice", false), false)
	if ack.ReasonCode != packet.Success || !ack.SessionPresent {
		t.Fatalf("alice back: %s present=%v", ack.ReasonCode, ack.SessionPresent)
	}
	p, ok := cl.read().(*packet.Publish)
	if !ok || string(p.Payload) != "for alice" {
		t.Fatalf("alice's queued message: %+v", p)
	}
}

// Another principal resuming alice's session is refused and given nothing;
// alice then resumes it with its queue.
func TestResumeUnderAnotherPrincipalRefused(t *testing.T) {
	f := startPrincipals(t, nil)
	queueForAlice(t, f)
	if _, ack := f.connect("tcp", packet.V5, asUser("mallory", false), false); ack.ReasonCode != packet.NotAuthorized || ack.SessionPresent {
		t.Fatalf("mallory resuming c1: %s present=%v", ack.ReasonCode, ack.SessionPresent)
	}
	aliceGetsHerSession(t, f)
}

// A Clean Start by another principal is refused too: it would discard the
// session's queue.
func TestCleanStartUnderAnotherPrincipalRefused(t *testing.T) {
	f := startPrincipals(t, nil)
	queueForAlice(t, f)
	if _, ack := f.connect("tcp", packet.V5, asUser("mallory", true), false); ack.ReasonCode != packet.NotAuthorized {
		t.Fatalf("mallory clean-starting c1: %s", ack.ReasonCode)
	}
	aliceGetsHerSession(t, f)
}

// The refusal leaves the rightful client's live connection alone.
func TestLiveConnectionUntouchedByAnotherPrincipal(t *testing.T) {
	f := startPrincipals(t, nil)
	cl, ack := f.connect("tcp", packet.V5, asUser("alice", true), false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("alice: %s", ack.ReasonCode)
	}
	cl.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "t/a", QoS: 1}}})
	cl.read()
	if _, ack := f.connect("tcp", packet.V5, asUser("mallory", false), false); ack.ReasonCode != packet.NotAuthorized {
		t.Fatalf("mallory: %s", ack.ReasonCode)
	}
	cl.send(&packet.Pingreq{})
	if p := cl.read(); p.Type() != packet.PINGRESP {
		t.Fatalf("alice's connection after mallory's CONNECT: %T %+v", p, p)
	}
	pub, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true, HasUsername: true, Username: "mallory", HasPassword: true, Password: []byte("pm")}, false)
	pub.send(&packet.Publish{Topic: "t/a", QoS: 1, PacketID: 1, Payload: []byte("live")})
	pub.read()
	if p, ok := cl.read().(*packet.Publish); !ok || string(p.Payload) != "live" {
		t.Fatalf("alice's delivery: %+v", p)
	}
}

// With auth.sessionBinding off another principal resumes the session, as
// before the binding.
func TestSessionBindingOff(t *testing.T) {
	f := startPrincipals(t, func(d *config.Document) { d.Auth.SessionBinding = "off" })
	queueForAlice(t, f)
	if _, ack := f.connect("tcp", packet.V5, asUser("mallory", false), false); ack.ReasonCode != packet.Success || !ack.SessionPresent {
		t.Fatalf("mallory resuming c1 with the binding off: %s present=%v", ack.ReasonCode, ack.SessionPresent)
	}
}

// dropAs connects as user with client ID c1 and a session that ends with
// the connection, and closes the socket without a DISCONNECT; a persistent
// session would bind c1 to user, which is not what this measures.
func dropAs(t *testing.T, f *fixture, user string) {
	t.Helper()
	c := asUser(user, true)
	c.Properties.SessionExpiryInterval = nil
	cl, ack := f.connect("tcp", packet.V5, c, false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("%s: %s", user, ack.ReasonCode)
	}
	_ = cl.c.Close()
	deadline := time.Now().Add(5 * time.Second)
	for _, held := f.srv.Sessions().Record("c1"); held; _, held = f.srv.Sessions().Record("c1") {
		if time.Now().After(deadline) {
			t.Fatal("precondition: c1's session outlived its connection")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Flapping is counted and banned per client ID and principal: mallory
// flapping c1, which has no session, bans mallory's use of c1 only.
func TestFlappingBansThePrincipalsUseOfTheClientID(t *testing.T) {
	f := startPrincipals(t, func(d *config.Document) {
		d.Flapping = config.Flapping{Enabled: true, WindowSeconds: 10, MaxCount: 3, BanSeconds: 30}
	})
	for i := 0; i < 3; i++ {
		dropAs(t, f, "mallory")
		time.Sleep(100 * time.Millisecond)
	}
	waitTrips(t, f, 1)
	waitIndexed(t, f, "clientid-username:mallory/c1", true)
	if _, ok := f.srv.records.Load().Get(bans.Collection, "clientid:c1"); ok {
		t.Fatal("mallory's flapping banned the client ID for everyone")
	}
	if _, ack := f.connect("tcp", packet.V5, asUser("mallory", true), false); ack.ReasonCode != packet.Banned {
		t.Fatalf("mallory after the trip: %s", ack.ReasonCode)
	}
	if _, ack := f.connect("tcp", packet.V5, asUser("alice", true), false); ack.ReasonCode != packet.Success {
		t.Fatalf("alice after mallory's trip: %s", ack.ReasonCode)
	}
}

// Across nodes the takeover runs before the principals can be compared: a
// refused CONNECT on n2 for alice's session on n1 closes alice's connection
// there (Session taken over), and n2 holds the session offline, its lease
// recorded offline as a disconnect leaves it; alice resumes it with its
// subscription. Nothing reaches the refused principal.
func TestCrossNodeRefusalLeavesTheSessionOffline(t *testing.T) {
	alice, _ := auth.HashPassword([]byte("pa"))
	mallory, _ := auth.HashPassword([]byte("pm"))
	users := filepath.Join(t.TempDir(), "users.json")
	if err := os.WriteFile(users, []byte(`{"users":[{"username":"alice","password":"`+alice+`"},{"username":"mallory","password":"`+mallory+`"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	nodes := startTrio(t, func(_ string, d *config.Document) {
		d.Auth.Anonymous = false
		d.Modules["auth.file"] = json.RawMessage(`{"path":"` + users + `"}`)
	})
	a1, ack := nodes["n1"].connect("tcp", packet.V5, asUser("alice", true), false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("alice on n1: %s", ack.ReasonCode)
	}
	a1.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "t/a", QoS: 1}}})
	a1.read()
	if _, ack := nodes["n2"].connect("tcp", packet.V5, asUser("mallory", false), false); ack.ReasonCode != packet.NotAuthorized || ack.SessionPresent {
		t.Fatalf("mallory on n2: %s present=%v", ack.ReasonCode, ack.SessionPresent)
	}
	if d, ok := a1.read().(*packet.Disconnect); !ok || d.ReasonCode != packet.SessionTakenOver {
		t.Fatalf("alice's n1 connection: %+v", d)
	}
	waitOffline(t, nodes["n2"], "c1")
	deadline := time.Now().Add(5 * time.Second)
	for {
		l, ok := nodes["n2"].srv.Leases().Table().Lookup("default", "c1")
		if ok && l.Owner == "n2" && !l.Online {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the lease after the refusal: %+v %v, want n2's and offline", l, ok)
		}
		time.Sleep(20 * time.Millisecond)
	}
	a2, ack := nodes["n2"].connect("tcp", packet.V5, asUser("alice", false), false)
	if ack.ReasonCode != packet.Success || !ack.SessionPresent {
		t.Fatalf("alice on n2: %s present=%v", ack.ReasonCode, ack.SessionPresent)
	}
	pub, _ := nodes["n2"].connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true, HasUsername: true, Username: "alice", HasPassword: true, Password: []byte("pa")}, false)
	pub.send(&packet.Publish{Topic: "t/a", QoS: 1, PacketID: 1, Payload: []byte("after")})
	pub.read()
	if p, ok := a2.read().(*packet.Publish); !ok || string(p.Payload) != "after" {
		t.Fatalf("alice's delivery on n2: %+v", p)
	}
}

// At MQTT 3.1.1 the refusal is return code 5, Not authorized.
func TestPrincipalRefusalAt311(t *testing.T) {
	f := startPrincipals(t, nil)
	queueForAlice(t, f)
	c := asUser("mallory", false)
	c.Properties = packet.Properties{}
	if _, ack := f.connect("tcp", packet.V4, c, false); ack.ReasonCode != packet.NotAuthorized {
		t.Fatalf("mallory at 3.1.1: %s", ack.ReasonCode)
	}
	aliceGetsHerSession(t, f)
}

// A session that has expired binds nothing: another principal may then use
// its client ID.
func TestExpiredSessionBindsNothing(t *testing.T) {
	f := startPrincipals(t, nil)
	c := asUser("alice", true)
	one := uint32(1)
	c.Properties.SessionExpiryInterval = &one
	cl, ack := f.connect("tcp", packet.V5, c, false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("alice: %s", ack.ReasonCode)
	}
	cl.send(&packet.Disconnect{})
	waitOffline(t, f, "c1")
	deadline := time.Now().Add(5 * time.Second)
	for _, held := f.srv.Sessions().Record("c1"); held; _, held = f.srv.Sessions().Record("c1") {
		if time.Now().After(deadline) {
			t.Fatal("precondition: alice's session did not expire")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, ack := f.connect("tcp", packet.V5, asUser("mallory", false), false); ack.ReasonCode != packet.Success || ack.SessionPresent {
		t.Fatalf("mallory after alice's session expired: %s present=%v", ack.ReasonCode, ack.SessionPresent)
	}
}
