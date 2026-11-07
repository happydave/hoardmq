package broker

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

// TestSuperuserEndsAtTheNextRedecision: auth.http's endpoint says
// is_superuser at the CONNECT and not after. Before the re-decision a publish
// the endpoint would deny is allowed without a request. At the re-decision the
// connection is closed with 0x98, and the reconnect's publish is asked about
// and refused. A user whose answer does not change stays connected across
// re-decisions.
func TestSuperuserEndsAtTheNextRedecision(t *testing.T) {
	var mu sync.Mutex
	connects, asked := map[string]int{}, map[string]int{}
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		_ = json.NewDecoder(r.Body).Decode(&req)
		user, _ := req["username"].(string)
		mu.Lock()
		defer mu.Unlock()
		if req["action"] == "connect" {
			connects[user]++
			_ = json.NewEncoder(w).Encode(map[string]any{"result": "allow", "is_superuser": user == "root" && connects[user] == 1})
			return
		}
		asked[user]++
		_ = json.NewEncoder(w).Encode(map[string]string{"result": "deny"})
	}))
	defer endpoint.Close()
	count := func(m map[string]int, user string) int {
		mu.Lock()
		defer mu.Unlock()
		return m[user]
	}
	f := start(t, func(d *config.Document) {
		d.Auth.Anonymous = false
		d.Auth.DecisionTTLSeconds = 1
		d.Modules["auth.http"] = json.RawMessage(`{"url":"` + endpoint.URL + `"}`)
	})
	expiry := uint32(600)
	login := func(user string) *packet.Connect {
		return &packet.Connect{ClientID: "c-" + user, CleanStart: false, HasUsername: true, Username: user, HasPassword: true, Password: []byte("pw"),
			Properties: packet.Properties{SessionExpiryInterval: &expiry}}
	}
	publish := func(cl *client, id uint16) packet.ReasonCode {
		cl.send(&packet.Publish{Topic: "s/deny", QoS: 1, PacketID: id, Payload: []byte("x")})
		return cl.read().(*packet.Puback).ReasonCode
	}

	root, ack := f.connect("tcp", packet.V5, login("root"), false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("root: %s", ack.ReasonCode)
	}
	if code := publish(root, 1); code >= 0x80 || count(asked, "root") != 0 {
		t.Fatalf("root as superuser: %s after %d requests, want accepted with none", code, count(asked, "root"))
	}
	steady, ack := f.connect("tcp", packet.V5, login("steady"), false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("steady: %s", ack.ReasonCode)
	}

	// The re-decision comes within the decision TTL of 1 s.
	var end *packet.Disconnect
	deadline := time.Now().Add(5 * time.Second)
	for end == nil && time.Now().Before(deadline) {
		switch p := root.read().(type) {
		case *packet.Disconnect:
			end = p
		default:
			t.Fatalf("root was sent %T before its re-decision", p)
		}
	}
	if end == nil || end.ReasonCode != packet.AdministrativeAction {
		t.Fatalf("root's re-decision: %+v, want DISCONNECT 0x98", end)
	}
	again, ack := f.connect("tcp", packet.V5, login("root"), false)
	if ack.ReasonCode != packet.Success || !ack.SessionPresent {
		t.Fatalf("root's reconnect: %+v, want its session present", ack)
	}
	if code := publish(again, 2); code != packet.NotAuthorized || count(asked, "root") != 1 {
		t.Fatalf("root no longer a superuser: %s after %d requests, want 0x87 after one", code, count(asked, "root"))
	}

	// steady's answer never changed: held across at least two re-decisions.
	for count(connects, "steady") < 3 && time.Now().Before(deadline.Add(5*time.Second)) {
		time.Sleep(50 * time.Millisecond)
	}
	if n := count(connects, "steady"); n < 3 {
		t.Fatalf("steady was decided %d times, want at least 3", n)
	}
	steady.send(&packet.Pingreq{})
	if _, ok := steady.read().(*packet.Pingresp); !ok {
		t.Fatal("steady's connection was ended")
	}
}
