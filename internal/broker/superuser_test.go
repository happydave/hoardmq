package broker

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/module/testmod"
	"github.com/happydave/hoardmq/internal/packet"
)

// TestSuperuserOnlyFromAuthHTTPOnANode: auth.http's superuser attribute,
// attached `true` by each other source, grants nothing; only auth.http's own
// is_superuser lets a publish through without a request. Each source:
//   - a users-file user (auth.file admits);
//   - an auth.db credential written through the admin API;
//   - a mechanism's success (the test module's);
//   - another module's allow (the test module's);
//   - a member that passes leaving the attribute, auth.http admitting.
func TestSuperuserOnlyFromAuthHTTPOnANode(t *testing.T) {
	var mu sync.Mutex
	asked := map[string]int{}
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		_ = json.NewDecoder(r.Body).Decode(&req)
		user, _ := req["username"].(string)
		if req["action"] == "connect" {
			_ = json.NewEncoder(w).Encode(map[string]any{"result": "allow", "is_superuser": user == "root"})
			return
		}
		mu.Lock()
		asked[user]++
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]string{"result": "deny"})
	}))
	defer endpoint.Close()
	askedOf := func(user string) int {
		mu.Lock()
		defer mu.Unlock()
		return asked[user]
	}
	super := map[string]string{"auth.http.superuser": "true"}

	phc, err := auth.HashPassword([]byte("pw"))
	if err != nil {
		t.Fatal(err)
	}
	users := filepath.Join(t.TempDir(), "users.json")
	if err := os.WriteFile(users, []byte(`{"users":[{"username":"fileuser","password":"`+phc+`","attributes":{"auth.http.superuser":"true"}}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	testmod.Register()
	testmod.Shared.Reset()
	testmod.Shared.Set(func(c *testmod.Control) { c.AuthResult, c.MechAttributes = auth.Result{}, super })
	t.Cleanup(func() {
		testmod.Shared.Set(func(c *testmod.Control) { c.AuthResult, c.MechAttributes = auth.Result{}, nil })
	})
	f := start(t, func(d *config.Document) {
		withAdmin(t, t.TempDir())(d)
		d.Auth.Anonymous = false
		d.Modules["auth.db"] = json.RawMessage(`{}`)
		d.Modules["auth.file"] = json.RawMessage(`{"path":"` + users + `"}`)
		d.Modules["auth.http"] = json.RawMessage(`{"url":"` + endpoint.URL + `"}`)
		d.Modules[string(testmod.ID)] = json.RawMessage(`{}`)
		// The test module first in both chains, so it can pass or allow ahead
		// of the others.
		d.Hooks.Order = []string{string(testmod.ID)}
	})
	if code, data := api(t, f, "POST", "/api/v1/credentials", adminOps, `{"username":"dbuser","password":"pw","attributes":{"auth.http.superuser":"true"}}`); code != 201 {
		t.Fatalf("the auth.db credential: %d %s", code, data)
	}
	// auth.http's endpoint admits anyone, so wait for the credential itself.
	adminWait(t, "the auth.db credential applied", func() bool {
		return metric(f, "hoardmq_records", map[string]string{"collection": "auth.users"}) == 1
	})

	method := testmod.Method
	publishes := func(name string, connect *packet.Connect, before func()) packet.ReasonCode {
		t.Helper()
		if before != nil {
			before()
		}
		cl, ack := f.connect("tcp", packet.V5, connect, false)
		if ack.ReasonCode != packet.Success {
			t.Fatalf("%s: CONNACK %s", name, ack.ReasonCode)
		}
		cl.send(&packet.Publish{Topic: "s/deny", QoS: 1, PacketID: 1, Payload: []byte("x")})
		code := cl.read().(*packet.Puback).ReasonCode
		_ = cl.c.Close()
		return code
	}
	login := func(user string) *packet.Connect {
		return &packet.Connect{ClientID: "c-" + user, CleanStart: true, HasUsername: true, Username: user, HasPassword: true, Password: []byte("pw")}
	}
	for _, c := range []struct {
		name, user string
		connect    *packet.Connect
		before     func()
	}{
		{"the users file", "fileuser", login("fileuser"), nil},
		{"auth.db", "dbuser", login("dbuser"), nil},
		{"a mechanism", "mechuser", &packet.Connect{ClientID: "c-mechuser", CleanStart: true, HasUsername: true, Username: "mechuser",
			Properties: packet.Properties{AuthenticationMethod: &method}}, nil},
		{"another module", "moduser", login("moduser"), func() {
			testmod.Shared.Set(func(c *testmod.Control) { c.AuthResult = auth.Result{Decision: auth.Allow, Attributes: super} })
		}},
		{"a member that passes", "plain", login("plain"), func() {
			testmod.Shared.Set(func(c *testmod.Control) { c.AuthResult = auth.Result{Decision: auth.Pass, Attributes: super} })
		}},
	} {
		n := askedOf(c.user)
		if code := publishes(c.name, c.connect, c.before); code != packet.NotAuthorized || askedOf(c.user) != n+1 {
			t.Errorf("%s: publish answered %s after %d requests, want 0x87 after the endpoint was asked", c.name, code, askedOf(c.user)-n)
		}
		testmod.Shared.Set(func(c *testmod.Control) { c.AuthResult = auth.Result{} })
	}
	if calls := testmod.Shared.Recorded(); index(calls, "mechanism mechuser") < 0 {
		t.Fatalf("the test mechanism did not admit mechuser:\n%s", strings.Join(calls, "\n"))
	}
	if code := publishes("auth.http's superuser", login("root"), nil); code >= 0x80 || askedOf("root") != 0 {
		t.Fatalf("root: publish answered %s after %d requests, want accepted with none", code, askedOf("root"))
	}
}
