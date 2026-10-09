package conformance

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/auth/scramtest"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

// scramNode is a node offering SCRAM-SHA-256 over a users file holding
// "dev" with the password "pencil".
func scramNode(t *testing.T) *Node {
	t.Helper()
	cred, err := auth.HashSCRAM([]byte("pencil"), auth.MinSCRAMIterations)
	if err != nil {
		t.Fatal(err)
	}
	users := filepath.Join(t.TempDir(), "users.json")
	if err := os.WriteFile(users, []byte(`{"users":[{"username":"dev","password":"`+cred+`"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	return StartNode(t, func(d *config.Document) {
		d.Modules["auth.scram"] = json.RawMessage(`{"path":"` + users + `"}`)
	})
}

func methodOf(p packet.Properties) string {
	if p.AuthenticationMethod == nil {
		return ""
	}
	return *p.AuthenticationMethod
}

// authenticateReason is MQTT-3.15.2-1's set: the Authenticate Reason Codes.
func authenticateReason(c packet.ReasonCode) bool {
	return c == packet.Success || c == packet.ContinueAuthentication || c == packet.ReAuthenticate
}

// scramConnect runs the exchange over raw frames and returns the CONNACK.
func scramConnect(t *testing.T, r *Raw, c *scramtest.Client) *packet.Connack {
	t.Helper()
	method := auth.SCRAMMethod
	r.Send(&packet.Connect{ProtocolLevel: packet.V5, ClientID: "scram-" + c.User, CleanStart: true,
		Properties: packet.Properties{AuthenticationMethod: &method, AuthenticationData: c.First()}})
	a, ok := r.Read().(*packet.Auth)
	if !ok {
		t.Fatal("no AUTH after a CONNECT naming SCRAM-SHA-256")
	}
	if a.ReasonCode != packet.ContinueAuthentication || methodOf(a.Properties) != method || !authenticateReason(a.ReasonCode) {
		t.Fatalf("the server's AUTH: %s, method %q", a.ReasonCode, methodOf(a.Properties))
	}
	r.Send(&packet.Auth{ReasonCode: packet.ContinueAuthentication, Properties: packet.Properties{AuthenticationMethod: &method, AuthenticationData: c.Final(a.Properties.AuthenticationData)}})
	ack, ok := r.Read().(*packet.Connack)
	if !ok {
		t.Fatal("no CONNACK after the client-final AUTH")
	}
	return ack
}

// Enhanced authentication (§ 4.12) with SCRAM-SHA-256.
func TestEnhancedAuthSCRAM(t *testing.T) {
	node := scramNode(t)
	t.Run("unsupported method", func(t *testing.T) {
		Statement(t, "MQTT-4.12.0-1")
		r := node.For(t).DialRaw(packet.V5)
		method := "X-UNSUPPORTED"
		ack := r.Connect(&packet.Connect{ClientID: "scram-x", CleanStart: true, Properties: packet.Properties{AuthenticationMethod: &method}})
		if ack.ReasonCode != packet.BadAuthenticationMethod {
			t.Fatalf("CONNACK %s", ack.ReasonCode)
		}
		r.ExpectClosed()
	})
	t.Run("exchange completes", func(t *testing.T) {
		Statement(t, "MQTT-4.12.0-2", "MQTT-4.12.0-5", "MQTT-3.15.2-1")
		r := node.For(t).DialRaw(packet.V5)
		c := &scramtest.Client{User: "dev", Password: "pencil", Nonce: "c0nf0rmance"}
		ack := scramConnect(t, r, c)
		if ack.ReasonCode != packet.Success || methodOf(ack.Properties) != auth.SCRAMMethod || !c.Verify(ack.Properties.AuthenticationData) {
			t.Fatalf("CONNACK %s, method %q, server proof verified %v", ack.ReasonCode, methodOf(ack.Properties), c.Verify(ack.Properties.AuthenticationData))
		}
		// Re-authentication: every server AUTH carries the method and an
		// Authenticate Reason Code.
		method := auth.SCRAMMethod
		again := &scramtest.Client{User: "dev", Password: "pencil", Nonce: "again"}
		r.Send(&packet.Auth{ReasonCode: packet.ReAuthenticate, Properties: packet.Properties{AuthenticationMethod: &method, AuthenticationData: again.First()}})
		a := r.Read().(*packet.Auth)
		if a.ReasonCode != packet.ContinueAuthentication || methodOf(a.Properties) != method {
			t.Fatalf("re-authentication's AUTH: %s %q", a.ReasonCode, methodOf(a.Properties))
		}
		r.Send(&packet.Auth{ReasonCode: packet.ContinueAuthentication, Properties: packet.Properties{AuthenticationMethod: &method, AuthenticationData: again.Final(a.Properties.AuthenticationData)}})
		done := r.Read().(*packet.Auth)
		if done.ReasonCode != packet.Success || methodOf(done.Properties) != method || !authenticateReason(done.ReasonCode) || !again.Verify(done.Properties.AuthenticationData) {
			t.Fatalf("re-authentication's success: %s %q", done.ReasonCode, methodOf(done.Properties))
		}
	})
	t.Run("wrong proof", func(t *testing.T) {
		Statement(t, "MQTT-4.12.0-4")
		r := node.For(t).DialRaw(packet.V5)
		ack := scramConnect(t, r, &scramtest.Client{User: "dev", Password: "wrong", Nonce: "n"})
		if ack.ReasonCode < 0x80 {
			t.Fatalf("CONNACK %s", ack.ReasonCode)
		}
		r.ExpectClosed()
	})
	t.Run("failed re-authentication", func(t *testing.T) {
		Statement(t, "MQTT-4.12.1-2")
		r := node.For(t).DialRaw(packet.V5)
		if ack := scramConnect(t, r, &scramtest.Client{User: "dev", Password: "pencil", Nonce: "n1"}); ack.ReasonCode != packet.Success {
			t.Fatalf("CONNACK %s", ack.ReasonCode)
		}
		method := auth.SCRAMMethod
		bad := &scramtest.Client{User: "dev", Password: "wrong", Nonce: "n2"}
		r.Send(&packet.Auth{ReasonCode: packet.ReAuthenticate, Properties: packet.Properties{AuthenticationMethod: &method, AuthenticationData: bad.First()}})
		a := r.Read().(*packet.Auth)
		r.Send(&packet.Auth{ReasonCode: packet.ContinueAuthentication, Properties: packet.Properties{AuthenticationMethod: &method, AuthenticationData: bad.Final(a.Properties.AuthenticationData)}})
		if d := r.ReadEnd(); d == nil || d.ReasonCode < 0x80 {
			t.Fatalf("DISCONNECT %+v", d)
		}
	})
}
