package broker

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/audit"
	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/auth/scramtest"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/oidc/oidctest"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/testcert"
)

// metric is a series' value from the node's metrics provider.
func metric(f *fixture, name string, labels map[string]string) float64 {
	for _, m := range f.srv.Registry().Metrics().Snapshot() {
		if m.Name != name || len(m.Labels) != len(labels) {
			continue
		}
		match := true
		for k, v := range labels {
			match = match && m.Labels[k] == v
		}
		if match {
			return m.Value
		}
	}
	return 0
}

// auditEvents flushes the node's audit log and reads it.
func auditEvents(t *testing.T, f *fixture) []audit.Event {
	t.Helper()
	if err := f.srv.audit.Load().Flush(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(f.srv.AuditDir(), "audit.log"))
	if err != nil {
		t.Fatal(err)
	}
	var out []audit.Event
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		var e audit.Event
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatal(err)
		}
		out = append(out, e)
	}
	return out
}

func deviceToken(t *testing.T, p *oidctest.Provider, change func(map[string]any)) string {
	t.Helper()
	now := time.Now()
	c := map[string]any{"iss": p.URL, "sub": "dev-1", "aud": "hoardmq", "iat": now.Unix(), "exp": now.Add(time.Hour).Unix()}
	if change != nil {
		change(c)
	}
	return p.SignWith(t, "k1-RS256", map[string]any{"typ": "at+jwt"}, c)
}

func deviceACL(t *testing.T) json.RawMessage {
	t.Helper()
	acl := filepath.Join(t.TempDir(), "acl.json")
	_ = os.WriteFile(acl, []byte(`{"rules":[{"topic":"devices/${username}/#","allow":["publish","subscribe"]}]}`), 0o600)
	return json.RawMessage(`{"path":"` + acl + `"}`)
}

func tokenConnect(user, token string) *packet.Connect {
	c := &packet.Connect{ClientID: "dev", CleanStart: true, HasPassword: true, Password: []byte(token)}
	if user != "" {
		c.HasUsername, c.Username = true, user
	}
	return c
}

// AC 1, JWKS: access tokens connect at 5.0 and 3.1.1 and the identity is the
// token's subject; an expired token and one for another audience are refused.
func TestJWTAuthenticationOnANode(t *testing.T) {
	p := oidctest.New(t, "hoardmq")
	f := start(t, func(d *config.Document) {
		d.Auth.Anonymous = false
		d.Modules["authz.file"] = deviceACL(t)
		d.Modules["auth.jwt"] = json.RawMessage(`{"issuer":"` + p.URL + `","audiences":["hoardmq"],"jwksUri":"` + p.URL + `/keys"}`)
	})
	good := deviceToken(t, p, nil)
	cl, ack := f.connect("tcp", packet.V5, tokenConnect("", good), false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("5.0 with a token: %s", ack.ReasonCode)
	}
	cl.send(&packet.Publish{Topic: "devices/dev-1/t", QoS: 1, PacketID: 1})
	if a := cl.read().(*packet.Puback); a.ReasonCode != packet.NoMatchingSubscribers {
		t.Fatalf("its own topic: %s", a.ReasonCode)
	}
	cl.send(&packet.Publish{Topic: "devices/dev-2/t", QoS: 1, PacketID: 2})
	if a := cl.read().(*packet.Puback); a.ReasonCode != packet.NotAuthorized {
		t.Fatalf("another device's topic: %s", a.ReasonCode)
	}
	if _, ack := f.connect("tcp", packet.V4, tokenConnect("dev-1", good), false); ack.ReasonCode != packet.Success {
		t.Fatalf("3.1.1 with a token: %s", ack.ReasonCode)
	}
	expired := deviceToken(t, p, func(c map[string]any) {
		c["exp"], c["iat"] = time.Now().Add(-90*time.Second).Unix(), time.Now().Add(-time.Hour).Unix()
	})
	if _, ack := f.connect("tcp", packet.V5, tokenConnect("dev-1", expired), false); ack.ReasonCode != packet.BadUserNameOrPassword {
		t.Fatalf("5.0, expired: %s", ack.ReasonCode)
	}
	if _, ack := f.connect("tcp", packet.V4, tokenConnect("dev-1", expired), false); ack.ReasonCode != packet.BadUserNameOrPassword {
		t.Fatalf("3.1.1, expired: %s", ack.ReasonCode)
	}
	other := deviceToken(t, p, func(c map[string]any) { c["aud"] = "other" })
	if _, ack := f.connect("tcp", packet.V5, tokenConnect("dev-1", other), false); ack.ReasonCode != packet.BadUserNameOrPassword {
		t.Fatalf("another audience: %s", ack.ReasonCode)
	}
	found := false
	for _, e := range auditEvents(t, f) {
		if e.Action == "auth.failure" && e.Detail["reason"] == "jwt: token expired" {
			found = true
		}
		for _, v := range e.Detail {
			if strings.Contains(v, expired) || strings.Contains(v, good) {
				t.Fatal("an audit record carries a token")
			}
		}
	}
	if !found {
		t.Fatal("no auth.failure record with the expiry's reason")
	}
	if n := metric(f, "hoardmq_auth_jwt_key_fetches_total", map[string]string{"outcome": "ok"}); n < 1 {
		t.Fatalf("key fetches counted %v", n)
	}
}

// A token that expires mid-session ends the session at its expiry.
func TestJWTSessionEndsAtExpiry(t *testing.T) {
	p := oidctest.New(t, "hoardmq")
	f := start(t, func(d *config.Document) {
		d.Auth.Anonymous = false
		d.Modules["auth.jwt"] = json.RawMessage(`{"issuer":"` + p.URL + `","audiences":["hoardmq"],"jwksUri":"` + p.URL + `/keys","clockSkewSeconds":0}`)
	})
	short := deviceToken(t, p, func(c map[string]any) { c["exp"] = time.Now().Add(2 * time.Second).Unix() })
	cl, ack := f.connect("tcp", packet.V5, tokenConnect("dev-1", short), false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("connect: %s", ack.ReasonCode)
	}
	_ = cl.c.SetReadDeadline(time.Now().Add(5 * time.Second))
	pk, err := cl.dec.Read()
	if d, ok := pk.(*packet.Disconnect); err != nil || !ok || d.ReasonCode != packet.NotAuthorized {
		t.Fatalf("at the token's expiry: %+v %v", pk, err)
	}
	if n := metric(f, "hoardmq_auth_rechecks_total", map[string]string{"outcome": "refused"}); n != 1 {
		t.Fatalf("rechecks refused %v", n)
	}
}

// AC 1, introspection: an active token connects; revoked at the provider, it
// is refused; each CONNECT asks.
func TestJWTIntrospectionOnANode(t *testing.T) {
	p := oidctest.New(t, "hoardmq")
	p.Lock()
	p.IntrospectClientID, p.IntrospectSecret = "broker", "s3cret"
	p.Unlock()
	secret := filepath.Join(t.TempDir(), "secret")
	_ = os.WriteFile(secret, []byte("s3cret\n"), 0o600)
	f := start(t, func(d *config.Document) {
		d.Auth.Anonymous = false
		d.Modules["auth.jwt"] = json.RawMessage(`{"issuer":"` + p.URL + `","audiences":["hoardmq"],"introspection":{"endpoint":"` + p.URL + `/introspect","clientId":"broker","clientSecretFile":"` + secret + `"}}`)
	})
	tok := deviceToken(t, p, nil)
	if _, ack := f.connect("tcp", packet.V5, tokenConnect("dev-1", tok), false); ack.ReasonCode != packet.Success {
		t.Fatalf("an active token: %s", ack.ReasonCode)
	}
	p.Revoke(tok)
	if _, ack := f.connect("tcp", packet.V5, tokenConnect("dev-1", tok), false); ack.ReasonCode != packet.BadUserNameOrPassword {
		t.Fatalf("a revoked token: %s", ack.ReasonCode)
	}
	if n := p.Count("introspect"); n != 2 {
		t.Fatalf("%d introspections for two CONNECTs", n)
	}
	if a, i := metric(f, "hoardmq_auth_jwt_introspections_total", map[string]string{"outcome": "active"}), metric(f, "hoardmq_auth_jwt_introspections_total", map[string]string{"outcome": "inactive"}); a != 1 || i != 1 {
		t.Fatalf("introspections counted active %v, inactive %v", a, i)
	}
}

// revocationNode is a node whose mTLS listener checks revocation with the
// configuration given, and a responder for its CA.
func revocationNode(t *testing.T, x509Config string, mutate func(*config.Document)) (*fixture, *testcert.Responder) {
	t.Helper()
	f := start(t, func(d *config.Document) {
		d.Auth.Anonymous = false
		d.Modules["auth.x509"] = json.RawMessage(x509Config)
		if mutate != nil {
			mutate(d)
		}
	})
	return f, testcert.NewResponder(t, f.ca)
}

// useCert makes the fixture's client certificate a new leaf naming the
// responder's endpoints.
func useCert(t *testing.T, f *fixture, r *testcert.Responder, serial int64) {
	t.Helper()
	leaf, err := f.ca.IssueWith("device", serial, testcert.Options{OCSPServer: []string{r.URL + "/ocsp"}, CRLDistributionPoints: []string{r.URL + "/crl"}})
	if err != nil {
		t.Fatal(err)
	}
	f.client = leaf
}

// AC 2: a revoked certificate is refused by OCSP and by CRL; unreachable
// endpoints follow the policy, counted.
func TestCertificateRevocationOnANode(t *testing.T) {
	for _, source := range []string{`{"crl":false}`, `{"ocsp":false}`} {
		f, r := revocationNode(t, source, nil)
		r.Lock()
		r.Revoked[11] = true
		r.Unlock()
		useCert(t, f, r, 10)
		if _, ack := f.connect("mtls", packet.V5, &packet.Connect{ClientID: "d", CleanStart: true}, true); ack.ReasonCode != packet.Success {
			t.Fatalf("%s, good: %s", source, ack.ReasonCode)
		}
		useCert(t, f, r, 11)
		if _, ack := f.connect("mtls", packet.V5, &packet.Connect{ClientID: "d", CleanStart: true}, true); ack.ReasonCode != packet.NotAuthorized {
			t.Fatalf("%s, revoked: %s", source, ack.ReasonCode)
		}
	}
	for _, c := range []struct {
		config string
		want   packet.ReasonCode
		policy string
	}{
		{`{"fetchWaitMs":500}`, packet.NotAuthorized, "closed"},
		{`{"fetchWaitMs":500,"failOpen":true}`, packet.Success, "open"},
	} {
		f, r := revocationNode(t, c.config, nil)
		r.Lock()
		r.Down = true
		r.Unlock()
		useCert(t, f, r, 12)
		if _, ack := f.connect("mtls", packet.V5, &packet.Connect{ClientID: "d", CleanStart: true}, true); ack.ReasonCode != c.want {
			t.Fatalf("unreachable, fail-%s: %s", c.policy, ack.ReasonCode)
		}
		if n := metric(f, "hoardmq_auth_revocation_unknown_total", map[string]string{"policy": c.policy}); n != 1 {
			t.Fatalf("fail-%s counted %v", c.policy, n)
		}
	}
	// A revoked certificate beside a password auth.file accepts is refused.
	phc, _ := auth.HashPassword([]byte("pw"))
	users := filepath.Join(t.TempDir(), "users.json")
	_ = os.WriteFile(users, []byte(`{"users":[{"username":"alice","password":"`+phc+`"}]}`), 0o600)
	f, r := revocationNode(t, `{}`, func(d *config.Document) {
		d.Modules["auth.file"] = json.RawMessage(`{"path":"` + users + `"}`)
	})
	r.Lock()
	r.Revoked[13] = true
	r.Unlock()
	useCert(t, f, r, 13)
	login := &packet.Connect{ClientID: "a", CleanStart: true, HasUsername: true, Username: "alice", HasPassword: true, Password: []byte("pw")}
	if _, ack := f.connect("mtls", packet.V5, login, true); ack.ReasonCode != packet.NotAuthorized {
		t.Fatalf("a revoked certificate beside a good password: %s", ack.ReasonCode)
	}
	useCert(t, f, r, 14)
	login2 := *login
	if _, ack := f.connect("mtls", packet.V5, &login2, true); ack.ReasonCode != packet.Success {
		t.Fatalf("a good certificate beside a good password: %s", ack.ReasonCode)
	}
}

// AC 3 on a node: a SCRAM exchange completes against a credential written
// by hash-password -scram; a pbkdf2-form user cannot complete SCRAM, and logs
// in by password where auth.file reads the same file.
func TestSCRAMOnANode(t *testing.T) {
	scram, err := auth.HashSCRAM([]byte("pencil"), auth.MinSCRAMIterations)
	if err != nil {
		t.Fatal(err)
	}
	phc, _ := auth.HashPassword([]byte("pw"))
	users := filepath.Join(t.TempDir(), "users.json")
	_ = os.WriteFile(users, []byte(`{"users":[{"username":"dev-1","password":"`+scram+`"},{"username":"old","password":"`+phc+`"}]}`), 0o600)
	f := start(t, func(d *config.Document) {
		d.Auth.Anonymous = false
		d.Modules["authz.file"] = deviceACL(t)
		d.Modules["auth.scram"] = json.RawMessage(`{"path":"` + users + `"}`)
		d.Modules["auth.file"] = json.RawMessage(`{"path":"` + users + `"}`)
	})
	exchange := func(user, password string) (*client, *packet.Connack, *scramtest.Client) {
		c := &scramtest.Client{User: user, Password: password, Nonce: "n0nce"}
		method := auth.SCRAMMethod
		nc := f.dial("tcp", false)
		cl := &client{t: t, c: nc, enc: &packet.Encoder{Version: packet.V5}, dec: packet.NewDecoder(nc)}
		cl.dec.Version = packet.V5
		t.Cleanup(func() { _ = nc.Close() })
		cl.send(&packet.Connect{ProtocolLevel: packet.V5, ClientID: user, CleanStart: true, Properties: packet.Properties{AuthenticationMethod: &method, AuthenticationData: c.First()}})
		a, ok := cl.read().(*packet.Auth)
		if !ok {
			t.Fatal("no AUTH")
		}
		cl.send(&packet.Auth{ReasonCode: packet.ContinueAuthentication, Properties: packet.Properties{AuthenticationMethod: &method, AuthenticationData: c.Final(a.Properties.AuthenticationData)}})
		ack, ok := cl.read().(*packet.Connack)
		if !ok {
			t.Fatal("no CONNACK")
		}
		return cl, ack, c
	}
	cl, ack, c := exchange("dev-1", "pencil")
	if ack.ReasonCode != packet.Success || !c.Verify(ack.Properties.AuthenticationData) {
		t.Fatalf("SCRAM: %s, server verified %v", ack.ReasonCode, c.Verify(ack.Properties.AuthenticationData))
	}
	cl.send(&packet.Publish{Topic: "devices/dev-1/t", QoS: 1, PacketID: 1})
	if a := cl.read().(*packet.Puback); a.ReasonCode != packet.NoMatchingSubscribers {
		t.Fatalf("its own topic: %s", a.ReasonCode)
	}
	if _, ack, _ := exchange("old", "pw"); ack.ReasonCode != packet.BadUserNameOrPassword {
		t.Fatalf("a pbkdf2-form user over SCRAM: %s", ack.ReasonCode)
	}
	if _, ack := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "old", CleanStart: true, HasUsername: true, Username: "old", HasPassword: true, Password: []byte("pw")}, false); ack.ReasonCode != packet.Success {
		t.Fatalf("the pbkdf2-form user by password: %s", ack.ReasonCode)
	}
	if _, ack := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "dev-1b", CleanStart: true, HasUsername: true, Username: "dev-1", HasPassword: true, Password: []byte("pencil")}, false); ack.ReasonCode != packet.Success {
		t.Fatalf("the SCRAM-form user by password: %s", ack.ReasonCode)
	}
}

// Nothing contacts the key endpoint at New; Start prefetches in the
// background.
func TestJWTKeysNotFetchedBeforeStart(t *testing.T) {
	p := oidctest.New(t, "hoardmq")
	doc := config.Default()
	doc.Node.DataDir = filepath.Join(t.TempDir(), "data")
	doc.Modules = map[string]json.RawMessage{"auth.jwt": json.RawMessage(`{"issuer":"` + p.URL + `","audiences":["hoardmq"],"jwksUri":"` + p.URL + `/keys"}`)}
	if _, err := New(doc, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if n := p.Count("jwks"); n != 0 {
		t.Fatalf("New fetched the key set %d times", n)
	}
	start(t, func(d *config.Document) {
		d.Modules["auth.jwt"] = doc.Modules["auth.jwt"]
	})
	deadline := time.Now().Add(5 * time.Second)
	for p.Count("jwks") < 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := p.Count("jwks"); n != 1 {
		t.Fatalf("Start's prefetch: %d fetches", n)
	}
}

// A certificate revoked mid-session ends the session within the decision
// TTL, once the cached good answer has lapsed.
func TestRevokedMidSessionEnds(t *testing.T) {
	f, r := revocationNode(t, `{"crl":false}`, func(d *config.Document) { d.Auth.DecisionTTLSeconds = 1 })
	r.Lock()
	r.Lifetime = time.Second
	r.Unlock()
	useCert(t, f, r, 20)
	cl, ack := f.connect("mtls", packet.V5, &packet.Connect{ClientID: "d", CleanStart: true}, true)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("connect: %s", ack.ReasonCode)
	}
	r.Lock()
	r.Revoked[20] = true
	r.Unlock()
	_ = cl.c.SetReadDeadline(time.Now().Add(5 * time.Second))
	pk, err := cl.dec.Read()
	if d, ok := pk.(*packet.Disconnect); err != nil || !ok || d.ReasonCode != packet.NotAuthorized {
		t.Fatalf("after the revocation: %+v %v", pk, err)
	}
}
