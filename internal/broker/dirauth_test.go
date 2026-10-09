package broker

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/admin"
	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/ldap/ldaptest"
	"github.com/happydave/hoardmq/internal/module"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/testcert"
)

// fallbackAuth sorts after auth.ldap and allows any client whose password is
// "fallback-ok": what a chain's later authenticator decides.
type fallbackAuth struct{}

func (fallbackAuth) ID() module.ID                                    { return "auth.zfallback" }
func (fallbackAuth) Capabilities() module.Capability                  { return module.CapAuthentication }
func (fallbackAuth) Provision(*module.Context, json.RawMessage) error { return nil }
func (fallbackAuth) Start(context.Context) error                      { return nil }
func (fallbackAuth) Stop(context.Context) error                       { return nil }
func (fallbackAuth) Authenticate(_ context.Context, r *auth.Request) auth.Result {
	if string(r.Password) == "fallback-ok" {
		return auth.Result{Decision: auth.Allow}
	}
	return auth.Result{Decision: auth.Pass}
}

func init() {
	module.Register("auth.zfallback", func() module.Module { return fallbackAuth{} })
}

const ldapTestBase = "ou=people,dc=hoardmq,dc=test"

// ldapDirectory starts a directory over StartTLS with the test CA, holding
// alice and the service account; it returns the module configuration.
func ldapDirectory(t *testing.T, extra string) (*ldaptest.Server, json.RawMessage) {
	t.Helper()
	dir := t.TempDir()
	ca, err := testcert.NewCA("ldap-ca")
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := ca.Issue("localhost", 2)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := tls.X509KeyPair(leaf.CertPEM, leaf.KeyPEM)
	if err != nil {
		t.Fatal(err)
	}
	s := ldaptest.NewTLS(t, ldapTestBase, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12},
		ldaptest.Entry{DN: "uid=alice," + ldapTestBase, Password: "wonderland", Attrs: map[string][]string{"objectClass": {"person"}, "uid": {"alice"}}},
		ldaptest.Entry{DN: "cn=broker,dc=hoardmq,dc=test", Password: "svc-secret"})
	caFile, err := ca.WriteCA(dir)
	if err != nil {
		t.Fatal(err)
	}
	pw := filepath.Join(dir, "svc")
	if err := os.WriteFile(pw, []byte("svc-secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return s, json.RawMessage(`{"url":"` + s.URL + `","startTLS":true,"caFile":"` + caFile + `","search":{"baseDn":"` + ldapTestBase +
		`","filter":"(&(objectClass=person)(uid=${username}))","bindDn":"cn=broker,dc=hoardmq,dc=test","bindPasswordFile":"` + pw + `"}` + extra + `}`)
}

func userConnect(v packet.Version, user, pw string) *packet.Connect {
	return &packet.Connect{ProtocolLevel: v, ClientID: "c-" + user, CleanStart: true, HasUsername: true, Username: user, HasPassword: true, Password: []byte(pw)}
}

// AC 1: an LDAP bind authenticates a client at 5.0 and 3.1.1, over StartTLS;
// the identity is the client's username; a wrong password is refused and
// audited with its reason, never the password.
func TestLDAPAuthenticationOnANode(t *testing.T) {
	s, mod := ldapDirectory(t, "")
	f := start(t, func(d *config.Document) {
		d.Auth.Anonymous = false
		d.Modules["authz.file"] = deviceACL(t)
		d.Modules["auth.ldap"] = mod
	})
	cl, ack := f.connect("tcp", packet.V5, userConnect(packet.V5, "alice", "wonderland"), false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("5.0 as alice: %s", ack.ReasonCode)
	}
	cl.send(&packet.Publish{Topic: "devices/alice/t", QoS: 1, PacketID: 1})
	if a := cl.read().(*packet.Puback); a.ReasonCode != packet.NoMatchingSubscribers {
		t.Fatalf("her own topic: %s", a.ReasonCode)
	}
	cl.send(&packet.Publish{Topic: "devices/bob/t", QoS: 1, PacketID: 2})
	if a := cl.read().(*packet.Puback); a.ReasonCode != packet.NotAuthorized {
		t.Fatalf("another's topic: %s", a.ReasonCode)
	}
	if _, ack := f.connect("tcp", packet.V4, userConnect(packet.V4, "alice", "wonderland"), false); ack.ReasonCode != packet.Success {
		t.Fatalf("3.1.1 as alice: %s", ack.ReasonCode)
	}
	if s.TLSBinds("uid=alice,"+ldapTestBase) != 2 {
		t.Fatalf("alice bound %d times over TLS, want 2", s.TLSBinds("uid=alice,"+ldapTestBase))
	}
	for _, v := range []packet.Version{packet.V5, packet.V4} {
		if _, ack := f.connect("tcp", v, userConnect(v, "alice", "looking-glass"), false); ack.ReasonCode != packet.BadUserNameOrPassword {
			t.Fatalf("a wrong password at %d: %s", v, ack.ReasonCode)
		}
	}
	found := false
	for _, e := range auditEvents(t, f) {
		if e.Action == "auth.failure" && e.Detail["reason"] == "ldap: invalid credentials" {
			found = true
		}
		for _, v := range e.Detail {
			if strings.Contains(v, "looking-glass") || strings.Contains(v, "wonderland") {
				t.Fatal("an audit record carries a password")
			}
		}
	}
	if !found {
		t.Fatal("no auth.failure record with the directory's reason")
	}
	if n := metric(f, "hoardmq_auth_ldap_decisions_total", map[string]string{"outcome": "allowed"}); n != 2 {
		t.Fatalf("allowed decisions counted %v", n)
	}
}

// AC 1: a directory outage follows the configured chain fallback. Under deny
// the chain stops at auth.ldap; under pass the authenticator after it decides.
func TestLDAPOutageFollowsTheChain(t *testing.T) {
	for _, policy := range []string{"deny", "pass"} {
		s, mod := ldapDirectory(t, `,"onUnavailable":"`+policy+`","timeoutMs":500`)
		f := start(t, func(d *config.Document) {
			d.Auth.Anonymous = false
			d.Modules["auth.ldap"] = mod
			d.Modules["auth.zfallback"] = json.RawMessage(`{}`)
		})
		if _, ack := f.connect("tcp", packet.V5, userConnect(packet.V5, "alice", "wonderland"), false); ack.ReasonCode != packet.Success {
			t.Fatalf("%s: alice before the outage: %s", policy, ack.ReasonCode)
		}
		s.Stop()
		start := time.Now()
		_, fallback := f.connect("tcp", packet.V5, userConnect(packet.V5, "alice", "fallback-ok"), false)
		_, own := f.connect("tcp", packet.V5, userConnect(packet.V5, "alice", "wonderland"), false)
		if time.Since(start) > 2*(500*time.Millisecond+time.Second) {
			t.Fatalf("%s: two decisions took %v", policy, time.Since(start))
		}
		want := packet.BadUserNameOrPassword
		if policy == "pass" {
			want = packet.Success
		}
		if fallback.ReasonCode != want || own.ReasonCode != packet.BadUserNameOrPassword {
			t.Fatalf("%s: the later authenticator's client %s, want %s; alice's own password %s", policy, fallback.ReasonCode, want, own.ReasonCode)
		}
		if n := metric(f, "hoardmq_auth_ldap_unavailable_total", map[string]string{"policy": policy}); n != 2 {
			t.Fatalf("%s: unavailable counted %v, want 2", policy, n)
		}
		if policy == "deny" {
			found := false
			for _, e := range auditEvents(t, f) {
				found = found || e.Action == "auth.failure" && e.Detail["reason"] == "ldap: directory unavailable"
			}
			if !found {
				t.Fatal("deny: no auth.failure record naming the outage")
			}
		}
	}
}

func credentialsTrio(t *testing.T) map[string]*fixture {
	t.Helper()
	dir := t.TempDir()
	nodes := startTrio(t, func(_ string, d *config.Document) {
		withAdmin(t, dir)(d)
		d.Auth.Anonymous = false
		d.Modules["auth.db"] = json.RawMessage(`{}`)
		d.Cluster.Readiness.StableMs = 0
	})
	waitMap(t, nodes, 3)
	return nodes
}

func connectsAs(t *testing.T, f *fixture, user, pw string) packet.ReasonCode {
	t.Helper()
	_, ack := f.connect("tcp", packet.V5, userConnect(packet.V5, user, pw), false)
	return ack.ReasonCode
}

// eventually retries a CONNECT until it answers want: a node answers from
// its own applied copy, which follows the commit.
func eventually(t *testing.T, f *fixture, user, pw string, want packet.ReasonCode) {
	t.Helper()
	var got packet.ReasonCode
	adminWait(t, user+" to be answered "+want.String(), func() bool {
		got = connectsAs(t, f, user, pw)
		return got == want
	})
}

// AC 3: the in-broker database supports add, update and remove through the
// admin API, a credential is valid on every node, and every mutation is
// audited on the node that served it, never with the password or its hash.
func TestCredentialsThroughTheAdminAPI(t *testing.T) {
	nodes := credentialsTrio(t)
	n1, n2, n3 := nodes["n1"], nodes["n2"], nodes["n3"]
	code, data := api(t, n1, "POST", "/api/v1/credentials", adminOps, `{"username":"alice","password":"s3cret-1"}`)
	c := decode[admin.Credential](t, data)
	if code != 201 || c.Form != "pbkdf2-sha256" || strings.Contains(string(data), "$pbkdf2") {
		t.Fatalf("create on n1: %d %s", code, data)
	}
	for _, f := range []*fixture{n1, n2, n3} {
		eventually(t, f, "alice", "s3cret-1", packet.Success)
	}
	if n := metric(n3, "hoardmq_records", map[string]string{"collection": "auth.users"}); n != 1 {
		t.Errorf("n3: %v credentials counted, want 1", n)
	}
	if code, data := api(t, n2, "POST", "/api/v1/credentials", adminOps, `{"username":"alice","password":"x"}`); code != 409 {
		t.Fatalf("create again on n2: %d %s", code, data)
	}
	code, data = api(t, n2, "PUT", "/api/v1/credentials/alice", adminOps, `{"password":"s3cret-2","scram":true}`)
	if code != 200 || decode[admin.Credential](t, data).Form != "scram-sha-256" {
		t.Fatalf("update on n2: %d %s", code, data)
	}
	eventually(t, n3, "alice", "s3cret-2", packet.Success)
	if got := connectsAs(t, n3, "alice", "s3cret-1"); got != packet.BadUserNameOrPassword {
		t.Fatalf("the old password on n3: %s", got)
	}
	if code, data := api(t, n3, "PUT", "/api/v1/credentials/alice", adminOps, `{"attributes":{"team":"blue"},"ifVersion":`+uitoa(c.Version)+`}`); code != 409 {
		t.Fatalf("an update at the first version: %d %s", code, data)
	}
	hashed, err := auth.HashSCRAM([]byte("s3cret-3"), auth.MinSCRAMIterations)
	if err != nil {
		t.Fatal(err)
	}
	if code, data := api(t, n1, "PUT", "/api/v1/credentials/alice", adminOps, `{"passwordHash":"`+hashed+`"}`); code != 200 {
		t.Fatalf("a hash on n1: %d %s", code, data)
	}
	eventually(t, n2, "alice", "s3cret-3", packet.Success)
	if code, _ := api(t, n1, "PUT", "/api/v1/credentials/alice", adminOps, `{"passwordHash":"plaintext"}`); code != 400 {
		t.Fatalf("a plain passwordHash: %d", code)
	}
	code, data = api(t, n2, "GET", "/api/v1/credentials", adminReader, "")
	if code != 200 || strings.Contains(string(data), "password") || strings.Contains(string(data), "$scram") ||
		len(decode[admin.CredentialPage](t, data).Items) != 1 {
		t.Fatalf("the reader's list: %d %s", code, data)
	}
	if code, _ := api(t, n2, "POST", "/api/v1/credentials", adminReader, `{"username":"mallory","password":"x"}`); code != 403 {
		t.Fatalf("the reader's create: %d", code)
	}
	if code, data := api(t, n3, "DELETE", "/api/v1/credentials/alice", adminOps, ""); code != 200 {
		t.Fatalf("delete on n3: %d %s", code, data)
	}
	for _, f := range []*fixture{n1, n2, n3} {
		eventually(t, f, "alice", "s3cret-3", packet.BadUserNameOrPassword)
	}
	for name, f := range map[string]*fixture{"n1": n1, "n2": n2, "n3": n3} {
		want := map[string][]string{"n1": {"credential.create", "credential.update"}, "n2": {"credential.update"}, "n3": {"credential.delete"}}[name]
		for _, action := range want {
			if !auditHas(t, f, "token:ops", action, "alice", "ok") {
				t.Errorf("%s: no %s record for alice", name, action)
			}
		}
		for _, e := range auditEvents(t, f) {
			for _, v := range e.Detail {
				if strings.Contains(v, "s3cret") || strings.Contains(v, "$pbkdf2") || strings.Contains(v, "$scram") {
					t.Fatalf("%s: an audit record carries a password or a hash: %+v", name, e)
				}
			}
		}
	}
	if n := metric(n1, "hoardmq_records", map[string]string{"collection": "auth.users"}); n != 0 {
		t.Errorf("n1: %v credentials after the delete", n)
	}
	if !auditHas(t, n2, "token:reader", "credential.create", "", "denied") {
		t.Error("n2: the reader's refused create is not audited")
	}
}

// A credential is in the consensus log: a restarted node still holds it.
func TestCredentialsSurviveARestart(t *testing.T) {
	adminDir := t.TempDir()
	f := start(t, func(d *config.Document) {
		withAdmin(t, adminDir)(d)
		d.Auth.Anonymous = false
		d.Modules["auth.db"] = json.RawMessage(`{}`)
	})
	if code, data := api(t, f, "POST", "/api/v1/credentials", adminOps, `{"username":"alice","password":"s3cret-1"}`); code != 201 {
		t.Fatalf("create %d %s", code, data)
	}
	dataDir := filepath.Join(f.dir, "data")
	f.stop()
	g := start(t, func(d *config.Document) {
		withAdmin(t, adminDir)(d)
		d.Node.DataDir = dataDir
		d.Auth.Anonymous = false
		d.Modules["auth.db"] = json.RawMessage(`{}`)
	})
	eventually(t, g, "alice", "s3cret-1", packet.Success)
}

func uitoa(v uint64) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// A records write proposed on a follower is forwarded to the leader, commits,
// and applies on every node at the index its result names; a write whose
// context has ended proposes nothing.
func TestRecordsProposedOnAFollower(t *testing.T) {
	nodes := startTrio(t, func(_ string, d *config.Document) { d.Cluster.Readiness.StableMs = 0 })
	waitMap(t, nodes, 3)
	var follower *fixture
	for _, f := range nodes {
		if f.srv.Consensus().Status().Role != "leader" {
			follower = f
			break
		}
	}
	st := follower.srv.records.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	absent := uint64(0)
	res, err := st.Put(ctx, "test.things", "k1", json.RawMessage(`{"n":1}`), &absent, "test", time.Now())
	if err != nil || !res.OK || res.Index == 0 {
		t.Fatalf("put on a follower: %+v %v", res, err)
	}
	if rec, ok := st.Get("test.things", "k1"); !ok || rec.Version != res.Index {
		t.Fatalf("the follower's own copy on return: %+v %v", rec, ok)
	}
	for name, f := range nodes {
		adminWait(t, name+" to apply k1", func() bool {
			rec, ok := f.srv.records.Load().Get("test.things", "k1")
			return ok && rec.Version == res.Index
		})
	}
	done, stop := context.WithCancel(context.Background())
	stop()
	if _, err := st.Put(done, "test.things", "k2", json.RawMessage(`{}`), nil, "test", time.Now()); err == nil {
		t.Fatal("a write with an ended context succeeded")
	}
	time.Sleep(300 * time.Millisecond)
	for name, f := range nodes {
		if _, ok := f.srv.records.Load().Get("test.things", "k2"); ok {
			t.Fatalf("%s holds a write whose context had ended", name)
		}
	}
}
