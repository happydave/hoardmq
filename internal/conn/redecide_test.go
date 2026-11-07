package conn

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/packet"
)

// TestIdentityChanged: what an authorizer reads, compared.
func TestIdentityChanged(t *testing.T) {
	base := auth.Identity{Namespace: "default", ClientID: "c", Username: "u", Attributes: map[string]string{"a": "1"}, AdmittedBy: "auth.x"}
	with := func(f func(*auth.Identity)) auth.Identity {
		id := base
		id.Attributes = map[string]string{"a": "1"}
		f(&id)
		return id
	}
	for _, c := range []struct {
		name string
		next auth.Identity
		want bool
	}{
		{"equal", with(func(*auth.Identity) {}), false},
		{"another username", with(func(id *auth.Identity) { id.Username = "v" }), true},
		{"another admitter", with(func(id *auth.Identity) { id.AdmittedBy = "auth.y" }), true},
		{"an attribute added", with(func(id *auth.Identity) { id.Attributes["b"] = "2" }), true},
		{"an attribute removed", with(func(id *auth.Identity) { id.Attributes = nil }), true},
		{"an attribute's value", with(func(id *auth.Identity) { id.Attributes["a"] = "2" }), true},
		{"another common name", with(func(id *auth.Identity) { id.CertCN = "d" }), true},
	} {
		if got := identityChanged(base, c.next); got != c.want {
			t.Errorf("%s: changed %v, want %v", c.name, got, c.want)
		}
	}
	if identityChanged(auth.Identity{Attributes: map[string]string{}}, auth.Identity{}) {
		t.Error("an empty and an absent attribute map differ")
	}
}

// TestIdentityOf: the username rule and the kept result's attributes.
func TestIdentityOf(t *testing.T) {
	attrs := map[string]string{"a": "1"}
	for _, c := range []struct {
		name   string
		req    *auth.Request
		result auth.Result
		want   string
	}{
		{"the result's username", &auth.Request{Username: "sent", HasUsername: true}, auth.Result{Username: "mapped", Attributes: attrs}, "mapped"},
		{"the CONNECT's", &auth.Request{Username: "sent", HasUsername: true, CertCN: "cn"}, auth.Result{Attributes: attrs}, "sent"},
		{"the certificate's, with no username sent", &auth.Request{CertCN: "cn"}, auth.Result{Attributes: attrs}, "cn"},
	} {
		id := identityOf("default", "c", c.req, c.result, "auth.x")
		if id.Username != c.want || id.Attributes["a"] != "1" || id.AdmittedBy != "auth.x" || id.ClientID != "c" || id.Namespace != "default" {
			t.Errorf("%s: %+v, want username %q", c.name, id, c.want)
		}
	}
}

// changing answers its first call with first and every later one with
// later, each held briefly so the connection re-decides soon.
func changing(first, later auth.Result) *countingAuth {
	return &countingAuth{next: func(n int) auth.Result {
		r := later
		if n == 1 {
			r = first
		}
		if r.Decision == auth.Allow {
			r.Expires = time.Now().Add(150 * time.Millisecond)
		}
		return r
	}}
}

// TestRedecisionThatChangesTheIdentity: a re-decision that admits with
// another identity closes with 0x98 (a plain close at 4), handing the core the
// identity the session's Will is authorized with; one that admits with the
// same identity holds the connection; a refusal is 0x87 as before, handing the
// principal alone.
func TestRedecisionThatChangesTheIdentity(t *testing.T) {
	creds := func() *packet.Connect {
		return &packet.Connect{ClientID: "c", CleanStart: true, HasUsername: true, Username: "u", HasPassword: true, Password: []byte("p")}
	}
	allow := func(attrs map[string]string) auth.Result {
		return auth.Result{Decision: auth.Allow, Attributes: attrs}
	}
	one, two := map[string]string{"a": "1"}, map[string]string{"a": "2"}
	plain := auth.Identity{Namespace: "default", ClientID: "c", Username: "u"}
	for _, c := range []struct {
		name   string
		v      packet.Version
		chain  auth.AuthenticatorChain
		reason packet.ReasonCode // 0: held
		handed *auth.Identity
	}{
		{"an attribute's value", packet.V5, auth.AuthenticatorChain{namedAuth{changing(allow(one), allow(two)), "auth.x"}}, packet.AdministrativeAction,
			&auth.Identity{Namespace: "default", ClientID: "c", Username: "u", Attributes: two, AdmittedBy: "auth.x"}},
		{"at level 4", packet.V4, auth.AuthenticatorChain{namedAuth{changing(allow(one), allow(two)), "auth.x"}}, packet.AdministrativeAction, nil},
		{"the admitting module", packet.V5, auth.AuthenticatorChain{
			namedAuth{changing(allow(one), auth.Result{Decision: auth.Pass}), "auth.x"},
			namedAuth{always(auth.Allow), "auth.y"},
		}, packet.AdministrativeAction, &auth.Identity{Namespace: "default", ClientID: "c", Username: "u", AdmittedBy: "auth.y"}},
		{"the principal", packet.V5, auth.AuthenticatorChain{namedAuth{changing(allow(one), auth.Result{Decision: auth.Allow, Username: "other", Attributes: one}), "auth.x"}},
			packet.AdministrativeAction, &plain},
		{"a refusal", packet.V5, auth.AuthenticatorChain{namedAuth{changing(allow(one), auth.Result{Decision: auth.Deny}), "auth.x"}}, packet.NotAuthorized, &plain},
		{"the same identity", packet.V5, auth.AuthenticatorChain{namedAuth{changing(allow(one), allow(one)), "auth.x"}}, 0, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			o := defaultOptions()
			o.Authenticators = c.chain
			m := &counters{}
			o.Metrics = m
			core := &fakeCore{}
			h := newHarness(t, o, core)
			if ack := h.connect(c.v, creds()); ack.ReasonCode != packet.Success {
				t.Fatal(ack)
			}
			if c.reason == 0 {
				counted := c.chain[0].(namedAuth).Authenticator.(*countingAuth)
				waitCalls(t, counted, 3, 2*time.Second)
				h.send(&packet.Pingreq{})
				if _, ok := h.read().(*packet.Pingresp); !ok {
					t.Fatal("the connection was ended")
				}
				if got := core.handed(); len(got) != 0 {
					t.Fatalf("handed %+v", got)
				}
				return
			}
			d := h.readEnd()
			switch {
			case c.v != packet.V5 && d != nil:
				t.Fatalf("level 4 sent %+v", d)
			case c.v == packet.V5 && (d == nil || d.ReasonCode != c.reason):
				t.Fatalf("DISCONNECT %+v, want %s", d, c.reason)
			}
			want := "changed"
			if c.reason == packet.NotAuthorized {
				want = "refused"
			}
			if n := m.get("hoardmq_auth_rechecks_total{outcome=" + want + "}"); n != 1 {
				t.Errorf("rechecks %s %v, want 1", want, n)
			}
			if c.handed == nil {
				return
			}
			got := core.handed()
			if len(got) != 1 || identityChanged(got[0], *c.handed) {
				t.Fatalf("handed %+v, want %+v", got, *c.handed)
			}
		})
	}
}

// attrMech succeeds at once as alice, with attributes it reads at each
// exchange's start, from its module auth.mech.
type attrMech struct {
	mu    sync.Mutex
	attrs map[string]string
	begun atomic.Int32
}

func (m *attrMech) AdmitterID() string { return "auth.mech" }
func (m *attrMech) Methods() []string  { return []string{"X-TEST"} }
func (m *attrMech) Begin(context.Context, string, *auth.Request) auth.Exchange {
	m.begun.Add(1)
	m.mu.Lock()
	defer m.mu.Unlock()
	return onceExchange{attrs: m.attrs}
}
func (m *attrMech) set(attrs map[string]string) { m.mu.Lock(); m.attrs = attrs; m.mu.Unlock() }

type onceExchange struct{ attrs map[string]string }

func (x onceExchange) Step([]byte) auth.Step {
	return auth.Step{Kind: auth.Succeed, Username: "alice", Attributes: x.attrs}
}

// TestReauthenticationThatChangesTheIdentity: a re-authentication with the
// same identity is answered AUTH Success; one with another attribute closes
// with 0x98, handing the core the new identity.
func TestReauthenticationThatChangesTheIdentity(t *testing.T) {
	mech := &attrMech{attrs: map[string]string{"a": "1"}}
	o := defaultOptions()
	o.Mechanisms = offering(mech)
	core := &fakeCore{}
	h := newHarness(t, o, core)
	if ack := h.connect(packet.V5, methodConnect("X-TEST", "c1")); ack.ReasonCode != packet.Success {
		t.Fatal(ack)
	}
	reauth := &packet.Auth{ReasonCode: packet.ReAuthenticate, Properties: packet.Properties{AuthenticationMethod: str("X-TEST")}}
	h.send(reauth)
	if a, ok := h.read().(*packet.Auth); !ok || a.ReasonCode != packet.Success {
		t.Fatalf("the same identity: %+v", a)
	}
	mech.set(map[string]string{"a": "2"})
	h.send(reauth)
	if d := h.readEnd(); d == nil || d.ReasonCode != packet.AdministrativeAction {
		t.Fatalf("another attribute: %+v", d)
	}
	got := core.handed()
	if len(got) != 1 || got[0].Attributes["a"] != "2" || got[0].AdmittedBy != "auth.mech" || got[0].Username != "alice" {
		t.Fatalf("handed %+v", got)
	}
	if mech.begun.Load() != 3 {
		t.Fatalf("%d exchanges, want 3", mech.begun.Load())
	}
}

// TestMechanismConnectionHeldByItsCertificate: a mechanism-admitted
// connection's re-decision runs only the certificate chain, which builds no
// identity of its own, so a certificate allow holds the connection.
func TestMechanismConnectionHeldByItsCertificate(t *testing.T) {
	mech := &attrMech{attrs: map[string]string{"a": "1"}}
	certs := changing(auth.Result{Decision: auth.Allow, Attributes: map[string]string{"cert": "x"}}, auth.Result{Decision: auth.Allow})
	o := defaultOptions()
	o.Mechanisms = offering(mech)
	o.CertificateAuthenticators = auth.AuthenticatorChain{namedAuth{certs, "auth.x509"}}
	core := &fakeCore{}
	h := newHarness(t, o, core)
	if ack := h.connect(packet.V5, methodConnect("X-TEST", "c1")); ack.ReasonCode != packet.Success {
		t.Fatal(ack)
	}
	waitCalls(t, certs, 3, 2*time.Second)
	h.send(&packet.Pingreq{})
	if _, ok := h.read().(*packet.Pingresp); !ok {
		t.Fatal("the connection was ended")
	}
	if got := core.handed(); len(got) != 0 {
		t.Fatalf("handed %+v", got)
	}
}
