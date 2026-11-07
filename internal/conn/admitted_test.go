package conn

import (
	"testing"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/packet"
)

// namedAuth and namedMech name the module they stand for, as the registry's
// wrappers do.
type namedAuth struct {
	auth.Authenticator
	id string
}

func (n namedAuth) AdmitterID() string { return n.id }

type namedMech struct {
	auth.Mechanism
	id string
}

func (n namedMech) AdmitterID() string { return n.id }

// TestAdmitter: the module named is the one whose allow admission keeps.
func TestAdmitter(t *testing.T) {
	A, D, P := auth.Result{Decision: auth.Allow}, auth.Result{Decision: auth.Deny}, auth.Result{Decision: auth.Pass}
	for _, c := range []struct {
		name         string
		certs, chain auth.Result
		want         string
	}{
		{"the chain's allow", P, A, "chain"},
		{"both allow: the chain's, which admission keeps", A, A, "chain"},
		{"a certificate allow, the chain passing", A, P, "certs"},
		{"both pass", P, P, ""},
		{"a deny", D, P, ""},
	} {
		if got := admitter(c.certs, c.chain, "certs", "chain"); got != c.want {
			t.Errorf("%s: %q, want %q", c.name, got, c.want)
		}
	}
}

// TestIdentityAdmittedBy: the connection's identity names the module that
// admitted it, through each path, and nobody for an anonymous client.
func TestIdentityAdmittedBy(t *testing.T) {
	userConnect := &packet.Connect{ClientID: "c", CleanStart: true, HasUsername: true, Username: "alice", HasPassword: true, Password: []byte("p")}
	for _, c := range []struct {
		name    string
		certs   auth.AuthenticatorChain
		chain   auth.AuthenticatorChain
		mech    bool
		connect *packet.Connect
		want    string
	}{
		{"the chain", nil, auth.AuthenticatorChain{namedAuth{always(auth.Pass), "auth.a"}, namedAuth{always(auth.Allow), "auth.b"}}, false, userConnect, "auth.b"},
		{"the certificate chain", auth.AuthenticatorChain{namedAuth{always(auth.Allow), "auth.x509"}}, auth.AuthenticatorChain{namedAuth{always(auth.Pass), "auth.a"}}, false, nil, "auth.x509"},
		{"both: the chain's", auth.AuthenticatorChain{namedAuth{always(auth.Allow), "auth.x509"}}, auth.AuthenticatorChain{namedAuth{always(auth.Allow), "auth.b"}}, false, userConnect, "auth.b"},
		{"a mechanism", nil, nil, true, methodConnect("X-TEST", "c1"), "auth.mech"},
		{"anonymous", nil, auth.AuthenticatorChain{namedAuth{always(auth.Pass), "auth.a"}}, false, nil, ""},
	} {
		o := defaultOptions()
		o.CertificateAuthenticators, o.Authenticators = c.certs, c.chain
		if c.mech {
			m := &scriptMech{method: "X-TEST", steps: []auth.Step{{Kind: auth.Succeed, Username: "alice", Attributes: map[string]string{"auth.http.superuser": "true"}}}}
			o.Mechanisms = offering(namedMech{m, "auth.mech"})
		}
		h := newHarness(t, o, &fakeCore{})
		if ack := h.connect(packet.V5, c.connect); ack.ReasonCode != packet.Success {
			t.Fatalf("%s: CONNACK %s", c.name, ack.ReasonCode)
		}
		if got := h.conn.Identity().AdmittedBy; got != c.want {
			t.Errorf("%s: admitted by %q, want %q", c.name, got, c.want)
		}
	}
}
