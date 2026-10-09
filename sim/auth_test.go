package sim

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
)

// With HOARDMQ_SIM_AUTH=jwt the harness's clients really authenticate: they
// connect with their tokens, the nodes fetch the key set, and a client with
// no token is refused.
func TestJWTPopulation(t *testing.T) {
	t.Setenv(authEnv, "jwt")
	c := New(t, Options{})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for i, n := range c.Nodes {
		cl := NewClient("jwt-client-"+n.Name, []string{n.TCP}, 600, int64(i))
		if err := cl.Run(ctx); err != nil {
			t.Fatalf("a client with a token on %s: %v", n.Name, err)
		}
		cl.Close()
	}
	if code, _, err := ProbeConnect(c.Nodes[0].TCP, "jwt-probe"); err != nil || code != packet.Success {
		t.Fatalf("a probe with a token: %s %v", code, err)
	}
	if n := c.jwt.Count("jwks"); n < 1 || n > len(c.Nodes) {
		t.Fatalf("%d key-set fetches for %d nodes", n, len(c.Nodes))
	}
	if got := c.effective(0).Auth; got != "jwt" {
		t.Fatalf("provenance auth %q", got)
	}
	nc, err := net.DialTimeout("tcp", c.Nodes[0].TCP, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = nc.Close() }()
	enc, dec := &packet.Encoder{Version: packet.V5}, packet.NewDecoder(nc)
	dec.Version = packet.V5
	_ = nc.SetDeadline(time.Now().Add(5 * time.Second))
	if err := enc.Encode(nc, &packet.Connect{ProtocolLevel: packet.V5, ClientID: "no-token", CleanStart: true}); err != nil {
		t.Fatal(err)
	}
	p, err := dec.Read()
	if ack, ok := p.(*packet.Connack); err != nil || !ok || ack.ReasonCode != packet.NotAuthorized {
		t.Fatalf("a client without a token: %+v %v", p, err)
	}
}

// With HOARDMQ_SIM_AUTH=ldap the harness's clients really authenticate: the
// nodes bind to the directory as each client, a wrong password is refused,
// and a client without credentials is refused.
func TestLDAPPopulation(t *testing.T) {
	t.Setenv(authEnv, "ldap")
	c := New(t, Options{})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for i, n := range c.Nodes {
		cl := NewClient("ldap-client-"+n.Name, []string{n.TCP}, 600, int64(i))
		if err := cl.Run(ctx); err != nil {
			t.Fatalf("a client with its password on %s: %v", n.Name, err)
		}
		cl.Close()
	}
	if n := c.directory.Binds("uid=ldap-client-n1," + directoryBase); n < 1 {
		t.Fatalf("the directory saw %d binds as ldap-client-n1", n)
	}
	if got := c.effective(0).Auth; got != "ldap" {
		t.Fatalf("provenance auth %q", got)
	}
	for _, x := range []struct {
		name string
		c    *packet.Connect
		want packet.ReasonCode
	}{
		{"a wrong password", &packet.Connect{ProtocolLevel: packet.V5, ClientID: "wrong", CleanStart: true, HasUsername: true, Username: "wrong", HasPassword: true, Password: []byte("nope")}, packet.BadUserNameOrPassword},
		{"no credentials", &packet.Connect{ProtocolLevel: packet.V5, ClientID: "none", CleanStart: true}, packet.NotAuthorized},
	} {
		nc, err := net.DialTimeout("tcp", c.Nodes[0].TCP, 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		enc, dec := &packet.Encoder{Version: packet.V5}, packet.NewDecoder(nc)
		dec.Version = packet.V5
		_ = nc.SetDeadline(time.Now().Add(5 * time.Second))
		if err := enc.Encode(nc, x.c); err != nil {
			t.Fatal(err)
		}
		p, err := dec.Read()
		_ = nc.Close()
		if ack, ok := p.(*packet.Connack); err != nil || !ok || ack.ReasonCode != x.want {
			t.Fatalf("%s: %+v %v", x.name, p, err)
		}
	}
}

// With HOARDMQ_SIM_AUTH=file the population's clients connect with their
// passwords, which the nodes verify against the users file; a client outside
// the file connects anonymously beside them; a wrong password is refused.
func TestFilePopulation(t *testing.T) {
	t.Setenv(authEnv, "file")
	c := New(t, Options{FileUsers: 3})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for i, n := range c.Nodes {
		cl := NewClient(fmt.Sprintf("f%06d", i), []string{n.TCP}, 600, int64(i))
		if err := cl.Run(ctx); err != nil {
			t.Fatalf("a population client with its password on %s: %v", n.Name, err)
		}
		cl.Close()
	}
	var derivations int64
	for _, n := range c.Nodes {
		derivations += n.srv.StormStats().PasswordCache.Derivations
	}
	if derivations < int64(len(c.Nodes)) {
		t.Fatalf("%d derivations across the nodes for %d password clients", derivations, len(c.Nodes))
	}
	if code, _, err := ProbeConnect(c.Nodes[0].TCP, "file-probe"); err != nil || code != packet.Success {
		t.Fatalf("an anonymous probe beside the population: %s %v", code, err)
	}
	if got := c.effective(0).Auth; got != "file" {
		t.Fatalf("provenance auth %q", got)
	}
	nc, err := net.DialTimeout("tcp", c.Nodes[0].TCP, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = nc.Close() }()
	enc, dec := &packet.Encoder{Version: packet.V5}, packet.NewDecoder(nc)
	dec.Version = packet.V5
	_ = nc.SetDeadline(time.Now().Add(5 * time.Second))
	if err := enc.Encode(nc, &packet.Connect{ProtocolLevel: packet.V5, ClientID: "f000000", CleanStart: true, HasUsername: true, Username: "f000000", HasPassword: true, Password: []byte("nope")}); err != nil {
		t.Fatal(err)
	}
	p, err := dec.Read()
	if ack, ok := p.(*packet.Connack); err != nil || !ok || ack.ReasonCode != packet.BadUserNameOrPassword {
		t.Fatalf("a population client with a wrong password: %+v %v", p, err)
	}
}
