package transport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/testcert"
)

// A peer's leaf must carry both the client and the server auth usages, since
// each node both dials and accepts: one without either, without the
// extension at all (which x509 reads as any usage) or with only "any" is
// refused as a certificate refusal by whichever side checks it.
func TestPeerCertificateUsages(t *testing.T) {
	ca, err := testcert.NewCA("cluster-ca")
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(ca.PEM)
	auth := func(t *testing.T, name string, o testcert.Options) Auth {
		t.Helper()
		leaf, err := ca.IssueWith(name, time.Now().UnixNano(), o)
		if err != nil {
			t.Fatal(err)
		}
		cert, err := tls.X509KeyPair(leaf.CertPEM, leaf.KeyPEM)
		if err != nil {
			t.Fatal(err)
		}
		return Auth{Mode: "mtls", Cert: cert, CA: pool}
	}
	// newNode is a transport named name, peer of other at addr, listening.
	newNode := func(t *testing.T, name, other, addr string, a Auth) (*Transport, *events) {
		t.Helper()
		ev := newEvents()
		tr, err := New(Options{Node: name, ClusterID: "test", Listen: "127.0.0.1:0", Auth: a, HandshakeTimeout: 2 * time.Second,
			Peers: []config.Peer{{Name: other, Address: addr}}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Metrics: ev, Audit: ev.Audit})
		if err != nil {
			t.Fatal(err)
		}
		return tr, ev
	}
	// link starts an acceptor n2 and has a dialer n1 dial it once; it
	// returns the dialer's and the acceptor's events and the dial's error.
	link := func(t *testing.T, dialer, acceptor testcert.Options) (*events, *events, error) {
		t.Helper()
		acc, accEv := newNode(t, "n2", "n1", "127.0.0.1:1", auth(t, "n2", acceptor))
		if err := acc.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(acc.Stop)
		dia, diaEv := newNode(t, "n1", "n2", acc.Addr().String(), auth(t, "n1", dialer))
		dia.ctx, dia.cancel = context.WithCancel(context.Background())
		defer dia.cancel()
		_, err := dia.dial(dia.peers["n2"], RoleControl)
		return diaEv, accEv, err
	}
	both := testcert.Options{}
	if _, _, err := link(t, both, both); err != nil {
		t.Fatalf("both leaves with both usages: %v", err)
	}
	t.Run("an acceptor from another CA", func(t *testing.T) {
		other, err := testcert.NewCA("other")
		if err != nil {
			t.Fatal(err)
		}
		leaf, err := other.Issue("n2", 2)
		if err != nil {
			t.Fatal(err)
		}
		cert, err := tls.X509KeyPair(leaf.CertPEM, leaf.KeyPEM)
		if err != nil {
			t.Fatal(err)
		}
		acc, _ := newNode(t, "n2", "n1", "127.0.0.1:1", Auth{Mode: "mtls", Cert: cert, CA: pool})
		if err := acc.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(acc.Stop)
		dia, diaEv := newNode(t, "n1", "n2", acc.Addr().String(), auth(t, "n1", both))
		dia.ctx, dia.cancel = context.WithCancel(context.Background())
		defer dia.cancel()
		if _, err := dia.dial(dia.peers["n2"], RoleControl); err == nil {
			t.Fatal("linked")
		}
		if n := diaEv.count("hoardmq_cluster_refusals_total", "certificate"); n < 1 {
			t.Fatalf("the dialer counted %v certificate refusals", n)
		}
	})
	for _, c := range []struct {
		name string
		o    testcert.Options
	}{
		{"server auth only", testcert.Options{ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}},
		{"client auth only", testcert.Options{ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}},
		{"no extension", testcert.Options{NoExtKeyUsage: true}},
		{"any only", testcert.Options{ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageAny}}},
	} {
		t.Run(c.name+" on the dialer", func(t *testing.T) {
			_, accEv, err := link(t, c.o, both)
			if err == nil {
				t.Fatal("linked")
			}
			waitFor(t, "the acceptor's certificate refusal", func() bool {
				return accEv.count("hoardmq_cluster_refusals_total", "certificate") >= 1
			})
		})
		t.Run(c.name+" on the acceptor", func(t *testing.T) {
			diaEv, _, err := link(t, both, c.o)
			if err == nil {
				t.Fatal("linked")
			}
			if n := diaEv.count("hoardmq_cluster_refusals_total", "certificate"); n < 1 {
				t.Fatalf("the dialer counted %v certificate refusals; err %v", n, err)
			}
			// Without server auth the dialer's TLS check refuses before any
			// hello is sent.
			var invalid x509.CertificateInvalidError
			if c.name == "client auth only" && !errors.As(err, &invalid) {
				t.Fatalf("the dialer's refusal %v is not its TLS check's", err)
			}
		})
	}
}

func TestPeerUsagesOK(t *testing.T) {
	client, server, anyUsage := x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageAny
	for _, c := range []struct {
		usages []x509.ExtKeyUsage
		want   bool
	}{
		{[]x509.ExtKeyUsage{server, client}, true},
		{[]x509.ExtKeyUsage{client, server, anyUsage}, true},
		{[]x509.ExtKeyUsage{server}, false},
		{[]x509.ExtKeyUsage{client}, false},
		{[]x509.ExtKeyUsage{anyUsage}, false},
		{nil, false},
	} {
		if got := peerUsagesOK(&x509.Certificate{ExtKeyUsage: c.usages}); got != c.want {
			t.Errorf("peerUsagesOK(%v) = %v, want %v", c.usages, got, c.want)
		}
	}
}
