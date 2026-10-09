package listener

import (
	"crypto/tls"
	"net"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/testcert"
)

// adminHandshake completes one handshake against a TLS listener built from
// AdminTLS and returns the server side's connection state.
func adminHandshake(t *testing.T, cfg *tls.Config, client *tls.Config) (tls.ConnectionState, error) {
	t.Helper()
	ln, err := tls.Listen("tcp", "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	type result struct {
		st  tls.ConnectionState
		err error
	}
	done := make(chan result, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			done <- result{err: err}
			return
		}
		defer func() { _ = c.Close() }()
		tc := c.(*tls.Conn)
		_ = tc.SetDeadline(time.Now().Add(3 * time.Second))
		err = tc.Handshake()
		done <- result{tc.ConnectionState(), err}
	}()
	c, err := tls.DialWithDialer(&net.Dialer{Timeout: 3 * time.Second}, "tcp", ln.Addr().String(), client)
	if err == nil {
		// TLS 1.3 reports a client certificate refusal on the first read.
		_ = c.SetReadDeadline(time.Now().Add(time.Second))
		_, _ = c.Read(make([]byte, 1))
		_ = c.Close()
	}
	r := <-done
	return r.st, r.err
}

func TestAdminTLS(t *testing.T) {
	_, ca, certFile, keyFile, caFile := tlsFixtures(t)
	other, err := testcert.NewCA("other-ca")
	if err != nil {
		t.Fatal(err)
	}
	signed, _ := ca.Issue("ops", 10)
	foreign, _ := other.Issue("ops", 11)
	withCA, err := AdminTLS(&config.TLS{CertFile: certFile, KeyFile: keyFile, ClientCAFile: caFile})
	if err != nil {
		t.Fatal(err)
	}
	if len(withCA.NextProtos) != 1 || withCA.NextProtos[0] != "http/1.1" {
		t.Fatalf("ALPN %v", withCA.NextProtos)
	}
	httpClient := func(leaf *testcert.Leaf) *tls.Config {
		c := clientTLS(ca, leaf)
		c.NextProtos = []string{"http/1.1"}
		return c
	}
	// No certificate: accepted, so a bearer token can authenticate.
	st, err := adminHandshake(t, withCA, httpClient(nil))
	if err != nil || len(st.PeerCertificates) != 0 {
		t.Fatalf("without a certificate: %v %d", err, len(st.PeerCertificates))
	}
	// A certificate the CA signed: accepted and verified.
	st, err = adminHandshake(t, withCA, httpClient(signed))
	if err != nil || len(st.VerifiedChains) == 0 || st.VerifiedChains[0][0].Subject.CommonName != "ops" {
		t.Fatalf("a signed certificate: %v %d", err, len(st.VerifiedChains))
	}
	// A certificate another CA signed, with the same common name, sent
	// whatever CAs the server names (Go's client would otherwise send none):
	// refused at the handshake.
	pushy := httpClient(nil)
	fc, _ := tls.X509KeyPair(foreign.CertPEM, foreign.KeyPEM)
	pushy.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return &fc, nil }
	if _, err := adminHandshake(t, withCA, pushy); err == nil {
		t.Fatal("a certificate from another CA completed the handshake")
	}
	// Without a client CA no certificate is asked for, so none is seen.
	noCA, err := AdminTLS(&config.TLS{CertFile: certFile, KeyFile: keyFile})
	if err != nil {
		t.Fatal(err)
	}
	st, err = adminHandshake(t, noCA, httpClient(signed))
	if err != nil || len(st.PeerCertificates) != 0 {
		t.Fatalf("without a client CA: %v %d", err, len(st.PeerCertificates))
	}
}
