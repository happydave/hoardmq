package ldap

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/ldap/ldaptest"
	"github.com/happydave/hoardmq/internal/testcert"
)

const base = "ou=people,dc=x"

var alice = ldaptest.Entry{DN: "uid=alice," + base, Password: "wonderland", Attrs: map[string][]string{"objectClass": {"person"}, "uid": {"alice"}}}

func ctxFor(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

func endpoint(t *testing.T, url string) Endpoint {
	t.Helper()
	e, err := ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func TestBindAndSearch(t *testing.T) {
	s := ldaptest.New(t, base, alice)
	c, err := Dial(ctxFor(t, 2*time.Second), endpoint(t, s.URL))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := ctxFor(t, 2*time.Second)
	if code, err := c.Bind(ctx, alice.DN, []byte("wonderland")); err != nil || code != Success {
		t.Fatalf("bind %d %v", code, err)
	}
	if code, err := c.Bind(ctx, alice.DN, []byte("looking-glass")); err != nil || code != InvalidCredentials {
		t.Fatalf("wrong password %d %v", code, err)
	}
	f, _ := ParseTemplate("(&(objectClass=person)(uid=${username}))")
	dns, code, err := c.Search(ctx, base, ScopeSub, 2, f.Encode([]byte("alice")))
	if err != nil || code != Success || len(dns) != 1 || dns[0] != alice.DN {
		t.Fatalf("search %q %d %v", dns, code, err)
	}
	dns, code, err = c.Search(ctx, base, ScopeSub, 2, f.Encode([]byte("bob")))
	if err != nil || code != Success || len(dns) != 0 {
		t.Fatalf("search for bob %q %d %v", dns, code, err)
	}
}

// tlsFixture is a directory with a server certificate from a test CA, and the
// client configuration trusting that CA.
func tlsFixture(t *testing.T) (*ldaptest.Server, *tls.Config) {
	t.Helper()
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
	s := ldaptest.NewTLS(t, base, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}, alice)
	pool := x509.NewCertPool()
	pool.AddCert(ca.Cert)
	return s, &tls.Config{RootCAs: pool}
}

func TestStartTLSAndLDAPS(t *testing.T) {
	s, client := tlsFixture(t)
	for _, c := range []struct {
		name string
		url  string
		st   bool
	}{{"StartTLS", s.URL, true}, {"LDAPS", s.LDAPSURL, false}} {
		e := endpoint(t, c.url)
		e.StartTLS, e.Config = c.st, client
		conn, err := Dial(ctxFor(t, 2*time.Second), e)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		before := s.TLSBinds(alice.DN)
		if code, err := conn.Bind(ctxFor(t, 2*time.Second), alice.DN, []byte("wonderland")); err != nil || code != Success || !conn.TLS() {
			t.Fatalf("%s: bind %d %v tls=%v", c.name, code, err, conn.TLS())
		}
		if s.TLSBinds(alice.DN) != before+1 {
			t.Fatalf("%s: the directory did not see the bind over TLS", c.name)
		}
		conn.Close()
	}
	other, err := testcert.NewCA("another")
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(other.Cert)
	e := endpoint(t, s.LDAPSURL)
	e.Config = &tls.Config{RootCAs: pool}
	if _, err := Dial(ctxFor(t, 2*time.Second), e); err == nil {
		t.Fatal("a server certificate from another CA was accepted")
	}
	e = endpoint(t, s.URL)
	e.StartTLS, e.Config = true, &tls.Config{RootCAs: pool}
	if _, err := Dial(ctxFor(t, 2*time.Second), e); err == nil {
		t.Fatal("StartTLS to a server certificate from another CA was accepted")
	}
	plain := ldaptest.New(t, base, alice)
	e = endpoint(t, plain.URL)
	e.StartTLS, e.Config = true, client
	if _, err := Dial(ctxFor(t, 2*time.Second), e); err == nil {
		t.Fatal("a directory that refused StartTLS was used in clear")
	}
}

func bindAlice(c *Conn, ctx context.Context) error {
	code, err := c.Bind(ctx, alice.DN, []byte("wonderland"))
	if err == nil && code != Success {
		return errors.New("refused")
	}
	return err
}

func TestPoolBoundsConnections(t *testing.T) {
	s := ldaptest.New(t, base, alice)
	s.SetBindDelay(300 * time.Millisecond)
	p := &Pool{Endpoint: endpoint(t, s.URL), Max: 2}
	defer p.Close()
	var wg sync.WaitGroup
	errs := make([]error, 3)
	for i := range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			errs[i] = p.Do(ctx, func(c *Conn) error { return bindAlice(c, ctx) })
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("authentication %d: %v", i, err)
		}
	}
	if n := s.MaxConcurrent(); n > 2 {
		t.Fatalf("%d binds at once, want at most 2", n)
	}
	if n := s.Connections(); n > 2 {
		t.Fatalf("%d connections, want at most 2", n)
	}
	// With both slots busy well past its deadline, a third waits no longer
	// than its own deadline.
	s.SetBindDelay(700 * time.Millisecond)
	var hold sync.WaitGroup
	for range 2 {
		hold.Add(1)
		go func() {
			defer hold.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = p.Do(ctx, func(c *Conn) error { return bindAlice(c, ctx) })
		}()
	}
	time.Sleep(50 * time.Millisecond)
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	err := p.Do(ctx, func(c *Conn) error { return bindAlice(c, ctx) })
	cancel()
	if err == nil || time.Since(start) > 450*time.Millisecond {
		t.Fatalf("a third authentication: %v after %v", err, time.Since(start))
	}
	hold.Wait()
}

func TestPoolRetriesAConnectionClosedWhileIdle(t *testing.T) {
	s := ldaptest.New(t, base, alice)
	p := &Pool{Endpoint: endpoint(t, s.URL), Max: 1}
	defer p.Close()
	ctx := ctxFor(t, 2*time.Second)
	if err := p.Do(ctx, func(c *Conn) error { return bindAlice(c, ctx) }); err != nil {
		t.Fatal(err)
	}
	s.DropConnections()
	time.Sleep(20 * time.Millisecond)
	if err := p.Do(ctx, func(c *Conn) error { return bindAlice(c, ctx) }); err != nil {
		t.Fatalf("after the server closed the idle connection: %v", err)
	}
	if n := s.Connections(); n != 2 {
		t.Fatalf("%d connections, want 2 (the dropped one and a fresh one)", n)
	}
}

func TestRetryFresh(t *testing.T) {
	boom := errors.New("eof")
	for _, c := range []struct {
		reused        bool
		before, after int
		err           error
		want          bool
	}{
		{true, 3, 3, boom, true},
		{false, 0, 0, boom, false},
		{true, 3, 4, boom, false},
		{true, 3, 3, nil, false},
	} {
		if got := RetryFresh(c.reused, c.before, c.after, c.err); got != c.want {
			t.Errorf("%+v: %v", c, got)
		}
	}
}

func TestNoticeOfDisconnection(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = server.Close() }()
	go func() {
		buf := make([]byte, 512)
		_, _ = server.Read(buf)
		notice := message(0, element(classApplication, true, opExtendedResult, concat([][]byte{integer(tagEnumerated, classUniversal, 52), octets(nil), octets(nil), element(classContext, false, 10, []byte(noticeOfDisconnectN))})))
		_, _ = server.Write(notice)
	}()
	c := &Conn{nc: client, r: bufioReaderConn(client), next: 1}
	if _, err := c.Bind(ctxFor(t, time.Second), alice.DN, []byte("x")); !errors.Is(err, ErrDisconnected) {
		t.Fatalf("bind answered by a notice of disconnection: %v", err)
	}
}

func TestParseURL(t *testing.T) {
	for raw, want := range map[string]Endpoint{
		"ldap://127.0.0.1":        {Address: "127.0.0.1:389", Host: "127.0.0.1"},
		"ldaps://dir.example":     {Address: "dir.example:636", Host: "dir.example", LDAPS: true},
		"ldap://dir.example:1389": {Address: "dir.example:1389", Host: "dir.example"},
	} {
		got, err := ParseURL(raw)
		if err != nil || got.Address != want.Address || got.Host != want.Host || got.LDAPS != want.LDAPS {
			t.Errorf("%s: %+v %v", raw, got, err)
		}
	}
	for _, bad := range []string{"http://x", "ldap://", "ldap://x/dc=y?uid", "ldap://u:p@x", "x"} {
		if _, err := ParseURL(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	if !(Endpoint{Host: "127.0.0.1"}).Loopback() || !(Endpoint{Host: "::1"}).Loopback() || (Endpoint{Host: "dir.example"}).Loopback() {
		t.Fatal("loopback")
	}
}

func bufioReaderConn(c net.Conn) *bufio.Reader { return bufio.NewReader(c) }

// A failure after the directory has answered on a reused connection is not
// the idle-close case: it is not retried on a fresh connection.
func TestPoolDoesNotRetryAfterAnAnswer(t *testing.T) {
	s := ldaptest.New(t, base, alice)
	p := &Pool{Endpoint: endpoint(t, s.URL), Max: 1}
	defer p.Close()
	ctx := ctxFor(t, 2*time.Second)
	if err := p.Do(ctx, func(c *Conn) error { return bindAlice(c, ctx) }); err != nil {
		t.Fatal(err)
	}
	runs := 0
	err := p.Do(ctx, func(c *Conn) error {
		runs++
		_ = bindAlice(c, ctx)
		return errors.New("failed after an answer")
	})
	if err == nil || runs != 1 || s.Connections() != 1 {
		t.Fatalf("err %v, %d runs, %d connections; want an error, 1 run, 1 connection", err, runs, s.Connections())
	}
}

// A caller's TLS configuration below 1.2 is raised to 1.2: a directory that
// speaks at most TLS 1.1 is refused.
func TestTLSFloor(t *testing.T) {
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
	s := ldaptest.NewTLS(t, base, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS10, MaxVersion: tls.VersionTLS11}, alice)
	pool := x509.NewCertPool()
	pool.AddCert(ca.Cert)
	e := endpoint(t, s.LDAPSURL)
	e.Config = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS10}
	if c, err := Dial(ctxFor(t, 2*time.Second), e); err == nil {
		c.Close()
		t.Fatal("a TLS 1.1 directory was accepted")
	}
}

// A directory that ended an idle session with a notice of disconnection: the
// notice waits in the connection until the next request reads it, and that
// request is retried on a fresh connection, as for any idle close.
func TestPoolRetriesAfterANoticeOfDisconnection(t *testing.T) {
	notice := message(0, element(classApplication, true, opExtendedResult, concat([][]byte{integer(tagEnumerated, classUniversal, 52), octets(nil), octets(nil), element(classContext, false, 10, []byte(noticeOfDisconnectN))})))
	dials := 0
	p := &Pool{Max: 1}
	p.init()
	p.dial = func(context.Context, Endpoint) (*Conn, error) {
		dials++
		client, server := net.Pipe()
		first := dials == 1
		go func() {
			defer func() { _ = server.Close() }()
			r := bufio.NewReader(server)
			for n := 0; ; n++ {
				req, err := readMessage(r, MaxMessage)
				if err != nil {
					return
				}
				if first && n == 1 {
					_, _ = server.Write(notice)
					return
				}
				_, _ = server.Write(bindResponse(idOf(req), 0))
			}
		}()
		return &Conn{nc: client, r: bufio.NewReader(client), next: 1}, nil
	}
	ctx := ctxFor(t, 2*time.Second)
	for i := range 2 {
		if err := p.Do(ctx, func(c *Conn) error { return bindAlice(c, ctx) }); err != nil {
			t.Fatalf("authentication %d: %v", i, err)
		}
	}
	if dials != 2 {
		t.Fatalf("%d dials, want 2: the notice's connection replaced once", dials)
	}
}

// A search result reference (a referral) is read past, not followed and not
// counted as an entry.
func TestSearchIgnoresReferences(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = server.Close() }()
	go func() {
		r := bufio.NewReader(server)
		req, err := readMessage(r, MaxMessage)
		if err != nil {
			return
		}
		id := idOf(req)
		ref := message(id, element(classApplication, true, opSearchRef, octets([]byte("ldap://other.example/dc=x"))))
		entry := message(id, element(classApplication, true, opSearchEntry, concat([][]byte{octets([]byte("uid=alice,dc=x")), seq()})))
		done := message(id, element(classApplication, true, opSearchDone, concat([][]byte{integer(tagEnumerated, classUniversal, 0), octets(nil), octets(nil)})))
		for _, m := range [][]byte{ref, entry, done} {
			_, _ = server.Write(m)
		}
	}()
	c := &Conn{nc: client, r: bufio.NewReader(client), next: 1}
	f, _ := ParseTemplate("(uid=${username})")
	dns, code, err := c.Search(ctxFor(t, time.Second), "dc=x", ScopeSub, 2, f.Encode([]byte("alice")))
	if err != nil || code != Success || len(dns) != 1 || dns[0] != "uid=alice,dc=x" {
		t.Fatalf("%q %d %v", dns, code, err)
	}
}
