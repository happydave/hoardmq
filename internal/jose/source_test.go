package jose_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/jose"
	"github.com/happydave/hoardmq/internal/oidc/oidctest"
)

// clock is a settable time for a key source.
type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *clock) Add(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// fixedSource reads the fixture's JWKS directly, as the MQTT authenticator
// does, with a settable clock.
func fixedSource(t *testing.T, p *oidctest.Provider, c *clock) *jose.KeySource {
	t.Helper()
	s := jose.NewKeySource(p.URL+"/keys", mustClient(t), nil)
	s.Now = c.Now
	return s
}

func setDelay(p *oidctest.Provider, d time.Duration) {
	p.Lock()
	p.Delay = d
	p.Unlock()
}

// waitCount waits until the fixture has answered n requests to endpoint.
func waitCount(t *testing.T, p *oidctest.Provider, endpoint string, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for p.Count(endpoint) < n {
		if time.Now().After(deadline) {
			t.Fatalf("%s answered %d requests, want %d", endpoint, p.Count(endpoint), n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A fixed URI is fetched directly, without discovery.
func TestKeySourceFixedURI(t *testing.T) {
	p := oidctest.New(t, "cli")
	s := fixedSource(t, p, &clock{now: time.Now()})
	if _, err := s.Keys(t.Context(), "k1-RS256"); err != nil {
		t.Fatal(err)
	}
	if d, j := p.Count("discovery"), p.Count("jwks"); d != 0 || j != 1 {
		t.Fatalf("discovery %d, jwks %d; want 0 and 1", d, j)
	}
}

// A caller waits for a fetch in flight at most for its own context, and the
// fetch it gave up on still completes.
func TestKeySourceWaitBound(t *testing.T) {
	p := oidctest.New(t, "cli")
	setDelay(p, 500*time.Millisecond)
	s := fixedSource(t, p, &clock{now: time.Now()})
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := s.Keys(ctx, "k1-RS256")
	if !errors.Is(err, jose.ErrUnavailable) {
		t.Fatalf("a caller that stopped waiting: %v", err)
	}
	if el := time.Since(start); el > 400*time.Millisecond {
		t.Fatalf("a 100 ms wait took %v", el)
	}
	time.Sleep(700 * time.Millisecond)
	if _, err := s.Keys(t.Context(), "k1-RS256"); err != nil {
		t.Fatalf("after the fetch completed: %v", err)
	}
	if n := p.Count("jwks"); n != 1 {
		t.Fatalf("%d fetches, want 1", n)
	}
}

// While a refetch for an unknown kid is in flight, a token naming a known kid
// is answered from the held set at once.
func TestKnownKidDoesNotWaitForARefetch(t *testing.T) {
	p := oidctest.New(t, "cli")
	c := &clock{now: time.Now()}
	s := fixedSource(t, p, c)
	if _, err := s.Keys(t.Context(), "k1-RS256"); err != nil {
		t.Fatal(err)
	}
	setDelay(p, 500*time.Millisecond)
	c.Add(61 * time.Second)
	unknown := make(chan error, 1)
	go func() {
		_, err := s.Keys(t.Context(), "k-new")
		unknown <- err
	}()
	waitCount(t, p, "jwks", 2)
	start := time.Now()
	if _, err := s.Keys(t.Context(), "k1-RS256"); err != nil {
		t.Fatal(err)
	}
	if el := time.Since(start); el > 100*time.Millisecond {
		t.Fatalf("a known kid waited %v for a refetch it did not need", el)
	}
	if err := <-unknown; err != nil {
		t.Fatalf("the unknown kid's caller: %v", err)
	}
}

// Stale-while-refresh answers a known kid from an aged set at once and
// refreshes it in the background; without it the caller waits for the
// refresh.
func TestStaleWhileRefresh(t *testing.T) {
	for _, stale := range []bool{true, false} {
		p := oidctest.New(t, "cli")
		c := &clock{now: time.Now()}
		s := fixedSource(t, p, c)
		s.StaleWhileRefresh = stale
		if _, err := s.Keys(t.Context(), "k1-RS256"); err != nil {
			t.Fatal(err)
		}
		setDelay(p, 500*time.Millisecond)
		c.Add(61 * time.Minute)
		start := time.Now()
		if _, err := s.Keys(t.Context(), "k1-RS256"); err != nil {
			t.Fatal(err)
		}
		el := time.Since(start)
		switch {
		case stale && el > 100*time.Millisecond:
			t.Fatalf("stale-while-refresh waited %v", el)
		case !stale && p.Count("jwks") != 2:
			t.Fatalf("without stale-while-refresh the call returned before the refresh (%d fetches, %v)", p.Count("jwks"), el)
		}
		waitCount(t, p, "jwks", 2)
	}
}

// Prefetch starts one fetch and returns at once.
func TestPrefetch(t *testing.T) {
	p := oidctest.New(t, "cli")
	setDelay(p, 300*time.Millisecond)
	s := fixedSource(t, p, &clock{now: time.Now()})
	start := time.Now()
	s.Prefetch()
	if el := time.Since(start); el > 100*time.Millisecond {
		t.Fatalf("Prefetch blocked for %v", el)
	}
	s.Prefetch()
	waitCount(t, p, "jwks", 1)
	if _, err := s.Keys(t.Context(), "k1-RS256"); err != nil {
		t.Fatal(err)
	}
	if n := p.Count("jwks"); n != 1 {
		t.Fatalf("%d fetches, want 1", n)
	}
}

// The provider's TLS is verified against caFile.
func TestHTTPClientCAFile(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) }))
	defer srv.Close()
	dir := t.TempDir()
	ca := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := jose.HTTPClient(ca)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := c.Get(srv.URL)
	if err != nil {
		t.Fatalf("with the CA: %v", err)
	}
	_ = resp.Body.Close()
	plain, _ := jose.HTTPClient("")
	if _, err := plain.Get(srv.URL); err == nil {
		t.Fatal("the system roots trusted a test certificate")
	} else {
		var unknown x509.UnknownAuthorityError
		var verr *tls.CertificateVerificationError
		if !errors.As(err, &unknown) && !errors.As(err, &verr) {
			t.Fatalf("without the CA: %v", err)
		}
	}
	if _, err := jose.HTTPClient(filepath.Join(dir, "absent")); err == nil {
		t.Fatal("an absent CA file")
	}
}

func TestEndpointOK(t *testing.T) {
	for u, want := range map[string]string{
		"https://idp.example/keys":     "",
		"http://127.0.0.1:8080/keys":   "",
		"http://localhost/keys":        "",
		"http://idp.example/keys":      "not https",
		"ftp://idp.example/keys":       "not https",
		"/keys":                        "not an absolute URL",
		"https://idp.example/k?cid=x1": "",
	} {
		if got := jose.EndpointOK(u); got != want {
			t.Errorf("%s: %q, want %q", u, got, want)
		}
	}
}
