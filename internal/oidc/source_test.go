package oidc_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/jose"
	"github.com/happydave/hoardmq/internal/oidc"
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

func sourceFor(t *testing.T, p *oidctest.Provider, c *clock) *jose.KeySource {
	t.Helper()
	s := oidc.NewKeySource(p.URL, mustClient(t), nil)
	s.Now = c.Now
	return s
}

// verifyWith is what the admin API does with a token: keys for its kid, then
// the decision.
func verifyWith(t *testing.T, s *jose.KeySource, p *oidctest.Provider, tok string, now time.Time) error {
	t.Helper()
	parsed, err := jose.Parse(tok)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := s.Keys(t.Context(), parsed.Header.Kid)
	if err != nil {
		return err
	}
	_, err = jose.Verify(tok, keys, jose.Policy{Issuer: p.URL, Audiences: []string{"cli"}, Skew: time.Minute, MaxLifetime: 24 * time.Hour}, now)
	return err
}

func TestKeySourceContactsNothingUntilUsed(t *testing.T) {
	p := oidctest.New(t, "cli")
	_ = sourceFor(t, p, &clock{now: time.Now()})
	if n := p.Count("discovery") + p.Count("jwks"); n != 0 {
		t.Fatalf("a new key source made %d requests", n)
	}
}

// A caller that hangs up does not cancel the fetch others wait for: it gives
// up waiting, and the next caller finds the keys the fetch brought.
func TestFetchOutlivesItsCaller(t *testing.T) {
	p := oidctest.New(t, "cli")
	s := sourceFor(t, p, &clock{now: time.Now()})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, _ = s.Keys(ctx, "k1-RS256")
	deadline := time.Now().Add(5 * time.Second)
	for p.Count("jwks") < 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if _, err := s.Keys(t.Context(), "k1-RS256"); err != nil {
		t.Fatalf("a fetch started by a cancelled request: %v", err)
	}
	if n := p.Count("jwks"); n != 1 {
		t.Fatalf("%d fetches, want the one the cancelled request started", n)
	}
}

// Rotation: a token signed by a key the provider has just published is
// accepted once a minute has passed since the last fetch; after a refetch
// that no longer lists the old key, the old key's tokens are refused.
func TestKeyRotation(t *testing.T) {
	p := oidctest.New(t, "cli")
	c := &clock{now: time.Now()}
	s := sourceFor(t, p, c)
	old := "k1-RS256"
	if err := verifyWith(t, s, p, p.Mint(t, p.Claims()), c.Now()); err != nil {
		t.Fatalf("first token: %v", err)
	}
	if p.Count("jwks") != 1 {
		t.Fatalf("jwks fetched %d times", p.Count("jwks"))
	}
	next := p.AddKey(t, "RS256")
	p.Use(next)
	c.Add(61 * time.Second)
	if err := verifyWith(t, s, p, p.Mint(t, p.Claims()), c.Now()); err != nil {
		t.Fatalf("a token signed by the new key: %v", err)
	}
	if p.Count("jwks") != 2 {
		t.Fatalf("the new kid caused %d fetches in all, want 2", p.Count("jwks"))
	}
	oldToken := p.SignWith(t, old, nil, p.Claims())
	if err := verifyWith(t, s, p, oldToken, c.Now()); err != nil {
		t.Fatalf("the old key still published: %v", err)
	}
	p.Withdraw(old)
	third := p.AddKey(t, "RS256")
	c.Add(61 * time.Second)
	p.Use(third)
	if err := verifyWith(t, s, p, p.Mint(t, p.Claims()), c.Now()); err != nil {
		t.Fatalf("a third key: %v", err)
	}
	if err := verifyWith(t, s, p, oldToken, c.Now()); !errors.Is(err, jose.ErrUnknownKey) {
		t.Fatalf("a withdrawn key's token: %v", err)
	}
}

// Unknown kids cannot drive requests at the provider: within a minute of the
// last fetch, twenty of them cause none.
func TestRefetchBound(t *testing.T) {
	p := oidctest.New(t, "cli")
	c := &clock{now: time.Now()}
	s := sourceFor(t, p, c)
	if err := verifyWith(t, s, p, p.Mint(t, p.Claims()), c.Now()); err != nil {
		t.Fatal(err)
	}
	for i := range 20 {
		c.Add(2 * time.Second)
		tok := p.SignWith(t, "k1-RS256", map[string]any{"kid": "forged-" + strings.Repeat("x", i)}, p.Claims())
		if err := verifyWith(t, s, p, tok, c.Now()); !errors.Is(err, jose.ErrUnknownKey) {
			t.Fatalf("unknown kid %d: %v", i, err)
		}
	}
	if n := p.Count("jwks"); n != 1 {
		t.Fatalf("twenty unknown kids within a minute: %d fetches, want 1", n)
	}
	// The same unknown kid after the interval refetches once.
	c.Add(61 * time.Second)
	tok := p.SignWith(t, "k1-RS256", map[string]any{"kid": "forged-again"}, p.Claims())
	_ = verifyWith(t, s, p, tok, c.Now())
	if n := p.Count("jwks"); n != 2 {
		t.Fatalf("after the interval: %d fetches, want 2", n)
	}
}

// The key set is refreshed when it is an hour old, even for known kids.
func TestKeySetMaxAge(t *testing.T) {
	p := oidctest.New(t, "cli")
	c := &clock{now: time.Now()}
	s := sourceFor(t, p, c)
	tok := p.Mint(t, p.Claims())
	_ = verifyWith(t, s, p, tok, c.Now())
	c.Add(59 * time.Minute)
	_ = verifyWith(t, s, p, tok, c.Now())
	if n := p.Count("jwks"); n != 1 {
		t.Fatalf("at 59 minutes: %d fetches", n)
	}
	c.Add(time.Minute)
	_ = verifyWith(t, s, p, tok, c.Now())
	if n := p.Count("jwks"); n != 2 {
		t.Fatalf("at 60 minutes: %d fetches, want 2", n)
	}
}

func TestDiscoveryIssuerMismatch(t *testing.T) {
	p := oidctest.New(t, "cli")
	p.Lock()
	p.DiscoveryIssuer = p.URL + "/other"
	p.Unlock()
	s := sourceFor(t, p, &clock{now: time.Now()})
	if err := verifyWith(t, s, p, p.Mint(t, p.Claims()), time.Now()); !errors.Is(err, jose.ErrUnavailable) {
		t.Fatalf("a discovery document naming another issuer: %v", err)
	}
	if p.Count("jwks") != 0 {
		t.Fatal("keys fetched from a mismatched discovery document")
	}
}

// A provider that goes down after a first fetch leaves the keys held
// working; one down from the start leaves nothing to verify with.
func TestProviderDown(t *testing.T) {
	p := oidctest.New(t, "cli")
	c := &clock{now: time.Now()}
	s := sourceFor(t, p, c)
	tok := p.Mint(t, p.Claims())
	if err := verifyWith(t, s, p, tok, c.Now()); err != nil {
		t.Fatal(err)
	}
	p.Lock()
	p.Down = true
	p.Unlock()
	c.Add(2 * time.Hour)
	if err := verifyWith(t, s, p, tok, c.Now().Add(-2*time.Hour)); err != nil {
		t.Fatalf("the provider down after a fetch: %v", err)
	}
	q := oidctest.New(t, "cli")
	q.Lock()
	q.Down = true
	q.Unlock()
	s2 := sourceFor(t, q, c)
	if err := verifyWith(t, s2, q, q.Mint(t, q.Claims()), time.Now()); !errors.Is(err, jose.ErrUnavailable) {
		t.Fatalf("the provider down from the start: %v", err)
	}
}

// Concurrent requests share one fetch.
func TestConcurrentRequestsShareAFetch(t *testing.T) {
	p := oidctest.New(t, "cli")
	s := sourceFor(t, p, &clock{now: time.Now()})
	tok := p.Mint(t, p.Claims())
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := verifyWith(t, s, p, tok, time.Now()); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if n := p.Count("jwks"); n != 1 {
		t.Fatalf("16 concurrent first uses: %d fetches", n)
	}
}

func mustClient(t *testing.T) *http.Client {
	t.Helper()
	c, err := jose.HTTPClient("")
	if err != nil {
		t.Fatal(err)
	}
	return c
}
