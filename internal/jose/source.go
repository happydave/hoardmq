package jose

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"
)

// maxDocumentBytes bounds a JWKS or any other document fetched here.
const maxDocumentBytes = 1 << 20

// FetchTimeout bounds one request to a key or metadata endpoint.
const FetchTimeout = 10 * time.Second

// HTTPClient is a client for an identity provider: FetchTimeout, and TLS
// verified against caFile when one is named, else the system roots.
func HTTPClient(caFile string) (*http.Client, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if caFile != "" {
		pool, err := CertPool(caFile)
		if err != nil {
			return nil, err
		}
		transport.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	}
	return &http.Client{Timeout: FetchTimeout, Transport: transport}, nil
}

// CertPool reads a PEM bundle; a file holding no certificate is an error.
func CertPool(caFile string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("%s: no certificate found", caFile)
	}
	return pool, nil
}

// EndpointOK is the rule for a URL a provider is reached at: absolute, and
// https unless the host is loopback. It returns the problem, or "".
func EndpointOK(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || !u.IsAbs() || u.Host == "" {
		return "not an absolute URL"
	}
	switch u.Scheme {
	case "https":
		return ""
	case "http":
		if h := u.Hostname(); h == "localhost" || net.ParseIP(h) != nil && net.ParseIP(h).IsLoopback() {
			return ""
		}
	}
	return "not https"
}

// Fetch GETs a JSON document of at most 1 MiB and requires 200.
func Fetch(ctx context.Context, c *http.Client, u string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: %s", u, resp.Status)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxDocumentBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxDocumentBytes {
		return nil, fmt.Errorf("GET %s: larger than 1 MiB", u)
	}
	return data, nil
}

// Refetch reasons the fetch decision weighs.
const (
	reasonNone    = ""
	reasonFirst   = "first use"
	reasonUnknown = "unknown key"
	reasonAge     = "key set older than its maximum age"
)

// fetchDue is the decision to fetch the key set: when none is held, when the
// token names a key the set lacks, or when the set is older than maxAge — and
// in each case only if the last attempt began at least minInterval ago, so
// tokens naming unknown keys cannot drive requests at the provider. It
// returns the reason, or "".
func fetchDue(have, known bool, fetchedAt, attemptAt, now time.Time, minInterval, maxAge time.Duration) string {
	reason := reasonNone
	switch {
	case !have:
		reason = reasonFirst
	case !known:
		reason = reasonUnknown
	case now.Sub(fetchedAt) >= maxAge:
		reason = reasonAge
	}
	if reason == reasonNone || (!attemptAt.IsZero() && now.Sub(attemptAt) < minInterval) {
		return reasonNone
	}
	return reason
}

// mustWait is whether a caller waits for a fetch in flight: when no set is
// held, when its key is not in the held set, or when the set is past its
// maximum age and the source does not serve a stale set during a refresh.
func mustWait(have, known, aged, staleWhileRefresh bool) bool {
	return !have || !known || (aged && !staleWhileRefresh)
}

// KeySource holds a JWKS, fetched on first use and refetched by fetchDue's
// rule. One fetch is in flight at a time and runs to its own timeout; a
// caller that needs it waits at most for its own context, and a caller that
// does not is answered from the held set at once. A failed fetch keeps the
// keys already held.
type KeySource struct {
	// Locate returns the JWKS URL: a fixed URI, or discovery's jwks_uri.
	Locate func(ctx context.Context) (string, error)
	// Name identifies the source in logs.
	Name        string
	Client      *http.Client
	Now         func() time.Time
	MinInterval time.Duration
	MaxAge      time.Duration
	// StaleWhileRefresh answers from the held set while an age refresh runs.
	StaleWhileRefresh bool
	Log               *slog.Logger
	// OnFetch, when set, is told each fetch's outcome.
	OnFetch func(err error)

	mu        sync.Mutex
	keys      KeySet
	fetchedAt time.Time
	attemptAt time.Time
	inflight  chan struct{}
}

// Default refetch bounds.
const (
	DefaultMinInterval = 60 * time.Second
	DefaultMaxAge      = time.Hour
)

// NewKeySource returns a source reading the JWKS at uri, with the default
// bounds. It contacts nothing.
func NewKeySource(uri string, c *http.Client, log *slog.Logger) *KeySource {
	return NewLocatedKeySource(uri, func(context.Context) (string, error) { return uri, nil }, c, log)
}

// NewLocatedKeySource is NewKeySource with the URL found by locate.
func NewLocatedKeySource(name string, locate func(context.Context) (string, error), c *http.Client, log *slog.Logger) *KeySource {
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &KeySource{Locate: locate, Name: name, Client: c, Now: time.Now, MinInterval: DefaultMinInterval, MaxAge: DefaultMaxAge, Log: log}
}

// ErrUnavailable is no key set held: the provider has not answered.
var ErrUnavailable = errors.New("provider unavailable")

// Keys returns the key set to verify a token naming kid with, starting a
// fetch when fetchDue says so and waiting for one in flight when mustWait
// says so, for at most ctx.
func (s *KeySource) Keys(ctx context.Context, kid string) (KeySet, error) {
	s.mu.Lock()
	now := s.Now()
	have := s.keys != nil
	_, known := s.keys[kid]
	if reason := fetchDue(have, known, s.fetchedAt, s.attemptAt, now, s.MinInterval, s.MaxAge); reason != reasonNone && s.inflight == nil {
		s.start(now, reason)
	}
	aged := have && now.Sub(s.fetchedAt) >= s.MaxAge
	done := s.inflight
	wait := done != nil && mustWait(have, known, aged, s.StaleWhileRefresh)
	s.mu.Unlock()
	if wait {
		select {
		case <-done:
		case <-ctx.Done():
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.keys == nil {
		return nil, ErrUnavailable
	}
	return s.keys, nil
}

// Prefetch starts a fetch when no set is held and none is in flight, and
// returns at once.
func (s *KeySource) Prefetch() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.Now()
	if s.keys == nil && s.inflight == nil && fetchDue(false, false, s.fetchedAt, s.attemptAt, now, s.MinInterval, s.MaxAge) != reasonNone {
		s.start(now, reasonFirst)
	}
}

// start begins a fetch; s.mu is held. The fetch is bounded by FetchTimeout
// alone: a caller that stops waiting must not cancel the fetch others wait
// for, and so hold the keys back for MinInterval.
func (s *KeySource) start(now time.Time, reason string) {
	s.attemptAt = now
	done := make(chan struct{})
	s.inflight = done
	go func() {
		keys, err := s.fetch()
		if s.OnFetch != nil {
			s.OnFetch(err)
		}
		s.mu.Lock()
		if err != nil {
			s.Log.Warn("jose: fetching the key set failed", "source", s.Name, "reason", reason, "err", err)
		} else {
			s.keys, s.fetchedAt = keys, now
		}
		s.inflight = nil
		s.mu.Unlock()
		close(done)
	}()
}

func (s *KeySource) fetch() (KeySet, error) {
	ctx, cancel := context.WithTimeout(context.Background(), FetchTimeout)
	defer cancel()
	uri, err := s.Locate(ctx)
	if err != nil {
		return nil, err
	}
	data, err := Fetch(ctx, s.Client, uri)
	if err != nil {
		return nil, err
	}
	return ParseKeySet(data)
}
