package auth

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Revocation checks a verified client certificate against its issuer's OCSP
// responder and CRL. It is a certificate authenticator: no certificate
// passes; revoked is a deny; unknown on every enabled source is a deny when
// failing closed and good when failing open; good allows a certificate-only
// client and passes one that sent a username or password, whose other
// credential is not the certificate's to vouch for. Every certificate of the
// verified chain below the root is checked; the root is the trust anchor.
type Revocation struct {
	OCSP bool
	CRL  bool
	// OCSPURL and CRLURLs replace the certificate's own endpoints.
	OCSPURL  string
	CRLURLs  []string
	FailOpen bool
	// FetchWait bounds how long a CONNECT waits for a fetch; Timeout bounds
	// the fetch.
	FetchWait time.Duration
	Timeout   time.Duration
	Client    *http.Client
	Now       func() time.Time
	// Count, when set, is told each outcome for the metrics.
	Count func(name string, labels map[string]string)

	mu       sync.Mutex
	ocsp     map[string]OCSPAnswer
	crls     map[string]*CRL
	inflight map[string]*flight
}

// flight is one fetch in progress; its result is read once done is closed.
type flight struct {
	done chan struct{}
	ocsp OCSPAnswer
	crl  *CRL
	err  error
}

// Cache bounds.
const (
	maxOCSPEntries = 100_000
	maxCRLEntries  = 64
	maxCRLBytes    = 16 << 20
	maxOCSPBytes   = 1 << 20
)

// ChecksCertificates marks Revocation as a certificate authenticator.
func (*Revocation) ChecksCertificates() {}

// levelCheck is one certificate's statements: OCSP's, and the CRL's when
// OCSP had no definite answer.
type levelCheck struct {
	ocsp, crl       RevocationStatus
	ocspRan, crlRan bool
}

// Authenticate implements Authenticator.
func (r *Revocation) Authenticate(ctx context.Context, req *Request) Result {
	chain := req.CertChain
	if len(chain) == 0 {
		return Result{Decision: Pass}
	}
	// Each certificate below the root against its issuer; a lone certificate
	// has no issuer to ask and stays unknown. The levels are checked at once.
	levels := make([]levelCheck, max(1, len(chain)-1))
	var wg sync.WaitGroup
	for i := range levels {
		var issuer *x509.Certificate
		if i+1 < len(chain) {
			issuer = chain[i+1]
		}
		wg.Add(1)
		go func(i int, cert, issuer *x509.Certificate) {
			defer wg.Done()
			levels[i] = r.checkLevel(ctx, cert, issuer, i == 0)
		}(i, chain[i], issuer)
	}
	wg.Wait()
	for _, l := range levels {
		if l.ocspRan {
			r.count("hoardmq_auth_revocation_checks_total", map[string]string{"method": "ocsp", "status": l.ocsp.String()})
		}
		if l.crlRan {
			r.count("hoardmq_auth_revocation_checks_total", map[string]string{"method": "crl", "status": l.crl.String()})
		}
	}
	d, reason, unknown := chainOutcome(levels, r.OCSP, r.CRL, r.FailOpen, req.HasUsername || req.HasPassword)
	if unknown {
		policy := "closed"
		if r.FailOpen {
			policy = "open"
		}
		r.count("hoardmq_auth_revocation_unknown_total", map[string]string{"policy": policy})
	}
	return Result{Decision: d, Reason: reason}
}

func (r *Revocation) count(name string, labels map[string]string) {
	if r.Count != nil {
		r.Count(name, labels)
	}
}

// checkLevel asks a certificate's sources: OCSP, then the CRL when OCSP had
// no definite answer. The configured endpoints are the leaf's; an
// intermediate is asked at its own.
func (r *Revocation) checkLevel(ctx context.Context, cert, issuer *x509.Certificate, leaf bool) levelCheck {
	l := levelCheck{ocsp: StatusUnknown, crl: StatusUnknown}
	ocspURL, crlURLs := "", []string(nil)
	if leaf {
		ocspURL, crlURLs = r.OCSPURL, r.CRLURLs
	}
	if r.OCSP {
		l.ocsp, l.ocspRan = r.ocspStatus(ctx, cert, issuer, ocspURL), true
	}
	if r.CRL && l.ocsp == StatusUnknown {
		l.crl, l.crlRan = r.crlStatus(ctx, cert, issuer, crlURLs), true
	}
	return l
}

// revocationOutcome is the decision on one certificate's statements.
func revocationOutcome(ocsp, crl RevocationStatus, ocspOn, crlOn, failOpen, otherCredentials bool) (Decision, string, bool) {
	return chainOutcome([]levelCheck{{ocsp: ocsp, crl: crl}}, ocspOn, crlOn, failOpen, otherCredentials)
}

// chainOutcome is the decision on the chain's statements, leaf first. Each
// certificate's status is OCSP's when definite, else the CRL's. Revoked
// anywhere refuses; otherwise unknown anywhere is the policy's to decide. It
// also reports whether the chain's status was unknown.
func chainOutcome(levels []levelCheck, ocspOn, crlOn, failOpen, otherCredentials bool) (Decision, string, bool) {
	unknown := false
	for i, l := range levels {
		status, method := StatusUnknown, ""
		switch {
		case ocspOn && l.ocsp != StatusUnknown:
			status, method = l.ocsp, "ocsp"
		case crlOn && l.crl != StatusUnknown:
			status, method = l.crl, "crl"
		}
		switch {
		case status == StatusRevoked && i == 0:
			return Deny, "x509: revoked (" + method + ")", false
		case status == StatusRevoked:
			return Deny, "x509: intermediate revoked (" + method + ")", false
		case status == StatusUnknown:
			unknown = true
		}
	}
	switch {
	case unknown && !failOpen:
		return Deny, "x509: revocation unknown", true
	case otherCredentials:
		return Pass, "", unknown
	}
	return Allow, "", unknown
}

func (r *Revocation) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// ocspStatus is the cached answer while it is current, else a fetch's,
// waited for at most FetchWait. url, when set, replaces the certificate's
// responder.
func (r *Revocation) ocspStatus(ctx context.Context, leaf, issuer *x509.Certificate, url string) RevocationStatus {
	if issuer == nil {
		return StatusUnknown
	}
	if url == "" && len(leaf.OCSPServer) > 0 {
		url = leaf.OCSPServer[0]
	}
	if url == "" {
		return StatusUnknown
	}
	kh, err := keyHash(issuer)
	if err != nil {
		return StatusUnknown
	}
	key := "ocsp:" + hex.EncodeToString(kh) + ":" + leaf.SerialNumber.Text(16)
	now := r.now()
	r.mu.Lock()
	if a, ok := r.ocsp[key]; ok && a.NextUpdate.After(now) {
		r.mu.Unlock()
		return a.Status
	}
	f := r.start(key, func() (OCSPAnswer, *CRL, error) {
		a, err := r.fetchOCSP(url, leaf, issuer)
		return a, nil, err
	})
	r.mu.Unlock()
	if !r.wait(ctx, f) || f.err != nil {
		return StatusUnknown
	}
	return f.ocsp.Status
}

// crlStatus is ocspStatus for the CRL at the first of urls, else the
// certificate's first http or https distribution point. A list is cached
// for the issuer it was accepted for.
func (r *Revocation) crlStatus(ctx context.Context, leaf, issuer *x509.Certificate, urls []string) RevocationStatus {
	if issuer == nil {
		return StatusUnknown
	}
	var url string
	for _, u := range append(append([]string{}, urls...), leaf.CRLDistributionPoints...) {
		if strings.HasPrefix(u, "http://") || strings.HasPrefix(u, "https://") {
			url = u
			break
		}
	}
	if url == "" {
		return StatusUnknown
	}
	kh, err := keyHash(issuer)
	if err != nil {
		return StatusUnknown
	}
	key := "crl:" + hex.EncodeToString(kh) + ":" + url
	now := r.now()
	r.mu.Lock()
	if c, ok := r.crls[key]; ok && c.NextUpdate.After(now) {
		r.mu.Unlock()
		return c.Status(leaf)
	}
	f := r.start(key, func() (OCSPAnswer, *CRL, error) {
		c, err := r.fetchCRL(url, issuer)
		return OCSPAnswer{}, c, err
	})
	r.mu.Unlock()
	if !r.wait(ctx, f) || f.err != nil || f.crl == nil {
		return StatusUnknown
	}
	return f.crl.Status(leaf)
}

// start returns the fetch in flight for key, starting it when there is none;
// r.mu is held. The fetch runs to its own timeout whoever stops waiting, and
// a current answer is cached until its nextUpdate.
func (r *Revocation) start(key string, fetch func() (OCSPAnswer, *CRL, error)) *flight {
	if r.inflight == nil {
		r.inflight = map[string]*flight{}
	}
	if f, ok := r.inflight[key]; ok {
		return f
	}
	f := &flight{done: make(chan struct{})}
	r.inflight[key] = f
	go func() {
		f.ocsp, f.crl, f.err = fetch()
		r.mu.Lock()
		delete(r.inflight, key)
		if f.err == nil {
			r.store(key, f)
		}
		r.mu.Unlock()
		close(f.done)
	}()
	return f
}

// store caches a fetched answer; r.mu is held. An OCSP answer without a
// nextUpdate, or unknown, is not cached; a full cache first drops what has
// lapsed, and caches nothing more if still full.
func (r *Revocation) store(key string, f *flight) {
	now := r.now()
	if f.crl != nil {
		if r.crls == nil {
			r.crls = map[string]*CRL{}
		}
		if len(r.crls) >= maxCRLEntries {
			for k, c := range r.crls {
				if !c.NextUpdate.After(now) {
					delete(r.crls, k)
				}
			}
		}
		if len(r.crls) < maxCRLEntries {
			r.crls[key] = f.crl
		}
		return
	}
	if f.ocsp.NextUpdate.IsZero() || f.ocsp.Status == StatusUnknown {
		return
	}
	if r.ocsp == nil {
		r.ocsp = map[string]OCSPAnswer{}
	}
	if len(r.ocsp) >= maxOCSPEntries {
		for k, a := range r.ocsp {
			if !a.NextUpdate.After(now) {
				delete(r.ocsp, k)
			}
		}
	}
	if len(r.ocsp) < maxOCSPEntries {
		r.ocsp[key] = f.ocsp
	}
}

// wait waits for the fetch at most FetchWait and ctx, and reports whether it
// finished.
func (r *Revocation) wait(ctx context.Context, f *flight) bool {
	t := time.NewTimer(r.FetchWait)
	defer t.Stop()
	select {
	case <-f.done:
		return true
	case <-t.C:
	case <-ctx.Done():
	}
	return false
}

func (r *Revocation) fetchOCSP(url string, leaf, issuer *x509.Certificate) (OCSPAnswer, error) {
	body, err := OCSPRequest(leaf, issuer)
	if err != nil {
		return OCSPAnswer{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), r.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return OCSPAnswer{}, err
	}
	req.Header.Set("Content-Type", "application/ocsp-request")
	req.Header.Set("Accept", "application/ocsp-response")
	data, err := r.do(req, maxOCSPBytes)
	if err != nil {
		return OCSPAnswer{}, err
	}
	return AcceptOCSP(data, leaf, issuer, r.now())
}

func (r *Revocation) fetchCRL(url string, issuer *x509.Certificate) (*CRL, error) {
	ctx, cancel := context.WithTimeout(context.Background(), r.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	data, err := r.do(req, maxCRLBytes)
	if err != nil {
		return nil, err
	}
	return AcceptCRL(data, issuer, r.now())
}

func (r *Revocation) do(req *http.Request, limit int64) ([]byte, error) {
	resp, err := r.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s %s: %s", req.Method, req.URL, resp.Status)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, errors.New("answer larger than its bound")
	}
	return data, nil
}
