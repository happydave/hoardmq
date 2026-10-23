package auth

import (
	"context"
	"crypto/x509"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/testcert"
)

// A responder whose answers carry no nextUpdate is replayed: every request is
// answered with the same signed "good" response. One produced a minute ago
// admits the certificate; the same response two days old does not, and the
// fail-closed policy refuses the client.
func TestOCSPReplayWithoutNextUpdate(t *testing.T) {
	ctx := context.Background()
	p := newPKI(t)
	var mu sync.Mutex
	var served []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		der := served
		mu.Unlock()
		w.Header().Set("Content-Type", "application/ocsp-response")
		_, _ = w.Write(der)
	}))
	t.Cleanup(srv.Close)
	leaf, err := p.ca.IssueWith("device", 3, testcert.Options{OCSPServer: []string{srv.URL}})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Truncate(time.Second)
	respond := func(thisUpdate time.Time) {
		der, err := testcert.OCSPResponse(testcert.ResponseSpec{Issuer: p.ca.Cert, Serial: big.NewInt(3),
			CertStatus: "good", ThisUpdate: thisUpdate, Signer: p.ca.Cert, SignerKey: p.ca.Key})
		if err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		served = der
		mu.Unlock()
	}
	rv := &Revocation{OCSP: true, FetchWait: 2 * time.Second, Timeout: 5 * time.Second, Client: &http.Client{},
		Now: func() time.Time { return now }}
	req := &Request{CertCN: "device", CertChain: []*x509.Certificate{leaf.Cert, p.ca.Cert}}

	respond(now.Add(-time.Minute))
	if got := rv.Authenticate(ctx, req); got.Decision != Allow {
		t.Fatalf("a response without nextUpdate produced a minute ago: %+v, want Allow", got)
	}
	respond(now.Add(-48 * time.Hour))
	if got := rv.Authenticate(ctx, req); got.Decision != Deny || got.Reason != "x509: revocation unknown" {
		t.Fatalf("the same response two days old: %+v, want Deny (revocation unknown)", got)
	}

	// A configured maximum age replaces the default.
	rv.OCSPMaxAge = time.Hour
	respond(now.Add(-59 * time.Minute))
	if got := rv.Authenticate(ctx, req); got.Decision != Allow {
		t.Fatalf("59 minutes old under an hour's maximum: %+v, want Allow", got)
	}
	respond(now.Add(-61 * time.Minute))
	if got := rv.Authenticate(ctx, req); got.Decision != Deny || got.Reason != "x509: revocation unknown" {
		t.Fatalf("61 minutes old under an hour's maximum: %+v, want Deny (revocation unknown)", got)
	}
}
