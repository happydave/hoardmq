package auth

import (
	"context"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/testcert"
)

const (
	partURL  = "http://crl.example/part1.crl"
	otherURL = "http://crl.example/part2.crl"
)

// scopedCRL is ca's CRL with the issuing distribution point, listing the
// serials, as AcceptCRL accepts it.
func scopedCRL(t *testing.T, ca *testcert.CA, idp testcert.IDP, revoked ...int64) *CRL {
	t.Helper()
	now := time.Now()
	der, err := ca.CRL(revoked, now.Add(-time.Minute), now.Add(time.Hour), idp.Extension())
	if err != nil {
		t.Fatal(err)
	}
	c, err := AcceptCRL(der, ca.Cert, now)
	if err != nil {
		t.Fatalf("AcceptCRL: %v", err)
	}
	return c
}

func leafWith(t *testing.T, ca *testcert.CA, serial int64, dp ...string) *x509.Certificate {
	t.Helper()
	l, err := ca.IssueWith("device", serial, testcert.Options{CRLDistributionPoints: dp})
	if err != nil {
		t.Fatal(err)
	}
	return l.Cert
}

func caWith(t *testing.T, ca *testcert.CA, serial int64, dp ...string) *x509.Certificate {
	t.Helper()
	i, err := ca.Intermediate("sub", serial, testcert.Options{CRLDistributionPoints: dp})
	if err != nil {
		t.Fatal(err)
	}
	return i.Cert
}

// A CRL scoped by an issuing distribution point speaks only for the
// certificates in its scope: those naming its point, of its kind. Outside
// it the status is unknown, never good.
func TestCRLScope(t *testing.T) {
	ca, err := testcert.NewCA("scope-ca")
	if err != nil {
		t.Fatal(err)
	}
	userPart := scopedCRL(t, ca, testcert.IDP{URIs: []string{partURL}, UserOnly: true}, 10)
	caPart := scopedCRL(t, ca, testcert.IDP{CAOnly: true}, 20)
	someReasons := scopedCRL(t, ca, testcert.IDP{SomeReasons: true}, 30)
	unscoped := scopedCRLNoIDP(t, ca, 40)
	for _, c := range []struct {
		name string
		crl  *CRL
		cert *x509.Certificate
		want RevocationStatus
	}{
		{"user part: a listed leaf naming it", userPart, leafWith(t, ca, 10, partURL), StatusRevoked},
		{"user part: an unlisted leaf naming it", userPart, leafWith(t, ca, 11, partURL), StatusGood},
		{"user part: a listed leaf naming another part", userPart, leafWith(t, ca, 10, otherURL), StatusUnknown},
		{"user part: an unlisted leaf naming another part", userPart, leafWith(t, ca, 12, otherURL), StatusUnknown},
		{"user part: a leaf naming none", userPart, leafWith(t, ca, 13), StatusUnknown},
		{"user part: a CA naming it", userPart, caWith(t, ca, 14, partURL), StatusUnknown},
		{"CA part: a listed intermediate", caPart, caWith(t, ca, 20), StatusRevoked},
		{"CA part: an unlisted intermediate", caPart, caWith(t, ca, 21), StatusGood},
		{"CA part: a leaf", caPart, leafWith(t, ca, 22), StatusUnknown},
		{"some reasons: listed", someReasons, leafWith(t, ca, 30), StatusRevoked},
		{"some reasons: unlisted", someReasons, leafWith(t, ca, 31), StatusUnknown},
		{"no IDP: listed", unscoped, leafWith(t, ca, 40, otherURL), StatusRevoked},
		{"no IDP: unlisted, any point", unscoped, caWith(t, ca, 41, otherURL), StatusGood},
	} {
		if got := c.crl.Status(c.cert); got != c.want {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}

func scopedCRLNoIDP(t *testing.T, ca *testcert.CA, revoked ...int64) *CRL {
	t.Helper()
	now := time.Now()
	der, err := ca.CRL(revoked, now.Add(-time.Minute), now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	c, err := AcceptCRL(der, ca.Cert, now)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// Scopes this reader does not implement make the list unusable, as does any
// other critical extension.
func TestCRLScopesRefused(t *testing.T) {
	ca, err := testcert.NewCA("refuse-ca")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	truncated := testcert.IDP{URIs: []string{partURL}}.Extension()
	truncated.Value = truncated.Value[:len(truncated.Value)-3]
	delta := pkix.Extension{Id: asn1.ObjectIdentifier{2, 5, 29, 27}, Critical: true, Value: []byte{0x02, 0x01, 0x01}}
	for name, c := range map[string]struct {
		ext  pkix.Extension
		want string
	}{
		"relative name":   {testcert.IDP{Relative: true}.Extension(), "relative"},
		"indirect":        {testcert.IDP{URIs: []string{partURL}, Indirect: true}.Extension(), "indirect"},
		"attribute certs": {testcert.IDP{AttributeOnly: true}.Extension(), "attribute"},
		"truncated":       {truncated, "malformed"},
		"delta indicator": {delta, "critical extension 2.5.29.27"},
	} {
		der, err := ca.CRL(nil, now.Add(-time.Minute), now.Add(time.Hour), c.ext)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := AcceptCRL(der, ca.Cert, now); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want %q", name, err, c.want)
		}
	}
}

// chainPKI is root → intermediate → leaf, each CA with its own responder.
type chainPKI struct {
	root, sub   *testcert.CA
	rootR, subR *testcert.Responder
	leaf        *x509.Certificate
}

func newChainPKI(t *testing.T, subOpts func(rootR *testcert.Responder) testcert.Options) *chainPKI {
	t.Helper()
	root, err := testcert.NewCA("root")
	if err != nil {
		t.Fatal(err)
	}
	c := &chainPKI{root: root, rootR: testcert.NewResponder(t, root)}
	if c.sub, err = root.Intermediate("sub", 2, subOpts(c.rootR)); err != nil {
		t.Fatal(err)
	}
	c.subR = testcert.NewResponder(t, c.sub)
	leaf, err := c.sub.IssueWith("device", 3, testcert.Options{OCSPServer: []string{c.subR.URL + "/ocsp"}, CRLDistributionPoints: []string{c.subR.URL + "/crl"}})
	if err != nil {
		t.Fatal(err)
	}
	c.leaf = leaf.Cert
	return c
}

func (c *chainPKI) request() *Request {
	return &Request{CertCN: "device", CertChain: []*x509.Certificate{c.leaf, c.sub.Cert, c.root.Cert}}
}

func newRevocation(ocsp, crl, failOpen bool) *Revocation {
	return &Revocation{OCSP: ocsp, CRL: crl, FailOpen: failOpen, FetchWait: 2 * time.Second, Timeout: 5 * time.Second, Client: &http.Client{}}
}

// Every certificate below the root is checked: a revoked intermediate is
// refused over a good leaf, by either source; an intermediate whose status
// no source gives follows the policy.
func TestRevocationChecksTheChain(t *testing.T) {
	ctx := context.Background()
	endpoints := func(rootR *testcert.Responder) testcert.Options {
		return testcert.Options{OCSPServer: []string{rootR.URL + "/ocsp"}, CRLDistributionPoints: []string{rootR.URL + "/crl"}}
	}
	good := newChainPKI(t, endpoints)
	if got := newRevocation(true, true, false).Authenticate(ctx, good.request()); got.Decision != Allow {
		t.Fatalf("good chain: %+v", got)
	}
	if good.rootR.Count("ocsp") != 1 {
		t.Fatalf("the root's responder was asked %d times for the intermediate, want 1", good.rootR.Count("ocsp"))
	}
	// The configured endpoints are the leaf's: sent the intermediate, the
	// intermediate's responder could not answer for it.
	pinned := newRevocation(true, false, false)
	pinned.OCSPURL = good.subR.URL + "/ocsp"
	if got := pinned.Authenticate(ctx, good.request()); got.Decision != Allow {
		t.Fatalf("a configured responder for the leaf: %+v", got)
	}

	revoked := newChainPKI(t, endpoints)
	revoked.rootR.Lock()
	revoked.rootR.Revoked[2] = true
	revoked.rootR.Unlock()
	if got := newRevocation(true, true, false).Authenticate(ctx, revoked.request()); got.Decision != Deny || got.Reason != "x509: intermediate revoked (ocsp)" {
		t.Fatalf("intermediate revoked, OCSP: %+v", got)
	}
	if got := newRevocation(false, true, false).Authenticate(ctx, revoked.request()); got.Decision != Deny || got.Reason != "x509: intermediate revoked (crl)" {
		t.Fatalf("intermediate revoked, CRL only: %+v", got)
	}

	silent := newChainPKI(t, func(rootR *testcert.Responder) testcert.Options {
		return testcert.Options{OCSPServer: []string{rootR.URL + "/ocsp"}}
	})
	silent.rootR.Lock()
	silent.rootR.Unknown[2] = true
	silent.rootR.Unlock()
	if got := newRevocation(true, true, false).Authenticate(ctx, silent.request()); got.Decision != Deny || got.Reason != "x509: revocation unknown" {
		t.Fatalf("intermediate unknown, fail-closed: %+v", got)
	}
	if got := newRevocation(true, true, true).Authenticate(ctx, silent.request()); got.Decision != Allow {
		t.Fatalf("intermediate unknown, fail-open: %+v", got)
	}
}

// A CRL accepted for one issuer never answers for another's certificate,
// even at the same URL.
func TestCRLCacheIsPerIssuer(t *testing.T) {
	ctx := context.Background()
	root, err := testcert.NewCA("root")
	if err != nil {
		t.Fatal(err)
	}
	a, _ := root.Intermediate("a", 2, testcert.Options{})
	b, _ := root.Intermediate("b", 3, testcert.Options{})
	aR := testcert.NewResponder(t, a) // serves only a's list
	leafA, _ := a.IssueWith("device-a", 10, testcert.Options{CRLDistributionPoints: []string{aR.URL + "/crl"}})
	leafB, _ := b.IssueWith("device-b", 11, testcert.Options{CRLDistributionPoints: []string{aR.URL + "/crl"}})
	rv := newRevocation(false, true, false)
	leafOnly := func(leaf *testcert.Leaf, issuer *testcert.CA) *Request {
		return &Request{CertCN: "d", CertChain: []*x509.Certificate{leaf.Cert, issuer.Cert}}
	}
	if got := rv.Authenticate(ctx, leafOnly(leafA, a)); got.Decision != Allow {
		t.Fatalf("precondition: a's leaf: %+v", got)
	}
	if got := rv.Authenticate(ctx, leafOnly(leafB, b)); got.Decision != Deny || got.Reason != "x509: revocation unknown" {
		t.Fatalf("b's leaf, answered from a's list: %+v", got)
	}
}
