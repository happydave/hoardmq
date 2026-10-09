package auth

import (
	"bytes"
	"context"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/pem"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/testcert"
)

type pki struct {
	ca, other *testcert.CA
	leaf      *testcert.Leaf
	signer    *testcert.Leaf // delegated, with id-kp-OCSPSigning
	plain     *testcert.Leaf // issued by the CA, without it
	foreign   *testcert.Leaf // issued by the other CA, with it
}

func newPKI(t *testing.T) *pki {
	t.Helper()
	ca, err := testcert.NewCA("test-ca")
	if err != nil {
		t.Fatal(err)
	}
	other, err := testcert.NewCA("other-ca")
	if err != nil {
		t.Fatal(err)
	}
	must := func(l *testcert.Leaf, err error) *testcert.Leaf {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return l
	}
	return &pki{ca: ca, other: other,
		leaf:    must(ca.Issue("device", 3)),
		signer:  must(ca.IssueWith("responder", 50, testcert.Options{OCSPSigning: true})),
		plain:   must(ca.Issue("not-a-responder", 51)),
		foreign: must(other.IssueWith("foreign-responder", 52, testcert.Options{OCSPSigning: true}))}
}

// The OCSP decision against responses the test writer builds.
func TestAcceptOCSP(t *testing.T) {
	p := newPKI(t)
	now := time.Now().Truncate(time.Second)
	spec := func(change func(*testcert.ResponseSpec)) []byte {
		s := testcert.ResponseSpec{Issuer: p.ca.Cert, Serial: big.NewInt(3), CertStatus: "good",
			ThisUpdate: now.Add(-time.Minute), NextUpdate: now.Add(time.Hour), Signer: p.ca.Cert, SignerKey: p.ca.Key}
		if change != nil {
			change(&s)
		}
		der, err := testcert.OCSPResponse(s)
		if err != nil {
			t.Fatal(err)
		}
		return der
	}
	cases := []struct {
		name string
		der  []byte
		want RevocationStatus
		err  string // a substring of the refusal; "" accepts
	}{
		{"good, signed by the CA", spec(nil), StatusGood, ""},
		{"revoked", spec(func(s *testcert.ResponseSpec) { s.CertStatus = "revoked" }), StatusRevoked, ""},
		{"unknown", spec(func(s *testcert.ResponseSpec) { s.CertStatus = "unknown" }), StatusUnknown, ""},
		{"responder named by key", spec(func(s *testcert.ResponseSpec) { s.ByKey = true }), StatusGood, ""},
		{"signed by another CA's key", spec(func(s *testcert.ResponseSpec) { s.SignerKey = p.other.Key }), 0, "signature"},
		{"a delegated signer", spec(func(s *testcert.ResponseSpec) { s.Signer, s.SignerKey = p.signer.Cert, p.signer.Key }), StatusGood, ""},
		{"a delegated signer by key", spec(func(s *testcert.ResponseSpec) { s.Signer, s.SignerKey, s.ByKey = p.signer.Cert, p.signer.Key, true }), StatusGood, ""},
		{"a signer without OCSP signing", spec(func(s *testcert.ResponseSpec) { s.Signer, s.SignerKey = p.plain.Cert, p.plain.Key }), 0, "lacks OCSP signing"},
		{"a signer another CA issued", spec(func(s *testcert.ResponseSpec) { s.Signer, s.SignerKey = p.foreign.Cert, p.foreign.Key }), 0, "not signed by the issuer"},
		{"the answer for serial 2", spec(func(s *testcert.ResponseSpec) { s.Serial = big.NewInt(2) }), 0, "no answer for this certificate"},
		{"nextUpdate one second ago", spec(func(s *testcert.ResponseSpec) { s.NextUpdate = now.Add(-time.Second) }), 0, "nextUpdate"},
		{"thisUpdate 90 s ahead", spec(func(s *testcert.ResponseSpec) { s.ThisUpdate = now.Add(90 * time.Second) }), 0, "thisUpdate"},
		{"thisUpdate 30 s ahead", spec(func(s *testcert.ResponseSpec) { s.ThisUpdate = now.Add(30 * time.Second) }), StatusGood, ""},
		{"no nextUpdate", spec(func(s *testcert.ResponseSpec) { s.NextUpdate = time.Time{} }), StatusGood, ""},
		{"tryLater", spec(func(s *testcert.ResponseSpec) { s.ResponseStatus = 3 }), 0, "response status 3"},
		{"garbage", []byte("not DER"), 0, "malformed"},
	}
	// The delegated signer is valid for a day: two days on, a response it
	// signed with a year to run is refused.
	later := now.Add(48 * time.Hour)
	stale := spec(func(s *testcert.ResponseSpec) {
		s.Signer, s.SignerKey, s.ThisUpdate, s.NextUpdate = p.signer.Cert, p.signer.Key, later.Add(-time.Minute), later.Add(365*24*time.Hour)
	})
	if _, err := AcceptOCSP(stale, p.leaf.Cert, p.ca.Cert, later); err == nil || !strings.Contains(err.Error(), "not valid now") {
		t.Errorf("a delegated signer past its validity: %v", err)
	}
	for _, c := range cases {
		a, err := AcceptOCSP(c.der, p.leaf.Cert, p.ca.Cert, now)
		switch {
		case c.err == "" && err != nil:
			t.Errorf("%s: refused: %v", c.name, err)
		case c.err == "" && a.Status != c.want:
			t.Errorf("%s: %v, want %v", c.name, a.Status, c.want)
		case c.err != "" && (err == nil || !strings.Contains(err.Error(), c.err)):
			t.Errorf("%s: %v %v, want a refusal for %q", c.name, a, err, c.err)
		}
	}
}

// The request names the certificate by the issuer's name and key hashes and
// the serial.
func TestOCSPRequest(t *testing.T) {
	p := newPKI(t)
	der, err := OCSPRequest(p.leaf.Cert, p.ca.Cert)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := testcert.RequestedSerial(der)
	if err != nil || serial.Int64() != 3 {
		t.Fatalf("serial %v %v", serial, err)
	}
	var req ocspRequestASN1
	if _, err := asn1.Unmarshal(der, &req); err != nil {
		t.Fatal(err)
	}
	id := req.TBSRequest.RequestList[0].Cert
	want, _ := certID(p.leaf.Cert, p.ca.Cert)
	if string(id.IssuerKeyHash) != string(want.IssuerKeyHash) || string(id.IssuerNameHash) != string(want.IssuerNameHash) {
		t.Fatal("hashes")
	}
	if string(id.IssuerKeyHash) == string(id.IssuerNameHash) {
		t.Fatal("the key hash is the name hash")
	}
}

// The CRL decision.
func TestAcceptCRL(t *testing.T) {
	p := newPKI(t)
	now := time.Now().Truncate(time.Second)
	crl := func(ca *testcert.CA, this, next time.Time, ext ...pkix.Extension) []byte {
		t.Helper()
		der, err := ca.CRL([]int64{7}, this, next, ext...)
		if err != nil {
			t.Fatal(err)
		}
		return der
	}
	good := crl(p.ca, now.Add(-time.Minute), now.Add(time.Hour))
	c, err := AcceptCRL(good, p.ca.Cert, now)
	if err != nil {
		t.Fatal(err)
	}
	serial := func(n int64) *x509.Certificate { return &x509.Certificate{SerialNumber: big.NewInt(n)} }
	if c.Status(serial(7)) != StatusRevoked || c.Status(serial(8)) != StatusGood {
		t.Fatalf("7 %v, 8 %v", c.Status(serial(7)), c.Status(serial(8)))
	}
	idp := pkix.Extension{Id: asn1.ObjectIdentifier{2, 5, 29, 28}, Critical: true, Value: []byte{0x30, 0x00}}
	for name, c := range map[string]struct {
		der []byte
		err string
	}{
		"signed by another CA":        {crl(p.other, now.Add(-time.Minute), now.Add(time.Hour)), "another issuer"},
		"past its nextUpdate":         {crl(p.ca, now.Add(-2*time.Hour), now.Add(-time.Second)), "nextUpdate"},
		"thisUpdate 90 s ahead":       {crl(p.ca, now.Add(90*time.Second), now.Add(time.Hour)), "thisUpdate"},
		"an empty distribution point": {crl(p.ca, now.Add(-time.Minute), now.Add(time.Hour), idp), "issuing distribution point empty"},
	} {
		if _, err := AcceptCRL(c.der, p.ca.Cert, now); err == nil || !strings.Contains(err.Error(), c.err) {
			t.Errorf("%s: %v, want %q", name, err, c.err)
		}
	}
	// Another CA of the same name: the issuer matches, the signature does not.
	impostor, err := testcert.NewCA("test-ca")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := AcceptCRL(crl(impostor, now.Add(-time.Minute), now.Add(time.Hour)), p.ca.Cert, now); err == nil || !strings.Contains(err.Error(), "signature") {
		t.Errorf("a CRL under this CA's name, another key: %v", err)
	}
}

// The outcome over the sources' statements and the policy.
func TestRevocationOutcome(t *testing.T) {
	G, R, U := StatusGood, StatusRevoked, StatusUnknown
	for _, c := range []struct {
		ocsp, crl       RevocationStatus
		ocspOn, crlOn   bool
		failOpen, creds bool
		want            Decision
		reason          string
	}{
		{G, U, true, true, false, false, Allow, ""},
		{R, U, true, true, false, false, Deny, "revoked (ocsp)"},
		{U, R, true, true, false, false, Deny, "revoked (crl)"},
		{U, G, true, true, false, false, Allow, ""},
		{U, U, true, true, false, false, Deny, "revocation unknown"},
		{U, U, true, true, true, false, Allow, ""},
		{G, U, true, true, false, true, Pass, ""},
		{R, U, true, true, false, true, Deny, "revoked (ocsp)"},
		{U, R, false, true, false, false, Deny, "revoked (crl)"},
		{R, G, false, true, false, false, Allow, ""},
		{U, U, true, true, true, true, Pass, ""},
		{G, R, true, true, false, false, Allow, ""},
		{R, G, true, true, false, false, Deny, "revoked (ocsp)"},
	} {
		d, reason, _ := revocationOutcome(c.ocsp, c.crl, c.ocspOn, c.crlOn, c.failOpen, c.creds)
		if d != c.want || !strings.Contains(reason, c.reason) {
			t.Errorf("%+v: %v %q", c, d, reason)
		}
	}
}

// revocationFixture is a CA with a responder serving both sources, and a
// leaf whose certificate names them.
func revocationFixture(t *testing.T) (*pki, *testcert.Responder, *Revocation, map[string]int) {
	t.Helper()
	p := newPKI(t)
	r := testcert.NewResponder(t, p.ca)
	leaf, err := p.ca.IssueWith("device", 3, testcert.Options{OCSPServer: []string{r.URL + "/ocsp"}, CRLDistributionPoints: []string{"ldap://x/crl", r.URL + "/crl"}})
	if err != nil {
		t.Fatal(err)
	}
	p.leaf = leaf
	counts := map[string]int{}
	rv := &Revocation{OCSP: true, CRL: true, FetchWait: 2 * time.Second, Timeout: 5 * time.Second, Client: &http.Client{},
		Count: func(name string, l map[string]string) { counts[name+"{"+l["method"]+l["status"]+l["policy"]+"}"]++ }}
	return p, r, rv, counts
}

func chainOf(p *pki) *Request {
	return &Request{CertCN: "device", CertChain: []*x509.Certificate{p.leaf.Cert, p.ca.Cert}}
}

func TestRevocationAuthenticator(t *testing.T) {
	ctx := context.Background()
	p, r, rv, counts := revocationFixture(t)
	if got := rv.Authenticate(ctx, &Request{HasUsername: true, Username: "u"}); got.Decision != Pass {
		t.Fatalf("no certificate: %+v", got)
	}
	if got := rv.Authenticate(ctx, chainOf(p)); got.Decision != Allow {
		t.Fatalf("good: %+v", got)
	}
	if got := rv.Authenticate(ctx, chainOf(p)); got.Decision != Allow || r.Count("ocsp") != 1 {
		t.Fatalf("good again: %+v, %d OCSP requests", got, r.Count("ocsp"))
	}
	withPassword := chainOf(p)
	withPassword.HasUsername, withPassword.HasPassword = true, true
	if got := rv.Authenticate(ctx, withPassword); got.Decision != Pass {
		t.Fatalf("good, with a password: %+v", got)
	}
	// Revoked by OCSP: a new serial, so no cached answer.
	revoked, _ := p.ca.IssueWith("device", 4, testcert.Options{OCSPServer: []string{r.URL + "/ocsp"}, CRLDistributionPoints: []string{r.URL + "/crl"}})
	r.Lock()
	r.Revoked[4] = true
	r.Unlock()
	req := &Request{CertCN: "device", CertChain: []*x509.Certificate{revoked.Cert, p.ca.Cert}, HasUsername: true, HasPassword: true}
	if got := rv.Authenticate(ctx, req); got.Decision != Deny || got.Reason != "x509: revoked (ocsp)" {
		t.Fatalf("revoked: %+v", got)
	}
	if counts["hoardmq_auth_revocation_checks_total{ocsprevoked}"] != 1 {
		t.Fatalf("counts %v", counts)
	}
	// OCSP says unknown: the CRL decides.
	unknown, _ := p.ca.IssueWith("device", 5, testcert.Options{OCSPServer: []string{r.URL + "/ocsp"}, CRLDistributionPoints: []string{r.URL + "/crl"}})
	r.Lock()
	r.Unknown[5] = true
	r.Unlock()
	req = &Request{CertCN: "device", CertChain: []*x509.Certificate{unknown.Cert, p.ca.Cert}}
	if got := rv.Authenticate(ctx, req); got.Decision != Allow || r.Count("crl") != 1 {
		t.Fatalf("OCSP unknown, CRL good: %+v, %d CRL requests", got, r.Count("crl"))
	}
	r.Lock()
	r.Revoked[5] = true
	r.Unlock()
	fresh := &Revocation{OCSP: true, CRL: true, FetchWait: 2 * time.Second, Timeout: 5 * time.Second, Client: &http.Client{}}
	if got := fresh.Authenticate(ctx, req); got.Decision != Deny || got.Reason != "x509: revoked (ocsp)" {
		t.Fatalf("revoked now: %+v", got)
	}
	crlOnly := &Revocation{CRL: true, FetchWait: 2 * time.Second, Timeout: 5 * time.Second, Client: &http.Client{}}
	if got := crlOnly.Authenticate(ctx, req); got.Decision != Deny || got.Reason != "x509: revoked (crl)" {
		t.Fatalf("CRL only, revoked: %+v", got)
	}
}

// Unreachable sources follow the policy, and every admission it makes on an
// unknown status is counted.
func TestRevocationPolicy(t *testing.T) {
	ctx := context.Background()
	p, r, rv, counts := revocationFixture(t)
	r.Lock()
	r.Down = true
	r.Unlock()
	if got := rv.Authenticate(ctx, chainOf(p)); got.Decision != Deny || got.Reason != "x509: revocation unknown" {
		t.Fatalf("fail-closed: %+v", got)
	}
	rv.FailOpen = true
	if got := rv.Authenticate(ctx, chainOf(p)); got.Decision != Allow {
		t.Fatalf("fail-open: %+v", got)
	}
	if counts["hoardmq_auth_revocation_unknown_total{closed}"] != 1 || counts["hoardmq_auth_revocation_unknown_total{open}"] != 1 {
		t.Fatalf("counts %v", counts)
	}
	// No issuer in the chain, and no endpoints at all: unknown.
	rv.FailOpen = false
	if got := rv.Authenticate(ctx, &Request{CertCN: "device", CertChain: []*x509.Certificate{p.leaf.Cert}}); got.Decision != Deny {
		t.Fatalf("no issuer: %+v", got)
	}
	bare, _ := p.ca.Issue("bare", 9)
	if got := rv.Authenticate(ctx, &Request{CertCN: "bare", CertChain: []*x509.Certificate{bare.Cert, p.ca.Cert}}); got.Decision != Deny {
		t.Fatalf("no endpoints: %+v", got)
	}
}

// A slow responder: the CONNECT waits FetchWait; the fetch completes and its
// answer serves the next lookup from the cache.
func TestRevocationWaitAndCache(t *testing.T) {
	ctx := context.Background()
	p, r, rv, _ := revocationFixture(t)
	rv.CRL = false
	rv.FetchWait = 200 * time.Millisecond
	r.Lock()
	r.Delay = time.Second
	r.Unlock()
	start := time.Now()
	if got := rv.Authenticate(ctx, chainOf(p)); got.Decision != Deny {
		t.Fatalf("a slow responder: %+v", got)
	}
	if el := time.Since(start); el > 700*time.Millisecond {
		t.Fatalf("decided after %v", el)
	}
	time.Sleep(1200 * time.Millisecond)
	r.Lock()
	r.Delay = 0
	r.Unlock()
	if got := rv.Authenticate(ctx, chainOf(p)); got.Decision != Allow || r.Count("ocsp") != 1 {
		t.Fatalf("after the fetch: %+v, %d requests", got, r.Count("ocsp"))
	}
}

// Cached answers are used until their nextUpdate, then fetched again.
func TestRevocationCacheLapses(t *testing.T) {
	ctx := context.Background()
	p, r, rv, _ := revocationFixture(t)
	clock := time.Now()
	rv.Now = func() time.Time { return clock }
	r.Lock()
	r.Now = func() time.Time { return clock }
	r.Unlock()
	for range 2 {
		if got := rv.Authenticate(ctx, chainOf(p)); got.Decision != Allow {
			t.Fatal(got)
		}
	}
	if r.Count("ocsp") != 1 {
		t.Fatalf("two lookups within nextUpdate: %d requests", r.Count("ocsp"))
	}
	clock = clock.Add(61 * time.Minute)
	r.Lock()
	r.Now = func() time.Time { return clock }
	r.Unlock()
	if got := rv.Authenticate(ctx, chainOf(p)); got.Decision != Allow || r.Count("ocsp") != 2 {
		t.Fatalf("after nextUpdate: %+v, %d requests", got, r.Count("ocsp"))
	}
	// The same for the CRL.
	rv.OCSP = false
	for range 2 {
		_ = rv.Authenticate(ctx, chainOf(p))
	}
	if r.Count("crl") != 1 {
		t.Fatalf("two CRL lookups within nextUpdate: %d GETs", r.Count("crl"))
	}
	clock = clock.Add(61 * time.Minute)
	r.Lock()
	r.Now = func() time.Time { return clock }
	r.Unlock()
	_ = rv.Authenticate(ctx, chainOf(p))
	if r.Count("crl") != 2 {
		t.Fatalf("after the CRL's nextUpdate: %d GETs", r.Count("crl"))
	}
}

func readPEM(t *testing.T, name string) *x509.Certificate {
	t.Helper()
	dir, file := "ocsp", name
	if d, f, ok := strings.Cut(name, "/"); ok {
		dir, file = d, f
	}
	data, err := os.ReadFile(filepath.Join("testdata", dir, file))
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(data)
	c, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func readDER(t *testing.T, name string) []byte {
	t.Helper()
	dir, file := "ocsp", name
	if d, f, ok := strings.Cut(name, "/"); ok {
		dir, file = d, f
	}
	data, err := os.ReadFile(filepath.Join("testdata", dir, file))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// Fixtures openssl made (testdata/ocsp/generate.sh): its request for the
// same certificate is the broker's byte for byte, and its responses and CRL
// read as it wrote them.
func TestOpenSSLFixtures(t *testing.T) {
	ca, good, revoked := readPEM(t, "ca.pem"), readPEM(t, "leaf-good.pem"), readPEM(t, "leaf-revoked.pem")
	for _, c := range []struct {
		leaf *x509.Certificate
		file string
	}{{good, "req-good.der"}, {revoked, "req-revoked.der"}} {
		mine, err := OCSPRequest(c.leaf, ca)
		if err != nil || !bytes.Equal(mine, readDER(t, c.file)) {
			t.Errorf("%s: the broker's request differs from openssl's: %x", c.file, mine)
		}
	}
	now := time.Now()
	for _, c := range []struct {
		file string
		leaf *x509.Certificate
		want RevocationStatus
	}{
		{"resp-good-ca.der", good, StatusGood},
		{"resp-good-delegated.der", good, StatusGood},
		{"resp-revoked-ca.der", revoked, StatusRevoked},
		{"resp-revoked-delegated.der", revoked, StatusRevoked},
	} {
		a, err := AcceptOCSP(readDER(t, c.file), c.leaf, ca, now)
		if err != nil || a.Status != c.want {
			t.Errorf("%s: %v %v, want %v", c.file, a.Status, err, c.want)
		}
		if _, err := AcceptOCSP(readDER(t, c.file), c.leaf, ca, now.AddDate(11, 0, 0)); err == nil {
			t.Errorf("%s: accepted eleven years on", c.file)
		}
	}
	if _, err := AcceptOCSP(readDER(t, "resp-good-ca.der"), revoked, ca, now); err == nil {
		t.Error("the good leaf's answer was accepted for the revoked leaf")
	}
	crl, err := AcceptCRL(readDER(t, "crl.der"), ca, now)
	if err != nil {
		t.Fatal(err)
	}
	if crl.Status(revoked) != StatusRevoked || crl.Status(good) != StatusGood {
		t.Errorf("openssl's CRL: revoked %v, good %v", crl.Status(revoked), crl.Status(good))
	}
}

// openssl's CRL with a critical issuing distribution point naming partition 1
// and user certificates (testdata/idp/generate.sh): it lists serials 10 and
// 12, but 12 names partition 2, which the list does not speak for.
func TestOpenSSLIDPFixture(t *testing.T) {
	ca := readPEM(t, "idp/ca.pem")
	crl, err := AcceptCRL(readDER(t, "idp/crl-idp.der"), ca, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		file string
		want RevocationStatus
	}{
		{"idp/leaf-listed.pem", StatusRevoked},
		{"idp/leaf-unlisted.pem", StatusGood},
		{"idp/leaf-elsewhere.pem", StatusUnknown},
	} {
		if got := crl.Status(readPEM(t, c.file)); got != c.want {
			t.Errorf("%s: %v, want %v", c.file, got, c.want)
		}
	}
}
