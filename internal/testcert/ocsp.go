package testcert

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"errors"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"
)

// This file writes OCSP responses and reads OCSP requests for tests, from
// RFC 6960 § 4 directly and with its own structures, so a test of the
// broker's reader does not check it against itself.

var (
	oidSHA1            = asn1.ObjectIdentifier{1, 3, 14, 3, 2, 26}
	oidBasic           = asn1.ObjectIdentifier{1, 3, 6, 1, 5, 5, 7, 48, 1, 1}
	oidECDSAWithSHA256 = asn1.ObjectIdentifier{1, 2, 840, 10045, 4, 3, 2}
)

type wCertID struct {
	Alg      pkix.AlgorithmIdentifier
	NameHash []byte
	KeyHash  []byte
	Serial   *big.Int
}

type wRevoked struct {
	At time.Time `asn1:"generalized"`
}

type wSingle struct {
	ID         wCertID
	Status     asn1.RawValue
	ThisUpdate time.Time `asn1:"generalized"`
	NextUpdate time.Time `asn1:"generalized,explicit,tag:0,optional"`
}

type wData struct {
	ResponderID asn1.RawValue
	ProducedAt  time.Time `asn1:"generalized"`
	Responses   []wSingle
}

type wBasic struct {
	Data      asn1.RawValue
	Alg       pkix.AlgorithmIdentifier
	Signature asn1.BitString
	Certs     []asn1.RawValue `asn1:"explicit,tag:0,optional"`
}

type wBytes struct {
	Type  asn1.ObjectIdentifier
	Bytes []byte
}

type wResponse struct {
	Status asn1.Enumerated
	Bytes  wBytes `asn1:"explicit,tag:0,optional"`
}

type wStatusOnly struct {
	Status asn1.Enumerated
}

// ResponseSpec is one OCSP response to write.
type ResponseSpec struct {
	// ResponseStatus 0 is successful; any other writes only the status.
	ResponseStatus int
	Issuer         *x509.Certificate
	Serial         *big.Int
	// CertStatus is "good", "revoked" or "unknown".
	CertStatus string
	ThisUpdate time.Time
	// NextUpdate zero is omitted.
	NextUpdate time.Time
	// Signer and SignerKey sign the response; Signer goes into the
	// response's certificates unless it is the issuer.
	Signer    *x509.Certificate
	SignerKey *ecdsa.PrivateKey
	// ByKey names the responder by key hash rather than by name.
	ByKey bool
}

func spkiHash(c *x509.Certificate) []byte {
	var spki struct {
		Alg pkix.AlgorithmIdentifier
		Key asn1.BitString
	}
	_, _ = asn1.Unmarshal(c.RawSubjectPublicKeyInfo, &spki)
	h := sha1.Sum(spki.Key.Bytes)
	return h[:]
}

// OCSPResponse writes the response the spec describes.
func OCSPResponse(s ResponseSpec) ([]byte, error) {
	if s.ResponseStatus != 0 {
		return asn1.Marshal(wStatusOnly{Status: asn1.Enumerated(s.ResponseStatus)})
	}
	nh := sha1.Sum(s.Issuer.RawSubject)
	single := wSingle{
		ID: wCertID{Alg: pkix.AlgorithmIdentifier{Algorithm: oidSHA1, Parameters: asn1.NullRawValue},
			NameHash: nh[:], KeyHash: spkiHash(s.Issuer), Serial: s.Serial},
		ThisUpdate: s.ThisUpdate.UTC().Truncate(time.Second),
		NextUpdate: s.NextUpdate.UTC().Truncate(time.Second),
	}
	switch s.CertStatus {
	case "good":
		single.Status = asn1.RawValue{Class: asn1.ClassContextSpecific, Tag: 0}
	case "unknown":
		single.Status = asn1.RawValue{Class: asn1.ClassContextSpecific, Tag: 2}
	case "revoked":
		inner, err := asn1.Marshal(wRevoked{At: s.ThisUpdate.Add(-time.Hour).UTC().Truncate(time.Second)})
		if err != nil {
			return nil, err
		}
		// [1] IMPLICIT RevokedInfo: the SEQUENCE's contents under tag 1.
		var seq asn1.RawValue
		if _, err := asn1.Unmarshal(inner, &seq); err != nil {
			return nil, err
		}
		single.Status = asn1.RawValue{Class: asn1.ClassContextSpecific, Tag: 1, IsCompound: true, Bytes: seq.Bytes}
	default:
		return nil, errors.New("testcert: unknown certificate status " + s.CertStatus)
	}
	var rid asn1.RawValue
	if s.ByKey {
		inner, _ := asn1.Marshal(spkiHash(s.Signer))
		rid = asn1.RawValue{Class: asn1.ClassContextSpecific, Tag: 2, IsCompound: true, Bytes: inner}
	} else {
		rid = asn1.RawValue{Class: asn1.ClassContextSpecific, Tag: 1, IsCompound: true, Bytes: s.Signer.RawSubject}
	}
	data, err := asn1.Marshal(wData{ResponderID: rid, ProducedAt: s.ThisUpdate.UTC().Truncate(time.Second), Responses: []wSingle{single}})
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(data)
	sig, err := s.SignerKey.Sign(rand.Reader, digest[:], crypto.SHA256)
	if err != nil {
		return nil, err
	}
	basic := wBasic{Data: asn1.RawValue{FullBytes: data}, Alg: pkix.AlgorithmIdentifier{Algorithm: oidECDSAWithSHA256},
		Signature: asn1.BitString{Bytes: sig, BitLength: 8 * len(sig)}}
	if s.Signer.Raw != nil && string(s.Signer.Raw) != string(s.Issuer.Raw) {
		basic.Certs = []asn1.RawValue{{FullBytes: s.Signer.Raw}}
	}
	b, err := asn1.Marshal(basic)
	if err != nil {
		return nil, err
	}
	return asn1.Marshal(wResponse{Bytes: wBytes{Type: oidBasic, Bytes: b}})
}

type rRequest struct {
	TBS struct {
		List []struct {
			ID wCertID
		}
	}
}

// RequestedSerial reads the serial number an OCSP request asks about.
func RequestedSerial(der []byte) (*big.Int, error) {
	var r rRequest
	if _, err := asn1.Unmarshal(der, &r); err != nil {
		return nil, err
	}
	if len(r.TBS.List) != 1 {
		return nil, errors.New("testcert: want one request")
	}
	return r.TBS.List[0].ID.Serial, nil
}

// Responder serves OCSP (POST /ocsp) and a CRL (GET /crl) for a CA, with
// fields a test changes under Lock.
type Responder struct {
	URL string
	CA  *CA
	srv *httptest.Server

	mu sync.Mutex
	// Revoked and Unknown are the serials answered so; others are good.
	Revoked map[int64]bool
	Unknown map[int64]bool
	// Lifetime is each answer's and the CRL's nextUpdate after now.
	Lifetime time.Duration
	Now      func() time.Time
	Delay    time.Duration
	Down     bool
	// Signer, when set, signs OCSP answers as a delegated responder.
	Signer *Leaf
	counts map[string]int
}

// NewResponder starts a responder for ca that stops with the test.
func NewResponder(t interface{ Cleanup(func()) }, ca *CA) *Responder {
	r := &Responder{CA: ca, Revoked: map[int64]bool{}, Unknown: map[int64]bool{}, Lifetime: time.Hour, Now: time.Now, counts: map[string]int{}}
	r.srv = httptest.NewServer(http.HandlerFunc(r.serve))
	r.URL = r.srv.URL
	t.Cleanup(r.srv.Close)
	return r
}

// Lock and Unlock guard the exported fields.
func (r *Responder) Lock()   { r.mu.Lock() }
func (r *Responder) Unlock() { r.mu.Unlock() }

// Count is how many requests an endpoint answered: "ocsp" or "crl".
func (r *Responder) Count(endpoint string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counts[endpoint]
}

func (r *Responder) serve(w http.ResponseWriter, req *http.Request) {
	r.mu.Lock()
	down, delay, now, life := r.Down, r.Delay, r.Now(), r.Lifetime
	r.mu.Unlock()
	if down {
		http.Error(w, "down", http.StatusServiceUnavailable)
		return
	}
	time.Sleep(delay)
	switch req.URL.Path {
	case "/ocsp":
		r.mu.Lock()
		r.counts["ocsp"]++
		r.mu.Unlock()
		body, _ := io.ReadAll(req.Body)
		serial, err := RequestedSerial(body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		r.mu.Lock()
		status := "good"
		if r.Revoked[serial.Int64()] {
			status = "revoked"
		} else if r.Unknown[serial.Int64()] {
			status = "unknown"
		}
		signer, key := r.CA.Cert, r.CA.Key
		if r.Signer != nil {
			signer, key = r.Signer.Cert, r.Signer.Key
		}
		r.mu.Unlock()
		der, err := OCSPResponse(ResponseSpec{Issuer: r.CA.Cert, Serial: serial, CertStatus: status,
			ThisUpdate: now.Add(-time.Minute), NextUpdate: now.Add(life), Signer: signer, SignerKey: key})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/ocsp-response")
		_, _ = w.Write(der)
	case "/crl":
		r.mu.Lock()
		r.counts["crl"]++
		var revoked []int64
		for s := range r.Revoked {
			revoked = append(revoked, s)
		}
		r.mu.Unlock()
		der, err := r.CA.CRL(revoked, now.Add(-time.Minute), now.Add(life))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_, _ = w.Write(der)
	default:
		http.NotFound(w, req)
	}
}
