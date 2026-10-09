package auth

import (
	"bytes"
	"crypto/sha1"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"errors"
	"fmt"
	"math/big"
	"time"
)

// RevocationStatus is what a revocation source says about a certificate.
type RevocationStatus int

// Statuses. Unknown covers a source with no definite answer: none given,
// "unknown" given, or an answer that could not be accepted.
const (
	StatusUnknown RevocationStatus = iota
	StatusGood
	StatusRevoked
)

func (s RevocationStatus) String() string {
	switch s {
	case StatusGood:
		return "good"
	case StatusRevoked:
		return "revoked"
	}
	return "unknown"
}

// revocationSkew is how far ahead of now an answer's thisUpdate may be.
const revocationSkew = 60 * time.Second

// The OCSP structures of RFC 6960 § 4, as far as a client reads and writes
// them.
var (
	oidSHA1      = asn1.ObjectIdentifier{1, 3, 14, 3, 2, 26}
	oidOCSPBasic = asn1.ObjectIdentifier{1, 3, 6, 1, 5, 5, 7, 48, 1, 1}
)

type ocspCertID struct {
	HashAlgorithm  pkix.AlgorithmIdentifier
	IssuerNameHash []byte
	IssuerKeyHash  []byte
	SerialNumber   *big.Int
}

type ocspRequestEntry struct {
	Cert ocspCertID
}

type ocspTBSRequest struct {
	Version     int `asn1:"explicit,tag:0,default:0,optional"`
	RequestList []ocspRequestEntry
}

type ocspRequestASN1 struct {
	TBSRequest ocspTBSRequest
}

type ocspResponseASN1 struct {
	Status   asn1.Enumerated
	Response ocspResponseBytes `asn1:"explicit,tag:0,optional"`
}

type ocspResponseBytes struct {
	ResponseType asn1.ObjectIdentifier
	Response     []byte
}

type ocspBasicResponse struct {
	TBSResponseData    ocspResponseData
	SignatureAlgorithm pkix.AlgorithmIdentifier
	Signature          asn1.BitString
	Certificates       []asn1.RawValue `asn1:"explicit,tag:0,optional"`
}

type ocspResponseData struct {
	Raw         asn1.RawContent
	Version     int `asn1:"optional,default:0,explicit,tag:0"`
	ResponderID asn1.RawValue
	ProducedAt  time.Time `asn1:"generalized"`
	Responses   []ocspSingleResponse
	Extensions  []pkix.Extension `asn1:"explicit,tag:1,optional"`
}

type ocspSingleResponse struct {
	CertID     ocspCertID
	Good       asn1.Flag        `asn1:"tag:0,optional"`
	Revoked    ocspRevokedInfo  `asn1:"tag:1,optional"`
	Unknown    asn1.Flag        `asn1:"tag:2,optional"`
	ThisUpdate time.Time        `asn1:"generalized"`
	NextUpdate time.Time        `asn1:"generalized,explicit,tag:0,optional"`
	Extensions []pkix.Extension `asn1:"explicit,tag:1,optional"`
}

type ocspRevokedInfo struct {
	RevocationTime time.Time       `asn1:"generalized"`
	Reason         asn1.Enumerated `asn1:"explicit,tag:0,optional"`
}

// keyHash is the SHA-1 of a certificate's subjectPublicKey bits: the CertID's
// issuerKeyHash, and a responder identified by key. SHA-1 is RFC 5019's CertID
// hash, an identifier here and never a signature.
func keyHash(c *x509.Certificate) ([]byte, error) {
	var spki struct {
		Algorithm pkix.AlgorithmIdentifier
		PublicKey asn1.BitString
	}
	if _, err := asn1.Unmarshal(c.RawSubjectPublicKeyInfo, &spki); err != nil {
		return nil, err
	}
	h := sha1.Sum(spki.PublicKey.RightAlign())
	return h[:], nil
}

func certID(leaf, issuer *x509.Certificate) (ocspCertID, error) {
	kh, err := keyHash(issuer)
	if err != nil {
		return ocspCertID{}, err
	}
	nh := sha1.Sum(issuer.RawSubject)
	return ocspCertID{HashAlgorithm: pkix.AlgorithmIdentifier{Algorithm: oidSHA1, Parameters: asn1.NullRawValue},
		IssuerNameHash: nh[:], IssuerKeyHash: kh, SerialNumber: leaf.SerialNumber}, nil
}

// OCSPRequest is the DER request for leaf's status (RFC 6960 § 4.1): one
// CertID by SHA-1, no nonce (RFC 5019), unsigned.
func OCSPRequest(leaf, issuer *x509.Certificate) ([]byte, error) {
	id, err := certID(leaf, issuer)
	if err != nil {
		return nil, err
	}
	return asn1.Marshal(ocspRequestASN1{TBSRequest: ocspTBSRequest{RequestList: []ocspRequestEntry{{Cert: id}}}})
}

// OCSPAnswer is an accepted response's statement about one certificate.
type OCSPAnswer struct {
	Status     RevocationStatus
	ThisUpdate time.Time
	// NextUpdate is zero when the responder gave none.
	NextUpdate time.Time
	RevokedAt  time.Time
}

// signatureAlgorithms maps the signature OIDs a responder uses to what
// Certificate.CheckSignature verifies; SHA-1 signatures are refused there.
var signatureAlgorithms = map[string]x509.SignatureAlgorithm{
	"1.2.840.113549.1.1.5":  x509.SHA1WithRSA,
	"1.2.840.113549.1.1.11": x509.SHA256WithRSA,
	"1.2.840.113549.1.1.12": x509.SHA384WithRSA,
	"1.2.840.113549.1.1.13": x509.SHA512WithRSA,
	"1.2.840.10045.4.3.2":   x509.ECDSAWithSHA256,
	"1.2.840.10045.4.3.3":   x509.ECDSAWithSHA384,
	"1.2.840.10045.4.3.4":   x509.ECDSAWithSHA512,
	"1.3.101.112":           x509.PureEd25519,
}

// AcceptOCSP is the decision on a response to OCSPRequest(leaf, issuer) at
// now: successful, basic, signed by the issuer or by a responder the issuer
// delegated to (id-kp-OCSPSigning, valid at now), naming its signer, with a
// SingleResponse for this CertID that is current. It returns the answer, or
// why the response cannot be used.
func AcceptOCSP(der []byte, leaf, issuer *x509.Certificate, now time.Time) (OCSPAnswer, error) {
	var resp ocspResponseASN1
	if rest, err := asn1.Unmarshal(der, &resp); err != nil || len(rest) != 0 {
		return OCSPAnswer{}, errors.New("ocsp: malformed response")
	}
	if resp.Status != 0 {
		return OCSPAnswer{}, fmt.Errorf("ocsp: response status %d", resp.Status)
	}
	if !resp.Response.ResponseType.Equal(oidOCSPBasic) {
		return OCSPAnswer{}, errors.New("ocsp: not a basic response")
	}
	var basic ocspBasicResponse
	if rest, err := asn1.Unmarshal(resp.Response.Response, &basic); err != nil || len(rest) != 0 {
		return OCSPAnswer{}, errors.New("ocsp: malformed basic response")
	}
	signer, err := ocspSigner(basic, issuer, now)
	if err != nil {
		return OCSPAnswer{}, err
	}
	alg, ok := signatureAlgorithms[basic.SignatureAlgorithm.Algorithm.String()]
	if !ok {
		return OCSPAnswer{}, errors.New("ocsp: unsupported signature algorithm")
	}
	if err := signer.CheckSignature(alg, basic.TBSResponseData.Raw, basic.Signature.RightAlign()); err != nil {
		return OCSPAnswer{}, fmt.Errorf("ocsp: signature: %w", err)
	}
	want, err := certID(leaf, issuer)
	if err != nil {
		return OCSPAnswer{}, err
	}
	for _, r := range basic.TBSResponseData.Responses {
		if !r.CertID.HashAlgorithm.Algorithm.Equal(oidSHA1) || !bytes.Equal(r.CertID.IssuerNameHash, want.IssuerNameHash) ||
			!bytes.Equal(r.CertID.IssuerKeyHash, want.IssuerKeyHash) || r.CertID.SerialNumber == nil || r.CertID.SerialNumber.Cmp(want.SerialNumber) != 0 {
			continue
		}
		if r.ThisUpdate.After(now.Add(revocationSkew)) {
			return OCSPAnswer{}, errors.New("ocsp: thisUpdate in the future")
		}
		if !r.NextUpdate.IsZero() && !r.NextUpdate.After(now) {
			return OCSPAnswer{}, errors.New("ocsp: answer past its nextUpdate")
		}
		a := OCSPAnswer{ThisUpdate: r.ThisUpdate, NextUpdate: r.NextUpdate}
		switch {
		case bool(r.Good):
			a.Status = StatusGood
		case bool(r.Unknown):
			a.Status = StatusUnknown
		case !r.Revoked.RevocationTime.IsZero():
			a.Status, a.RevokedAt = StatusRevoked, r.Revoked.RevocationTime
		default:
			return OCSPAnswer{}, errors.New("ocsp: malformed certificate status")
		}
		return a, nil
	}
	return OCSPAnswer{}, errors.New("ocsp: no answer for this certificate")
}

// ocspSigner finds the certificate the response names as its responder: the
// issuer itself, or a certificate in the response that the issuer signed for
// OCSP signing and that is valid at now.
func ocspSigner(b ocspBasicResponse, issuer *x509.Certificate, now time.Time) (*x509.Certificate, error) {
	candidates := []*x509.Certificate{issuer}
	for _, raw := range b.Certificates {
		c, err := x509.ParseCertificate(raw.FullBytes)
		if err != nil {
			return nil, errors.New("ocsp: malformed responder certificate")
		}
		candidates = append(candidates, c)
	}
	for _, c := range candidates {
		named, err := responderNames(b.TBSResponseData.ResponderID, c)
		if err != nil {
			return nil, err
		}
		if !named {
			continue
		}
		if c == issuer || bytes.Equal(c.Raw, issuer.Raw) {
			return issuer, nil
		}
		if err := c.CheckSignatureFrom(issuer); err != nil {
			return nil, errors.New("ocsp: responder certificate not signed by the issuer")
		}
		delegated := false
		for _, u := range c.ExtKeyUsage {
			delegated = delegated || u == x509.ExtKeyUsageOCSPSigning
		}
		if !delegated {
			return nil, errors.New("ocsp: responder certificate lacks OCSP signing")
		}
		if now.Before(c.NotBefore) || now.After(c.NotAfter) {
			return nil, errors.New("ocsp: responder certificate not valid now")
		}
		return c, nil
	}
	return nil, errors.New("ocsp: no certificate for the responder")
}

// responderNames reports whether the ResponderID names c: by subject
// ([1] Name) or by key hash ([2] OCTET STRING).
func responderNames(id asn1.RawValue, c *x509.Certificate) (bool, error) {
	if id.Class != asn1.ClassContextSpecific {
		return false, errors.New("ocsp: malformed responder identifier")
	}
	switch id.Tag {
	case 1:
		return bytes.Equal(id.Bytes, c.RawSubject), nil
	case 2:
		var h []byte
		if _, err := asn1.Unmarshal(id.Bytes, &h); err != nil {
			return false, errors.New("ocsp: malformed responder key hash")
		}
		kh, err := keyHash(c)
		if err != nil {
			return false, err
		}
		return bytes.Equal(h, kh), nil
	}
	return false, errors.New("ocsp: malformed responder identifier")
}
