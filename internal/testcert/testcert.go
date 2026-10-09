// Package testcert generates throwaway certificate authorities and leaf
// certificates for tests. It is not used by the broker itself.
package testcert

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

// CA is a self-signed authority that can issue leaves.
type CA struct {
	Cert *x509.Certificate
	Key  *ecdsa.PrivateKey
	PEM  []byte
}

// NewCA creates a CA valid for one day.
func NewCA(name string) (*CA, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	return &CA{Cert: cert, Key: key, PEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}, nil
}

// Leaf is an issued certificate with its key, both PEM-encoded and parsed.
type Leaf struct {
	CertPEM []byte
	KeyPEM  []byte
	Cert    *x509.Certificate
	Key     *ecdsa.PrivateKey
}

// Options add revocation endpoints, or make the leaf a delegated OCSP
// signer.
type Options struct {
	OCSPServer            []string
	CRLDistributionPoints []string
	OCSPSigning           bool
}

// Issue creates a leaf for a common name, valid for localhost and 127.0.0.1,
// usable as a server or a client certificate.
func (ca *CA) Issue(cn string, serial int64) (*Leaf, error) {
	return ca.IssueWith(cn, serial, Options{})
}

// IssueWith is Issue with options.
func (ca *CA) IssueWith(cn string, serial int64, o Options) (*Leaf, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(serial),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
		OCSPServer:            o.OCSPServer,
		CRLDistributionPoints: o.CRLDistributionPoints,
	}
	if o.OCSPSigning {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageOCSPSigning}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.Cert, &key.PublicKey, ca.Key)
	if err != nil {
		return nil, err
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, err
	}
	return &Leaf{
		CertPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		KeyPEM:  pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
		Cert:    cert,
		Key:     key,
	}, nil
}

// CRL is a DER revocation list the CA signed, listing the serials.
func (ca *CA) CRL(revoked []int64, thisUpdate, nextUpdate time.Time, extensions ...pkix.Extension) ([]byte, error) {
	var entries []x509.RevocationListEntry
	for _, s := range revoked {
		entries = append(entries, x509.RevocationListEntry{SerialNumber: big.NewInt(s), RevocationTime: thisUpdate.Add(-time.Minute)})
	}
	return x509.CreateRevocationList(rand.Reader, &x509.RevocationList{
		Number: big.NewInt(1), ThisUpdate: thisUpdate, NextUpdate: nextUpdate,
		RevokedCertificateEntries: entries, ExtraExtensions: extensions,
	}, ca.Cert, ca.Key)
}

// Write stores the leaf as cert.pem and key.pem under dir and returns the paths.
func (l *Leaf) Write(dir, prefix string) (certFile, keyFile string, err error) {
	certFile = filepath.Join(dir, prefix+"-cert.pem")
	keyFile = filepath.Join(dir, prefix+"-key.pem")
	if err = os.WriteFile(certFile, l.CertPEM, 0o600); err != nil {
		return "", "", err
	}
	if err = os.WriteFile(keyFile, l.KeyPEM, 0o600); err != nil {
		return "", "", err
	}
	return certFile, keyFile, nil
}

// WriteCA stores the CA certificate under dir and returns the path.
func (ca *CA) WriteCA(dir string) (string, error) {
	p := filepath.Join(dir, "ca.pem")
	return p, os.WriteFile(p, ca.PEM, 0o600)
}

// Intermediate issues a CA certificate under ca, valid for one day, with the
// options' revocation endpoints; it issues leaves as ca does.
func (ca *CA) Intermediate(name string, serial int64, o Options) (*CA, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(serial),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		OCSPServer:            o.OCSPServer,
		CRLDistributionPoints: o.CRLDistributionPoints,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.Cert, &key.PublicKey, ca.Key)
	if err != nil {
		return nil, err
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	return &CA{Cert: cert, Key: key, PEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}, nil
}

// IDP describes an issuing distribution point (RFC 5280 § 5.2.5) for a test
// CRL. Relative puts a nameRelativeToCRLIssuer in place of URIs.
type IDP struct {
	URIs                          []string
	Relative                      bool
	UserOnly, CAOnly, SomeReasons bool
	Indirect, AttributeOnly       bool
}

// Extension encodes the IDP as a critical CRL extension.
func (i IDP) Extension() pkix.Extension {
	ctx := func(tag int, compound bool, b []byte) []byte {
		out, err := asn1.Marshal(asn1.RawValue{Class: asn1.ClassContextSpecific, Tag: tag, IsCompound: compound, Bytes: b})
		if err != nil {
			panic(err)
		}
		return out
	}
	yes := []byte{0xff}
	var body []byte
	if i.Relative {
		body = append(body, ctx(0, true, ctx(1, true, nil))...)
	} else if len(i.URIs) > 0 {
		var names []byte
		for _, u := range i.URIs {
			names = append(names, ctx(6, false, []byte(u))...)
		}
		body = append(body, ctx(0, true, ctx(0, true, names))...)
	}
	if i.UserOnly {
		body = append(body, ctx(1, false, yes)...)
	}
	if i.CAOnly {
		body = append(body, ctx(2, false, yes)...)
	}
	if i.SomeReasons {
		body = append(body, ctx(3, false, []byte{0x07, 0x80})...)
	}
	if i.Indirect {
		body = append(body, ctx(4, false, yes)...)
	}
	if i.AttributeOnly {
		body = append(body, ctx(5, false, yes)...)
	}
	seq, err := asn1.Marshal(asn1.RawValue{Class: asn1.ClassUniversal, Tag: asn1.TagSequence, IsCompound: true, Bytes: body})
	if err != nil {
		panic(err)
	}
	return pkix.Extension{Id: asn1.ObjectIdentifier{2, 5, 29, 28}, Critical: true, Value: seq}
}
