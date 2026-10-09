package auth

import (
	"bytes"
	"crypto/x509"
	"encoding/asn1"
	"errors"
	"slices"
	"time"
)

// CRL is an accepted certificate revocation list.
type CRL struct {
	ThisUpdate time.Time
	NextUpdate time.Time
	revoked    map[string]bool
	scope      *crlScope // the issuing distribution point's, nil when the list has none
}

// crlScope is what an issuing distribution point (RFC 5280 § 5.2.5) limits
// a list to: the certificates naming one of its distribution points, of one
// kind, or revoked for some reasons only.
type crlScope struct {
	named       bool // a distributionPoint was given: a certificate must name one of uris
	uris        []string
	userOnly    bool
	caOnly      bool
	someReasons bool
}

var oidIssuingDistributionPoint = asn1.ObjectIdentifier{2, 5, 29, 28}

// AcceptCRL is the decision on a CRL for certificates issuer signed, at now:
// issued by it (by name and signature), current, and free of critical
// extensions other than an issuing distribution point this reader honours.
// It returns the list, or why it cannot be used.
func AcceptCRL(der []byte, issuer *x509.Certificate, now time.Time) (*CRL, error) {
	rl, err := x509.ParseRevocationList(der)
	if err != nil {
		return nil, errors.New("crl: malformed")
	}
	if !bytes.Equal(rl.RawIssuer, issuer.RawSubject) {
		return nil, errors.New("crl: another issuer")
	}
	if err := rl.CheckSignatureFrom(issuer); err != nil {
		return nil, errors.New("crl: signature: " + err.Error())
	}
	if rl.ThisUpdate.After(now.Add(revocationSkew)) {
		return nil, errors.New("crl: thisUpdate in the future")
	}
	if rl.NextUpdate.IsZero() || !rl.NextUpdate.After(now) {
		return nil, errors.New("crl: past its nextUpdate")
	}
	c := &CRL{ThisUpdate: rl.ThisUpdate, NextUpdate: rl.NextUpdate, revoked: map[string]bool{}}
	for _, e := range rl.Extensions {
		if e.Id.Equal(oidIssuingDistributionPoint) {
			if c.scope, err = parseIDP(e.Value); err != nil {
				return nil, err
			}
			continue
		}
		if e.Critical {
			return nil, errors.New("crl: critical extension " + e.Id.String())
		}
	}
	for _, entry := range rl.RevokedCertificateEntries {
		for _, e := range entry.Extensions {
			if e.Critical {
				return nil, errors.New("crl: critical entry extension " + e.Id.String())
			}
		}
		c.revoked[entry.SerialNumber.String()] = true
	}
	return c, nil
}

// parseIDP reads an issuing distribution point. A list scoped to other
// issuers' certificates (indirect), to attribute certificates, or to a
// distribution point named relative to the issuer is refused: this reader
// does not implement what those mean.
func parseIDP(der []byte) (*crlScope, error) {
	malformed := errors.New("crl: issuing distribution point malformed")
	var seq asn1.RawValue
	if rest, err := asn1.Unmarshal(der, &seq); err != nil || len(rest) != 0 ||
		seq.Class != asn1.ClassUniversal || seq.Tag != asn1.TagSequence || !seq.IsCompound {
		return nil, malformed
	}
	if len(seq.Bytes) == 0 {
		// RFC 5280 § 5.2.5: a conforming issuer never issues an empty one.
		return nil, errors.New("crl: issuing distribution point empty")
	}
	s := &crlScope{}
	last := -1
	for rest := seq.Bytes; len(rest) > 0; {
		var f asn1.RawValue
		var err error
		if rest, err = asn1.Unmarshal(rest, &f); err != nil || f.Class != asn1.ClassContextSpecific || f.Tag <= last {
			return nil, malformed
		}
		last = f.Tag
		flag := func() (bool, error) {
			if f.IsCompound || len(f.Bytes) != 1 {
				return false, malformed
			}
			return f.Bytes[0] != 0, nil
		}
		switch f.Tag {
		case 0: // distributionPoint: a CHOICE, so explicitly tagged
			var name asn1.RawValue
			if r, err := asn1.Unmarshal(f.Bytes, &name); err != nil || len(r) != 0 || !f.IsCompound || name.Class != asn1.ClassContextSpecific {
				return nil, malformed
			}
			if name.Tag == 1 {
				return nil, errors.New("crl: issuing distribution point named relative to the issuer")
			}
			if name.Tag != 0 || !name.IsCompound {
				return nil, malformed
			}
			s.named = true
			for g := name.Bytes; len(g) > 0; {
				var gn asn1.RawValue
				if g, err = asn1.Unmarshal(g, &gn); err != nil {
					return nil, malformed
				}
				if gn.Class == asn1.ClassContextSpecific && gn.Tag == 6 && !gn.IsCompound {
					s.uris = append(s.uris, string(gn.Bytes))
				}
			}
		case 1:
			if s.userOnly, err = flag(); err != nil {
				return nil, err
			}
		case 2:
			if s.caOnly, err = flag(); err != nil {
				return nil, err
			}
		case 3:
			if f.IsCompound || len(f.Bytes) == 0 {
				return nil, malformed
			}
			s.someReasons = true
		case 4:
			if indirect, err := flag(); err != nil {
				return nil, err
			} else if indirect {
				return nil, errors.New("crl: indirect")
			}
		case 5:
			if attr, err := flag(); err != nil {
				return nil, err
			} else if attr {
				return nil, errors.New("crl: attribute certificates only")
			}
		default:
			return nil, malformed
		}
	}
	return s, nil
}

// Status is the list's statement about a certificate its issuer signed. A
// certificate outside the list's scope is unknown: the list says nothing
// about it (RFC 5280 § 6.3.3).
func (c *CRL) Status(cert *x509.Certificate) RevocationStatus {
	if s := c.scope; s != nil {
		if s.named && !slices.ContainsFunc(cert.CRLDistributionPoints, func(u string) bool { return slices.Contains(s.uris, u) }) {
			return StatusUnknown
		}
		if (s.userOnly && cert.IsCA) || (s.caOnly && !cert.IsCA) {
			return StatusUnknown
		}
	}
	if c.revoked[cert.SerialNumber.String()] {
		return StatusRevoked
	}
	if c.scope != nil && c.scope.someReasons {
		return StatusUnknown
	}
	return StatusGood
}
