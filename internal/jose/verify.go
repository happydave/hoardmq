package jose

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/sha512"
	"errors"
	"hash"
	"math/big"
	"slices"
	"strings"
	"time"
)

// Policy is what a token must satisfy beyond its signature. The zero
// switches are the ID-token rules: an access token is refused and azp, when
// present, must name an audience.
type Policy struct {
	Issuer    string
	Audiences []string
	// Skew is allowed on exp, iat and nbf; MaxLifetime bounds exp - iat.
	Skew        time.Duration
	MaxLifetime time.Duration
	// AcceptAccessTokens accepts typ at+jwt (RFC 9068).
	AcceptAccessTokens bool
	// IgnoreAZP skips the authorized-party check: an access token's azp
	// names the OAuth client, not the audience.
	IgnoreAZP bool
}

// ErrUnknownKey is a token naming a key the set does not hold: the caller may
// refetch the set once and verify again.
var ErrUnknownKey = errors.New("unknown signing key")

// minRSABits is the smallest RSA modulus accepted.
const minRSABits = 2048

// algorithm is a signature algorithm this package verifies.
type algorithm struct {
	hash  crypto.Hash
	kind  string // "RSA", "EC" or "OKP"
	curve elliptic.Curve
}

var algorithms = map[string]algorithm{
	"RS256": {crypto.SHA256, "RSA", nil},
	"RS384": {crypto.SHA384, "RSA", nil},
	"RS512": {crypto.SHA512, "RSA", nil},
	"ES256": {crypto.SHA256, "EC", elliptic.P256()},
	"ES384": {crypto.SHA384, "EC", elliptic.P384()},
	"ES512": {crypto.SHA512, "EC", elliptic.P521()},
	"EdDSA": {0, "OKP", nil},
}

// Verify is the decision on a token: the header's algorithm and key, the
// signature, then the issuer, audience and times against the policy, at now.
// It performs no effect; its error says which clause refused.
func Verify(token string, keys KeySet, p Policy, now time.Time) (Claims, error) {
	t, err := Parse(token)
	if err != nil {
		return nil, err
	}
	h := t.Header
	if h.Crit {
		return nil, errors.New("critical header extensions")
	}
	if typ := strings.ToLower(h.Typ); !p.AcceptAccessTokens && (typ == "at+jwt" || typ == "application/at+jwt") {
		return nil, errors.New("an access token, not an ID token")
	}
	alg, ok := algorithms[h.Alg]
	if !ok {
		return nil, errors.New("algorithm " + h.Alg + " not allowed")
	}
	if h.Kid == "" {
		return nil, errors.New("no key identifier")
	}
	key, ok := keys[h.Kid]
	if !ok {
		return nil, ErrUnknownKey
	}
	if err := keyFits(h.Alg, alg, key); err != nil {
		return nil, err
	}
	if !verifySignature(alg, key.Public, t.SigningInput, t.Signature) {
		return nil, errors.New("bad signature")
	}
	if err := checkClaims(t.Claims, p, now); err != nil {
		return nil, err
	}
	return t.Claims, nil
}

// keyFits refuses a key whose type, curve or size does not fit the header's
// algorithm, or whose own alg or use says otherwise.
func keyFits(name string, alg algorithm, k Key) error {
	if k.Alg != "" && k.Alg != name {
		return errors.New("the key is for " + k.Alg + ", not " + name)
	}
	if k.Use != "" && k.Use != "sig" {
		return errors.New("the key is not a signing key")
	}
	switch pub := k.Public.(type) {
	case *rsa.PublicKey:
		if alg.kind != "RSA" {
			return errors.New("an RSA key for " + name)
		}
		if pub.N.BitLen() < minRSABits {
			return errors.New("an RSA key under 2048 bits")
		}
	case *ecdsa.PublicKey:
		if alg.kind != "EC" || pub.Curve != alg.curve {
			return errors.New("an EC key on the wrong curve for " + name)
		}
	case ed25519.PublicKey:
		if alg.kind != "OKP" {
			return errors.New("an Ed25519 key for " + name)
		}
	default:
		return errors.New("an unsupported key")
	}
	return nil
}

func digest(h crypto.Hash, data []byte) []byte {
	var w hash.Hash
	switch h {
	case crypto.SHA256:
		w = sha256.New()
	case crypto.SHA384:
		w = sha512.New384()
	default:
		w = sha512.New()
	}
	w.Write(data)
	return w.Sum(nil)
}

// verifySignature checks sig over input with pub. An ECDSA signature is the
// fixed-length r || s of RFC 7518 § 3.4, not ASN.1.
func verifySignature(alg algorithm, pub crypto.PublicKey, input, sig []byte) bool {
	switch alg.kind {
	case "RSA":
		k, ok := pub.(*rsa.PublicKey)
		return ok && rsa.VerifyPKCS1v15(k, alg.hash, digest(alg.hash, input), sig) == nil
	case "EC":
		k, ok := pub.(*ecdsa.PublicKey)
		if !ok {
			return false
		}
		size := (k.Curve.Params().BitSize + 7) / 8
		if len(sig) != 2*size {
			return false
		}
		r := new(big.Int).SetBytes(sig[:size])
		s := new(big.Int).SetBytes(sig[size:])
		return ecdsa.Verify(k, digest(alg.hash, input), r, s)
	case "OKP":
		k, ok := pub.(ed25519.PublicKey)
		return ok && ed25519.Verify(k, input, sig)
	}
	return false
}

// checkClaims is the policy over a verified token's payload.
func checkClaims(c Claims, p Policy, now time.Time) error {
	if iss, _ := c.String("iss"); iss != p.Issuer {
		return errors.New("issuer mismatch")
	}
	aud, ok := c.Strings("aud")
	if !ok || !slices.ContainsFunc(aud, func(a string) bool { return slices.Contains(p.Audiences, a) }) {
		return errors.New("audience mismatch")
	}
	if _, present := c["azp"]; present && !p.IgnoreAZP {
		if azp, _ := c.String("azp"); !slices.Contains(p.Audiences, azp) {
			return errors.New("authorized party mismatch")
		}
	}
	skew := int64(p.Skew / time.Second)
	n := now.Unix()
	exp, ok := c.Time("exp")
	if !ok {
		return errors.New("no expiry")
	}
	if n >= exp+skew {
		return errors.New("token expired")
	}
	iat, ok := c.Time("iat")
	if !ok {
		return errors.New("no issue time")
	}
	if iat > n+skew {
		return errors.New("issued in the future")
	}
	if _, present := c["nbf"]; present {
		nbf, ok := c.Time("nbf")
		if !ok || nbf > n+skew {
			return errors.New("not yet valid")
		}
	}
	if exp-iat > int64(p.MaxLifetime/time.Second) {
		return errors.New("lifetime longer than allowed")
	}
	return nil
}
