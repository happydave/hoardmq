package jose

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
)

// Key is one public key from an issuer's JWKS.
type Key struct {
	ID string
	// Alg and Use are the key's own members, empty when absent.
	Alg    string
	Use    string
	Public crypto.PublicKey
}

// KeySet is an issuer's signing keys by key identifier.
type KeySet map[string]Key

// ParseKeySet reads a JWKS document. Keys of a type this package cannot
// verify with, or without a key identifier, are left out; a key that claims
// a supported type but does not decode is an error.
func ParseKeySet(data []byte) (KeySet, error) {
	var doc struct {
		Keys []map[string]any `json:"keys"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("jwks: %w", err)
	}
	if doc.Keys == nil {
		return nil, errors.New("jwks: no keys member")
	}
	out := KeySet{}
	for _, m := range doc.Keys {
		str := func(name string) string { s, _ := m[name].(string); return s }
		kid := str("kid")
		if kid == "" {
			continue
		}
		var pub crypto.PublicKey
		var err error
		switch str("kty") {
		case "RSA":
			pub, err = rsaKey(str("n"), str("e"))
		case "EC":
			pub, err = ecKey(str("crv"), str("x"), str("y"))
		case "OKP":
			if str("crv") != "Ed25519" {
				continue
			}
			var x []byte
			x, err = segment(str("x"))
			if err == nil && len(x) != ed25519.PublicKeySize {
				err = errors.New("Ed25519 key of the wrong size")
			}
			pub = ed25519.PublicKey(x)
		default:
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("jwks: key %q: %w", kid, err)
		}
		out[kid] = Key{ID: kid, Alg: str("alg"), Use: str("use"), Public: pub}
	}
	return out, nil
}

func rsaKey(n, e string) (crypto.PublicKey, error) {
	nb, err := segment(n)
	if err != nil || len(nb) == 0 {
		return nil, errors.New("bad modulus")
	}
	eb, err := segment(e)
	if err != nil || len(eb) == 0 || len(eb) > 4 {
		return nil, errors.New("bad exponent")
	}
	exp := int(new(big.Int).SetBytes(eb).Int64())
	if exp < 3 {
		return nil, errors.New("bad exponent")
	}
	return &rsa.PublicKey{N: new(big.Int).SetBytes(nb), E: exp}, nil
}

func ecKey(crv, x, y string) (crypto.PublicKey, error) {
	var curve elliptic.Curve
	switch crv {
	case "P-256":
		curve = elliptic.P256()
	case "P-384":
		curve = elliptic.P384()
	case "P-521":
		curve = elliptic.P521()
	default:
		return nil, fmt.Errorf("curve %q", crv)
	}
	xb, err1 := segment(x)
	yb, err2 := segment(y)
	if err1 != nil || err2 != nil {
		return nil, errors.New("bad coordinates")
	}
	size := (curve.Params().BitSize + 7) / 8
	if len(xb) != size || len(yb) != size {
		return nil, errors.New("coordinates of the wrong size")
	}
	// The uncompressed point, 0x04 || X || Y; a point off the curve is refused.
	point := append(append([]byte{4}, xb...), yb...)
	pub, err := ecdsa.ParseUncompressedPublicKey(curve, point)
	if err != nil {
		return nil, err
	}
	return pub, nil
}
