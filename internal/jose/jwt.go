// Package jose verifies compact JWS tokens — OpenID Connect ID tokens and
// OAuth access tokens — against a JWKS, and holds the key set with its
// refetch rule. It uses only the standard library.
package jose

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
)

// MaxTokenBytes bounds a compact token before anything in it is decoded.
const MaxTokenBytes = 16 << 10

// Header is a token's JOSE header.
type Header struct {
	Alg string
	Kid string
	Typ string
	// Crit is set when the header names critical extensions, none of which
	// this package understands.
	Crit bool
}

// Claims are a token's payload members; numbers are json.Number.
type Claims map[string]any

// Parsed is a compact JWS split into its parts. Nothing in it is verified.
type Parsed struct {
	Header       Header
	Claims       Claims
	SigningInput []byte
	Signature    []byte
}

// errMalformed is every way a value fails to be a compact JWS.
var errMalformed = errors.New("not a compact JWS")

// Parse splits a compact JWS and decodes its header and payload.
func Parse(token string) (Parsed, error) {
	if len(token) > MaxTokenBytes {
		return Parsed{}, errors.New("token larger than 16 KiB")
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return Parsed{}, errMalformed
	}
	hb, err := segment(parts[0])
	if err != nil {
		return Parsed{}, errMalformed
	}
	var h map[string]any
	if err := strictJSON(hb, &h); err != nil {
		return Parsed{}, errMalformed
	}
	alg, _ := h["alg"].(string)
	if alg == "" {
		return Parsed{}, errMalformed
	}
	kid, _ := h["kid"].(string)
	typ, _ := h["typ"].(string)
	_, crit := h["crit"]
	pb, err := segment(parts[1])
	if err != nil {
		return Parsed{}, errMalformed
	}
	var c Claims
	if err := strictJSON(pb, &c); err != nil || c == nil {
		return Parsed{}, errMalformed
	}
	sig, err := segment(parts[2])
	if err != nil {
		return Parsed{}, errMalformed
	}
	return Parsed{Header: Header{Alg: alg, Kid: kid, Typ: typ, Crit: crit}, Claims: c,
		SigningInput: []byte(parts[0] + "." + parts[1]), Signature: sig}, nil
}

// segment decodes unpadded base64url, refusing padding and other alphabets.
func segment(s string) ([]byte, error) {
	return base64.RawURLEncoding.Strict().DecodeString(s)
}

// strictJSON decodes one JSON value with numbers kept as json.Number and
// nothing after it.
func strictJSON(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if dec.More() {
		return errMalformed
	}
	return nil
}

// String returns a claim's string value.
func (c Claims) String(name string) (string, bool) {
	s, ok := c[name].(string)
	return s, ok
}

// Time returns a numeric claim as seconds since the epoch, truncated.
func (c Claims) Time(name string) (int64, bool) {
	n, ok := c[name].(json.Number)
	if !ok {
		return 0, false
	}
	if i, err := n.Int64(); err == nil {
		return i, true
	}
	f, err := n.Float64()
	if err != nil {
		return 0, false
	}
	return int64(f), true
}

// Strings returns a claim that is a string or an array of strings; absent is
// no values, anything else is not ok.
func (c Claims) Strings(name string) ([]string, bool) {
	switch v := c[name].(type) {
	case nil:
		return nil, true
	case string:
		return []string{v}, true
	case []any:
		out := make([]string, 0, len(v))
		for _, x := range v {
			s, ok := x.(string)
			if !ok {
				return nil, false
			}
			out = append(out, s)
		}
		return out, true
	default:
		return nil, false
	}
}
