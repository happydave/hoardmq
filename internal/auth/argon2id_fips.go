//go:build fips

package auth

import "strings"

// This build carries no cryptography outside the standard library, so the
// Argon2id form is refused wherever a hash is read or written.

const argon2idID = "argon2id"

// Argon2idParams are an Argon2id hash's costs.
type Argon2idParams struct {
	MemoryKiB uint32
	Time      uint32
	Lanes     uint8
}

// DefaultArgon2idParams is RFC 9106 § 4's second recommended option.
var DefaultArgon2idParams = Argon2idParams{MemoryKiB: 64 * 1024, Time: 3, Lanes: 4}

// Argon2idAvailable is whether this build accepts the Argon2id form.
const Argon2idAvailable = false

func isArgon2id(phc string) bool { return strings.HasPrefix(phc, "$"+argon2idID+"$") }

func checkArgon2id(string) error { return ErrArgon2idUnavailable }

func verifyArgon2id(string, []byte) (bool, error) { return false, ErrArgon2idUnavailable }

// Check is unavailable in this build.
func (Argon2idParams) Check() error { return ErrArgon2idUnavailable }

// HashArgon2id is unavailable in this build.
func HashArgon2id([]byte, Argon2idParams) (string, error) { return "", ErrArgon2idUnavailable }
