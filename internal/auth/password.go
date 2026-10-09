package auth

import (
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Password hashes are PHC strings: $pbkdf2-sha256$i=<iterations>$<salt>$<hash>
// with base64 (no padding) for the salt and the hash. PBKDF2-HMAC-SHA256 is
// the first supported algorithm because it is in the standard library and is
// FIPS-approved; a second prefix can be added without changing the file.
const (
	phcID             = "pbkdf2-sha256"
	defaultIterations = 600000
	saltLen           = 16
	keyLen            = 32
)

var b64 = base64.RawStdEncoding

// HashPassword derives a PHC string for the password with a fresh random salt.
func HashPassword(password []byte) (string, error) {
	return hashPassword(password, defaultIterations)
}

func hashPassword(password []byte, iterations int) (string, error) {
	salt := make([]byte, saltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("salt: %w", err)
	}
	key, err := pbkdf2.Key(sha256.New, string(password), salt, iterations, keyLen)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("$%s$i=%d$%s$%s", phcID, iterations, b64.EncodeToString(salt), b64.EncodeToString(key)), nil
}

// ErrNotPHC is returned for a stored password that is not a supported PHC
// string, such as a plain-text password.
var ErrNotPHC = errors.New("password is not a supported PHC hash string")

// ErrArgon2idUnavailable is an Argon2id hash in a build without Argon2id,
// where the form is not a supported one: it is ErrNotPHC as well.
var ErrArgon2idUnavailable = fmt.Errorf("%w: argon2id is not in this build (built with the fips tag)", ErrNotPHC)

// VerifyPassword reports whether password matches the PHC string (PBKDF2 or
// Argon2id) or the SCRAM form. A malformed or unsupported string returns
// ErrNotPHC.
func VerifyPassword(phc string, password []byte) (bool, error) {
	if isArgon2id(phc) {
		return verifyArgon2id(phc, password)
	}
	if strings.HasPrefix(phc, "$"+scramID+"$") {
		c, err := ParseSCRAM(phc)
		if err != nil {
			return false, ErrNotPHC
		}
		return c.MatchesPassword(password)
	}
	parts := strings.Split(phc, "$")
	if len(parts) != 5 || parts[0] != "" || parts[1] != phcID || !strings.HasPrefix(parts[2], "i=") {
		return false, ErrNotPHC
	}
	iterations, err := strconv.Atoi(parts[2][2:])
	if err != nil || iterations < 1 {
		return false, ErrNotPHC
	}
	salt, err := b64.DecodeString(parts[3])
	if err != nil {
		return false, ErrNotPHC
	}
	want, err := b64.DecodeString(parts[4])
	if err != nil || len(want) == 0 {
		return false, ErrNotPHC
	}
	got, err := pbkdf2.Key(sha256.New, string(password), salt, iterations, len(want))
	if err != nil {
		return false, err
	}
	return hmac.Equal(got, want), nil
}

// CheckPHC reports whether the string is a well-formed supported PHC string,
// without deriving anything.
func CheckPHC(phc string) error {
	if isArgon2id(phc) {
		return checkArgon2id(phc)
	}
	if strings.HasPrefix(phc, "$"+scramID+"$") {
		_, err := ParseSCRAM(phc)
		return err
	}
	parts := strings.Split(phc, "$")
	if len(parts) != 5 || parts[0] != "" || parts[1] != phcID || !strings.HasPrefix(parts[2], "i=") {
		return ErrNotPHC
	}
	if n, err := strconv.Atoi(parts[2][2:]); err != nil || n < 1 {
		return ErrNotPHC
	}
	if _, err := b64.DecodeString(parts[3]); err != nil {
		return ErrNotPHC
	}
	if h, err := b64.DecodeString(parts[4]); err != nil || len(h) == 0 {
		return ErrNotPHC
	}
	return nil
}

// The SCRAM form stores what RFC 5802 § 3 says a server stores:
// $scram-sha-256$i=<iterations>$<salt>$<StoredKey>$<ServerKey>, each value in
// the PHC form's base64. It serves password login too: the password's
// SaltedPassword yields the StoredKey to compare. The pbkdf2-sha256 form's
// hash is the SaltedPassword itself, from which a stolen file would give a
// SCRAM login without cracking anything, so SCRAM does not accept it.
const (
	scramID = "scram-sha-256"
	// MinSCRAMIterations is RFC 7677 § 4's floor.
	MinSCRAMIterations = 4096
	// DefaultSCRAMIterations matches the pbkdf2-sha256 default.
	DefaultSCRAMIterations = defaultIterations
)

// SCRAMCredential is a user's SCRAM-SHA-256 authentication information.
type SCRAMCredential struct {
	Salt       []byte
	Iterations int
	StoredKey  []byte
	ServerKey  []byte
}

// ErrSCRAMIterations is a SCRAM form below RFC 7677's iteration floor.
var ErrSCRAMIterations = fmt.Errorf("a SCRAM credential needs at least %d iterations", MinSCRAMIterations)

// HashSCRAM derives the SCRAM form for the password with a fresh random salt.
func HashSCRAM(password []byte, iterations int) (string, error) {
	if iterations < MinSCRAMIterations {
		return "", ErrSCRAMIterations
	}
	salt := make([]byte, saltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("salt: %w", err)
	}
	c, err := DeriveSCRAM(password, salt, iterations)
	if err != nil {
		return "", err
	}
	return c.String(), nil
}

// DeriveSCRAM computes the credential for a password, salt and iteration
// count (RFC 5802 § 3).
func DeriveSCRAM(password, salt []byte, iterations int) (SCRAMCredential, error) {
	salted, err := pbkdf2.Key(sha256.New, string(password), salt, iterations, sha256.Size)
	if err != nil {
		return SCRAMCredential{}, err
	}
	clientKey := hmacSHA256(salted, []byte("Client Key"))
	stored := sha256.Sum256(clientKey)
	return SCRAMCredential{Salt: salt, Iterations: iterations, StoredKey: stored[:], ServerKey: hmacSHA256(salted, []byte("Server Key"))}, nil
}

func hmacSHA256(key, data []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write(data)
	return m.Sum(nil)
}

// String is the stored form.
func (c SCRAMCredential) String() string {
	return fmt.Sprintf("$%s$i=%d$%s$%s$%s", scramID, c.Iterations, b64.EncodeToString(c.Salt), b64.EncodeToString(c.StoredKey), b64.EncodeToString(c.ServerKey))
}

// ParseSCRAM reads the stored form.
func ParseSCRAM(s string) (SCRAMCredential, error) {
	parts := strings.Split(s, "$")
	if len(parts) != 6 || parts[0] != "" || parts[1] != scramID || !strings.HasPrefix(parts[2], "i=") {
		return SCRAMCredential{}, ErrNotPHC
	}
	n, err := strconv.Atoi(parts[2][2:])
	if err != nil {
		return SCRAMCredential{}, ErrNotPHC
	}
	if n < MinSCRAMIterations {
		return SCRAMCredential{}, ErrSCRAMIterations
	}
	salt, err1 := b64.DecodeString(parts[3])
	stored, err2 := b64.DecodeString(parts[4])
	server, err3 := b64.DecodeString(parts[5])
	if err1 != nil || err2 != nil || err3 != nil || len(salt) == 0 || len(stored) != sha256.Size || len(server) != sha256.Size {
		return SCRAMCredential{}, ErrNotPHC
	}
	return SCRAMCredential{Salt: salt, Iterations: n, StoredKey: stored, ServerKey: server}, nil
}

// MatchesPassword is the decision on a password against the credential: the
// StoredKey its SaltedPassword yields, compared in constant time.
func (c SCRAMCredential) MatchesPassword(password []byte) (bool, error) {
	d, err := DeriveSCRAM(password, c.Salt, c.Iterations)
	if err != nil {
		return false, err
	}
	return hmac.Equal(d.StoredKey, c.StoredKey), nil
}
