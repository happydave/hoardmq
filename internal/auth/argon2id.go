//go:build !fips

package auth

import (
	"crypto/hmac"
	"crypto/rand"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/happydave/hoardmq/internal/argon2"
)

// The Argon2id form is the PHC string
// $argon2id$v=19$m=<KiB>,t=<passes>,p=<lanes>$<salt>$<hash>, the salt and hash
// in unpadded standard base64 as the other forms.
const argon2idID = "argon2id"

// Argon2idParams are an Argon2id hash's costs.
type Argon2idParams struct {
	MemoryKiB uint32
	Time      uint32
	Lanes     uint8
}

// DefaultArgon2idParams is RFC 9106 § 4's second recommended option: 64 MiB,
// three passes, four lanes.
var DefaultArgon2idParams = Argon2idParams{MemoryKiB: 64 * 1024, Time: 3, Lanes: 4}

// Bounds on a stored Argon2id hash, checked before anything is derived: a
// string from a users file or the credential API decides how much memory
// and time a login spends.
const (
	argon2MaxMemoryKiB = 1 << 20 // 1 GiB
	argon2MaxTime      = 16
	argon2MaxLanes     = 16
	argon2MinSalt      = 8
	argon2MinHash      = 16
	argon2MaxHash      = 64
	argon2HashLen      = 32
)

// Argon2idAvailable is whether this build accepts the Argon2id form.
const Argon2idAvailable = true

// argon2Derivations counts derivations begun; argon2Reserved, when set, is
// called once a verification holds its memory and before it derives. Both
// are for tests.
var (
	argon2Derivations atomic.Int64
	argon2Reserved    func()
)

// Check is the bound a stored hash's costs meet.
func (p Argon2idParams) Check() error {
	if p.Lanes < 1 || p.Lanes > argon2MaxLanes || p.MemoryKiB < 8*uint32(p.Lanes) || p.MemoryKiB > argon2MaxMemoryKiB ||
		p.Time < 1 || p.Time > argon2MaxTime {
		return fmt.Errorf("argon2id: memory must be 8×lanes to %d KiB, time 1 to %d, lanes 1 to %d",
			argon2MaxMemoryKiB, argon2MaxTime, argon2MaxLanes)
	}
	return nil
}

type argon2idHash struct {
	params     Argon2idParams
	salt, hash []byte
}

func isArgon2id(phc string) bool { return strings.HasPrefix(phc, "$"+argon2idID+"$") }

// parseArgon2id reads and bounds the form; anything else is ErrNotPHC.
func parseArgon2id(phc string) (argon2idHash, error) {
	parts := strings.Split(phc, "$")
	if len(parts) != 6 || parts[0] != "" || parts[1] != argon2idID || parts[2] != "v=19" {
		return argon2idHash{}, ErrNotPHC
	}
	kv := strings.Split(parts[3], ",")
	if len(kv) != 3 || !strings.HasPrefix(kv[0], "m=") || !strings.HasPrefix(kv[1], "t=") || !strings.HasPrefix(kv[2], "p=") {
		return argon2idHash{}, ErrNotPHC
	}
	m, err1 := strconv.ParseUint(kv[0][2:], 10, 32)
	t, err2 := strconv.ParseUint(kv[1][2:], 10, 32)
	p, err3 := strconv.ParseUint(kv[2][2:], 10, 8)
	if err1 != nil || err2 != nil || err3 != nil ||
		p < 1 || p > argon2MaxLanes || m < 8*p || m > argon2MaxMemoryKiB || t < 1 || t > argon2MaxTime {
		return argon2idHash{}, ErrNotPHC
	}
	salt, err := b64.DecodeString(parts[4])
	if err != nil || len(salt) < argon2MinSalt {
		return argon2idHash{}, ErrNotPHC
	}
	hash, err := b64.DecodeString(parts[5])
	if err != nil || len(hash) < argon2MinHash || len(hash) > argon2MaxHash {
		return argon2idHash{}, ErrNotPHC
	}
	return argon2idHash{params: Argon2idParams{MemoryKiB: uint32(m), Time: uint32(t), Lanes: uint8(p)}, salt: salt, hash: hash}, nil
}

func checkArgon2id(phc string) error {
	_, err := parseArgon2id(phc)
	return err
}

// verifyArgon2id derives within the node's memory budget and compares in
// constant time.
func verifyArgon2id(phc string, password []byte) (bool, error) {
	h, err := parseArgon2id(phc)
	if err != nil {
		return false, err
	}
	cost := int64(h.params.MemoryKiB) * 1024
	if err := argon2Budget.reserve(cost); err != nil {
		return false, err
	}
	defer argon2Budget.release(cost)
	if argon2Reserved != nil {
		argon2Reserved()
	}
	argon2Derivations.Add(1)
	got := argon2.IDKey(password, h.salt, h.params.Time, h.params.MemoryKiB, h.params.Lanes, uint32(len(h.hash)))
	return hmac.Equal(got, h.hash), nil
}

// HashArgon2id derives the Argon2id form for the password with a fresh
// random salt, within the bounds a stored hash must meet.
func HashArgon2id(password []byte, p Argon2idParams) (string, error) {
	if err := p.Check(); err != nil {
		return "", err
	}
	salt := make([]byte, saltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("salt: %w", err)
	}
	key := argon2.IDKey(password, salt, p.Time, p.MemoryKiB, p.Lanes, argon2HashLen)
	return fmt.Sprintf("$%s$v=%d$m=%d,t=%d,p=%d$%s$%s", argon2idID, argon2.Version, p.MemoryKiB, p.Time, p.Lanes,
		b64.EncodeToString(salt), b64.EncodeToString(key)), nil
}
