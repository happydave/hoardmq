//go:build !fips

package cli

import (
	"strings"
	"testing"

	"github.com/happydave/hoardmq/internal/auth"
)

// hash-password -argon2id writes a hash that verifies; the default is still
// PBKDF2; the SCRAM and Argon2id forms are one choice; costs out of bounds
// are a usage error.
func TestHashPasswordArgon2id(t *testing.T) {
	code, out, errOut := runWithStdin(t, "s3cret\n", nil, "hash-password", "-argon2id", "-memory", "64", "-time", "1", "-parallelism", "1")
	if code != ExitOK || !strings.HasPrefix(out, "$argon2id$v=19$m=64,t=1,p=1$") {
		t.Fatalf("exit %d out %q err %q", code, out, errOut)
	}
	if ok, err := auth.VerifyPassword(strings.TrimSpace(out), []byte("s3cret")); err != nil || !ok {
		t.Fatalf("the hash does not verify: %v %v", ok, err)
	}
	if code, out, _ := runWithStdin(t, "s3cret\n", nil, "hash-password"); code != ExitOK || !strings.HasPrefix(out, "$pbkdf2-sha256$") {
		t.Errorf("the default form: %d %q", code, out)
	}
	if code, _, _ := runWithStdin(t, "s3cret\n", nil, "hash-password", "-argon2id", "-scram"); code != ExitUsage {
		t.Errorf("-argon2id with -scram: exit %d, want %d", code, ExitUsage)
	}
	if code, _, errOut := runWithStdin(t, "s3cret\n", nil, "hash-password", "-argon2id", "-memory", "4"); code != ExitUsage || !strings.Contains(errOut, "memory") {
		t.Errorf("-memory 4: exit %d %q, want %d", code, errOut, ExitUsage)
	}
}
