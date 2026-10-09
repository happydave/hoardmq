//go:build fips

package cli

import (
	"strings"
	"testing"
)

// In the fips build hash-password -argon2id says the form is not in this
// build; the default still writes PBKDF2.
func TestHashPasswordArgon2idUnavailable(t *testing.T) {
	code, _, errOut := runWithStdin(t, "s3cret\n", nil, "hash-password", "-argon2id")
	if code != ExitProblems || !strings.Contains(errOut, "not in this build") {
		t.Fatalf("exit %d err %q", code, errOut)
	}
	if code, out, _ := runWithStdin(t, "s3cret\n", nil, "hash-password"); code != ExitOK || !strings.HasPrefix(out, "$pbkdf2-sha256$") {
		t.Errorf("the default form: %d %q", code, out)
	}
}
