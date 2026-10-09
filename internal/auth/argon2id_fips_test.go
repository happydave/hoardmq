//go:build fips

package auth

import (
	"errors"
	"strings"
	"testing"
)

// TestArgon2idRefusedInTheFIPSBuild: this build reads and writes no Argon2id
// form, and says why.
func TestArgon2idRefusedInTheFIPSBuild(t *testing.T) {
	phc := "$argon2id$v=19$m=64,t=1,p=1$c29tZXNhbHQ$AAAAAAAAAAAAAAAAAAAAAA"
	if err := CheckPHC(phc); !errors.Is(err, ErrArgon2idUnavailable) {
		t.Errorf("CheckPHC: %v", err)
	}
	if ok, err := VerifyPassword(phc, []byte("x")); ok || !errors.Is(err, ErrArgon2idUnavailable) {
		t.Errorf("VerifyPassword: %v %v", ok, err)
	}
	if _, err := HashArgon2id([]byte("x"), DefaultArgon2idParams); !errors.Is(err, ErrArgon2idUnavailable) {
		t.Errorf("HashArgon2id: %v", err)
	}
	if Argon2idAvailable {
		t.Error("Argon2idAvailable in the fips build")
	}
}

// A users file holding an Argon2id hash is refused at load in this build,
// naming the user and why.
func TestUsersFileArgon2idRefusedInTheFIPSBuild(t *testing.T) {
	_, err := parseUsers([]byte(`{"users":[{"username":"u","password":"$argon2id$v=19$m=64,t=1,p=1$c29tZXNhbHQ$AAAAAAAAAAAAAAAAAAAAAA"}]}`))
	if !errors.Is(err, ErrArgon2idUnavailable) || !strings.Contains(err.Error(), `"u"`) {
		t.Fatalf("parseUsers: %v, want the user named and ErrArgon2idUnavailable", err)
	}
}
