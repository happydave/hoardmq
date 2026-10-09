//go:build !fips

package auth

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// cheap is a cost that derives in milliseconds.
var cheap = Argon2idParams{MemoryKiB: 64, Time: 1, Lanes: 1}

// TestArgon2idRoundTrip: a hash written here verifies with its password and
// no other, and passes CheckPHC.
func TestArgon2idRoundTrip(t *testing.T) {
	phc, err := HashArgon2id([]byte("correct horse"), cheap)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(phc, "$argon2id$v=19$m=64,t=1,p=1$") {
		t.Fatalf("form %q", phc)
	}
	if err := CheckPHC(phc); err != nil {
		t.Fatalf("CheckPHC: %v", err)
	}
	for pw, want := range map[string]bool{"correct horse": true, "correct horsf": false, "": false} {
		if ok, err := VerifyPassword(phc, []byte(pw)); err != nil || ok != want {
			t.Errorf("password %q: %v %v, want %v", pw, ok, err, want)
		}
	}
	if def, _ := HashPassword([]byte("x")); !strings.HasPrefix(def, "$pbkdf2-sha256$") {
		t.Errorf("the default hash is %q, want PBKDF2", def)
	}
}

// TestArgon2idReferenceStrings: PHC strings the reference implementation
// (golang.org/x/crypto v0.57.0, argon2.IDKey, computed outside this module)
// wrote for these passwords verify, and a wrong password does not. The first
// is this package's default cost.
func TestArgon2idReferenceStrings(t *testing.T) {
	for pw, phc := range map[string]string{
		"correct horse": "$argon2id$v=19$m=65536,t=3,p=4$MDEyMzQ1Njc4OWFiY2RlZg$L1b1NjppUoj/KIEORq1pof4waLxklM0mqKZzt9nCsKI",
		"hunter2":       "$argon2id$v=19$m=19456,t=2,p=1$c2FsdHNhbHRzYWx0c2FsdA$04jpQlFqpaJ6VZbUUk/zpWGISNgUVsjbydDAuyrAG+s",
		"pencil":        "$argon2id$v=19$m=8192,t=1,p=8$OGJ5dGVzYWw$dn4dYXKjU6WNMV8p+j5M1A",
		"~unicode é":    "$argon2id$v=19$m=1024,t=4,p=3$YSBsb25nZXIgc2FsdCBvZiAyNCBieXRl$Ox0Nk221BQ0vYi3R0dCi741izQ3U1aWqvunB2JfqhIsU3YtHLi3o5uiRbft59+BuMR5ioczUE9p8kmBepeB+MA",
	} {
		if ok, err := VerifyPassword(phc, []byte(pw)); err != nil || !ok {
			t.Errorf("%s: %v %v", phc, ok, err)
		}
		if ok, err := VerifyPassword(phc, []byte(pw+"!")); err != nil || ok {
			t.Errorf("%s with a wrong password: %v %v", phc, ok, err)
		}
	}
}

// TestArgon2idBounds: a string outside the bounds is refused by both
// functions before anything is derived, since the string decides the cost a
// login spends.
func TestArgon2idBounds(t *testing.T) {
	salt, hash := "c29tZXNhbHQ", "AAAAAAAAAAAAAAAAAAAAAA" // 8 and 16 bytes
	good := "$argon2id$v=19$m=64,t=1,p=1$" + salt + "$" + hash
	if err := CheckPHC(good); err != nil {
		t.Fatalf("precondition: the in-bounds string is refused: %v", err)
	}
	for name, phc := range map[string]string{
		"version 16":        "$argon2id$v=16$m=64,t=1,p=1$" + salt + "$" + hash,
		"no lanes":          "$argon2id$v=19$m=64,t=1,p=0$" + salt + "$" + hash,
		"17 lanes":          "$argon2id$v=19$m=1024,t=1,p=17$" + salt + "$" + hash,
		"memory under 8×p":  "$argon2id$v=19$m=15,t=1,p=2$" + salt + "$" + hash,
		"memory over 1 GiB": "$argon2id$v=19$m=1048577,t=1,p=1$" + salt + "$" + hash,
		"no passes":         "$argon2id$v=19$m=64,t=0,p=1$" + salt + "$" + hash,
		"17 passes":         "$argon2id$v=19$m=64,t=17,p=1$" + salt + "$" + hash,
		"short salt":        "$argon2id$v=19$m=64,t=1,p=1$c29tZQ$" + hash,
		"short hash":        "$argon2id$v=19$m=64,t=1,p=1$" + salt + "$AAAAAAAAAAA",
		"reordered":         "$argon2id$v=19$t=1,m=64,p=1$" + salt + "$" + hash,
		"repeated":          "$argon2id$v=19$m=64,m=64,p=1$" + salt + "$" + hash,
		"bad base64":        "$argon2id$v=19$m=64,t=1,p=1$***$" + hash,
		"missing part":      "$argon2id$v=19$m=64,t=1,p=1$" + salt,
	} {
		before := argon2Derivations.Load()
		if err := CheckPHC(phc); !errors.Is(err, ErrNotPHC) {
			t.Errorf("%s: CheckPHC %v, want ErrNotPHC", name, err)
		}
		if ok, err := VerifyPassword(phc, []byte("x")); ok || !errors.Is(err, ErrNotPHC) {
			t.Errorf("%s: VerifyPassword %v %v, want ErrNotPHC", name, ok, err)
		}
		if argon2Derivations.Load() != before {
			t.Errorf("%s: derived before refusing", name)
		}
	}
}

// TestArgon2idMemoryBudget: a verification waits for its share of the budget
// and derives only once it holds it; a cost over the whole budget is refused
// without deriving.
func TestArgon2idMemoryBudget(t *testing.T) {
	t.Cleanup(func() { SetArgon2MemoryBytes(DefaultArgon2MemoryBytes) })
	phc, err := HashArgon2id([]byte("pw"), cheap)
	if err != nil {
		t.Fatal(err)
	}
	cost := int64(cheap.MemoryKiB) * 1024
	SetArgon2MemoryBytes(2 * cost)

	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	var holding sync.WaitGroup
	holding.Add(2)
	argon2Reserved = func() {
		holding.Done()
		<-release
	}
	t.Cleanup(func() { argon2Reserved = nil })
	var done sync.WaitGroup
	for i := 0; i < 3; i++ {
		done.Add(1)
		go func() {
			defer done.Done()
			if ok, err := VerifyPassword(phc, []byte("pw")); !ok || err != nil {
				t.Errorf("verification: %v %v", ok, err)
			}
		}()
	}
	holding.Wait() // two hold the budget, held in the seam
	time.Sleep(50 * time.Millisecond)
	argon2Budget.mu.Lock()
	used := argon2Budget.used
	argon2Budget.mu.Unlock()
	if used != 2*cost {
		t.Fatalf("budget in use %d, want %d: the third verification did not wait", used, 2*cost)
	}
	holding.Add(1) // the third reaches the seam once one releases
	unblock()
	done.Wait()

	argon2Reserved = nil
	SetArgon2MemoryBytes(cost - 1)
	before := argon2Derivations.Load()
	refused := make(chan error, 1)
	go func() {
		_, err := VerifyPassword(phc, []byte("pw"))
		refused <- err
	}()
	select {
	case err := <-refused:
		if !errors.Is(err, ErrArgon2Budget) {
			t.Fatalf("a cost over the budget: %v, want ErrArgon2Budget", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a cost over the whole budget waited instead of being refused")
	}
	if argon2Derivations.Load() != before {
		t.Fatal("a cost over the budget was derived")
	}
}

// A hash over the budget is refused with that reason, not as a wrong
// password, by both password authenticators.
func TestArgon2idOverBudgetReason(t *testing.T) {
	t.Cleanup(func() { SetArgon2MemoryBytes(DefaultArgon2MemoryBytes) })
	phc, err := HashArgon2id([]byte("pw"), cheap)
	if err != nil {
		t.Fatal(err)
	}
	SetArgon2MemoryBytes(int64(cheap.MemoryKiB)*1024 - 1)
	req := &Request{HasUsername: true, Username: "u", Password: []byte("pw")}
	users, err := parseUsers([]byte(`{"users":[{"username":"u","password":"` + phc + `"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	file := &FileAuthenticator{users: users}
	cred, _ := json.Marshal(StoredCredential{Password: phc})
	db := &DBAuthenticator{Lookup: func(string) (json.RawMessage, bool, bool) { return cred, true, true }}
	for name, a := range map[string]Authenticator{"file": file, "db": db} {
		r := a.Authenticate(context.Background(), req)
		if r.Decision != Deny || !strings.Contains(r.Reason, "memory") {
			t.Errorf("%s: %v %q, want denied for the budget", name, r.Decision, r.Reason)
		}
	}
}
