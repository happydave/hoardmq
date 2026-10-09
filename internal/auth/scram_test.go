package auth

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"
)

var t0ctx = context.Background()

// RFC 7677 § 3's example: user "user", password "pencil".
const (
	rfcSalt        = "W22ZaJ0SNY7soEsUEjb6gQ=="
	rfcClientFirst = "n,,n=user,r=rOprNGfwEbeRWgbNEkqO"
	rfcServerNonce = "%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0"
	rfcServerFirst = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"
	rfcClientFinal = "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
	rfcServerFinal = "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="
)

func rfcCredential(t *testing.T) SCRAMCredential {
	t.Helper()
	salt, _ := base64.StdEncoding.DecodeString(rfcSalt)
	c, err := DeriveSCRAM([]byte("pencil"), salt, 4096)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// scramWith is a server whose users are the map and whose nonce is RFC 7677's.
func scramWith(users map[string]SCRAMCredential) *SCRAMServer {
	s := NewSCRAMServer(func(u string) (SCRAMCredential, bool) { c, ok := users[u]; return c, ok })
	s.Nonce = func() string { return rfcServerNonce }
	return s
}

func run(s *SCRAMServer, first, final string) (Step, Step) {
	e := s.Begin(t0ctx, SCRAMMethod, &Request{})
	a := e.Step([]byte(first))
	if a.Kind != Continue {
		return a, Step{}
	}
	return a, e.Step([]byte(final))
}

func TestSCRAMRFC7677Example(t *testing.T) {
	s := scramWith(map[string]SCRAMCredential{"user": rfcCredential(t)})
	a, b := run(s, rfcClientFirst, rfcClientFinal)
	if a.Kind != Continue || string(a.Data) != rfcServerFirst {
		t.Fatalf("server-first %v %q\n want %q", a.Kind, a.Data, rfcServerFirst)
	}
	if b.Kind != Succeed || string(b.Data) != rfcServerFinal || b.Username != "user" {
		t.Fatalf("server-final %+v", b)
	}
}

func TestSCRAMRefusals(t *testing.T) {
	users := map[string]SCRAMCredential{"user": rfcCredential(t), "a,b": rfcCredential(t)}
	s := scramWith(users)
	lastChanged := rfcClientFinal[:len(rfcClientFinal)-2] + "A="
	for _, c := range []struct {
		name, first, final, reason string
	}{
		{"the proof's last character changed", rfcClientFirst, lastChanged, "invalid proof"},
		{"the combined nonce changed", rfcClientFirst, strings.Replace(rfcClientFinal, "k0,p=", "k1,p=", 1), "nonce mismatch"},
		{"c=eSws after an n,, header", rfcClientFirst, strings.Replace(rfcClientFinal, "c=biws", "c=eSws", 1), "channel binding does not match"},
		{"channel binding asked for", "p=tls-unique,,n=user,r=rOprNGfwEbeRWgbNEkqO", "", "channel binding is not supported"},
		{"an authorization identity", "n,a=admin,n=user,r=rOprNGfwEbeRWgbNEkqO", "", "authorization identity"},
		{"a mandatory extension", "n,,m=x,n=user,r=rOprNGfwEbeRWgbNEkqO", "", "malformed client-first"},
		{"a bad escape in the username", "n,,n=a=2Db,r=rOprNGfwEbeRWgbNEkqO", "", "malformed username"},
		{"a comma-free nonce", "n,,n=user,r=", "", "malformed nonce"},
	} {
		a, b := run(s, c.first, c.final)
		got := b
		if a.Kind == Fail {
			got = a
		}
		if got.Kind != Fail || !strings.Contains(got.Reason, c.reason) {
			t.Errorf("%s: %+v / %+v, want a refusal for %q", c.name, a, b, c.reason)
		}
		if a.Kind == Fail && a.Checked || b.Kind == Fail && !b.Checked {
			t.Errorf("%s: Checked says %v at the first step, %v at the final", c.name, a.Checked, b.Checked)
		}
	}
	// "y,," is accepted: the client could bind and believes the server cannot.
	yFinal := strings.Replace(rfcClientFinal, "c=biws", "c=eSws", 1)
	if a, _ := run(s, "y,,n=user,r=rOprNGfwEbeRWgbNEkqO", yFinal); a.Kind != Continue {
		t.Errorf("a y,, header: %+v", a)
	}
}

// "=2C" in a username is a comma.
func TestSCRAMUsernameEscape(t *testing.T) {
	cred := rfcCredential(t)
	s := scramWith(map[string]SCRAMCredential{"a,b": cred})
	e := s.Begin(t0ctx, SCRAMMethod, &Request{})
	a := e.Step([]byte("n,,n=a=2Cb,r=rOprNGfwEbeRWgbNEkqO"))
	if a.Kind != Continue || !strings.Contains(string(a.Data), "s="+rfcSalt) {
		t.Fatalf("the escaped user was not found: %+v", a)
	}
}

// An unknown user sees a stable salt and is refused at the final step; a user
// stored in the pbkdf2 form is unknown to SCRAM.
func TestSCRAMUnknownUser(t *testing.T) {
	phcUsers := write(t, "users.json", `{"users":[{"username":"old","password":"`+mustPBKDF2(t)+`"}]}`)
	f, err := LoadUsersFile(phcUsers)
	if err != nil {
		t.Fatal(err)
	}
	s := NewSCRAMServer(f.SCRAM)
	salt := func() string {
		a := s.Begin(t0ctx, SCRAMMethod, &Request{}).Step([]byte("n,,n=ghost,r=abc"))
		if a.Kind != Continue {
			t.Fatalf("an unknown user was refused at the first step: %+v", a)
		}
		for _, f := range strings.Split(string(a.Data), ",") {
			if strings.HasPrefix(f, "s=") {
				return f
			}
		}
		t.Fatal("no salt")
		return ""
	}
	if a, b := salt(), salt(); a != b {
		t.Fatalf("two exchanges for one unknown user: %s, %s", a, b)
	}
	for _, user := range []string{"ghost", "old"} {
		e := s.Begin(t0ctx, SCRAMMethod, &Request{})
		first := e.Step([]byte("n,,n=" + user + ",r=abc"))
		nonce := strings.TrimPrefix(strings.Split(string(first.Data), ",")[0], "r=")
		final := e.Step([]byte("c=biws,r=" + nonce + ",p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="))
		if final.Kind != Fail || !final.Checked || !strings.Contains(final.Reason, "unknown user") {
			t.Errorf("%s: %+v", user, final)
		}
	}
}

func mustPBKDF2(t *testing.T) string {
	t.Helper()
	phc, err := hashPassword([]byte("pencil"), 1000)
	if err != nil {
		t.Fatal(err)
	}
	return phc
}

// The SCRAM form serves password login, and its iteration floor is enforced
// at load.
func TestSCRAMFormPasswordLogin(t *testing.T) {
	stored := rfcCredential(t).String()
	if ok, err := VerifyPassword(stored, []byte("pencil")); err != nil || !ok {
		t.Fatalf("pencil: %v %v", ok, err)
	}
	if ok, _ := VerifyPassword(stored, []byte("pencil!")); ok {
		t.Fatal("pencil! verified")
	}
	parsed, err := ParseSCRAM(stored)
	if err != nil || parsed.String() != stored {
		t.Fatalf("round trip: %v %q", err, parsed.String())
	}
	low := strings.Replace(stored, "i=4096", "i=4095", 1)
	if _, err := LoadUsersFile(write(t, "u.json", `{"users":[{"username":"u","password":"`+low+`"}]}`)); err == nil || !strings.Contains(err.Error(), "4096") {
		t.Fatalf("i=4095: %v", err)
	}
	if _, err := LoadUsersFile(write(t, "u2.json", `{"users":[{"username":"u","password":"`+stored+`"}]}`)); err != nil {
		t.Fatalf("i=4096: %v", err)
	}
	short := stored[:len(stored)-4]
	if CheckPHC(short) == nil {
		t.Fatal("a truncated ServerKey passed")
	}
	f, _ := LoadUsersFile(write(t, "u3.json", `{"users":[{"username":"u","password":"`+stored+`"}]}`))
	if r := f.Authenticate(t0ctx, &Request{HasUsername: true, Username: "u", HasPassword: true, Password: []byte("pencil")}); r.Decision != Allow {
		t.Fatalf("auth.file with the SCRAM form: %+v", r)
	}
	if _, err := HashSCRAM([]byte("x"), 4095); err == nil {
		t.Fatal("HashSCRAM below the floor")
	}
	h, err := HashSCRAM([]byte("x"), 4096)
	if ok, _ := VerifyPassword(h, []byte("x")); err != nil || !ok {
		t.Fatalf("HashSCRAM: %v %v", err, ok)
	}
}

// The final step refuses an unknown user even over a proof that verifies:
// the stand-in credential is not what refuses them.
func TestSCRAMFinalRefusesUnknownWhateverTheProof(t *testing.T) {
	first, err := parseClientFirst(rfcClientFirst)
	if err != nil {
		t.Fatal(err)
	}
	if _, reason := verifyClientFinal(first, rfcServerFirst, rfcCredential(t), true, rfcClientFinal); reason != "" {
		t.Fatalf("known: %s", reason)
	}
	if _, reason := verifyClientFinal(first, rfcServerFirst, rfcCredential(t), false, rfcClientFinal); reason != "unknown user" {
		t.Fatalf("unknown with a verifying proof: %q", reason)
	}
}
