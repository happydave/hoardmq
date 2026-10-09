package auth

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func dbLookup(t *testing.T, open bool, users map[string]StoredCredential) func(string) (json.RawMessage, bool, bool) {
	t.Helper()
	return func(u string) (json.RawMessage, bool, bool) {
		c, ok := users[u]
		if !ok {
			return nil, false, open
		}
		b, err := json.Marshal(c)
		if err != nil {
			t.Fatal(err)
		}
		return b, true, open
	}
}

func TestDBAuthenticator(t *testing.T) {
	alicePHC, err := hashPassword([]byte("s3cret"), 1000)
	if err != nil {
		t.Fatal(err)
	}
	carol, err := DeriveSCRAM([]byte("pencil"), []byte("0123456789abcdef"), MinSCRAMIterations)
	if err != nil {
		t.Fatal(err)
	}
	users := map[string]StoredCredential{
		"alice": {Password: alicePHC, Attributes: map[string]string{"team": "blue"}},
		"carol": {Password: carol.String()},
		"eve":   {Password: "not-a-hash"},
	}
	a := &DBAuthenticator{Lookup: dbLookup(t, true, users)}
	ctx := context.Background()
	if r := a.Authenticate(ctx, ldapReq("alice", "s3cret")); r.Decision != Allow || r.Attributes["team"] != "blue" {
		t.Fatalf("alice: %+v", r)
	}
	if r := a.Authenticate(ctx, ldapReq("alice", "wrong")); r.Decision != Deny || r.Reason != "db: wrong password" {
		t.Fatalf("alice, wrong: %+v", r)
	}
	if r := a.Authenticate(ctx, &Request{Username: "alice", HasUsername: true}); r.Decision != Deny {
		t.Fatalf("alice, no password: %+v", r)
	}
	if r := a.Authenticate(ctx, ldapReq("bob", "s3cret")); r.Decision != Pass {
		t.Fatalf("bob: %+v", r)
	}
	if r := a.Authenticate(ctx, &Request{Password: []byte("s3cret"), HasPassword: true}); r.Decision != Pass {
		t.Fatalf("no username: %+v", r)
	}
	if r := a.Authenticate(ctx, ldapReq("carol", "pencil")); r.Decision != Allow {
		t.Fatalf("carol, SCRAM form: %+v", r)
	}
	if r := a.Authenticate(ctx, ldapReq("eve", "x")); r.Decision != Deny || r.Reason != "db: stored credential unreadable" {
		t.Fatalf("eve: %+v", r)
	}
	closed := &DBAuthenticator{Lookup: dbLookup(t, false, users)}
	if r := closed.Authenticate(ctx, ldapReq("alice", "s3cret")); r.Decision != Pass {
		t.Fatalf("a database not open yet: %+v", r)
	}
	garbled := &DBAuthenticator{Lookup: func(string) (json.RawMessage, bool, bool) { return json.RawMessage(`[1]`), true, true }}
	if r := garbled.Authenticate(ctx, ldapReq("alice", "s3cret")); r.Decision != Deny {
		t.Fatalf("a garbled record: %+v", r)
	}
}

func TestCheckCredential(t *testing.T) {
	good, _ := hashPassword([]byte("x"), 1000)
	if err := CheckCredential("alice", StoredCredential{Password: good, Attributes: map[string]string{"k": "v"}}); err != nil {
		t.Fatal(err)
	}
	many := map[string]string{}
	for i := range 33 {
		many[strings.Repeat("k", i+1)] = "v"
	}
	for name, c := range map[string]struct {
		user string
		cred StoredCredential
	}{
		"empty username":      {"", StoredCredential{Password: good}},
		"257-byte username":   {strings.Repeat("u", 257), StoredCredential{Password: good}},
		"not UTF-8":           {"\xff", StoredCredential{Password: good}},
		"plain text password": {"alice", StoredCredential{Password: "hunter2"}},
		"33 attributes":       {"alice", StoredCredential{Password: good, Attributes: many}},
		"empty attribute":     {"alice", StoredCredential{Password: good, Attributes: map[string]string{"": "v"}}},
		"long attribute":      {"alice", StoredCredential{Password: good, Attributes: map[string]string{"k": strings.Repeat("v", 257)}}},
	} {
		if err := CheckCredential(c.user, c.cred); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	if err := CheckCredential(strings.Repeat("u", 256), StoredCredential{Password: good}); err != nil {
		t.Fatalf("256-byte username: %v", err)
	}
	if CredentialForm(good) != "pbkdf2-sha256" {
		t.Fatal("pbkdf2 form")
	}
	sc, _ := HashSCRAM([]byte("x"), MinSCRAMIterations)
	if CredentialForm(sc) != "scram-sha-256" {
		t.Fatal("scram form")
	}
}
