package auth

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPasswordRoundTrip(t *testing.T) {
	phc, err := hashPassword([]byte("s3cret"), 1000)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(phc, "$pbkdf2-sha256$i=1000$") {
		t.Fatalf("phc %q", phc)
	}
	if ok, err := VerifyPassword(phc, []byte("s3cret")); err != nil || !ok {
		t.Fatalf("verify: %v %v", ok, err)
	}
	if ok, _ := VerifyPassword(phc, []byte("wrong")); ok {
		t.Fatal("wrong password accepted")
	}
	for _, bad := range []string{"plain", "$argon2id$v=19$m=1$s$h", "$pbkdf2-sha256$i=0$AAAA$AAAA", "$pbkdf2-sha256$i=1$!!$AAAA", "$pbkdf2-sha256$i=1$AAAA$"} {
		if _, err := VerifyPassword(bad, []byte("x")); !errors.Is(err, ErrNotPHC) {
			t.Errorf("%q: err %v", bad, err)
		}
		if CheckPHC(bad) == nil {
			t.Errorf("%q passed CheckPHC", bad)
		}
	}
	if CheckPHC(phc) != nil {
		t.Error("good phc failed CheckPHC")
	}
	if _, err := HashPassword([]byte("x")); err != nil {
		t.Fatal(err)
	}
}

func write(t *testing.T, name, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestFileAuthenticator(t *testing.T) {
	phc, _ := hashPassword([]byte("pw"), 1000)
	path := write(t, "users.json", `{"users":[{"username":"alice","password":"`+phc+`","attributes":{"role":"admin"}}]}`)
	f, err := LoadUsersFile(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if r := f.Authenticate(ctx, &Request{HasUsername: true, Username: "alice", HasPassword: true, Password: []byte("pw")}); r.Decision != Allow || r.Attributes["role"] != "admin" {
		t.Errorf("good login: %+v", r)
	}
	if r := f.Authenticate(ctx, &Request{HasUsername: true, Username: "alice", HasPassword: true, Password: []byte("no")}); r.Decision != Deny {
		t.Errorf("wrong password: %+v", r)
	}
	if r := f.Authenticate(ctx, &Request{HasUsername: true, Username: "bob", HasPassword: true, Password: []byte("pw")}); r.Decision != Pass {
		t.Errorf("unknown user: %+v", r)
	}
	if r := f.Authenticate(ctx, &Request{}); r.Decision != Pass {
		t.Errorf("no username: %+v", r)
	}
	// Reload picks up a new user; a bad reload keeps the old table.
	phc2, _ := hashPassword([]byte("pw2"), 1000)
	if err := os.WriteFile(path, []byte(`{"users":[{"username":"bob","password":"`+phc2+`"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := f.Reload(); err != nil {
		t.Fatal(err)
	}
	if r := f.Authenticate(ctx, &Request{HasUsername: true, Username: "bob", HasPassword: true, Password: []byte("pw2")}); r.Decision != Allow {
		t.Errorf("after reload: %+v", r)
	}
	if err := os.WriteFile(path, []byte(`{"users":[{"username":"x","password":"plain"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := f.Reload(); err == nil || !strings.Contains(err.Error(), `"x"`) {
		t.Errorf("plain password reload: %v", err)
	}
	if r := f.Authenticate(ctx, &Request{HasUsername: true, Username: "bob", HasPassword: true, Password: []byte("pw2")}); r.Decision != Allow {
		t.Errorf("old table not kept: %+v", r)
	}
}

func TestUsersFileValidation(t *testing.T) {
	phc, _ := hashPassword([]byte("pw"), 1000)
	cases := map[string]string{
		"plain text":  `{"users":[{"username":"a","password":"secret"}]}`,
		"empty name":  `{"users":[{"username":"","password":"` + phc + `"}]}`,
		"duplicate":   `{"users":[{"username":"a","password":"` + phc + `"},{"username":"a","password":"` + phc + `"}]}`,
		"unknown key": `{"users":[{"username":"a","password":"` + phc + `","x":1}]}`,
		"bad json":    `{`,
	}
	for name, body := range cases {
		if _, err := LoadUsersFile(write(t, "u.json", body)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := LoadUsersFile(filepath.Join(t.TempDir(), "missing.json")); err == nil {
		t.Error("missing file accepted")
	}
}

func TestChains(t *testing.T) {
	ctx := context.Background()
	fixed := func(d Decision, attrs map[string]string) Authenticator {
		return authFunc(func(context.Context, *Request) Result { return Result{Decision: d, Attributes: attrs} })
	}
	if r := (AuthenticatorChain{}).Authenticate(ctx, &Request{}); r.Decision != Pass {
		t.Error("empty chain should pass")
	}
	chain := AuthenticatorChain{fixed(Pass, map[string]string{"a": "1"}), fixed(Allow, map[string]string{"b": "2"}), fixed(Deny, nil)}
	if r := chain.Authenticate(ctx, &Request{}); r.Decision != Allow || r.Attributes["a"] != "1" || r.Attributes["b"] != "2" {
		t.Errorf("chain: %+v", r)
	}
	az := AuthorizerChain{azFunc(func(context.Context, Action, string, *Identity) Decision { return Pass }), azFunc(func(context.Context, Action, string, *Identity) Decision { return Deny })}
	if az.Authorize(ctx, Publish, "t", &Identity{}) != Deny {
		t.Error("authorizer chain")
	}
	if (AuthorizerChain{}).Authorize(ctx, Publish, "t", &Identity{}) != Pass {
		t.Error("empty authorizer chain should pass")
	}
	if Allow.String() != "allow" || Deny.String() != "deny" || Pass.String() != "pass" || Unsubscribe.String() != "unsubscribe" {
		t.Error("names")
	}
	if !(&Request{CertCN: "x"}).HasCredentials() || (&Request{}).HasCredentials() {
		t.Error("HasCredentials")
	}
}

type authFunc func(context.Context, *Request) Result

func (f authFunc) Authenticate(ctx context.Context, r *Request) Result { return f(ctx, r) }

type azFunc func(context.Context, Action, string, *Identity) Decision

func (f azFunc) Authorize(ctx context.Context, a Action, t string, id *Identity) Decision {
	return f(ctx, a, t, id)
}

func TestFileAuthorizer(t *testing.T) {
	path := write(t, "acl.json", `{"rules":[
		{"principal": {"username": "admin"}, "topic": "#", "allow": ["publish", "subscribe", "unsubscribe"]},
		{"principal": {"username": "dev"}, "topic": "secret/#", "deny": ["subscribe"]},
		{"topic": "devices/${clientid}/#", "allow": ["publish", "subscribe", "unsubscribe"]},
		{"principal": {"cn": "gateway"}, "topic": "gw/${cn}/+", "allow": ["publish"]},
		{"topic": "public/+", "allow": ["subscribe"]}
	]}`)
	a, err := LoadACLFile(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	admin := &Identity{Username: "admin", ClientID: "c0"}
	dev := &Identity{Username: "dev", ClientID: "d1"}
	anon := &Identity{ClientID: "d2"}
	noid := &Identity{}
	gw := &Identity{CertCN: "gateway", ClientID: "g"}
	cases := []struct {
		id     *Identity
		action Action
		topic  string
		want   Decision
	}{
		{admin, Publish, "anything/at/all", Allow},
		{admin, Subscribe, "#", Allow},
		{dev, Subscribe, "secret/x", Deny},
		{dev, Subscribe, "secret/#", Deny},
		{dev, Publish, "secret/x", Pass},
		{dev, Publish, "devices/d1/temp", Allow},
		{dev, Subscribe, "devices/d1/#", Allow},
		{dev, Subscribe, "devices/d1/+", Allow},
		{dev, Subscribe, "devices/+/#", Pass},
		{dev, Subscribe, "devices/d2/temp", Pass},
		{anon, Publish, "devices/d2/x", Allow},
		{noid, Publish, "devices//x", Pass},
		{anon, Subscribe, "public/a", Allow},
		{anon, Subscribe, "public/+", Allow},
		{anon, Subscribe, "public/#", Pass},
		{anon, Subscribe, "public/a/b", Pass},
		{anon, Publish, "public/a", Pass},
		{gw, Publish, "gw/gateway/x", Allow},
		{gw, Publish, "gw/other/x", Pass},
		{&Identity{ClientID: "g"}, Publish, "gw/gateway/x", Pass},
	}
	for _, c := range cases {
		if got := a.Authorize(ctx, c.action, c.topic, c.id); got != c.want {
			t.Errorf("%+v %s %q = %s, want %s", c.id, c.action, c.topic, got, c.want)
		}
	}
}

func TestACLFileValidation(t *testing.T) {
	cases := map[string]string{
		"empty topic":    `{"rules":[{"topic":"","allow":["publish"]}]}`,
		"bad filter":     `{"rules":[{"topic":"a/#/b","allow":["publish"]}]}`,
		"no actions":     `{"rules":[{"topic":"a"}]}`,
		"unknown action": `{"rules":[{"topic":"a","allow":["read"]}]}`,
		"unknown key":    `{"rules":[{"topic":"a","allow":["publish"],"x":1}]}`,
	}
	for name, body := range cases {
		if _, err := LoadACLFile(write(t, "acl.json", body)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestCovers(t *testing.T) {
	cases := []struct {
		rule, req string
		want      bool
	}{
		{"a/b", "a/b", true}, {"a/+", "a/b", true}, {"#", "a/#", true}, {"a/#", "a/#", true}, {"a/#", "a/b/#", true},
		{"a/#", "#", false}, {"a/b", "a/#", false}, {"a/+", "a/#", false}, {"a/+", "a/+", true}, {"a/b", "a/+", false},
		{"+/+", "a/+", true}, {"+/b", "+/b", true}, {"+/b", "+/c", false}, {"a/+/c", "a/+/c", true}, {"a/+/c", "a/+", false},
		{"$SYS/#", "$SYS/+", true}, {"#", "$SYS/+", true},
	}
	for _, c := range cases {
		if got := covers(c.rule, c.req); got != c.want {
			t.Errorf("covers(%q, %q) = %v want %v", c.rule, c.req, got, c.want)
		}
	}
}
