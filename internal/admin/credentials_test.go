package admin

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/cluster/consensus"
	"github.com/happydave/hoardmq/internal/cluster/records"
)

// localRecords is a records store whose proposals apply at once, at
// increasing indexes, as a one-member cluster's would.
func localRecords() *records.Store {
	comp := records.New(0)
	index := uint64(0)
	return &records.Store{Component: comp, Propose: func(_ context.Context, _ consensus.Kind, payload []byte) (json.RawMessage, error) {
		index++
		v, err := comp.Apply(index, payload)
		if err != nil {
			return nil, err
		}
		return json.Marshal(v)
	}}
}

func credFixture(t *testing.T) (*fixture, *records.Store) {
	t.Helper()
	f := newFixture(t, 0)
	st := localRecords()
	f.b.records = st
	return f, st
}

func storedHash(t *testing.T, st *records.Store, user string) string {
	t.Helper()
	rec, ok := st.Get(auth.CredentialsCollection, user)
	if !ok {
		t.Fatalf("no record for %s", user)
	}
	var c auth.StoredCredential
	if err := json.Unmarshal(rec.Value, &c); err != nil {
		t.Fatal(err)
	}
	return c.Password
}

func TestCredentialRoutes(t *testing.T) {
	f, st := credFixture(t)
	rec := f.do("POST", "/api/v1/credentials", opsToken, `{"username":"alice","password":"s3cret-1","attributes":{"team":"blue"}}`)
	if rec.Code != 201 {
		t.Fatalf("create %d %s", rec.Code, rec.Body)
	}
	hash := storedHash(t, st, "alice")
	if !strings.HasPrefix(hash, "$pbkdf2-sha256$i=600000$") {
		t.Fatalf("stored %q", hash)
	}
	if ok, _ := auth.VerifyPassword(hash, []byte("s3cret-1")); !ok {
		t.Fatal("the stored hash does not verify the password")
	}
	c := decodeJSON[Credential](t, rec.Body.Bytes())
	if c.Username != "alice" || c.Form != "pbkdf2-sha256" || c.Attributes["team"] != "blue" || c.Version == 0 || c.UpdatedBy != "token:ops" {
		t.Fatalf("created %+v", c)
	}
	if strings.Contains(rec.Body.String(), hash) || strings.Contains(rec.Body.String(), "s3cret-1") {
		t.Fatal("the answer carries the password or its hash")
	}
	if e, _ := f.b.lastAudit(t); e.Action != "credential.create" || e.Target != "alice" || e.Outcome != "ok" || e.Actor != "token:ops" || e.Detail["form"] != "pbkdf2-sha256" {
		t.Fatalf("audit %+v", e)
	}
	if rec := f.do("POST", "/api/v1/credentials", opsToken, `{"username":"alice","password":"x"}`); rec.Code != 409 {
		t.Fatalf("a second create %d %s", rec.Code, rec.Body)
	}
	if e, _ := f.b.lastAudit(t); e.Action != "credential.create" || e.Outcome != "conflict" {
		t.Fatalf("audit of the refused create %+v", e)
	}
	first := c.Version
	rec = f.do("PUT", "/api/v1/credentials/alice", opsToken, `{"password":"s3cret-2","scram":true}`)
	if rec.Code != 200 {
		t.Fatalf("update %d %s", rec.Code, rec.Body)
	}
	c = decodeJSON[Credential](t, rec.Body.Bytes())
	if c.Form != "scram-sha-256" || c.Attributes["team"] != "blue" || c.Version <= first {
		t.Fatalf("updated %+v", c)
	}
	if ok, _ := auth.VerifyPassword(storedHash(t, st, "alice"), []byte("s3cret-2")); !ok {
		t.Fatal("the new password does not verify")
	}
	if rec := f.do("PUT", "/api/v1/credentials/alice", opsToken, `{"attributes":{},"ifVersion":`+itoa(first)+`}`); rec.Code != 409 {
		t.Fatalf("an update at a stale version %d %s", rec.Code, rec.Body)
	}
	if rec := f.do("PUT", "/api/v1/credentials/alice", opsToken, `{"attributes":{},"ifVersion":`+itoa(c.Version)+`}`); rec.Code != 200 || decodeJSON[Credential](t, rec.Body.Bytes()).Attributes != nil {
		t.Fatalf("attributes replaced by none %d %s", rec.Code, rec.Body)
	}
	if ok, _ := auth.VerifyPassword(storedHash(t, st, "alice"), []byte("s3cret-2")); !ok {
		t.Fatal("an attributes-only update changed the password")
	}
	hashed, _ := auth.HashSCRAM([]byte("s3cret-3"), auth.MinSCRAMIterations)
	if rec := f.do("PUT", "/api/v1/credentials/alice", opsToken, `{"passwordHash":"`+hashed+`"}`); rec.Code != 200 {
		t.Fatalf("a hash %d %s", rec.Code, rec.Body)
	}
	if storedHash(t, st, "alice") != hashed {
		t.Fatal("the hash was not stored as given")
	}
	if rec := f.do("PUT", "/api/v1/credentials/alice", opsToken, `{"passwordHash":"plaintext"}`); rec.Code != 400 {
		t.Fatalf("a plain passwordHash %d %s", rec.Code, rec.Body)
	}
	if rec := f.do("PUT", "/api/v1/credentials/bob", opsToken, `{"password":"x"}`); rec.Code != 404 {
		t.Fatalf("an update of nobody %d %s", rec.Code, rec.Body)
	}
	rec = f.do("GET", "/api/v1/credentials", readToken, "")
	if rec.Code != 200 || strings.Contains(rec.Body.String(), "$scram") || strings.Contains(rec.Body.String(), "password") {
		t.Fatalf("list %d %s", rec.Code, rec.Body)
	}
	if p := decodeJSON[CredentialPage](t, rec.Body.Bytes()); len(p.Items) != 1 || p.Items[0].Username != "alice" || p.Next != "" {
		t.Fatalf("page %+v", p)
	}
	if rec := f.do("GET", "/api/v1/credentials/alice", readToken, ""); rec.Code != 200 || strings.Contains(rec.Body.String(), "$scram") {
		t.Fatalf("get %d %s", rec.Code, rec.Body)
	}
	if rec := f.do("POST", "/api/v1/credentials", readToken, `{"username":"mallory","password":"x"}`); rec.Code != 403 {
		t.Fatalf("a reader creating %d %s", rec.Code, rec.Body)
	}
	if e, _ := f.b.lastAudit(t); e.Action != "credential.create" || e.Outcome != "denied" || e.Actor != "token:reader" {
		t.Fatalf("audit of the reader's create %+v", e)
	}
	if _, ok := st.Get(auth.CredentialsCollection, "mallory"); ok {
		t.Fatal("a reader created a credential")
	}
	if rec := f.do("DELETE", "/api/v1/credentials/alice?ifVersion=1", opsToken, ""); rec.Code != 409 {
		t.Fatalf("a delete at a stale version %d %s", rec.Code, rec.Body)
	}
	rec = f.do("DELETE", "/api/v1/credentials/alice", opsToken, "")
	if rec.Code != 200 || strings.Contains(rec.Body.String(), "$scram") {
		t.Fatalf("delete %d %s", rec.Code, rec.Body)
	}
	if gone := decodeJSON[Credential](t, rec.Body.Bytes()); gone.Username != "alice" || gone.Form != "scram-sha-256" || gone.Version == 0 {
		t.Fatalf("the removed credential %+v", gone)
	}
	if e, _ := f.b.lastAudit(t); e.Action != "credential.delete" || e.Target != "alice" || e.Outcome != "ok" {
		t.Fatalf("audit %+v", e)
	}
	if _, ok := st.Get(auth.CredentialsCollection, "alice"); ok {
		t.Fatal("alice survived her delete")
	}
	if rec := f.do("DELETE", "/api/v1/credentials/alice", opsToken, ""); rec.Code != 404 {
		t.Fatalf("a second delete %d %s", rec.Code, rec.Body)
	}
	for _, e := range f.b.audits {
		for _, v := range e.Detail {
			if strings.Contains(v, "s3cret") || strings.Contains(v, "$pbkdf2") || strings.Contains(v, "$scram") {
				t.Fatalf("an audit record carries a password or a hash: %+v", e)
			}
		}
	}
}

func TestCredentialRequestRules(t *testing.T) {
	s := func(v string) *string { return &v }
	for _, c := range []struct {
		name           string
		password, hash *string
		scram, req     bool
		want           string
	}{
		{"a password", s("x"), nil, false, true, ""},
		{"a hash", nil, s("$h"), false, true, ""},
		{"both", s("x"), s("$h"), false, true, "not both"},
		{"neither on create", nil, nil, false, true, "is required"},
		{"neither on update", nil, nil, false, false, ""},
		{"scram with a hash", nil, s("$h"), true, true, "scram applies"},
		{"an empty password", s(""), nil, false, true, "empty"},
	} {
		if got := passwordProblem(c.password, c.hash, c.scram, c.req); (c.want == "") != (got == "") || !strings.Contains(got, c.want) {
			t.Errorf("%s: %q, want %q", c.name, got, c.want)
		}
	}
	f, _ := credFixture(t)
	for body, want := range map[string]int{
		`{"username":"","password":"x"}`:                        400,
		`{"username":"a","password":""}`:                        400,
		`{"username":"a"}`:                                      400,
		`{"username":"a","password":"x","passwordHash":"$h$x"}`: 400,
		`{"username":"a","password":"x","attributes":{"":"v"}}`: 400,
		`{"username":"a","password":"x","colour":"red"}`:        400,
	} {
		if rec := f.do("POST", "/api/v1/credentials", opsToken, body); rec.Code != want {
			t.Errorf("%s: %d %s, want %d", body, rec.Code, rec.Body, want)
		}
	}
	if rec := f.do("PUT", "/api/v1/credentials/a", opsToken, `{}`); rec.Code != 400 {
		t.Errorf("an empty update: %d", rec.Code)
	}
}

// The database's own failures: not open on the node, and a write the
// cluster did not confirm.
func TestCredentialStoreUnavailable(t *testing.T) {
	f := newFixture(t, 0)
	if rec := f.do("GET", "/api/v1/credentials", opsToken, ""); rec.Code != 503 {
		t.Fatalf("not open: %d %s", rec.Code, rec.Body)
	}
	st := &records.Store{Component: records.New(0), Propose: func(context.Context, consensus.Kind, []byte) (json.RawMessage, error) {
		return nil, errors.New("no leader")
	}}
	f.b.records = st
	if rec := f.do("POST", "/api/v1/credentials", opsToken, `{"username":"a","password":"x"}`); rec.Code != 503 {
		t.Fatalf("unconfirmed: %d %s", rec.Code, rec.Body)
	}
	// A cluster that never answers: the write gives up at its bound.
	f.b.records = &records.Store{Component: records.New(0), Propose: func(ctx context.Context, _ consensus.Kind, _ []byte) (json.RawMessage, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	f.srv.writeTimeout = 300 * time.Millisecond
	done := make(chan int, 1)
	go func() { done <- f.do("DELETE", "/api/v1/credentials/a", opsToken, "").Code }()
	select {
	case code := <-done:
		if code != 503 {
			t.Fatalf("no answer from the cluster: %d", code)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("a write the cluster never answered was still waiting after 3 s")
	}
}

func decodeJSON[T any](t *testing.T, b []byte) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatalf("decode %s: %v", b, err)
	}
	return v
}

func itoa(n uint64) string {
	b, _ := json.Marshal(n)
	return string(b)
}
