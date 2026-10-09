package auth

import (
	"context"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/ldap"
	"github.com/happydave/hoardmq/internal/ldap/ldaptest"
)

const ldapBase = "ou=people,dc=x"

var (
	ldapAlice = ldaptest.Entry{DN: "uid=alice," + ldapBase, Password: "wonderland", Attrs: map[string][]string{"objectClass": {"person"}, "uid": {"alice"}}}
	ldapSvc   = ldaptest.Entry{DN: "cn=svc,dc=x", Password: "svc-secret"}
	ldapDup1  = ldaptest.Entry{DN: "uid=dup,ou=a," + ldapBase, Password: "x", Attrs: map[string][]string{"objectClass": {"person"}, "uid": {"dup"}}}
	ldapDup2  = ldaptest.Entry{DN: "uid=dup,ou=b," + ldapBase, Password: "x", Attrs: map[string][]string{"objectClass": {"person"}, "uid": {"dup"}}}
)

func ldapReq(user, pw string) *Request {
	return &Request{Username: user, HasUsername: true, Password: []byte(pw), HasPassword: true}
}

func searchAuth(t *testing.T, s *ldaptest.Server, svcPassword string) *LDAPAuthenticator {
	t.Helper()
	e, err := ldap.ParseURL(s.URL)
	if err != nil {
		t.Fatal(err)
	}
	f, err := ldap.ParseTemplate("(&(objectClass=person)(uid=${username}))")
	if err != nil {
		t.Fatal(err)
	}
	a := &LDAPAuthenticator{Pool: &ldap.Pool{Endpoint: e, Max: 1}, Timeout: 2 * time.Second,
		Search: &LDAPSearch{Base: ldapBase, Scope: ldap.ScopeSub, Filter: f, BindDN: ldapSvc.DN, BindPassword: []byte(svcPassword)}}
	t.Cleanup(a.Pool.Close)
	return a
}

func TestLDAPOutcome(t *testing.T) {
	search := func(entries int) ldapStages {
		return ldapStages{searchMode: true, searched: true, entries: entries}
	}
	bound := func(st ldapStages, code int) ldapStages { st.userBound, st.bindCode = true, code; return st }
	for _, c := range []struct {
		name string
		st   ldapStages
		pass bool
		want Decision
	}{
		{"direct, bound", ldapStages{userBound: true}, false, Allow},
		{"direct, invalid credentials", ldapStages{userBound: true, bindCode: 49}, false, Deny},
		{"direct, busy, deny policy", ldapStages{userBound: true, bindCode: 51}, false, Deny},
		{"direct, busy, pass policy", ldapStages{userBound: true, bindCode: 51}, true, Pass},
		{"direct, unavailable, pass policy", ldapStages{userBound: true, bindCode: 52}, true, Pass},
		{"direct, other code", ldapStages{userBound: true, bindCode: 50}, true, Deny},
		{"search, no entry", search(0), false, Pass},
		{"search, two entries", search(2), true, Deny},
		{"search, size limit, two entries", func() ldapStages { st := search(2); st.searchCode = 4; return st }(), true, Deny},
		{"search, one entry, bound", bound(search(1), 0), false, Allow},
		{"search, one entry, size limit, bound", func() ldapStages { st := bound(search(1), 0); st.searchCode = 4; return st }(), false, Allow},
		{"not searched (service bind refused, or a failure), pass policy", ldapStages{searchMode: true}, true, Pass},
		{"not searched, deny policy", ldapStages{searchMode: true}, false, Deny},
		{"no such base, pass policy", func() ldapStages { st := search(0); st.searchCode = 32; return st }(), true, Pass},
		{"no such base, deny policy", func() ldapStages { st := search(0); st.searchCode = 32; return st }(), false, Deny},
		{"direct, not bound (a failure), pass policy", ldapStages{}, true, Pass},
		{"direct, not bound, deny policy", ldapStages{}, false, Deny},
	} {
		got, _ := ldapOutcome(c.st, c.pass)
		if got.Decision != c.want {
			t.Errorf("%s: %s (%q), want %s", c.name, got.Decision, got.Reason, c.want)
		}
	}
	if r, o := ldapOutcome(ldapStages{userBound: true, bindCode: 50}, false); r.Reason != "ldap: bind refused (50)" || o != "refused" {
		t.Errorf("bind refused: %q %s", r.Reason, o)
	}
}

func TestLDAPClaimsAndSender(t *testing.T) {
	for _, c := range []struct {
		req  *Request
		want bool
	}{
		{ldapReq("alice", "pw"), true},
		{ldapReq("", "pw"), false},
		{ldapReq("alice", ""), false},
		{&Request{Username: "alice", HasUsername: true}, false},
		{&Request{Password: []byte("pw"), HasPassword: true}, false},
	} {
		if got := ldapClaims(c.req); got != c.want {
			t.Errorf("claims %+v: %v", c.req, got)
		}
	}
	for _, c := range []struct {
		e    ldap.Endpoint
		want bool
	}{
		{ldap.Endpoint{Host: "dir.example"}, false},
		{ldap.Endpoint{Host: "dir.example", LDAPS: true}, true},
		{ldap.Endpoint{Host: "dir.example", StartTLS: true}, true},
		{ldap.Endpoint{Host: "127.0.0.1"}, true},
	} {
		if got := ldapMaySend(c.e); got != c.want {
			t.Errorf("may send to %+v: %v", c.e, got)
		}
	}
	if !bindAfterSearch(ldapStages{searched: true, entries: 1}) || bindAfterSearch(ldapStages{searched: true, entries: 2}) ||
		bindAfterSearch(ldapStages{searched: true, entries: 1, searchCode: 32}) || bindAfterSearch(ldapStages{entries: 1}) {
		t.Fatal("bind after search")
	}
}

func TestLDAPSearchMode(t *testing.T) {
	s := ldaptest.New(t, ldapBase, ldapAlice, ldapSvc, ldapDup1, ldapDup2)
	a := searchAuth(t, s, ldapSvc.Password)
	ctx := context.Background()
	if r := a.Authenticate(ctx, ldapReq("alice", "wonderland")); r.Decision != Allow {
		t.Fatalf("alice: %+v", r)
	}
	if log := s.BindLog(); len(log) != 2 || log[0] != ldapSvc.DN || log[1] != ldapAlice.DN {
		t.Fatalf("binds %q", log)
	}
	if n, v := ldaptest.FilterEqualities(s.LastFilter()); n != 2 || v[1] != "uid=alice" {
		t.Fatalf("filter %d %q", n, v)
	}
	if r := a.Authenticate(ctx, ldapReq("alice", "looking-glass")); r.Decision != Deny || r.Reason != "ldap: invalid credentials" {
		t.Fatalf("wrong password: %+v", r)
	}
	before := len(s.BindLog())
	if r := a.Authenticate(ctx, ldapReq("bob", "x")); r.Decision != Pass {
		t.Fatalf("bob: %+v", r)
	}
	if log := s.BindLog()[before:]; len(log) != 1 || log[0] != ldapSvc.DN {
		t.Fatalf("an unknown user's binds %q, want the service's only", log)
	}
	before = len(s.BindLog())
	if r := a.Authenticate(ctx, ldapReq("dup", "x")); r.Decision != Deny || r.Reason != "ldap: ambiguous user" {
		t.Fatalf("dup: %+v", r)
	}
	if log := s.BindLog()[before:]; len(log) != 1 {
		t.Fatalf("an ambiguous user's binds %q, want the service's only", log)
	}
	if r := a.Authenticate(ctx, ldapReq("*)(uid=*", "x")); r.Decision != Pass {
		t.Fatalf("an injected username: %+v", r)
	}
	if n, v := ldaptest.FilterEqualities(s.LastFilter()); n != 2 || v[0] != "objectClass=person" || v[1] != "uid=*)(uid=*" {
		t.Fatalf("an injected username's filter %d %q", n, v)
	}
	before = s.Binds(ldapAlice.DN)
	for _, r := range []*Request{ldapReq("alice", ""), {Username: "alice", HasUsername: true}, {Password: []byte("wonderland"), HasPassword: true}} {
		if got := a.Authenticate(ctx, r); got.Decision != Pass {
			t.Fatalf("%+v: %+v", r, got)
		}
	}
	if s.Binds(ldapAlice.DN) != before {
		t.Fatal("alice's DN was bound without a password")
	}
}

func TestLDAPUnavailable(t *testing.T) {
	s := ldaptest.New(t, ldapBase, ldapAlice, ldapSvc)
	a := searchAuth(t, s, "wrong-service-password")
	if r := a.Authenticate(context.Background(), ldapReq("alice", "wonderland")); r.Decision != Deny || r.Reason != "ldap: directory unavailable" {
		t.Fatalf("service bind refused: %+v", r)
	}
	a = searchAuth(t, s, ldapSvc.Password)
	a.Timeout = 300 * time.Millisecond
	s.SetBindDelay(3 * time.Second)
	for _, pass := range []bool{false, true} {
		a.PassOnUnavailable = pass
		start := time.Now()
		r := a.Authenticate(context.Background(), ldapReq("alice", "wonderland"))
		want := Deny
		if pass {
			want = Pass
		}
		if r.Decision != want || time.Since(start) > 600*time.Millisecond {
			t.Fatalf("a slow directory, pass=%v: %+v after %v", pass, r, time.Since(start))
		}
	}
	s.SetBindDelay(0)
	s.Stop()
	a.PassOnUnavailable = false
	if r := a.Authenticate(context.Background(), ldapReq("alice", "wonderland")); r.Decision != Deny {
		t.Fatalf("a stopped directory: %+v", r)
	}
	counts := map[string]int{}
	a.Count = func(name string, labels map[string]string) { counts[name+"/"+labels["outcome"]+labels["policy"]]++ }
	a.PassOnUnavailable = true
	_ = a.Authenticate(context.Background(), ldapReq("alice", "wonderland"))
	if counts["hoardmq_auth_ldap_decisions_total/unavailable"] != 1 || counts["hoardmq_auth_ldap_unavailable_total/pass"] != 1 {
		t.Fatalf("counts %v", counts)
	}
}

func TestLDAPDirectBind(t *testing.T) {
	s := ldaptest.New(t, ldapBase, ldapAlice)
	e, _ := ldap.ParseURL(s.URL)
	tpl, err := ldap.ParseDNTemplate("uid=${username}," + ldapBase)
	if err != nil {
		t.Fatal(err)
	}
	a := &LDAPAuthenticator{Pool: &ldap.Pool{Endpoint: e, Max: 1}, BindDN: tpl, Timeout: 2 * time.Second}
	defer a.Pool.Close()
	if r := a.Authenticate(context.Background(), ldapReq("alice", "wonderland")); r.Decision != Allow {
		t.Fatalf("alice: %+v", r)
	}
	if r := a.Authenticate(context.Background(), ldapReq("a,b", "x")); r.Decision != Deny {
		t.Fatalf("a,b: %+v", r)
	}
	if log := s.BindLog(); log[len(log)-1] != `uid=a\,b,`+ldapBase {
		t.Fatalf("bound %q", log[len(log)-1])
	}
}

// A connection reused after a user's bind searches only after an anonymous
// bind, never as the user it last bound.
func TestLDAPAnonymousSearchRebinds(t *testing.T) {
	s := ldaptest.New(t, ldapBase, ldapAlice)
	a := searchAuth(t, s, "")
	a.Search.BindDN, a.Search.BindPassword = "", nil
	for range 2 {
		if r := a.Authenticate(context.Background(), ldapReq("alice", "wonderland")); r.Decision != Allow {
			t.Fatalf("alice: %+v", r)
		}
	}
	if log := s.BindLog(); len(log) != 4 || log[0] != "" || log[1] != ldapAlice.DN || log[2] != "" || log[3] != ldapAlice.DN {
		t.Fatalf("binds %q", log)
	}
	if s.Connections() != 1 {
		t.Fatalf("%d connections, want the one reused", s.Connections())
	}
}

// A password is never sent to a directory in clear off this host: the
// decision is taken before any bind. The endpoint names a remote host but
// reaches the fixture, which would answer a bind.
func TestLDAPRefusesAClearRemoteDirectory(t *testing.T) {
	s := ldaptest.New(t, ldapBase, ldapAlice)
	e, _ := ldap.ParseURL(s.URL)
	e.Host = "dir.example"
	a := &LDAPAuthenticator{Pool: &ldap.Pool{Endpoint: e, Max: 1}, BindDN: "uid=${username}," + ldapBase, Timeout: time.Second}
	defer a.Pool.Close()
	if r := a.Authenticate(context.Background(), ldapReq("alice", "wonderland")); r.Decision != Deny || r.Reason != "ldap: directory unavailable" {
		t.Fatalf("%+v", r)
	}
	if n := s.Binds(ldapAlice.DN); n != 0 {
		t.Fatalf("%d binds sent in clear to a remote host", n)
	}
}
