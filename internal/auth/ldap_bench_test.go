package auth

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/ldap"
	"github.com/happydave/hoardmq/internal/ldap/ldaptest"
)

// BenchmarkLDAPConnectDecision is one CONNECT's search-mode decision against
// the in-process directory: the service bind, the search and the user's bind
// on a pooled connection. Run with -cpu to state the parallelism; the pool
// holds the default 16 connections.
func BenchmarkLDAPConnectDecision(b *testing.B) {
	s := ldaptest.New(b, ldapBase, ldapSvc)
	s.SetOpen(func(uid string) (string, bool) { return "pw-" + uid, true })
	e, err := ldap.ParseURL(s.URL)
	if err != nil {
		b.Fatal(err)
	}
	f, err := ldap.ParseTemplate("(&(objectClass=person)(uid=${username}))")
	if err != nil {
		b.Fatal(err)
	}
	a := &LDAPAuthenticator{Pool: &ldap.Pool{Endpoint: e, Max: 16}, Timeout: 5 * time.Second,
		Search: &LDAPSearch{Base: ldapBase, Scope: ldap.ScopeSub, Filter: f, BindDN: ldapSvc.DN, BindPassword: []byte(ldapSvc.Password)}}
	defer a.Pool.Close()
	var n atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := "c-" + strconv.FormatInt(n.Add(1)%10000, 10)
			if r := a.Authenticate(context.Background(), ldapReq(id, "pw-"+id)); r.Decision != Allow {
				b.Fatalf("%s: %+v", id, r)
			}
		}
	})
}
