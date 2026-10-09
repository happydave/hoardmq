package admin

import (
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/bans"
	"github.com/happydave/hoardmq/internal/config"
)

func TestBanRoutes(t *testing.T) {
	f, st := credFixture(t)
	rec := f.do("POST", "/api/v1/bans", opsToken, `{"kind":"cidr","value":"10.0.0.0/8","reason":"abuse","expiresInSeconds":600}`)
	if rec.Code != 201 {
		t.Fatalf("create %d %s", rec.Code, rec.Body)
	}
	b := decodeJSON[Ban](t, rec.Body.Bytes())
	if b.Key != "cidr:10.0.0.0/8" || b.Kind != bans.CIDR || b.Source != bans.SourceAdmin || b.ExpiresAt == nil || b.Version == 0 || b.UpdatedBy != "token:ops" {
		t.Fatalf("created %+v", b)
	}
	if left := time.Until(*b.ExpiresAt); left < 590*time.Second || left > 610*time.Second {
		t.Fatalf("expires in %v, want about 600 s", left)
	}
	if _, ok := st.Get(bans.Collection, "cidr:10.0.0.0/8"); !ok {
		t.Fatal("no record written")
	}
	if e, _ := f.b.lastAudit(t); e.Action != "ban.create" || e.Target != "cidr:10.0.0.0/8" || e.Outcome != "ok" || e.Detail["kind"] != "cidr" || e.Detail["expiresAt"] == "" {
		t.Fatalf("audit %+v", e)
	}
	if rec := f.do("POST", "/api/v1/bans", opsToken, `{"kind":"cidr","value":"10.0.0.0/8"}`); rec.Code != 409 {
		t.Fatalf("a duplicate %d %s", rec.Code, rec.Body)
	}
	for body, want := range map[string]string{
		`{"kind":"host","value":"x"}`:                                        "kind must be one of",
		`{"kind":"cidr","value":"10.1.2.3/8"}`:                               "10.0.0.0/8",
		`{"kind":"clientid","value":"x","expiresInSeconds":0}`:               "at least 1",
		`{"kind":"clientid","value":"x","expiresAt":"2020-01-01T00:00:00Z"}`: "in the future",
	} {
		rec := f.do("POST", "/api/v1/bans", opsToken, body)
		if rec.Code != 400 || !strings.Contains(rec.Body.String(), want) {
			t.Errorf("%s: %d %s, want 400 naming %q", body, rec.Code, rec.Body, want)
		}
	}
	f.do("POST", "/api/v1/bans", opsToken, `{"kind":"clientid","value":"a"}`)
	rec = f.do("GET", "/api/v1/bans?limit=1", readToken, "")
	page := decodeJSON[BanPage](t, rec.Body.Bytes())
	if rec.Code != 200 || len(page.Items) != 1 || page.Items[0].Key != "cidr:10.0.0.0/8" || page.Next != "cidr:10.0.0.0/8" {
		t.Fatalf("first page %d %+v", rec.Code, page)
	}
	rec = f.do("GET", "/api/v1/bans?after=cidr:10.0.0.0/8", readToken, "")
	if page = decodeJSON[BanPage](t, rec.Body.Bytes()); len(page.Items) != 1 || page.Items[0].Key != "clientid:a" {
		t.Fatalf("second page %+v", page)
	}
	if rec := f.do("POST", "/api/v1/bans", readToken, `{"kind":"clientid","value":"b"}`); rec.Code != 403 {
		t.Fatalf("a read-only principal's ban %d", rec.Code)
	}
	if e, _ := f.b.lastAudit(t); e.Action != "ban.create" || e.Outcome != "denied" {
		t.Fatalf("audit of the refused ban %+v", e)
	}
	rec = f.do("DELETE", "/api/v1/bans/cidr:10.0.0.0%2F8", opsToken, "")
	if rec.Code != 200 {
		t.Fatalf("delete %d %s", rec.Code, rec.Body)
	}
	if _, ok := st.Get(bans.Collection, "cidr:10.0.0.0/8"); ok {
		t.Fatal("the record survived its delete")
	}
	if e, _ := f.b.lastAudit(t); e.Action != "ban.delete" || e.Target != "cidr:10.0.0.0/8" || e.Outcome != "ok" {
		t.Fatalf("audit %+v", e)
	}
	if rec := f.do("DELETE", "/api/v1/bans/cidr:10.0.0.0%2F8", opsToken, ""); rec.Code != 404 {
		t.Fatalf("a second delete %d", rec.Code)
	}
}

func TestLimitsRoutes(t *testing.T) {
	f, st := credFixture(t)
	f.b.configured = config.Limits{ClientMessageRate: 100, ConnectRate: 50}
	rec := f.do("GET", "/api/v1/limits", readToken, "")
	v := decodeJSON[LimitsView](t, rec.Body.Bytes())
	if rec.Code != 200 || v.InForce.ClientMessageRate != 100 || v.InForce.ConnectRate != 50 || v.Administered.ClientMessageRate != nil {
		t.Fatalf("before %d %+v", rec.Code, v)
	}
	rec = f.do("PUT", "/api/v1/limits", opsToken, `{"clientMessageRate":5,"clientByteRate":1000}`)
	v = decodeJSON[LimitsView](t, rec.Body.Bytes())
	if rec.Code != 200 || v.InForce.ClientMessageRate != 5 || v.InForce.ClientByteRate != 1000 || v.InForce.ConnectRate != 50 {
		t.Fatalf("put %d %+v", rec.Code, v)
	}
	if r, ok := st.Get(config.LimitsCollection, config.DefaultLimitsKey); !ok || !strings.Contains(string(r.Value), `"clientMessageRate":5`) {
		t.Fatalf("the record: %v %s", ok, r.Value)
	}
	if e, _ := f.b.lastAudit(t); e.Action != "limits.update" || e.Target != "default" || e.Outcome != "ok" || !strings.Contains(e.Detail["record"], "clientByteRate") {
		t.Fatalf("audit %+v", e)
	}
	for body, want := range map[string]string{`{"clientMessageRate":-1}`: "negative", `{"clientByteBurst":10}`: "needs a clientByteRate"} {
		f.do("PUT", "/api/v1/limits", opsToken, `{}`)
		if rec := f.do("PUT", "/api/v1/limits", opsToken, body); rec.Code != 400 || !strings.Contains(rec.Body.String(), want) {
			t.Errorf("%s: %d %s", body, rec.Code, rec.Body)
		}
	}
	if rec := f.do("GET", "/api/v1/limits?namespace=tenant-a", readToken, ""); rec.Code != 400 || !strings.Contains(rec.Body.String(), "namespaces are not active") {
		t.Fatalf("another namespace %d %s", rec.Code, rec.Body)
	}
	if rec := f.do("PUT", "/api/v1/limits", readToken, `{}`); rec.Code != 403 {
		t.Fatalf("a read-only principal's change %d", rec.Code)
	}
}

func TestOverloadRoute(t *testing.T) {
	f, _ := credFixture(t)
	rec := f.do("GET", "/api/v1/overload", readToken, "")
	v := decodeJSON[OverloadView](t, rec.Body.Bytes())
	if rec.Code != 200 || v.NodeLevel != 2 || v.ClusterLevel != 3 || v.Peers["n2"] != 3 || v.Indicators["queued_messages"] != 7 {
		t.Fatalf("%d %+v", rec.Code, v)
	}
}
