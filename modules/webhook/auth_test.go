package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/ext/auth"
	"github.com/happydave/hoardmq/internal/testcert"
)

// recorded is one request an endpoint received.
type recorded struct {
	header http.Header
	body   map[string]any
}

// recorder is an endpoint that keeps what it receives and answers with
// answer.
type recorder struct {
	mu     sync.Mutex
	got    []recorded
	answer func(w http.ResponseWriter, body map[string]any)
}

func (r *recorder) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	data, _ := io.ReadAll(req.Body)
	var body map[string]any
	_ = json.Unmarshal(data, &body)
	r.mu.Lock()
	r.got = append(r.got, recorded{header: req.Header.Clone(), body: body})
	r.mu.Unlock()
	r.answer(w, body)
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.got)
}

func (r *recorder) last() recorded {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.got[len(r.got)-1]
}

func answerJSON(w http.ResponseWriter, maxAge int, v any) {
	if maxAge > 0 {
		w.Header().Set("Cache-Control", fmt.Sprintf("max-age=%d", maxAge))
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func connectRequest(username string) *auth.Request {
	return &auth.Request{
		Namespace: "default", ClientID: "c1", Username: username, HasUsername: true,
		Password: []byte("hunter2-pass"), HasPassword: true, PeerAddr: "192.0.2.7:51000", Listener: "tcp",
	}
}

// TestConfiguration: good documents validate and connect to nothing; each
// bad one is refused naming its key.
func TestConfiguration(t *testing.T) {
	dir := t.TempDir()
	notPEM := filepath.Join(dir, "not.pem")
	if err := os.WriteFile(notPEM, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	// A port nothing listens on: Validate must not try it.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closed := "http://" + l.Addr().String() + "/auth"
	_ = l.Close()

	for _, good := range []string{
		`{"url":"` + closed + `"}`,
		`{"url":"https://auth.example.com/mqtt","onUnavailable":"pass","timeoutMs":100,"cache":{"maxEntries":0}}`,
		`{"url":"http://localhost:9/x","authenticate":false,"sendPassword":false}`,
	} {
		a := &authHTTP{readFile: readFileOS}
		if err := a.Provision(moduleContext(&logBuf{}, 0), json.RawMessage(good)); err != nil {
			t.Errorf("auth.http %s: provision %v", good, err)
			continue
		}
		if err := a.Validate(); err != nil {
			t.Errorf("auth.http %s: %v", good, err)
		}
	}
	for _, good := range []string{
		`{"url":"http://hooks.example.com/e"}`,
		`{"url":"https://hooks.example.com/e","events":["message.publish"],"mode":"sync","payload":"omit",
		  "buffer":{"maxEvents":1},"batch":{"maxEvents":1000,"maxDelayMs":0},"retry":{"maxAttempts":10,"backoffMs":10}}`,
	} {
		e := &eventsHTTP{readFile: readFileOS}
		if err := e.Provision(moduleContext(&logBuf{}, 0), json.RawMessage(good)); err != nil {
			t.Errorf("events.http %s: provision %v", good, err)
			continue
		}
		if err := e.Validate(); err != nil {
			t.Errorf("events.http %s: %v", good, err)
		}
	}

	for _, c := range []struct{ doc, want string }{
		{`{}`, "url is required"},
		{`{"url":"ftp://a/b"}`, "url must be"},
		{`{"url":"http://auth.example.com/mqtt"}`, "url must be https"},
		{`{"url":"https://a","timeoutMs":99}`, "timeoutMs"},
		{`{"url":"https://a","timeoutMs":30001}`, "timeoutMs"},
		{`{"url":"https://a","onUnavailable":"allow"}`, "onUnavailable"},
		{`{"url":"https://a","cache":{"maxEntries":-1}}`, "cache.maxEntries"},
		{`{"url":"https://a","certFile":"c.pem"}`, "certFile and keyFile"},
		{`{"url":"https://a","caFile":"` + notPEM + `"}`, "caFile"},
		{`{"url":"https://a","bearerTokenFile":"` + filepath.Join(dir, "absent") + `"}`, "bearerTokenFile"},
		{`{"url":"https://a","bearerTokenFile":"` + notPEM + `","headers":{"authorization":"x"}}`, "Authorization"},
		{`{"url":"https://a","headers":{"x-a":"v\r\nInjected: 1"}}`, "the value of x-a holds a line break"},
		{`{"url":"https://a","headers":{"bad name":"v"}}`, "is not a header name"},
	} {
		a := &authHTTP{readFile: readFileOS}
		err := a.Provision(moduleContext(&logBuf{}, 0), json.RawMessage(c.doc))
		if err == nil {
			err = a.Validate()
		}
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("auth.http %s: %v, want %q", c.doc, err, c.want)
		}
		if err != nil && strings.Contains(err.Error(), "Injected") {
			t.Errorf("auth.http %s: the refusal quotes the header's value: %v", c.doc, err)
		}
	}
	for _, c := range []struct{ doc, want string }{
		{`{"url":"https://a","retries":2}`, "retries"},
		{`{"url":"https://a","events":["client.unsubscribed"]}`, "client.unsubscribed"},
		{`{"url":"https://a","mode":"later"}`, "mode"},
		{`{"url":"https://a","payload":"hex"}`, "payload"},
		{`{"url":"https://a","buffer":{"maxEvents":0}}`, "buffer.maxEvents"},
		{`{"url":"https://a","batch":{"maxEvents":1001}}`, "batch.maxEvents"},
		{`{"url":"https://a","batch":{"maxDelayMs":10001}}`, "batch.maxDelayMs"},
		{`{"url":"https://a","retry":{"maxAttempts":0}}`, "retry.maxAttempts"},
		{`{"url":"https://a","retry":{"backoffMs":9}}`, "retry.backoffMs"},
		{`{"url":"https://a","timeoutMs":50}`, "timeoutMs"},
	} {
		e := &eventsHTTP{readFile: readFileOS}
		err := e.Provision(moduleContext(&logBuf{}, 0), json.RawMessage(c.doc))
		if err == nil {
			err = e.Validate()
		}
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("events.http %s: %v, want %q", c.doc, err, c.want)
		}
	}
}

// TestAuthenticate: the endpoint's answers decide a CONNECT as written, and
// the request carries the connection and the password only when sent.
func TestAuthenticate(t *testing.T) {
	var now time.Time
	rec := &recorder{answer: func(w http.ResponseWriter, body map[string]any) {
		switch body["username"] {
		case "allow":
			answerJSON(w, 0, map[string]any{"result": "allow", "client_attrs": map[string]string{"tenant": "a", SuperuserAttribute: "true"}})
		case "deny":
			answerJSON(w, 0, map[string]any{"result": "deny"})
		case "ignore":
			answerJSON(w, 0, map[string]any{"result": "ignore"})
		case "nocontent":
			w.WriteHeader(http.StatusNoContent)
		case "super":
			answerJSON(w, 0, map[string]any{"result": "allow", "is_superuser": true})
		case "maxage-first":
			answerJSON(w, 10, map[string]any{"result": "allow", "expire_at": now.Unix() + 100})
		case "expire-first":
			answerJSON(w, 100, map[string]any{"result": "allow", "expire_at": now.Unix() + 5})
		case "expired":
			answerJSON(w, 0, map[string]any{"result": "allow", "expire_at": now.Unix()})
		case "client-error":
			w.WriteHeader(http.StatusForbidden)
		case "":
			answerJSON(w, 0, map[string]any{"result": "ignore"})
		}
	}}
	srv := httptest.NewServer(rec)
	defer srv.Close()
	a, _, _, c := newAuth(t, `{"url":"`+srv.URL+`"}`, 0)
	now = c.now()

	for _, tc := range []struct {
		user    string
		want    auth.Decision
		attrs   map[string]string
		expires time.Time
	}{
		// Every allow writes the superuser attribute, true or false.
		{"allow", auth.Allow, map[string]string{"tenant": "a", SuperuserAttribute: "false"}, time.Time{}},
		{"deny", auth.Deny, nil, time.Time{}},
		{"ignore", auth.Pass, nil, time.Time{}},
		{"nocontent", auth.Allow, map[string]string{SuperuserAttribute: "false"}, time.Time{}},
		{"super", auth.Allow, map[string]string{SuperuserAttribute: "true"}, time.Time{}},
		{"maxage-first", auth.Allow, map[string]string{SuperuserAttribute: "false"}, now.Add(10 * time.Second)},
		{"expire-first", auth.Allow, map[string]string{SuperuserAttribute: "false"}, now.Add(5 * time.Second)},
		// An allow that has already lapsed would be re-asked at once, forever.
		{"expired", auth.Deny, nil, time.Time{}},
		// EMQX reads a 4xx as ignore; here it is no answer, so deny.
		{"client-error", auth.Deny, nil, time.Time{}},
	} {
		r := a.Authenticate(context.Background(), connectRequest(tc.user))
		if r.Decision != tc.want {
			t.Errorf("%s: decision %v, want %v", tc.user, r.Decision, tc.want)
		}
		if fmt.Sprint(r.Attributes) != fmt.Sprint(tc.attrs) {
			t.Errorf("%s: attributes %v, want %v", tc.user, r.Attributes, tc.attrs)
		}
		if !r.Expires.Equal(tc.expires) {
			t.Errorf("%s: expires %v, want %v", tc.user, r.Expires, tc.expires)
		}
	}

	got := rec.last()
	if got.header.Get("hoardmq-hook") != "connect" {
		t.Errorf("hoardmq-hook %q", got.header.Get("hoardmq-hook"))
	}
	want := map[string]any{
		"action": "connect", "clientid": "c1", "username": "client-error", "password": "hunter2-pass",
		"peerhost": "192.0.2.7", "peername": "192.0.2.7:51000", "cert_common_name": "", "listener": "tcp",
		"namespace": "default", "node": "n1",
	}
	if fmt.Sprint(got.body) != fmt.Sprint(want) {
		t.Errorf("request body\n got %v\nwant %v", got.body, want)
	}

	noPassword := connectRequest("allow")
	noPassword.Password, noPassword.HasPassword = nil, false
	a.Authenticate(context.Background(), noPassword)
	if _, ok := rec.last().body["password"]; ok {
		t.Error("a CONNECT without a password sent one")
	}
	b, _, _, _ := newAuth(t, `{"url":"`+srv.URL+`","sendPassword":false}`, 0)
	b.Authenticate(context.Background(), connectRequest("allow"))
	if _, ok := rec.last().body["password"]; ok {
		t.Error("sendPassword false sent the password")
	}
	// A CONNECT with no credentials is asked like any; the endpoint decides.
	before := rec.count()
	if r := a.Authenticate(context.Background(), &auth.Request{Namespace: "default", ClientID: "anon", PeerAddr: "192.0.2.7:1"}); r.Decision != auth.Pass || rec.count() != before+1 {
		t.Errorf("anonymous: %v after %d requests", r.Decision, rec.count()-before)
	}
	off, _, _, _ := newAuth(t, `{"url":"`+srv.URL+`","authenticate":false}`, 0)
	before = rec.count()
	if r := off.Authenticate(context.Background(), connectRequest("deny")); r.Decision != auth.Pass || rec.count() != before {
		t.Errorf("authenticate false: %v after %d requests", r.Decision, rec.count()-before)
	}
}

// TestAuthorize: answers decide, are held for max-age within the decision
// TTL, a superuser asks nothing, and no request carries a password.
func TestAuthorize(t *testing.T) {
	rec := &recorder{answer: func(w http.ResponseWriter, body map[string]any) {
		switch body["topic"] {
		case "a/allow":
			answerJSON(w, 10, map[string]any{"result": "allow"})
		case "a/long":
			answerJSON(w, 100, map[string]any{"result": "allow"})
		case "a/deny":
			answerJSON(w, 0, map[string]any{"result": "deny"})
		default:
			answerJSON(w, 0, map[string]any{"result": "ignore"})
		}
	}}
	srv := httptest.NewServer(rec)
	defer srv.Close()
	a, m, _, c := newAuth(t, `{"url":"`+srv.URL+`"}`, 30*time.Second)
	id := &auth.Identity{Namespace: "default", ClientID: "c1", Username: "u1"}
	ctx := context.Background()

	if d := a.Authorize(ctx, auth.Publish, "a/allow", id); d != auth.Allow {
		t.Fatalf("publish a/allow: %v", d)
	}
	got := rec.last()
	if got.header.Get("hoardmq-hook") != "publish" || got.body["action"] != "publish" || got.body["topic"] != "a/allow" {
		t.Errorf("request %v %v", got.header, got.body)
	}
	if _, ok := got.body["password"]; ok {
		t.Error("an authorization request carried a password")
	}
	n := rec.count()
	c.advance(9 * time.Second)
	if d := a.Authorize(ctx, auth.Publish, "a/allow", id); d != auth.Allow || rec.count() != n {
		t.Errorf("within max-age: %v after %d more requests", d, rec.count()-n)
	}
	if m.get("hoardmq_auth_http_cache_hits_total{}") != 1 {
		t.Errorf("cache hits %v", m.keys())
	}
	if d := a.Authorize(ctx, auth.Subscribe, "a/allow", id); d != auth.Allow || rec.count() != n+1 {
		t.Errorf("another action is another key: %v after %d more requests", d, rec.count()-n)
	}
	c.advance(2 * time.Second)
	if a.Authorize(ctx, auth.Publish, "a/allow", id); rec.count() != n+2 {
		t.Errorf("after max-age: %d more requests, want 2", rec.count()-n)
	}

	// max-age 100 within a decision TTL of 30 s is held 30 s.
	a.Authorize(ctx, auth.Publish, "a/long", id)
	n = rec.count()
	c.advance(29 * time.Second)
	a.Authorize(ctx, auth.Publish, "a/long", id)
	if rec.count() != n {
		t.Errorf("within the decision TTL: %d more requests", rec.count()-n)
	}
	c.advance(2 * time.Second)
	a.Authorize(ctx, auth.Publish, "a/long", id)
	if rec.count() != n+1 {
		t.Errorf("past the decision TTL: %d more requests, want 1", rec.count()-n)
	}

	// Without max-age nothing is held.
	for range 2 {
		if d := a.Authorize(ctx, auth.Subscribe, "a/deny", id); d != auth.Deny {
			t.Errorf("subscribe a/deny: %v", d)
		}
	}
	if d := a.Authorize(ctx, auth.Unsubscribe, "other", id); d != auth.Pass {
		t.Errorf("ignore: %v", d)
	}
	n = rec.count()
	a.Authorize(ctx, auth.Subscribe, "a/deny", id)
	if rec.count() != n+1 {
		t.Error("an answer without max-age was held")
	}

	super := &auth.Identity{Namespace: "default", ClientID: "c2", Attributes: map[string]string{SuperuserAttribute: "true"}, AdmittedBy: string(AuthID)}
	n = rec.count()
	if d := a.Authorize(ctx, auth.Subscribe, "a/deny", super); d != auth.Allow || rec.count() != n {
		t.Errorf("superuser: %v after %d requests", d, rec.count()-n)
	}

	for key, want := range map[string]float64{
		"hoardmq_auth_http_requests_total{action=publish,outcome=allow}":       4,
		"hoardmq_auth_http_requests_total{action=subscribe,outcome=deny}":      3,
		"hoardmq_auth_http_requests_total{action=subscribe,outcome=superuser}": 1,
		"hoardmq_auth_http_requests_total{action=unsubscribe,outcome=pass}":    1,
	} {
		if got := m.get(key); got != want {
			t.Errorf("%s = %v, want %v\n%s", key, got, want, m.keys())
		}
	}
	for _, g := range rec.got {
		if _, ok := g.body["password"]; ok {
			t.Errorf("a %v request carried a password", g.body["action"])
		}
	}

	off, _, _, _ := newAuth(t, `{"url":"`+srv.URL+`","authorize":false}`, 0)
	n = rec.count()
	if d := off.Authorize(ctx, auth.Publish, "a/deny", id); d != auth.Pass || rec.count() != n {
		t.Errorf("authorize false: %v after %d requests", d, rec.count()-n)
	}
}

// TestAuthorizeCacheBound: a full cache drops its oldest answer, and the
// gauge reports what it holds.
func TestAuthorizeCacheBound(t *testing.T) {
	rec := &recorder{answer: func(w http.ResponseWriter, _ map[string]any) {
		answerJSON(w, 60, map[string]any{"result": "allow"})
	}}
	srv := httptest.NewServer(rec)
	defer srv.Close()
	a, m, _, _ := newAuth(t, `{"url":"`+srv.URL+`","cache":{"maxEntries":2}}`, 0)
	id := &auth.Identity{Namespace: "default", ClientID: "c1"}
	ctx := context.Background()
	for _, topic := range []string{"t/1", "t/2", "t/3"} {
		a.Authorize(ctx, auth.Publish, topic, id)
	}
	if got := m.get("hoardmq_auth_http_cache_entries{}"); got != 2 {
		t.Errorf("cache entries %v", got)
	}
	n := rec.count()
	a.Authorize(ctx, auth.Publish, "t/3", id)
	a.Authorize(ctx, auth.Publish, "t/2", id)
	if rec.count() != n {
		t.Errorf("the newest two were asked again: %d", rec.count()-n)
	}
	a.Authorize(ctx, auth.Publish, "t/1", id)
	if rec.count() != n+1 {
		t.Error("the oldest was not dropped")
	}

	none, _, _, _ := newAuth(t, `{"url":"`+srv.URL+`","cache":{"maxEntries":0}}`, 0)
	n = rec.count()
	none.Authorize(ctx, auth.Publish, "t/1", id)
	none.Authorize(ctx, auth.Publish, "t/1", id)
	if rec.count() != n+2 {
		t.Error("maxEntries 0 held an answer")
	}
}

// TestUnavailable: an endpoint that gives no decision denies, or passes
// under onUnavailable pass; each is counted and none is held.
func TestUnavailable(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	refused := "http://" + l.Addr().String() + "/auth"
	_ = l.Close()

	var mu sync.Mutex
	asked := map[string]int{}
	allow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		answerJSON(w, 60, map[string]any{"result": "allow"})
	}))
	defer allow.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		asked[r.URL.Path]++
		mu.Unlock()
		w.Header().Set("Cache-Control", "max-age=60")
		switch r.URL.Path {
		case "/hang":
			// Read first: the server notices the client's timeout, ending
			// r.Context(), only once the body has been read.
			_, _ = io.ReadAll(r.Body)
			<-r.Context().Done()
		case "/500":
			w.WriteHeader(http.StatusInternalServerError)
		case "/text":
			_, _ = io.WriteString(w, "allow")
		case "/result":
			answerJSON(w, 60, map[string]any{"result": "maybe"})
		case "/redirect":
			http.Redirect(w, r, allow.URL, http.StatusFound)
		case "/huge":
			// An answer that reads as allow in its first 64 KiB.
			_, _ = io.WriteString(w, `{"result":"allow"}`+strings.Repeat(" ", maxResponseBytes))
		}
	}))
	defer srv.Close()

	cases := map[string]string{
		"refused": refused, "hang": srv.URL + "/hang", "500": srv.URL + "/500", "text": srv.URL + "/text",
		"result": srv.URL + "/result", "redirect": srv.URL + "/redirect", "huge": srv.URL + "/huge",
	}
	id := &auth.Identity{Namespace: "default", ClientID: "c1"}
	for name, url := range cases {
		for _, policy := range []string{"", "deny", "pass"} {
			conf := `{"url":"` + url + `","timeoutMs":100`
			if policy != "" {
				conf += `,"onUnavailable":"` + policy + `"`
			}
			a, m, log, _ := newAuth(t, conf+`}`, 0)
			want := auth.Deny
			if policy == "pass" {
				want = auth.Pass
			}
			start := time.Now()
			if r := a.Authenticate(context.Background(), connectRequest("u")); r.Decision != want {
				t.Errorf("%s %q: connect %v, want %v", name, policy, r.Decision, want)
			}
			if el := time.Since(start); el > 2*time.Second {
				t.Errorf("%s: connect took %v past a 100 ms timeout", name, el)
			}
			for range 2 {
				if d := a.Authorize(context.Background(), auth.Publish, "t", id); d != want {
					t.Errorf("%s %q: publish %v, want %v", name, policy, d, want)
				}
			}
			label := policy
			if label == "" {
				label = "deny"
			}
			if got := m.get("hoardmq_auth_http_unavailable_total{policy=" + label + "}"); got != 3 {
				t.Errorf("%s %q: unavailable counted %v, want 3 (one connect, two publishes: none held)", name, policy, got)
			}
			if !strings.Contains(log.String(), "auth.http endpoint unavailable") {
				t.Errorf("%s: no Warn", name)
			}
		}
	}
	mu.Lock()
	if asked["/redirect"] != 9 {
		t.Errorf("the redirecting endpoint was asked %d times, want 9", asked["/redirect"])
	}
	mu.Unlock()

	follow, _, _, _ := newAuth(t, `{"url":"`+srv.URL+`/redirect","followRedirects":true}`, 0)
	if d := follow.Authorize(context.Background(), auth.Publish, "t", id); d != auth.Allow {
		t.Errorf("followRedirects: %v", d)
	}
}

// TestTLSAndCredentials: the endpoint's certificate is verified against
// caFile, a client certificate is presented, the token and headers arrive,
// and no log line or metric label holds a credential.
func TestTLSAndCredentials(t *testing.T) {
	dir := t.TempDir()
	ca, err := testcert.NewCA("webhook test CA")
	if err != nil {
		t.Fatal(err)
	}
	server, err := ca.Issue("localhost", 2)
	if err != nil {
		t.Fatal(err)
	}
	client, err := ca.Issue("hoardmq", 3)
	if err != nil {
		t.Fatal(err)
	}
	caFile, err := ca.WriteCA(dir)
	if err != nil {
		t.Fatal(err)
	}
	certFile, keyFile, err := client.Write(dir, "client")
	if err != nil {
		t.Fatal(err)
	}
	tokenFile := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenFile, []byte("s3cret-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	rec := &recorder{answer: func(w http.ResponseWriter, _ map[string]any) {
		answerJSON(w, 0, map[string]any{"result": "allow"})
	}}
	srv := httptest.NewUnstartedServer(rec)
	pair, err := tlsPair(server)
	if err != nil {
		t.Fatal(err)
	}
	srv.TLS = serverTLS(pair, ca)
	srv.StartTLS()
	defer srv.Close()

	base := `{"url":"` + srv.URL + `","caFile":"` + caFile + `"`
	full := base + `,"certFile":"` + certFile + `","keyFile":"` + keyFile + `","bearerTokenFile":"` + tokenFile +
		`","headers":{"x-tenant":"tenant-header-value"}}`
	a, m, log, _ := newAuth(t, full, 0)
	if r := a.Authenticate(context.Background(), connectRequest("u")); r.Decision != auth.Allow {
		t.Fatalf("with the CA and the client certificate: %v\n%s", r.Decision, log.String())
	}
	// The secrets the logs are searched for below reached the module: the
	// endpoint received each one.
	got := rec.last()
	if got.header.Get("Authorization") != "Bearer s3cret-token" || got.header.Get("X-Tenant") != "tenant-header-value" {
		t.Errorf("headers %v", got.header)
	}
	if got.body["password"] != "hunter2-pass" {
		t.Errorf("the password did not reach the endpoint: %v", got.body)
	}

	noCert, m2, log2, _ := newAuth(t, base+`}`, 0)
	if r := noCert.Authenticate(context.Background(), connectRequest("u")); r.Decision != auth.Deny {
		t.Errorf("without the client certificate: %v", r.Decision)
	}
	noCA, m3, log3, _ := newAuth(t, `{"url":"`+srv.URL+`","certFile":"`+certFile+`","keyFile":"`+keyFile+
		`","bearerTokenFile":"`+tokenFile+`","headers":{"x-tenant":"tenant-header-value"}}`, 0)
	if r := noCA.Authenticate(context.Background(), connectRequest("u")); r.Decision != auth.Deny {
		t.Errorf("without the CA: %v", r.Decision)
	}

	// A URL whose query holds a secret, refused, logs no part of it.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	refused := "http://" + l.Addr().String() + "/auth?key=url-secret"
	_ = l.Close()
	q, m4, log4, _ := newAuth(t, `{"url":"`+refused+`","bearerTokenFile":"`+tokenFile+
		`","headers":{"x-tenant":"tenant-header-value"}}`, 0)
	q.Authenticate(context.Background(), connectRequest("u"))
	q.Authorize(context.Background(), auth.Publish, "t", &auth.Identity{ClientID: "c1"})

	for i, out := range []string{log.String() + m.keys(), log2.String() + m2.keys(), log3.String() + m3.keys(), log4.String() + m4.keys()} {
		for _, secret := range []string{"hunter2-pass", "s3cret-token", "tenant-header-value", "url-secret"} {
			if strings.Contains(out, secret) {
				t.Errorf("case %d: %q in a log line or a metric label:\n%s", i, secret, out)
			}
		}
	}
	// The refused endpoint's lines carry the transport error, so they are
	// lines a URL or a credential could have reached.
	if !strings.Contains(log4.String(), "auth.http endpoint unavailable") || !strings.Contains(log4.String(), "connection refused") {
		t.Errorf("the refused endpoint did not log its error:\n%s", log4.String())
	}
}

// TestMaxAge: cache-control's max-age, and nothing held for a zero or
// unreadable one.
func TestMaxAge(t *testing.T) {
	for h, want := range map[string]time.Duration{
		"max-age=10": 10 * time.Second, "no-cache, MAX-AGE = 7": 7 * time.Second, "max-age=0": 0,
		"max-age=abc": 0, "max-age=-5": 0, "": 0, "public": 0,
	} {
		if got := maxAge(h); got != want {
			t.Errorf("maxAge(%q) = %v, want %v", h, got, want)
		}
	}
}

// TestSuperuserOnlyFromItsOwnAdmission: the attribute makes a superuser only
// on a connection auth.http admitted; from any other source, or false or
// absent, the authorizer asks the endpoint.
func TestSuperuserOnlyFromItsOwnAdmission(t *testing.T) {
	rec := &recorder{answer: func(w http.ResponseWriter, _ map[string]any) {
		answerJSON(w, 0, map[string]any{"result": "deny"})
	}}
	srv := httptest.NewServer(rec)
	defer srv.Close()
	a, _, _, _ := newAuth(t, `{"url":"`+srv.URL+`"}`, 0)
	attr := func(v string) map[string]string { return map[string]string{SuperuserAttribute: v} }
	for _, tc := range []struct {
		name  string
		id    auth.Identity
		super bool
	}{
		{"the users file", auth.Identity{AdmittedBy: "auth.file", Attributes: attr("true")}, false},
		{"auth.db", auth.Identity{AdmittedBy: "auth.db", Attributes: attr("true")}, false},
		{"a mechanism", auth.Identity{AdmittedBy: "auth.scram", Attributes: attr("true")}, false},
		{"another module", auth.Identity{AdmittedBy: "hook.test", Attributes: attr("true")}, false},
		{"no module", auth.Identity{Attributes: attr("true")}, false},
		{"auth.http, false", auth.Identity{AdmittedBy: string(AuthID), Attributes: attr("false")}, false},
		{"auth.http, absent", auth.Identity{AdmittedBy: string(AuthID)}, false},
		{"auth.http, true", auth.Identity{AdmittedBy: string(AuthID), Attributes: attr("true")}, true},
	} {
		id := tc.id
		id.Namespace, id.ClientID, id.Username = "default", "c-"+tc.name, "u"
		if got := superuser(&id); got != tc.super {
			t.Errorf("%s: superuser %v, want %v", tc.name, got, tc.super)
		}
		n := rec.count()
		d := a.Authorize(context.Background(), auth.Publish, "s/deny", &id)
		switch {
		case tc.super && (d != auth.Allow || rec.count() != n):
			t.Errorf("%s: %v after %d requests, want allow with none", tc.name, d, rec.count()-n)
		case !tc.super && (d != auth.Deny || rec.count() != n+1):
			t.Errorf("%s: %v after %d requests, want the endpoint's deny", tc.name, d, rec.count()-n)
		}
	}
}
