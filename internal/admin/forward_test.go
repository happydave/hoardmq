package admin

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// TestForwardedToTheOwner: a client route whose client another node holds is
// sent there with the caller's principal, path and body, and the owner's
// answer is written back naming it; the caller's audit record names the
// owner. A route that acts on no client is never forwarded.
func TestForwardedToTheOwner(t *testing.T) {
	f := newFixture(t, 0)
	f.b.errFor["far"] = HeldElsewhere("far", "n3")
	f.b.forwardAns = ForwardAnswer{Status: 200, ContentType: "application/json", Body: []byte(`{"from":"n3"}`)}

	rec := f.do("POST", "/api/v1/traces/far", opsToken, `{"limit":20}`)
	if rec.Code != 200 || rec.Body.String() != `{"from":"n3"}` || rec.Header().Get(NodeHeader) != "n3" {
		t.Fatalf("forwarded trace start: %d %q %s", rec.Code, rec.Header().Get(NodeHeader), rec.Body)
	}
	if f.b.forwardCount() != 1 || f.b.forwardTo[0] != "n3" {
		t.Fatalf("forwards %v to %v", f.b.forwards, f.b.forwardTo)
	}
	req := f.b.forwards[0]
	if req.Method != "POST" || req.Path != "/traces/far" || string(req.Body) != `{"limit":20}` ||
		req.Principal.Name != "token:ops" || !strings.Contains(strings.Join(req.Principal.Scopes, ","), "traces:write") ||
		req.Request != rec.Header().Get("X-Request-Id") {
		t.Fatalf("the request sent: %+v", req)
	}
	e, _ := f.b.lastAudit(t)
	if e.Action != "trace.start" || e.Outcome != "ok" || e.Detail["forwardedTo"] != "n3" || e.Actor != "token:ops" {
		t.Fatalf("the caller's audit: %+v", e)
	}

	// A query string travels with the request.
	f.b.errFor["far"] = HeldElsewhere("far", "n3")
	f.do("GET", "/api/v1/clients/far?x=1", opsToken, "")
	if got := f.b.forwards[len(f.b.forwards)-1]; got.Path != "/clients/far" || got.Query != "x=1" || got.Method != "GET" {
		t.Fatalf("a read sent: %+v", got)
	}

	// A held-elsewhere answer naming this node itself is not forwarded.
	before := f.b.forwardCount()
	f.b.errFor["self"] = HeldElsewhere("self", "n1")
	if rec := f.do("POST", "/api/v1/clients/self/disconnect", opsToken, ""); rec.Code != 409 || f.b.forwardCount() != before {
		t.Fatalf("held by this node: %d, %d forwards", rec.Code, f.b.forwardCount()-before)
	}
}

// TestForwardFailures: a forward with no answer is answered with the owner
// named — 409 when nothing ran there, 502 when it ran and its answer could
// not come back, 504 when it may have run — and the caller's record says why.
func TestForwardFailures(t *testing.T) {
	for _, c := range []struct {
		err     error
		status  int
		outcome string
	}{
		{&ForwardError{Outcome: ForwardUnreachable, Reason: "node n3 is not connected"}, 409, ForwardUnreachable},
		{&ForwardError{Outcome: ForwardUnsupported, Reason: "node n3's build takes no forwarded requests"}, 409, ForwardUnsupported},
		{&ForwardError{Outcome: ForwardAdminOff, Reason: "node n3 has no admin API"}, 409, ForwardAdminOff},
		{&ForwardError{Outcome: ForwardTooLarge, Reason: "an answer of 20000000 bytes"}, 502, ForwardTooLarge},
		{&ForwardError{Outcome: ForwardTimeout, Reason: "no answer in 7s"}, 504, ForwardTimeout},
	} {
		f := newFixture(t, 0)
		f.b.errFor["far"] = HeldElsewhere("far", "n3")
		f.b.forwardErr = c.err
		rec := f.do("POST", "/api/v1/clients/far/disconnect", opsToken, "")
		var p Problem
		if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil || rec.Code != c.status || p.Owner != "n3" || rec.Header().Get(NodeHeader) != "" {
			t.Errorf("%s: %d %s", c.outcome, rec.Code, rec.Body)
			continue
		}
		if c.status == 409 && !strings.Contains(p.Detail, c.err.(*ForwardError).Reason) {
			t.Errorf("%s: the detail does not say why: %q", c.outcome, p.Detail)
		}
		e, _ := f.b.lastAudit(t)
		if e.Action != "client.disconnect" || e.Detail["forward"] != c.outcome || e.Detail["forwardedTo"] != "n3" {
			t.Errorf("%s: audit %+v", c.outcome, e)
		}
	}
}

// TestNotForwardedWithoutTheScope: a caller that lacks the route's scope is
// refused here, and nothing is sent.
func TestNotForwardedWithoutTheScope(t *testing.T) {
	f := newFixture(t, 0)
	f.b.errFor["far"] = HeldElsewhere("far", "n3")
	if rec := f.do("POST", "/api/v1/clients/far/disconnect", readToken, ""); rec.Code != 403 || f.b.forwardCount() != 0 {
		t.Fatalf("a reader's disconnect: %d, %d forwards", rec.Code, f.b.forwardCount())
	}
}

func forwarded(path, method string, scopes ...string) ForwardRequest {
	return ForwardRequest{Method: method, Path: path, Principal: ForwardPrincipal{Name: "token:ops", Method: "token", Scopes: scopes}, Request: "caller-7"}
}

// TestServeForwardedActsAsTheCaller: the owner runs a forwarded client route
// as the caller's principal and audits it with the calling node and the
// caller's request identifier.
func TestServeForwardedActsAsTheCaller(t *testing.T) {
	f := newFixture(t, 0)
	ans := f.srv.ServeForwarded(context.Background(), "n2", forwarded("/clients/c1/disconnect", "POST", "clients:write"))
	if ans.Status != 200 || len(f.b.disconnected) != 1 || f.b.disconnected[0] != "c1" {
		t.Fatalf("forwarded disconnect: %d %s, disconnected %v", ans.Status, ans.Body, f.b.disconnected)
	}
	e, _ := f.b.lastAudit(t)
	if e.Actor != "token:ops" || e.Action != "client.disconnect" || e.Outcome != "ok" || e.Detail["forwardedFrom"] != "n2" ||
		e.Detail["request"] != "caller-7" || e.Detail["remote"] != "n2" {
		t.Fatalf("the owner's audit: %+v", e)
	}
	body := ForwardRequest{Method: "POST", Path: "/traces/c1", Body: []byte(`{"limit":20}`),
		Principal: ForwardPrincipal{Name: "token:ops", Scopes: []string{"traces:write"}}}
	if ans := f.srv.ServeForwarded(context.Background(), "n2", body); ans.Status != 200 || !strings.Contains(string(ans.Body), `"limit": 20`) {
		t.Fatalf("a forwarded body: %d %s", ans.Status, ans.Body)
	}
}

// TestServeForwardedRefusesOtherRoutes: a forwarded request for a route that
// acts on no client runs nothing and is audited, whatever the principal
// holds.
func TestServeForwardedRefusesOtherRoutes(t *testing.T) {
	f := newFixture(t, 0)
	// Every scope the routes need, so only the route decides.
	all := []string{"apikeys:write", "cluster:drain", "modules:write", "clients:write"}
	for _, req := range []ForwardRequest{
		forwarded("/apikeys", "POST", all...),
		forwarded("/cluster/drain", "POST", all...),
		forwarded("/modules/metrics.internal/disable", "POST", all...),
		forwarded("/clients/../cluster/drain", "POST", all...),
		forwarded("clients/c1/disconnect", "POST", all...),
	} {
		before := f.b.auditCount()
		ans := f.srv.ServeForwarded(context.Background(), "n2", req)
		if ans.Status != 403 {
			t.Errorf("%s %s: %d %s", req.Method, req.Path, ans.Status, ans.Body)
		}
		if f.b.auditCount() != before+1 {
			t.Errorf("%s %s: %d audit records", req.Method, req.Path, f.b.auditCount()-before)
			continue
		}
		if e, _ := f.b.lastAudit(t); e.Action != "admin.forward.refused" || e.Outcome != "denied" || e.Actor != "token:ops" || e.Detail["forwardedFrom"] != "n2" {
			t.Errorf("%s %s: audit %+v", req.Method, req.Path, e)
		}
	}
	if f.b.drains != 0 || len(f.b.toggled) != 0 || len(f.b.disconnected) != 0 {
		t.Fatalf("a refused forward ran: drains %d, toggled %v, disconnected %v", f.b.drains, f.b.toggled, f.b.disconnected)
	}
}

// TestServeForwardedChecksTheScope: the owner checks the route's scope
// against the forwarded principal.
func TestServeForwardedChecksTheScope(t *testing.T) {
	f := newFixture(t, 0)
	ans := f.srv.ServeForwarded(context.Background(), "n2", forwarded("/clients/c1/disconnect", "POST", "clients:read"))
	if ans.Status != 403 || len(f.b.disconnected) != 0 {
		t.Fatalf("a forwarded disconnect without clients:write: %d, disconnected %v", ans.Status, f.b.disconnected)
	}
	if e, _ := f.b.lastAudit(t); e.Action != "client.disconnect" || e.Outcome != "denied" || e.Detail["forwardedFrom"] != "n2" {
		t.Fatalf("audit %+v", e)
	}
}

// TestServeForwardedIsNotForwardedAgain: a client that moved again answers
// the 409 naming its new owner; there is no second hop.
func TestServeForwardedIsNotForwardedAgain(t *testing.T) {
	f := newFixture(t, 0)
	f.b.errFor["moved"] = HeldElsewhere("moved", "n3")
	ans := f.srv.ServeForwarded(context.Background(), "n2", forwarded("/clients/moved/disconnect", "POST", "clients:write"))
	var p Problem
	if err := json.Unmarshal(ans.Body, &p); err != nil || ans.Status != 409 || p.Owner != "n3" || f.b.forwardCount() != 0 {
		t.Fatalf("a forwarded call for a client that moved: %d %s, %d forwards", ans.Status, ans.Body, f.b.forwardCount())
	}
}

// TestOpenAPIDocumentsForwardedAnswers: the forwarded routes document the
// owner's 502, 503 and 504; a route that acts on no client does not.
func TestOpenAPIDocumentsForwardedAnswers(t *testing.T) {
	f := newFixture(t, 0)
	var doc struct {
		Paths map[string]map[string]struct {
			Responses map[string]any `json:"responses"`
		} `json:"paths"`
	}
	if err := json.Unmarshal(f.do("GET", "/api/v1/openapi.json", opsToken, "").Body.Bytes(), &doc); err != nil {
		t.Fatal(err)
	}
	disconnect := doc.Paths[Prefix+"/clients/{clientId}/disconnect"]["post"].Responses
	drain := doc.Paths[Prefix+"/cluster/drain"]["post"].Responses
	for _, code := range []string{"502", "503", "504"} {
		if disconnect[code] == nil {
			t.Errorf("disconnect documents no %s", code)
		}
		if drain[code] != nil {
			t.Errorf("drain documents %s", code)
		}
	}
}

// TestForwardKeepsAnIdentifierWithASlash: a client identifier holding a
// slash reaches the owner as one path segment.
func TestForwardKeepsAnIdentifierWithASlash(t *testing.T) {
	f := newFixture(t, 0)
	f.b.errFor["gw/one"] = HeldElsewhere("gw/one", "n3")
	f.b.forwardAns = ForwardAnswer{Status: 200, ContentType: "application/json", Body: []byte(`{}`)}
	if rec := f.do("POST", "/api/v1/clients/gw%2Fone/disconnect", opsToken, ""); rec.Code != 200 {
		t.Fatalf("a slash identifier: %d %s", rec.Code, rec.Body)
	}
	sent := f.b.forwards[len(f.b.forwards)-1]
	if sent.Path != "/clients/gw%2Fone/disconnect" {
		t.Fatalf("the path sent: %q", sent.Path)
	}
	owner := newFixture(t, 0)
	sent.Principal.Scopes = []string{"clients:write"}
	if ans := owner.srv.ServeForwarded(context.Background(), "n2", sent); ans.Status != 200 || len(owner.b.disconnected) != 1 || owner.b.disconnected[0] != "gw/one" {
		t.Fatalf("the owner serving it: %d %s, disconnected %v", ans.Status, ans.Body, owner.b.disconnected)
	}
}
