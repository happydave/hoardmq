package broker

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/admin"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

// forwardTrio starts three nodes with the admin API (on every node but those
// in without), and connects c1 to n1 once n2's table names n1 as its owner.
func forwardTrio(t *testing.T, without ...string) (map[string]*fixture, *client) {
	t.Helper()
	dir := t.TempDir()
	nodes := startTrio(t, func(name string, d *config.Document) {
		off := false
		for _, w := range without {
			off = off || w == name
		}
		if !off {
			withAdmin(t, dir)(d)
		}
		d.Cluster.Shards = 8
		d.Cluster.Readiness.StableMs = 0
	})
	waitMap(t, nodes, 3)
	exp := uint32(600)
	c1, ack := nodes["n1"].connect("tcp", packet.V5, &packet.Connect{ClientID: "c1", CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("connect %s", ack.ReasonCode)
	}
	adminWait(t, "n2 to see c1's lease on n1", func() bool {
		l, ok := nodes["n2"].srv.Leases().Table().Lookup("default", "c1")
		return ok && l.Owner == "n1"
	})
	return nodes, c1
}

// forwards reads a node's count of forwards with one outcome.
func forwards(_ *testing.T, f *fixture, outcome string) float64 {
	return metric(f, "hoardmq_admin_forwards_total", map[string]string{"outcome": outcome})
}

// adminAudit is a node's audit tail through its admin API, the events
// matching action and target.
func adminAudit(t *testing.T, f *fixture, action, target string) []admin.AuditEvent {
	t.Helper()
	code, data := api(t, f, "GET", "/api/v1/audit?limit=1000", adminOps, "")
	if code != 200 {
		t.Fatalf("audit tail %d %s", code, data)
	}
	var out []admin.AuditEvent
	for _, e := range decode[[]admin.AuditEvent](t, data) {
		if e.Action == action && e.Target == target {
			out = append(out, e)
		}
	}
	return out
}

// forwardAudited asserts that the caller and the owner each recorded the
// action as the operator's token, the caller naming the owner and the owner
// the caller, under one request identifier.
func forwardAudited(t *testing.T, caller, owner *fixture, action, target, outcome string) {
	t.Helper()
	callerName, ownerName := caller.srv.config().Node.Name, owner.srv.config().Node.Name
	var request string
	for _, e := range adminAudit(t, caller, action, target) {
		if e.Actor == "token:ops" && e.Outcome == outcome && e.Detail["forwardedTo"] == ownerName {
			request = e.Detail["request"]
		}
	}
	if request == "" {
		t.Fatalf("%s holds no %s %s record forwarded to %s: %+v", callerName, action, target, ownerName, adminAudit(t, caller, action, target))
	}
	for _, e := range adminAudit(t, owner, action, target) {
		if e.Actor == "token:ops" && e.Outcome == outcome && e.Detail["forwardedFrom"] == callerName && e.Detail["request"] == request {
			return
		}
	}
	t.Fatalf("%s holds no %s %s record from %s for request %s: %+v", ownerName, action, target, callerName, request, adminAudit(t, owner, action, target))
}

// TestAdminDisconnectForwardedToTheOwner: a disconnect called on n2 closes c1
// on n1, and both nodes record it as the operator.
func TestAdminDisconnectForwardedToTheOwner(t *testing.T) {
	nodes, c1 := forwardTrio(t)
	n1, n2 := nodes["n1"], nodes["n2"]
	before := forwards(t, n2, "answered")
	code, data, hdr := apiH(t, n2, "POST", "/api/v1/clients/c1/disconnect", adminOps, "")
	if code != 200 || hdr.Get(admin.NodeHeader) != "n1" {
		t.Fatalf("n2 disconnect: %d %q %s", code, hdr.Get(admin.NodeHeader), data)
	}
	if d, ok := c1.read().(*packet.Disconnect); !ok || d.ReasonCode != packet.AdministrativeAction {
		t.Fatal("c1 did not read DISCONNECT 0x98")
	}
	forwardAudited(t, n2, n1, "client.disconnect", "c1", "ok")
	if got := forwards(t, n2, "answered"); got < before+1 {
		t.Fatalf("answered forwards %v, before %v", got, before)
	}
}

// TestAdminTraceForwardedToTheOwner: a recording started, read and stopped
// through n2 records c1's packets on n1.
func TestAdminTraceForwardedToTheOwner(t *testing.T) {
	nodes, c1 := forwardTrio(t)
	n2 := nodes["n2"]
	if code, data, hdr := apiH(t, n2, "POST", "/api/v1/traces/c1", adminOps, `{"limit":50}`); code != 200 || hdr.Get(admin.NodeHeader) != "n1" ||
		decode[admin.TraceStarted](t, data).Limit != 50 {
		t.Fatalf("n2 trace start: %d %s", code, data)
	}
	c1.send(&packet.Pingreq{})
	if _, ok := c1.read().(*packet.Pingresp); !ok {
		t.Fatal("c1 had no PINGRESP")
	}
	code, data := api(t, n2, "GET", "/api/v1/traces/c1", adminOps, "")
	if evs := decode[[]admin.TraceEvent](t, data); code != 200 || len(evs) < 2 {
		t.Fatalf("n2 trace read: %d %s", code, data)
	}
	code, data = api(t, n2, "DELETE", "/api/v1/traces/c1", adminOps, "")
	if evs := decode[[]admin.TraceEvent](t, data); code != 200 || len(evs) < 2 {
		t.Fatalf("n2 trace stop: %d %s", code, data)
	}
	if code, data := api(t, n2, "GET", "/api/v1/traces/c1", adminOps, ""); code != 200 || len(decode[[]admin.TraceEvent](t, data)) != 0 {
		t.Fatalf("the recording on n1 after the stop: %d %s", code, data)
	}
}

// failedForward asserts a disconnect called on n2 is answered 409 naming n1,
// says why, leaves c1 connected, and is counted with the outcome.
func failedForward(t *testing.T, nodes map[string]*fixture, outcome, says string) {
	t.Helper()
	n2 := nodes["n2"]
	before := forwards(t, n2, outcome)
	code, data, hdr := apiH(t, n2, "POST", "/api/v1/clients/c1/disconnect", adminOps, "")
	if p := decode[admin.Problem](t, data); code != 409 || p.Owner != "n1" || !strings.Contains(p.Detail, says) || hdr.Get(admin.NodeHeader) != "" {
		t.Fatalf("n2 disconnect, %s: %d %s", outcome, code, data)
	}
	if r, ok := nodes["n1"].srv.Sessions().Record("c1"); !ok || !r.Online {
		t.Fatal("c1 is no longer online on n1")
	}
	if got := forwards(t, n2, outcome); got < before+1 {
		t.Fatalf("%s forwards %v, before %v", outcome, got, before)
	}
}

// TestAdminForwardToAnOlderBuild: n1 at framing version 3 takes no forwarded
// request; n2 answers as before, naming n1.
func TestAdminForwardToAnOlderBuild(t *testing.T) {
	fixtureHooks = Hooks{FramingVersion: func(node string) uint8 {
		if node == "n1" {
			return 3
		}
		return 0
	}}
	t.Cleanup(func() { fixtureHooks = Hooks{} })
	nodes, _ := forwardTrio(t)
	failedForward(t, nodes, admin.ForwardUnsupported, "takes no forwarded admin requests")
}

// TestAdminForwardToANodeWithoutTheAPI: n1 has no admin API, so it runs
// nothing forwarded to it.
func TestAdminForwardToANodeWithoutTheAPI(t *testing.T) {
	nodes, _ := forwardTrio(t, "n1")
	failedForward(t, nodes, admin.ForwardAdminOff, "n1 has no admin API")
}

// TestAdminForwardThatTimesOut: an owner that does not answer in time is
// answered 504 naming it, in about the forward timeout.
func TestAdminForwardThatTimesOut(t *testing.T) {
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	fixtureHooks = Hooks{AdminForwardTimeout: 500 * time.Millisecond, AdminForwardGate: func(node string) {
		if node == "n1" {
			<-release
		}
	}}
	t.Cleanup(func() { fixtureHooks = Hooks{} })
	nodes, _ := forwardTrio(t)
	t.Cleanup(unblock)
	n2 := nodes["n2"]
	before := forwards(t, n2, admin.ForwardTimeout)
	began := time.Now()
	code, data := api(t, n2, "POST", "/api/v1/clients/c1/disconnect", adminOps, "")
	took := time.Since(began)
	if p := decode[admin.Problem](t, data); code != 504 || p.Owner != "n1" {
		t.Fatalf("a held forward: %d %s", code, data)
	}
	if took < 500*time.Millisecond || took > 3*time.Second {
		t.Fatalf("answered after %v, the forward timeout is 500ms", took)
	}
	if got := forwards(t, n2, admin.ForwardTimeout); got < before+1 {
		t.Fatalf("timeouts %v, before %v", got, before)
	}
	// n1 answers once released; nobody waits for it, so n2 drops and counts it.
	unblock()
	adminWait(t, "n2 to drop n1's late answer", func() bool {
		return metric(n2, "hoardmq_admin_forward_answers_dropped_total", map[string]string{}) >= 1
	})
}

// TestAdminForwardsAnOwnerIsBusyWith: an owner running as many forwarded
// requests as it takes answers the next one 503 at once.
func TestAdminForwardsAnOwnerIsBusyWith(t *testing.T) {
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	var held sync.WaitGroup
	held.Add(forwardsInFlight)
	var entered atomic.Int32
	fixtureHooks = Hooks{AdminForwardGate: func(node string) {
		if node == "n1" {
			if entered.Add(1) <= forwardsInFlight {
				held.Done()
			}
			<-release
		}
	}}
	t.Cleanup(func() { fixtureHooks = Hooks{} })
	nodes, _ := forwardTrio(t)
	t.Cleanup(unblock)
	n2 := nodes["n2"]
	for range forwardsInFlight {
		go func() { _, _ = admin2(t, n2) }()
	}
	held.Wait()
	before := forwards(t, n2, "busy")
	code, data := admin2(t, n2)
	if code != 503 {
		t.Fatalf("a forward beyond the %d running: %d %s", forwardsInFlight, code, data)
	}
	if got := forwards(t, n2, "busy"); got < before+1 {
		t.Fatalf("busy %v, before %v", got, before)
	}
	unblock()
}

// admin2 asks n2 for c1's detail, which n1 holds, without failing the test
// from another goroutine.
func admin2(t *testing.T, n2 *fixture) (int, string) {
	req, _ := http.NewRequest("GET", "http://"+n2.srv.AdminAddr().String()+"/api/v1/clients/c1", nil)
	req.Header.Set("Authorization", "Bearer "+adminOps)
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return 0, err.Error()
	}
	defer func() { _ = resp.Body.Close() }()
	data, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(data)
}

// TestAdminForwardAnswerTooLarge: an answer larger than the frame limit has
// run on the owner and cannot come back; the caller says so with 502.
func TestAdminForwardAnswerTooLarge(t *testing.T) {
	fixtureHooks = Hooks{AdminAnswerLimit: 64}
	t.Cleanup(func() { fixtureHooks = Hooks{} })
	nodes, _ := forwardTrio(t)
	n2 := nodes["n2"]
	before := forwards(t, n2, admin.ForwardTooLarge)
	code, data := api(t, n2, "GET", "/api/v1/clients/c1", adminOps, "")
	if p := decode[admin.Problem](t, data); code != 502 || p.Owner != "n1" || !strings.Contains(p.Detail, "ran the request") {
		t.Fatalf("a large answer: %d %s", code, data)
	}
	if got := forwards(t, n2, admin.ForwardTooLarge); got < before+1 {
		t.Fatalf("too-large %v, before %v", got, before)
	}
}

// TestAdminForwardNeedsTheScopeHere: a caller without clients:write is
// refused on n2 and nothing is sent.
func TestAdminForwardNeedsTheScopeHere(t *testing.T) {
	nodes, _ := forwardTrio(t)
	n2 := nodes["n2"]
	before := forwards(t, n2, "answered")
	if code, data := api(t, n2, "POST", "/api/v1/clients/c1/disconnect", adminReader, ""); code != 403 {
		t.Fatalf("a reader's disconnect: %d %s", code, data)
	}
	if r, ok := nodes["n1"].srv.Sessions().Record("c1"); !ok || !r.Online {
		t.Fatal("c1 is no longer online on n1")
	}
	if got := forwards(t, n2, "answered"); got != before {
		t.Fatalf("answered forwards %v, before %v", got, before)
	}
}

// TestAdminSearchesWalkOneAtATime: a second search waits while one walks the
// lease table, and goes once it is done.
func TestAdminSearchesWalkOneAtATime(t *testing.T) {
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	var walking atomic.Int32
	first := make(chan struct{})
	var once sync.Once
	fixtureHooks = Hooks{AdminSearchGate: func() {
		if walking.Add(1) == 1 {
			once.Do(func() { close(first) })
			<-release
		}
	}}
	t.Cleanup(func() { fixtureHooks = Hooks{} })
	f := start(t, withAdmin(t, t.TempDir()))
	t.Cleanup(unblock)
	done := make(chan int, 2)
	search := func() {
		code, _ := searchStatus(f)
		done <- code
	}
	go search()
	<-first
	go search()
	select {
	case code := <-done:
		t.Fatalf("a search answered (%d) while another walked the table", code)
	case <-time.After(300 * time.Millisecond):
	}
	if n := walking.Load(); n != 1 {
		t.Fatalf("%d searches reached the table while one walked it", n)
	}
	unblock()
	for range 2 {
		select {
		case code := <-done:
			if code != 200 {
				t.Fatalf("a search after the walk: %d", code)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("a search did not finish after the walk")
		}
	}
}

// searchStatus lists a node's clients without failing the test from another
// goroutine.
func searchStatus(f *fixture) (int, error) {
	req, _ := http.NewRequest("GET", "http://"+f.srv.AdminAddr().String()+"/api/v1/clients", nil)
	req.Header.Set("Authorization", "Bearer "+adminReader)
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return 0, err
	}
	_ = resp.Body.Close()
	return resp.StatusCode, nil
}

// TestAdminAnswerOnlyFromItsNode: an answer is handed to the call waiting for
// it only when it comes from the node the call was sent to.
func TestAdminAnswerOnlyFromItsNode(t *testing.T) {
	f := &adminForwarder{s: &Server{}, pending: map[uint64]pendingForward{}}
	ch := make(chan adminForwardAnswer, 1)
	f.pending[7] = pendingForward{node: "n1", ch: ch}
	body, _ := json.Marshal(adminForwardAnswer{ID: 7, Answer: admin.ForwardAnswer{Status: 200}})
	f.receiveAnswer("n3", body)
	select {
	case a := <-ch:
		t.Fatalf("n3 answered a call sent to n1: %+v", a)
	default:
	}
	f.receiveAnswer("n1", body)
	select {
	case a := <-ch:
		if a.Answer.Status != 200 {
			t.Fatalf("n1's answer: %+v", a)
		}
	default:
		t.Fatal("n1's answer was not handed to its call")
	}
}
