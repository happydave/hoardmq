package cli

import (
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/broker"
	"github.com/happydave/hoardmq/internal/module"
)

// servedNode is a serve run in the test's process: the node it started, its
// stop signal, and its result once it returns.
type servedNode struct {
	srv  *broker.Server
	stop func()
	done chan servedResult
}

type servedResult struct {
	code   int
	errOut string
	at     time.Time
}

// startServe runs serve on a single-node document with an observability
// listener, extra holding further top-level members, and returns once the
// node has started and its consensus core is ready. The stop signal is
// delivered by calling stop; the test's cleanup delivers it too and waits for
// serve to return.
func startServe(t *testing.T, extra string) *servedNode {
	t.Helper()
	dir := t.TempDir()
	body := `{"version":1,"node":{"dataDir":"` + filepath.Join(dir, "data") + `"},` +
		`"listeners":[{"name":"t","type":"tcp","address":"127.0.0.1:0"}],` +
		`"observability":{"address":"127.0.0.1:0"},"modules":{"metrics.prometheus":{}},"log":{"format":"json"}`
	if extra != "" {
		body += "," + extra
	}
	doc := writeDoc(t, dir, "serve.json", body+"}")
	ctx, cancel := context.WithCancel(context.Background())
	nodes := make(chan *broker.Server, 1)
	prevNotify, prevStarted := notifyContext, started
	notifyContext = func(context.Context) (context.Context, context.CancelFunc) { return ctx, func() {} }
	started = func(s *broker.Server) { nodes <- s }
	t.Cleanup(func() { notifyContext, started = prevNotify, prevStarted })
	n := &servedNode{stop: sync.OnceFunc(cancel), done: make(chan servedResult, 1)}
	go func() {
		code, _, errOut := run(t, nil, "serve", "-config", doc)
		n.done <- servedResult{code: code, errOut: errOut, at: time.Now()}
	}()
	t.Cleanup(func() {
		select {
		case <-n.done:
		case <-time.After(10 * time.Second):
			t.Error("serve did not return within 10 s of the stop signal")
		}
	})
	// Cleanups run last first: the stop signal, then the wait above.
	t.Cleanup(n.stop)
	select {
	case n.srv = <-nodes:
	case r := <-n.done:
		t.Fatalf("serve returned before it started: %d %q", r.code, r.errOut)
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not start within 10 s")
	}
	deadline := time.Now().Add(10 * time.Second)
	for !n.srv.Consensus().Ready() {
		// serve ends only on a drain or the stop signal, and neither has come.
		if gone, log := n.returned(); gone {
			t.Fatalf("serve returned with no drain and no stop signal: %q", log)
		}
		if time.Now().After(deadline) {
			t.Fatal("the node's consensus core was not ready within 10 s")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return n
}

// wait returns serve's result, failing the test if it does not come within d.
// Once it has come, the done channel is closed for later readers.
func (n *servedNode) wait(t *testing.T, d time.Duration) servedResult {
	t.Helper()
	select {
	case r := <-n.done:
		n.done <- r
		return r
	case <-time.After(d):
		t.Fatalf("serve did not return within %s", d)
		return servedResult{}
	}
}

// returned reports whether serve has returned, and its log if so, without
// waiting.
func (n *servedNode) returned() (bool, string) {
	select {
	case r := <-n.done:
		n.done <- r
		return true, r.errOut
	default:
		return false, ""
	}
}

func getDrainStatus(t *testing.T, url, bearer string) (int, module.DrainStatus) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	var st module.DrainStatus
	if resp.StatusCode == http.StatusOK {
		if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
			t.Fatalf("GET %s: %v", url, err)
		}
	}
	return resp.StatusCode, st
}

// inOrder reports whether each of lines appears in log after the one before.
func inOrder(log string, lines ...string) bool {
	at := 0
	for _, l := range lines {
		i := strings.Index(log[at:], l)
		if i < 0 {
			return false
		}
		at += i + len(l)
	}
	return true
}

func pollEvery(t *testing.T, d time.Duration) {
	prev := drainPoll
	drainPoll = d
	t.Cleanup(func() { drainPoll = prev })
}

// The chart's preStop: a node holding its exit after the drain lets drain
// -wait read the completion while the node still serves, and exits on the
// stop signal that follows the hook.
func TestServeHoldsItsExitForTheStopSignal(t *testing.T) {
	// The waiter's own poll: its first read comes a second after its start,
	// when a node that does not hold its exit has gone.
	pollEvery(t, time.Second)
	n := startServe(t, `"cluster":{"drain":{"holdExitMs":30000}}`)
	obs := n.srv.ObservabilityAddr().String()
	code, out, errOut := run(t, nil, "drain", "-address", obs, "-wait", "-timeout", "30s")
	if code != ExitOK || !strings.Contains(out, "drain complete: leases remaining=0") {
		t.Fatalf("drain -wait did not read the completion: %d %q %q", code, out, errOut)
	}
	if status, st := getDrainStatus(t, "http://"+obs+"/drain", ""); status != http.StatusOK || !st.Complete {
		t.Fatalf("the node's status after the hook returned: %d %+v", status, st)
	}
	if gone, log := n.returned(); gone {
		t.Fatalf("serve returned before the stop signal: %q", log)
	}
	n.stop()
	r := n.wait(t, 5*time.Second)
	if r.code != ExitOK || !inOrder(r.errOut, `"drain complete; waiting for the stop signal"`, `"stop signal after the drain; exiting"`, `"shutting down"`) {
		t.Fatalf("exit %d, log %q", r.code, r.errOut)
	}
}

// With no signal the hold ends on its own, no sooner than it says.
func TestServeHoldEndsWithoutASignal(t *testing.T) {
	n := startServe(t, `"cluster":{"drain":{"holdExitMs":300}}`)
	n.srv.Drain()
	select {
	case <-n.srv.Drained():
	case <-time.After(10 * time.Second):
		t.Fatal("the drain did not complete within 10 s")
	}
	seen := time.Now()
	r := n.wait(t, 10*time.Second)
	// The test sees Drained close after it does, so allow 100 ms of the
	// 300 ms hold for that.
	if held := r.at.Sub(seen); held < 200*time.Millisecond {
		t.Errorf("serve returned %s after the drain completed, inside its 300 ms hold", held)
	}
	if r.code != ExitOK || !inOrder(r.errOut, `"drain complete; waiting for the stop signal"`, `"no stop signal within the hold; exiting"`, `"shutting down"`) {
		t.Fatalf("exit %d, log %q", r.code, r.errOut)
	}
}

// With no hold configured, serve exits as its drain completes and drain
// -wait succeeds: a bare host's behaviour.
func TestServeWithoutAHoldExitsOnItsDrain(t *testing.T) {
	pollEvery(t, 20*time.Millisecond)
	n := startServe(t, "")
	code, out, errOut := run(t, nil, "drain", "-address", n.srv.ObservabilityAddr().String(), "-wait", "-timeout", "30s")
	if code != ExitOK || !strings.Contains(out, "drain complete") {
		t.Fatalf("drain -wait: %d %q %q", code, out, errOut)
	}
	r := n.wait(t, 10*time.Second)
	if r.code != ExitOK || !strings.Contains(r.errOut, `"drain complete; exiting"`) || strings.Contains(r.errOut, "waiting for the stop signal") {
		t.Fatalf("exit %d, log %q", r.code, r.errOut)
	}
}

// A drain the stop signal started is not held: the signal has come.
func TestServeDrainOnSignalIsNotHeld(t *testing.T) {
	dir := t.TempDir()
	data := filepath.Join(dir, "data")
	prev := notifyContext
	calls := 0
	notifyContext = func(parent context.Context) (context.Context, context.CancelFunc) {
		calls++
		ctx, cancel := context.WithCancel(parent)
		if calls == 1 {
			cancel()
		}
		return ctx, cancel
	}
	t.Cleanup(func() { notifyContext = prev })
	doc := writeDoc(t, dir, "drain.json", `{"version":1,"node":{"dataDir":"`+data+`"},"listeners":[{"name":"t","type":"tcp","address":"127.0.0.1:0"}],"cluster":{"drain":{"onSignal":true,"holdExitMs":60000}},"log":{"format":"json"}}`)
	done := make(chan servedResult, 1)
	go func() {
		code, _, errOut := run(t, nil, "serve", "-config", doc)
		done <- servedResult{code: code, errOut: errOut}
	}()
	select {
	case r := <-done:
		if r.code != ExitOK || !inOrder(r.errOut, "draining before exit", `"drain complete; exiting"`) || strings.Contains(r.errOut, "waiting for the stop signal") {
			t.Fatalf("exit %d, log %q", r.code, r.errOut)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serve held a drain the stop signal started (60 s hold, 10 s bound)")
	}
}

// The chart's preStop with the admin API on: the hook drains through the API
// with its token and reads the completion; the hold opens neither door.
func TestServeHoldsItsExitThroughTheAdminAPI(t *testing.T) {
	pollEvery(t, time.Second)
	token := writeDoc(t, t.TempDir(), "drain.token", "prestop-drain-token-0123456789ab")
	n := startServe(t, `"cluster":{"drain":{"holdExitMs":30000}},"admin":{"address":"127.0.0.1:0","tokens":[{"name":"prestop-drain","tokenFile":"`+token+`","scopes":["cluster:drain","cluster:read"]}]}`)
	admin, obs := n.srv.AdminAddr().String(), n.srv.ObservabilityAddr().String()
	code, out, errOut := run(t, nil, "drain", "-admin", admin, "-token-file", token, "-wait", "-timeout", "30s")
	if code != ExitOK || !strings.Contains(out, "drain complete: leases remaining=0") {
		t.Fatalf("drain -admin -wait did not read the completion: %d %q %q", code, out, errOut)
	}
	if status, _ := getDrainStatus(t, "http://"+admin+"/api/v1/cluster/drain", ""); status != http.StatusUnauthorized {
		t.Errorf("the admin API's drain status without a token during the hold: %d", status)
	}
	if status, st := getDrainStatus(t, "http://"+admin+"/api/v1/cluster/drain", "prestop-drain-token-0123456789ab"); status != http.StatusOK || !st.Complete {
		t.Errorf("the admin API's drain status with the token during the hold: %d %+v", status, st)
	}
	if status, _ := getDrainStatus(t, "http://"+obs+"/drain", ""); status != http.StatusForbidden {
		t.Errorf("the observability listener's /drain during the hold: %d", status)
	}
	n.stop()
	if r := n.wait(t, 5*time.Second); r.code != ExitOK || !inOrder(r.errOut, `"drain complete; waiting for the stop signal"`, `"stop signal after the drain; exiting"`) {
		t.Fatalf("exit %d, log %q", r.code, r.errOut)
	}
}
