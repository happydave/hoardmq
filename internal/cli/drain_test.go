package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/module"
)

func TestDrainVerb(t *testing.T) {
	drainPoll = 20 * time.Millisecond
	var posts, gets atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/drain" {
			http.NotFound(w, r)
			return
		}
		st := module.DrainStatus{Draining: true, Phase: "redirecting-clients"}
		switch r.Method {
		case http.MethodPost:
			posts.Add(1)
			w.WriteHeader(http.StatusAccepted)
		case http.MethodGet:
			if gets.Add(1) >= 3 {
				st.Complete, st.Phase = true, "complete"
			}
		}
		_ = json.NewEncoder(w).Encode(st)
	}))
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")
	code, out, errOut := run(t, nil, "drain", "-address", addr)
	if code != ExitOK || posts.Load() != 1 || !strings.Contains(out, "drain started") {
		t.Fatalf("drain: %d %q %q", code, out, errOut)
	}
	code, out, errOut = run(t, nil, "drain", "-address", addr, "-wait")
	if code != ExitOK || !strings.Contains(out, "drain complete") || gets.Load() < 3 {
		t.Fatalf("drain -wait: %d %q %q", code, out, errOut)
	}
	// The node exits when its drain completes: a vanished endpoint after the
	// start is success. Each gone case has a short -timeout, so a misread
	// ending fails here rather than polling for five minutes.
	gets.Store(0)
	gone := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(module.DrainStatus{Draining: true})
			return
		}
		panic(http.ErrAbortHandler)
	}))
	goneAddr := strings.TrimPrefix(gone.URL, "http://")
	go func() {
		time.Sleep(50 * time.Millisecond)
		gone.Close()
	}()
	code, out, errOut = run(t, nil, "drain", "-address", goneAddr, "-wait", "-timeout", "5s")
	if code != ExitOK || !strings.Contains(out, "node has exited") {
		t.Fatalf("drain -wait with the node gone: %d %q %q", code, out, errOut)
	}
	// Through the admin API the same: a node gone after the start is success.
	adminGone := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/cluster/drain" || r.Header.Get("Authorization") != "Bearer drain-token-0123456789" {
			http.Error(w, "unexpected", http.StatusTeapot)
			return
		}
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(module.DrainStatus{Draining: true})
			return
		}
		panic(http.ErrAbortHandler)
	}))
	adminAddr := strings.TrimPrefix(adminGone.URL, "http://")
	go func() {
		time.Sleep(50 * time.Millisecond)
		adminGone.Close()
	}()
	code, out, errOut = run(t, []string{"HOARDMQ_ADMIN_TOKEN=drain-token-0123456789"}, "drain", "-admin", adminAddr, "-wait", "-timeout", "5s")
	if code != ExitOK || !strings.Contains(out, "node has exited") {
		t.Fatalf("drain -admin -wait with the node gone: %d %q %q", code, out, errOut)
	}
	// A node that resets the connection as it exits is gone too, whatever the
	// error's wording.
	reset := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(module.DrainStatus{Draining: true})
			return
		}
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		if tc, ok := conn.(*net.TCPConn); ok {
			_ = tc.SetLinger(0)
		}
		_ = conn.Close()
	}))
	defer reset.Close()
	code, out, errOut = run(t, []string{"HOARDMQ_ADMIN_TOKEN=drain-token-0123456789"}, "drain", "-admin", strings.TrimPrefix(reset.URL, "http://"), "-wait", "-timeout", "5s")
	if code != ExitOK || !strings.Contains(out, "node has exited") {
		t.Fatalf("drain -admin -wait with the connection reset: %d %q %q", code, out, errOut)
	}
	// A drain that does not complete within -timeout fails the waiter, so a
	// preStop hook reports it.
	never := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusAccepted)
		}
		_ = json.NewEncoder(w).Encode(module.DrainStatus{Draining: true, Phase: "awaiting-takeovers", LeasesRemaining: 1})
	}))
	defer never.Close()
	code, out, errOut = run(t, nil, "drain", "-address", strings.TrimPrefix(never.URL, "http://"), "-wait", "-timeout", "300ms")
	if code != ExitProblems || !strings.Contains(errOut, "timed out") || strings.Contains(out, "drain complete") {
		t.Fatalf("drain -wait on a drain that does not complete: %d %q %q", code, out, errOut)
	}
	// An unreachable node is a problem.
	if code, _, errOut := run(t, nil, "drain", "-address", "127.0.0.1:1"); code != ExitProblems || errOut == "" {
		t.Fatalf("unreachable: %d %q", code, errOut)
	}
	if code, out, _ := run(t, nil, "drain", "-h"); code != ExitOK || !strings.Contains(out, "-wait") {
		t.Fatalf("help: %d %q", code, out)
	}
}

// slowDrain lowers the waiter's poll and per-request timeout for one test.
func slowDrain(t *testing.T) {
	t.Helper()
	poll, perRequest := drainPoll, drainRequestTimeout
	drainPoll, drainRequestTimeout = 10*time.Millisecond, 100*time.Millisecond
	t.Cleanup(func() { drainPoll, drainRequestTimeout = poll, perRequest })
}

// drainServer answers a drain's POST, and each GET with answer(n), n counting
// the GETs from 1. It serves both the observability and the admin path.
func drainServer(t *testing.T, answer func(n int32, w http.ResponseWriter, r *http.Request)) (addr string, gets *atomic.Int32) {
	t.Helper()
	gets = new(atomic.Int32)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/drain" && r.URL.Path != "/api/v1/cluster/drain" {
			http.NotFound(w, r)
			return
		}
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(module.DrainStatus{Draining: true, Phase: "redirecting-clients"})
			return
		}
		answer(gets.Add(1), w, r)
	}))
	t.Cleanup(srv.Close)
	return strings.TrimPrefix(srv.URL, "http://"), gets
}

// hang holds a request past the waiter's per-request timeout. It returns when
// the client gives up or the test ends; call it after drainServer, so its
// release runs before the server closes.
func hang(t *testing.T) func(r *http.Request) {
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	return func(r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}
}

func drainWait(t *testing.T, admin bool, addr string, timeout string) (int, string, string) {
	t.Helper()
	if admin {
		return run(t, []string{"HOARDMQ_ADMIN_TOKEN=drain-token-0123456789"}, "drain", "-admin", addr, "-wait", "-timeout", timeout)
	}
	return run(t, nil, "drain", "-address", addr, "-wait", "-timeout", timeout)
}

// TestDrainWaitTimeoutIsNotTheExit: a node that stops answering mid-drain has
// not been seen to exit, so the waiter must not report the drain complete.
func TestDrainWaitTimeoutIsNotTheExit(t *testing.T) {
	slowDrain(t)
	for _, admin := range []bool{true, false} {
		var hold func(*http.Request)
		addr, gets := drainServer(t, func(_ int32, _ http.ResponseWriter, r *http.Request) { hold(r) })
		hold = hang(t)
		code, out, errOut := drainWait(t, admin, addr, "600ms")
		if gets.Load() < 1 {
			t.Fatalf("admin=%v: the waiter never asked for the status", admin)
		}
		if code != ExitProblems || strings.Contains(out, "drain complete") || !strings.Contains(errOut, "timed out waiting") || !strings.Contains(errOut, "Client.Timeout") {
			t.Fatalf("admin=%v: a hung node read as done: %d %q %q", admin, code, out, errOut)
		}
	}
}

// TestDrainWaitTimeoutNamesTheLastErrorOnly: an error the node answered
// after is not the reason a drain timed out.
func TestDrainWaitTimeoutNamesTheLastErrorOnly(t *testing.T) {
	slowDrain(t)
	addr, gets := drainServer(t, func(n int32, w http.ResponseWriter, _ *http.Request) {
		if n == 1 {
			http.Error(w, "busy", http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(module.DrainStatus{Draining: true, Phase: "awaiting-takeovers", LeasesRemaining: 1})
	})
	code, out, errOut := drainWait(t, false, addr, "300ms")
	if gets.Load() < 2 || !strings.Contains(errOut, "still waiting") {
		t.Fatalf("the 503 and a later status were not both seen: %d %q %q", code, out, errOut)
	}
	if code != ExitProblems || !strings.Contains(errOut, "timed out waiting") || strings.Contains(errOut, "last error") {
		t.Fatalf("the timeout named an error the node answered after: %d %q %q", code, out, errOut)
	}
}

// TestDrainWaitOutlivesAStall: an error that does not show the node gone
// leaves the waiter polling, so a drain that completes after it reads as
// complete.
func TestDrainWaitOutlivesAStall(t *testing.T) {
	slowDrain(t)
	stalls := map[string]func(t *testing.T) func(w http.ResponseWriter, r *http.Request){
		"timeout": func(t *testing.T) func(w http.ResponseWriter, r *http.Request) {
			hold := hang(t)
			return func(_ http.ResponseWriter, r *http.Request) { hold(r) }
		},
		"503": func(*testing.T) func(w http.ResponseWriter, r *http.Request) {
			return func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "busy", http.StatusServiceUnavailable) }
		},
		"empty body": func(*testing.T) func(w http.ResponseWriter, r *http.Request) {
			return func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }
		},
	}
	for name, stall := range stalls {
		for _, admin := range []bool{true, false} {
			var first func(w http.ResponseWriter, r *http.Request)
			addr, _ := drainServer(t, func(n int32, w http.ResponseWriter, r *http.Request) {
				if n == 1 {
					first(w, r)
					return
				}
				_ = json.NewEncoder(w).Encode(module.DrainStatus{Draining: true, Complete: true, Phase: "complete"})
			})
			first = stall(t)
			code, out, errOut := drainWait(t, admin, addr, "5s")
			if !strings.Contains(errOut, "still waiting") {
				t.Fatalf("%s, admin=%v: the stall was not seen: %d %q %q", name, admin, code, out, errOut)
			}
			if code != ExitOK || !strings.Contains(out, "drain complete: leases remaining=0") {
				t.Fatalf("%s, admin=%v: the drain did not read as complete after the stall: %d %q %q", name, admin, code, out, errOut)
			}
		}
	}
}

func TestDrainNodeGone(t *testing.T) {
	sys := func(op string, errno syscall.Errno) error {
		return &url.Error{Op: "Get", URL: "http://node/drain", Err: &net.OpError{Op: op, Net: "tcp", Err: os.NewSyscallError(op, errno)}}
	}
	inAPI := func(err error) error { return &apiError{network: true, detail: err.Error(), err: err} }
	syntax := json.Unmarshal(nil, new(module.DrainStatus))
	timeout := &url.Error{Op: "Get", URL: "http://node/drain", Err: context.DeadlineExceeded}
	dialTimeout := &url.Error{Op: "Get", URL: "http://node/drain", Err: &net.OpError{Op: "dial", Net: "tcp", Err: os.ErrDeadlineExceeded}}
	cases := []struct {
		name string
		err  error
		gone bool
	}{
		{"refused", sys("connect", syscall.ECONNREFUSED), true},
		{"reset", sys("read", syscall.ECONNRESET), true},
		{"broken pipe", sys("write", syscall.EPIPE), true},
		{"closed before the answer", &url.Error{Op: "Get", URL: "http://node/drain", Err: io.EOF}, true},
		{"closed mid-body", fmt.Errorf("read drain status: %w", io.ErrUnexpectedEOF), true},
		{"admin: refused", inAPI(sys("connect", syscall.ECONNREFUSED)), true},
		{"admin: reset", inAPI(sys("read", syscall.ECONNRESET)), true},
		{"admin: broken pipe", inAPI(sys("write", syscall.EPIPE)), true},
		{"admin: closed before the answer", inAPI(&url.Error{Op: "Get", URL: "http://node/drain", Err: io.EOF}), true},
		{"admin: closed mid-body", inAPI(io.ErrUnexpectedEOF), true},
		{"timeout awaiting the answer", timeout, false},
		{"dial timeout", dialTimeout, false},
		{"admin: timeout", inAPI(timeout), false},
		{"admin: dial timeout", inAPI(dialTimeout), false},
		{"admin: 503", &apiError{status: "503 Service Unavailable", code: http.StatusServiceUnavailable}, false},
		{"empty body", fmt.Errorf("decode drain status: %w", syntax), false},
		{"words only", errors.New("Get http://node/drain: EOF after connection refused"), false},
	}
	for _, c := range cases {
		if got := nodeGone(c.err); got != c.gone {
			t.Errorf("%s: nodeGone(%v) = %v, want %v", c.name, c.err, got, c.gone)
		}
	}
}
