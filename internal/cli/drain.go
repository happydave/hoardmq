package cli

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"strings"
	"syscall"
	"time"

	"github.com/happydave/hoardmq/internal/module"
)

// drainPoll is how often -wait asks for the drain status, and
// drainRequestTimeout how long one request may take.
var (
	drainPoll           = time.Second
	drainRequestTimeout = 5 * time.Second
)

// runDrain starts a node's drain and, with -wait, polls until the drain is
// complete or the node has gone (the process exits when its drain completes,
// or up to cluster.drain.holdExitMs later, which lets a preStop hook read the
// completion). It is what a Kubernetes preStop hook runs. With -admin it goes
// through the admin API and its authentication; without, it posts to the
// observability listener's /drain, which is closed once the admin API is
// configured.
func runDrain(a *app, args []string) int {
	fs := flag.NewFlagSet("hoardmq drain", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	address := fs.String("address", "127.0.0.1:9090", "the node's observability listener")
	wait := fs.Bool("wait", false, "block until the drain is complete or the node has exited")
	timeout := fs.Duration("timeout", 5*time.Minute, "how long -wait may block")
	ac := a.adminFlags(fs, "")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fs.SetOutput(a.stdout)
			fs.PrintDefaults()
			return ExitOK
		}
		fprintf(a.stderr, "hoardmq drain: %v\n", err)
		return ExitUsage
	}
	var request func(method string) (module.DrainStatus, error)
	if *ac.address != "" {
		perRequest, text := drainRequestTimeout, false
		ac.timeout, ac.jsonOut = &perRequest, &text
		if err := a.ready(ac); err != nil {
			fprintf(a.stderr, "hoardmq drain: %v\n", err)
			return ExitProblems
		}
		request = func(method string) (module.DrainStatus, error) {
			data, err := ac.call(method, "/cluster/drain", nil, nil)
			if err != nil {
				return module.DrainStatus{}, err
			}
			var st module.DrainStatus
			if err := json.Unmarshal(data, &st); err != nil {
				return module.DrainStatus{}, fmt.Errorf("decode drain status: %w", err)
			}
			return st, nil
		}
	} else {
		base := "http://" + strings.TrimPrefix(*address, "http://")
		client := &http.Client{Timeout: drainRequestTimeout}
		request = func(method string) (module.DrainStatus, error) { return drainRequest(client, method, base+"/drain") }
	}
	st, err := request(http.MethodPost)
	if err != nil {
		fprintf(a.stderr, "hoardmq drain: %v\n", err)
		return ExitProblems
	}
	fprintf(a.stdout, "drain started: phase=%s\n", st.Phase)
	if !*wait {
		return ExitOK
	}
	deadline := time.Now().Add(*timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		time.Sleep(drainPoll)
		st, err := request(http.MethodGet)
		if err != nil {
			// A node that does not hold its exit is gone as its drain
			// completes. Any other error, a timeout included, may be a
			// node that is still draining: returning would let the
			// kubelet's SIGTERM end that drain unfinished.
			if nodeGone(err) {
				fprintln(a.stdout, "drain complete: node has exited")
				return ExitOK
			}
			fprintf(a.stderr, "hoardmq drain: %v; still waiting\n", err)
			lastErr = err
			continue
		}
		lastErr = nil
		if st.Complete {
			fprintf(a.stdout, "drain complete: leases remaining=%d\n", st.LeasesRemaining)
			return ExitOK
		}
		fprintf(a.stdout, "draining: phase=%s clients=%d leases=%d\n", st.Phase, st.ClientsRemaining, st.LeasesRemaining)
	}
	if lastErr != nil {
		fprintf(a.stderr, "hoardmq drain: timed out waiting for the drain to complete (last error: %v)\n", lastErr)
		return ExitProblems
	}
	fprintln(a.stderr, "hoardmq drain: timed out waiting for the drain to complete")
	return ExitProblems
}

// nodeGone reports whether a status request's error shows the node's process
// has gone: the connection refused, reset or closed under the request. A
// timeout does not; the node may be alive and still draining.
func nodeGone(err error) bool {
	for _, target := range []error{syscall.ECONNREFUSED, syscall.ECONNRESET, syscall.EPIPE, io.EOF, io.ErrUnexpectedEOF} {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

func drainRequest(client *http.Client, method, url string) (module.DrainStatus, error) {
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return module.DrainStatus{}, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return module.DrainStatus{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return module.DrainStatus{}, fmt.Errorf("%s %s: %s: %s", method, url, resp.Status, strings.TrimSpace(string(body)))
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return module.DrainStatus{}, fmt.Errorf("read drain status: %w", err)
	}
	var st module.DrainStatus
	if err := json.Unmarshal(data, &st); err != nil {
		return module.DrainStatus{}, fmt.Errorf("decode drain status: %w", err)
	}
	return st, nil
}
