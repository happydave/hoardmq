package sim

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"
)

const (
	repeatEnv      = "HOARDMQ_SIM_REPEAT"
	repeatIndexEnv = "HOARDMQ_SIM_REPEAT_INDEX"
	repeatTotalEnv = "HOARDMQ_SIM_REPEAT_TOTAL"
)

// TestFailoverRepeat runs the failover measurement N times and reports the
// distribution, because one run cannot characterise a build: rounds 13 to 16
// of 2026-09-07 were the same commit and gave 1m39.804s, 5m36.225s, and
// twice not-reached.
//
//	HOARDMQ_SIM_REPEAT=4 HOARDMQ_SIM_FAILOVER=100000 \
//	HOARDMQ_SIM_ARTIFACT=/abs/path go test ./sim -run TestFailoverRepeat -timeout 3h
//
// Each measurement is its own process with its own artifact directory, and
// is ended once its completion marker appears rather than when the process
// exits: closing a hundred thousand clients against a starved broker
// outlives the measurement, and every published round has been ended by hand
// for that reason. Between runs the driver waits for the machine to settle
// on the measurement's own condition, because a fixed sleep is what caused
// two runs of the discovery batch to be refused.
func TestFailoverRepeat(t *testing.T) {
	n, err := strconv.Atoi(os.Getenv(repeatEnv))
	if err != nil || n < 1 {
		t.Skipf("set %s=N to run the failover measurement N times", repeatEnv)
	}
	base := os.Getenv(artifactEnv)
	if base == "" {
		t.Fatalf("%s must name an absolute directory: a repeat keeps each run's artifact", artifactEnv)
	}
	if !filepath.IsAbs(base) {
		t.Fatalf("%s must be absolute (%q resolves under the sim package)", artifactEnv, base)
	}
	clients := 1000
	if v, err := strconv.Atoi(os.Getenv("HOARDMQ_SIM_FAILOVER")); err == nil && v > 0 {
		clients = v
	}

	// The remote mode adds the broker host's connection-tracking table to what
	// the next run must be under; an in-process run never fills it.
	remote := os.Getenv(clientCommandEnv) != ""

	prov := readProvenance(os.Args, os.Environ(), ".")
	runs := make([]repeatRun, 0, n)
	for i := 1; i <= n; i++ {
		if err := waitSettled(t, clients, remote, 10*time.Minute); err != nil {
			runs = append(runs, failedRun(repeatRun{Index: i}, err.Error()))
			continue
		}
		dir := filepath.Join(base, fmt.Sprintf("repeat-%02d", i))
		if err := os.MkdirAll(dir, 0o750); err != nil {
			t.Fatal(err)
		}
		t.Logf("repeat %d/%d: starting, artifacts under %s", i, n, dir)
		start := time.Now()
		runOne(t, i, n, dir)
		// The kill above skipped t.TempDir's cleanup; finishRun does it after
		// reading the figures, here between runs, where the driver is already
		// waiting for the machine to settle.
		r, note := finishRun(i, dir)
		runs = append(runs, r)
		if note != "" {
			t.Logf("repeat %d/%d: %s", i, n, note)
		}
		switch {
		case r.Failed:
			t.Logf("repeat %d/%d: FAILED after %s — %s", i, n, time.Since(start).Round(time.Second), r.Why)
		default:
			t.Logf("repeat %d/%d: %s (fill %s), after %s", i, n, r.Recovery, r.FillTime, time.Since(start).Round(time.Second))
		}
	}

	summary := summarise(runs)
	prov.TreeAtEnd = checkTree(".", prov.Tree)
	summary.Provenance = &prov
	raw, err := json.MarshalIndent(summary, "", " ")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(base, "repeat-summary.json")
	if err := os.WriteFile(path, raw, 0o640); err != nil { //nolint:gosec // operator-chosen dir
		t.Fatal(err)
	}
	t.Logf("\n%s\nwritten %s", summary.String(), path)
	if summary.FailedRuns > 0 {
		t.Fatalf("%d of %d runs produced no measurement; see repeat-summary.json", summary.FailedRuns, summary.Runs)
	}
}

// runOne starts one measurement, waits for its completion marker, and ends
// it. The child runs in its own process group so that ending it ends the
// test binary and not only the `go test` that wrapped it.
func runOne(t *testing.T, index, total int, dir string) {
	t.Helper()
	// -run is anchored so a child runs the measurement and never this
	// driver; the repeat variable is cleared for the same reason, twice over.
	cmd := exec.Command("go", "test", ".", "-run", "^TestFailover$", "-timeout", "1h", "-count=1") //nolint:gosec // fixed arguments
	cmd.Env = append(os.Environ(),
		repeatEnv+"=",
		repeatIndexEnv+"="+strconv.Itoa(index),
		repeatTotalEnv+"="+strconv.Itoa(total),
		artifactEnv+"="+dir,
	)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	out, err := os.Create(filepath.Join(dir, "run.out")) //nolint:gosec // operator-chosen dir
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = out.Close() }()
	cmd.Stdout, cmd.Stderr = out, out
	if err := cmd.Start(); err != nil {
		t.Fatalf("repeat %d: starting the measurement: %v", index, err)
	}

	done := make(chan struct{})
	go func() { _ = cmd.Wait(); close(done) }()
	defer func() {
		// Ending the child after the marker, never before: the marker
		// certifies the artifact, and a child ended earlier truncates the
		// files it was still writing.
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			<-done
		}
	}()

	deadline := time.After(70 * time.Minute)
	for {
		if inner, err := runArtifactDir(dir); err == nil {
			if _, err := os.Stat(filepath.Join(inner, completeFile)); err == nil {
				return
			}
		}
		select {
		case <-done:
			// The child is gone. Its own output says why — a refusal on the
			// settle check reads "machine is not settled" — and readRun
			// reports the missing marker.
			return
		case <-deadline:
			t.Errorf("repeat %d: no completion marker after 70m; the run is not usable", index)
			return
		case <-time.After(2 * time.Second):
		}
	}
}

// waitSettled blocks until the machine is below every count the measurement
// itself will refuse to start above. It waits on those conditions rather than
// on a sleep: twenty seconds between runs left 21,736 sockets in TIME-WAIT
// and the measurement refused, correctly.
//
// The conditions share one deadline, because the driver's bound is on getting
// the next run started, not on each thing it waits for.
func waitSettled(t *testing.T, clients int, remote bool, within time.Duration) error {
	t.Helper()
	deadline := time.Now().Add(within)
	for _, w := range settleWaits(clients, remote) {
		if err := w.wait(t, deadline, within); err != nil {
			return err
		}
	}
	return nil
}

// wait blocks until this condition's reading is at or below its limit. A
// reading the host does not expose is not a reason to wait: the starting
// condition it mirrors also proceeds without it.
func (w settleWait) wait(t *testing.T, deadline time.Time, within time.Duration) error {
	t.Helper()
	waited := time.Duration(0)
	for {
		n, ok := w.read()
		if !ok || n <= w.limit {
			if waited > 0 {
				t.Logf("settled: %d %s after %s, limit %d", n, w.what, waited.Round(time.Second), w.limit)
			}
			return nil
		}
		if waited == 0 {
			t.Logf("waiting for the machine to settle: %d %s, limit %d", n, w.what, w.limit)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("machine did not settle within %s: %d %s, limit %d", within, n, w.what, w.limit)
		}
		time.Sleep(5 * time.Second)
		waited += 5 * time.Second
	}
}
