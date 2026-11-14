package sim

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// testSleep is a sleep the test starts, observed as a node's recorded process.
type testSleep struct {
	cmd  *exec.Cmd
	rec  procRecord
	done chan struct{}
}

func startTestSleep(t *testing.T) *testSleep {
	t.Helper()
	cmd := exec.Command("sleep", "60")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	s := &testSleep{cmd: cmd, done: make(chan struct{})}
	go func() { _ = cmd.Wait(); close(s.done) }()
	t.Cleanup(func() { _ = cmd.Process.Kill(); <-s.done })
	rec, alive, err := observeProc(context.Background(), localShell{}, cmd.Process.Pid)
	if err != nil || !alive || rec.Exe == "" {
		t.Fatalf("observing the sleep: %+v, alive %v, %v", rec, alive, err)
	}
	s.rec = rec
	return s
}

// end kills the sleep and waits until it is reaped.
func (s *testSleep) end(t *testing.T) {
	t.Helper()
	_ = s.cmd.Process.Kill()
	select {
	case <-s.done:
	case <-time.After(5 * time.Second):
		t.Fatal("the sleep was not reaped")
	}
}

// sleepNode is a node whose recorded process is the sleep, with a log in
// its run directory under root.
func sleepNode(t *testing.T, name, root, runID string, s *testSleep) *nodeProc {
	t.Helper()
	dir := filepath.Join(root, runID, name)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "node.log"), []byte("the last line of "+name+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return &nodeProc{node: scaleNode{Name: name, Host: "local", RunRoot: root}, sh: localShell{}, dir: dir, rec: s.rec}
}

// TestCheckNodesNamesAGoneNode: after a phase, a node whose process has gone
// fails the run, named with the end of its log; so does one whose PID now
// runs another start; the victim between its kill and its restart does not.
func TestCheckNodesNamesAGoneNode(t *testing.T) {
	root := filepath.Join(t.TempDir(), "hoardmq-scale", "runs")
	id := "scale-20261113-000000-a1b2c3"
	n1, n2, n3, n4 := startTestSleep(t), startTestSleep(t), startTestSleep(t), startTestSleep(t)
	run := func() *scaleRun {
		r := &scaleRun{rec: &scaleRecord{}, logf: t.Logf, procs: map[string]*nodeProc{
			"n1": sleepNode(t, "n1", root, id, n1), "n2": sleepNode(t, "n2", root, id, n2),
			"n3": sleepNode(t, "n3", root, id, n3), "n4": sleepNode(t, "n4", root, id, n4)}}
		r.procs["n3"].down = true
		return r
	}

	r := run()
	r.checkNodes(context.Background())
	if len(r.failed) != 0 {
		t.Fatalf("every node running: failed %q", r.failed)
	}

	n2.end(t)
	n3.end(t)
	r = run()
	r.procs["n4"].rec.StartTicks++
	r.checkNodes(context.Background())
	got := strings.Join(r.failed, "\n")
	for _, want := range []string{"n2 exited outside the crash", "the last line of n2", "n4 exited outside the crash"} {
		if !strings.Contains(got, want) {
			t.Errorf("failures %q lack %q", got, want)
		}
	}
	if len(r.failed) != 2 || strings.Contains(got, "n1 ") || strings.Contains(got, "n3 ") {
		t.Errorf("failures %q, want n2 and n4 alone: n1 runs, and n3 is down for the crash", got)
	}
}

// TestTeardownNamesOnlyTheNodesItStopped: teardown records as stopped the
// node it signalled, and as not running the one whose process had gone,
// then removes the run's directory under the test's temporary root.
func TestTeardownNamesOnlyTheNodesItStopped(t *testing.T) {
	root := filepath.Join(t.TempDir(), "hoardmq-scale", "runs")
	id := "scale-20261113-000000-a1b2c3"
	n1, n2 := startTestSleep(t), startTestSleep(t)
	r := &scaleRun{rec: &scaleRecord{RunID: id}, logf: t.Logf, samplers: map[string]*hostSampler{}, procs: map[string]*nodeProc{
		"n1": sleepNode(t, "n1", root, id, n1), "n2": sleepNode(t, "n2", root, id, n2)}}
	n2.end(t)
	r.teardown()
	td := r.rec.Teardown
	if strings.Join(td.Stopped, ",") != "n1" || strings.Join(td.NotRunning, ",") != "n2" {
		t.Fatalf("stopped %v, not running %v; want n1 stopped and n2 not running", td.Stopped, td.NotRunning)
	}
	select {
	case <-n1.done:
	case <-time.After(5 * time.Second):
		t.Fatal("n1's sleep still runs after teardown")
	}
	if _, err := os.Stat(filepath.Join(root, id)); !os.IsNotExist(err) {
		t.Fatalf("the run directory survived teardown: %v", err)
	}
}

// TestScaleRefusedRunStartsNothing: a run whose host facts break a starting
// condition (a node's port in use) reports the refusal and writes its record,
// and writes no configuration, makes no run directory and starts no phase.
func TestScaleRefusedRunStartsNothing(t *testing.T) {
	if _, err := exec.LookPath("ss"); err != nil {
		t.Skip("the host facts read listeners with ss, which this host lacks")
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	held := l.Addr().(*net.TCPAddr).Port
	dir := t.TempDir()
	root := filepath.Join(dir, "hoardmq-scale", "runs")
	n := func(name, addr string, client int) scaleNode {
		return scaleNode{Name: name, Host: "local", Cores: "0", Address: addr, ClientPort: client, ClusterPort: 2, ObsPort: 3,
			Binary: filepath.Join(dir, "bin", "hoardmq"), RunRoot: root}
	}
	cfg := scaleConfig{Clients: 10, Victim: "n2", Layout: scaleLayout{
		Nodes:  []scaleNode{n("n1", "127.0.0.1", held), n("n2", "127.0.0.2", 1)},
		Driver: scaleDriver{Binary: filepath.Join(dir, "bin", "simclients"), Cores: "0", Sources: []string{"127.0.0.1"}}}}
	artifact := filepath.Join(dir, "artifact")
	rec, err := runScale(context.Background(), cfg, Provenance{}, artifact, t.Logf)
	if !errors.Is(err, errScaleRefused) {
		t.Fatalf("err %v, want the refusal", err)
	}
	if got := strings.Join(rec.Refused, "; "); !strings.Contains(got, fmt.Sprintf("port %d is in use", held)) {
		t.Errorf("refused %q, want it to name port %d in use", got, held)
	}
	if len(rec.Phases) != 0 {
		t.Errorf("phases %+v ran after a refusal", rec.Phases)
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Errorf("the run root was made after a refusal: %v", err)
	}
	report, err := os.ReadFile(filepath.Join(artifact, "report.md"))
	if err != nil || !strings.Contains(string(report), "Refused to start") || !strings.Contains(string(report), strconv.Itoa(held)) {
		t.Errorf("report %q, %v; want the refusal", report, err)
	}
	if _, err := os.Stat(filepath.Join(artifact, "complete")); err != nil {
		t.Errorf("no complete marker: %v", err)
	}
}
