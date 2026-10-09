// Package external runs third-party conformance suites against a node. The
// suites are not part of the module: they are fetched into tmp/ on demand,
// need python3, and take minutes, so the test runs only when
// HOARDMQ_EXTERNAL=1 is set and skips otherwise.
package external

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/conformance"
	"github.com/happydave/hoardmq/internal/config"
)

type expectation struct {
	Upstream string `json:"upstream"`
	Commit   string `json:"commit"`
	Suites   map[string]struct {
		Ran              int               `json:"ran"`
		ExpectedFailures map[string]string `json:"expectedFailures"`
	} `json:"suites"`
}

// lastRun is what the coverage generator renders under "External suites".
type lastRun struct {
	Commit string              `json:"commit"`
	Suites map[string]suiteRun `json:"suites"`
}

type suiteRun struct {
	Ran      int      `json:"ran"`
	Passed   int      `json:"passed"`
	Failures []string `json:"failures"`
}

var (
	failLine = regexp.MustCompile(`^(?:FAIL|ERROR): (\w+)`)
	ranLine  = regexp.MustCompile(`^Ran (\d+) tests?`)
)

// TestPaho runs the Eclipse Paho interoperability suites (MQTT 5.0 and
// 3.1.1) against an in-process node on localhost:1883, the only address the
// 3.1.1 suite accepts, and compares the outcome with paho/expected.json.
func TestPaho(t *testing.T) {
	if os.Getenv("HOARDMQ_EXTERNAL") == "" {
		t.Skip("set HOARDMQ_EXTERNAL=1 to run the Paho interoperability suites")
	}
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not on PATH")
	}
	data, err := os.ReadFile(filepath.Join("paho", "expected.json"))
	if err != nil {
		t.Fatal(err)
	}
	var want expectation
	if err := json.Unmarshal(data, &want); err != nil {
		t.Fatal(err)
	}
	suite := suiteDir(t, want)

	probe, err := net.Listen("tcp", "localhost:1883")
	if err != nil {
		t.Skipf("localhost:1883 not available: %v", err)
	}
	_ = probe.Close()
	conformance.StartNode(t, func(d *config.Document) {
		d.Listeners[0].Address = "localhost:1883"
		d.Limits.MaxKeepAlive = 60
	})

	run := lastRun{Commit: want.Commit, Suites: map[string]suiteRun{}}
	names := make([]string, 0, len(want.Suites))
	for name := range want.Suites {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		exp := want.Suites[name]
		args := []string{name}
		if name == "client_test5.py" {
			args = append(args, "-p", "1883")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
		cmd := exec.CommandContext(ctx, python, args...)
		cmd.Dir = filepath.Join(suite, "interoperability")
		out, _ := cmd.CombinedOutput()
		cancel()
		ran, failures := parse(out)
		if ran == 0 {
			t.Fatalf("%s: no tests ran\n%s", name, tail(out))
		}
		res := suiteRun{Ran: ran, Passed: ran - len(failures), Failures: failures}
		run.Suites[name] = res
		if ran != exp.Ran {
			t.Errorf("%s: ran %d tests, expected %d", name, ran, exp.Ran)
		}
		for _, f := range failures {
			if _, ok := exp.ExpectedFailures[f]; !ok {
				t.Errorf("%s: unexpected failure %s\n%s", name, f, tail(out))
			}
		}
		for f := range exp.ExpectedFailures {
			if !contains(failures, f) {
				t.Errorf("%s: %s passed; remove it from expected.json", name, f)
			}
		}
		t.Logf("%s: %d ran, %d passed, failures %v", name, ran, res.Passed, failures)
	}
	enc, _ := json.MarshalIndent(run, "", "  ")
	if err := os.WriteFile(filepath.Join("paho", "last-run.json"), append(enc, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}

// suiteDir returns the paho.mqtt.testing checkout, from HOARDMQ_PAHO_TESTING
// or tmp/paho.mqtt.testing under the module root, cloning it there at the
// pinned commit when absent.
func suiteDir(t *testing.T, want expectation) string {
	if dir := os.Getenv("HOARDMQ_PAHO_TESTING"); dir != "" {
		return dir
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, "tmp", "paho.mqtt.testing")
	if _, err := os.Stat(filepath.Join(dir, "interoperability")); err == nil {
		return dir
	}
	git, err := exec.LookPath("git")
	if err != nil {
		t.Skip("git not on PATH and no checkout present")
	}
	if err := os.MkdirAll(filepath.Dir(dir), 0o755); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if out, err := exec.CommandContext(ctx, git, "clone", "--quiet", want.Upstream, dir).CombinedOutput(); err != nil {
		t.Skipf("cannot clone %s: %v\n%s", want.Upstream, err, out)
	}
	checkout := exec.CommandContext(ctx, git, "checkout", "--quiet", want.Commit)
	checkout.Dir = dir
	if out, err := checkout.CombinedOutput(); err != nil {
		t.Fatalf("checkout %s: %v\n%s", want.Commit, err, out)
	}
	return dir
}

// parse extracts the unittest summary: the count of tests run and the names
// that failed or errored.
func parse(out []byte) (int, []string) {
	var ran int
	seen := map[string]bool{}
	failures := []string{}
	sc := bufio.NewScanner(bytes.NewReader(out))
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if m := failLine.FindStringSubmatch(line); m != nil && !seen[m[1]] {
			seen[m[1]] = true
			failures = append(failures, m[1])
		}
		if m := ranLine.FindStringSubmatch(line); m != nil {
			ran, _ = strconv.Atoi(m[1])
		}
	}
	sort.Strings(failures)
	return ran, failures
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

func tail(out []byte) string {
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) > 40 {
		lines = lines[len(lines)-40:]
	}
	return strings.Join(lines, "\n")
}
