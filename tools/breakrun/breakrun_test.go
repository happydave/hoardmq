package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The throwaway module every test runs breaks against: package p with a
// function and its test, and package q whose TestMain checks a variable.
var module = map[string]string{
	"go.mod": "module example.com/m\n\ngo 1.22\n",
	"p/p.go": "package p\n\n// Add adds.\nfunc Add(a, b int) int { return a + b }\n\n// Slow waits for a file to exist.\nfunc Slow() {}\n",
	"p/p_test.go": `package p

import (
	"os"
	"testing"
	"time"
)

func TestAdd(t *testing.T) {
	if Add(1, 2) != 3 {
		t.Fatal("Add(1, 2) is not 3")
	}
}

func TestOther(t *testing.T) {
	if Add(0, 0) != 0 {
		t.Fatal("Add(0, 0) is not 0")
	}
}

func TestWaits(t *testing.T) {
	if os.Getenv("BREAKRUN_WAIT") == "" {
		return
	}
	time.Sleep(30 * time.Second)
}
`,
	"q/q.go": "package q\n\n// AtInit is set by init when broken.\nvar AtInit = 0\n",
	"q/q_test.go": `package q

import (
	"fmt"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	if AtInit != 0 {
		fmt.Println("set at init, want unset")
		os.Exit(1)
	}
	os.Exit(m.Run())
}

func TestQ(t *testing.T) {}
`,
}

// tree writes the module in a fresh git repository and returns its root.
func tree(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	for name, body := range module {
		p := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, args := range [][]string{{"init", "-q"}, {"add", "."}, {"-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "m"}} {
		cmd := exec.Command("git", args...)
		cmd.Dir = root
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	return root
}

func writeSpec(t *testing.T, root string, breaks []Break) string {
	t.Helper()
	data, err := json.Marshal(breaks)
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(root, "breaks.json")
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// inTree runs the runner in root and returns its exit code and summary.
func inTree(t *testing.T, root string, breaks []Break) (int, string) {
	t.Helper()
	spec := writeSpec(t, root, breaks)
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(root); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chdir(wd) }()
	out := filepath.Join(root, "out", "nested")
	code := run([]string{"-spec", spec, "-out", out})
	data, err := os.ReadFile(filepath.Join(out, "summary"))
	if err != nil {
		t.Fatalf("no summary (exit %d): %v", code, err)
	}
	return code, string(data)
}

// clean fails unless every file of the module holds its original bytes.
func clean(t *testing.T, root string) {
	t.Helper()
	for name, body := range module {
		got, err := os.ReadFile(filepath.Join(root, name))
		if err != nil || string(got) != body {
			t.Fatalf("%s is not restored:\n%s", name, got)
		}
	}
}

func outcome(t *testing.T, summary, id string) string {
	t.Helper()
	for _, l := range strings.Split(summary, "\n") {
		if f := strings.Fields(l); len(f) > 1 && f[0] == id {
			return f[1]
		}
	}
	t.Fatalf("no line for %s in:\n%s", id, summary)
	return ""
}

const add = "func Add(a, b int) int { return a + b }"

// TestEachOutcome: each outcome from a real go test, the output directory
// made where it did not exist, and the tree restored after every break.
func TestEachOutcome(t *testing.T) {
	root := tree(t)
	breaks := []Break{
		{ID: "caught", Edits: []Edit{{"p/p.go", add, "func Add(a, b int) int { return a - b }"}}, Pkg: "./p/", Run: "^TestAdd$", Expect: "Add(1, 2) is not 3"},
		{ID: "caught-any", Edits: []Edit{{"p/p.go", add, "func Add(a, b int) int { return a - b }"}}, Pkg: "./p/", Run: "^TestAdd$"},
		{ID: "elsewhere", Edits: []Edit{{"p/p.go", add, "func Add(a, b int) int { return a + b + 1 }"}}, Pkg: "./p/", Run: "^(TestAdd|TestOther)$", Expect: "an assertion this break does not reach"},
		{ID: "survived", Edits: []Edit{{"p/p.go", add, "func Add(a, b int) int { return b + a }"}}, Pkg: "./p/", Run: "^TestAdd$", Expect: "Add(1, 2) is not 3"},
		{ID: "nobuild", Edits: []Edit{{"p/p.go", add, "func Add(a, b int) int { return a + }"}}, Pkg: "./p/", Run: "^TestAdd$", Expect: "Add(1, 2) is not 3"},
		{ID: "notest", Edits: []Edit{{"p/p.go", add, "func Add(a, b int) int { return a - b }"}}, Pkg: "./p/", Run: "^TestNothing$", Expect: "Add(1, 2) is not 3"},
		{ID: "testmain", Edits: []Edit{{"q/q.go", "var AtInit = 0", "var AtInit = 1"}}, Pkg: "./q/", Run: "^TestQ$", Expect: "set at init, want unset"},
		{ID: "anchor", Edits: []Edit{{"p/p.go", add, "func Add(a, b int) int { return a * b }"}, {"p/p_test.go", "no such text", "x"}}, Pkg: "./p/", Run: "^TestAdd$"},
	}
	code, summary := inTree(t, root, breaks)
	for id, want := range map[string]string{
		"caught": Caught, "caught-any": Caught, "elsewhere": FailedElsewhere, "survived": Survived,
		"nobuild": DidNotBuild, "notest": RanNoTest, "testmain": Caught, "anchor": AnchorMiss,
	} {
		if got := outcome(t, summary, id); got != want {
			t.Errorf("%s: %s, want %s\n%s", id, got, want, summary)
		}
	}
	if code != 0 {
		t.Errorf("exit %d", code)
	}
	clean(t, root)
}

// TestRefusesADirtyTree: a spec'd file already changed (a killed run's
// leftover) is refused before any break runs.
func TestRefusesADirtyTree(t *testing.T) {
	root := tree(t)
	if err := os.WriteFile(filepath.Join(root, "p", "p.go"), []byte("package p\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	spec := writeSpec(t, root, []Break{{ID: "b", Edits: []Edit{{"p/p.go", "package p", "package p"}}, Pkg: "./p/", Run: "."}})
	wd, _ := os.Getwd()
	if err := os.Chdir(root); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chdir(wd) }()
	if code := run([]string{"-spec", spec, "-out", filepath.Join(root, "out")}); code != 2 {
		t.Fatalf("exit %d, want 2", code)
	}
}

// TestRestoredOnSIGTERM: the runner sent SIGTERM while a break's test runs
// restores the tree before it exits.
func TestRestoredOnSIGTERM(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "breakrun")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	root := tree(t)
	spec := writeSpec(t, root, []Break{{ID: "slow", Edits: []Edit{{"p/p.go", add, "func Add(a, b int) int { return a - b }"}}, Pkg: "./p/", Run: "^TestWaits$"}})
	cmd := exec.Command(bin, "-spec", spec, "-out", filepath.Join(root, "out"))
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "BREAKRUN_WAIT=1")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	pid := cmd.Process.Pid
	// Wait until the break is applied, then signal the process this test
	// started.
	deadline := time.Now().Add(60 * time.Second)
	for {
		b, _ := os.ReadFile(filepath.Join(root, "p", "p.go"))
		if strings.Contains(string(b), "a - b") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the break was never applied")
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond)
	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 130 {
			t.Fatalf("exit %v, want 130", err)
		}
	case <-time.After(30 * time.Second):
		_ = syscall.Kill(pid, syscall.SIGKILL)
		t.Fatal("the runner did not exit on SIGTERM")
	}
	clean(t, root)
}

// TestClassify: the decision over logs.
func TestClassify(t *testing.T) {
	for _, c := range []struct {
		exit        int
		log, expect string
		want        string
	}{
		{1, "FAIL\tm/p [build failed]", "x", DidNotBuild},
		{1, "FAIL\tm/p [setup failed]", "", DidNotBuild},
		{1, "=== RUN TestA\n--- FAIL: TestA\nthe message", "the message", Caught},
		{1, "set at init, want unset\nFAIL\tm/q", "set at init", Caught},
		{1, "=== RUN TestA\n--- FAIL: TestA\nanother", "the message", FailedElsewhere},
		{1, "=== RUN TestA\n--- FAIL: TestA", "", Caught},
		{1, "panic: boom", "", FailedElsewhere},
		{0, "ok\tm/p\t0.1s [no tests to run]", "x", RanNoTest},
		{0, "?\tm/p\t[no test files]", "", RanNoTest},
		{0, "=== RUN TestA\n--- PASS: TestA", "x", Survived},
	} {
		if got := Classify(c.exit, c.log, c.expect); got != c.want {
			t.Errorf("Classify(%d, %q, %q) = %s, want %s", c.exit, c.log, c.expect, got, c.want)
		}
	}
}
