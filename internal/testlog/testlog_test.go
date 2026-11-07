package testlog

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fakeT stands in for a test whose outcome the test sets, its cleanups run
// by finish in the order testing runs them.
type fakeT struct {
	name     string
	failed   bool
	cleanups []func()
	logs     []string
}

func (f *fakeT) Name() string      { return f.name }
func (f *fakeT) Helper()           {}
func (f *fakeT) Cleanup(fn func()) { f.cleanups = append(f.cleanups, fn) }
func (f *fakeT) Failed() bool      { return f.failed }
func (f *fakeT) Logf(format string, args ...any) {
	f.logs = append(f.logs, fmt.Sprintf(format, args...))
}
func (f *fakeT) finish() {
	for i := len(f.cleanups) - 1; i >= 0; i-- {
		f.cleanups[i]()
	}
	f.cleanups = nil
}

// tmp points the package at a fresh directory and returns it.
func tmp(t *testing.T) string {
	dir := t.TempDir()
	t.Setenv("GOTMPDIR", dir)
	return dir
}

func entries(t *testing.T, dir string) []string {
	t.Helper()
	es, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range es {
		names = append(names, e.Name())
	}
	return names
}

func read(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// TestAPassingTestsLogIsRemoved: the file holds the nodes' lines while the
// test runs and is gone once its cleanups have run.
func TestAPassingTestsLogIsRemoved(t *testing.T) {
	dir := tmp(t)
	ft := &fakeT{name: "TestCluster/sub case"}
	Logger(ft, "n1").Debug("adopting", "client", "x0")
	names := entries(t, dir)
	if len(names) != 1 || !strings.HasPrefix(names[0], "hoardmq-nodes-TestCluster_sub_case-") || !strings.HasSuffix(names[0], ".log") {
		t.Fatalf("files while the test runs: %q", names)
	}
	if got := read(t, filepath.Join(dir, names[0])); !strings.Contains(got, "node=n1") || !strings.Contains(got, "adopting") || !strings.Contains(got, "level=DEBUG") {
		t.Fatalf("the file holds %q, want the node's debug line", got)
	}
	ft.finish()
	if names := entries(t, dir); len(names) != 0 {
		t.Fatalf("a passing test left %q", names)
	}
	for _, l := range ft.logs {
		if strings.Contains(l, "kept") {
			t.Fatalf("a passing test logged %q", l)
		}
	}
}

// TestAFailingTestsLogIsKeptAndNamed: a failed test keeps the file with
// every node's lines, and names it by its absolute path.
func TestAFailingTestsLogIsKeptAndNamed(t *testing.T) {
	dir := tmp(t)
	ft := &fakeT{name: "TestCluster"}
	Logger(ft, "n1").Info("first")
	Logger(ft, "n2").Warn("second")
	ft.failed = true
	ft.finish()
	names := entries(t, dir)
	if len(names) != 1 {
		t.Fatalf("files after a failed test: %q", names)
	}
	path := filepath.Join(dir, names[0])
	if len(ft.logs) != 1 || ft.logs[0] != "node logs kept: "+path {
		t.Fatalf("logged %q, want the kept file's path %s", ft.logs, path)
	}
	got := read(t, path)
	if !strings.Contains(got, "node=n1") || !strings.Contains(got, "first") || !strings.Contains(got, "node=n2") || !strings.Contains(got, "second") {
		t.Fatalf("the kept file holds %q, want both nodes' lines", got)
	}
}

// TestOneFileATest: every node of one test writes to one file; another test
// gets its own.
func TestOneFileATest(t *testing.T) {
	dir := tmp(t)
	a, b := &fakeT{name: "TestA"}, &fakeT{name: "TestB"}
	if Writer(a) != Writer(a) {
		t.Fatal("two calls for one test gave two writers")
	}
	if Writer(a) == Writer(b) {
		t.Fatal("two tests share a writer")
	}
	if names := entries(t, dir); len(names) != 2 {
		t.Fatalf("files for two tests: %q", names)
	}
	a.finish()
	b.finish()
}

// TestAFinishedTestIsForgotten: once a test's cleanups have run its writer is
// no longer handed out, so a test that ends holds no file open.
func TestAFinishedTestIsForgotten(t *testing.T) {
	tmp(t)
	ft := &fakeT{name: "TestOnce"}
	first := Writer(ft)
	ft.finish()
	mu.Lock()
	_, held := files[ft]
	mu.Unlock()
	if held {
		t.Fatal("the finished test's writer is still held")
	}
	if Writer(ft) == first {
		t.Fatal("a closed writer was handed out again")
	}
	ft.finish()
}

// TestAnUncreatableFileDiscards: with nowhere to write, the nodes' logs are
// discarded and the reason logged. T has no way to fail the test.
func TestAnUncreatableFileDiscards(t *testing.T) {
	t.Setenv("GOTMPDIR", filepath.Join(t.TempDir(), "missing"))
	ft := &fakeT{name: "TestNowhere"}
	if w := Writer(ft); w != io.Discard {
		t.Fatalf("writer %T, want io.Discard", w)
	}
	if len(ft.logs) != 1 || !strings.HasPrefix(ft.logs[0], "node logs discarded: ") {
		t.Fatalf("logged %q", ft.logs)
	}
	ft.finish()
}

func TestFileName(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{"TestA", "TestA"},
		{"TestA/sub-1", "TestA_sub-1"},
		{"TestA/a b*?", "TestA_a_b__"},
		{strings.Repeat("x", 80), strings.Repeat("x", 64)},
	} {
		if got := fileName(c.in); got != c.want {
			t.Errorf("fileName(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
