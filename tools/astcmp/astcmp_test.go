package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const base = "package p\n\n// F returns x, see the tracker.\nfunc F(x int) int {\n\t// why\n\treturn x + 1\n}\n"

// Each change to a whole file and whether only its comments changed.
func TestSame(t *testing.T) {
	for _, c := range []struct {
		name, before, after string
		want                bool
	}{
		{"comments only", base, "package p\n\n// F returns x, the reason stated.\nfunc F(x int) int {\n\treturn x + 1 // why, moved\n}\n", true},
		{"one token", base, "package p\n\n// F returns x, see the tracker.\nfunc F(x int) int {\n\t// why\n\treturn x + 2\n}\n", false},
		{"one identifier", base, "package p\n\n// F returns x, see the tracker.\nfunc F(y int) int {\n\t// why\n\treturn y + 1\n}\n", false},
		{"a string literal", "package p\n\nvar s = \"first\"\n", "package p\n\nvar s = \"second\"\n", false},
		{"a directive", base, "package p\n\n// F returns x, see the tracker.\n//\n//go:noinline\nfunc F(x int) int {\n\t// why\n\treturn x + 1\n}\n", false},
		{"a nolint", base, "package p\n\n// F returns x, see the tracker.\nfunc F(x int) int {\n\t// why\n\treturn x + 1 //nolint:gosec\n}\n", false},
	} {
		got, err := sameMasked("p.go", []byte(c.before), []byte(c.after), nil, nil)
		if err != nil || got != c.want {
			t.Errorf("%s: same=%v err=%v, want same=%v", c.name, got, err, c.want)
		}
	}
}

// Literals on the named lines may differ, and must; nothing else may.
func TestSameMasked(t *testing.T) {
	for _, c := range []struct {
		name, before, after string
		want, wantErr       bool
	}{
		{"a named literal", "package p\n\nvar s = \"old reason\"\n", "package p\n\nvar s = \"the reason\"\n", true, false},
		{"an unnamed literal", "package p\n\nvar s = \"old reason\"\nvar t = \"x\"\n", "package p\n\nvar s = \"the reason\"\nvar t = \"y\"\n", false, false},
		{"a named literal and a token", "package p\n\nvar s = \"old reason\"\nvar n = 1\n", "package p\n\nvar s = \"the reason\"\nvar n = 2\n", false, false},
		{"a named literal that did not change", "package p\n\nvar s = \"x\"\n", "package p\n\nvar s = \"x\"\n", false, true},
		{"a named line with no literal", "package p\n\nvar n = 1\n", "package p\n\nvar n = 1\n", false, true},
	} {
		got, err := sameMasked("p.go", []byte(c.before), []byte(c.after), []int{3}, []int{3})
		if got != c.want || (err != nil) != c.wantErr {
			t.Errorf("%s: same=%v err=%v, want same=%v and an error %v", c.name, got, err, c.want, c.wantErr)
		}
	}
}

// The command reads the base from git and the file from the working tree,
// and a one-token change is reported CHANGED with status 1.
func TestRunReportsAOneTokenChange(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not on the path")
	}
	dir := t.TempDir()
	gitIn := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", dir, "-c", "user.name=t", "-c", "user.email=t@example.invalid", "-c", "commit.gpgsign=false"}, args...)...)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	write := func(name, body string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	gitIn("init", "-q")
	write("p.go", base)
	write("q.go", base)
	gitIn("add", "p.go", "q.go")
	gitIn("commit", "-q", "-m", "base")

	write("p.go", strings.Replace(base, "// why", "// the reason, reworded", 1))
	write("q.go", strings.Replace(base, "x + 1", "x + 2", 1))
	var out, errOut bytes.Buffer
	if got := run(dir, "HEAD", "", "", []string{"p.go"}, &out, &errOut); got != 0 || out.String() != "p.go: same\n" {
		t.Errorf("a comment change: status %d, output %q %q; want 0 and \"p.go: same\"", got, out.String(), errOut.String())
	}
	out.Reset()
	if got := run(dir, "HEAD", "", "", []string{"p.go", "q.go"}, &out, &errOut); got != 1 || out.String() != "p.go: same\nq.go: CHANGED\n" {
		t.Errorf("a one-token change: status %d, output %q %q; want 1 and q.go CHANGED", got, out.String(), errOut.String())
	}
}
