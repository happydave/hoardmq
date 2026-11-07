package webhook

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestBuildsOutOfTree: the package's own files, copied into a module of
// their own that requires the broker's, build offline, as a module kept
// outside the broker's tree would.
func TestBuildsOutOfTree(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Fatalf("the go command is not on PATH: %v", err)
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	pkg := filepath.Join(dir, "webhook")
	if err := os.Mkdir(pkg, 0o755); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	copied := 0
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		data, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(pkg, f), data, 0o644); err != nil {
			t.Fatal(err)
		}
		copied++
	}
	if copied == 0 {
		t.Fatal("no source files to copy")
	}
	mod := "module example.com/outoftree\n\ngo 1.26.4\n\n" +
		"require github.com/happydave/hoardmq v0.0.0-00010101000000-000000000000\n\n" +
		"replace github.com/happydave/hoardmq => " + root + "\n"
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte(mod), 0o644); err != nil {
		t.Fatal(err)
	}
	sum, err := os.ReadFile(filepath.Join(root, "go.sum"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "go.sum"), sum, 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("go", "build", "./...")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOPROXY=off", "GOSUMDB=off", "GOWORK=off", "GOFLAGS=-mod=readonly")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build in a module of its own: %v\n%s", err, out)
	}
}
