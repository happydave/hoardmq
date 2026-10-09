//go:build !fips

package argon2

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The broker built with the fips tag links no part of this package; built
// without it, it does. Read from go list, so a new import in a file the fips
// build compiles fails here.
func TestFIPSBuildLinksNoArgon2(t *testing.T) {
	gomod, err := exec.Command("go", "env", "GOMOD").Output()
	if err != nil {
		t.Fatalf("go env GOMOD: %v", err)
	}
	root := filepath.Dir(strings.TrimSpace(string(gomod)))
	deps := func(tags ...string) string {
		t.Helper()
		args := append([]string{"list"}, tags...)
		cmd := exec.Command("go", append(args, "-deps", "./cmd/hoardmq")...)
		cmd.Dir = root
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("go list %v: %v\n%s", tags, err, out)
		}
		return string(out)
	}
	const pkg = "github.com/happydave/hoardmq/internal/argon2\n"
	if !strings.Contains(deps(), pkg) {
		t.Fatal("precondition: the default build does not link internal/argon2")
	}
	if strings.Contains(deps("-tags", "fips"), pkg) {
		t.Fatal("the fips build links internal/argon2")
	}
}

// BenchmarkIDKeyDefault is one derivation at the default cost: 64 MiB,
// three passes, four lanes.
func BenchmarkIDKeyDefault(b *testing.B) {
	pw, salt := []byte("correct horse"), []byte("0123456789abcdef")
	b.ReportAllocs()
	for b.Loop() {
		IDKey(pw, salt, 3, 64*1024, 4, 32)
	}
}
