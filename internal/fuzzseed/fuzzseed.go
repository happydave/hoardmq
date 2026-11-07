// Package fuzzseed adds a fuzz target's seeds and, on request, writes them to
// the target's testdata/fuzz directory, so the committed seed files come from
// the tests' own inputs.
package fuzzseed

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Env names the variable that, set to "write", makes Add write each seed.
const Env = "HOARDMQ_FUZZ_SEEDS"

// Add adds one seed to f, its values of the types the target takes ([]byte,
// string, bool). With HOARDMQ_FUZZ_SEEDS=write it also writes the seed to
// testdata/fuzz/<target>/ in the package's directory.
func Add(f *testing.F, values ...any) {
	f.Helper()
	f.Add(values...)
	if os.Getenv(Env) != "write" {
		return
	}
	data, err := Encode(values...)
	if err != nil {
		f.Fatal(err)
	}
	sum := sha256.Sum256(data)
	dir := filepath.Join("testdata", "fuzz", f.Name())
	if err := os.MkdirAll(dir, 0o755); err != nil {
		f.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "seed-"+hex.EncodeToString(sum[:8])), data, 0o644); err != nil {
		f.Fatal(err)
	}
}

// Encode is the seed in Go's corpus file format, version 1.
func Encode(values ...any) ([]byte, error) {
	var b strings.Builder
	b.WriteString("go test fuzz v1\n")
	for _, v := range values {
		switch v := v.(type) {
		case []byte:
			fmt.Fprintf(&b, "[]byte(%q)\n", v)
		case string:
			fmt.Fprintf(&b, "string(%q)\n", v)
		case bool:
			fmt.Fprintf(&b, "bool(%v)\n", v)
		default:
			return nil, fmt.Errorf("fuzzseed: a seed of type %T", v)
		}
	}
	return []byte(b.String()), nil
}
