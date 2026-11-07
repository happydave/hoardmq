package config

import (
	"fmt"
	"os"
	"testing"
)

// TestMain checks, before any test touches the schema, that loading the
// package compiled nothing: a command that never validates a document must
// not pay the schema's compile.
func TestMain(m *testing.M) {
	if n := compilations.Load(); n != 0 {
		fmt.Fprintf(os.Stderr, "the schema was compiled %d times at init, want 0\n", n)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

// TestSchemaCompiledOnce: validating compiles the schema on first use, once.
func TestSchemaCompiledOnce(t *testing.T) {
	_ = Schema()
	if _, problems := Parse([]byte(`{"version":1,"node":{"dataDir":"d"}}`), nil); len(problems) != 0 {
		t.Fatal(problems)
	}
	if n := compilations.Load(); n != 1 {
		t.Fatalf("compiled %d times, want 1", n)
	}
}
