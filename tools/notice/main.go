// Command notice regenerates the NOTICE file at the module root from the
// module list. Run it through `go generate ./...`.
package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/happydave/hoardmq/internal/modlicense"
)

//go:generate go run .

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "notice:", err)
		os.Exit(1)
	}
}

func run() error {
	// go generate runs this from the tool's own directory; the module list
	// must be taken from the module root, for the binary's packages only.
	gomod, err := exec.Command("go", "env", "GOMOD").Output()
	if err != nil {
		return fmt.Errorf("go env GOMOD: %w", err)
	}
	root := filepath.Dir(strings.TrimSpace(string(gomod)))
	mods, err := modlicense.ListBuild(context.Background(), root, "./cmd/...")
	if err != nil {
		return err
	}
	text := modlicense.Notice(modlicense.Classify(mods))
	return os.WriteFile(filepath.Join(root, "NOTICE"), []byte(text), 0o644)
}
