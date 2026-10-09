// Command hoardmq is the HoardMQ broker binary: it serves, validates
// configuration, and reports its version.
package main

import (
	"os"

	"github.com/happydave/hoardmq/internal/cli"
)

func main() {
	os.Exit(cli.Run(os.Args[1:], os.Environ(), os.Stdin, os.Stdout, os.Stderr))
}
