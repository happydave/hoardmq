// Package hoardmqcmd is the HoardMQ broker's command line as a library: a
// binary built with hoardmq-build, and the default binary, call Main from a
// main package that imports the modules to compile in. Every such binary
// carries the built-in modules and the modules this package links.
package hoardmqcmd

import (
	"os"

	"github.com/happydave/hoardmq/internal/cli"
	_ "github.com/happydave/hoardmq/modules/sinkkafka" // sink.kafka, the Kafka forwarder
)

// Main runs the hoardmq command with the process's arguments, environment and
// standard streams, and exits with its status.
func Main() {
	os.Exit(cli.Run(os.Args[1:], os.Environ(), os.Stdin, os.Stdout, os.Stderr))
}
