// Command failoverbounds judges failover runs against the owner's pass bounds:
//
//	go run ./sim/cmd/failoverbounds <run.json or directory> ...
//
// It prints the per-run verdicts, the set's verdict and the rider on a lost
// owner's forgotten routes as Markdown, and exits 0 when the set passes, 1
// when it fails and 2 when it could not read the runs.
package main

import (
	"os"

	"github.com/happydave/hoardmq/sim"
)

func main() {
	os.Exit(sim.JudgeRunFiles(os.Stdout, os.Args[1:]))
}
