// Command simclients runs a failover measurement's simulated clients in their
// own process, for the remote-client mode of sim's TestFailover (WI 1480). It
// takes no arguments: the test drives it through stdin and stdout, and it
// exits when stdin closes. See sim/driver.go for the protocol.
package main

import (
	"os"

	"github.com/happydave/hoardmq/sim"
)

func main() {
	os.Exit(sim.RunDriver(os.Stdin, os.Stdout, os.Stderr))
}
