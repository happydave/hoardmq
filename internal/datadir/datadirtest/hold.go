// Package datadirtest runs a second process that holds a data directory's
// lock, for tests of what a held directory refuses.
package datadirtest

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/happydave/hoardmq/internal/datadir"
)

const holdEnv = "HOARDMQ_TEST_HOLD_DATA_DIR"

// MaybeHold makes the test binary the holder when it was started by Hold:
// it takes the lock, says so, and keeps it until its stdin closes. A test
// package that calls Hold calls this first in its TestMain.
func MaybeHold() {
	dir, ok := os.LookupEnv(holdEnv)
	if !ok {
		return
	}
	l := datadir.Exclusive(dir)
	if l.Outcome != datadir.Held {
		fmt.Printf("not held: %s\n", l.Describe())
		os.Exit(1)
	}
	fmt.Println("held")
	_, _ = bufio.NewReader(os.Stdin).ReadString('\n')
	os.Exit(0)
}

// Hold starts this test binary again as a process holding dir's lock and
// returns its PID once it holds it. The process ends when the test ends, or
// when the returned release is called.
func Hold(t testing.TB, dir string) (pid int, release func()) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), holdEnv+"="+dir)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := false
	release = func() {
		if done {
			return
		}
		done = true
		_ = stdin.Close()
		_ = cmd.Wait()
	}
	t.Cleanup(release)
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil || strings.TrimSpace(line) != "held" {
		t.Fatalf("the holding process: %q %v", line, err)
	}
	return cmd.Process.Pid, release
}
