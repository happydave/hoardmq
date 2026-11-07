package broker

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/testlog"
)

// TestFixtureNodesLogToTheTestsFile: with HOARDMQ_TEST_LOG unset, every node
// a fixture starts writes its debug log, tagged with its name, to the test's
// node-log file.
func TestFixtureNodesLogToTheTestsFile(t *testing.T) {
	t.Setenv("HOARDMQ_TEST_LOG", "")
	nodes := startTrio(t, nil)
	f, ok := testlog.Writer(t).(*os.File)
	if !ok {
		t.Fatalf("the test's node-log writer is %T, not a file", testlog.Writer(t))
	}
	waitUntil(t, "a debug line from each node in the test's file", 5*time.Second, func() bool {
		data, err := os.ReadFile(f.Name())
		if err != nil {
			t.Fatal(err)
		}
		for name := range nodes {
			if !strings.Contains(string(data), "level=DEBUG") || !strings.Contains(string(data), "node="+name) {
				return false
			}
		}
		return true
	})
}

// TestNodeLoggerHonoursTestLog: without HOARDMQ_TEST_LOG a fixture node logs
// to a file in the temporary directory; with it, to stderr and no file.
func TestNodeLoggerHonoursTestLog(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("GOTMPDIR", dir)
	count := func() int {
		es, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		return len(es)
	}
	t.Run("unset", func(t *testing.T) {
		t.Setenv("HOARDMQ_TEST_LOG", "")
		nodeLogger(t, "n1").Info("line")
		if n := count(); n != 1 {
			t.Fatalf("%d files in the temporary directory, want the test's node log", n)
		}
	})
	t.Run("set", func(t *testing.T) {
		t.Setenv("HOARDMQ_TEST_LOG", "1")
		before := count()
		nodeLogger(t, "n1").Info("line")
		if n := count(); n != before {
			t.Fatalf("%d files after logging with HOARDMQ_TEST_LOG set, %d before", n, before)
		}
	})
}
