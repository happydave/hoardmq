package sim

import (
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/happydave/hoardmq/internal/testlog"
)

// TestClusterLogsToTheTestsFileByDefault: a cluster built without Log writes
// every node's debug log, tagged with its name, to the test's node-log file.
func TestClusterLogsToTheTestsFileByDefault(t *testing.T) {
	c := New(t, Options{})
	f, ok := testlog.Writer(t).(*os.File)
	if !ok {
		t.Fatalf("the test's node-log writer is %T, not a file", testlog.Writer(t))
	}
	data, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "level=DEBUG") {
		t.Fatalf("no debug line in %s", f.Name())
	}
	for _, n := range c.Nodes {
		if !strings.Contains(string(data), "node="+n.Name) {
			t.Fatalf("no line from %s in %s", n.Name, f.Name())
		}
	}
	// A node started again through TryStart logs there too.
	n1 := c.Node("n1")
	before := listening(t, f.Name(), "n1")
	if before == 0 {
		t.Fatalf("no listening line from n1 in %s", f.Name())
	}
	c.Crash(n1)
	if err := c.TryStart(n1); err != nil {
		t.Fatal(err)
	}
	if after := listening(t, f.Name(), "n1"); after <= before {
		t.Fatalf("n1 logged %d listening lines after TryStart, %d before", after, before)
	}
}

// listening counts a node's "listening" lines in a log file.
func listening(t *testing.T, path, node string) int {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, l := range strings.Split(string(data), "\n") {
		if fields := strings.Fields(l); slices.Contains(fields, "msg=listening") && slices.Contains(fields, "node="+node) {
			n++
		}
	}
	return n
}
