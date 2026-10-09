package sim

import (
	"net"
	"strings"
	"testing"
)

// TestSettleWaits asserts what WI 1496 is for: a remote-client batch waits for
// the connection-tracking table, an in-process batch does not, and the limit it
// waits for is the same one serverStartProblems refuses above. The last part is
// the one that matters — the two sides drifting apart is how two 100,000-client
// runs came to be refused for a condition nothing waited on.
func TestSettleWaits(t *testing.T) {
	inProcess := settleWaits(100000, false)
	if len(inProcess) != 1 {
		t.Fatalf("in-process waits: %d conditions, want 1", len(inProcess))
	}
	if got, want := inProcess[0].limit, int64(settleLimit(100000)); got != want {
		t.Errorf("socket limit %d, want %d", got, want)
	}

	remote := settleWaits(100000, true)
	if len(remote) != 2 {
		t.Fatalf("remote waits: %d conditions, want 2", len(remote))
	}
	conntrack := remote[1]
	if !strings.Contains(conntrack.what, "connection-tracking") {
		t.Errorf("second remote condition is %q, want the connection-tracking table", conntrack.what)
	}

	// The wait's limit against the refusal's, at the boundary: a reading the
	// driver stops waiting at must be one the measurement then accepts, and one
	// entry more must be refused. Asserting the refusal at both values is what
	// makes this a check on the two agreeing rather than on one constant.
	addrs := []net.IP{net.ParseIP("10.231.14.1"), net.ParseIP("10.231.14.2"), net.ParseIP("10.231.14.3")}
	nodes := []string{"10.231.14.1:8001", "10.231.14.2:8001", "10.231.14.3:8001"}
	if p := serverStartProblems(100000, conntrack.limit, 1048576, true, nodes, addrs); len(p) != 0 {
		t.Errorf("the driver would stop waiting at %d entries, which the run refuses: %v", conntrack.limit, p)
	}
	if p := serverStartProblems(100000, conntrack.limit+1, 1048576, true, nodes, addrs); len(p) != 1 {
		t.Errorf("one entry over the wait's limit is not refused: %v", p)
	}
}

// TestSettleWaitReadsHost checks the conditions read the host rather than a
// constant, and that a reading the host does not expose is reported as unknown
// rather than as zero. Both conditions read files that exist on Linux; where
// one does not, the wait proceeds, which is the same choice the refusal makes.
func TestSettleWaitReadsHost(t *testing.T) {
	for _, w := range settleWaits(1000, true) {
		n, ok := w.read()
		if !ok {
			t.Logf("%s: not exposed on this host", w.what)
			continue
		}
		if n < 0 {
			t.Errorf("%s: read %d", w.what, n)
		}
	}
}
