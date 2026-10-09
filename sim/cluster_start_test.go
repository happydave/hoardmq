package sim

import (
	"net"
	"sync"
	"testing"
)

// TestFreePortNeverRepeats: one process never hands out a port twice (WI
// 1488). The window is narrowed so that without the record a repeat is all
// but certain: 60 draws from 121 ports.
func TestFreePortNeverRepeats(t *testing.T) {
	saved := portWindowOf
	portMu.Lock()
	savedHanded := handedPorts
	handedPorts = map[int]bool{}
	portMu.Unlock()
	t.Cleanup(func() {
		portWindowOf = saved
		portMu.Lock()
		handedPorts = savedHanded
		portMu.Unlock()
	})
	lo, _ := portWindow()
	portWindowOf = func() (int, int) { return lo + 3000, lo + 3121 }
	seen := map[string]bool{}
	for i := 0; i < 60; i++ {
		addr := freePort(t)
		if seen[addr] {
			t.Fatalf("draw %d returned %s a second time", i+1, addr)
		}
		seen[addr] = true
	}
}

// TestNewStartsAgainWhenADrawnPortIsTaken: a port taken between the draw and
// the bind makes the first start fail; New stops what it started and starts
// the cluster again on new addresses (WI 1488).
func TestNewStartsAgainWhenADrawnPortIsTaken(t *testing.T) {
	held, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = held.Close() }()
	saved := drawPort
	t.Cleanup(func() { drawPort = saved })
	var mu sync.Mutex
	draws := 0
	drawPort = func(tb testing.TB) string {
		mu.Lock()
		draws++
		d := draws
		mu.Unlock()
		// Draws go client, cluster, observability per node: the fifth is n2's
		// cluster address on the first attempt.
		if d == 5 {
			return held.Addr().String()
		}
		return freePort(tb)
	}
	c := New(t, Options{})
	if got := c.Node("n2").Cluster; got == held.Addr().String() {
		t.Fatalf("n2 still has the taken address %s", got)
	}
	if draws <= 9 {
		t.Fatalf("%d draws: the cluster was not started again on new addresses", draws)
	}
	// The held listener is still ours and still accepting.
	go func() {
		if nc, err := held.Accept(); err == nil {
			_ = nc.Close()
		}
	}()
	nc, err := net.Dial("tcp", held.Addr().String())
	if err != nil {
		t.Fatalf("the held listener was disturbed: %v", err)
	}
	_ = nc.Close()
}
