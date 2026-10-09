package broker

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/config"
)

// holdPort listens on a loopback port for the rest of the test: a port
// another process took between the draw and the bind.
func holdPort(t *testing.T) net.Listener {
	t.Helper()
	held, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = held.Close() })
	return held
}

// recordDraws replaces drawPort for the test. Every draw is a real one,
// except that taken(i) hands out the held address for draw i (from 0).
func recordDraws(t *testing.T, held string, taken func(i int) bool) *[]string {
	t.Helper()
	saved := drawPort
	t.Cleanup(func() { drawPort = saved })
	draws := &[]string{}
	drawPort = func(tb *testing.T) string {
		addr := freePort(tb)
		if taken(len(*draws)) {
			addr = held
		}
		*draws = append(*draws, addr)
		return addr
	}
	return draws
}

// refuses fails the test unless nothing listens on addr any more.
func refuses(t *testing.T, what, addr string) {
	t.Helper()
	nc, err := net.DialTimeout("tcp", addr, time.Second)
	if err == nil {
		_ = nc.Close()
		t.Fatalf("%s %s still accepts connections: a node of a failed attempt is still running", what, addr)
	}
}

func secretAuth(t *testing.T) config.ClusterAuth {
	t.Helper()
	secret := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(secret, []byte("broker-cluster-test-secret-bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	return config.ClusterAuth{Mode: "secret", SecretFile: secret}
}

// TestClusterStartsAgainWhenADrawnPortIsTaken: n2's drawn address is taken
// before it binds, so its start fails; the nodes started are shut down and
// the trio starts again on new addresses and settles.
func TestClusterStartsAgainWhenADrawnPortIsTaken(t *testing.T) {
	held := holdPort(t)
	draws := recordDraws(t, held.Addr().String(), func(i int) bool { return i == 1 })
	nodes := startTrio(t, nil)
	if got := nodes["n2"].srv.config().Cluster.Listen; got == held.Addr().String() {
		t.Fatalf("n2 still has the taken address %s", got)
	}
	if len(*draws) < 6 {
		t.Fatalf("%d draws: the cluster was not started again on new addresses", len(*draws))
	}
	refuses(t, "the first attempt's n1 address", (*draws)[0])
	// The held listener is still the test's and still accepting.
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

// TestClusterStartGivesUpAfterThreeAttempts: n2's address is taken on each
// of the first five attempts; the start gives up after the third with the
// address-in-use error, every attempt's started node shut down.
func TestClusterStartGivesUpAfterThreeAttempts(t *testing.T) {
	held := holdPort(t)
	draws := recordDraws(t, held.Addr().String(), func(i int) bool { return i%3 == 1 && i < 15 })
	auth := secretAuth(t)
	_, err := tryStartCluster(t, []string{"n1", "n2", "n3"}, func(_ string, d *config.Document) { d.Cluster.Auth = auth })
	if err == nil {
		t.Fatalf("the cluster started after %d draws; want the start to give up after three attempts", len(*draws))
	}
	if !errors.Is(err, syscall.EADDRINUSE) {
		t.Fatalf("got %v, want the address-in-use error of the third attempt", err)
	}
	if len(*draws) != 9 {
		t.Fatalf("%d draws, want 9: three attempts of three nodes", len(*draws))
	}
	for i := 0; i < len(*draws); i += 3 {
		refuses(t, "an attempt's n1 address", (*draws)[i])
	}
}

// TestClusterStartDoesNotRetryOtherErrors: a start error that is not an
// address in use fails the first attempt, with no new draw.
func TestClusterStartDoesNotRetryOtherErrors(t *testing.T) {
	notADir := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(notADir, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	draws := recordDraws(t, "", func(int) bool { return false })
	auth := secretAuth(t)
	_, err := tryStartCluster(t, []string{"n1", "n2", "n3"}, func(name string, d *config.Document) {
		d.Cluster.Auth = auth
		if name == "n2" {
			d.Node.DataDir = filepath.Join(notADir, "data")
		}
	})
	if err == nil || !strings.Contains(err.Error(), "data directory") {
		t.Fatalf("got %v, want n2's data directory error", err)
	}
	if errors.Is(err, syscall.EADDRINUSE) {
		t.Fatalf("the error is an address in use: %v", err)
	}
	if len(*draws) != 3 {
		t.Fatalf("%d draws, want 3: one attempt", len(*draws))
	}
	refuses(t, "n1's address", (*draws)[0])
}

// TestFreePortNeverRepeats: one process never hands out a port twice. The
// window is narrowed so that without the record a repeat is all but certain:
// 60 draws from 121 ports.
func TestFreePortNeverRepeats(t *testing.T) {
	savedWindow := portWindow
	portMu.Lock()
	savedHanded := handedPorts
	handedPorts = map[int]bool{}
	portMu.Unlock()
	t.Cleanup(func() {
		portWindow = savedWindow
		portMu.Lock()
		handedPorts = savedHanded
		portMu.Unlock()
	})
	portWindow = func() (int, int) { return 23000, 23121 }
	seen := map[string]bool{}
	for i := 0; i < 60; i++ {
		addr := freePort(t)
		if seen[addr] {
			t.Fatalf("draw %d returned %s a second time", i+1, addr)
		}
		seen[addr] = true
	}
}
