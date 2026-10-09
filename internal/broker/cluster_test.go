package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/testcert"
)

// TestThreeNodeTransport brings up three in-process nodes with mutual TLS,
// wires their transport addresses, and checks every pair is up, that the
// gauge is scraped, and that stopping one node is seen by the other two.
func TestThreeNodeTransport(t *testing.T) {
	dir := t.TempDir()
	ca, err := testcert.NewCA("cluster-ca")
	if err != nil {
		t.Fatal(err)
	}
	caFile, err := ca.WriteCA(dir)
	if err != nil {
		t.Fatal(err)
	}
	names := []string{"n1", "n2", "n3"}
	certs := map[string]*config.ClusterTLS{}
	for i, name := range names {
		leaf, err := ca.Issue(name, int64(10+i))
		if err != nil {
			t.Fatal(err)
		}
		certFile, keyFile, err := leaf.Write(dir, name)
		if err != nil {
			t.Fatal(err)
		}
		certs[name] = &config.ClusterTLS{CertFile: certFile, KeyFile: keyFile, CAFile: caFile}
	}
	nodes := startCluster(t, names, func(name string, d *config.Document) {
		d.Cluster.HeartbeatMs = 100
		d.Cluster.Auth = config.ClusterAuth{Mode: "mtls", TLS: certs[name]}
		if name == "n1" {
			d.Observability.Address = "127.0.0.1:0"
			d.Modules["metrics.prometheus"] = json.RawMessage(`{}`)
		}
	})
	upCount := func(f *fixture) int {
		n := 0
		for _, p := range f.srv.Cluster().Peers() {
			if p.State == transport.Up {
				n++
			}
		}
		return n
	}
	wait := func(what string, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if cond() {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for %s", what)
	}
	for name, f := range nodes {
		wait(name+" fully connected", func() bool { return upCount(f) == 2 })
	}
	// A frame crosses between two nodes through the broker-owned transport.
	got := make(chan string, 1)
	nodes["n3"].srv.Cluster().Handle(transport.Consensus, func(peer string, payload []byte) { got <- peer + ":" + string(payload) })
	if err := nodes["n1"].srv.Cluster().Send(context.Background(), "n3", transport.Consensus, []byte("hi")); err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-got:
		if m != "n1:hi" {
			t.Fatalf("got %q", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("frame did not arrive")
	}
	// The gauge follows the peers connecting, which is asynchronous: a frame
	// reaching n3 says nothing about n1's link to n2. Wait for it rather
	// than scrape once and hope, which fails about one run in three under
	// the race detector.
	var body string
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, b := scrape(t, nodes["n1"], "/metrics"); strings.Contains(b, "hoardmq_cluster_peers_connected 2") {
			body = b
			break
		} else {
			body = b
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !strings.Contains(body, "hoardmq_cluster_peers_connected 2") {
		t.Fatalf("gauge missing:\n%s", body)
	}
	// Stop n2: the others see it down and keep each other.
	if err := nodes["n2"].srv.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"n1", "n3"} {
		f := nodes[name]
		wait(name+" sees n2 down", func() bool {
			for _, p := range f.srv.Cluster().Peers() {
				if p.Name == "n2" {
					return p.State != transport.Up
				}
			}
			return false
		})
		if upCount(f) != 1 {
			t.Fatalf("%s: %+v", name, f.srv.Cluster().Peers())
		}
	}
	// The audit log of n1 records the peer transitions.
	raw, err := os.ReadFile(filepath.Join(nodes["n1"].srv.AuditDir(), "audit.log"))
	if err != nil {
		t.Fatal(err)
	}
	if data := string(raw); !strings.Contains(data, `"cluster.peer.up"`) || !strings.Contains(data, `"cluster.peer.down"`) {
		t.Fatalf("audit lacks peer events:\n%s", data)
	}
}

func TestNoClusterListenerMeansNoTransport(t *testing.T) {
	f := start(t, nil)
	if f.srv.Cluster() != nil {
		t.Fatal("transport built without a cluster listener")
	}
}

// clusterStartAttempts bounds how many times startCluster draws a cluster's
// addresses and starts its nodes.
const clusterStartAttempts = 3

// startCluster starts one node per name, in order, each listening on a drawn
// cluster address with the others as its peers, then configure's settings.
//
// A port drawn free can be taken before its node binds it, by another test
// binary under the whole-module gate. A node cannot re-draw alone, because
// the nodes started before it already list its address as a peer, so the
// nodes started are shut down and the whole cluster starts again on new
// addresses, as sim.New does.
func startCluster(t *testing.T, names []string, configure func(name string, d *config.Document)) map[string]*fixture {
	t.Helper()
	nodes, err := tryStartCluster(t, names, configure)
	if err != nil {
		t.Fatal(err)
	}
	return nodes
}

// tryStartCluster is startCluster returning the last start error.
func tryStartCluster(t *testing.T, names []string, configure func(name string, d *config.Document)) (map[string]*fixture, error) {
	t.Helper()
	for attempt := 1; ; attempt++ {
		addrs := map[string]string{}
		for _, n := range names {
			addrs[n] = drawPort(t)
		}
		nodes, err := startNodes(t, names, addrs, configure)
		if err == nil {
			return nodes, nil
		}
		if !errors.Is(err, syscall.EADDRINUSE) || attempt == clusterStartAttempts {
			return nil, err
		}
		t.Logf("cluster start attempt %d: %v; starting again on new addresses", attempt, err)
	}
}

// startNodes starts the nodes in order. On the first failure it shuts down
// the nodes it started and returns the error, naming the node.
func startNodes(t *testing.T, names []string, addrs map[string]string, configure func(name string, d *config.Document)) (map[string]*fixture, error) {
	t.Helper()
	nodes := map[string]*fixture{}
	for _, name := range names {
		f, err := tryStart(t, func(d *config.Document) {
			d.Node.Name = name
			d.Cluster.Listen = addrs[name]
			for _, other := range names {
				if other != name {
					d.Cluster.Peers = append(d.Cluster.Peers, config.Peer{Name: other, Address: addrs[other]})
				}
			}
			if configure != nil {
				configure(name, d)
			}
		})
		if err != nil {
			for _, started := range nodes {
				started.stop()
			}
			return nil, fmt.Errorf("%s start: %w", name, err)
		}
		nodes[name] = f
	}
	return nodes, nil
}

// drawPort is where startCluster gets a node's address; tests replace it to
// force a collision.
var drawPort = freePort

// portWindow is the range freePort draws from: below Linux's default
// ephemeral range (32768–60999), so a client's source port is unlikely to
// take a node's address. Tests narrow it.
var portWindow = func() (lo, hi int) { return 20000, 32000 }

// handedPorts holds every port freePort has returned in this process, so two
// draws never return the same port before either is bound.
var (
	portMu      sync.Mutex
	handedPorts = map[int]bool{}
)

// freePort draws a loopback port that binds now and that this process has
// not handed out before. Nothing holds it until the node binds it.
func freePort(t *testing.T) string {
	t.Helper()
	lo, hi := portWindow()
	for i := 0; i < 200; i++ {
		port := lo + rand.IntN(hi-lo)
		portMu.Lock()
		if handedPorts[port] {
			portMu.Unlock()
			continue
		}
		handedPorts[port] = true
		portMu.Unlock()
		addr := fmt.Sprintf("127.0.0.1:%d", port)
		l, err := net.Listen("tcp", addr)
		if err != nil {
			portMu.Lock()
			delete(handedPorts, port)
			portMu.Unlock()
			continue
		}
		_ = l.Close()
		return addr
	}
	t.Fatal("no free port found")
	return ""
}

// TestThreeNodeConsensus brings up three brokers with the shared-secret
// transport and statically configured peers, and checks the consensus core
// forms one group: an agreed leader, readiness on every node, and the raft
// gauges on the scrape.
func TestThreeNodeConsensus(t *testing.T) {
	dir := t.TempDir()
	secret := filepath.Join(dir, "secret")
	if err := os.WriteFile(secret, []byte("broker-cluster-test-secret-bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	names := []string{"n1", "n2", "n3"}
	nodes := startCluster(t, names, func(name string, d *config.Document) {
		d.Cluster.HeartbeatMs = 100
		d.Cluster.Auth = config.ClusterAuth{Mode: "secret", SecretFile: secret}
		d.Cluster.Raft.TickMs = 10
		d.Cluster.Raft.JoinTimeoutMs = 1000
		if name == "n1" {
			d.Observability.Address = "127.0.0.1:0"
			d.Modules["metrics.prometheus"] = json.RawMessage(`{}`)
		}
	})
	wait := func(what string, d time.Duration, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(d)
		for time.Now().Before(deadline) {
			if cond() {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for %s", what)
	}
	wait("one agreed leader and every node ready", 20*time.Second, func() bool {
		var leader string
		for _, f := range nodes {
			l, ok := f.srv.Consensus().Leader()
			if !ok || !f.srv.Consensus().Ready() {
				return false
			}
			if leader == "" {
				leader = l
			} else if leader != l {
				return false
			}
		}
		return true
	})
	// Three voters once promotion has run.
	wait("three voters", 15*time.Second, func() bool {
		voters := 0
		for _, m := range nodes["n1"].srv.Consensus().Members() {
			if m.Role == consensus.Voter {
				voters++
			}
		}
		return voters == 3
	})
	for _, f := range nodes {
		if st := f.srv.Consensus().Status(); st.Role == "joining" || st.Role == "removed" {
			t.Fatalf("%+v", st)
		}
	}
	if code, _ := scrape(t, nodes["n1"], "/readyz"); code != 200 {
		t.Fatalf("readyz %d", code)
	}
	_, body := scrape(t, nodes["n1"], "/metrics")
	for _, want := range []string{"hoardmq_raft_term ", "hoardmq_raft_commit_index ", `hoardmq_raft_members{role="voter"} 3`} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q\n%s", want, body)
		}
	}
	// Only one node reports itself leader.
	leaders := 0
	for _, f := range nodes {
		if f.srv.Consensus().Status().Role == "leader" {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("%d leaders", leaders)
	}
}

// TestConsensusIsNilBeforeStart: a caller guards Consensus() with a nil check,
// so a server with no core must compare equal to nil.
func TestConsensusIsNilBeforeStart(t *testing.T) {
	doc := config.Default()
	doc.Node.DataDir = filepath.Join(t.TempDir(), "data")
	doc.Listeners = []config.Listener{{Name: "tcp", Type: "tcp", Address: "127.0.0.1:0"}}
	srv, err := New(doc, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if c := srv.Consensus(); c != nil {
		t.Fatalf("Consensus() before Start = %#v, want a nil interface", c)
	}
}

func TestSingleNodeIsAOneMemberGroup(t *testing.T) {
	f := start(t, func(d *config.Document) {
		d.Observability.Address = "127.0.0.1:0"
		d.Modules["metrics.prometheus"] = json.RawMessage(`{}`)
	})
	st := f.srv.Consensus().Status()
	if st.Role != "leader" || !st.Bootstrapped || !st.Joined {
		t.Fatalf("%+v", st)
	}
	if code, _ := scrape(t, f, "/readyz"); code != 200 {
		t.Fatalf("readyz %d", code)
	}
	if ms := f.srv.Consensus().Members(); len(ms) != 1 || ms[0].Role != consensus.Voter {
		t.Fatalf("members %+v", ms)
	}
}

// TestRoutingCallbacksAreSafeBesideStart means something only under -race. The
// routing plane calls members and ownedSessions from transport goroutines that
// exist before Start opens the consensus core, so they are called here across
// the whole of Start. Both answers are asserted so that a loop which never ran,
// or a core that never appeared, cannot pass.
func TestRoutingCallbacksAreSafeBesideStart(t *testing.T) {
	doc := config.Default()
	doc.Node.DataDir = filepath.Join(t.TempDir(), "data")
	doc.Cluster.Raft.TickMs = 10
	doc.Listeners = []config.Listener{{Name: "tcp", Type: "tcp", Address: "127.0.0.1:0"}}
	srv, err := New(doc, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	var before, after int
	go func() {
		defer close(done)
		for {
			names := srv.members()
			_, known := srv.ownedSessions(doc.Node.Name)
			if len(names) == 0 && !known {
				before++
			}
			if len(names) > 0 {
				after++
			}
			select {
			case <-stop:
				return
			default:
			}
		}
	}()
	// Let the loop observe the server before Start.
	time.Sleep(20 * time.Millisecond)
	if err := srv.Start(); err != nil {
		close(stop)
		<-done
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.Shutdown(context.Background()) })
	waitFor := time.Now().Add(10 * time.Second)
	for len(srv.members()) == 0 && time.Now().Before(waitFor) {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	close(stop)
	<-done
	if before == 0 {
		t.Error("the callbacks were never called before the core existed")
	}
	if after == 0 {
		t.Error("the callbacks never saw the core's members after Start")
	}
}
