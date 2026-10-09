package transport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/testcert"
)

// events collects audit events and metric counts from a transport.
type events struct {
	mu     sync.Mutex
	audit  []string // action|target|outcome|reason-or-error
	counts map[string]float64
	gauges map[string]float64
}

func newEvents() *events { return &events{counts: map[string]float64{}, gauges: map[string]float64{}} }

func (e *events) Audit(action, target, outcome string, detail map[string]string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.audit = append(e.audit, action+"|"+target+"|"+outcome+"|"+detail["error"])
}

func (e *events) Add(name string, labels map[string]string, delta float64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.counts[key(name, labels)] += delta
}

func (e *events) Set(name string, labels map[string]string, v float64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.gauges[key(name, labels)] = v
}

func key(name string, labels map[string]string) string {
	parts := []string{name}
	for k, v := range labels {
		parts = append(parts, k+"="+v)
	}
	return strings.Join(parts, ",")
}

// has reports whether an audit event with the action and outcome was seen.
func (e *events) has(action, outcome string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, a := range e.audit {
		f := strings.SplitN(a, "|", 4)
		if f[0] == action && f[2] == outcome {
			return true
		}
	}
	return false
}

// countAudit counts audit events with the action.
func (e *events) countAudit(action string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	n := 0
	for _, a := range e.audit {
		if strings.HasPrefix(a, action+"|") {
			n++
		}
	}
	return n
}

func (e *events) count(name, label string) float64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	for k, v := range e.counts {
		if strings.HasPrefix(k, name) && strings.Contains(k, label) {
			return v
		}
	}
	return 0
}

type node struct {
	t  *Transport
	ev *events

	mu       sync.Mutex
	ups, dns map[string]int // transitions seen per peer
	tookUp   map[string]int // transitions already waited for
	tookDown map[string]int
	dataUps  map[string]int // data-connection transitions seen per peer
	dataDns  map[string]int
}

func (n *node) observe(peer string, s State) {
	n.mu.Lock()
	defer n.mu.Unlock()
	switch s {
	case Up:
		n.ups[peer]++
	case Down:
		n.dns[peer]++
	}
}

func (n *node) observeData(peer string, s State) {
	n.mu.Lock()
	defer n.mu.Unlock()
	switch s {
	case Up:
		n.dataUps[peer]++
	case Down:
		n.dataDns[peer]++
	}
}

func (n *node) dataTransitions(peer string) (ups, downs int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.dataUps[peer], n.dataDns[peer]
}

// cluster builds transports for the named nodes, all peers of each other,
// with the given authentication, on ephemeral loopback ports. Nothing is
// started; addresses are filled in after each listener binds, so the
// transports are started here in name order and peers learn addresses
// through SetPeers.
type cluster struct {
	nodes map[string]*node
	ca    *testcert.CA
}

const testSecret = "a-shared-secret-of-enough-bytes"

func secretAuth() Auth { return Auth{Mode: "secret", Secret: []byte(testSecret)} }

func (c *cluster) mtlsAuth(t *testing.T, name string) Auth {
	t.Helper()
	leaf, err := c.ca.Issue(name, int64(len(name)+2))
	if err != nil {
		t.Fatal(err)
	}
	cert, err := tls.X509KeyPair(leaf.CertPEM, leaf.KeyPEM)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(c.ca.PEM)
	return Auth{Mode: "mtls", Cert: cert, CA: pool}
}

func newCluster(t *testing.T, mode string, names []string, tweak func(name string, o *Options)) *cluster {
	t.Helper()
	c := &cluster{nodes: map[string]*node{}}
	if mode == "mtls" {
		ca, err := testcert.NewCA("cluster-ca")
		if err != nil {
			t.Fatal(err)
		}
		c.ca = ca
	}
	// Every node lists every other node; addresses are placeholders until
	// the listeners bind.
	for _, name := range names {
		ev := newEvents()
		o := Options{Node: name, ClusterID: "test", Listen: "127.0.0.1:0", Heartbeat: 100 * time.Millisecond, HeartbeatMisses: 3, HandshakeTimeout: 2 * time.Second,
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Metrics: ev, Audit: ev.Audit}
		if mode == "mtls" {
			o.Auth = c.mtlsAuth(t, name)
		} else {
			o.Auth = secretAuth()
		}
		for _, other := range names {
			if other != name {
				o.Peers = append(o.Peers, config.Peer{Name: other, Address: "127.0.0.1:1"})
			}
		}
		if tweak != nil {
			tweak(name, &o)
		}
		tr, err := New(o)
		if err != nil {
			t.Fatal(err)
		}
		n := &node{t: tr, ev: ev, ups: map[string]int{}, dns: map[string]int{}, tookUp: map[string]int{}, tookDown: map[string]int{}, dataUps: map[string]int{}, dataDns: map[string]int{}}
		tr.Observe(n.observe)
		tr.ObserveData(n.observeData)
		c.nodes[name] = n
	}
	return c
}

// start binds every node, then tells each the real addresses of the others.
func (c *cluster) start(t *testing.T) {
	t.Helper()
	addrs := map[string]string{}
	for name, n := range c.nodes {
		if err := n.t.Start(); err != nil {
			t.Fatal(err)
		}
		addrs[name] = n.t.Addr().String()
		t.Cleanup(n.t.Stop)
	}
	for name, n := range c.nodes {
		var peers []config.Peer
		for other, a := range addrs {
			if other != name {
				peers = append(peers, config.Peer{Name: other, Address: a})
			}
		}
		n.t.SetPeers(peers)
	}
}

// waitUp blocks until the node has seen one more Up transition for the
// peer than previous waits consumed.
func (n *node) waitUp(t *testing.T, peer string) {
	t.Helper()
	n.waitTransition(t, peer, "come up", n.ups, n.tookUp)
}

func (n *node) waitDown(t *testing.T, peer string) {
	t.Helper()
	n.waitTransition(t, peer, "go down", n.dns, n.tookDown)
}

func (n *node) waitTransition(t *testing.T, peer, what string, seen, taken map[string]int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		n.mu.Lock()
		if seen[peer] > taken[peer] {
			taken[peer]++
			n.mu.Unlock()
			return
		}
		n.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("%s: peer %s did not %s; peers %+v", n.t.o.Node, peer, what, n.t.Peers())
}

// waitFor polls a condition.
func waitFor(t *testing.T, what string, cond func() bool) {
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

// frames collects payloads a handler received.
type frames struct {
	mu  sync.Mutex
	got []string
}

func (f *frames) n() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.got)
}

func (f *frames) at(i int) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.got[i]
}

func (f *frames) all() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.got...)
}

// received collects frames of one class on a node.
func received(n *node, c Class) *frames {
	f := &frames{}
	n.t.Handle(c, func(peer string, payload []byte) {
		f.mu.Lock()
		f.got = append(f.got, string(payload))
		f.mu.Unlock()
	})
	return f
}

func TestSecretModeConnectsAndCarriesFrames(t *testing.T) {
	for _, mode := range []string{"secret", "mtls"} {
		t.Run(mode, func(t *testing.T) {
			c := newCluster(t, mode, []string{"n1", "n2"}, nil)
			got := received(c.nodes["n2"], ForwardedPublish)
			c.start(t)
			c.nodes["n1"].waitUp(t, "n2")
			c.nodes["n2"].waitUp(t, "n1")
			if err := c.nodes["n1"].t.Send(context.Background(), "n2", ForwardedPublish, []byte("hello")); err != nil {
				t.Fatal(err)
			}
			waitFor(t, "frame", func() bool { return got.n() == 1 })
			if got.at(0) != "hello" {
				t.Fatalf("got %q", got.all())
			}
			// Both directions.
			back := received(c.nodes["n1"], RouteUpdate)
			if err := c.nodes["n2"].t.Send(context.Background(), "n1", RouteUpdate, []byte("route")); err != nil {
				t.Fatal(err)
			}
			waitFor(t, "reverse frame", func() bool { return back.n() == 1 })
			for _, n := range c.nodes {
				if ps := n.t.Peers(); len(ps) != 1 || ps[0].State != Up || ps[0].Version != Version4 {
					t.Fatalf("%s peers %+v", n.t.o.Node, ps)
				}
				if !n.ev.has("cluster.peer.up", "ok") {
					t.Fatalf("%s: no peer.up audit event", n.t.o.Node)
				}
			}
		})
	}
}

func TestRefusals(t *testing.T) {
	// A two-node mtls cluster, plus intruders.
	c := newCluster(t, "mtls", []string{"n1", "n2"}, nil)
	seen := received(c.nodes["n2"], ForwardedPublish)
	c.start(t)
	c.nodes["n2"].waitUp(t, "n1")
	target := c.nodes["n2"].t.Addr().String()
	acceptor := c.nodes["n2"]

	dialWith := func(t *testing.T, name string, auth Auth, peers []config.Peer, clusterID string) error {
		t.Helper()
		if clusterID == "" {
			clusterID = "test"
		}
		o := Options{Node: name, ClusterID: clusterID, Listen: "127.0.0.1:0", Auth: auth, HandshakeTimeout: 2 * time.Second, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
		o.Peers = peers
		tr, err := New(o)
		if err != nil {
			t.Fatal(err)
		}
		tr.ctx, tr.cancel = context.WithCancel(context.Background())
		defer tr.cancel()
		p := tr.peers["n2"]
		if p == nil {
			p = newPeer(config.Peer{Name: "n2", Address: target})
			tr.peers["n2"] = p
		}
		p.setAddress(target)
		_, err = tr.dial(p, RoleControl)
		return err
	}

	t.Run("wrong CA", func(t *testing.T) {
		otherCA, _ := testcert.NewCA("other")
		other := &cluster{ca: otherCA}
		auth := other.mtlsAuth(t, "n1")
		// Trust the real CA so the TLS layer completes and the server side
		// refuses the client certificate.
		auth.CA = c.mtlsAuth(t, "x").CA
		before := acceptor.ev.count("hoardmq_cluster_refusals_total", "tls")
		err := dialWith(t, "n1", auth, nil, "")
		if err == nil {
			t.Fatal("connected with a certificate from another CA")
		}
		waitFor(t, "tls refusal", func() bool { return acceptor.ev.count("hoardmq_cluster_refusals_total", "tls") > before })
	})
	t.Run("common name mismatch", func(t *testing.T) {
		auth := c.mtlsAuth(t, "n9") // valid cert, wrong name for the claim
		err := dialWith(t, "n1", auth, nil, "")
		if err == nil {
			t.Fatal("connected with a certificate for another name")
		}
		waitFor(t, "certificate refusal", func() bool { return acceptor.ev.has("cluster.peer.refused", "certificate") })
	})
	t.Run("unknown peer name", func(t *testing.T) {
		auth := c.mtlsAuth(t, "n7")
		err := dialWith(t, "n7", auth, []config.Peer{{Name: "n2", Address: target}}, "")
		if err == nil {
			t.Fatal("connected as an unconfigured peer")
		}
		waitFor(t, "unknown-peer refusal", func() bool { return acceptor.ev.has("cluster.peer.refused", "unknown-peer") })
	})
	t.Run("cluster id mismatch", func(t *testing.T) {
		auth := c.mtlsAuth(t, "n1")
		err := dialWith(t, "n1", auth, nil, "other-cluster")
		if err == nil {
			t.Fatal("connected across clusters")
		}
		waitFor(t, "cluster-id refusal", func() bool { return acceptor.ev.has("cluster.peer.refused", "cluster-id") })
	})
	t.Run("plaintext to a TLS listener", func(t *testing.T) {
		conn, err := net.Dial("tcp", target)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close() }()
		// A plaintext hello is not a TLS ClientHello; the server closes.
		_, _ = conn.Write([]byte{0, 0, 0, 10, 0, 0, 0, 4, 't', 'e', 's', 't', 0, 0})
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		buf := make([]byte, 16)
		if _, err := conn.Read(buf); err == nil {
			t.Fatal("server answered a plaintext dial")
		}
		waitFor(t, "tls refusal", func() bool { return acceptor.ev.has("cluster.peer.refused", "tls") })
	})
	if seen.n() != 0 {
		t.Fatalf("a handler saw frames from a refused peer: %v", seen.all())
	}
}

func TestSecretRefusals(t *testing.T) {
	c := newCluster(t, "secret", []string{"n1", "n2"}, nil)
	seen := received(c.nodes["n2"], ForwardedPublish)
	c.start(t)
	c.nodes["n2"].waitUp(t, "n1")
	target := c.nodes["n2"].t.Addr().String()
	acceptor := c.nodes["n2"]

	t.Run("wrong secret", func(t *testing.T) {
		o := Options{Node: "n1", ClusterID: "test", Auth: Auth{Mode: "secret", Secret: []byte("a-different-secret-of-enough-bytes")}, HandshakeTimeout: 2 * time.Second, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
		tr, err := New(o)
		if err != nil {
			t.Fatal(err)
		}
		tr.ctx, tr.cancel = context.WithCancel(context.Background())
		defer tr.cancel()
		p := newPeer(config.Peer{Name: "n2", Address: target})
		tr.peers["n2"] = p
		if _, err := tr.dial(p, RoleControl); err == nil {
			t.Fatal("connected with the wrong secret")
		}
		// The dialer refuses the acceptor's proof first; the acceptor sees
		// the connection end without a valid dialer proof.
		waitFor(t, "acceptor refusal", func() bool {
			return acceptor.ev.has("cluster.peer.refused", "mac") || acceptor.ev.has("cluster.peer.refused", "connection")
		})
	})
	t.Run("replayed handshake", func(t *testing.T) {
		// Record a genuine dialer hello and proof by running a handshake
		// through a recording connection, then replay the bytes.
		var recorded [][]byte
		var mu sync.Mutex
		raw, err := net.Dial("tcp", target)
		if err != nil {
			t.Fatal(err)
		}
		rec := &recordingConn{Conn: raw, onWrite: func(b []byte) {
			mu.Lock()
			recorded = append(recorded, append([]byte(nil), b...))
			mu.Unlock()
		}}
		n, err := c.nodes["n1"].t.handshake(rec, true)
		if err != nil || n.peer != "n2" {
			t.Fatalf("genuine handshake: %v %+v", err, n)
		}
		_ = raw.Close()
		// Replay: send the same hello and the same proof to a fresh
		// connection. The acceptor's nonce differs, so the proof fails.
		replay, err := net.Dial("tcp", target)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = replay.Close() }()
		mu.Lock()
		frames := recorded
		mu.Unlock()
		if len(frames) < 2 {
			t.Fatalf("recorded %d writes", len(frames))
		}
		_, _ = replay.Write(frames[0])
		// Read the acceptor's hello, then send the stale proof.
		_ = replay.SetReadDeadline(time.Now().Add(2 * time.Second))
		if _, err := readFrame(replay, 1<<20, true); err != nil {
			t.Fatalf("acceptor hello: %v", err)
		}
		_, _ = replay.Write(frames[1])
		buf := make([]byte, 1)
		if _, err := replay.Read(buf); err == nil {
			t.Fatal("acceptor kept a replayed connection open")
		}
		waitFor(t, "mac refusal", func() bool { return acceptor.ev.has("cluster.peer.refused", "mac") })
	})
	if seen.n() != 0 {
		t.Fatalf("a handler saw frames from a refused peer: %v", seen.all())
	}
}

// recordingConn records what is written through it.
type recordingConn struct {
	net.Conn
	onWrite func([]byte)
}

func (r *recordingConn) Write(b []byte) (int, error) {
	r.onWrite(b)
	return r.Conn.Write(b)
}

func TestVersionNegotiation(t *testing.T) {
	// n1 advertises the current version (2), n2 only 1: they meet at 1, and
	// the classes of version 2 are refused on that link in both directions.
	c := newCluster(t, "secret", []string{"n1", "n2"}, func(name string, o *Options) {
		if name == "n2" {
			o.MaxVersion = Version1
		}
	})
	got := received(c.nodes["n2"], Consensus)
	alive := received(c.nodes["n2"], LeaseAlive)
	c.start(t)
	c.nodes["n1"].waitUp(t, "n2")
	c.nodes["n2"].waitUp(t, "n1")
	for _, n := range c.nodes {
		if v := n.t.Peers()[0].Version; v != 1 {
			t.Fatalf("%s negotiated %d", n.t.o.Node, v)
		}
	}
	if err := c.nodes["n1"].t.Send(context.Background(), "n2", Consensus, []byte("x")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "frame after negotiation", func() bool { return got.n() == 1 })
	if err := c.nodes["n1"].t.Send(context.Background(), "n2", LeaseAlive, []byte("r")); !errors.Is(err, ErrUnsupportedClass) {
		t.Fatalf("lease-alive on a version-1 link: %v", err)
	}
	if alive.n() != 0 {
		t.Fatal("a version-1 link delivered a version-2 class")
	}

	// Two current nodes meet at the current version and exchange the version-2 class.
	e := newCluster(t, "secret", []string{"p1", "p2"}, nil)
	ealive := received(e.nodes["p2"], LeaseAlive)
	e.start(t)
	e.nodes["p1"].waitUp(t, "p2")
	e.nodes["p2"].waitUp(t, "p1")
	for _, n := range e.nodes {
		if v := n.t.Peers()[0].Version; v != Version4 {
			t.Fatalf("%s negotiated %d", n.t.o.Node, v)
		}
	}
	if err := e.nodes["p1"].t.Send(context.Background(), "p2", LeaseAlive, []byte("r")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "lease-alive on a version-2 link", func() bool { return ealive.n() == 1 })

	// Disjoint ranges: a node whose minimum is 2 cannot talk to a version-1 node.
	d := newCluster(t, "secret", []string{"m1", "m2"}, func(name string, o *Options) {
		if name == "m2" {
			o.MaxVersion = Version1
		}
	})
	d.nodes["m1"].t.vmin = 2
	d.start(t)
	waitFor(t, "version refusal", func() bool {
		return d.nodes["m2"].ev.has("cluster.peer.refused", "version") || d.nodes["m1"].ev.has("cluster.peer.refused", "version")
	})
	if ps := d.nodes["m2"].t.Peers(); ps[0].State == Up {
		t.Fatal("disjoint versions connected")
	}
}

// TestUrgentLaneLeavesAheadOfTheQueue: with the writer held and the ordered
// queue filled behind it, a frame on the urgent lane is written before all
// but the frame the writer was already holding. The order is read from the
// writer: each class has its own dispatcher on the receiving side, so the
// order the handlers run in across two classes is scheduling, not the wire.
// The negative control is the same run with the renewal sent on the queued
// class itself: it is then written last.
func TestUrgentLaneLeavesAheadOfTheQueue(t *testing.T) {
	const queued = 200
	run := func(t *testing.T, class Class) int {
		t.Helper()
		c := newCluster(t, "secret", []string{"n1", "n2"}, nil)
		var mu sync.Mutex
		var wrote []string
		arrived := 0
		record := func(peer string, payload []byte) {
			mu.Lock()
			arrived++
			mu.Unlock()
		}
		c.nodes["n2"].t.Handle(Consensus, record)
		c.nodes["n2"].t.Handle(LeaseAlive, record)
		c.nodes["n1"].t.written = func(peer string, class Class, payload []byte) {
			if class == Consensus || class == LeaseAlive {
				mu.Lock()
				wrote = append(wrote, string(payload))
				mu.Unlock()
			}
		}
		c.start(t)
		c.nodes["n1"].waitUp(t, "n2")
		c.nodes["n2"].waitUp(t, "n1")
		c.nodes["n1"].t.pauseWrites.Store(true)
		for i := 0; i < queued; i++ {
			if err := c.nodes["n1"].t.Send(context.Background(), "n2", Consensus, []byte("q")); err != nil {
				t.Fatal(err)
			}
		}
		if err := c.nodes["n1"].t.Send(context.Background(), "n2", class, []byte("renewal")); err != nil {
			t.Fatal(err)
		}
		c.nodes["n1"].t.pauseWrites.Store(false)
		waitFor(t, "all frames", func() bool {
			mu.Lock()
			defer mu.Unlock()
			return arrived == queued+1
		})
		mu.Lock()
		defer mu.Unlock()
		for i, p := range wrote {
			if p == "renewal" {
				return i
			}
		}
		t.Fatal("the renewal was never written")
		return -1
	}
	// At most one ordered frame precedes it: the one the writer had already
	// taken when it was paused.
	if at := run(t, LeaseAlive); at > 1 {
		t.Fatalf("urgent frame written at position %d behind %d queued frames", at, queued)
	}
	if at := run(t, Consensus); at != queued {
		t.Fatalf("negative control: a queued-class frame was written at position %d, expected %d", at, queued)
	}
}

// TestVersion2ClassOnVersion1LinkIsAProtocolError: the receive side of a
// version-1 link closes on a version-2 class written past Send's check.
func TestVersion2ClassOnVersion1LinkIsAProtocolError(t *testing.T) {
	c := newCluster(t, "secret", []string{"n1", "n2"}, func(name string, o *Options) {
		if name == "n2" {
			o.MaxVersion = Version1
		}
	})
	c.start(t)
	c.nodes["n1"].waitUp(t, "n2")
	c.nodes["n2"].waitUp(t, "n1")
	l := c.nodes["n1"].t.peers["n2"].current(RoleControl)
	if l.version != Version1 {
		t.Fatalf("link negotiated %d", l.version)
	}
	c.nodes["n1"].t.pauseWrites.Store(true)
	if _, err := l.c.Write([]byte{0, 0, 0, 2, byte(LeaseAlive), 0}); err != nil {
		t.Fatal(err)
	}
	c.nodes["n2"].waitDown(t, "n1")
	if !c.nodes["n2"].ev.has("cluster.peer.protocol", "closed") {
		t.Fatal("no protocol audit event")
	}
}

// TestUrgentLaneRefusesWhenFull: the urgent lane never blocks a sender; past
// its depth it refuses and counts.
func TestUrgentLaneRefusesWhenFull(t *testing.T) {
	c := newCluster(t, "secret", []string{"n1", "n2"}, nil)
	c.start(t)
	c.nodes["n1"].waitUp(t, "n2")
	c.nodes["n2"].waitUp(t, "n1")
	c.nodes["n1"].t.pauseWrites.Store(true)
	time.Sleep(20 * time.Millisecond)
	refused := 0
	for i := 0; i < urgentDepth+8; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		err := c.nodes["n1"].t.Send(ctx, "n2", LeaseAlive, []byte("r"))
		cancel()
		if errors.Is(err, ErrUrgentFull) {
			refused++
		} else if err != nil {
			t.Fatal(err)
		}
	}
	// The writer may have taken one frame before pausing.
	if refused < 7 || refused > 9 {
		t.Fatalf("refused %d of %d past the lane's depth", refused, 8)
	}
	if c.nodes["n1"].ev.count("hoardmq_cluster_urgent_dropped_total", "n2") != float64(refused) {
		t.Fatal("refusals not counted")
	}
	c.nodes["n1"].t.pauseWrites.Store(false)
}

// TestSlowHandlerDoesNotSilenceThePeer is WI 1340's receiver-side control:
// a handler that holds the reader for longer than the silence limit must
// not make the peer look silent. Before dispatch moved off the reader the
// heartbeat behind the frame went unread and the peer was declared down.
func TestSlowHandlerDoesNotSilenceThePeer(t *testing.T) {
	c := newCluster(t, "secret", []string{"n1", "n2"}, nil)
	release := make(chan struct{})
	c.nodes["n2"].t.Handle(RouteUpdate, func(peer string, payload []byte) { <-release })
	c.start(t)
	c.nodes["n1"].waitUp(t, "n2")
	c.nodes["n2"].waitUp(t, "n1")
	if err := c.nodes["n1"].t.Send(context.Background(), "n2", RouteUpdate, []byte("slow")); err != nil {
		t.Fatal(err)
	}
	// The handler holds for six silence limits (100 ms x 3 in this cluster).
	// A reset shows on the dialer's side (n1 sees the close and redials) and
	// as a second peer.up on either side; the acceptor's old reader is still
	// parked in the handler, so its own peer state is not the evidence.
	time.Sleep(1800 * time.Millisecond)
	for _, n := range []string{"n1", "n2"} {
		if c.nodes[n].ev.has("cluster.peer.down", "closed") {
			t.Fatalf("%s saw the link close while n2's handler was busy: %v", n, c.nodes[n].ev.audit)
		}
		if ups := c.nodes[n].ev.countAudit("cluster.peer.up"); ups != 1 {
			t.Fatalf("%s saw %d peer.up events (a redial) while n2's handler was busy", n, ups)
		}
	}
	// Control traffic still flows both ways while the data handler holds.
	got := received(c.nodes["n2"], LeaseAlive)
	if err := c.nodes["n1"].t.Send(context.Background(), "n2", LeaseAlive, []byte("renew")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "a renewal past a busy handler", func() bool { return got.n() == 1 })
	close(release)
}

// TestTwoConnectionsAreIndependent: between version-3 peers a control frame
// gets through while the data writer is held, and a data frame while the
// control writer is held.
func TestTwoConnectionsAreIndependent(t *testing.T) {
	c := newCluster(t, "secret", []string{"n1", "n2"}, nil)
	ctl := received(c.nodes["n2"], LeaseNotification)
	data := received(c.nodes["n2"], ForwardedPublish)
	c.start(t)
	c.nodes["n1"].waitUp(t, "n2")
	c.nodes["n2"].waitUp(t, "n1")
	ps := c.nodes["n1"].t.Peers()
	if ps[0].Version != Version4 || ps[0].State != Up || ps[0].DataState != Up {
		t.Fatalf("peer after start: %+v", ps[0])
	}
	c.nodes["n1"].t.pauseRole[RoleData].Store(true)
	if err := c.nodes["n1"].t.Send(context.Background(), "n2", LeaseNotification, []byte("ctl")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "control frame past a held data writer", func() bool { return ctl.n() == 1 })
	c.nodes["n1"].t.pauseRole[RoleData].Store(false)
	c.nodes["n1"].t.pauseRole[RoleControl].Store(true)
	if err := c.nodes["n1"].t.Send(context.Background(), "n2", ForwardedPublish, []byte("data")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "data frame past a held control writer", func() bool { return data.n() == 1 })
	c.nodes["n1"].t.pauseRole[RoleControl].Store(false)
}

// TestAdminClassesNeedVersion4: a link negotiated at 3 refuses the admin
// classes on the sending side and stays up; at 4 they are delivered.
func TestAdminClassesNeedVersion4(t *testing.T) {
	old := newCluster(t, "secret", []string{"n1", "n2"}, func(name string, o *Options) {
		if name == "n2" {
			o.MaxVersion = Version3
		}
	})
	refused := received(old.nodes["n2"], AdminRequest)
	routes := received(old.nodes["n2"], RouteUpdate)
	old.start(t)
	old.nodes["n1"].waitUp(t, "n2")
	old.nodes["n2"].waitUp(t, "n1")
	if v := old.nodes["n1"].t.Peers()[0].Version; v != Version3 {
		t.Fatalf("negotiated %d with a version-3 peer", v)
	}
	if err := old.nodes["n1"].t.Send(context.Background(), "n2", AdminRequest, []byte("req")); !errors.Is(err, ErrUnsupportedClass) {
		t.Fatalf("admin request on a version-3 link: %v", err)
	}
	if err := old.nodes["n1"].t.Send(context.Background(), "n2", RouteUpdate, []byte("after")); err != nil {
		t.Fatalf("the link after a refused admin request: %v", err)
	}
	waitFor(t, "a frame after the refusal", func() bool { return routes.n() == 1 })
	if refused.n() != 0 {
		t.Fatal("a version-3 link delivered an admin request")
	}

	cur := newCluster(t, "secret", []string{"p1", "p2"}, nil)
	req := received(cur.nodes["p2"], AdminRequest)
	ans := received(cur.nodes["p1"], AdminAnswer)
	cur.start(t)
	cur.nodes["p1"].waitUp(t, "p2")
	cur.nodes["p2"].waitUp(t, "p1")
	if err := cur.nodes["p1"].t.Send(context.Background(), "p2", AdminRequest, []byte("req")); err != nil {
		t.Fatal(err)
	}
	if err := cur.nodes["p2"].t.Send(context.Background(), "p1", AdminAnswer, []byte("ans")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "both admin classes at version 4", func() bool { return req.n() == 1 && ans.n() == 1 })
}

// TestVersion2PeerGetsOneConnection: a peer that negotiates version 2 is
// served on one link carrying every class; the version-3 classes are
// refused to it.
func TestVersion2PeerGetsOneConnection(t *testing.T) {
	c := newCluster(t, "secret", []string{"n1", "n2"}, func(name string, o *Options) {
		if name == "n2" {
			o.MaxVersion = Version2
		}
	})
	data := received(c.nodes["n2"], ForwardedPublish)
	alive := received(c.nodes["n2"], LeaseAlive)
	c.start(t)
	c.nodes["n1"].waitUp(t, "n2")
	c.nodes["n2"].waitUp(t, "n1")
	time.Sleep(300 * time.Millisecond) // any second dial would have landed
	for _, n := range c.nodes {
		ps := n.t.Peers()
		if ps[0].Version != Version2 || ps[0].State != Up || ps[0].DataState != Up {
			t.Fatalf("%s: %+v", n.t.o.Node, ps[0])
		}
		p := n.t.peers[otherOf(n.t.o.Node)]
		if p.current(RoleControl) != p.current(RoleData) {
			t.Fatalf("%s holds two links to a version-2 peer", n.t.o.Node)
		}
		if ups := n.ev.countAudit("cluster.peer.up"); ups != 1 {
			t.Fatalf("%s saw %d peer.up events", n.t.o.Node, ups)
		}
	}
	if err := c.nodes["n1"].t.Send(context.Background(), "n2", ForwardedPublish, []byte("d")); err != nil {
		t.Fatal(err)
	}
	if err := c.nodes["n1"].t.Send(context.Background(), "n2", LeaseAlive, []byte("a")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "both classes on the one link", func() bool { return data.n() == 1 && alive.n() == 1 })
	if err := c.nodes["n1"].t.Send(context.Background(), "n2", ConsensusSnapshot, []byte("s")); !errors.Is(err, ErrUnsupportedClass) {
		t.Fatalf("snapshot class to a version-2 peer: %v", err)
	}
}

// TestMissingRoleFrameIsRefused: a version-3 dialer that never names its
// connection's role is refused within the handshake timeout.
func TestMissingRoleFrameIsRefused(t *testing.T) {
	c := newCluster(t, "secret", []string{"n1", "n2"}, func(name string, o *Options) { o.HandshakeTimeout = 500 * time.Millisecond })
	c.start(t)
	c.nodes["n1"].waitUp(t, "n2")
	c.nodes["n2"].waitUp(t, "n1")
	// A raw connection from n1's identity to n2 that completes the handshake
	// and then says nothing.
	dialer := c.nodes["n1"].t
	conn, err := net.Dial("tcp", c.nodes["n2"].t.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := dialer.handshake(conn, true); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "protocol refusal", func() bool { return c.nodes["n2"].ev.has("cluster.peer.refused", "protocol") })
	if ps := c.nodes["n2"].t.Peers(); ps[0].State != Up {
		t.Fatalf("the established pair was disturbed: %+v", ps[0])
	}
}

// TestDataConnectionResetKeepsThePeerUp: cutting the data connection fires
// the data observers and is redialed while the peer stays up and control
// frames keep flowing; cutting the control connection takes the peer down.
func TestDataConnectionResetKeepsThePeerUp(t *testing.T) {
	c := newCluster(t, "secret", []string{"n1", "n2"}, nil)
	ctl := received(c.nodes["n2"], LeaseNotification)
	c.start(t)
	c.nodes["n1"].waitUp(t, "n2")
	c.nodes["n2"].waitUp(t, "n1")
	n1, n2 := c.nodes["n1"], c.nodes["n2"]
	ups0, dns0 := n1.dataTransitions("n2")
	n1.t.peers["n2"].current(RoleData).close(errors.New("cut by the test"))
	waitFor(t, "data down then up on n1", func() bool {
		ups, dns := n1.dataTransitions("n2")
		return dns == dns0+1 && ups == ups0+1
	})
	if ps := n1.t.Peers(); ps[0].State != Up {
		t.Fatalf("peer went %s on a data reset", ps[0].State)
	}
	if n1.ev.has("cluster.peer.down", "closed") {
		t.Fatal("a data reset was reported as the peer going down")
	}
	if err := n1.t.Send(context.Background(), "n2", LeaseNotification, []byte("still here")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "control frame after a data reset", func() bool { return ctl.n() == 1 })
	// Control cut: the peer goes down and comes back with both connections.
	n1.t.peers["n2"].current(RoleControl).close(errors.New("cut by the test"))
	n1.waitDown(t, "n2")
	n1.waitUp(t, "n2")
	if ps := n1.t.Peers(); ps[0].State != Up || ps[0].DataState != Up {
		t.Fatalf("after a control reset: %+v", ps[0])
	}
	_ = n2
}

func TestSendErrors(t *testing.T) {
	c := newCluster(t, "secret", []string{"n1", "n2"}, nil)
	c.start(t)
	c.nodes["n1"].waitUp(t, "n2")
	tr := c.nodes["n1"].t
	if err := tr.Send(context.Background(), "nobody", Consensus, nil); !errors.Is(err, ErrUnknownPeer) {
		t.Fatalf("unknown peer: %v", err)
	}
	if err := tr.Send(context.Background(), "n2", Heartbeat, nil); err == nil {
		t.Fatal("heartbeat class accepted from a caller")
	}
	c.nodes["n2"].t.Stop()
	c.nodes["n1"].waitDown(t, "n2")
	if err := tr.Send(context.Background(), "n2", Consensus, nil); !errors.Is(err, ErrPeerDown) {
		t.Fatalf("down peer: %v", err)
	}
	tr.Stop()
	if err := tr.Send(context.Background(), "n2", Consensus, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed transport: %v", err)
	}
}

// TestPeerInfoShowsFramesQueuedBehindAPausedWriter: frames admitted while the
// writer is held appear in the sender's queue length, and the queue empties
// once the writer resumes (WI 1474). Without the figure, a frame that never
// left the sender and a frame the peer never read look the same.
func TestPeerInfoShowsFramesQueuedBehindAPausedWriter(t *testing.T) {
	const queued = 50
	c := newCluster(t, "secret", []string{"n1", "n2"}, nil)
	var got atomic.Int64
	c.nodes["n2"].t.Handle(Consensus, func(string, []byte) { got.Add(1) })
	c.start(t)
	c.nodes["n1"].waitUp(t, "n2")
	c.nodes["n2"].waitUp(t, "n1")
	c.nodes["n1"].t.pauseWrites.Store(true)
	for i := 0; i < queued; i++ {
		if err := c.nodes["n1"].t.Send(context.Background(), "n2", Consensus, []byte("q")); err != nil {
			t.Fatal(err)
		}
	}
	peerInfo := func() PeerInfo {
		for _, p := range c.nodes["n1"].t.Peers() {
			if p.Name == "n2" {
				return p
			}
		}
		t.Fatal("no n2 in n1's peers")
		return PeerInfo{}
	}
	waitFor(t, "frames queued behind the paused writer", func() bool { return peerInfo().OutQueued >= queued-1 })
	c.nodes["n1"].t.pauseWrites.Store(false)
	waitFor(t, "the frames delivered", func() bool { return got.Load() == queued })
	if q := peerInfo().OutQueued; q != 0 {
		t.Fatalf("out queue %d after every frame was delivered", q)
	}
}

// TestControlUpIsReportedBeforeThePeerIsUp: a peer whose control connection
// is in place and whose data connection is not yet — a restarted peer between
// its two dials — reads ControlUp while not Up; a live pair reads both; a
// stopped peer reads neither.
func TestControlUpIsReportedBeforeThePeerIsUp(t *testing.T) {
	p := newPeer(config.Peer{Name: "n2", Address: "127.0.0.1:1"})
	p.links[RoleControl] = &link{}
	p.state = Connecting
	if pi := p.info(); !pi.ControlUp || pi.State == Up {
		t.Fatalf("control link only: ControlUp %v, State %s; want true, not Up", pi.ControlUp, pi.State)
	}
	p.links[RoleControl] = nil
	if pi := p.info(); pi.ControlUp {
		t.Fatal("ControlUp with no control link")
	}

	c := newCluster(t, "secret", []string{"n1", "n2"}, nil)
	c.start(t)
	c.nodes["n1"].waitUp(t, "n2")
	if ps := c.nodes["n1"].t.Peers(); !ps[0].ControlUp || ps[0].State != Up {
		t.Fatalf("live pair: %+v", ps[0])
	}
	c.nodes["n2"].t.Stop()
	c.nodes["n1"].waitDown(t, "n2")
	if ps := c.nodes["n1"].t.Peers(); ps[0].ControlUp {
		t.Fatalf("stopped peer reads ControlUp: %+v", ps[0])
	}
}
