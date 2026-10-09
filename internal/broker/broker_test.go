package broker

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/testcert"
)

type fixture struct {
	t       *testing.T
	srv     *Server
	ca      *testcert.CA
	dir     string
	client  *testcert.Leaf
	stopped bool
}

// stop shuts the server down mid-test; the cleanup then leaves it alone.
func (f *fixture) stop() {
	f.t.Helper()
	if f.stopped {
		return
	}
	f.stopped = true
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := f.srv.Shutdown(ctx); err != nil {
		f.t.Errorf("shutdown: %v", err)
	}
}

// fixtureHooks are the seams every node started next is built with. A test
// that sets them restores the zero value in a cleanup and does not run in
// parallel.
var fixtureHooks Hooks

func start(t *testing.T, mutate func(*config.Document)) *fixture {
	t.Helper()
	f, err := tryStart(t, mutate)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// tryStart is start returning the server's start error instead of failing
// the test, for a caller that can start again on new addresses.
func tryStart(t *testing.T, mutate func(*config.Document)) (*fixture, error) {
	t.Helper()
	dir := t.TempDir()
	ca, err := testcert.NewCA("ca")
	if err != nil {
		t.Fatal(err)
	}
	server, _ := ca.Issue("server", 2)
	certFile, keyFile, _ := server.Write(dir, "server")
	caFile, _ := ca.WriteCA(dir)
	client, _ := ca.Issue("gateway-1", 3)

	doc := config.Default()
	doc.Node.DataDir = filepath.Join(dir, "data")
	doc.Auth.Anonymous = true
	doc.Limits.ConnectTimeout = 2
	// A fast consensus clock so a one-member group elects itself at once.
	doc.Cluster.Raft.TickMs = 10
	doc.Cluster.Raft.JoinTimeoutMs = 1000
	// Tests expect a fresh node ready at once; the readiness test that owns
	// the stability condition sets its own interval.
	doc.Cluster.Readiness.StableMs = 0
	doc.Listeners = []config.Listener{
		{Name: "tcp", Type: "tcp", Address: "127.0.0.1:0"},
		{Name: "tls", Type: "tls", Address: "127.0.0.1:0", TLS: &config.TLS{CertFile: certFile, KeyFile: keyFile}},
		{Name: "mtls", Type: "tls", Address: "127.0.0.1:0", TLS: &config.TLS{CertFile: certFile, KeyFile: keyFile, ClientCAFile: caFile}},
		{Name: "ws", Type: "ws", Address: "127.0.0.1:0"},
		{Name: "wss", Type: "wss", Address: "127.0.0.1:0", TLS: &config.TLS{CertFile: certFile, KeyFile: keyFile}},
	}
	acl := filepath.Join(dir, "acl.json")
	if err := os.WriteFile(acl, []byte(`{"rules":[{"topic":"#","allow":["publish","subscribe","unsubscribe"]}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	doc.Modules = map[string]json.RawMessage{"authz.file": json.RawMessage(`{"path":"` + acl + `"}`)}
	if mutate != nil {
		mutate(&doc)
	}
	if p := doc.Validate(); len(p) != 0 {
		t.Fatalf("config: %v", p)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if os.Getenv("HOARDMQ_TEST_LOG") != "" {
		// Debug logs for a failing cluster test, prefixed with the node name.
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})).With("node", doc.Node.Name)
	}
	srv, err := NewWithHooks(doc, logger, fixtureHooks)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start(); err != nil {
		return nil, err
	}
	// Readiness includes the consensus core; wait for it unless the test
	// wires a multi-node cluster itself (peers configured).
	if len(doc.Cluster.Peers) == 0 && doc.Cluster.Discovery.Mode != "dns" {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) && !srv.Consensus().Ready() {
			time.Sleep(10 * time.Millisecond)
		}
		if !srv.Consensus().Ready() {
			t.Fatalf("consensus core not ready: %+v", srv.Consensus().Status())
		}
	}
	f := &fixture{t: t, srv: srv, ca: ca, dir: dir, client: client}
	t.Cleanup(func() {
		if f.stopped {
			return
		}
		f.stopped = true
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			t.Errorf("shutdown: %v", err)
		}
	})
	return f, nil
}

func (f *fixture) tlsConfig(withClientCert bool) *tls.Config {
	pool := x509.NewCertPool()
	pool.AddCert(f.ca.Cert)
	cfg := &tls.Config{RootCAs: pool, ServerName: "localhost"}
	if withClientCert {
		cert, _ := tls.X509KeyPair(f.client.CertPEM, f.client.KeyPEM)
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg
}

// dial opens a transport to the named listener.
func (f *fixture) dial(name string, withClientCert bool) net.Conn {
	f.t.Helper()
	addr := f.srv.Addr(name).String()
	switch name {
	case "tcp":
		c, err := net.Dial("tcp", addr)
		if err != nil {
			f.t.Fatal(err)
		}
		return c
	case "tls", "mtls":
		cfg := f.tlsConfig(withClientCert)
		cfg.NextProtos = []string{"mqtt"}
		c, err := tls.Dial("tcp", addr, cfg)
		if err != nil {
			f.t.Fatal(err)
		}
		return c
	case "ws":
		c, err := net.Dial("tcp", addr)
		if err != nil {
			f.t.Fatal(err)
		}
		return wsClient(f.t, c)
	case "wss":
		cfg := f.tlsConfig(withClientCert)
		cfg.NextProtos = []string{"http/1.1"}
		c, err := tls.Dial("tcp", addr, cfg)
		if err != nil {
			f.t.Fatal(err)
		}
		return wsClient(f.t, c)
	}
	f.t.Fatalf("unknown listener %s", name)
	return nil
}

// wsClient performs the upgrade and returns a net.Conn speaking masked
// binary frames.
func wsClient(t *testing.T, raw net.Conn) net.Conn {
	t.Helper()
	req := "GET /mqtt HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Protocol: mqtt\r\n\r\n"
	if _, err := raw.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(raw)
	resp, err := http.ReadResponse(br, nil)
	if err != nil || resp.StatusCode != 101 {
		t.Fatalf("upgrade: %v %v", err, resp)
	}
	return &wsClientConn{Conn: raw, r: br}
}

type wsClientConn struct {
	net.Conn
	r        *bufio.Reader
	leftover []byte
}

func (c *wsClientConn) Read(p []byte) (int, error) {
	for len(c.leftover) == 0 {
		var h [2]byte
		if _, err := io.ReadFull(c.r, h[:]); err != nil {
			return 0, err
		}
		n := uint64(h[1] & 0x7F)
		switch n {
		case 126:
			var e [2]byte
			if _, err := io.ReadFull(c.r, e[:]); err != nil {
				return 0, err
			}
			n = uint64(binary.BigEndian.Uint16(e[:]))
		case 127:
			var e [8]byte
			if _, err := io.ReadFull(c.r, e[:]); err != nil {
				return 0, err
			}
			n = binary.BigEndian.Uint64(e[:])
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(c.r, payload); err != nil {
			return 0, err
		}
		switch h[0] & 0x0F {
		case 0x2, 0x0:
			c.leftover = payload
		case 0x8:
			return 0, io.EOF
		}
	}
	n := copy(p, c.leftover)
	c.leftover = c.leftover[n:]
	return n, nil
}

func (c *wsClientConn) Write(p []byte) (int, error) {
	h := []byte{0x82}
	switch n := len(p); {
	case n < 126:
		h = append(h, byte(n)|0x80)
	case n < 65536:
		h = append(h, 126|0x80, byte(n>>8), byte(n))
	default:
		h = append(h, 127|0x80)
		h = binary.BigEndian.AppendUint64(h, uint64(n))
	}
	key := [4]byte{9, 8, 7, 6}
	h = append(h, key[:]...)
	for i, b := range p {
		h = append(h, b^key[i%4])
	}
	if _, err := c.Conn.Write(h); err != nil {
		return 0, err
	}
	return len(p), nil
}

// client is a minimal MQTT client over the codec.
type client struct {
	t   *testing.T
	c   net.Conn
	enc *packet.Encoder
	dec *packet.Decoder
}

func (f *fixture) connect(name string, v packet.Version, connect *packet.Connect, withClientCert bool) (*client, *packet.Connack) {
	f.t.Helper()
	nc := f.dial(name, withClientCert)
	cl := &client{t: f.t, c: nc, enc: &packet.Encoder{Version: v}, dec: packet.NewDecoder(nc)}
	cl.dec.Version = v
	f.t.Cleanup(func() { _ = nc.Close() })
	connect.ProtocolLevel = v
	cl.send(connect)
	ack, ok := cl.read().(*packet.Connack)
	if !ok {
		f.t.Fatal("no CONNACK")
	}
	return cl, ack
}

func (c *client) send(p packet.Packet) {
	c.t.Helper()
	_ = c.c.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if err := c.enc.Encode(c.c, p); err != nil {
		c.t.Fatalf("send: %v", err)
	}
}

func (c *client) read() packet.Packet {
	c.t.Helper()
	_ = c.c.SetReadDeadline(time.Now().Add(2 * time.Second))
	p, err := c.dec.Read()
	if err != nil {
		c.t.Fatalf("read: %v", err)
	}
	return p
}

func TestEveryTransportEveryLevel(t *testing.T) {
	f := start(t, nil)
	for _, name := range []string{"tcp", "tls", "mtls", "ws", "wss"} {
		for _, v := range []packet.Version{packet.V3, packet.V4, packet.V5} {
			t.Run(name+"/"+v.String(), func(t *testing.T) {
				cl, ack := f.connect(name, v, &packet.Connect{ClientID: "c-" + name, CleanStart: true, KeepAlive: 30}, name == "mtls")
				if ack.ReasonCode != packet.Success {
					t.Fatalf("connack %s", ack.ReasonCode)
				}
				cl.send(&packet.Pingreq{})
				if _, ok := cl.read().(*packet.Pingresp); !ok {
					t.Fatal("no PINGRESP")
				}
				cl.send(&packet.Publish{Topic: "a/b", Payload: []byte("q0")})
				cl.send(&packet.Publish{Topic: "a/b", QoS: 1, PacketID: 1, Payload: []byte("q1")})
				if a, ok := cl.read().(*packet.Puback); !ok || a.PacketID != 1 {
					t.Fatal("no PUBACK")
				}
				cl.send(&packet.Publish{Topic: "a/b", QoS: 2, PacketID: 2, Payload: []byte("q2")})
				if _, ok := cl.read().(*packet.Pubrec); !ok {
					t.Fatal("no PUBREC")
				}
				cl.send(&packet.Pubrel{Ack: packet.Ack{PacketID: 2}})
				if _, ok := cl.read().(*packet.Pubcomp); !ok {
					t.Fatal("no PUBCOMP")
				}
				cl.send(&packet.Subscribe{PacketID: 3, Subscriptions: []packet.Subscription{{Filter: "a/#", QoS: 2}}})
				if s, ok := cl.read().(*packet.Suback); !ok || s.ReasonCodes[0] != packet.GrantedQoS2 {
					t.Fatalf("suback %+v", s)
				}
				cl.send(&packet.Unsubscribe{PacketID: 4, Filters: []string{"a/#", "never"}})
				if u, ok := cl.read().(*packet.Unsuback); !ok || (v == packet.V5 && (u.ReasonCodes[0] != packet.Success || u.ReasonCodes[1] != packet.NoSubscriptionExisted)) {
					t.Fatalf("unsuback %+v", u)
				}
				cl.send(&packet.Disconnect{})
			})
		}
	}
}

func TestClientCertificateIdentity(t *testing.T) {
	f := start(t, func(d *config.Document) {
		d.Auth.Anonymous = false
		acl := filepath.Join(t.TempDir(), "acl.json")
		_ = os.WriteFile(acl, []byte(`{"rules":[{"principal":{"cn":"gateway-1"},"topic":"gw/${cn}/#","allow":["publish"]}]}`), 0o600)
		d.Modules["authz.file"] = json.RawMessage(`{"path":"` + acl + `"}`)
	})
	// A verified certificate authenticates on its own; the common name is
	// the identity the ACL template sees.
	cl, ack := f.connect("mtls", packet.V5, &packet.Connect{ClientID: "gw", CleanStart: true}, true)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("verified certificate: %s", ack.ReasonCode)
	}
	cl.send(&packet.Publish{Topic: "gw/gateway-1/x", QoS: 1, PacketID: 1})
	if a := cl.read().(*packet.Puback); a.ReasonCode != packet.NoMatchingSubscribers {
		t.Fatalf("own topic (authorized, nobody subscribed): %s", a.ReasonCode)
	}
	cl.send(&packet.Publish{Topic: "gw/other/x", QoS: 1, PacketID: 2})
	if a := cl.read().(*packet.Puback); a.ReasonCode != packet.NotAuthorized {
		t.Fatalf("other topic: %s", a.ReasonCode)
	}
	// A certificate plus a password nobody checks is refused.
	_, ack = f.connect("mtls", packet.V5, &packet.Connect{ClientID: "gw2", CleanStart: true, HasUsername: true, Username: "u", HasPassword: true, Password: []byte("p")}, true)
	if ack.ReasonCode != packet.BadUserNameOrPassword {
		t.Fatalf("certificate with unchecked password: %s", ack.ReasonCode)
	}
	// Plain anonymous is refused on this server.
	_, ack = f.connect("tcp", packet.V5, &packet.Connect{ClientID: "anon", CleanStart: true}, false)
	if ack.ReasonCode != packet.NotAuthorized {
		t.Fatalf("anonymous: %s", ack.ReasonCode)
	}
}

func TestFileAuthenticationEndToEnd(t *testing.T) {
	phc, _ := auth.HashPassword([]byte("pw"))
	users := filepath.Join(t.TempDir(), "users.json")
	_ = os.WriteFile(users, []byte(`{"users":[{"username":"alice","password":"`+phc+`"}]}`), 0o600)
	f := start(t, func(d *config.Document) {
		d.Auth.Anonymous = false
		d.Modules["auth.file"] = json.RawMessage(`{"path":"` + users + `"}`)
	})
	_, ack := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "a", CleanStart: true, HasUsername: true, Username: "alice", HasPassword: true, Password: []byte("pw")}, false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("good login: %s", ack.ReasonCode)
	}
	_, ack = f.connect("tcp", packet.V5, &packet.Connect{ClientID: "a", CleanStart: true, HasUsername: true, Username: "alice", HasPassword: true, Password: []byte("no")}, false)
	if ack.ReasonCode != packet.BadUserNameOrPassword {
		t.Fatalf("bad password: %s", ack.ReasonCode)
	}
	_, ack = f.connect("tcp", packet.V4, &packet.Connect{ClientID: "a", CleanStart: true}, false)
	if ack.ReasonCode != packet.NotAuthorized {
		t.Fatalf("anonymous: %s", ack.ReasonCode)
	}
}

func TestDefaultDenyWithoutACL(t *testing.T) {
	f := start(t, func(d *config.Document) { delete(d.Modules, "authz.file") })
	cl, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "d", CleanStart: true}, false)
	cl.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "x"}}})
	if s := cl.read().(*packet.Suback); s.ReasonCodes[0] != packet.NotAuthorized {
		t.Fatalf("%+v", s)
	}
}

func TestShutdownTellsClients(t *testing.T) {
	f := start(t, nil)
	cl, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "s", CleanStart: true}, false)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := f.srv.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if d, ok := cl.read().(*packet.Disconnect); !ok || d.ReasonCode != packet.ServerShuttingDown {
		t.Fatalf("got %+v", d)
	}
	if _, err := net.Dial("tcp", f.srv.Addr("tcp").String()); err == nil {
		t.Error("listener still accepting after shutdown")
	}
}

func TestStartIsAllOrNothing(t *testing.T) {
	blocker, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Close() }()
	dir := t.TempDir()
	doc := config.Default()
	doc.Node.DataDir = filepath.Join(dir, "data")
	doc.Listeners = []config.Listener{
		{Name: "ok", Type: "tcp", Address: "127.0.0.1:0"},
		{Name: "taken", Type: "tcp", Address: blocker.Addr().String()},
	}
	srv, err := New(doc, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start(); err == nil {
		t.Fatal("expected bind failure")
	}
	if _, err := os.Stat(doc.Node.DataDir); !os.IsNotExist(err) {
		t.Error("data directory created despite bind failure")
	}
	if srv.Addr("ok") != nil {
		t.Error("first listener left open")
	}
	if err := srv.Shutdown(context.Background()); err != nil {
		t.Error(err)
	}
}

func TestModuleConfigErrors(t *testing.T) {
	doc := config.Default()
	doc.Node.DataDir = t.TempDir()
	for name, raw := range map[string]string{
		"unknown key":  `{"path":"x","extra":1}`,
		"missing path": `{}`,
		"missing file": `{"path":"/nonexistent/users.json"}`,
	} {
		doc.Modules = map[string]json.RawMessage{"auth.file": json.RawMessage(raw)}
		if _, err := New(doc, nil); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
