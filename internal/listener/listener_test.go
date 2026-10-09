package listener

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/testcert"
)

func open(t *testing.T, cfg config.Listener) *Listener {
	t.Helper()
	if cfg.Address == "" {
		cfg.Address = "127.0.0.1:0"
	}
	if cfg.Name == "" {
		cfg.Name = cfg.Type
	}
	l, err := Open(cfg, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

// accept runs Accept in the background and returns the connection or fails.
func accept(t *testing.T, l *Listener) net.Conn {
	t.Helper()
	ch := make(chan net.Conn, 1)
	go func() {
		c, err := l.Accept()
		if err != nil {
			t.Error(err)
			close(ch)
			return
		}
		ch <- c
	}()
	select {
	case c := <-ch:
		if c == nil {
			t.FailNow()
		}
		return c
	case <-time.After(3 * time.Second):
		t.Fatal("accept timed out")
	}
	return nil
}

func TestTCPEcho(t *testing.T) {
	l := open(t, config.Listener{Type: "tcp"})
	client, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	server := accept(t, l)
	defer func() { _ = server.Close() }()
	if _, err := client.Write([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 2)
	if _, err := io.ReadFull(server, buf); err != nil || string(buf) != "hi" {
		t.Fatalf("read %q %v", buf, err)
	}
	if l.Active() != 1 {
		t.Errorf("active %d", l.Active())
	}
	_ = server.Close()
	_ = server.Close()
	if l.Active() != 0 {
		t.Errorf("active after close %d", l.Active())
	}
}

func TestMaxConnections(t *testing.T) {
	l := open(t, config.Listener{Type: "tcp", MaxConnections: 1})
	c1, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c1.Close() }()
	s1 := accept(t, l)
	defer func() { _ = s1.Close() }()
	c2, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c2.Close() }()
	// The second connection must be closed by the listener: a read on it ends.
	go func() {
		_, _ = l.Accept()
	}()
	_ = c2.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := c2.Read(make([]byte, 1)); err == nil {
		t.Fatal("surplus connection stayed open")
	}
}

func TestAcceptAfterClose(t *testing.T) {
	l := open(t, config.Listener{Type: "tcp"})
	_ = l.Close()
	if _, err := l.Accept(); err != ErrClosed {
		t.Fatalf("got %v", err)
	}
	w := open(t, config.Listener{Type: "ws"})
	_ = w.Close()
	if _, err := w.Accept(); err != ErrClosed {
		t.Fatalf("ws: got %v", err)
	}
}

func TestBindFailure(t *testing.T) {
	l := open(t, config.Listener{Type: "tcp"})
	if _, err := Open(config.Listener{Name: "dup", Type: "tcp", Address: l.Addr().String()}, 0); err == nil {
		t.Fatal("expected bind failure")
	}
}

func tlsFixtures(t *testing.T) (dir string, ca *testcert.CA, certFile, keyFile, caFile string) {
	t.Helper()
	dir = t.TempDir()
	ca, err := testcert.NewCA("test-ca")
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := ca.Issue("server", 2)
	if err != nil {
		t.Fatal(err)
	}
	certFile, keyFile, err = leaf.Write(dir, "server")
	if err != nil {
		t.Fatal(err)
	}
	caFile, err = ca.WriteCA(dir)
	if err != nil {
		t.Fatal(err)
	}
	return dir, ca, certFile, keyFile, caFile
}

func clientTLS(ca *testcert.CA, leaf *testcert.Leaf) *tls.Config {
	pool := x509.NewCertPool()
	pool.AddCert(ca.Cert)
	cfg := &tls.Config{RootCAs: pool, ServerName: "localhost", NextProtos: []string{"mqtt"}}
	if leaf != nil {
		cert, _ := tls.X509KeyPair(leaf.CertPEM, leaf.KeyPEM)
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg
}

// tlsPair dials the listener with the client configuration while the server
// side completes its handshake, which only happens on an explicit Handshake
// or a first read.
func tlsPair(t *testing.T, l *Listener, cfg *tls.Config) (*tls.Conn, net.Conn, error) {
	t.Helper()
	type dialed struct {
		c   *tls.Conn
		err error
	}
	ch := make(chan dialed, 1)
	go func() {
		c, err := tls.Dial("tcp", l.Addr().String(), cfg)
		ch <- dialed{c, err}
	}()
	server := accept(t, l)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	herr := Handshake(ctx, server)
	d := <-ch
	if herr != nil {
		if d.c != nil {
			_ = d.c.Close()
		}
		_ = server.Close()
		return nil, nil, herr
	}
	if d.err != nil {
		_ = server.Close()
		return nil, nil, d.err
	}
	return d.c, server, nil
}

func TestTLSWithAndWithoutClientCertificate(t *testing.T) {
	_, ca, certFile, keyFile, caFile := tlsFixtures(t)
	// Optional client certificate.
	l := open(t, config.Listener{Type: "tls", TLS: &config.TLS{CertFile: certFile, KeyFile: keyFile}})
	client, server, err := tlsPair(t, l, clientTLS(ca, nil))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(server, make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	if client.ConnectionState().NegotiatedProtocol != "mqtt" {
		t.Error("ALPN mqtt not negotiated")
	}
	if PeerCertificate(server) != nil {
		t.Error("no client certificate expected")
	}
	_ = client.Close()
	_ = server.Close()

	// Required client certificate.
	req := open(t, config.Listener{Type: "tls", TLS: &config.TLS{CertFile: certFile, KeyFile: keyFile, ClientCAFile: caFile, MinVersion: "1.3"}})
	leaf, _ := ca.Issue("device-7", 3)
	client, server, err = tlsPair(t, req, clientTLS(ca, leaf))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(server, make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	if pc := PeerCertificate(server); pc == nil || pc.Subject.CommonName != "device-7" {
		t.Errorf("peer certificate %v", pc)
	}
	if client.ConnectionState().Version != tls.VersionTLS13 {
		t.Error("expected TLS 1.3")
	}
	_ = client.Close()
	_ = server.Close()

	// Without a certificate the server-side handshake fails.
	if _, _, err := tlsPair(t, req, clientTLS(ca, nil)); err == nil {
		t.Fatal("connection without client certificate accepted")
	}
}

func TestTLSConfigErrors(t *testing.T) {
	dir, _, certFile, keyFile, _ := tlsFixtures(t)
	if _, err := Open(config.Listener{Name: "x", Type: "tls", Address: "127.0.0.1:0", TLS: &config.TLS{CertFile: certFile, KeyFile: certFile}}, 0); err == nil {
		t.Error("mismatched key accepted")
	}
	bad := dir + "/nope.pem"
	if _, err := Open(config.Listener{Name: "x", Type: "tls", Address: "127.0.0.1:0", TLS: &config.TLS{CertFile: certFile, KeyFile: keyFile, ClientCAFile: bad}}, 0); err == nil {
		t.Error("missing client CA accepted")
	}
	empty := dir + "/empty.pem"
	_ = os.WriteFile(empty, []byte("nothing"), 0o600)
	if _, err := Open(config.Listener{Name: "x", Type: "tls", Address: "127.0.0.1:0", TLS: &config.TLS{CertFile: certFile, KeyFile: keyFile, ClientCAFile: empty}}, 0); err == nil {
		t.Error("empty client CA accepted")
	}
}

func TestCertificateReload(t *testing.T) {
	_, ca, certFile, keyFile, _ := tlsFixtures(t)
	loader, err := newCertLoader(certFile, keyFile)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	loader.now = func() time.Time { return now }
	first, _ := loader.get(nil)
	// Same second: no re-check even if the files changed.
	leaf2, _ := ca.Issue("server-2", 4)
	if _, _, err := leaf2.Write(strings.TrimSuffix(certFile, "/server-cert.pem"), "server"); err != nil {
		t.Fatal(err)
	}
	future := now.Add(2 * time.Second)
	_ = os.Chtimes(certFile, future, future)
	_ = os.Chtimes(keyFile, future, future)
	if c, _ := loader.get(nil); c != first {
		t.Fatal("reloaded within the same second")
	}
	now = now.Add(2 * time.Second)
	c, _ := loader.get(nil)
	if c == first {
		t.Fatal("certificate not reloaded")
	}
	parsed, _ := x509.ParseCertificate(c.Certificate[0])
	if parsed.Subject.CommonName != "server-2" {
		t.Errorf("reloaded CN %s", parsed.Subject.CommonName)
	}
	// A broken pair keeps the previous certificate and records the error.
	_ = os.WriteFile(keyFile, []byte("garbage"), 0o600)
	later := now.Add(4 * time.Second)
	_ = os.Chtimes(keyFile, later, later)
	now = later
	c2, _ := loader.get(nil)
	if c2 != c || loader.LoadError == nil {
		t.Errorf("broken reload: same=%v err=%v", c2 == c, loader.LoadError)
	}
}

// --- WebSocket ---

func wsHandshake(t *testing.T, addr, path, proto string) (net.Conn, *bufio.Reader, int) {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	req := "GET " + path + " HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
	if proto != "" {
		req += "Sec-WebSocket-Protocol: " + proto + "\r\n"
	}
	req += "\r\n"
	if _, err := c.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(c)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode == 101 {
		if resp.Header.Get("Sec-WebSocket-Accept") != "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=" {
			t.Errorf("accept key %q", resp.Header.Get("Sec-WebSocket-Accept"))
		}
		if resp.Header.Get("Sec-WebSocket-Protocol") != "mqtt" {
			t.Errorf("protocol %q", resp.Header.Get("Sec-WebSocket-Protocol"))
		}
	} else {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
	return c, br, resp.StatusCode
}

func frame(fin bool, op byte, payload []byte, mask bool) []byte {
	b := []byte{op}
	if fin {
		b[0] |= 0x80
	}
	n := len(payload)
	var lenByte byte
	switch {
	case n < 126:
		lenByte = byte(n)
	case n < 65536:
		lenByte = 126
	default:
		lenByte = 127
	}
	if mask {
		lenByte |= 0x80
	}
	b = append(b, lenByte)
	switch {
	case n >= 65536:
		b = binary.BigEndian.AppendUint64(b, uint64(n))
	case n >= 126:
		b = binary.BigEndian.AppendUint16(b, uint16(n))
	}
	if mask {
		key := [4]byte{1, 2, 3, 4}
		b = append(b, key[:]...)
		for i, p := range payload {
			b = append(b, p^key[i%4])
		}
	} else {
		b = append(b, payload...)
	}
	return b
}

func readFrame(t *testing.T, r io.Reader) (op byte, payload []byte) {
	t.Helper()
	var h [2]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		t.Fatal(err)
	}
	n := uint64(h[1] & 0x7F)
	switch n {
	case 126:
		var e [2]byte
		_, _ = io.ReadFull(r, e[:])
		n = uint64(binary.BigEndian.Uint16(e[:]))
	case 127:
		var e [8]byte
		_, _ = io.ReadFull(r, e[:])
		n = binary.BigEndian.Uint64(e[:])
	}
	payload = make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		t.Fatal(err)
	}
	return h[0] & 0x0F, payload
}

func TestWebSocketHandshakeRules(t *testing.T) {
	l := open(t, config.Listener{Type: "ws", Path: "/custom"})
	c, _, code := wsHandshake(t, l.Addr().String(), "/custom", "")
	_ = c.Close()
	if code != 400 {
		t.Errorf("missing subprotocol: %d", code)
	}
	c, _, code = wsHandshake(t, l.Addr().String(), "/mqtt", "mqtt")
	_ = c.Close()
	if code != 404 {
		t.Errorf("wrong path: %d", code)
	}
	c, _, code = wsHandshake(t, l.Addr().String(), "/custom", "foo, mqtt")
	if code != 101 {
		t.Fatalf("upgrade: %d", code)
	}
	server := accept(t, l)
	_ = server.Close()
	_ = c.Close()
	resp, err := http.Get("http://" + l.Addr().String() + "/custom")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Errorf("plain GET: %d", resp.StatusCode)
	}
}

func TestWebSocketDataAndControlFrames(t *testing.T) {
	l := open(t, config.Listener{Type: "ws"})
	c, br, code := wsHandshake(t, l.Addr().String(), "/mqtt", "mqtt")
	if code != 101 {
		t.Fatal(code)
	}
	defer func() { _ = c.Close() }()
	server := accept(t, l)
	defer func() { _ = server.Close() }()

	// A masked binary frame arrives as bytes; a large one uses the 16-bit length.
	big := bytes.Repeat([]byte("z"), 300)
	_, _ = c.Write(frame(true, opBinary, []byte("abc"), true))
	_, _ = c.Write(frame(true, opBinary, big, true))
	buf := make([]byte, 303)
	if _, err := io.ReadFull(server, buf); err != nil || string(buf[:3]) != "abc" || !bytes.Equal(buf[3:], big) {
		t.Fatalf("read %v", err)
	}
	// Fragmented message.
	_, _ = c.Write(frame(false, opBinary, []byte("frag"), true))
	_, _ = c.Write(frame(false, opContinuation, []byte("men"), true))
	_, _ = c.Write(frame(true, opContinuation, []byte("ted"), true))
	buf = make([]byte, 10)
	if _, err := io.ReadFull(server, buf); err != nil || string(buf) != "fragmented" {
		t.Fatalf("fragmented read %q %v", buf, err)
	}
	// Ping is answered with a pong carrying the payload; a pong is ignored.
	_, _ = c.Write(frame(true, opPong, []byte("ignored"), true))
	_, _ = c.Write(frame(true, opPing, []byte("ping!"), true))
	_, _ = c.Write(frame(true, opBinary, []byte("after"), true))
	buf = make([]byte, 5)
	if _, err := io.ReadFull(server, buf); err != nil || string(buf) != "after" {
		t.Fatalf("after ping %q %v", buf, err)
	}
	if op, payload := readFrame(t, br); op != opPong || string(payload) != "ping!" {
		t.Errorf("pong: op %x payload %q", op, payload)
	}
	// Server writes are unmasked binary frames; a big one uses the 64-bit length.
	if _, err := server.Write([]byte("reply")); err != nil {
		t.Fatal(err)
	}
	if op, payload := readFrame(t, br); op != opBinary || string(payload) != "reply" {
		t.Errorf("reply: op %x payload %q", op, payload)
	}
	huge := bytes.Repeat([]byte("h"), 70000)
	if _, err := server.Write(huge); err != nil {
		t.Fatal(err)
	}
	if op, payload := readFrame(t, br); op != opBinary || !bytes.Equal(payload, huge) {
		t.Errorf("huge: op %x len %d", op, len(payload))
	}
	// Client close: server answers close and Read returns EOF.
	closeBody := []byte{0x03, 0xE8}
	_, _ = c.Write(frame(true, opClose, closeBody, true))
	if _, err := server.Read(make([]byte, 1)); err != io.EOF {
		t.Errorf("after close: %v", err)
	}
	if op, payload := readFrame(t, br); op != opClose || !bytes.Equal(payload, closeBody) {
		t.Errorf("close reply: op %x payload %x", op, payload)
	}
}

func TestWebSocketViolations(t *testing.T) {
	cases := []struct {
		name string
		send []byte
		code uint16
	}{
		{"unmasked", frame(true, opBinary, []byte("x"), false), closeProtocolError},
		{"text", frame(true, opText, []byte("x"), true), closeUnsupported},
		{"reserved bits", func() []byte { f := frame(true, opBinary, []byte("x"), true); f[0] |= 0x40; return f }(), closeProtocolError},
		{"continuation without message", frame(true, opContinuation, []byte("x"), true), closeProtocolError},
		{"data inside fragment", append(frame(false, opBinary, []byte("x"), true), frame(true, opBinary, []byte("y"), true)...), closeProtocolError},
		{"unknown opcode", frame(true, 0x3, []byte("x"), true), closeProtocolError},
		{"fragmented control", frame(false, opPing, []byte("x"), true), closeProtocolError},
		{"too large", frame(true, opBinary, bytes.Repeat([]byte("x"), 2000), true), closeTooLarge},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			l, err := Open(config.Listener{Name: "ws", Type: "ws", Address: "127.0.0.1:0"}, 1024)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = l.Close() }()
			c, br, code := wsHandshake(t, l.Addr().String(), "/mqtt", "mqtt")
			if code != 101 {
				t.Fatal(code)
			}
			defer func() { _ = c.Close() }()
			server := accept(t, l)
			defer func() { _ = server.Close() }()
			_, _ = c.Write(tc.send)
			// A violation may follow a valid frame; it is reported on the
			// read that reaches it.
			var rerr error
			for i := 0; i < 3 && rerr == nil; i++ {
				_, rerr = server.Read(make([]byte, 16))
			}
			if rerr == nil {
				t.Fatal("violation not reported")
			}
			op, payload := readFrame(t, br)
			if op != opClose || len(payload) < 2 || binary.BigEndian.Uint16(payload) != tc.code {
				t.Errorf("close frame: op %x payload %x", op, payload)
			}
		})
	}
}

func TestWebSocketOverTLS(t *testing.T) {
	_, ca, certFile, keyFile, _ := tlsFixtures(t)
	l := open(t, config.Listener{Type: "wss", TLS: &config.TLS{CertFile: certFile, KeyFile: keyFile}})
	wcfg := clientTLS(ca, nil)
	wcfg.NextProtos = []string{"http/1.1"}
	tc, err := tls.Dial("tcp", l.Addr().String(), wcfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tc.Close() }()
	req := "GET /mqtt HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Protocol: mqtt\r\n\r\n"
	if _, err := tc.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(tc)
	resp, err := http.ReadResponse(br, nil)
	if err != nil || resp.StatusCode != 101 {
		t.Fatalf("%v %v", err, resp)
	}
	server := accept(t, l)
	defer func() { _ = server.Close() }()
	_, _ = tc.Write(frame(true, opBinary, []byte("secure"), true))
	buf := make([]byte, 6)
	if _, err := io.ReadFull(server, buf); err != nil || string(buf) != "secure" {
		t.Fatalf("%q %v", buf, err)
	}
	if PeerCertificate(server) != nil {
		t.Error("unexpected peer certificate")
	}
}

func TestWebSocketBufferedBytesAfterHandshake(t *testing.T) {
	// Bytes sent in the same segment as the handshake must not be lost.
	l := open(t, config.Listener{Type: "ws"})
	c, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	req := "GET /mqtt HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Protocol: mqtt\r\n\r\n"
	if _, err := c.Write(append([]byte(req), frame(true, opBinary, []byte("early"), true)...)); err != nil {
		t.Fatal(err)
	}
	server := accept(t, l)
	defer func() { _ = server.Close() }()
	buf := make([]byte, 5)
	if _, err := io.ReadFull(server, buf); err != nil || string(buf) != "early" {
		t.Fatalf("%q %v", buf, err)
	}
}
