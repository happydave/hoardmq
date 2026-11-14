//go:build linux

package listener

import (
	"bufio"
	"crypto/tls"
	"net"
	"net/http"
	"syscall"
	"testing"

	"github.com/happydave/hoardmq/internal/config"
)

// socketKeepAlive reads a TCP socket's keepalive options from the kernel.
func socketKeepAlive(t *testing.T, tc *net.TCPConn) (on, idle, intvl, cnt int) {
	t.Helper()
	raw, err := tc.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var errs [4]error
	if err := raw.Control(func(fd uintptr) {
		on, errs[0] = syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_KEEPALIVE)
		idle, errs[1] = syscall.GetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_KEEPIDLE)
		intvl, errs[2] = syscall.GetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_KEEPINTVL)
		cnt, errs[3] = syscall.GetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_KEEPCNT)
	}); err != nil {
		t.Fatal(err)
	}
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	return on, idle, intvl, cnt
}

// acceptOn makes one client connection to the listener and returns the
// accepted connection with its TCP socket, reached by the test's own
// unwrapping for the listener's kind.
func acceptOn(t *testing.T, l *Listener, kind string, wcfg *tls.Config) (net.Conn, *net.TCPConn) {
	t.Helper()
	switch kind {
	case "tcp":
		c, err := net.Dial("tcp", l.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close() })
		server := accept(t, l)
		t.Cleanup(func() { _ = server.Close() })
		return server, server.(*countedConn).Conn.(*net.TCPConn)
	case "tls":
		client, server, err := tlsPair(t, l, wcfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
		return server, server.(*countedConn).Conn.(*tls.Conn).NetConn().(*net.TCPConn)
	case "ws":
		c, _, code := wsHandshake(t, l.Addr().String(), "/mqtt", "mqtt")
		if code != 101 {
			t.Fatalf("websocket upgrade answered %d", code)
		}
		t.Cleanup(func() { _ = c.Close() })
		server := accept(t, l)
		t.Cleanup(func() { _ = server.Close() })
		return server, server.(*countedConn).Conn.(*wsConn).raw.(*net.TCPConn)
	case "wss":
		tc, err := tls.Dial("tcp", l.Addr().String(), wcfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = tc.Close() })
		req := "GET /mqtt HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Protocol: mqtt\r\n\r\n"
		if _, err := tc.Write([]byte(req)); err != nil {
			t.Fatal(err)
		}
		resp, err := http.ReadResponse(bufio.NewReader(tc), nil)
		if err != nil || resp.StatusCode != 101 {
			t.Fatalf("websocket upgrade over TLS: %v %v", err, resp)
		}
		server := accept(t, l)
		t.Cleanup(func() { _ = server.Close() })
		return server, server.(*countedConn).Conn.(*wsConn).raw.(*tls.Conn).NetConn().(*net.TCPConn)
	}
	t.Fatalf("unknown kind %q", kind)
	return nil, nil
}

// TestSetTCPKeepAliveOnEveryListenerKind: on a TCP, TLS, WebSocket and
// WebSocket-over-TLS listener, a connection whose client asked for a keep
// alive of 30 has its socket's TCP keepalive turned off, and one asking 0
// keeps the keepalive it was accepted with (Go's default: on, 15 s idle,
// probes 15 s apart, 9 of them), read back from the kernel.
func TestSetTCPKeepAliveOnEveryListenerKind(t *testing.T) {
	_, ca, certFile, keyFile, _ := tlsFixtures(t)
	certs := &config.TLS{CertFile: certFile, KeyFile: keyFile}
	mqttTLS := clientTLS(ca, nil)
	httpTLS := clientTLS(ca, nil)
	httpTLS.NextProtos = []string{"http/1.1"}
	for _, k := range []struct {
		kind string
		tls  *config.TLS
		wcfg *tls.Config
	}{
		{"tcp", nil, nil},
		{"tls", certs, mqttTLS},
		{"ws", nil, nil},
		{"wss", certs, httpTLS},
	} {
		t.Run(k.kind, func(t *testing.T) {
			l := open(t, config.Listener{Type: k.kind, TLS: k.tls})

			server, tc := acceptOn(t, l, k.kind, k.wcfg)
			if on, _, _, _ := socketKeepAlive(t, tc); on != 1 {
				t.Fatalf("accepted with SO_KEEPALIVE %d, want 1", on)
			}
			outcome, err := SetTCPKeepAlive(server, 30)
			if err != nil || outcome != TCPKeepAliveOff {
				t.Fatalf("keep alive 30: outcome %q, %v; want %q", outcome, err, TCPKeepAliveOff)
			}
			if on, _, _, _ := socketKeepAlive(t, tc); on != 0 {
				t.Errorf("keep alive 30: SO_KEEPALIVE %d after, want 0", on)
			}

			server, tc = acceptOn(t, l, k.kind, k.wcfg)
			outcome, err = SetTCPKeepAlive(server, 0)
			if err != nil || outcome != TCPKeepAliveKept {
				t.Fatalf("keep alive 0: outcome %q, %v; want %q", outcome, err, TCPKeepAliveKept)
			}
			if on, idle, intvl, cnt := socketKeepAlive(t, tc); on != 1 || idle != 15 || intvl != 15 || cnt != 9 {
				t.Errorf("keep alive 0: SO_KEEPALIVE %d, idle %d, interval %d, count %d; want 1, 15, 15, 9", on, idle, intvl, cnt)
			}
		})
	}
}

// TestSetTCPKeepAliveWithoutTCP: a connection with no TCP socket under it is
// left alone and reported as such, whatever its keep alive.
func TestSetTCPKeepAliveWithoutTCP(t *testing.T) {
	a, b := net.Pipe()
	defer func() { _ = a.Close(); _ = b.Close() }()
	for _, ka := range []uint16{0, 30} {
		if outcome, err := SetTCPKeepAlive(a, ka); err != nil || outcome != TCPKeepAliveNotTCP {
			t.Errorf("keep alive %d over a pipe: outcome %q, %v; want %q", ka, outcome, err, TCPKeepAliveNotTCP)
		}
	}
}
