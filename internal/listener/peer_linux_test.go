//go:build linux

package listener

import (
	"net"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/config"
)

// gone polls PeerGone until it reports true or the wait ends, since a close
// reaches the server's socket asynchronously.
func eventuallyGone(c net.Conn, wait time.Duration) bool {
	g := c.(interface{ PeerGone() bool })
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		if g.PeerGone() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// TestPeerGoneSeesAClosedClient is WI 1485: once a client has sent its bytes
// and closed, and the server has read them, the peek reports the peer gone,
// and the listener counts it.
func TestPeerGoneSeesAClosedClient(t *testing.T) {
	l := open(t, config.Listener{Type: "tcp"})
	client, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	server := accept(t, l)
	t.Cleanup(func() { _ = server.Close() })
	if _, err := client.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	_ = client.Close()
	buf := make([]byte, 1)
	_ = server.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := server.Read(buf); err != nil {
		t.Fatal(err)
	}
	if !eventuallyGone(server, 2*time.Second) {
		t.Fatal("a client that closed was reported present")
	}
	if n := l.Stats().Abandoned; n == 0 {
		t.Errorf("the abandoned count did not move: %d", n)
	}
}

// TestPeerGoneSeesAReset covers a client that resets rather than closes.
func TestPeerGoneSeesAReset(t *testing.T) {
	l := open(t, config.Listener{Type: "tcp"})
	client, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	server := accept(t, l)
	t.Cleanup(func() { _ = server.Close() })
	_ = client.(*net.TCPConn).SetLinger(0)
	_ = client.Close()
	if !eventuallyGone(server, 2*time.Second) {
		t.Fatal("a client that reset was reported present")
	}
}

// TestPeerGoneLeavesLiveClientsAlone: a client still connected, idle or with
// bytes waiting, is present.
func TestPeerGoneLeavesLiveClientsAlone(t *testing.T) {
	l := open(t, config.Listener{Type: "tcp"})
	client, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	server := accept(t, l)
	t.Cleanup(func() { _ = server.Close() })
	g := server.(interface{ PeerGone() bool })
	if g.PeerGone() {
		t.Fatal("an idle live client was reported gone")
	}
	if _, err := client.Write([]byte("xy")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1)
	_ = server.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := server.Read(buf); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if g.PeerGone() {
		t.Fatal("a live client with a byte pending was reported gone")
	}
	_ = server.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := server.Read(buf); err != nil {
		t.Fatal(err)
	}
	if g.PeerGone() {
		t.Fatal("an idle live client was reported gone after reading everything")
	}
	if n := l.Stats().Abandoned; n != 0 {
		t.Errorf("a live client was counted abandoned: %d", n)
	}
	if peerGone(&net.UnixConn{}) {
		t.Error("a connection that is not TCP must report present")
	}
}
