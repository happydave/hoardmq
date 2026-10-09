//go:build linux

package listener

import (
	"errors"
	"net"
	"syscall"
)

// peerGone reports whether a TCP connection's peer has already closed it:
// a non-blocking peek reads end of stream, or the peer reset. Any other
// outcome — pending data, nothing yet, an error of another kind, or a
// connection that is not plain TCP — reports the peer present.
func peerGone(c net.Conn) bool {
	tc, ok := c.(*net.TCPConn)
	if !ok {
		return false
	}
	raw, err := tc.SyscallConn()
	if err != nil {
		return false
	}
	gone := false
	var buf [1]byte
	_ = raw.Read(func(fd uintptr) bool {
		n, _, err := syscall.Recvfrom(int(fd), buf[:], syscall.MSG_PEEK|syscall.MSG_DONTWAIT)
		gone = (err == nil && n == 0) || errors.Is(err, syscall.ECONNRESET)
		return true
	})
	return gone
}
