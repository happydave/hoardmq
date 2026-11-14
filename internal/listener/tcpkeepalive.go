package listener

import (
	"crypto/tls"
	"net"
)

// The outcomes SetTCPKeepAlive reports.
const (
	// TCPKeepAliveOff: TCP keepalive was turned off.
	TCPKeepAliveOff = "off"
	// TCPKeepAliveKept: the client asked for a keep alive of 0, so TCP
	// keepalive stays.
	TCPKeepAliveKept = "kept"
	// TCPKeepAliveNotTCP: no TCP socket was reached; nothing changed.
	TCPKeepAliveNotTCP = "not-tcp"
	// TCPKeepAliveFailed: the socket refused the change and keeps what it had.
	TCPKeepAliveFailed = "failed"
)

// tcpKeepAliveOff decides whether an established client connection's TCP
// keepalive goes off, from the keep alive the client's CONNECT asked for. A
// client that asked for one has a read deadline of one and a half keep alives
// in force (a server maximum only shortens it), which finds a gone peer
// without the probes. A client that asked for 0 relies on TCP keepalive to be
// closed once it has gone, even where a server maximum imposes a keep alive.
func tcpKeepAliveOff(requested uint16, tcp bool) bool {
	return tcp && requested != 0
}

// SetTCPKeepAlive turns TCP keepalive off on a connection whose CONNECT asked
// for the keep alive requested, where tcpKeepAliveOff says so, and reports
// the outcome. It never turns keepalive on or changes its timers.
func SetTCPKeepAlive(c net.Conn, requested uint16) (string, error) {
	tc := tcpConn(c)
	if !tcpKeepAliveOff(requested, tc != nil) {
		if tc == nil {
			return TCPKeepAliveNotTCP, nil
		}
		return TCPKeepAliveKept, nil
	}
	if err := tc.SetKeepAlive(false); err != nil {
		return TCPKeepAliveFailed, err
	}
	return TCPKeepAliveOff, nil
}

// tcpConn reaches the TCP connection under a connection this package
// produced, or nil.
func tcpConn(c net.Conn) *net.TCPConn {
	for {
		switch x := c.(type) {
		case *countedConn:
			c = x.Conn
		case *wsConn:
			c = x.raw
		case *tls.Conn:
			c = x.NetConn()
		case *net.TCPConn:
			return x
		default:
			return nil
		}
	}
}
