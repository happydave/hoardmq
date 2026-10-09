package sim

import (
	"net"
	"time"
)

// The ways a failover run's clients dial, as recorded in its provenance.
const (
	dialNoSource  = "no source address"
	dialAtConnect = "source address, port chosen at connect"
	dialAtBind    = "source address, port chosen at bind"
)

// sourceDialer is the dialer a client uses. With a source address it dials
// from that address and, where the system has the option, leaves the port to
// connect() (dial_linux.go).
func sourceDialer(source string) net.Dialer {
	d := net.Dialer{Timeout: 2 * time.Second}
	if source != "" {
		d.LocalAddr = &net.TCPAddr{IP: net.ParseIP(source)}
		d.ControlContext = sourceControl
	}
	return d
}

// clientDial is what a run records about its clients' dialling: how many
// source addresses they were given, and the mode one real socket dialled
// with. A single address is not given to the clients at all.
func clientDial(sources []string) (int, string) {
	if len(sources) <= 1 {
		return 0, dialNoSource
	}
	return len(sources), probeDialMode(sources[0])
}

// probeDialMode dials a private loopback listener from source through the
// clients' own dialer and reads the option back from the socket, so the
// recorded mode is what the kernel did rather than what the build intended.
func probeDialMode(source string) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "probe failed: " + err.Error()
	}
	defer func() { _ = l.Close() }()
	go func() {
		if c, err := l.Accept(); err == nil {
			_ = c.Close()
		}
	}()
	d := sourceDialer(source)
	nc, err := d.Dial("tcp", l.Addr().String())
	if err != nil {
		return "probe failed: " + err.Error()
	}
	defer func() { _ = nc.Close() }()
	return connDialMode(nc)
}

// connDialMode reads the dial mode back from a socket a client dialler made.
func connDialMode(nc net.Conn) string {
	tc, ok := nc.(*net.TCPConn)
	if !ok {
		return "probe failed: not a TCP connection"
	}
	sc, err := tc.SyscallConn()
	if err != nil {
		return "probe failed: " + err.Error()
	}
	on, err := bindNoPort(sc)
	if err != nil {
		return "probe failed: " + err.Error()
	}
	if on {
		return dialAtConnect
	}
	return dialAtBind
}
