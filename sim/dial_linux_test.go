//go:build linux

package sim

import (
	"net"
	"strings"
	"testing"
)

// TestSourceDialLeavesThePortToConnect is WI 1477: a client dialling from a
// source address still dials from it, and its socket carries
// IP_BIND_ADDRESS_NO_PORT, read back from the kernel.
func TestSourceDialLeavesThePortToConnect(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := l.Accept()
		if err == nil {
			accepted <- c
		}
		close(accepted)
	}()
	d := sourceDialer("127.0.0.2")
	nc, err := d.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = nc.Close() })
	if c := <-accepted; c != nil {
		t.Cleanup(func() { _ = c.Close() })
	}
	if ip := nc.LocalAddr().(*net.TCPAddr).IP.String(); ip != "127.0.0.2" {
		t.Fatalf("dialled from %s, want the source address 127.0.0.2", ip)
	}
	sc, err := nc.(*net.TCPConn).SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	on, err := bindNoPort(sc)
	if err != nil {
		t.Fatal(err)
	}
	if !on {
		t.Fatal("IP_BIND_ADDRESS_NO_PORT is not set on the client's socket: the port was chosen at bind()")
	}
}

// TestProbeRecordsTheModeTheKernelUsed: a run with source addresses records
// the mode read back from a real socket.
func TestProbeRecordsTheModeTheKernelUsed(t *testing.T) {
	n, mode := clientDial([]string{"127.0.0.1", "127.0.0.2", "127.0.0.3"})
	if n != 3 || mode != dialAtConnect {
		t.Fatalf("got %d source addresses, mode %q; want 3, %q", n, mode, dialAtConnect)
	}
}

// TestSourceControlNamesTheOptionWhenItCannotBeSet: a socket the option
// cannot be set on fails the dial with an error naming the option.
func TestSourceControlNamesTheOptionWhenItCannotBeSet(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	sc, err := l.(*net.TCPListener).SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	_ = l.Close()
	err = sourceControl(t.Context(), "tcp4", "127.0.0.2:0", sc)
	if err == nil || !strings.Contains(err.Error(), "IP_BIND_ADDRESS_NO_PORT") {
		t.Fatalf("got %v, want an error naming IP_BIND_ADDRESS_NO_PORT", err)
	}
}
