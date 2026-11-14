//go:build linux

package sim

import (
	"net"
	"syscall"
	"testing"
)

// TestClientDialerKeepAlive: a client's socket carries SO_KEEPALIVE by
// default and not when its keepalive is off, read back from the kernel.
func TestClientDialerKeepAlive(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	accepted := make(chan net.Conn, 2)
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			accepted <- c
		}
	}()
	t.Cleanup(func() {
		for len(accepted) > 0 {
			_ = (<-accepted).Close()
		}
	})
	for _, off := range []bool{false, true} {
		cl := NewClient("ka", nil, 0, 1)
		cl.KeepAliveOff = off
		d := cl.dialer()
		nc, err := d.Dial("tcp", l.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		raw, err := nc.(*net.TCPConn).SyscallConn()
		if err != nil {
			t.Fatal(err)
		}
		var v int
		var gerr error
		if err := raw.Control(func(fd uintptr) { v, gerr = syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_KEEPALIVE) }); err != nil || gerr != nil {
			t.Fatal(err, gerr)
		}
		_ = nc.Close()
		if want := map[bool]int{false: 1, true: 0}[off]; v != want {
			t.Fatalf("keepalive off %v: SO_KEEPALIVE %d, want %d", off, v, want)
		}
	}
}
