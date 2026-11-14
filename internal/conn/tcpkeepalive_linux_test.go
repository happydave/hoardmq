//go:build linux

package conn

import (
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/happydave/hoardmq/internal/packet"
)

// tcpHarness serves one accepted TCP connection; srv is the server's socket.
type tcpHarness struct {
	*harness
	srv *net.TCPConn
}

// newTCPHarness accepts a real loopback TCP connection and serves it. setup,
// if set, runs on the server's socket before Serve starts.
func newTCPHarness(t *testing.T, opts Options, core *fakeCore, setup func(*net.TCPConn)) *tcpHarness {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	server, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	srv := server.(*net.TCPConn)
	if setup != nil {
		setup(srv)
	}
	c := New(server, "test", opts, core)
	ctx, cancel := context.WithCancel(context.Background())
	h := &harness{t: t, core: core, conn: c, client: client, dec: packet.NewDecoder(client), done: make(chan struct{}), cancel: cancel}
	go func() {
		c.Serve(ctx)
		close(h.done)
	}()
	t.Cleanup(func() {
		cancel()
		_ = client.Close()
		select {
		case <-h.done:
		case <-time.After(3 * time.Second):
			t.Error("Serve did not return")
		}
	})
	return &tcpHarness{harness: h, srv: srv}
}

// keepAliveOpts reads a socket's keepalive options from the kernel.
func keepAliveOpts(t *testing.T, tc *net.TCPConn) (on, idle, intvl, cnt int) {
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
	if err := errors.Join(errs[:]...); err != nil {
		t.Fatal(err)
	}
	return on, idle, intvl, cnt
}

// TestTCPKeepAliveFollowsTheRequestedKeepAlive: an accepted connection keeps
// its TCP keepalive until its CONNECT is accepted; then a client asking for a
// keep alive of 30 has it turned off and one asking 0 keeps it as accepted.
// Under a server maximum of 3600 it is still the client's ask that decides:
// 0, with 3600 in force, keeps it, and 7200, held to 3600, turns it off. A
// CONNECT the core refuses is not counted.
func TestTCPKeepAliveFollowsTheRequestedKeepAlive(t *testing.T) {
	m := &counters{}
	opts := defaultOptions()
	opts.Metrics = m
	off := "hoardmq_connects_tcp_keepalive_total{listener=test,outcome=off}"
	kept := "hoardmq_connects_tcp_keepalive_total{listener=test,outcome=kept}"

	h := newTCPHarness(t, opts, &fakeCore{}, nil)
	time.Sleep(200 * time.Millisecond) // served, no CONNECT yet
	if on, _, _, _ := keepAliveOpts(t, h.srv); on != 1 {
		t.Fatalf("before CONNECT: SO_KEEPALIVE %d, want 1", on)
	}
	if ack := h.connect(packet.V5, &packet.Connect{ClientID: "thirty", KeepAlive: 30}); ack.ReasonCode != packet.Success {
		t.Fatal(ack)
	}
	if on, _, _, _ := keepAliveOpts(t, h.srv); on != 0 || h.conn.TCPKeepAlive() {
		t.Errorf("keep alive 30: SO_KEEPALIVE %d, TCPKeepAlive %v; want 0, false", on, h.conn.TCPKeepAlive())
	}
	if n := m.get(off); n != 1 {
		t.Errorf("keep alive 30: %s = %v, want 1", off, n)
	}

	h = newTCPHarness(t, opts, &fakeCore{}, nil)
	h.connect(packet.V5, &packet.Connect{ClientID: "zero"})
	if on, idle, intvl, cnt := keepAliveOpts(t, h.srv); on != 1 || idle != 15 || intvl != 15 || cnt != 9 || !h.conn.TCPKeepAlive() {
		t.Errorf("keep alive 0: SO_KEEPALIVE %d, idle %d, interval %d, count %d, TCPKeepAlive %v; want 1, 15, 15, 9, true",
			on, idle, intvl, cnt, h.conn.TCPKeepAlive())
	}
	if n := m.get(kept); n != 1 {
		t.Errorf("keep alive 0: %s = %v, want 1", kept, n)
	}

	capped := opts
	capped.MaxKeepAlive = 3600
	h = newTCPHarness(t, capped, &fakeCore{}, nil)
	h.connect(packet.V5, &packet.Connect{ClientID: "replaced"})
	if on, idle, intvl, cnt := keepAliveOpts(t, h.srv); on != 1 || idle != 15 || intvl != 15 || cnt != 9 || h.conn.KeepAlive() != 3600 || !h.conn.TCPKeepAlive() {
		t.Errorf("keep alive 0 under a maximum of 3600: SO_KEEPALIVE %d, idle %d, interval %d, count %d, in force %d, TCPKeepAlive %v; want 1, 15, 15, 9, 3600, true",
			on, idle, intvl, cnt, h.conn.KeepAlive(), h.conn.TCPKeepAlive())
	}
	if n := m.get(kept); n != 2 {
		t.Errorf("after the replaced 0: %s = %v, want 2", kept, n)
	}

	h = newTCPHarness(t, capped, &fakeCore{}, nil)
	h.connect(packet.V5, &packet.Connect{ClientID: "held", KeepAlive: 7200})
	if on, _, _, _ := keepAliveOpts(t, h.srv); on != 0 || h.conn.KeepAlive() != 3600 {
		t.Errorf("keep alive 7200 under a maximum of 3600: SO_KEEPALIVE %d, in force %d; want 0, 3600", on, h.conn.KeepAlive())
	}
	if n := m.get(off); n != 2 {
		t.Errorf("after the held 7200: %s = %v, want 2", off, n)
	}

	h = newTCPHarness(t, opts, &fakeCore{connectResult: ConnectResult{Reason: packet.ServerUnavailable}}, nil)
	if ack := h.connect(packet.V5, &packet.Connect{ClientID: "refused", KeepAlive: 30}); ack.ReasonCode != packet.ServerUnavailable {
		t.Fatal(ack)
	}
	h.readEnd()
	if n, k := m.get(off), m.get(kept); n != 2 || k != 2 {
		t.Errorf("after a refused CONNECT: off %v, kept %v; want 2, 2", n, k)
	}
}

// silentPeerEnv marks the child process TestSilentPeerAtKeepAliveZeroIsClosed
// runs itself in.
const silentPeerEnv = "HOARDMQ_TEST_SILENT_PEER_CHILD"

// TestSilentPeerAtKeepAliveZeroIsClosed: a client that asked for a keep
// alive of 0 relies on TCP keepalive alone to be closed once it falls silent,
// with no read deadline or, under a server maximum, a distant one. The test
// runs again in a new user and network namespace, where it may take the
// loopback interface down; there it shortens the server sockets' keepalive
// timers (1 s idle, 1 s apart, 2 probes; the broker leaves SO_KEEPALIVE itself
// as it decides) and silences every peer at once. The connection asking 0,
// and the one asking 0 under a maximum of 3600 (a 5400 s deadline), must end
// within 10 s; the one asking 30, whose deadline is 45 s off, must still be
// open at 6 s.
func TestSilentPeerAtKeepAliveZeroIsClosed(t *testing.T) {
	if os.Getenv(silentPeerEnv) == "" {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSilentPeerAtKeepAliveZeroIsClosed$", "-test.count=1", "-test.v")
		cmd.Env = append(os.Environ(), silentPeerEnv+"=1")
		cmd.SysProcAttr = &syscall.SysProcAttr{
			Cloneflags:  syscall.CLONE_NEWUSER | syscall.CLONE_NEWNET,
			UidMappings: []syscall.SysProcIDMap{{ContainerID: 0, HostID: os.Getuid(), Size: 1}},
			GidMappings: []syscall.SysProcIDMap{{ContainerID: 0, HostID: os.Getgid(), Size: 1}},
		}
		out, err := cmd.CombinedOutput()
		var exit *exec.ExitError
		switch {
		case err == nil:
			t.Logf("child:\n%s", out)
		case errors.As(err, &exit):
			t.Fatalf("child failed (%v):\n%s", err, out)
		default:
			t.Skipf("needs unprivileged user and network namespaces to silence a peer: %v", err)
		}
		return
	}

	if err := setLoopback(true); err != nil {
		t.Fatalf("lo up: %v", err)
	}
	short := func(tc *net.TCPConn) {
		raw, err := tc.SyscallConn()
		if err != nil {
			t.Fatal(err)
		}
		var errs [3]error
		_ = raw.Control(func(fd uintptr) {
			errs[0] = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_KEEPIDLE, 1)
			errs[1] = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_KEEPINTVL, 1)
			errs[2] = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_KEEPCNT, 2)
		})
		if err := errors.Join(errs[:]...); err != nil {
			t.Fatal(err)
		}
	}
	zeroCore, cappedCore, thirtyCore := &fakeCore{}, &fakeCore{}, &fakeCore{}
	zero := newTCPHarness(t, defaultOptions(), zeroCore, short)
	zero.connect(packet.V5, &packet.Connect{ClientID: "zero"})
	capped := defaultOptions()
	capped.MaxKeepAlive = 3600
	newTCPHarness(t, capped, cappedCore, short).connect(packet.V5, &packet.Connect{ClientID: "capped"})
	thirty := newTCPHarness(t, defaultOptions(), thirtyCore, short)
	thirty.connect(packet.V5, &packet.Connect{ClientID: "thirty", KeepAlive: 30})
	if err := setLoopback(false); err != nil {
		t.Fatalf("lo down: %v", err)
	}
	silenced := time.Now()
	ended := func(f *fakeCore) bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return len(f.disconnects) > 0
	}
	for _, c := range []struct {
		name string
		core *fakeCore
	}{{"keep-alive-0", zeroCore}, {"keep-alive-0 under a maximum of 3600", cappedCore}} {
		for !ended(c.core) {
			if time.Since(silenced) > 10*time.Second {
				t.Fatalf("the %s connection was still open 10 s after its peer fell silent", c.name)
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Logf("the %s connection ended by %v after its peer fell silent", c.name, time.Since(silenced).Round(time.Millisecond))
		c.core.mu.Lock()
		d := c.core.disconnects[0]
		c.core.mu.Unlock()
		if d.graceful || !d.will {
			t.Errorf("the %s connection's end: %+v, want not graceful, Will sent", c.name, d)
		}
	}
	time.Sleep(time.Until(silenced.Add(6 * time.Second)))
	if ended(thirtyCore) {
		t.Error("the keep-alive-30 connection ended within 6 s of its peer falling silent; its deadline is 45 s")
	}
}

// setLoopback brings the network namespace's loopback interface up or down
// through the interface-flags ioctls, whose request is a 40-byte ifreq: the
// name, then the flags as a 16-bit word at offset 16.
func setLoopback(up bool) error {
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_DGRAM, 0)
	if err != nil {
		return err
	}
	defer func() { _ = syscall.Close(fd) }()
	var ifr [40]byte
	copy(ifr[:], "lo")
	if _, _, e := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), syscall.SIOCGIFFLAGS, uintptr(unsafe.Pointer(&ifr[0]))); e != 0 {
		return e
	}
	flags := (*uint16)(unsafe.Pointer(&ifr[16]))
	if up {
		*flags |= syscall.IFF_UP
	} else {
		*flags &^= syscall.IFF_UP
	}
	if _, _, e := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), syscall.SIOCSIFFLAGS, uintptr(unsafe.Pointer(&ifr[0]))); e != 0 {
		return e
	}
	return nil
}
