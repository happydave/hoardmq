package sim

import (
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestClientStartProblems(t *testing.T) {
	want := clientStartWant{Clients: 100000, Sources: []string{"10.231.14.11", "10.231.14.12", "10.231.14.13", "10.231.14.14",
		"10.231.14.15", "10.231.14.16", "10.231.14.17", "10.231.14.18"}}
	base := func() clientStartFacts {
		f := settledFacts()
		f.SelfExe = "/home/notdave/hoardmq-clients/simclients"
		f.Procs = map[int]string{f.SelfPID: f.SelfExe, 1: "/usr/lib/systemd/systemd"}
		f.Addrs = []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("192.0.2.137")}
		for _, s := range want.Sources {
			f.Addrs = append(f.Addrs, net.ParseIP(s))
		}
		return f
	}
	if p := clientStartProblems(base(), want); len(p) != 0 {
		t.Fatalf("a settled host refused: %v", p)
	}
	for _, tc := range []struct {
		name   string
		change func(*clientStartFacts, *clientStartWant)
		expect string
	}{
		{"another driver", func(f *clientStartFacts, _ *clientStartWant) { f.Procs[4242] = f.SelfExe }, "pid [4242]"},
		{"a replaced driver", func(f *clientStartFacts, _ *clientStartWant) { f.Procs[4243] = f.SelfExe + " (deleted)" }, "pid [4243]"},
		{"unsettled sockets", func(f *clientStartFacts, _ *clientStartWant) { f.TCPInUseTW = clientSettledSockets + 1 }, "2001 TCP sockets"},
		{"sockets unreadable", func(f *clientStartFacts, _ *clientStartWant) { f.TCPKnown = false }, "socket count unreadable"},
		{"open files one short", func(f *clientStartFacts, w *clientStartWant) { f.OpenFiles = uint64(w.Clients) + clientFileMargin - 1 }, "open-file limit"},
		{"memory under the floor", func(f *clientStartFacts, _ *clientStartWant) { f.MemAvailableBytes = clientMinMemory - 1 }, "MemAvailable"},
		{"memory under the measured need", func(f *clientStartFacts, w *clientStartWant) {
			w.BytesPerClient = 1 << 20
			f.MemAvailableBytes = uint64(w.Clients)*w.BytesPerClient*3 - 1
		}, "MemAvailable"},
		{"loaded", func(f *clientStartFacts, _ *clientStartWant) { f.Load1 = clientMaxLoad1 + 0.01 }, "1-minute load"},
		{"source missing", func(f *clientStartFacts, _ *clientStartWant) { f.Addrs = f.Addrs[:len(f.Addrs)-1] }, "10.231.14.18 is not on this host"},
		{"port budget one over", func(f *clientStartFacts, w *clientStartWant) {
			// 8 sources: 112,936 clients is 14,117 per source against 28,232 / 2.
			w.Clients = 8 * 14117
			f.OpenFiles = 1 << 20
			f.MemAvailableBytes = 1 << 40
		}, "exceed half the ephemeral range"},
		{"ports unreadable", func(f *clientStartFacts, _ *clientStartWant) { f.PortsKnown = false }, "port range unreadable"},
	} {
		f, w := base(), want
		w.Sources = append([]string(nil), want.Sources...)
		tc.change(&f, &w)
		p := clientStartProblems(f, w)
		if len(p) != 1 || !strings.Contains(p[0], tc.expect) {
			t.Errorf("%s: problems %v, want one naming %q", tc.name, p, tc.expect)
		}
	}
	// At the edge: exactly half the range per source is allowed.
	f, w := base(), want
	w.Clients = 8 * 14116
	f.OpenFiles, f.MemAvailableBytes = 1<<20, 1<<40
	if p := clientStartProblems(f, w); len(p) != 0 {
		t.Fatalf("a budget at exactly half refused: %v", p)
	}
}

func TestSourcePresent(t *testing.T) {
	addrs := []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("10.231.14.11")}
	for s, want := range map[string]bool{"127.0.0.18": true, "10.231.14.11": true, "10.231.14.12": false, "gtr": false} {
		if got := sourcePresent(s, addrs); got != want {
			t.Errorf("%s: %v, want %v", s, got, want)
		}
	}
}

// timeoutError is a net.Error that timed out.
type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

// TestClassifyFailure maps real connect failures, made against local
// listeners through the client's own connect, and synthesised errnos for
// exhaustion that a test cannot safely cause, to their classes.
func TestClassifyFailure(t *testing.T) {
	connect := func(addr string) error {
		_, err := NewClient("classify", []string{addr}, 60, 1).connect(addr)
		return err
	}
	// Refused: a port nothing listens on.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closed := l.Addr().String()
	_ = l.Close()
	if got := classifyFailure(connect(closed)); got != failRefused {
		t.Errorf("closed port: %s", got)
	}
	serve := func(handle func(net.Conn)) string {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = l.Close() })
		go func() {
			for {
				c, err := l.Accept()
				if err != nil {
					return
				}
				go handle(c)
			}
		}()
		return l.Addr().String()
	}
	eof := serve(func(c net.Conn) {
		// Read the whole CONNECT first: closing with unread bytes sends a reset.
		_ = c.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
		_, _ = io.Copy(io.Discard, c)
		_ = c.Close()
	})
	if got := classifyFailure(connect(eof)); got != failEOF {
		t.Errorf("closed after CONNECT: %s", got)
	}
	reset := serve(func(c net.Conn) {
		_, _ = io.ReadAtLeast(c, make([]byte, 2), 2)
		_ = c.(*net.TCPConn).SetLinger(0)
		_ = c.Close()
	})
	if got := classifyFailure(connect(reset)); got != failReset {
		t.Errorf("reset after CONNECT: %s", got)
	}
	silent := serve(func(c net.Conn) {
		time.Sleep(7 * time.Second)
		_ = c.Close()
	})
	if got := classifyFailure(connect(silent)); got != failHandshakeDeadline {
		t.Errorf("no CONNACK: %s", got)
	}
	dialErr := func(err error) error {
		return &connectError{stage: "dial", err: &net.OpError{Op: "dial", Net: "tcp", Err: err}}
	}
	for err, want := range map[error]string{
		dialErr(os.NewSyscallError("connect", syscall.EADDRNOTAVAIL)): failAddrNotAvailable,
		dialErr(os.NewSyscallError("socket", syscall.EMFILE)):         failTooManyFiles,
		dialErr(os.NewSyscallError("socket", syscall.ENFILE)):         failTooManyFiles,
		dialErr(timeoutError{}):                                       failDialTimeout,
		errors.New("expected CONNACK, got *packet.Suback"):            failOther,
	} {
		if got := classifyFailure(err); got != want {
			t.Errorf("%v: %s, want %s", err, got, want)
		}
	}
}
