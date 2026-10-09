package listener

import (
	"net"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/config"
)

// startAccept runs one Accept in the background; the channel yields its
// result. A paused Accept keeps waiting until a slot frees or the listener
// closes, so a test that starts one must read its result, not abandon it.
func startAccept(l *Listener) <-chan net.Conn {
	ch := make(chan net.Conn, 1)
	go func() {
		c, err := l.Accept()
		if err != nil {
			ch <- nil
			return
		}
		ch <- c
	}()
	return ch
}

func within(ch <-chan net.Conn, d time.Duration) (net.Conn, bool) {
	select {
	case c := <-ch:
		return c, true
	case <-time.After(d):
		return nil, false
	}
}

// Pause-accept: at the handshake bound the listener stops accepting until a
// handshake ends; nothing is refused, and the slot returns on HandshakeDone
// or on Close, whichever comes first.
func TestMaxHandshakesPausesAccept(t *testing.T) {
	l := open(t, config.Listener{Type: "tcp", MaxHandshakes: 1})
	if got := l.Stats(); got.MaxHandshakes != 1 || got.Handshaking != 0 {
		t.Fatalf("initial stats %+v", got)
	}
	a, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Close() }()
	sa, ok := within(startAccept(l), 2*time.Second)
	if !ok || sa == nil {
		t.Fatal("first accept did not return")
	}
	if got := l.Stats(); got.Handshaking != 1 {
		t.Fatalf("after one accept: %+v", got)
	}

	b, err := net.Dial("tcp", l.Addr().String()) // sits in the kernel backlog
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close() }()
	paused := startAccept(l)
	if _, ok := within(paused, 300*time.Millisecond); ok {
		t.Fatal("accept returned while the only handshake slot was taken; accept must pause")
	}
	// Ending the first handshake releases the paused Accept.
	sa.(*countedConn).HandshakeDone(true)
	sb, ok := within(paused, 2*time.Second)
	if !ok || sb == nil {
		t.Fatal("accept did not resume after the slot was released")
	}
	got := l.Stats()
	if got.PausedSec <= 0 {
		t.Fatalf("pause was not measured: %+v", got)
	}
	if got.Accepted != 1 {
		t.Fatalf("accepted count %d, want 1", got.Accepted)
	}
	// The first handshake held its slot for at least the 300 ms this test
	// watched the second Accept stay paused.
	if got.AcceptedSlotSec < 0.3 || got.FailedSlotSec != 0 {
		t.Fatalf("slot time: accepted %.3f s, failed %.3f s; want the accepted handshake's 0.3 s or more and nothing failed yet", got.AcceptedSlotSec, got.FailedSlotSec)
	}
	// Close without HandshakeDone returns the slot too, as a failed handshake.
	_ = sb.Close()
	if got := l.Stats(); got.Handshaking != 0 || got.Failed != 1 || got.FailedSlotSec <= 0 {
		t.Fatalf("after closing an unfinished handshake: %+v", got)
	}
	// HandshakeDone after Close is a no-op (the slot returned once).
	sb.(*countedConn).HandshakeDone(true)
	if got := l.Stats(); got.Accepted != 1 || got.Failed != 1 {
		t.Fatalf("double release counted twice: %+v", got)
	}
	_ = sa.Close()
	if got := l.Stats(); got.Handshaking != 0 || got.Active != 0 {
		t.Fatalf("slot or active count leaked: %+v", got)
	}
}

// The default applies when the field is zero, and Close wakes a paused Accept.
func TestMaxHandshakesDefaultAndCloseWakesPause(t *testing.T) {
	l := open(t, config.Listener{Type: "tcp"})
	if got := l.Stats(); got.MaxHandshakes != config.DefaultMaxHandshakes {
		t.Fatalf("default bound %d, want %d", got.MaxHandshakes, config.DefaultMaxHandshakes)
	}

	p := open(t, config.Listener{Type: "tcp", MaxHandshakes: 1, Name: "paused"})
	a, err := net.Dial("tcp", p.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Close() }()
	if _, ok := within(startAccept(p), 2*time.Second); !ok {
		t.Fatal("first accept")
	}
	done := make(chan error, 1)
	go func() {
		_, err := p.Accept() // paused: the one slot is taken
		done <- err
	}()
	time.Sleep(100 * time.Millisecond)
	_ = p.Close()
	select {
	case err := <-done:
		if err != ErrClosed {
			t.Fatalf("paused accept returned %v, want ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not wake the paused Accept")
	}
}
