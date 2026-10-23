package transport

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"
)

// declareFrame writes only a frame header declaring a payload of n bytes of
// class c, then closes the connection.
func declareFrame(c net.Conn, n int, cl Class) {
	var hdr [headerSize]byte
	binary.BigEndian.PutUint32(hdr[:4], uint32(n+2))
	hdr[4] = byte(cl)
	_, _ = c.Write(hdr[:])
	_ = c.Close()
}

// allocatedDuring is how many bytes the process allocated while f ran.
func allocatedDuring(f func()) uint64 {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// TestHandshakeFrameIsBoundedBeforeAuthentication: a peer that has not
// authenticated declares a handshake frame of the whole frame limit, as the
// dialer's hello and as the acceptor's. The node must refuse it as a
// protocol error without allocating it.
func TestHandshakeFrameIsBoundedBeforeAuthentication(t *testing.T) {
	const limit = 16 << 20
	tr, err := New(Options{Node: "n2", ClusterID: "test", Auth: secretAuth(), MaxFrame: limit, HandshakeTimeout: 2 * time.Second,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	for _, dialer := range []bool{false, true} {
		name := "acceptor reading a hello"
		if dialer {
			name = "dialer reading the acceptor's hello"
		}
		t.Run(name, func(t *testing.T) {
			ours, theirs := net.Pipe()
			defer func() { _ = ours.Close() }()
			go func() {
				if dialer {
					// The dialer speaks first; read its hello, then answer.
					_, _ = readFrame(theirs, 1<<20, true)
				}
				declareFrame(theirs, limit, classHello)
			}()
			var herr error
			n := allocatedDuring(func() { _, herr = tr.handshake(ours, dialer) })
			if n >= 1<<20 {
				t.Errorf("allocated %d bytes for an unauthenticated hello declaring %d; the payload must not be allocated", n, limit)
			}
			var r *refusal
			if herr == nil || !errors.As(herr, &r) || r.reason != "protocol" {
				t.Errorf("handshake answered %v, want a protocol refusal", herr)
			}
		})
	}
}

// TestHandshakeBoundIsTheLargestHello: the bound is the size of the largest
// hello the encoder writes, with the acceptor's proof. A hello declaring
// exactly that is not refused for its length, and one byte more is.
func TestHandshakeBoundIsTheLargestHello(t *testing.T) {
	long := strings.Repeat("x", 65535)
	largest := len(hello{cluster: long, name: long, vmin: 1, vmax: 1}.encode()) + 32
	if largest != maxHandshakeFrame {
		t.Errorf("the largest hello with its proof is %d bytes, the bound %d", largest, maxHandshakeFrame)
	}
	tr, err := New(Options{Node: "n2", ClusterID: "test", Auth: secretAuth(), HandshakeTimeout: 2 * time.Second,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		declared int
		refused  bool
	}{{largest, false}, {largest + 1, true}} {
		ours, theirs := net.Pipe()
		go declareFrame(theirs, tc.declared, classHello)
		_, herr := tr.handshake(ours, false)
		_ = ours.Close()
		if herr == nil {
			t.Fatalf("a hello declaring %d bytes and sending none was accepted", tc.declared)
		}
		if got := strings.Contains(herr.Error(), "exceeds the limit of"); got != tc.refused {
			t.Errorf("a hello declaring %d bytes: refused for its length %v (%v), want %v", tc.declared, got, herr, tc.refused)
		}
	}
}

// TestHandshakeBoundIsNeverAboveTheFrameLimit: with a frame limit below the
// handshake bound, a hello over the frame limit is refused for it.
func TestHandshakeBoundIsNeverAboveTheFrameLimit(t *testing.T) {
	const limit = 4096
	tr, err := New(Options{Node: "n2", ClusterID: "test", Auth: secretAuth(), MaxFrame: limit, HandshakeTimeout: 2 * time.Second,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	ours, theirs := net.Pipe()
	defer func() { _ = ours.Close() }()
	go declareFrame(theirs, limit+1, classHello)
	_, herr := tr.handshake(ours, false)
	if want := fmt.Sprintf("exceeds the limit of %d", limit); herr == nil || !strings.Contains(herr.Error(), want) {
		t.Fatalf("a hello one byte over a %d-byte frame limit answered %v, want a refusal naming the limit", limit, herr)
	}
}

// TestLongNamesStillConnect: a cluster identifier and node names of 1,000
// bytes each fit well inside the bound, and the pair comes up.
func TestLongNamesStillConnect(t *testing.T) {
	names := []string{"a" + strings.Repeat("x", 999), "b" + strings.Repeat("x", 999)}
	c := newCluster(t, "secret", names, func(_ string, o *Options) { o.ClusterID = strings.Repeat("c", 1000) })
	c.start(t)
	c.nodes[names[0]].waitUp(t, names[1])
}

// TestOversizedHelloOverASocketIsRefused: an unauthenticated connection to a
// running node that declares a hello of the whole frame limit is closed and
// audited as a protocol refusal, within a second: the handshake timeout
// here is two.
func TestOversizedHelloOverASocketIsRefused(t *testing.T) {
	c := newCluster(t, "secret", []string{"n1", "n2"}, nil)
	c.start(t)
	acceptor := c.nodes["n2"]
	conn, err := net.Dial("tcp", acceptor.t.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	var hdr [headerSize]byte
	binary.BigEndian.PutUint32(hdr[:4], uint32(16<<20+2))
	hdr[4] = byte(classHello)
	if _, err := conn.Write(hdr[:]); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := conn.Read(make([]byte, 1)); err == nil || errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("the node kept an oversized unauthenticated hello's connection open: %v", err)
	}
	waitFor(t, "a protocol refusal", func() bool { return acceptor.ev.has("cluster.peer.refused", "protocol") })
}
