package sim

import (
	"bytes"
	"context"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"testing"
	"time"
)

// TestNodeAndClientGoroutinesCarryTheirLabels: a node's goroutines and a
// client's inherit the labels their start ran under, which is what lets the
// storm's CPU profile be split into client and node CPU (WI 1474). The
// goroutine profile carries the same labels as the CPU profile and can be
// read without a profile parser.
func TestNodeAndClientGoroutinesCarryTheirLabels(t *testing.T) {
	c := New(t, Options{Nodes: 1})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cl := NewClient("labelled", c.Addresses(), 60, 1)
	if err := cl.Run(ctx); err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	var buf bytes.Buffer
	if err := pprof.Lookup("goroutine").WriteTo(&buf, 1); err != nil {
		t.Fatal(err)
	}
	profile := buf.String()
	for _, want := range []string{`"sim":"node"`, `"node":"n1"`, `"sim":"client"`} {
		if !strings.Contains(profile, want) {
			t.Errorf("no goroutine carries %s", want)
		}
	}
}

func TestFirstFenceNamesTheFirstNodeThatFenced(t *testing.T) {
	if _, ok := firstFence(map[string]int64{"n2": 0, "n3": 0}); ok {
		t.Fatal("no fence yet, but the trigger fired")
	}
	if node, ok := firstFence(map[string]int64{"n2": 0, "n3": 1}); !ok || node != "n3" {
		t.Fatalf("n3 fenced: got %q, %v", node, ok)
	}
	if node, _ := firstFence(map[string]int64{"n3": 2, "n2": 1}); node != "n2" {
		t.Fatalf("both fenced: the first by name is n2, got %q", node)
	}
}

func TestParseSSReadsQueuesAndMarksTheClusterPort(t *testing.T) {
	out := "0      0      127.0.0.1:41000  127.0.0.1:52344\n" +
		"4096   0      127.0.0.1:52344  127.0.0.1:41000\n" +
		"12     208    [::1]:41001      [::1]:52350\n" +
		"garbage line\n"
	socks := parseSS(out, map[string]bool{"41000": true, "41001": true})
	if len(socks) != 3 {
		t.Fatalf("parsed %d sockets, want 3: %+v", len(socks), socks)
	}
	if !socks[0].Listen || socks[1].Listen || socks[1].RecvQ != 4096 || socks[2].SendQ != 208 || !socks[2].Listen {
		t.Fatalf("parsed %+v", socks)
	}
}

// TestReadClusterSocketsSeesUnreadBytes: a connection whose reader has not
// read shows its bytes in the receive queue, and the queue empties once it
// reads. This is the reading that tells a frame the peer never read from one
// that was never sent.
func TestReadClusterSocketsSeesUnreadBytes(t *testing.T) {
	if _, err := exec.LookPath("ss"); err != nil {
		t.Skip("ss (iproute2) is not installed on this host")
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	accepted := make(chan net.Conn, 1)
	go func() {
		if conn, err := ln.Accept(); err == nil {
			accepted <- conn
		}
	}()
	dialed, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dialed.Close() }()
	server := <-accepted
	defer func() { _ = server.Close() }()
	const sent = 4096
	if _, err := dialed.Write(make([]byte, sent)); err != nil {
		t.Fatal(err)
	}
	port := portOf(ln.Addr().String())
	unread := func() int {
		t.Helper()
		socks, err := readClusterSockets([]string{port})
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range socks {
			if s.Listen {
				return s.RecvQ
			}
		}
		return -1
	}
	deadline := time.Now().Add(5 * time.Second)
	for unread() != sent {
		if time.Now().After(deadline) {
			t.Fatalf("the accepted side's receive queue never showed %d unread bytes (last %d)", sent, unread())
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := io.ReadFull(server, make([]byte, sent)); err != nil {
		t.Fatal(err)
	}
	for unread() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("the receive queue did not empty after the read (last %d)", unread())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestCPUProfileIsWrittenOnStopAndRefusedWhenOneIsRunning(t *testing.T) {
	dir := t.TempDir()
	stop, rec := startCPUProfile(dir, "cpu.pb.gz", time.Second)
	if stop == nil || rec.Unavailable != "" {
		t.Fatalf("the first profile did not start: %+v", rec)
	}
	if _, second := startCPUProfile(dir, "second.pb.gz", time.Second); second.Unavailable == "" {
		t.Error("a second profile started while the first was running")
	}
	end := time.Now().Add(200 * time.Millisecond)
	for x := 0; time.Now().Before(end); x++ {
		_ = x * x
	}
	rec = stop(2 * time.Second)
	if rec.File != "cpu.pb.gz" || rec.StopAfterSec != 2 || rec.Unavailable != "" {
		t.Fatalf("stopped profile record: %+v", rec)
	}
	if fi, err := os.Stat(filepath.Join(dir, "cpu.pb.gz")); err != nil || fi.Size() == 0 {
		t.Fatalf("profile file: %v, %v", fi, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "second.pb.gz")); !os.IsNotExist(err) {
		t.Errorf("the refused profile left a file: %v", err)
	}
}
