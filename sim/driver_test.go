package sim

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// settledFacts is a client host that meets every starting condition, for
// tests that exercise the protocol rather than the conditions.
func settledFacts() clientStartFacts {
	return clientStartFacts{SelfPID: os.Getpid(), SelfExe: "/driver", Procs: map[int]string{}, TCPKnown: true, OpenFiles: 1 << 20,
		MemAvailableBytes: 64 << 30, MemKnown: true, LoadKnown: true, Addrs: []net.IP{net.ParseIP("127.0.0.1")},
		PortLo: 32768, PortHi: 60999, PortsKnown: true}
}

// syncBuffer is a bytes.Buffer safe for the driver's stderr and the test.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// pipedDriver runs the driver over in-memory pipes and returns the test's
// side of the protocol, the driver's stdin writer and its exit code.
type pipedDriver struct {
	pop    *remotePopulation
	stdin  io.WriteCloser
	code   chan int
	stderr *syncBuffer
}

func startPipedDriver(t *testing.T, c *Cluster, clients int, opts driverOptions) *pipedDriver {
	t.Helper()
	toDriverR, toDriverW := io.Pipe()
	fromDriverR, fromDriverW := io.Pipe()
	d := &pipedDriver{stdin: toDriverW, code: make(chan int, 1), stderr: &syncBuffer{}}
	go func() {
		code := runDriver(toDriverR, fromDriverW, d.stderr, opts)
		_ = fromDriverW.Close()
		_ = toDriverR.Close()
		d.code <- code
	}()
	settings := remoteSettings{Command: []string{"pipe"}, Nodes: c.Addresses(), Sources: []string{"127.0.0.11", "127.0.0.12"}}
	d.pop = newRemotePopulation(settings, clients, remoteOptions{silence: 30 * time.Second, startWait: 5 * time.Second, closeWait: time.Minute}, toDriverW, fromDriverR)
	return d
}

func testDriverOptions() driverOptions {
	o := defaultDriverOptions()
	o.facts = settledFacts
	o.checkEvery = 10 * time.Millisecond
	o.hostEvery = 100 * time.Millisecond
	o.progressEvery = 100 * time.Millisecond
	return o
}

// connectionsOnRunning is the running nodes' total client connections.
func connectionsOnRunning(c *Cluster) int {
	total := 0
	for _, n := range c.Running() {
		if srv := n.Server(); srv != nil {
			total += srv.Connections()
		}
	}
	return total
}

// TestDriverProtocolEndToEnd runs a small failover through the driver: fill,
// mark, crash of the fullest node, recovery, finish and close. The second
// case drops every sample and host figure, so the test sees recovery only
// through the reliable allback: a driver that sent allback through the
// droppable queue fails it.
func TestDriverProtocolEndToEnd(t *testing.T) {
	for _, tc := range []struct {
		name     string
		dropBulk bool
	}{{"samples", false}, {"bulk dropped", true}} {
		t.Run(tc.name, func(t *testing.T) {
			const clients = 300
			c := New(t, Options{})
			opts := testDriverOptions()
			opts.dropBulk = tc.dropBulk
			d := startPipedDriver(t, c, clients, opts)
			t.Cleanup(d.pop.close)

			info, err := d.pop.hello()
			if err != nil {
				t.Fatal(err)
			}
			if info.Dial == "" || strings.HasPrefix(info.Dial, "probe failed") {
				t.Fatalf("the driver read no dial mode: %q", info.Dial)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			var completed atomic.Int64
			failed, err := d.pop.fill(ctx, &completed)
			if err != nil {
				t.Fatal(err)
			}
			if failed != 0 || completed.Load() != clients {
				t.Fatalf("fill: %d failed, %d completed of %d", failed, completed.Load(), clients)
			}
			perNode, err := d.pop.perNode()
			if err != nil {
				t.Fatal(err)
			}
			sum := 0
			for _, n := range perNode {
				sum += n
			}
			if sum != clients {
				t.Fatalf("per-node counts sum to %d, want %d: %v", sum, clients, perNode)
			}
			victim := c.Nodes[0]
			for _, n := range c.Nodes {
				if perNode[n.TCP] > perNode[victim.TCP] {
					victim = n
				}
			}
			affected := perNode[victim.TCP]
			if err := d.pop.mark(); err != nil {
				t.Fatal(err)
			}
			first, err := d.pop.sample()
			if err != nil {
				t.Fatal(err)
			}
			if first.Online != clients {
				t.Fatalf("the mark's reading has %d online, want %d", first.Online, clients)
			}
			// Let the driver take readings with every client online before the
			// crash, so a driver that counted them as recovery is caught.
			time.Sleep(4 * opts.sampleEvery)
			c.Crash(victim)
			if err := d.pop.crashed(); err != nil {
				t.Fatal(err)
			}
			// The readings are judged by the same watch the measurement uses,
			// so this covers the whole path: the driver counts recovered
			// clients, the crossings arrive reliably, and the test decides.
			deadline := time.Now().Add(time.Minute)
			w := newHoldWatch(clients, 3*opts.sampleEvery, deadline)
			// The mark's reading, from before the crash, must not read as
			// recovery.
			if s, err := d.pop.sample(); err != nil || w.observe(s.At, s.Recovered) {
				t.Fatalf("recovery reported straight after the crash: %+v, %v", s, err)
			}
			var s popSample
			for time.Now().Before(deadline) {
				if s, err = d.pop.sample(); err != nil {
					t.Fatal(err)
				}
				if w.observe(s.At, s.Recovered) {
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
			if _, reason := w.verdict(); reason != recoveryReached {
				t.Fatalf("recovery not seen: %d online, %d recovered, %q", s.Online, s.Recovered, reason)
			}
			if s.Recovered != clients || s.Online != clients {
				t.Fatalf("recovered %d, online %d, want %d of each", s.Recovered, s.Online, clients)
			}
			summary, err := d.pop.finish()
			if err != nil {
				t.Fatal(err)
			}
			if summary.DroppedClients < affected || len(summary.Reconnects) != summary.DroppedClients {
				t.Fatalf("summary: %d dropped, %d reconnect times, %d on the crashed node", summary.DroppedClients, len(summary.Reconnects), affected)
			}
			for i := 1; i < len(summary.Reconnects); i++ {
				if summary.Reconnects[i] < summary.Reconnects[i-1] {
					t.Fatalf("reconnect times not sorted at %d", i)
				}
			}
			if percentile(summary.Reconnects, 1) != summary.Reconnects[len(summary.Reconnects)-1] {
				t.Fatal("the max percentile is not the largest reconnect time")
			}
			rec := d.pop.record()
			if tc.dropBulk {
				if rec.BulkDropped == 0 || len(rec.Series) != 0 {
					t.Fatalf("bulk dropped %d, series %d: the drop case did not drop", rec.BulkDropped, len(rec.Series))
				}
			} else if len(rec.Series) == 0 || !rec.Verdict.Measured {
				t.Fatalf("no client-host series (%d) or no measured verdict", len(rec.Series))
			}
			d.pop.close()
			select {
			case code := <-d.code:
				if code != driverOK {
					t.Fatalf("driver exit %d; stderr:\n%s", code, d.stderr.String())
				}
			case <-time.After(time.Minute):
				t.Fatal("the driver did not exit after close")
			}
			waitConnections(t, c, 0, 30*time.Second)
		})
	}
}

func waitConnections(t *testing.T, c *Cluster, want int, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		got := connectionsOnRunning(c)
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d client connections remain, want %d", got, want)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestDriverEndsWhenInputCloses: an ssh that dies closes the driver's stdin,
// and the driver must end every client and exit rather than linger.
func TestDriverEndsWhenInputCloses(t *testing.T) {
	const clients = 200
	c := New(t, Options{})
	d := startPipedDriver(t, c, clients, testDriverOptions())
	if _, err := d.pop.hello(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var completed atomic.Int64
	if _, err := d.pop.fill(ctx, &completed); err != nil {
		t.Fatal(err)
	}
	if got := connectionsOnRunning(c); got != clients {
		t.Fatalf("%d connections after the fill, want %d", got, clients)
	}
	_ = d.stdin.Close()
	select {
	case code := <-d.code:
		if code != driverInputGone {
			t.Fatalf("driver exit %d, want %d", code, driverInputGone)
		}
	case <-time.After(3 * time.Minute):
		t.Fatal("the driver did not exit when its input closed")
	}
	waitConnections(t, c, 0, 30*time.Second)
}

// TestDriverRefusesOutOfOrder: a message out of order is refused by name and
// ends the driver.
func TestDriverRefusesOutOfOrder(t *testing.T) {
	var in bytes.Buffer
	_ = json.NewEncoder(&in).Encode(driverMsg{Type: msgFill})
	var out bytes.Buffer
	code := runDriver(&in, &out, io.Discard, testDriverOptions())
	if code != driverProtocol {
		t.Fatalf("exit %d, want %d", code, driverProtocol)
	}
	var m driverMsg
	if err := json.NewDecoder(&out).Decode(&m); err != nil {
		t.Fatal(err)
	}
	if m.Type != msgRefused || len(m.Reasons) != 1 || !strings.Contains(m.Reasons[0], `"fill"`) || !strings.Contains(m.Reasons[0], `"hello"`) {
		t.Fatalf("got %+v", m)
	}
}

// TestDriverRefusesAnUnsettledHost: the driver waits for its starting
// conditions and refuses, naming them, when they do not come.
func TestDriverRefusesAnUnsettledHost(t *testing.T) {
	// The input stays open: a closed input ends the driver before it refuses.
	in, inW := io.Pipe()
	t.Cleanup(func() { _ = inW.Close() })
	go func() {
		_ = json.NewEncoder(inW).Encode(driverMsg{Type: msgHello, Clients: 10, Nodes: []string{"127.0.0.1:1"}, Sources: []string{"127.0.0.11"}, WaitMs: 50})
	}()
	opts := testDriverOptions()
	opts.facts = func() clientStartFacts {
		f := settledFacts()
		f.TCPInUseTW = 5000
		return f
	}
	var out bytes.Buffer
	if code := runDriver(in, &out, io.Discard, opts); code != driverRefused {
		t.Fatalf("exit %d, want %d", code, driverRefused)
	}
	var m driverMsg
	if err := json.NewDecoder(&out).Decode(&m); err != nil {
		t.Fatal(err)
	}
	if m.Type != msgRefused || len(m.Reasons) != 1 || !strings.Contains(m.Reasons[0], "5000 TCP sockets") {
		t.Fatalf("got %+v", m)
	}
}

// blockingWriter holds every write until released.
type blockingWriter struct {
	release chan struct{}
	mu      sync.Mutex
	b       bytes.Buffer
}

func (w *blockingWriter) Write(p []byte) (int, error) {
	<-w.release
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.Write(p)
}

// TestDriverWriterKeepsReliableMessages: with the reader stalled, offered
// messages are dropped and counted, and a reliable message still arrives.
func TestDriverWriterKeepsReliableMessages(t *testing.T) {
	out := &blockingWriter{release: make(chan struct{})}
	release := sync.OnceFunc(func() { close(out.release) })
	t.Cleanup(release)
	w := newDriverWriter(out, io.Discard, false)
	for i := 0; i < 1000; i++ {
		w.offer(driverMsg{Type: msgSample, Online: i})
	}
	if w.dropped.Load() == 0 {
		t.Fatal("a stalled reader dropped nothing")
	}
	sent := make(chan struct{})
	go func() {
		w.send(driverMsg{Type: msgCross, Online: 7})
		close(sent)
	}()
	release()
	<-sent
	w.stop()
	out.mu.Lock()
	defer out.mu.Unlock()
	dec := json.NewDecoder(&out.b)
	found := false
	for {
		var m driverMsg
		if err := dec.Decode(&m); err != nil {
			break
		}
		if m.Type == msgCross && m.Online == 7 {
			found = true
		}
	}
	if !found {
		t.Fatal("the reliable message was lost")
	}
}
