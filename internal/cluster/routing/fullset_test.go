package routing

import (
	"bytes"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/dispatch"
)

// A node's full route set used to travel as one transport frame, refused
// over the frame limit. These tests put a node's own entries on either side
// of a small limit and read what its peers can prove: that a full set
// arrives, that a node started later learns the node's routes, and that a gap
// in its deltas is repaired.

// frameLimit is the transport frame limit these tests run under; each entry
// of the filters below takes about 90 bytes of a set's JSON.
const frameLimit = 64 << 10

// lockedBuffer collects one node's log lines.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) count(s string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Count(l.b.String(), s)
}

// labelMetrics records counters and gauges by name and every label.
type labelMetrics struct {
	mu sync.Mutex
	n  map[string]float64
}

func newLabelMetrics() *labelMetrics { return &labelMetrics{n: map[string]float64{}} }

func metricKey(name string, labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(name)
	for _, k := range keys {
		fmt.Fprintf(&b, "|%s=%s", k, labels[k])
	}
	return b.String()
}

func (m *labelMetrics) Add(name string, labels map[string]string, d float64) {
	m.mu.Lock()
	m.n[metricKey(name, labels)] += d
	m.mu.Unlock()
}

func (m *labelMetrics) Set(name string, labels map[string]string, v float64) {
	m.mu.Lock()
	m.n[metricKey(name, labels)] = v
	m.mu.Unlock()
}

// get reads a metric by name and label pairs ("reason", "too-large", ...).
func (m *labelMetrics) get(name string, kv ...string) float64 {
	labels := map[string]string{}
	for i := 0; i+1 < len(kv); i += 2 {
		labels[kv[i]] = kv[i+1]
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.n[metricKey(name, labels)]
}

// frameOpts are one test node's settings.
type frameOpts struct {
	maxVersion uint8
	legacy     bool
	members    func() []string
	tweak      func(p *Plane)
	// wrap stands in for the plane's transport; sync overrides the 300 ms
	// sync interval.
	wrap func(*transport.Transport) Transport
	sync time.Duration
}

// fnode is a test node with its log and metrics.
type fnode struct {
	*tnode
	logs *lockedBuffer
	m    *labelMetrics
	stop func()
}

// frameNode starts one node's transport and plane under the frame limit,
// with its log at Debug.
func frameNode(t *testing.T, name string, o frameOpts) *fnode {
	t.Helper()
	buf := &lockedBuffer{}
	log := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	tr, err := transport.New(transport.Options{Node: name, ClusterID: "t", Listen: "127.0.0.1:0",
		Auth: transport.Auth{Mode: "secret", Secret: []byte(secret)}, MaxFrame: frameLimit, MaxVersion: o.maxVersion,
		Heartbeat: 50 * time.Millisecond, HeartbeatMisses: 4, HandshakeTimeout: 2 * time.Second, Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	m := newLabelMetrics()
	sink := &fakeSink{}
	var ptr Transport = tr
	if o.wrap != nil {
		ptr = o.wrap(tr)
	}
	interval := 300 * time.Millisecond
	if o.sync != 0 {
		interval = o.sync
	}
	plane := New(Options{Node: name, Transport: ptr, Strategy: dispatch.RoundRobin, SyncInterval: interval,
		Logger: log, Metrics: m, MaxFrame: frameLimit, Legacy: o.legacy, Members: o.members})
	if o.tweak != nil {
		o.tweak(plane)
	}
	plane.SetSink(sink)
	if err := tr.Start(); err != nil {
		t.Fatal(err)
	}
	plane.Start()
	stop := sync.OnceFunc(func() {
		plane.Stop()
		tr.Stop()
	})
	t.Cleanup(stop)
	return &fnode{tnode: &tnode{name: name, tr: tr, plane: plane, sink: sink}, logs: buf, m: m, stop: stop}
}

// frameFilter is the population's filter for client i.
func frameFilter(i int) string { return fmt.Sprintf("f/f%06d", i) }

// growOwnSet has b observe n filters from first, fifty at a time, each chunk
// waited for at a, so every delta stays far under the limit and none is lost.
func growOwnSet(t *testing.T, a, b *fnode, first, n int) {
	t.Helper()
	for i := first; i < first+n; i += 50 {
		end := min(i+50, first+n)
		for j := i; j < end; j++ {
			b.plane.Observe("default", frameFilter(j), 1)
		}
		last := frameFilter(end - 1)
		waitFor(t, "delta at "+a.name, 5*time.Second, func() bool {
			_, ok := a.plane.Table().Lookup(b.name, "default", last)
			return ok
		})
	}
}

func pairUp(t *testing.T, nodes ...*fnode) {
	t.Helper()
	for _, n := range nodes {
		var ps []config.Peer
		for _, o := range nodes {
			if o != n {
				ps = append(ps, config.Peer{Name: o.name, Address: o.tr.Addr().String()})
			}
		}
		n.tr.SetPeers(ps)
	}
	waitFor(t, "peers up", 10*time.Second, func() bool {
		for _, n := range nodes {
			for _, p := range n.tr.Peers() {
				if p.State != transport.Up || p.DataState != transport.Up {
					return false
				}
			}
		}
		return true
	})
}

// liveFrom counts the live entries a node holds of an owner's.
func liveFrom(n *fnode, owner string, upTo int) int {
	held := 0
	for i := 0; i < upTo; i++ {
		if e, ok := n.plane.Table().Lookup(owner, "default", frameFilter(i)); ok && !e.Tombstone {
			held++
		}
	}
	return held
}

// TestFullSetInPartsAboveTheFrameLimit: with b's own entries over the frame
// limit, b's periodic full set still arrives at a, in parts, a node started
// afterwards learns every owner, and a gap in b's deltas at a is repaired.
// On a build that sends the set in one frame none of the three happens.
func TestFullSetInPartsAboveTheFrameLimit(t *testing.T) {
	for _, tc := range []struct {
		name    string
		entries int
	}{
		{"under", 300}, // about 27 KB a set
		{"over", 1500}, // about 135 KB a set
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := frameNode(t, "a", frameOpts{})
			b := frameNode(t, "b", frameOpts{})
			pairUp(t, a, b)
			growOwnSet(t, a, b, 0, tc.entries)

			// Periodic full sets arrive and keep every entry of b's at a:
			// a set applied in pieces would withdraw what the other pieces
			// carry.
			synced := a.m.get("hoardmq_route_syncs_total", "direction", "in")
			waitFor(t, "two more full sets from b at a", 5*time.Second, func() bool {
				return a.m.get("hoardmq_route_syncs_total", "direction", "in") >= synced+2
			})
			if age := a.plane.SyncAges()["b"]; age > 2*time.Second {
				t.Errorf("a's last full set from b %v ago with %d entries", age, tc.entries)
			}
			if held := liveFrom(a, "b", tc.entries); held != tc.entries {
				t.Errorf("a holds %d of b's %d entries after its full sets", held, tc.entries)
			}
			if n := b.m.get("hoardmq_route_send_failures_total", "kind", "full", "reason", "too-large"); n != 0 {
				t.Errorf("b refused %v full sets for the frame limit", n)
			}

			// A node started now, as a restarted one is, needs a full set
			// from every owner before it knows their routes.
			c := frameNode(t, "c", frameOpts{})
			pairUp(t, a, b, c)
			waitFor(t, "c to know every owner", 5*time.Second, c.plane.OwnersKnown)

			// A gap in b's deltas at a: a short partition loses the deltas
			// sent across it. A full set proves b's routes again.
			if !a.plane.routesHeld("b", b.plane.RouteMark()) {
				t.Fatalf("a did not hold b's routes before the gap")
			}
			a.tr.Isolate("b", true)
			b.tr.Isolate("a", true)
			time.Sleep(400 * time.Millisecond)
			for j := tc.entries; j < tc.entries+5; j++ {
				b.plane.Observe("default", frameFilter(j), 1)
			}
			time.Sleep(400 * time.Millisecond)
			a.tr.Isolate("b", false)
			b.tr.Isolate("a", false)
			waitFor(t, "a and b up again", 10*time.Second, func() bool {
				p, _ := a.plane.peerInfo("b")
				return p.State == transport.Up && p.DataState == transport.Up
			})
			b.plane.Observe("default", frameFilter(tc.entries+5), 1)
			waitFor(t, "a to hold b's routes up to b's mark again", 5*time.Second, func() bool {
				return a.plane.routesHeld("b", b.plane.RouteMark())
			})
		})
	}
}
