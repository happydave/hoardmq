package sim

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/packet"
)

// bounceEnv reads an integer knob of the bounce storm, def when unset.
func bounceEnv(t *testing.T, name string, def int) int {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		t.Fatalf("%s=%q: want a non-negative integer", name, v)
	}
	return n
}

// TestBounceStormKeepsSessions is a diagnostic storm: a lost node's clients
// reconnect to the survivors while the survivors' adoptions are held back and
// their imports and acquires are delayed past the client's 5 s CONNECT
// deadline, so each client's CONNECTs bounce between the survivors and many
// finish after their client has left. Afterwards every one of the lost node's
// clients must still receive a message on its own filter: a session served
// empty, or resumed empty with Session Present, fails it. It runs only when
// HOARDMQ_SIM_BOUNCE is set; HOARDMQ_SIM_BOUNCE_LOG names the nodes' debug
// log, and HOARDMQ_SIM_BOUNCE_DELIVERY=off ends it after the route census.
func TestBounceStormKeepsSessions(t *testing.T) {
	if os.Getenv("HOARDMQ_SIM_BOUNCE") == "" {
		t.Skip("diagnostic storm; set HOARDMQ_SIM_BOUNCE")
	}
	seed := int64(bounceEnv(t, "HOARDMQ_SIM_BOUNCE_SEED", 1))
	clients := bounceEnv(t, "HOARDMQ_SIM_BOUNCE_CLIENTS", 3000)
	importMax := time.Duration(bounceEnv(t, "HOARDMQ_SIM_BOUNCE_IMPORT_MS", 8000)) * time.Millisecond
	acquireMax := time.Duration(bounceEnv(t, "HOARDMQ_SIM_BOUNCE_ACQUIRE_MS", 3000)) * time.Millisecond
	adoptDelay := time.Duration(bounceEnv(t, "HOARDMQ_SIM_BOUNCE_ADOPT_MS", 1000)) * time.Millisecond
	window := time.Duration(bounceEnv(t, "HOARDMQ_SIM_BOUNCE_WINDOW_S", 240)) * time.Second
	takeoverDelay := time.Duration(bounceEnv(t, "HOARDMQ_SIM_BOUNCE_TAKEOVER_MS", 0)) * time.Millisecond

	var logFile *os.File
	if p := os.Getenv("HOARDMQ_SIM_BOUNCE_LOG"); p != "" {
		f, err := os.Create(p) //nolint:gosec // operator-chosen path
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = f.Close() })
		logFile = f
	}
	var armed atomic.Bool
	var rngMu sync.Mutex
	rng := rand.New(rand.NewSource(seed)) //nolint:gosec // seeded simulation
	draw := func(max time.Duration) time.Duration {
		if max <= 0 {
			return 0
		}
		rngMu.Lock()
		defer rngMu.Unlock()
		return time.Duration(rng.Int63n(int64(max)))
	}
	o := Options{Timings: TimingsProduct, Shards: 64, AdoptionDelay: adoptDelay,
		ImportGate: func(node, _, _ string) {
			if armed.Load() {
				time.Sleep(draw(importMax))
			}
		},
		AcquireGate: func(node, _, _ string, granted bool) {
			if armed.Load() && !granted {
				time.Sleep(draw(acquireMax))
			}
		}}
	if logFile != nil {
		o.Log = logFile
	}
	if takeoverDelay > 0 {
		// Every takeover pass on the survivors waits this long, so pulls queue
		// behind the workers as at scale.
		o.TakeoverDelay = func(node string) time.Duration {
			if node == "n2" {
				return 0
			}
			return takeoverDelay
		}
	}
	c := New(t, o)
	n1, n2, n3 := c.Node("n1"), c.Node("n2"), c.Node("n3")
	addrs := []string{n1.TCP, n2.TCP, n3.TCP}

	ctx, cancel := context.WithTimeout(context.Background(), window+5*time.Minute)
	defer cancel()
	all := make([]*Client, clients)
	var wg sync.WaitGroup
	sem := make(chan struct{}, 128)
	var fillErr atomic.Value
	for i := 0; i < clients; i++ {
		order := append(append([]string(nil), addrs[i%3:]...), addrs[:i%3]...)
		cl := NewClient(fmt.Sprintf("f%06d", i), order, 3600, int64(i))
		all[i] = cl
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if err := cl.Run(ctx); err != nil {
				fillErr.Store(err)
				return
			}
			if err := cl.Subscribe(ctx, "b/"+cl.ID, 1); err != nil {
				fillErr.Store(err)
			}
		}()
	}
	wg.Wait()
	defer func() {
		for _, cl := range all {
			cl.Close()
		}
	}()
	if err, _ := fillErr.Load().(error); err != nil {
		t.Fatalf("fill: %v", err)
	}
	var victims []*Client
	for i, cl := range all {
		if i%3 == 1 {
			victims = append(victims, cl)
		}
	}
	for _, cl := range victims {
		cl.TakeEvents()
	}
	before := map[*Client]int64{}
	for _, cl := range victims {
		before[cl] = cl.SessionLost.Load()
	}
	time.Sleep(5 * time.Second)

	crashed := time.Now()
	armed.Store(true)
	c.Crash(n2)
	t.Logf("crashed n2 with %d clients, %d of them its own; seed %d, imports up to %v, acquires up to %v, adoption pass %v, takeover pass %v",
		clients, len(victims), seed, importMax, acquireMax, adoptDelay, takeoverDelay)
	deadline := crashed.Add(window)
	back := 0
	for time.Now().Before(deadline) {
		back = 0
		for _, cl := range victims {
			if cl.Recovered() {
				back++
			}
		}
		if back == len(victims) {
			break
		}
		time.Sleep(time.Second)
	}
	armed.Store(false)
	t.Logf("%d of %d recovered %.1f s after the crash", back, len(victims), time.Since(crashed).Seconds())
	for settle := time.Now().Add(2 * time.Minute); back < len(victims) && time.Now().Before(settle); time.Sleep(time.Second) {
		back = 0
		for _, cl := range victims {
			if cl.Recovered() {
				back++
			}
		}
	}
	t.Logf("%d of %d recovered %.1f s after the crash, the gates off since the window", back, len(victims), time.Since(crashed).Seconds())
	// Let the delayed imports and passes started under the gates finish.
	time.Sleep(15 * time.Second)
	// Read before the publishes, whose sequential pass outlasts the test's
	// bound at tens of thousands of clients.
	census := routeCensus(t, all, n1, n3)
	if os.Getenv("HOARDMQ_SIM_BOUNCE_DELIVERY") == "off" {
		census.judge(t)
		return
	}

	pub := NewClient("bounce-pub", []string{n1.TCP, n3.TCP}, 0, seed)
	if err := pub.Run(ctx); err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	for _, cl := range victims {
		cl.TakeDeliveries()
	}
	// Each client's own filter gets one QoS 1 message. No Matching
	// Subscribers says the broker holds no subscription for it; a refusal is
	// retried, and one never accepted is reported apart, not as a loss.
	noSub := map[*Client]bool{}
	unpublished := map[*Client]packet.ReasonCode{}
	for _, cl := range victims {
		var code packet.ReasonCode
		var err error
		for try := 0; try < 60; try++ {
			code, err = pub.Publish(ctx, "b/"+cl.ID, 1, 1)
			if err == nil {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		switch {
		case err != nil:
			unpublished[cl] = code
		case code == packet.NoMatchingSubscribers:
			noSub[cl] = true
		}
	}
	got := map[*Client]bool{}
	waitDeadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(waitDeadline) && len(got) < len(victims) {
		for _, cl := range victims {
			for _, d := range cl.TakeDeliveries() {
				if d.Topic == "b/"+cl.ID {
					got[cl] = true
				}
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	var silent, told, offline, unpub, undelivered []string
	for _, cl := range victims {
		lost := cl.SessionLost.Load() - before[cl]
		switch {
		case got[cl]:
		case unpublished[cl] != 0:
			unpub = append(unpub, fmt.Sprintf("%s:%s", cl.ID, unpublished[cl]))
		case !cl.Recovered():
			offline = append(offline, cl.ID)
		case lost > 0:
			told = append(told, cl.ID)
		case noSub[cl]:
			silent = append(silent, cl.ID)
		default:
			undelivered = append(undelivered, cl.ID)
		}
	}
	// Where each lost client's session is now, and what it holds: a session
	// held with its filter while publishes find no subscriber is a routing
	// fault, one held without it a lost session.
	for _, id := range append(append([]string(nil), silent...), undelivered...) {
		for _, n := range []*Node{n1, n3} {
			l, ok := n.Server().Leases().Table().Lookup("default", id)
			st, held := n.Server().Sessions().Export("default", id)
			subs := -1
			if held {
				subs = len(st.Subscriptions)
			}
			t.Logf("lost %s on %s: lease %v owner %s token %d online %v; held %v subscriptions %d", id, n.Name, ok, l.Owner, l.Epoch, l.Online, held, subs)
		}
	}
	empties := 0
	if logFile != nil {
		if data, err := os.ReadFile(logFile.Name()); err == nil {
			empties = strings.Count(string(data), "session served empty although its lease says one existed")
		}
	}
	t.Logf("delivered %d of %d; silent losses (no matching subscriber) %d %v; told Session Present 0 %d %v; accepted, not delivered %d %v; not online %d %v; never accepted %d %v; empty serves logged %d",
		len(got), len(victims), len(silent), first(silent), len(told), first(told), len(undelivered), first(undelivered), len(offline), first(offline), len(unpub), first(unpub), empties)
	if len(silent) > 0 || len(told) > 0 || len(undelivered) > 0 || empties > 0 {
		t.Errorf("sessions lost: %d silently, %d told; %d empty serves", len(silent), len(told), empties)
	}
	census.judge(t)
}

// judge fails the test on any census gap a publish could miss.
func (c censusCounts) judge(t *testing.T) {
	t.Helper()
	if c.unrouted > 0 || c.undispatched > 0 || c.unseen > 0 {
		t.Errorf("route census: %d held filters unadvertised by their node, %d held filters missing from the dispatcher, %d advertised routes the other survivor lacks",
			c.unrouted, c.undispatched, c.unseen)
	}
}

// censusCounts is what routeCensus found wrong.
type censusCounts struct{ unrouted, undispatched, unseen, ghosts int }

// routeCensus compares, for every client and each survivor, the session the
// node holds with its filter, the dispatcher's match for that filter, the
// node's own advertised entry, and the other survivor's view of that entry.
// A held filter the node does not advertise is reachable only by publishes
// made on the node itself; one the dispatcher lacks is reachable by none.
func routeCensus(t *testing.T, all []*Client, a, b *Node) censusCounts {
	t.Helper()
	var c censusCounts
	var named []string
	for _, cl := range all {
		filter := "b/" + cl.ID
		for _, pair := range [][2]*Node{{a, b}, {b, a}} {
			n, other := pair[0], pair[1]
			st, held := n.Server().Sessions().Export("default", cl.ID)
			holds := false
			if held {
				for _, sb := range st.Subscriptions {
					if sb.Filter == filter {
						holds = true
					}
				}
			}
			dispatched := len(n.Server().Sessions().Router().Match(&message.Message{Namespace: "default", Topic: filter, QoS: 1}, nil)) > 0
			own := n.Server().Routes().Table().OwnCount("default", filter) > 0
			e, ok := other.Server().Routes().Table().Lookup(n.Name, "default", filter)
			seen := ok && !e.Tombstone
			switch {
			case holds && !own:
				c.unrouted++
				named = append(named, fmt.Sprintf("%s on %s: held, dispatched %v, not advertised, other survivor sees it %v", cl.ID, n.Name, dispatched, seen))
			case holds && !dispatched:
				c.undispatched++
				named = append(named, fmt.Sprintf("%s on %s: held and advertised, missing from the dispatcher", cl.ID, n.Name))
			case own && !seen:
				c.unseen++
				named = append(named, fmt.Sprintf("%s on %s: advertised, the other survivor holds no live entry", cl.ID, n.Name))
			case own && !holds:
				c.ghosts++
				named = append(named, fmt.Sprintf("%s on %s: advertised with no session holding the filter, dispatched %v", cl.ID, n.Name, dispatched))
			}
		}
	}
	t.Logf("route census over %d clients: %d held filters unadvertised, %d missing from the dispatcher, %d advertised and unseen, %d advertised without a holder; %v",
		len(all), c.unrouted, c.undispatched, c.unseen, c.ghosts, first(named))
	return c
}

func first(ids []string) []string {
	if len(ids) > 20 {
		return ids[:20]
	}
	return ids
}
