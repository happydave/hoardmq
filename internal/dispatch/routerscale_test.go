package dispatch

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/session"
)

// scaleConn is a connection that takes every delivery and keeps none, safe
// for concurrent use.
type scaleConn struct {
	id        string
	identity  auth.Identity
	delivered atomic.Int64
}

func (f *scaleConn) ClientID() string               { return f.id }
func (f *scaleConn) Version() packet.Version        { return packet.V5 }
func (f *scaleConn) Identity() *auth.Identity       { return &f.identity }
func (f *scaleConn) SessionExpiryOverride() *uint32 { return nil }
func (f *scaleConn) Send(packet.Packet) error       { return nil }
func (f *scaleConn) Offer(packet.Packet) error      { return nil }
func (f *scaleConn) Close(packet.ReasonCode)        {}
func (f *scaleConn) SendQuota() int                 { return 1 << 30 }
func (f *scaleConn) Deliver(*packet.Publish) error {
	f.delivered.Add(1)
	return nil
}

// TestRouterScale measures the router under a fan-in load (many publishers,
// few topics each, subscribers on exact topics and a wildcard) with and
// without a concurrent subscribe and unsubscribe churn, at each publisher
// count HOARDMQ_ROUTER_SCALE lists. For each it reports the publishes a
// second, through the router alone and through the session manager's publish
// path, and the share of the run the publishers spent waiting for the
// router's lock, from the mutex profile. It only measures.
func TestRouterScale(t *testing.T) {
	spec := os.Getenv("HOARDMQ_ROUTER_SCALE")
	if spec == "" {
		t.Skip("HOARDMQ_ROUTER_SCALE is not set")
	}
	var counts []int
	for _, f := range strings.Split(spec, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(f))
		if err != nil || n < 1 {
			t.Fatalf("HOARDMQ_ROUTER_SCALE %q: publisher counts of at least 1", spec)
		}
		counts = append(counts, n)
	}
	const (
		devices  = 10000 // exact subscribers, one topic each
		watchers = 10    // dev/+/state subscribers
		window   = 3 * time.Second
	)
	r := New(RoundRobin)
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	m := session.New(session.Options{Router: r, Authorizers: auth.AuthorizerChain{allow{}}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Clock: clock, MaxQueued: 100})
	connect := func(id string) *scaleConn {
		c := &scaleConn{id: id, identity: auth.Identity{ClientID: id}}
		m.Connect(c, &packet.Connect{ProtocolLevel: packet.V5, ClientID: id, CleanStart: true}, 0)
		m.Resume(c)
		return c
	}
	subscribe := func(c *scaleConn, filter string) {
		m.Subscribe(c, &packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: filter}}})
		m.Subscribed(c)
	}
	var subs []*scaleConn
	for i := 0; i < devices; i++ {
		c := connect(fmt.Sprintf("dev-%d", i))
		subscribe(c, fmt.Sprintf("dev/%d/state", i))
		subs = append(subs, c)
	}
	for i := 0; i < watchers; i++ {
		c := connect(fmt.Sprintf("watch-%d", i))
		subscribe(c, "dev/+/state")
		subs = append(subs, c)
	}
	delivered := func() int64 {
		var n int64
		for _, c := range subs {
			n += c.delivered.Load()
		}
		return n
	}
	t.Logf("router-scale setup: %d exact subscribers, %d on dev/+/state; each publish matches %d; GOMAXPROCS %d", devices, watchers, 1+watchers, runtime.GOMAXPROCS(0))

	runtime.SetMutexProfileFraction(1)
	t.Cleanup(func() { runtime.SetMutexProfileFraction(0) })

	for _, path := range []string{"router", "publish"} {
		for _, churn := range []bool{false, true} {
			for _, n := range counts {
				var pubConns []*scaleConn
				if path == "publish" {
					for p := 0; p < n; p++ {
						pubConns = append(pubConns, connect(fmt.Sprintf("pub-%d-%v-%d", n, churn, p)))
					}
				}
				var stop atomic.Bool
				var ops, churned atomic.Int64
				var wg sync.WaitGroup
				before := routerWait(t)
				deliveredBefore := delivered()
				began := time.Now()
				for p := 0; p < n; p++ {
					wg.Add(1)
					go func(p int) {
						defer wg.Done()
						for k := 0; !stop.Load(); k++ {
							tp := fmt.Sprintf("dev/%d/state", (p*7919+k)%devices)
							if path == "router" {
								if got := len(r.Match(&message.Message{Namespace: "default", Topic: tp}, nil)); got != 1+watchers {
									t.Errorf("%s matched %d, want %d", tp, got, 1+watchers)
									return
								}
							} else {
								if code, _ := m.Publish(pubConns[p], &packet.Publish{Topic: tp, Payload: []byte("x")}); code.IsError() {
									t.Errorf("publish %s: %v", tp, code)
									return
								}
							}
							ops.Add(1)
						}
					}(p)
				}
				if churn {
					for g := 0; g < 4; g++ {
						c := connect(fmt.Sprintf("churn-%s-%d-%d", path, n, g))
						wg.Add(1)
						go func(c *scaleConn, g int) {
							defer wg.Done()
							for k := 0; !stop.Load(); k++ {
								f := fmt.Sprintf("churn/%d/%d", g, k%100)
								subscribe(c, f)
								m.Unsubscribe(c, &packet.Unsubscribe{PacketID: 1, Filters: []string{f}})
								churned.Add(2)
							}
						}(c, g)
					}
				}
				time.Sleep(window)
				stop.Store(true)
				wg.Wait()
				el := time.Since(began)
				wait := routerWait(t) - before
				if path == "publish" && delivered()-deliveredBefore != ops.Load()*int64(1+watchers) {
					t.Errorf("%d publishes delivered %d, want %d", ops.Load(), delivered()-deliveredBefore, ops.Load()*int64(1+watchers))
				}
				t.Logf("router-scale %s churn=%v publishers=%d: %.0f a second; %d subscribe changes; router lock wait %.3f s in %.2f s (%.1f%% of the publishers' time)",
					path, churn, n, float64(ops.Load())/el.Seconds(), churned.Load(), wait.Seconds(), el.Seconds(), 100*wait.Seconds()/(el.Seconds()*float64(n)))
				for _, c := range pubConns {
					m.Disconnect(c, true, packet.Success, false)
				}
			}
		}
	}
}

// routerWait is the total time goroutines have waited for the router's
// lock, from the mutex profile's samples whose stack runs through it.
func routerWait(t *testing.T) time.Duration {
	var buf bytes.Buffer
	if err := pprof.Lookup("mutex").WriteTo(&buf, 1); err != nil {
		t.Fatal(err)
	}
	var cyclesPerSecond float64
	var total float64
	var cycles float64
	inRouter := false
	flush := func() {
		if inRouter {
			total += cycles
		}
		cycles, inRouter = 0, false
	}
	sc := bufio.NewScanner(&buf)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		switch {
		case strings.HasPrefix(line, "cycles/second="):
			cyclesPerSecond, _ = strconv.ParseFloat(strings.TrimPrefix(line, "cycles/second="), 64)
		case len(line) > 0 && line[0] >= '0' && line[0] <= '9':
			flush()
			f := strings.Fields(line)
			cycles, _ = strconv.ParseFloat(f[0], 64)
		case strings.HasPrefix(line, "#") && strings.Contains(line, "dispatch.(*Router)."):
			inRouter = true
		}
	}
	flush()
	if cyclesPerSecond == 0 {
		t.Fatal("the mutex profile names no cycles/second")
	}
	return time.Duration(total / cyclesPerSecond * float64(time.Second))
}
