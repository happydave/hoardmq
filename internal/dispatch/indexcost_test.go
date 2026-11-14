package dispatch

import (
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/session"
)

// The subscribe path's cost of the filter index, at a node's scale: a router
// holding 600,000 distinct filters, one subscriber each. These benchmarks
// build on a router without the index too, so they compare the two builds;
// the walk is found by reflection and is absent there.

const costFilters = 600000

var (
	costOnce     sync.Once
	costR        *Router
	costSessions []*session.Session
)

// costRouter is the shared pre-filled router: costFilters sessions, session i
// subscribed to cost/i.
func costRouter(b *testing.B) (*Router, []*session.Session) {
	b.Helper()
	costOnce.Do(func() {
		r := New(RoundRobin)
		clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
		m := session.New(session.Options{Router: r, Authorizers: auth.AuthorizerChain{allow{}}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Clock: clock, MaxQueued: 100})
		costSessions = make([]*session.Session, 0, costFilters)
		for i := 0; i < costFilters; i++ {
			id := fmt.Sprintf("k%d", i)
			c := &scaleConn{id: id, identity: auth.Identity{ClientID: id}}
			m.Connect(c, &packet.Connect{ProtocolLevel: packet.V5, ClientID: id, CleanStart: true}, 0)
			m.Resume(c)
			f := fmt.Sprintf("cost/%d", i)
			m.Subscribe(c, &packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: f}}})
			ms := r.Match(&message.Message{Namespace: "default", Topic: f}, nil)
			if len(ms) != 1 {
				panic(fmt.Sprintf("%s matched %d", f, len(ms)))
			}
			costSessions = append(costSessions, ms[0].Session)
		}
		costR = r
	})
	return costR, costSessions
}

// reconcilerPace is about the reconciler's step rate at a node's scale: some
// 150 walk steps (600,000 filters) spread over 48 s of its 60 s interval.
const reconcilerPace = 333 * time.Millisecond

// walker runs walk steps over r until stopped, pace apart (none for 0), when
// the router has a walk.
func walker(r *Router, pace time.Duration) (stop func() int64) {
	m := reflect.ValueOf(r).MethodByName("WalkStep")
	if !m.IsValid() {
		return func() int64 { return 0 }
	}
	quit := make(chan struct{})
	var steps atomic.Int64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		cursor := reflect.ValueOf(0)
		for {
			select {
			case <-quit:
				return
			default:
			}
			out := m.Call([]reflect.Value{cursor})
			steps.Add(1)
			cursor = out[1]
			if out[2].Bool() {
				cursor = reflect.ValueOf(0)
			}
			if pace > 0 {
				select {
				case <-quit:
					return
				case <-time.After(pace):
				}
			}
		}
	}()
	return func() int64 {
		close(quit)
		wg.Wait()
		return steps.Load()
	}
}

// BenchmarkIndexAdd adds a new distinct filter each iteration: a first
// subscriber, so the index write.
func BenchmarkIndexAdd(b *testing.B) {
	r, ss := costRouter(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Add(ss[i%len(ss)], session.Subscription{Filter: fmt.Sprintf("add/%d", i)})
	}
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		r.Remove(ss[i%len(ss)], fmt.Sprintf("add/%d", i))
	}
}

// BenchmarkIndexRemove removes a filter's only subscriber each iteration: the
// index delete and the prune.
func BenchmarkIndexRemove(b *testing.B) {
	r, ss := costRouter(b)
	for i := 0; i < b.N; i++ {
		r.Add(ss[i%len(ss)], session.Subscription{Filter: fmt.Sprintf("rm/%d", i)})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Remove(ss[i%len(ss)], fmt.Sprintf("rm/%d", i))
	}
}

// BenchmarkIndexRemoveAll removes a departing session's 100 filters each
// iteration, as a takeover's eviction does.
func BenchmarkIndexRemoveAll(b *testing.B) {
	r, ss := costRouter(b)
	filters := make([]string, 100)
	for i := 0; i < b.N; i++ {
		s := ss[i%len(ss)]
		b.StopTimer()
		for j := range filters {
			filters[j] = fmt.Sprintf("all/%d/%d", i, j)
			r.Add(s, session.Subscription{Filter: filters[j]})
		}
		b.StartTimer()
		r.RemoveAll(s, filters)
	}
}

// BenchmarkIndexMatchNoWalk matches with no walk running: the index's cost to
// a match alone.
func BenchmarkIndexMatchNoWalk(b *testing.B) {
	r, _ := costRouter(b)
	benchMatch(b, r, -1)
}

// BenchmarkIndexMatchBesideWalk matches on one goroutine while another runs
// walk steps without pause: a stress figure, far above the reconciler's rate.
func BenchmarkIndexMatchBesideWalk(b *testing.B) {
	r, _ := costRouter(b)
	benchMatch(b, r, 0)
}

// BenchmarkIndexMatchBesidePacedWalk matches while walk steps run at the
// reconciler's rate.
func BenchmarkIndexMatchBesidePacedWalk(b *testing.B) {
	r, _ := costRouter(b)
	benchMatch(b, r, reconcilerPace)
}

// withFanIn subscribes every session to one filter for the benchmark.
func withFanIn(b *testing.B, r *Router, ss []*session.Session) {
	for _, s := range ss {
		r.Add(s, session.Subscription{Filter: "fan/in"})
	}
	b.Cleanup(func() {
		for _, s := range ss {
			r.Remove(s, "fan/in")
		}
	})
}

// BenchmarkIndexMatchFanInBesideWalk is BenchmarkIndexMatchBesideWalk with one
// filter holding every session, copied alone in its step.
func BenchmarkIndexMatchFanInBesideWalk(b *testing.B) {
	r, ss := costRouter(b)
	withFanIn(b, r, ss)
	benchMatch(b, r, 0)
}

// BenchmarkIndexMatchFanInBesidePacedWalk is the fan-in case at the
// reconciler's rate.
func BenchmarkIndexMatchFanInBesidePacedWalk(b *testing.B) {
	r, ss := costRouter(b)
	withFanIn(b, r, ss)
	benchMatch(b, r, reconcilerPace)
}

// benchMatch matches with a walk beside it, pace apart (0 unpaced), or with
// none for a negative pace.
func benchMatch(b *testing.B, r *Router, pace time.Duration) {
	topics := make([]*message.Message, 1024)
	for i := range topics {
		topics[i] = &message.Message{Namespace: "default", Topic: fmt.Sprintf("cost/%d", (i*7919)%costFilters)}
	}
	stop := func() int64 { return 0 }
	if pace >= 0 {
		stop = walker(r, pace)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if len(r.Match(topics[i%len(topics)], nil)) != 1 {
			b.Fatal("a cost filter matched other than one subscriber")
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(stop()), "walksteps")
}
