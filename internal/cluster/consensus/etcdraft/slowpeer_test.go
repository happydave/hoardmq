package etcdraft

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
	"github.com/happydave/hoardmq/internal/cluster/transport"
)

// TestSlowPeerAndTheLeadersOtherSends measures whether one follower whose
// link drains slowly, but stays up, delays the leader's sends to the other
// follower. It runs only with HOARDMQ_SLOWPEER set and only measures: it
// fails only when a precondition does not hold.
//
// The broker's timing: a 100 ms tick, an election at 10 ticks, a heartbeat
// every tick, and the transport's 1 s heartbeat with 3 misses. The slow link
// is the transport's per-peer fault, a delay before each consensus frame the
// leader writes to that follower (HOARDMQ_SLOWPEER_DELAY, default 1.5 s, under
// the 3 s silence limit, so the link stays up). HOARDMQ_SLOWPEER_DEPTH is the
// transport queue depth (default 1024, the broker's), and
// HOARDMQ_SLOWPEER_SECONDS how long the slow phase lasts (default 150).
func TestSlowPeerAndTheLeadersOtherSends(t *testing.T) {
	if os.Getenv("HOARDMQ_SLOWPEER") == "" {
		t.Skip("HOARDMQ_SLOWPEER is not set")
	}
	envInt := func(name string, def int) int {
		if v := os.Getenv(name); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n <= 0 {
				t.Fatalf("%s %q: a positive number", name, v)
			}
			return n
		}
		return def
	}
	depth := envInt("HOARDMQ_SLOWPEER_DEPTH", 1024)
	delay := time.Duration(envInt("HOARDMQ_SLOWPEER_DELAY_MS", 1500)) * time.Millisecond
	slowFor := time.Duration(envInt("HOARDMQ_SLOWPEER_SECONDS", 150)) * time.Second

	names := []string{"n1", "n2", "n3"}
	c := newTCluster(t, func(o *Options) {
		o.Tick, o.ElectionTicks, o.HeartbeatTicks = 100*time.Millisecond, 10, 1
	})
	c.heartbeat, c.misses, c.queueDepth = time.Second, 3, depth
	for _, name := range names {
		c.addTransport(name, others(name, names))
	}
	elog := &electionLog{}
	for _, name := range names {
		c.startCore(c.nodes[name], names, func(o *Options) { o.Logger = slog.New(&electionHandler{node: name, log: elog}) })
	}
	lead := c.settled(3)
	var slow, healthy *tnode
	for _, name := range names {
		if n := c.nodes[name]; n != lead {
			if slow == nil {
				slow = n
			} else {
				healthy = n
			}
		}
	}
	startTerm := lead.core.Status().Term

	// What the leader sends: the time of each send to the healthy follower
	// (gaps), and each send to the slow one that ended in an error (the
	// queue full past the send's timeout).
	var mu sync.Mutex
	var toHealthy []time.Time
	var slowErrs atomic.Int64
	var firstSlowErr atomic.Int64
	obs := func(peer string, cl transport.Class, _ int, err error) {
		if cl != transport.Consensus {
			return
		}
		switch peer {
		case healthy.name:
			mu.Lock()
			toHealthy = append(toHealthy, time.Now())
			mu.Unlock()
		case slow.name:
			if err != nil {
				slowErrs.Add(1)
				firstSlowErr.CompareAndSwap(0, time.Now().UnixNano())
			}
		}
	}
	lead.observe.Store(&obs)

	// A steady load of proposals, so appends go to both followers.
	stop := make(chan struct{})
	var proposals atomic.Int64
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				for _, n := range c.nodes {
					if n.core.Status().Role == "leader" {
						ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
						if _, err := n.core.Propose(ctx, consensus.KindConfig, consensus.ConfigCommand(fmt.Sprintf("slow%d-%d", g, i), "v")[1:]); err == nil {
							proposals.Add(1)
						}
						cancel()
						break
					}
				}
				time.Sleep(10 * time.Millisecond)
			}
		}(g)
	}
	defer func() {
		close(stop)
		wg.Wait()
	}()

	// Leadership, sampled every 50 ms: the first change and who started it.
	type change struct {
		at     time.Duration
		term   uint64
		leader string
	}
	var changes []change
	gapsOf := func(from, to time.Time) []time.Duration {
		mu.Lock()
		defer mu.Unlock()
		var ts []time.Time
		for _, x := range toHealthy {
			if !x.Before(from) && x.Before(to) {
				ts = append(ts, x)
			}
		}
		var gaps []time.Duration
		for i := 1; i < len(ts); i++ {
			gaps = append(gaps, ts[i].Sub(ts[i-1]))
		}
		sort.Slice(gaps, func(i, j int) bool { return gaps[i] < gaps[j] })
		return gaps
	}
	report := func(phase string, from, to time.Time) {
		g := gapsOf(from, to)
		if len(g) == 0 {
			t.Logf("slowpeer %s: no sends to the healthy follower", phase)
			return
		}
		over := 0
		for _, x := range g {
			if x > 500*time.Millisecond {
				over++
			}
		}
		t.Logf("slowpeer %s: %d sends to the healthy follower; gap median %s, p99 %s, max %s; %d gaps over 500 ms",
			phase, len(g)+1, g[len(g)/2], g[len(g)*99/100], g[len(g)-1], over)
	}

	// Baseline: 10 s with every link healthy.
	base := time.Now()
	time.Sleep(10 * time.Second)
	report("baseline", base, time.Now())

	lead.tr.SetFault(slow.name, &transport.Fault{Delay: delay, Classes: []transport.Class{transport.Consensus}})
	began := time.Now()
	deadline := began.Add(slowFor)
	lastTerm := startTerm
	campaigning := map[string]int{}
	twoLeaders, run, longestRun := 0, 0, 0
	for time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
		leaders := 0
		for _, n := range c.nodes {
			st := n.core.Status()
			if st.Role == "candidate" {
				campaigning[n.name]++
			}
			if st.Role == "leader" {
				leaders++
				if st.Term > lastTerm {
					changes = append(changes, change{at: time.Since(began), term: st.Term, leader: n.name})
					lastTerm = st.Term
				}
			}
		}
		// A deposed leader whose loop is held still reports the role at its
		// old term until the loop runs again.
		if leaders > 1 {
			twoLeaders++
			run++
			longestRun = max(longestRun, run)
		} else {
			run = 0
		}
	}
	lead.tr.SetFault(slow.name, nil)
	full := time.Duration(0)
	if ns := firstSlowErr.Load(); ns != 0 {
		full = time.Unix(0, ns).Sub(began)
	}
	t.Logf("slowpeer setup: depth %d, delay %s to %s, healthy %s, leader %s at term %d; %d proposals committed",
		depth, delay, slow.name, healthy.name, lead.name, startTerm, proposals.Load())
	t.Logf("slowpeer slow phase %s: the first send to the slow follower to time out at %s; %d timed out",
		slowFor, full, slowErrs.Load())
	end := deadline
	if len(changes) > 0 {
		end = began.Add(changes[0].at)
	}
	report("slow phase, to the first leader change or the end", began, end)
	if full > 0 {
		report("slow phase, before the first time-out", began, began.Add(full))
		report("slow phase, from the first time-out to the first leader change or the end", began.Add(full), end)
	}
	for _, ch := range changes {
		t.Logf("slowpeer leader change at %s: %s leads at term %d", ch.at.Round(time.Millisecond), ch.leader, ch.term)
	}
	if len(changes) == 0 {
		t.Logf("slowpeer no leader change in %s", slowFor)
	}
	st := lead.core.Status()
	t.Logf("slowpeer samples (every 50 ms) with the healthy follower campaigning: %d; the slow follower: %d; with two nodes reporting the leader role: %d; the first leader's term now %d",
		campaigning[healthy.name], campaigning[slow.name], twoLeaders, st.Term)

	// The verdict, against fixed bounds: the healthy follower's largest gap
	// before any leader change, the elections it started (from the raft
	// library's own log lines), and the longest time two nodes said they
	// led. The slow follower's elections are counted apart.
	maxGap := time.Duration(0)
	if g := gapsOf(began, end); len(g) > 0 {
		maxGap = g[len(g)-1]
	}
	starts := elog.startsSince(began)
	counts := elog.countsSince(began)
	for _, kind := range []string{"start", "prevote-ignored", "prevote-rejected", "prevote-cast", "vote-cast", "stepdown", "leader"} {
		t.Logf("slowpeer raft %s: the healthy follower %d, the slow follower %d, the first leader %d",
			kind, counts[kind][healthy.name], counts[kind][slow.name], counts[kind][lead.name])
	}
	for _, e := range elog.eventsSince(began, 40) {
		t.Logf("slowpeer event %s %s: %s", e.at.Sub(began).Round(time.Millisecond), e.node, e.msg)
	}
	t.Logf("slowpeer verdict: largest gap to the healthy follower %s (pass at 250ms or less: %v); election starts by the healthy follower %d (pass at 0: %v), by the slow follower %d, by the first leader %d; new terms %d; longest two-leader run %s (pass under 1s: %v)",
		maxGap, maxGap <= 250*time.Millisecond, starts[healthy.name], starts[healthy.name] == 0, starts[slow.name], starts[lead.name],
		st.Term-startTerm, time.Duration(longestRun)*50*time.Millisecond, longestRun*50 < 1000)
}

// electionLog keeps the raft library's election starts, by node and time.
type electionLog struct {
	mu     sync.Mutex
	starts []electionStart
}

type electionStart struct {
	node string
	at   time.Time
	// kind is "start" for an election start; the others are the lines that
	// say how one went: "stepdown" (a leader's check-quorum), "prevote-ignored"
	// (inside the lease), "prevote-rejected", "prevote-cast", "vote-cast",
	// "leader".
	kind string
	msg  string
}

func (l *electionLog) startsSince(t time.Time) map[string]int {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := map[string]int{}
	for _, s := range l.starts {
		if s.kind == "start" && !s.at.Before(t) {
			out[s.node]++
		}
	}
	return out
}

// countsSince is the lines of each kind by node since t.
func (l *electionLog) countsSince(t time.Time) map[string]map[string]int {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := map[string]map[string]int{}
	for _, s := range l.starts {
		if s.at.Before(t) {
			continue
		}
		if out[s.kind] == nil {
			out[s.kind] = map[string]int{}
		}
		out[s.kind][s.node]++
	}
	return out
}

// eventsSince is every line but the lease-ignored pre-votes since t, in
// order, at most n.
func (l *electionLog) eventsSince(t time.Time, n int) []electionStart {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []electionStart
	for _, s := range l.starts {
		if !s.at.Before(t) && s.kind != "prevote-ignored" && len(out) < n {
			out = append(out, s)
		}
	}
	return out
}

// electionHandler is a slog handler that records one node's election lines
// from the raft library and keeps nothing else.
type electionHandler struct {
	node string
	log  *electionLog
}

func (h *electionHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *electionHandler) Handle(_ context.Context, r slog.Record) error {
	kind := ""
	switch m := r.Message; {
	case strings.Contains(m, "is starting a new election"):
		kind = "start"
	case strings.Contains(m, "quorum is not active"):
		kind = "stepdown"
	case strings.Contains(m, "MsgPreVote") && strings.Contains(m, "lease is not expired"):
		kind = "prevote-ignored"
	case strings.Contains(m, "rejected MsgPreVote"):
		kind = "prevote-rejected"
	case strings.Contains(m, "cast MsgPreVote"):
		kind = "prevote-cast"
	case strings.Contains(m, "cast MsgVote"):
		kind = "vote-cast"
	case strings.Contains(m, "became leader at term"):
		kind = "leader"
	}
	if kind != "" {
		h.log.mu.Lock()
		h.log.starts = append(h.log.starts, electionStart{node: h.node, at: r.Time, kind: kind, msg: r.Message})
		h.log.mu.Unlock()
	}
	return nil
}
func (h *electionHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *electionHandler) WithGroup(string) slog.Handler      { return h }
