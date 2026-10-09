package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/happydave/hoardmq/internal/bans"
	"github.com/happydave/hoardmq/internal/cluster/records"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/conn"
	"github.com/happydave/hoardmq/internal/ratelimit"
)

// effective is the limits in force: the configuration with the administered
// record laid over it.
func effective(cfg config.Limits, r config.LimitsOverride) config.Limits { return cfg.With(r) }

// clientLimitsOf is what a connection's buckets are built from.
func clientLimitsOf(e config.Limits) *conn.ClientLimits {
	return &conn.ClientLimits{
		MessageRate: float64(e.ClientMessageRate), MessageBurst: float64(config.Burst(e.ClientMessageRate, e.ClientMessageBurst)),
		ByteRate: float64(e.ClientByteRate), ByteBurst: float64(config.Burst(e.ClientByteRate, e.ClientByteBurst)),
	}
}

// admission is the connection's gate: refused for overload while the cluster
// is at level 3, otherwise by the connect-rate limits in force.
type admission struct {
	rate     atomic.Pointer[connectGate]
	overload *overload
	refused  atomic.Int64 // refused for overload
}

// AdmitConnect implements conn.ConnectGate.
func (a *admission) AdmitConnect(listener string) (bool, string) {
	if a.overload.Level() >= 3 {
		a.refused.Add(1)
		return false, conn.LimitOverload
	}
	if g := a.rate.Load(); g != nil {
		return g.AdmitConnect(listener)
	}
	return true, ""
}

// policy keeps the node's view of the replicated ban list and limits current,
// sweeps lapsed bans on the consensus leader, and turns flapping into bans.
type policy struct {
	s       *Server
	index   atomic.Pointer[bans.Index]
	limits  atomic.Pointer[conn.ClientLimits]
	current atomic.Pointer[config.Limits]
	// mu orders the limits' rebuilds, from the administered record and from
	// a reload; override is the record last applied.
	mu       sync.Mutex
	override config.LimitsOverride
	sweep    time.Duration
	flap     *flapping
	stop     chan struct{}
	done     sync.WaitGroup
	banned   atomic.Int64
	expired  atomic.Int64
	rebuilds atomic.Int64 // ban index builds
	reads    atomic.Int64 // refreshes, a build or not
	listSeen banPrint     // the watcher's own
}

func newPolicy(s *Server, hooks Hooks) *policy {
	p := &policy{s: s, sweep: 10 * time.Second, stop: make(chan struct{})}
	if hooks.BanSweepInterval > 0 {
		p.sweep = hooks.BanSweepInterval
	}
	p.flap = newFlapping(s.config().Flapping, p)
	p.apply(config.LimitsOverride{})
	return p
}

// apply puts the administered override into force over the configured
// limits: the connections' and the node gate's.
func (p *policy) apply(r config.LimitsOverride) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.override = r
	p.rebuild(false)
}

// reconfigure puts a reloaded configuration's limits into force under the
// override last applied. The gate is rebuilt even when the node's limits are
// unchanged, since a listener's own rate may have changed.
func (p *policy) reconfigure() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rebuild(true)
}

func (p *policy) rebuild(force bool) {
	e := effective(p.s.config().Limits, p.override)
	if old := p.current.Load(); !force && old != nil && *old == e {
		return
	}
	p.current.Store(&e)
	p.limits.Store(clientLimitsOf(e))
	cfg := *p.s.config()
	cfg.Limits.ConnectRate, cfg.Limits.ConnectBurst = e.ConnectRate, e.ConnectBurst
	g := newConnectGate(cfg)
	if old := p.s.admit.rate.Load(); old != nil && g != nil {
		g.carry(old)
	}
	p.s.admit.rate.Store(g)
}

// ClientLimits implements conn.Options.ClientLimits: the default namespace's
// limits for every namespace until namespaces are active (WI 1322).
func (p *policy) ClientLimits(string) *conn.ClientLimits { return p.limits.Load() }

// Banned implements conn.Options.Banned.
func (p *policy) Banned(clientID, username string, hasUsername bool, remote string) (string, bool) {
	m, ok := p.index.Load().Banned(clientID, username, hasUsername, bans.PeerAddr(remote), time.Now())
	if ok {
		p.banned.Add(1)
	}
	return m.Kind, ok
}

// start runs the watcher and the sweeper once the records store is open.
func (p *policy) start(st *records.Store) {
	p.done.Add(2)
	go p.watch(st)
	go p.sweepLoop(st)
}

func (p *policy) close() {
	close(p.stop)
	p.done.Wait()
}

// listAll reads a whole collection in pages.
func listAll(st *records.Store, collection string) []records.Entry {
	var out []records.Entry
	after := ""
	for {
		items, more := st.List(collection, after, 1000)
		out = append(out, items...)
		if !more || len(items) == 0 {
			return out
		}
		after = items[len(items)-1].Key
	}
}

// banItems reads the ban list as items; a record that does not decode is
// kept with an empty entry, which Build then refuses and counts.
func banItems(st *records.Store) []bans.Item {
	var items []bans.Item
	for _, e := range listAll(st, bans.Collection) {
		var b bans.Entry
		_ = json.Unmarshal(e.Value, &b)
		items = append(items, bans.Item{Key: e.Key, Entry: b, Version: e.Version})
	}
	return items
}

// banPrint identifies a ban list's contents: its size and the sum and
// highest of its versions, which are log indexes, so any write changes it.
type banPrint struct{ n, sum, top uint64 }

func printOf(items []bans.Item) banPrint {
	var f banPrint
	for _, it := range items {
		f.n++
		f.sum += it.Version
		f.top = max(f.top, it.Version)
	}
	return f
}

// refresh rebuilds the ban index when the ban list changed (a write to
// another collection compiles nothing) and reads the limits from the store.
func (p *policy) refresh(st *records.Store) {
	items := banItems(st)
	if f := printOf(items); p.index.Load() == nil || f != p.listSeen {
		p.index.Store(bans.Build(items))
		p.listSeen = f
		p.rebuilds.Add(1)
	}
	var r config.LimitsOverride
	if rec, ok := st.Get(config.LimitsCollection, config.DefaultLimitsKey); ok {
		_ = json.Unmarshal(rec.Value, &r)
	}
	p.apply(r)
	p.reads.Add(1)
}

// watch refreshes on every apply of the records component.
func (p *policy) watch(st *records.Store) {
	defer p.done.Done()
	ctx, cancel := context.WithCancel(context.Background())
	go func() { <-p.stop; cancel() }()
	for {
		applied := st.Applied()
		p.refresh(st)
		if st.WaitApplied(ctx, applied+1) != nil {
			return
		}
	}
}

// sweepLoop deletes lapsed bans on the node that leads the consensus core,
// each only at the version it was read at.
func (p *policy) sweepLoop(st *records.Store) {
	defer p.done.Done()
	t := time.NewTicker(p.sweep)
	defer t.Stop()
	for {
		select {
		case <-p.stop:
			return
		case <-t.C:
		}
		if p.s.consensus == nil || p.s.consensus.Status().Role != "leader" {
			continue
		}
		now := time.Now()
		for _, it := range bans.Lapsed(banItems(st), now) {
			v := it.Version
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			res, err := st.Delete(ctx, bans.Collection, it.Key, &v, "sweeper", now.UTC())
			cancel()
			outcome := "deleted"
			switch {
			case err != nil:
				outcome = "unavailable"
			case !res.OK:
				outcome = res.Reason
			default:
				p.expired.Add(1)
			}
			p.s.count("hoardmq_bans_expired_total", map[string]string{"outcome": outcome}, 1)
			p.s.record("sweeper", "ban.delete", it.Key, outcome, nil)
		}
	}
}

// flapping counts, per client identifier, the established connections that
// ended abnormally on the client's side, and bans a client that reaches the
// threshold within the window.
type flapping struct {
	cfg     atomic.Pointer[config.Flapping] // in force; a reload replaces it
	p       *policy
	mu      sync.Mutex
	seen    map[string][]time.Time
	banned  map[string]time.Time // proposed, until
	limiter ratelimit.Bucket
	trips   atomic.Int64
	written atomic.Int64
	// ends counts every established connection's end, by how it ended.
	ends [3]atomic.Int64
}

// maxTracked bounds the identifiers the detector remembers.
const maxTracked = 100000

func newFlapping(cfg config.Flapping, p *policy) *flapping {
	f := &flapping{p: p, seen: map[string][]time.Time{}, banned: map[string]time.Time{}, limiter: ratelimit.New(100, 100, time.Now())}
	f.cfg.Store(&cfg)
	return f
}

// tripped is the decision: whether count ends fall within the window ending
// at now.
func tripped(times []time.Time, now time.Time, window time.Duration, count int) bool {
	n := 0
	for _, t := range times {
		if now.Sub(t) < window {
			n++
		}
	}
	return n >= count
}

// ended is told how each established connection ended.
func (f *flapping) ended(c *conn.Conn, how conn.Ending) {
	if int(how) < len(f.ends) {
		f.ends[how].Add(1)
	}
	cfg := f.cfg.Load()
	if !cfg.Enabled || how != conn.EndedAbnormal || c.ClientID() == "" {
		return
	}
	id, now := c.ClientID(), time.Now()
	window := time.Duration(cfg.WindowSeconds) * time.Second
	f.mu.Lock()
	times := f.seen[id]
	kept := times[:0]
	for _, t := range times {
		if now.Sub(t) < window {
			kept = append(kept, t)
		}
	}
	kept = append(kept, now)
	if len(kept) > cfg.MaxCount {
		kept = kept[len(kept)-cfg.MaxCount:]
	}
	if _, known := f.seen[id]; !known && len(f.seen) >= maxTracked {
		f.evictOldest()
	}
	f.seen[id] = kept
	trip := tripped(kept, now, window, cfg.MaxCount)
	propose := false
	if trip {
		f.trips.Add(1)
		delete(f.seen, id)
		if until, ok := f.banned[id]; !ok || !now.Before(until) {
			var ok bool
			if f.limiter, ok = f.limiter.Take(now, 1); ok {
				f.banned[id] = now.Add(time.Duration(cfg.BanSeconds) * time.Second)
				propose = true
			} else {
				f.p.s.count("hoardmq_flapping_bans_total", map[string]string{"outcome": "limited"}, 1)
			}
		}
	}
	f.mu.Unlock()
	if trip {
		f.p.s.count("hoardmq_flapping_trips_total", nil, 1)
	}
	if propose {
		go f.propose(id, now)
	}
}

// evictOldest drops the identifier whose latest end is oldest. Called with
// mu held, only when the table is full.
func (f *flapping) evictOldest() {
	var oldest string
	var at time.Time
	for id, times := range f.seen {
		if last := times[len(times)-1]; oldest == "" || last.Before(at) {
			oldest, at = id, last
		}
	}
	delete(f.seen, oldest)
	for id, until := range f.banned {
		if time.Now().After(until) {
			delete(f.banned, id)
		}
	}
}

// propose writes the ban, only where none exists.
func (f *flapping) propose(id string, now time.Time) {
	st := f.p.s.records.Load()
	outcome := "written"
	cfg := f.cfg.Load()
	until := now.Add(time.Duration(cfg.BanSeconds) * time.Second).UTC()
	key := bans.Key(bans.ClientID, id)
	defer func() {
		f.p.s.count("hoardmq_flapping_bans_total", map[string]string{"outcome": outcome}, 1)
		f.p.s.record("flapping", "ban.create", key, outcome, map[string]string{"expiresAt": until.Format(time.RFC3339)})
	}()
	if st == nil {
		outcome = "unavailable"
		return
	}
	e := bans.Entry{Reason: fmt.Sprintf("flapping: %d abnormal ends within %d s", cfg.MaxCount, cfg.WindowSeconds), ExpiresAt: &until, Source: bans.SourceFlapping}
	if bans.CheckBan(key, e) != nil {
		outcome = "invalid"
		return
	}
	v, _ := json.Marshal(e)
	absent := uint64(0)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := st.Put(ctx, bans.Collection, key, v, &absent, "flapping", now.UTC())
	switch {
	case err != nil:
		outcome = "unavailable"
	case !res.OK:
		outcome = res.Reason
	default:
		f.written.Add(1)
	}
}
