package broker

import (
	"math"
	"runtime/debug"
	"runtime/metrics"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/conn"
)

// Indicators are what the node's stress level is measured from.
type Indicators struct {
	Queued int64 `json:"queued_messages"`
	Lag    int64 `json:"replication_lag"`
	Heap   int64 `json:"heap_bytes"`
}

// overloadLimits are the limits the indicators are fractions of; 0 leaves an
// indicator out.
type overloadLimits struct{ Queued, Lag, Heap int64 }

// levelOf is the node's stress level: each indicator's fraction of its limit
// gives a level — under 0.6 is 0, under 0.8 is 1, under 0.95 is 2, else 3 —
// and the highest decides.
func levelOf(ind Indicators, lim overloadLimits) uint8 {
	var level uint8
	for _, p := range [][2]int64{{ind.Queued, lim.Queued}, {ind.Lag, lim.Lag}, {ind.Heap, lim.Heap}} {
		if p[1] <= 0 {
			continue
		}
		f := float64(p[0]) / float64(p[1])
		var l uint8
		switch {
		case f < 0.6:
			l = 0
		case f < 0.8:
			l = 1
		case f < 0.95:
			l = 2
		default:
			l = 3
		}
		level = max(level, l)
	}
	return level
}

// clusterLevel is the level a node enforces: the highest of its own and the
// levels its connected peers report.
func clusterLevel(own uint8, peers []transport.PeerInfo) uint8 {
	level := own
	for _, p := range peers {
		if p.State == transport.Up {
			level = max(level, p.Stress)
		}
	}
	return min(level, 3)
}

// The fraction of the connections that read a PUBLISH which each level
// pauses, and for how long before each read.
var shedLevels = [4]struct {
	fraction float64
	pause    time.Duration
}{{0, 0}, {0.10, 100 * time.Millisecond}, {0.50, 250 * time.Millisecond}, {1, 500 * time.Millisecond}}

// shedPlan is the decision on whom to pause, each for the level's pause: the
// level's fraction of the readers (at least one). A reader is a connection
// that read PUBLISH bytes since the last sample, or one already paused.
// Those already paused (held, in the order they were chosen) keep their place
// first: their rate is the one the pause imposed, not the one they offer, so
// ranking them afresh would release the noisiest client for being quiet
// under its own pause. The rest are filled from the other readers, noisiest
// first. It returns the indexes chosen, in the order chosen.
func shedPlan(rates []int64, level uint8, held []int) ([]int, time.Duration) {
	if level == 0 {
		return nil, 0
	}
	isHeld := make(map[int]bool, len(held))
	for _, i := range held {
		isHeld[i] = true
	}
	var others []int
	for i, r := range rates {
		if r > 0 && !isHeld[i] {
			others = append(others, i)
		}
	}
	sort.SliceStable(others, func(a, b int) bool { return rates[others[a]] > rates[others[b]] })
	s := shedLevels[min(level, 3)]
	n := int(math.Ceil(s.fraction * float64(len(held)+len(others))))
	chosen := append([]int(nil), held[:min(n, len(held))]...)
	for _, i := range others {
		if len(chosen) >= n {
			break
		}
		chosen = append(chosen, i)
	}
	return chosen, s.pause
}

// overloadStats is what the controller has seen, for the storm stats and the
// admin API.
type overloadStats struct {
	NodeLevel     uint8      `json:"node_level"`
	ClusterLevel  uint8      `json:"cluster_level"`
	Indicators    Indicators `json:"indicators"`
	MaxNodeLevel  uint8      `json:"max_node_level"`
	MaxLevelAt    time.Time  `json:"max_level_at,omitzero"`
	MaxCluster    uint8      `json:"max_cluster_level"`
	MaxClusterAt  time.Time  `json:"max_cluster_level_at,omitzero"`
	PeakIndicator Indicators `json:"peak_indicators"`
	Shed          int        `json:"shed_connections"`
	Samples       int64      `json:"samples"`
}

// overload is the node's stress controller: it samples the indicators,
// advertises the node's level, and while the cluster's is above 0 pauses the
// noisiest readers; at level 3 the connect gate refuses new connections.
type overload struct {
	s        *Server
	lim      atomic.Pointer[overloadLimits]
	interval time.Duration
	inject   func(node string) uint8

	level atomic.Uint32 // the cluster level enforced
	mu    sync.Mutex
	stats overloadStats
	last  map[*conn.Conn]int64
	shed  []*conn.Conn // paused, in the order chosen
	stop  chan struct{}
	done  chan struct{}
}

func newOverload(s *Server, cfg config.Overload, hooks Hooks) *overload {
	o := &overload{s: s, interval: time.Second, inject: hooks.Stress, last: map[*conn.Conn]int64{},
		stop: make(chan struct{}), done: make(chan struct{})}
	o.setLimits(cfg)
	if hooks.OverloadInterval > 0 {
		o.interval = hooks.OverloadInterval
	}
	return o
}

// setLimits puts the limits into force from the next sample. A memoryBytes
// of 0 is the process's memory limit, read now.
func (o *overload) setLimits(cfg config.Overload) {
	heap := cfg.MemoryBytes
	if heap == 0 {
		if l := debug.SetMemoryLimit(-1); l != math.MaxInt64 {
			heap = l
		}
	}
	o.lim.Store(&overloadLimits{Queued: int64(cfg.QueuedMessages), Lag: int64(cfg.ReplicationLag), Heap: heap})
}

// Level is the cluster level this node enforces.
func (o *overload) Level() uint8 {
	if o == nil {
		return 0
	}
	return uint8(o.level.Load())
}

func (o *overload) start() { go o.run() }

func (o *overload) close() {
	close(o.stop)
	<-o.done
}

func (o *overload) run() {
	defer close(o.done)
	t := time.NewTicker(o.interval)
	defer t.Stop()
	for {
		select {
		case <-o.stop:
			return
		case <-t.C:
			o.sample()
		}
	}
}

var heapSample = []metrics.Sample{{Name: "/gc/heap/live:bytes"}}

// indicators reads the three indicators: the queued count and the heap
// without a lock, the backlog under the replicator's locks, briefly.
func (o *overload) indicators() Indicators {
	var ind Indicators
	if o.s.sessions != nil {
		ind.Queued = o.s.sessions.Queued()
	}
	if o.s.repl != nil {
		ind.Lag = o.s.repl.Backlog()
	}
	if o.lim.Load().Heap > 0 {
		s := append([]metrics.Sample(nil), heapSample...)
		metrics.Read(s)
		if s[0].Value.Kind() == metrics.KindUint64 {
			ind.Heap = int64(s[0].Value.Uint64())
		}
	}
	return ind
}

// sample is one tick: the level, its advertisement, and the shedding.
func (o *overload) sample() {
	ind := o.indicators()
	node := levelOf(ind, *o.lim.Load())
	if o.inject != nil {
		node = o.inject(o.s.config().Node.Name)
	}
	var peers []transport.PeerInfo
	if o.s.cluster != nil {
		o.s.cluster.SetStress(node)
		peers = o.s.cluster.Peers()
	}
	level := clusterLevel(node, peers)
	o.level.Store(uint32(level))
	shed := o.shedConnections(level)
	o.mu.Lock()
	st := &o.stats
	st.NodeLevel, st.ClusterLevel, st.Indicators, st.Shed = node, level, ind, shed
	st.Samples++
	if node > st.MaxNodeLevel {
		st.MaxNodeLevel, st.MaxLevelAt = node, time.Now()
	}
	if level > st.MaxCluster {
		st.MaxCluster, st.MaxClusterAt = level, time.Now()
	}
	st.PeakIndicator.Queued = max(st.PeakIndicator.Queued, ind.Queued)
	st.PeakIndicator.Lag = max(st.PeakIndicator.Lag, ind.Lag)
	st.PeakIndicator.Heap = max(st.PeakIndicator.Heap, ind.Heap)
	o.mu.Unlock()
	o.s.count("hoardmq_overload_samples_total", nil, 1)
}

// shedConnections pauses the level's share of the noisiest readers and ends
// the pause of the rest. At level 0 it walks nothing unless connections are
// still paused from the last sample. It returns how many are paused.
func (o *overload) shedConnections(level uint8) int {
	if level == 0 && len(o.shed) == 0 {
		clear(o.last)
		return 0
	}
	o.s.mu.Lock()
	conns := make([]*conn.Conn, 0, len(o.s.conns))
	for c := range o.s.conns {
		conns = append(conns, c)
	}
	o.s.mu.Unlock()
	rates := make([]int64, len(conns))
	next := make(map[*conn.Conn]int64, len(conns))
	index := make(map[*conn.Conn]int, len(conns))
	for i, c := range conns {
		b := c.PublishedBytes()
		rates[i] = b - o.last[c]
		next[c] = b
		index[c] = i
	}
	o.last = next
	var held []int
	for _, c := range o.shed {
		if i, live := index[c]; live {
			held = append(held, i)
		}
	}
	chosen, pause := shedPlan(rates, level, held)
	paused := make(map[*conn.Conn]bool, len(chosen))
	shed := make([]*conn.Conn, 0, len(chosen))
	for _, i := range chosen {
		conns[i].Shed(pause)
		paused[conns[i]] = true
		shed = append(shed, conns[i])
	}
	for _, c := range o.shed {
		if !paused[c] {
			c.Shed(0)
		}
	}
	o.shed = shed
	return len(shed)
}

// Stats is what the controller has seen.
func (o *overload) Stats() overloadStats {
	if o == nil {
		return overloadStats{}
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.stats
}
