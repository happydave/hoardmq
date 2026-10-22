package etcdraft

import (
	"encoding/binary"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
	"github.com/happydave/hoardmq/internal/cluster/lease"
	"github.com/happydave/hoardmq/internal/cluster/records"
	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

// The snapshot measurement: a three-node cluster holding N leases (with
// Wills of HOARDMQ_SNAPSHOT_WILL bytes), its snapshot built, then a wiped
// follower restarted and filled from it. It reports the build's time, the
// process heap's peaks, the time to rejoin, and the round trip of a probe
// on the data connection from the leader to that follower before and while
// it is filled. It uses only what the build before streamed snapshots also
// has, so the same file measures both.

type sumMetrics struct {
	mu sync.Mutex
	v  map[string]float64
}

func (m *sumMetrics) Add(name string, _ map[string]string, d float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.v == nil {
		m.v = map[string]float64{}
	}
	m.v[name] += d
}

func (m *sumMetrics) Set(name string, _ map[string]string, v float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.v == nil {
		m.v = map[string]float64{}
	}
	m.v["gauge:"+name] = v
}

func (m *sumMetrics) get(name string) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.v[name]
}

// heapPeaks samples the heap in use, keeping the peak since the last reset.
type heapPeaks struct {
	peak atomic.Uint64
	stop chan struct{}
	done chan struct{}
}

func startHeapPeaks() *heapPeaks {
	h := &heapPeaks{stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(h.done)
		var ms runtime.MemStats
		t := time.NewTicker(50 * time.Millisecond)
		defer t.Stop()
		for {
			runtime.ReadMemStats(&ms)
			for {
				cur := h.peak.Load()
				if ms.HeapInuse <= cur || h.peak.CompareAndSwap(cur, ms.HeapInuse) {
					break
				}
			}
			select {
			case <-h.stop:
				return
			case <-t.C:
			}
		}
	}()
	return h
}

func (h *heapPeaks) reset() uint64 { return h.peak.Swap(0) }
func (h *heapPeaks) close()        { close(h.stop); <-h.done }

func heapNow() uint64 {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapInuse
}

// rtts collects probe round trips by phase.
type rtts struct {
	mu    sync.Mutex
	phase string
	by    map[string][]time.Duration
}

func (r *rtts) add(d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.by == nil {
		r.by = map[string][]time.Duration{}
	}
	r.by[r.phase] = append(r.by[r.phase], d)
}

func (r *rtts) set(phase string) { r.mu.Lock(); r.phase = phase; r.mu.Unlock() }

func (r *rtts) summary(phase string) string {
	r.mu.Lock()
	ds := append([]time.Duration(nil), r.by[phase]...)
	r.mu.Unlock()
	if len(ds) == 0 {
		return "n=0"
	}
	sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
	q := func(p float64) time.Duration { return ds[min(len(ds)-1, int(p*float64(len(ds))))] }
	return fmt.Sprintf("n=%d p50=%v p99=%v max=%v", len(ds), q(0.5), q(0.99), ds[len(ds)-1])
}

func TestSnapshotMeasure(t *testing.T) {
	n, _ := strconv.Atoi(os.Getenv("HOARDMQ_SNAPSHOT_MEASURE"))
	if n == 0 {
		t.Skip("set HOARDMQ_SNAPSHOT_MEASURE to the lease count")
	}
	willBytes, _ := strconv.Atoi(os.Getenv("HOARDMQ_SNAPSHOT_WILL"))
	names := []string{"n1", "n2", "n3"}
	metrics := map[string]*sumMetrics{}
	tables := map[string]*lease.Table{}
	var mu sync.Mutex
	batches := (n + 199) / 200
	c := newTCluster(t, func(o *Options) {
		m := consensus.NewMachine()
		tab := lease.NewTable()
		m.Register(lease.Kind, tab)
		m.Register(records.Kind, records.New(0))
		o.Machine = m
		o.SnapshotEntries = uint64(batches + 20)
		o.SnapshotTrailing = 10
		mu.Lock()
		if metrics[o.Node] == nil {
			metrics[o.Node] = &sumMetrics{}
		}
		o.Metrics = metrics[o.Node]
		tables[o.Node] = tab
		mu.Unlock()
	})
	for _, name := range names {
		c.addTransport(name, others(name, names))
	}
	for _, name := range names {
		c.startCore(c.nodes[name], names, nil)
	}
	c.settled(3)
	heap := startHeapPeaks()
	defer heap.close()
	base := heapNow()

	now := time.Date(2026, 10, 22, 1, 0, 0, 0, time.UTC)
	began := time.Now()
	for start := 0; start < n; start += 200 {
		ops := make([]lease.Op, 0, 200)
		for i := start; i < min(start+200, n); i++ {
			op := lease.Op{Kind: lease.OpAcquire, ClientID: fmt.Sprintf("%036d", i), Node: names[i%3], ExpiryInterval: 3600, Now: now}
			if willBytes > 0 {
				op.Will = &store.WillRecord{Delay: 5, Message: &message.Message{Topic: "devices/" + op.ClientID + "/status", Payload: make([]byte, willBytes),
					Publisher: message.Publisher{ClientID: op.ClientID, Node: op.Node}, Seq: uint64(i + 1), Arrived: now}}
			}
			ops = append(ops, op)
		}
		proposeOnLeader(t, c, lease.Kind, lease.Encode(ops))
	}
	populated := time.Since(began)
	for i := 0; i < 25; i++ {
		proposeOnLeader(t, c, consensus.KindConfig, consensus.ConfigCommand(fmt.Sprintf("pad%d", i), "v")[1:])
	}
	lead := c.leader()
	waitFor(t, "every node's snapshot", 20*time.Minute, func() bool {
		for _, name := range names {
			if c.nodes[name].core.Status().SnapshotIndex == 0 {
				return false
			}
		}
		return true
	})
	buildPeak := heap.reset()
	held := heapNow()
	lm := metrics[lead.name]
	docStart := time.Now()
	doc, err := lead.core.machine.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	docTook := time.Since(docStart)
	t.Logf("MEASURE n=%d will=%d populate=%v heapBase=%d heapHeld=%d buildPeak=%d formatOneDoc=%dB/%v capture=%vs buildSeconds=%v snapshotBytes=%v",
		n, willBytes, populated.Round(time.Millisecond), base, held, buildPeak, len(doc), docTook.Round(time.Millisecond),
		lm.get("gauge:hoardmq_raft_snapshot_capture_seconds"), lm.get("hoardmq_raft_snapshot_build_seconds_total"), lm.get("gauge:hoardmq_raft_snapshot_bytes"))

	var victim *tnode
	for _, name := range names {
		if nd := c.nodes[name]; nd != lead && victim == nil {
			victim = nd
		}
	}
	// The probe: a frame on the data connection every 10 ms, echoed.
	probes := &rtts{}
	victim.tr.Handle(transport.ReplicationAppend, func(peer string, p []byte) {
		_ = victim.tr.Send(t.Context(), peer, transport.ReplicationAck, p)
	})
	lead.tr.Handle(transport.ReplicationAck, func(_ string, p []byte) {
		if len(p) == 8 {
			probes.add(time.Since(time.Unix(0, int64(binary.BigEndian.Uint64(p)))))
		}
	})
	stopProbe := make(chan struct{})
	probeDone := make(chan struct{})
	go func() {
		defer close(probeDone)
		tk := time.NewTicker(10 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-stopProbe:
				return
			case <-tk.C:
				_ = lead.tr.Send(t.Context(), victim.name, transport.ReplicationAppend, binary.BigEndian.AppendUint64(nil, uint64(time.Now().UnixNano())))
			}
		}
	}()
	defer func() { close(stopProbe); <-probeDone }()
	probes.set("before")
	time.Sleep(2 * time.Second)

	victim.core.Stop()
	if err := os.RemoveAll(victim.dir); err != nil {
		t.Fatal(err)
	}
	heap.reset()
	probes.set("transfer")
	restart := time.Now()
	c.startCore(victim, names, nil)
	snapAt := lead.core.Status().SnapshotIndex
	waitFor(t, "the wiped follower rejoins", 30*time.Minute, func() bool {
		mu.Lock()
		tb := tables[victim.name]
		mu.Unlock()
		role := ""
		for _, m := range c.leader().core.Members() {
			if m.Name == victim.name {
				role = string(m.Role)
			}
		}
		return tb != nil && tb.Count() == n && role == string(consensus.Voter) && victim.core.Status().Applied >= snapAt
	})
	rejoin := time.Since(restart)
	probes.set("after")
	transferPeak := heap.reset()
	vm := metrics[victim.name]
	t.Logf("MEASURE n=%d will=%d rejoin=%v transferPeak=%d outSeconds=%v inSeconds=%v outBytes=%v",
		n, willBytes, rejoin.Round(time.Millisecond), transferPeak, lm.get("hoardmq_raft_snapshot_transfer_seconds_total"),
		vm.get("hoardmq_raft_snapshot_transfer_seconds_total"), lm.get("hoardmq_raft_snapshot_transfer_bytes_total"))
	t.Logf("MEASURE probe before %s", probes.summary("before"))
	t.Logf("MEASURE probe transfer %s", probes.summary("transfer"))
}
