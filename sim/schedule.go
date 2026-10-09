package sim

import (
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/transport"
)

// Step is one scheduled fault.
type Step struct {
	At    time.Duration // offset from the workload start
	Kind  string        // partition, drop, delay, reorder, crash, drain, skew
	Node  string
	Peer  string        // for drop, delay, reorder
	Dur   time.Duration // how long the fault lasts (for crash: until restart)
	Drop  float64
	Delay time.Duration
	Skew  time.Duration
	Disk  string // for crash: "", truncate, corrupt
	// UntilSnapshot, for crash, keeps the node down past Dur until every
	// running node has compacted its log past what the node held, so its
	// return needs a snapshot however slowly the run is going.
	UntilSnapshot bool
}

func (e Step) String() string {
	switch e.Kind {
	case "partition":
		return fmt.Sprintf("%6.2fs partition %s for %s", e.At.Seconds(), e.Node, e.Dur)
	case "drop":
		return fmt.Sprintf("%6.2fs drop %.0f%% of frames %s→%s for %s", e.At.Seconds(), e.Drop*100, e.Node, e.Peer, e.Dur)
	case "delay":
		return fmt.Sprintf("%6.2fs delay frames %s→%s by %s for %s", e.At.Seconds(), e.Node, e.Peer, e.Delay, e.Dur)
	case "reorder":
		return fmt.Sprintf("%6.2fs reorder frames %s→%s for %s", e.At.Seconds(), e.Node, e.Peer, e.Dur)
	case "crash":
		if e.UntilSnapshot {
			return fmt.Sprintf("%6.2fs crash %s, restart after %s and once the survivors have compacted past it (disk: %s)", e.At.Seconds(), e.Node, e.Dur, orNone(e.Disk))
		}
		return fmt.Sprintf("%6.2fs crash %s, restart after %s (disk: %s)", e.At.Seconds(), e.Node, e.Dur, orNone(e.Disk))
	case "drain":
		return fmt.Sprintf("%6.2fs drain %s, restart after %s", e.At.Seconds(), e.Node, e.Dur)
	case "skew":
		return fmt.Sprintf("%6.2fs skew %s clock by %s for %s", e.At.Seconds(), e.Node, e.Skew, e.Dur)
	}
	return e.Kind
}

// scheduleSummary names what a schedule does, in the order the faults start,
// so a seed's description is read from the generator rather than written
// beside it. Two work items took their titles from a label that no longer
// matched its seed (WIs 1298, 1534).
func scheduleSummary(steps []Step) string {
	if len(steps) == 0 {
		return "no faults"
	}
	var order []string
	count := map[string]int{}
	for _, e := range steps {
		k := stepLabel(e)
		if count[k] == 0 {
			order = append(order, k)
		}
		count[k]++
	}
	parts := make([]string, 0, len(order))
	for _, k := range order {
		parts = append(parts, quantify(count[k], k))
	}
	switch len(parts) {
	case 1:
		return parts[0]
	default:
		return strings.Join(parts[:len(parts)-1], ", ") + " and " + parts[len(parts)-1]
	}
}

// stepLabel is one fault in the words the project uses for it. A crash's
// disk treatment is part of the label: a torn tail and a corrupted record
// are what two of the corpus seeds exist for.
func stepLabel(e Step) string {
	switch e.Kind {
	case "drop":
		return "a drop"
	case "delay":
		return "a delay"
	case "reorder":
		return "a reorder"
	case "skew":
		return "a clock skew"
	case "crash":
		switch e.Disk {
		case "truncate":
			return "a crash with a torn tail"
		case "corrupt":
			return "a crash with a corrupted record"
		}
		return "a crash"
	case "partition":
		return "a partition"
	case "drain":
		return "a drain"
	}
	return e.Kind
}

// quantify repeats a label n times in words. The generator's event count is
// small; anything above the named numbers is printed as a figure rather than
// mis-spelled.
func quantify(n int, label string) string {
	if n == 1 {
		return label
	}
	word := map[int]string{2: "two", 3: "three", 4: "four", 5: "five", 6: "six"}[n]
	if word == "" {
		word = strconv.Itoa(n)
	}
	return word + " " + plural(label)
}

// plural drops the article and pluralizes the noun.
func plural(label string) string {
	s := strings.TrimPrefix(label, "a ")
	switch {
	case strings.HasPrefix(s, "crash"):
		return strings.Replace(s, "crash", "crashes", 1)
	case strings.HasSuffix(s, "s"):
		return s
	default:
		return s + "s"
	}
}

func orNone(s string) string {
	if s == "" {
		return "none"
	}
	return s
}

// Generate derives a schedule from the seed: count events inside the
// window, at most one node down at a time (crash or drain), every fault
// over before the window ends.
func Generate(rng *rand.Rand, nodes []string, window time.Duration, count int, ttl time.Duration) []Step {
	kinds := []string{"partition", "drop", "delay", "reorder", "crash", "drain", "skew", "crash"}
	var events []Step
	var downUntil time.Duration // the moment the last crash/drain node is back
	for len(events) < count {
		kind := kinds[rng.Intn(len(kinds))]
		at := time.Duration(rng.Int63n(int64(window * 6 / 10)))
		node := nodes[rng.Intn(len(nodes))]
		peer := nodes[rng.Intn(len(nodes))]
		for peer == node {
			peer = nodes[rng.Intn(len(nodes))]
		}
		e := Step{At: at, Kind: kind, Node: node, Peer: peer}
		switch kind {
		case "partition":
			// Long enough for the minority to fence (ttl) and be probed.
			e.Dur = ttl + time.Duration(rng.Int63n(int64(ttl)))
		case "drop":
			e.Drop = 0.1 + rng.Float64()*0.4
			e.Dur = time.Second + time.Duration(rng.Int63n(int64(2*time.Second)))
		case "delay":
			e.Delay = 50*time.Millisecond + time.Duration(rng.Int63n(int64(300*time.Millisecond)))
			e.Dur = time.Second + time.Duration(rng.Int63n(int64(2*time.Second)))
		case "reorder":
			e.Dur = time.Second + time.Duration(rng.Int63n(int64(2*time.Second)))
		case "crash", "drain":
			if at < downUntil {
				at = downUntil + 200*time.Millisecond
				e.At = at
			}
			e.Dur = 500*time.Millisecond + time.Duration(rng.Int63n(int64(2*time.Second)))
			if kind == "crash" {
				switch rng.Intn(4) {
				case 0:
					e.Disk = "truncate"
				case 1:
					e.Disk = "corrupt"
				}
			}
			downUntil = e.At + e.Dur
		case "skew":
			e.Skew = time.Duration(rng.Int63n(int64(2*ttl))) - ttl
			e.Dur = time.Second + time.Duration(rng.Int63n(int64(2*time.Second)))
		}
		if e.At+e.Dur > window {
			continue
		}
		events = append(events, e)
	}
	sort.Slice(events, func(i, j int) bool { return events[i].At < events[j].At })
	return events
}

// Execute runs a schedule against the cluster from now, collecting the
// minority probes and fail-closed observations, and returns once every
// fault has ended and every node is back.
func Execute(c *Cluster, events []Step, ttl time.Duration) ([]Probe, []FailClosed) {
	start := time.Now()
	var mu sync.Mutex
	var probes []Probe
	var closed []FailClosed
	var wg sync.WaitGroup
	for _, e := range events {
		wg.Add(1)
		go func(e Step) {
			defer wg.Done()
			time.Sleep(time.Until(start.Add(e.At)))
			node := c.Node(e.Node)
			peer := c.Node(e.Peer)
			switch e.Kind {
			case "partition":
				for _, o := range c.Nodes {
					if o != node {
						c.Partition(node, o)
					}
				}
				// Probe the minority once its lease lapsed.
				probeAt := ttl + 500*time.Millisecond
				if probeAt < e.Dur {
					time.Sleep(probeAt)
					code, ref, err := ProbeConnect(node.TCP, fmt.Sprintf("probe-%d", time.Now().UnixNano()))
					mu.Lock()
					probes = append(probes, Probe{At: time.Now(), Node: node.Name, Code: code, Ref: ref, Err: err})
					mu.Unlock()
					time.Sleep(e.Dur - probeAt)
				} else {
					time.Sleep(e.Dur)
				}
				for _, o := range c.Nodes {
					if o != node {
						c.Heal(node, o)
					}
				}
			case "drop", "delay", "reorder":
				f := &transport.Fault{Drop: e.Drop, Delay: e.Delay, Reorder: e.Kind == "reorder", Rand: rand.New(rand.NewSource(int64(e.At)))} //nolint:gosec // seeded simulation
				c.Fault(node, peer, f)
				time.Sleep(e.Dur)
				c.Fault(node, peer, nil)
			case "crash":
				held, known := commitIndex(node)
				c.Crash(node)
				time.Sleep(e.Dur)
				if e.UntilSnapshot && known {
					waitCompactedPast(c, held+c.snapshotSlack(), snapshotWaitLimit)
				}
				switch e.Disk {
				case "truncate":
					_ = c.TruncateTail(node, StateLog, 20)
				case "corrupt":
					if err := c.CorruptRecord(node, StateLog); err == nil {
						err := c.TryStart(node)
						mu.Lock()
						closed = append(closed, FailClosed{Node: node.Name, Log: StateLog, Refused: err != nil, Err: err})
						mu.Unlock()
						if err != nil {
							// Repair: the corrupted tail becomes a torn write.
							_ = c.TruncateTail(node, StateLog, 9)
						}
					}
				}
				if !node.Running() {
					c.Start(node)
				}
			case "drain":
				c.Drain(node)
				time.Sleep(e.Dur)
				c.Start(node)
			case "skew":
				c.Skew(node, e.Skew)
				time.Sleep(e.Dur)
				c.Skew(node, 0)
			}
		}(e)
	}
	wg.Wait()
	return probes, closed
}

// snapshotWaitLimit bounds a crash step's wait for the survivors' compaction;
// past it the node restarts anyway and the scenario's own check says what
// happened.
const snapshotWaitLimit = 60 * time.Second

func commitIndex(n *Node) (uint64, bool) {
	srv := n.Server()
	if srv == nil || srv.Consensus() == nil {
		return 0, false
	}
	return srv.Consensus().Status().Commit, true
}

// snapshotSlack is how far a snapshot index must pass a node's commit index
// before that node cannot be caught up from the log: the trailing window a
// compaction leaves behind the snapshot, plus one snapshot interval for the
// entries the node may hold beyond its commit.
func (c *Cluster) snapshotSlack() uint64 {
	if c.o.SnapshotEntries <= 0 {
		return 11000 // the product's defaults: 10,000 entries, 1,000 trailing
	}
	return uint64(c.o.SnapshotEntries + snapshotTrailing(c.o.SnapshotEntries))
}

// snapshotTrailing is the trailing window the harness configures for a
// lowered snapshot threshold.
func snapshotTrailing(entries int) int { return max(entries/4, 1) }

// waitCompactedPast returns once every running node's snapshot index exceeds
// index, or the limit passes. Every node, because any of them may lead when
// the crashed node returns, and a leader that still holds the entries sends
// them rather than a snapshot.
func waitCompactedPast(c *Cluster, index uint64, limit time.Duration) bool {
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		past := true
		for _, n := range c.Running() {
			if srv := n.Server(); srv == nil || srv.Consensus() == nil || srv.Consensus().Status().SnapshotIndex <= index {
				past = false
			}
		}
		if past {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}
