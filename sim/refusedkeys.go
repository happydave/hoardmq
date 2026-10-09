package sim

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/happydave/hoardmq/internal/store"
)

// KeyBlock is one key a node's peer refused a record for, watched from the
// healed instant: when it was first seen blocked, whether it was cleared
// within the run and after how long, and whether a whole-stream resync of
// that peer — the only thing that clears a refusal — happened meanwhile. A
// restarted node starts with an empty set, so a key that vanished with a
// restart is cleared "by restart"; one the owner wrote whole again (an
// import), "by whole-write".
type KeyBlock struct {
	Node      string `json:"node"`
	Peer      string `json:"peer"`
	Namespace string `json:"ns"`
	ClientID  string `json:"client_id,omitempty"`
	Topic     string `json:"topic,omitempty"`
	Epoch     uint64 `json:"epoch"` // the highest epoch the peer refused a record at
	Reason    string `json:"reason"`
	// SinceHealMs is when the key was first seen blocked, after the heal; 0
	// when it was already blocked at the first sample.
	SinceHealMs float64 `json:"since_heal_ms"`
	// ClearedAfterMs is how long after it was first seen the key was gone;
	// -1 when it was still blocked at the last sample.
	ClearedAfterMs float64 `json:"cleared_after_ms"`
	ClearedBy      string  `json:"cleared_by,omitempty"` // resync, whole-write, restart, or "" for never
}

func (k KeyBlock) Cleared() bool { return k.ClearedAfterMs >= 0 }

// refusedWatch samples every running node's refused keys from the healed
// instant, every refusedSample, until stopped.
type refusedWatch struct {
	c       *Cluster
	healed  time.Time
	mu      sync.Mutex
	seen    map[keyBlockID]*keyBlockState
	resyncs map[string]map[string]int // node → peer → resyncs counted at the last sample
	stop    chan struct{}
	done    chan struct{}
}

type keyBlockID struct {
	node, peer string
	key        store.Scope
}

type keyBlockState struct {
	reason      string
	epoch       uint64 // the highest epoch the peer refused a record at
	first, last time.Time
	cleared     string
	resyncAt    map[string]int // peer → resyncs counted when first seen
	server      any            // the node's server instance when first seen
}

const refusedSample = 100 * time.Millisecond

func watchRefusedKeys(c *Cluster, healed time.Time) *refusedWatch {
	w := &refusedWatch{c: c, healed: healed, seen: map[keyBlockID]*keyBlockState{}, resyncs: map[string]map[string]int{}, stop: make(chan struct{}), done: make(chan struct{})}
	go w.run()
	return w
}

func (w *refusedWatch) run() {
	defer close(w.done)
	for {
		w.sample()
		select {
		case <-w.stop:
			w.sample()
			return
		case <-time.After(refusedSample):
		}
	}
}

// sample reads each running node once. A key present is extended; a key
// absent that was present is cleared — by the node having restarted if its
// server is a new instance, by a resync of its peer if one was counted since
// the key was first seen, else by the owner writing the session whole.
func (w *refusedWatch) sample() {
	now := time.Now()
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, n := range w.c.Running() {
		srv := n.Server()
		if srv == nil || srv.Replication() == nil {
			continue
		}
		resyncs := resyncCounts(n)
		w.resyncs[n.Name] = resyncs
		present := map[keyBlockID]bool{}
		for peer, keys := range srv.Replication().RefusedKeys() {
			for k, reason := range keys {
				id := keyBlockID{n.Name, peer, k.Bare()}
				present[id] = true
				if st := w.seen[id]; st != nil {
					if st.cleared == "" {
						st.last = now
						st.epoch = max(st.epoch, k.Epoch)
					}
					continue
				}
				w.seen[id] = &keyBlockState{reason: reason, epoch: k.Epoch, first: now, last: now, resyncAt: map[string]int{peer: resyncs[peer]}, server: srv}
			}
		}
		for id, st := range w.seen {
			if id.node != n.Name || st.cleared != "" || present[id] {
				continue
			}
			switch {
			case st.server != srv:
				st.cleared = "restart"
			case resyncs[id.peer] > st.resyncAt[id.peer]:
				st.cleared = "resync"
			default:
				st.cleared = "whole-write"
			}
			st.last = now
		}
	}
}

func resyncCounts(n *Node) map[string]int {
	out := map[string]int{}
	metrics, ok := snapshotMetrics(n)
	if !ok {
		return out
	}
	for _, m := range metrics {
		if m.Name == "hoardmq_replication_resyncs_total" {
			out[m.Labels["peer"]] += int(m.Value)
		}
	}
	return out
}

// finish stops the watch and returns the keys in a stable order.
func (w *refusedWatch) finish() []KeyBlock {
	if w == nil {
		return nil
	}
	close(w.stop)
	<-w.done
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]KeyBlock, 0, len(w.seen))
	for id, st := range w.seen {
		kb := KeyBlock{Node: id.node, Peer: id.peer, Namespace: id.key.Namespace, ClientID: id.key.ClientID, Topic: id.key.Topic, Epoch: st.epoch, Reason: st.reason,
			SinceHealMs: float64(st.first.Sub(w.healed)) / 1e6, ClearedAfterMs: -1}
		if st.cleared != "" {
			kb.ClearedAfterMs = float64(st.last.Sub(st.first)) / 1e6
			kb.ClearedBy = st.cleared
		}
		out = append(out, kb)
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.Node != b.Node {
			return a.Node < b.Node
		}
		if a.Peer != b.Peer {
			return a.Peer < b.Peer
		}
		if a.ClientID != b.ClientID {
			return a.ClientID < b.ClientID
		}
		return a.Topic < b.Topic
	})
	return out
}

// refusedKeyStats are the integer summaries for the run's stats map: keys
// seen blocked after the heal, and those still blocked at the end.
func refusedKeyStats(keys []KeyBlock) map[string]int {
	st := map[string]int{}
	if keys == nil {
		return st
	}
	st["refusedKeys"] = len(keys)
	never := 0
	for _, k := range keys {
		if !k.Cleared() {
			never++
		}
	}
	st["refusedKeysNeverCleared"] = never
	return st
}

// describeRefusedKeys is one line per node that had any.
func describeRefusedKeys(keys []KeyBlock) string {
	if len(keys) == 0 {
		return "refused keys after the heal: none"
	}
	type agg struct {
		byPeer, byReason map[string]int
		cleared          []float64
		by               map[string]int
		never            int
	}
	nodes := map[string]*agg{}
	var order []string
	for _, k := range keys {
		a := nodes[k.Node]
		if a == nil {
			a = &agg{byPeer: map[string]int{}, byReason: map[string]int{}, by: map[string]int{}}
			nodes[k.Node] = a
			order = append(order, k.Node)
		}
		a.byPeer[k.Peer]++
		a.byReason[k.Reason]++
		if k.Cleared() {
			a.cleared = append(a.cleared, k.ClearedAfterMs)
			a.by[k.ClearedBy]++
		} else {
			a.never++
		}
	}
	var parts []string
	for _, n := range order {
		a := nodes[n]
		sort.Float64s(a.cleared)
		s := fmt.Sprintf("%s: %d keys refused by %s (%s)", n, len(a.cleared)+a.never, describeCounts(a.byPeer), describeCounts(a.byReason))
		if len(a.cleared) > 0 {
			s += fmt.Sprintf(", %d cleared after %.1f–%.1f s (%s)", len(a.cleared), a.cleared[0]/1000, a.cleared[len(a.cleared)-1]/1000, describeCounts(a.by))
		}
		if a.never > 0 {
			s += fmt.Sprintf(", %d never within the run", a.never)
		}
		parts = append(parts, s)
	}
	return "refused keys after the heal: " + strings.Join(parts, "; ")
}

func describeCounts(m map[string]int) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s %d", k, m[k]))
	}
	return strings.Join(parts, ", ")
}
