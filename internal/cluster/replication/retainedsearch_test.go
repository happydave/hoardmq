package replication

import (
	"fmt"
	"math/rand"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
)

// searchExempt are seeds known to lose a message, each with the work item that
// owns it; every other seed must pass.
var searchExempt = map[int64]string{}

// searchDefect sets one of the retained moves' defect seams on every node of a
// sequence, from HOARDMQ_RETAINED_SEARCH_DEFECT, for the negative controls.
func searchDefect(t *testing.T) func(*Options) {
	d := os.Getenv("HOARDMQ_RETAINED_SEARCH_DEFECT")
	return func(o *Options) {
		quickAck(o)
		switch d {
		case "":
		case "delete-own":
			o.DeleteOwnOnHandOver = true
		case "note-overwritten":
			o.HandOverNoteOverwritten = true
		case "answer-anywhere":
			o.AnswerHandOverAnywhere = true
		default:
			t.Fatalf("HOARDMQ_RETAINED_SEARCH_DEFECT: unknown %q", d)
		}
	}
}

// searchCount is how many sequences TestRetainedMovesKeepEveryAcknowledgedMessage
// runs, and from which seed: HOARDMQ_RETAINED_SEARCH and
// HOARDMQ_RETAINED_SEARCH_SEED. The default is a gate-sized run of seeds 1-20;
// the regression budget is seeds 1-3000 (HOARDMQ_RETAINED_SEARCH=3000).
// HOARDMQ_RETAINED_SEARCH_EXEMPT=run runs the exempt seeds too.
func searchCount(t *testing.T) (n int, base int64) {
	n, base = 20, 1
	if v := os.Getenv("HOARDMQ_RETAINED_SEARCH"); v != "" {
		var err error
		if n, err = strconv.Atoi(v); err != nil {
			t.Fatalf("HOARDMQ_RETAINED_SEARCH: %v", err)
		}
	}
	if v := os.Getenv("HOARDMQ_RETAINED_SEARCH_SEED"); v != "" {
		var err error
		if base, err = strconv.ParseInt(v, 10, 64); err != nil {
			t.Fatalf("HOARDMQ_RETAINED_SEARCH_SEED: %v", err)
		}
	}
	return n, base
}

// TestRetainedMovesKeepEveryAcknowledgedMessage is a seeded search over short
// sequences of shard-map changes on three or four nodes: acknowledged retained
// messages are written under the first map; each later map is a random basis
// over the live nodes, applied with random replication frames lost between two
// nodes for that step; at one random step one node is lost first. After each
// step settles, every acknowledged message must be held by a live node, in its
// own or its replica store. A failing sequence names its seed and steps.
func TestRetainedMovesKeepEveryAcknowledgedMessage(t *testing.T) {
	n, base := searchCount(t)
	for i := 0; i < n; i++ {
		seed := base + int64(i)
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			if wi, ok := searchExempt[seed]; ok && os.Getenv("HOARDMQ_RETAINED_SEARCH_EXEMPT") != "run" {
				t.Skipf("seed %d loses a message; %s owns it (HOARDMQ_RETAINED_SEARCH_EXEMPT=run runs it)", seed, wi)
			}
			retainedSequence(t, seed)
		})
	}
}

func retainedSequence(t *testing.T, seed int64) {
	rng := rand.New(rand.NewSource(seed)) //nolint:gosec // seeded search
	names := []string{"n1", "n2", "n3"}
	if rng.Intn(2) == 0 {
		names = append(names, "n4")
	}
	replicas := 2 + rng.Intn(2)
	const shards = 4
	h, _ := withCountersAnd(t, names, shards, replicas, searchDefect(t))
	allAlive(h)
	var steps []string
	live := slices.Clone(names)
	// A basis leaves out live nodes as a drain does, never below the copy
	// count while enough nodes are live (placementBasis).
	randomBasis := func(from []string) []string {
		for {
			var b []string
			for _, n := range from {
				if rng.Intn(3) > 0 {
					b = append(b, n)
				}
			}
			if len(b) >= min(replicas, len(from)) {
				return b
			}
		}
	}
	basis := randomBasis(live)
	version := uint64(1)
	cur := shardmap.Compute(version, basis, replicas, shards)
	apply := func(m shardmap.Map) {
		for _, n := range live {
			if _, err := h.nodes[n].table.Apply(0, shardmap.Encode(m)); err != nil {
				t.Fatal(err)
			}
		}
	}
	settle := func() {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			done := true
			for _, n := range live {
				if h.nodes[n].rep.Reconciled() < version {
					done = false
				}
			}
			if done {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		deadline = time.Now().Add(1500 * time.Millisecond)
		for time.Now().Before(deadline) {
			pending := 0
			for _, n := range live {
				pending += h.nodes[n].rep.RetainedMovesPending()
			}
			if pending == 0 {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	apply(cur)
	steps = append(steps, fmt.Sprintf("v1 %v replicas %d", cur.Basis, replicas))
	settle()
	start := time.Now()
	acked := map[string]string{}
	for s := 0; s < shards; s++ {
		for k := 0; k < 2; k++ {
			tp := topicIn2(shards, s, k)
			payload := fmt.Sprintf("m%d-%d", s, k)
			if err := writeRetained(h, cur.Shards[s].Leader, tp, payload); err == nil {
				acked[tp] = payload
			}
		}
	}
	if len(acked) == 0 {
		t.Fatalf("seed %d: no retained write acknowledged under v1", seed)
	}
	// With HOARDMQ_RETAINED_TRACE set, every change in where an acknowledged
	// message is held (own store L, replica store R, per node) is recorded with
	// the steps, and printed when the sequence fails.
	var traceMu sync.Mutex
	var trace []string
	mark := func(line string) {
		traceMu.Lock()
		trace = append(trace, fmt.Sprintf("%7.1fms %s", float64(time.Since(start).Microseconds())/1000, line))
		traceMu.Unlock()
	}
	if os.Getenv("HOARDMQ_RETAINED_TRACE") != "" {
		stop := make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			last := map[string]string{}
			for {
				for _, n := range names {
					for tp, payload := range acked {
						st := ""
						if ownHolds(h, n, tp) == payload {
							st += "L"
						}
						if replicaHolds(h, n, tp) == payload {
							st += "R"
						}
						if k := n + " " + tp; last[k] != st {
							last[k] = st
							mark(fmt.Sprintf("%s %s %q", k, "holds", st))
						}
					}
				}
				select {
				case <-stop:
					return
				case <-time.After(2 * time.Millisecond):
				}
			}
		}()
		t.Cleanup(func() { close(stop); <-done })
	}
	check := func(when string) {
		var missing []string
		deadline := time.Now().Add(2 * time.Second)
		for {
			missing = missing[:0]
			for tp, payload := range acked {
				held := false
				for _, n := range live {
					if ownHolds(h, n, tp) == payload || replicaHolds(h, n, tp) == payload {
						held = true
						break
					}
				}
				if !held {
					missing = append(missing, tp)
				}
			}
			if len(missing) == 0 || time.Now().After(deadline) {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if len(missing) > 0 {
			slices.Sort(missing)
			traceMu.Lock()
			for _, l := range trace {
				t.Log(l)
			}
			traceMu.Unlock()
			t.Fatalf("seed %d: %s, acknowledged retained messages held by no live node: %v; live %v; steps: %s", seed, when, missing, live, strings.Join(steps, "; "))
		}
	}
	check("after v1")
	died := false
	stepCount := 1 + rng.Intn(4)
	deathAt := rng.Intn(stepCount)
	// A fault may outlast its step into the next, so a push lost at one map
	// change can still be missing when a node is lost at the next.
	var carried [][2]string
	for step := 0; step < stepCount; step++ {
		desc := ""
		if !died && step == deathAt && len(live) > 1 {
			lost := live[rng.Intn(len(live))]
			lose(h, lost, seed*10+int64(step))
			live = slices.DeleteFunc(live, func(n string) bool { return n == lost })
			died = true
			desc += " lost " + lost
		}
		next := randomBasis(live)
		version++
		m := shardmap.Compute(version, next, replicas, shards)
		var faults [][2]string
		if len(live) > 1 && rng.Intn(10) < 6 {
			var from string
			if rng.Intn(2) == 0 {
				from = cur.Shards[rng.Intn(shards)].Leader
			} else {
				from = m.Shards[rng.Intn(shards)].Leader
			}
			if slices.Contains(live, from) {
				to := live[rng.Intn(len(live))]
				if to != from {
					dropAll(h, from, to, seed*100+int64(step))
					faults = append(faults, [2]string{from, to})
					desc += fmt.Sprintf(" drop %s->%s", from, to)
				}
			}
		}
		mark(fmt.Sprintf("apply v%d %v%s", version, m.Basis, desc))
		apply(m)
		steps = append(steps, fmt.Sprintf("v%d %v%s", version, m.Basis, desc))
		settle()
		for _, f := range carried {
			h.nodes[f[0]].tr.SetFault(f[1], nil)
			mark(fmt.Sprintf("lift %s->%s (carried)", f[0], f[1]))
		}
		carried = nil
		for _, f := range faults {
			if step+1 < stepCount && rng.Intn(2) == 0 {
				carried = append(carried, f)
				steps[len(steps)-1] += " (carried)"
				continue
			}
			h.nodes[f[0]].tr.SetFault(f[1], nil)
			mark(fmt.Sprintf("lift %s->%s", f[0], f[1]))
		}
		settle()
		check(fmt.Sprintf("after v%d", version))
		cur = m
	}
}
