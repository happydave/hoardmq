package sim

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// RouteSample is one node's view of one topic at one instant: the remote
// nodes it would forward to, and whether an owner it is hiding holds a
// matching route. A publish acknowledged while every node's sample is empty
// is the WI 1353 defect, and this is what lets the checker say so instead
// of leaving it to a node debug log (WI 1354).
type RouteSample struct {
	At     time.Time `json:"at"`
	Node   string    `json:"node"`
	Topic  string    `json:"topic"`
	Remote []string  `json:"remote,omitempty"`
	Hidden bool      `json:"hidden,omitempty"`
}

// routeSampler polls every running node's routing table for the workload's
// topics. It reads the table's own lock and nothing else — no session lock,
// no product path — so it cannot change what it measures.
type routeSampler struct {
	c      *Cluster
	topics []string

	mu      sync.Mutex
	samples []RouteSample

	stopped chan struct{}
	done    chan struct{}
}

func newRouteSampler(c *Cluster, topics []string) *routeSampler {
	s := &routeSampler{c: c, topics: topics, stopped: make(chan struct{}), done: make(chan struct{})}
	go s.loop()
	return s
}

func (s *routeSampler) loop() {
	defer close(s.done)
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-s.stopped:
			return
		case <-tick.C:
		}
		now := time.Now()
		var batch []RouteSample
		for _, n := range s.c.Running() {
			srv := n.Server()
			if srv == nil || srv.Routes() == nil {
				continue
			}
			t := srv.Routes().Table()
			for _, topic := range s.topics {
				remote, groups := t.Match("default", topic)
				for _, g := range groups {
					for owner := range g.Members {
						remote = append(remote, owner)
					}
				}
				sort.Strings(remote)
				batch = append(batch, RouteSample{At: now, Node: n.Name, Topic: topic,
					Remote: remote, Hidden: t.HiddenMatch("default", topic)})
			}
		}
		s.mu.Lock()
		s.samples = append(s.samples, batch...)
		s.mu.Unlock()
	}
}

func (s *routeSampler) stop() {
	select {
	case <-s.stopped:
	default:
		close(s.stopped)
	}
	<-s.done
}

func (s *routeSampler) taken() []RouteSample {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]RouteSample(nil), s.samples...)
}

// routesAround renders what each node's table held for a topic in a window
// either side of an instant, so a loss is read against the routing state
// that produced it rather than against a hypothesis.
func routesAround(samples []RouteSample, topic string, at time.Time, window time.Duration) string {
	byNode := map[string][]string{}
	var nodes []string
	for _, s := range samples {
		if s.Topic != topic || s.At.Before(at.Add(-window)) || s.At.After(at.Add(window)) {
			continue
		}
		if _, seen := byNode[s.Node]; !seen {
			nodes = append(nodes, s.Node)
		}
		what := "-"
		if len(s.Remote) > 0 {
			what = strings.Join(s.Remote, "+")
		}
		if s.Hidden {
			what += "(hidden)"
		}
		// Collapse runs: a window of identical samples says one thing.
		if prev := byNode[s.Node]; len(prev) > 0 && strings.HasSuffix(prev[len(prev)-1], " "+what) {
			continue
		}
		byNode[s.Node] = append(byNode[s.Node], fmt.Sprintf("%s %s", s.At.Format("15:04:05.000"), what))
	}
	if len(nodes) == 0 {
		return "no route samples in the window"
	}
	sort.Strings(nodes)
	var out []string
	for _, n := range nodes {
		out = append(out, n+": "+strings.Join(byNode[n], ", "))
	}
	return strings.Join(out, "; ")
}
