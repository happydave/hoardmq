package broker

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// topology watches the cluster's shape — members with roles and the set of
// nodes alive by lease record — and reports how long it has been unchanged.
// Readiness requires it stable for the configured interval. The shard map is
// not part of it: a crash or a join changes the alive set as well, and a drain
// or a return changes only the map.
type topology struct {
	fingerprint func() string
	stable      time.Duration
	interval    time.Duration
	onChange    func()

	mu       sync.Mutex
	last     string
	since    time.Time
	sampled  bool
	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
}

func newTopology(fingerprint func() string, stable time.Duration, onChange func()) *topology {
	interval := stable / 10
	if interval < 100*time.Millisecond {
		interval = 100 * time.Millisecond
	}
	return &topology{fingerprint: fingerprint, stable: stable, interval: interval, onChange: onChange, stop: make(chan struct{}), done: make(chan struct{})}
}

func (t *topology) start() { go t.loop() }

func (t *topology) close() {
	t.stopOnce.Do(func() {
		close(t.stop)
		<-t.done
	})
}

func (t *topology) loop() {
	defer close(t.done)
	t.sample()
	tick := time.NewTicker(t.interval)
	defer tick.Stop()
	for {
		select {
		case <-t.stop:
			return
		case <-tick.C:
			t.sample()
		}
	}
}

// sample takes one fingerprint; a change resets the clock.
func (t *topology) sample() {
	fp := t.fingerprint()
	t.mu.Lock()
	changed := t.sampled && fp != t.last
	if !t.sampled || changed {
		t.last, t.since, t.sampled = fp, time.Now(), true
	}
	t.mu.Unlock()
	if changed && t.onChange != nil {
		t.onChange()
	}
}

// Stable reports whether the topology has been unchanged for the interval;
// a zero interval is always stable once sampled.
func (t *topology) Stable() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.sampled {
		return t.stable == 0
	}
	return time.Since(t.since) >= t.stable
}

// clusterFingerprint renders the shape the watcher compares.
func (s *Server) clusterFingerprint() string {
	var b strings.Builder
	if s.consensus != nil {
		members := s.consensus.Members()
		sort.Slice(members, func(i, j int) bool { return members[i].Name < members[j].Name })
		for _, m := range members {
			fmt.Fprintf(&b, "%s=%s;", m.Name, m.Role)
		}
	}
	if s.leases != nil && s.consensus != nil {
		var alive []string
		for _, m := range s.consensus.Members() {
			if s.leases.Table().Live(m.Name) {
				alive = append(alive, m.Name)
			}
		}
		sort.Strings(alive)
		fmt.Fprintf(&b, "alive=%s", strings.Join(alive, ","))
	}
	return b.String()
}
