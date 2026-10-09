package etcdraft

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
)

const recKind = consensus.KindReserved + 7

// recorder is a Recoverable component: it counts applies made while
// recovering and while live, and how often Recovered was called.
type recorder struct {
	mu         sync.Mutex
	state      []byte
	recovering bool
	silent     int // applies while recovering
	live       int // applies while not recovering
	restored   int
	recovered  int
}

func (r *recorder) Apply(_ uint64, p []byte) (any, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state = append(r.state, p...)
	if r.recovering {
		r.silent++
	} else {
		r.live++
	}
	return nil, nil
}
func (r *recorder) Snapshot() ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]byte(nil), r.state...), nil
}
func (r *recorder) Restore(d []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state = append([]byte(nil), d...)
	r.restored++
	return nil
}
func (r *recorder) SetRecovering(on bool) {
	r.mu.Lock()
	r.recovering = on
	r.mu.Unlock()
}
func (r *recorder) Recovered() {
	r.mu.Lock()
	r.recovered++
	r.mu.Unlock()
}

// recState is a copy of the recorder's counters, safe to print.
type recState struct {
	state                             []byte
	silent, live, restored, recovered int
	recovering                        bool
}

func (r *recorder) snapshot() recState {
	r.mu.Lock()
	defer r.mu.Unlock()
	return recState{state: append([]byte(nil), r.state...), silent: r.silent, live: r.live, restored: r.restored, recovered: r.recovered, recovering: r.recovering}
}

// TestRestartReplaysSilentlyThenRecoversOnce: a node restarted over its log
// re-applies the committed history with its Recoverable components silent,
// then tells them once that recovery ended; entries committed after the
// restart are applied live.
func TestRestartReplaysSilentlyThenRecoversOnce(t *testing.T) {
	names := []string{"n1", "n2", "n3"}
	recs := map[string]*recorder{}
	c := newTCluster(t, nil)
	for _, name := range names {
		c.addTransport(name, others(name, names))
	}
	register := func(name string) func(o *Options) {
		return func(o *Options) {
			recs[name] = &recorder{}
			o.Machine.Register(recKind, recs[name])
		}
	}
	for _, name := range names {
		c.startCore(c.nodes[name], names, register(name))
	}
	lead := c.settled(3)
	propose := func(n int) {
		for i := 0; i < n; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, err := lead.core.Propose(ctx, recKind, []byte{'a'})
			cancel()
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	propose(30)
	n3 := c.nodes["n3"]
	if n3 == lead {
		n3 = c.nodes["n2"]
	}
	waitFor(t, "n3 applied the history", 5*time.Second, func() bool { return len(recs[n3.name].snapshot().state) == 30 })
	if s := recs[n3.name].snapshot(); s.recovered != 0 || s.silent != 0 {
		t.Fatalf("a fresh node must not recover: %+v", s)
	}
	n3.core.Stop()
	propose(20)
	c.startCore(n3, names, register(n3.name))
	waitFor(t, "restarted node caught up", 10*time.Second, func() bool {
		s := recs[n3.name].snapshot()
		return len(s.state) == 50 && s.recovered == 1 && !n3.core.Recovering()
	})
	s := recs[n3.name].snapshot()
	if s.silent < 30 {
		t.Fatalf("the replayed history should be applied silently: silent %d live %d", s.silent, s.live)
	}
	if s.live > 20 {
		t.Fatalf("only entries after the restart may be applied live: live %d", s.live)
	}
	// Live proposals after recovery are applied live and recover nothing.
	propose(5)
	waitFor(t, "live applies", 5*time.Second, func() bool { return len(recs[n3.name].snapshot().state) == 55 })
	if s := recs[n3.name].snapshot(); s.recovered != 1 {
		t.Fatalf("recovered again without a restart: %d", s.recovered)
	}
}

// TestSnapshotInstallRecoversOnce: a node restarted after the leader
// compacted past its log receives a snapshot; the restore is silent and
// followed by exactly one Recovered.
func TestSnapshotInstallRecoversOnce(t *testing.T) {
	names := []string{"n1", "n2", "n3"}
	recs := map[string]*recorder{}
	c := newTCluster(t, func(o *Options) { o.SnapshotEntries = 20; o.SnapshotTrailing = 5 })
	for _, name := range names {
		c.addTransport(name, others(name, names))
	}
	register := func(name string) func(o *Options) {
		return func(o *Options) {
			recs[name] = &recorder{}
			o.Machine.Register(recKind, recs[name])
		}
	}
	for _, name := range names {
		c.startCore(c.nodes[name], names, register(name))
	}
	lead := c.settled(3)
	n3 := c.nodes["n3"]
	if n3 == lead {
		n3 = c.nodes["n2"]
	}
	n3.core.Stop()
	for i := 0; i < 80; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := lead.core.Propose(ctx, recKind, []byte{'b'})
		cancel()
		if err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "leader compacted", 10*time.Second, func() bool { return lead.core.Status().SnapshotIndex > 20 })
	c.startCore(n3, names, register(n3.name))
	waitFor(t, "restarted node caught up through a snapshot", 15*time.Second, func() bool {
		s := recs[n3.name].snapshot()
		return len(s.state) == 80 && s.restored >= 1 && s.recovered >= 1 && !n3.core.Recovering()
	})
	if s := recs[n3.name].snapshot(); s.recovered != 1 {
		t.Fatalf("one recovery expected across restart and snapshot install, got %d (restored %d)", s.recovered, s.restored)
	}
}
