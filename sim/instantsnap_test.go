package sim

import (
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeTaker records every snapshot asked for and can hold one in flight until
// released, so a test can restart a hold while a snapshot is being taken.
type fakeTaker struct {
	mu      sync.Mutex
	asked   []time.Time
	release chan struct{} // nil: return at once
}

func (f *fakeTaker) take(forStart time.Time) counterSnapshot {
	f.mu.Lock()
	f.asked = append(f.asked, forStart)
	rel := f.release
	f.mu.Unlock()
	if rel != nil {
		<-rel
	}
	return counterSnapshot{For: forStart, Metrics: map[string]map[string]float64{"n2": {"x": float64(forStart.Unix())}}}
}

func (f *fakeTaker) askedFor() []time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]time.Time(nil), f.asked...)
}

// waitAsked waits until the taker has been asked n times: the snapshot runs on
// its own goroutine.
func waitAsked(t *testing.T, f *fakeTaker, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for len(f.askedFor()) < n {
		if time.Now().After(deadline) {
			t.Fatalf("asked %d times, want %d", len(f.askedFor()), n)
		}
		time.Sleep(time.Millisecond)
	}
}

// D1: one snapshot per hold start, none while no hold is open, and the one
// kept is the confirming hold's.
func TestInstantSnapshotOnePerHoldStart(t *testing.T) {
	f := &fakeTaker{}
	s := newInstantSnapshots(f.take)
	t0 := time.Unix(1000, 0)
	s.observe(time.Time{})
	s.observe(t0)
	waitAsked(t, f, 1)
	// Later passes read the same hold again, until its snapshot has been
	// collected, and once more after: none of them may start another.
	deadline := time.Now().Add(5 * time.Second)
	for len(s.taken) == 0 && time.Now().Before(deadline) {
		s.observe(t0)
		time.Sleep(time.Millisecond)
	}
	s.observe(t0)
	if s.begun != 1 {
		t.Fatalf("a hold read again started %d snapshots, want 1", s.begun)
	}
	s.observe(time.Time{}) // the hold breaks
	t1 := t0.Add(10 * time.Second)
	s.observe(t1) // and restarts
	waitAsked(t, f, 2)
	snap, ok, late := s.finish(t1)
	if !ok || late || !snap.For.Equal(t1) {
		t.Fatalf("kept %v ok=%v late=%v, want the restarted hold's", snap.For, ok, late)
	}
	if got := f.askedFor(); len(got) != 2 || !got[0].Equal(t0) || !got[1].Equal(t1) {
		t.Fatalf("snapshots taken for %v, want one per hold start", got)
	}
	if s.begun != 2 {
		t.Errorf("begun %d, want 2", s.begun)
	}
}

// D1: a hold that restarts while a snapshot is in flight is taken once that
// one returns, never two at once.
func TestInstantSnapshotRestartWhileInFlight(t *testing.T) {
	f := &fakeTaker{release: make(chan struct{})}
	s := newInstantSnapshots(f.take)
	t0, t1 := time.Unix(1000, 0), time.Unix(1010, 0)
	s.observe(t0)
	waitAsked(t, f, 1)
	s.observe(time.Time{})
	s.observe(t1)
	// begun counts on the caller's goroutine, so this reads the decision
	// rather than racing the snapshot's own goroutine.
	if s.begun != 1 {
		t.Fatalf("a second snapshot started while the first was in flight (begun %d)", s.begun)
	}
	f.mu.Lock()
	rel := f.release
	f.release = nil
	f.mu.Unlock()
	close(rel)
	deadline := time.Now().Add(5 * time.Second)
	for len(f.askedFor()) < 2 && time.Now().Before(deadline) {
		s.observe(t1)
		time.Sleep(time.Millisecond)
	}
	snap, ok, late := s.finish(t1)
	if !ok || late || !snap.For.Equal(t1) {
		t.Fatalf("kept %v ok=%v late=%v, want the restarted hold's taken after the first returned", snap.For, ok, late)
	}
}

// D1: no instant, no snapshot kept; an instant with no snapshot taken for it
// is taken late and says so.
func TestInstantSnapshotAbsentAndLate(t *testing.T) {
	f := &fakeTaker{}
	s := newInstantSnapshots(f.take)
	if _, ok, _ := s.finish(time.Time{}); ok {
		t.Fatal("a run with no instant kept a snapshot")
	}
	s = newInstantSnapshots(f.take)
	t0 := time.Unix(1000, 0)
	snap, ok, late := s.finish(t0)
	if !ok || !late || !snap.For.Equal(t0) {
		t.Fatalf("got %v ok=%v late=%v, want a late snapshot for the instant", snap.For, ok, late)
	}
}

// E1.
func TestForgetOrder(t *testing.T) {
	key := forgottenFamily + `{reason="sessions-served"}`
	cases := []struct {
		name      string
		inst, end float64
		haveInst  bool
		want      string
	}{
		{"forgotten before the snapshot", 1, 1, true, forgetBefore},
		{"forgotten after it", 0, 1, true, forgetAfter},
		{"never forgotten", 0, 0, true, forgetNone},
		{"not read at the instant", 0, 1, false, forgetNotRead},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := forgetOrder(map[string]float64{key: c.inst}, map[string]float64{key: c.end}, c.haveInst, true)
			if got != c.want {
				t.Errorf("got %q, want %q", got, c.want)
			}
		})
	}
}

// D2: the split lists only what rose after the instant, per survivor, and
// always dates the forget.
func TestInstantSplitLine(t *testing.T) {
	gap := `hoardmq_forward_failures_total{reason="sequence-gap"}`
	barrier := `hoardmq_replication_barrier_failures_total{reason="unreachable"}`
	fgt := forgottenFamily + `{reason="sessions-served"}`
	atInstant := map[string]map[string]float64{
		"n2": {gap: 5, barrier: 100, fgt: 1},
		"n3": {gap: 2, barrier: 40, fgt: 0},
	}
	atEnd := map[string]map[string]float64{
		"n2": {gap: 5, barrier: 130, fgt: 1},
		"n3": {gap: 2, barrier: 40, fgt: 1},
	}
	rec := &instantRecord{Nodes: map[string]instantTiming{"n2": {0.01, 0.2}, "n3": {0.21, 0.3}}}
	line := instantSplitLine(atInstant, atEnd, rec)
	for _, want := range []string{
		"n2: lost owner's routes forgotten before the instant (snapshot began 0.010 s after it, read in 0.200 s)",
		"n3: lost owner's routes forgotten after the instant",
		"counted after the instant: n2 " + barrier + " +30",
	} {
		if !strings.Contains(line, want) {
			t.Errorf("line %q\nmissing %q", line, want)
		}
	}
	if strings.Contains(line, "sequence-gap") {
		t.Errorf("line %q lists a counter that did not rise", line)
	}
	if got := instantSplitLine(nil, atEnd, &instantRecord{Absent: "no recovery instant: x"}); got != "not split — no recovery instant: x" {
		t.Errorf("absent: %q", got)
	}
}
