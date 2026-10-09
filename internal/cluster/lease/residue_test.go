package lease

import (
	"testing"
	"time"
)

func TestImportFinds(t *testing.T) {
	for _, c := range []struct {
		name               string
		recovered          bool
		connAge, adoptedAt time.Duration
		want               connFound
	}{
		{"accepted before the adoption was recorded", false, 3 * time.Second, time.Second, connBeforeAdoption},
		{"accepted at the instant it was recorded", false, time.Second, time.Second, connBeforeAdoption},
		{"accepted after it was recorded", false, time.Second, 3 * time.Second, connAfterAdoption},
		{"an adoption queued by recovery, whatever the ages", true, time.Second, 3 * time.Second, connRecovered},
	} {
		if got := importFinds(c.recovered, c.connAge, c.adoptedAt); got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}

// TestWorkerImportCountsTheConnectionItFinds: the worker's import closes a
// client connected here, and says which side of the adoption's recording that
// connection was accepted on; with nobody connected it counts nothing.
func TestWorkerImportCountsTheConnectionItFinds(t *testing.T) {
	for _, c := range []struct {
		name      string
		connected bool
		age       time.Duration
		recovered bool
		want      [3]int64 // before, after, recovered
	}{
		{"nobody connected", false, 0, false, [3]int64{0, 0, 0}},
		{"connected before the adoption", true, time.Hour, false, [3]int64{1, 0, 0}},
		{"connected after the adoption", true, 0, false, [3]int64{0, 1, 0}},
		{"an adoption queued by recovery", true, time.Hour, true, [3]int64{0, 0, 1}},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newAdoptionFixture(t, false)
			if c.recovered {
				f.n.svc.adopting.begin(f.k, adoptJob{epoch: f.n.svc.adopting.m[f.k].job.epoch, prev: "n9", recovered: true})
			}
			if c.connected {
				f.n.sessions.setConnAge("default", "c", c.age)
			}
			f.n.svc.adoptState(f.k)
			if got := f.n.sessions.importedCount(); got != 1 {
				t.Fatalf("imported %d times, want 1", got)
			}
			st := f.n.svc.LivenessStats()
			if got := [3]int64{st.ImportConnBefore, st.ImportConnAfter, st.ImportConnRecovered}; got != c.want {
				t.Fatalf("before/after/recovered = %v, want %v", got, c.want)
			}
		})
	}
}

// TestInlineFetchThatFindsNothingIsCounted: the client is admitted with no
// state, and the run can say how often.
func TestInlineFetchThatFindsNothingIsCounted(t *testing.T) {
	f := newAdoptionFixture(t, false)
	f.state = nil
	res, err := acquire(t, f.n, "c", nil, 60, false)
	if err != nil {
		t.Fatal(err)
	}
	if res.State != nil {
		t.Fatalf("state %v from a fallback that had none", res.State)
	}
	if st := f.n.svc.LivenessStats(); st.AdoptInlineEmpty != 1 || st.AdoptedInline != 1 || st.AdoptInlinePulls != 0 {
		t.Fatalf("counters %+v", st)
	}
}

// TestInlineFetchWhoseLeaseMovedOnIsCountedApart: the lease going elsewhere
// during the fetch loses nothing, and must not read as a fetch that came
// back empty.
func TestInlineFetchWhoseLeaseMovedOnIsCountedApart(t *testing.T) {
	f := newAdoptionFixture(t, false)
	// The adoption on record is for an epoch the lease no longer has.
	f.n.svc.adopting.m[f.k].job.epoch++
	if _, err := acquire(t, f.n, "c", nil, 60, false); err != nil {
		t.Fatal(err)
	}
	if st := f.n.svc.LivenessStats(); st.AdoptInlineMoved != 1 || st.AdoptInlineEmpty != 0 {
		t.Fatalf("counters %+v", st)
	}
}
