package store

import (
	"testing"
	"time"
)

// TestAppendStatsDivideTheHold: the time the lock is held is recorded with
// the tap's share of it, and an append with no tap has none.
func TestAppendStatsDivideTheHold(t *testing.T) {
	const nap = 20 * time.Millisecond
	s := openStore(t, t.TempDir(), Options{Tap: func(Record) func() { time.Sleep(nap); return nil }})
	for _, id := range []string{"a", "b", "c"} {
		if err := s.SessionUpsert(SessionMeta{Namespace: "default", ClientID: id}); err != nil {
			t.Fatal(err)
		}
	}
	st := s.AppendStats()
	if st.Appends != 3 || st.TapSec < 3*nap.Seconds() || st.HeldSec < st.TapSec || st.HeldMaxMs < float64(nap/time.Millisecond) {
		t.Fatalf("three tapped appends: %+v, want 3 appends, at least %v in the tap, held no less than tapped, and a longest hold of at least %v", st, 3*nap, nap)
	}

	bare := openStore(t, t.TempDir(), Options{})
	if err := bare.SessionUpsert(SessionMeta{Namespace: "default", ClientID: "a"}); err != nil {
		t.Fatal(err)
	}
	if err := bare.AppendRaw(recSessionDelete, []byte(`{"namespace":"default","clientId":"a"}`)); err != nil {
		t.Fatal(err)
	}
	if st := bare.AppendStats(); st.Appends != 2 || st.TapSec != 0 || st.HeldSec <= 0 {
		t.Fatalf("two untapped appends, one raw: %+v, want 2 appends, no tap time and some held", st)
	}
}
