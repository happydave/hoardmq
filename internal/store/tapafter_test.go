package store

import (
	"sync/atomic"
	"testing"
	"time"
)

// TestTapAfterWorkRunsOutsideTheLock: work the tap names for after the append
// runs with the store's lock free, so another goroutine's append does not wait
// behind it. The assertion is that second append completing, not elapsed time.
func TestTapAfterWorkRunsOutsideTheLock(t *testing.T) {
	running := make(chan struct{})
	second := make(chan error, 1)
	s := openStore(t, t.TempDir(), Options{Tap: func(rec Record) func() {
		if rec.Key.ClientID != "first" {
			return nil
		}
		return func() {
			close(running)
			select {
			case err := <-second:
				if err != nil {
					t.Errorf("the second append failed: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Error("a second append did not complete while the after-work ran: the store's lock is still held")
			}
		}
	}})
	go func() {
		<-running
		second <- s.SessionUpsert(SessionMeta{Namespace: "default", ClientID: "second"})
	}()
	if err := s.SessionUpsert(SessionMeta{Namespace: "default", ClientID: "first"}); err != nil {
		t.Fatal(err)
	}
	st, err := s.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if st.Find("default", "first") == nil || st.Find("default", "second") == nil {
		t.Fatalf("both records are in the log: %d sessions", len(st.Sessions))
	}
}

// TestTapAfterWorkRunsOncePerAppendedRecord: every successful append that
// names after-work runs it once, and an append that failed runs none — the
// record it would have been about was not written either.
func TestTapAfterWorkRunsOncePerAppendedRecord(t *testing.T) {
	var runs atomic.Int64
	s := openStore(t, t.TempDir(), Options{Tap: func(Record) func() {
		return func() { runs.Add(1) }
	}})
	for _, id := range []string{"a", "b", "c"} {
		if err := s.SessionUpsert(SessionMeta{Namespace: "default", ClientID: id}); err != nil {
			t.Fatal(err)
		}
	}
	if got := runs.Load(); got != 3 {
		t.Fatalf("after-work ran %d times for three appends, want 3", got)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.SessionUpsert(SessionMeta{Namespace: "default", ClientID: "d"}); err == nil {
		t.Fatal("an append to a closed store returned no error")
	}
	if got := runs.Load(); got != 3 {
		t.Fatalf("after-work ran %d times once a failed append is added, want the same 3", got)
	}
}
