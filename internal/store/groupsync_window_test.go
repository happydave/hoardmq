package store

import (
	"testing"
	"time"
)

// TestGroupCommitCallerInTheSealWindowIsCovered forces the interleaving WI
// 1490 found: the leader has read its covered index and released the store
// lock, and before it seals its group a second caller appends and syncs. The
// second caller must not be released by the leader's fsync, which cannot
// cover its record.
func TestGroupCommitCallerInTheSealWindowIsCovered(t *testing.T) {
	s := openStore(t, t.TempDir(), Options{})
	defer func() { _ = s.Close() }()
	if err := s.AppendRaw(recSessionUpsert, []byte(`{"i":0}`)); err != nil {
		t.Fatal(err)
	}

	type result struct {
		mine, covered uint64
		err           error
	}
	second := make(chan result, 1)
	joined := func() bool {
		g := &s.syncer
		g.mu.Lock()
		defer g.mu.Unlock()
		return (g.cur != nil && g.cur.joined >= 2) || (g.next != nil && g.next.joined >= 1)
	}
	fired := false
	s.syncer.afterTarget = func() {
		if fired {
			return
		}
		fired = true
		go func() {
			if err := s.AppendRaw(recSessionUpsert, []byte(`{"i":1}`)); err != nil {
				second <- result{err: err}
				return
			}
			mine := s.LastIndex()
			err := s.Sync()
			second <- result{mine: mine, covered: s.syncer.coveredIndex(), err: err}
		}()
		deadline := time.Now().Add(5 * time.Second)
		for !joined() {
			if time.Now().After(deadline) {
				t.Error("the second caller never joined a group")
				return
			}
			time.Sleep(time.Millisecond)
		}
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-second:
		if r.err != nil {
			t.Fatal(r.err)
		}
		if r.covered < r.mine {
			t.Fatalf("the second caller was released with its record at index %d and the fsync covering only %d", r.mine, r.covered)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the second caller never returned")
	}
}
