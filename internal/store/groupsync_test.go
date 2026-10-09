package store

import (
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Durability, the invariant behind the whole change: a caller is released
// only by an fsync issued after its own append reached the file. The
// injected Sync records the file's size at fsync time; every caller's record
// must end at or before the size of the fsync that released it.
func TestGroupCommitReleasesOnlyAfterCoveringFsync(t *testing.T) {
	var mu sync.Mutex
	var sizes []int64 // file size observed by each fsync, in fsync order
	s := openStore(t, t.TempDir(), Options{Sync: func(f *os.File) error {
		st, err := f.Stat()
		if err != nil {
			return err
		}
		time.Sleep(500 * time.Microsecond)
		mu.Lock()
		sizes = append(sizes, st.Size())
		mu.Unlock()
		return f.Sync()
	}})
	defer func() { _ = s.Close() }()

	const callers = 1000
	var wg sync.WaitGroup
	violations := atomic.Int64{}
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := s.AppendRaw(recSessionUpsert, []byte(`{"i":`+itoa(i)+`}`)); err != nil {
				t.Error(err)
				return
			}
			// The caller's own record is at least its position at append
			// time; the store reports the covered append count, so compare
			// through it: after Sync, synced >= my append index.
			mine := s.LastIndex()
			if err := s.Sync(); err != nil {
				t.Error(err)
				return
			}
			if got := s.syncer.coveredIndex(); got < mine {
				violations.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if n := violations.Load(); n != 0 {
		t.Fatalf("%d callers released before an fsync covered their record", n)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(sizes) == 0 {
		t.Fatal("no fsync ran")
	}
	for i := 1; i < len(sizes); i++ {
		if sizes[i] < sizes[i-1] {
			t.Fatalf("fsync %d saw a smaller file than fsync %d", i, i-1)
		}
	}
}

// Grouping, with its negative control: concurrent callers share fsyncs,
// and the same callers run one after another do not — so the fsync counter
// is a real discriminator and the grouping assertion is not vacuous.
func TestGroupCommitSharesFsyncs(t *testing.T) {
	const callers = 200
	slow := func(syncs *atomic.Int32) func(*os.File) error {
		return func(f *os.File) error {
			syncs.Add(1)
			time.Sleep(2 * time.Millisecond)
			return f.Sync()
		}
	}

	t.Run("concurrent callers group", func(t *testing.T) {
		var syncs atomic.Int32
		s := openStore(t, t.TempDir(), Options{Sync: slow(&syncs)})
		defer func() { _ = s.Close() }()
		base := syncs.Load() // Open syncs the new segment's header
		var wg sync.WaitGroup
		for i := 0; i < callers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				_ = s.AppendRaw(recSessionUpsert, []byte(`{"i":`+itoa(i)+`}`))
				if err := s.Sync(); err != nil {
					t.Error(err)
				}
			}(i)
		}
		wg.Wait()
		if n := syncs.Load() - base; n > callers/5 {
			t.Fatalf("%d callers took %d fsyncs; group commit is not grouping", callers, n)
		}
		st := s.SyncStats()
		if st.Callers != callers || st.Ops < callers || st.Syncs != int64(syncs.Load()-base) {
			t.Fatalf("stats %+v disagree with the run (callers %d, syncs %d)", st, callers, syncs.Load()-base)
		}
	})

	t.Run("negative control: sequential callers do not group", func(t *testing.T) {
		var syncs atomic.Int32
		s := openStore(t, t.TempDir(), Options{Sync: slow(&syncs)})
		defer func() { _ = s.Close() }()
		base := syncs.Load()
		for i := 0; i < callers; i++ {
			_ = s.AppendRaw(recSessionUpsert, []byte(`{"i":`+itoa(i)+`}`))
			if err := s.Sync(); err != nil {
				t.Fatal(err)
			}
		}
		if n := syncs.Load() - base; n != callers {
			t.Fatalf("sequential callers took %d fsyncs, want exactly %d — the counter would not discriminate", n, callers)
		}
	})
}

// A hold collects callers up to the delay or the group size; a lone caller
// with no delay pays no hold at all.
func TestGroupCommitHoldBounds(t *testing.T) {
	t.Run("maxSize fires a held group early", func(t *testing.T) {
		var syncs atomic.Int32
		s := openStore(t, t.TempDir(), Options{GroupMaxDelay: time.Second, GroupMaxSize: 4,
			Sync: func(f *os.File) error { syncs.Add(1); return f.Sync() }})
		defer func() { _ = s.Close() }()
		var wg sync.WaitGroup
		start := time.Now()
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				_ = s.AppendRaw(recSessionUpsert, []byte(`{"i":`+itoa(i)+`}`))
				_ = s.Sync()
			}(i)
		}
		wg.Wait()
		if d := time.Since(start); d > 500*time.Millisecond {
			t.Fatalf("four callers with maxSize 4 waited %s; the group should have fired on size", d)
		}
		if st := s.SyncStats(); st.HeldGroups == 0 {
			t.Fatal("no held group recorded")
		}
	})

	t.Run("maxDelay bounds the hold", func(t *testing.T) {
		s := openStore(t, t.TempDir(), Options{GroupMaxDelay: 20 * time.Millisecond, GroupMaxSize: 1000})
		defer func() { _ = s.Close() }()
		_ = s.AppendRaw(recSessionUpsert, []byte(`{}`))
		start := time.Now()
		if err := s.Sync(); err != nil {
			t.Fatal(err)
		}
		if d := time.Since(start); d < 15*time.Millisecond || d > 300*time.Millisecond {
			t.Fatalf("held %s, want about the 20 ms delay", d)
		}
	})

	t.Run("no delay, lone caller: one fsync, no hold", func(t *testing.T) {
		var syncs atomic.Int32
		s := openStore(t, t.TempDir(), Options{Sync: func(f *os.File) error { syncs.Add(1); return f.Sync() }})
		defer func() { _ = s.Close() }()
		base := syncs.Load()
		_ = s.AppendRaw(recSessionUpsert, []byte(`{}`))
		start := time.Now()
		if err := s.Sync(); err != nil {
			t.Fatal(err)
		}
		if syncs.Load()-base != 1 || time.Since(start) > 200*time.Millisecond {
			t.Fatalf("lone caller: %d fsyncs in %s", syncs.Load()-base, time.Since(start))
		}
		if st := s.SyncStats(); st.HeldGroups != 0 {
			t.Fatalf("a hold was recorded with no delay configured: %+v", st)
		}
	})
}

// An fsync failure reaches every caller of its group and does not advance
// the covered position; once the disk works, the next group repairs the
// journal and succeeds. The backoff after a failed repair is taken out so the
// retry is not refused by it.
func TestGroupCommitErrorReachesTheGroupAndRetries(t *testing.T) {
	saved := repairBackoff
	repairBackoff = 0
	t.Cleanup(func() { repairBackoff = saved })
	var fail atomic.Bool
	boom := errors.New("disk said no")
	s := openStore(t, t.TempDir(), Options{Sync: func(f *os.File) error {
		time.Sleep(2 * time.Millisecond)
		if fail.Load() {
			return boom
		}
		return f.Sync()
	}})
	defer func() { _ = s.Close() }()

	fail.Store(true)
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = s.AppendRaw(recSessionUpsert, []byte(`{"i":`+itoa(i)+`}`))
			errs <- s.Sync()
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if !errors.Is(err, boom) {
			t.Fatalf("a caller of a failed group got %v, want the fsync error", err)
		}
	}
	if s.syncer.coveredIndex() != 0 {
		t.Fatal("a failed fsync advanced the covered position")
	}
	fail.Store(false)
	if err := s.Sync(); err != nil {
		t.Fatalf("retry after a failed group: %v", err)
	}
	if s.syncer.coveredIndex() < 20 {
		t.Fatalf("retry covered %d of 20 appends", s.syncer.coveredIndex())
	}
}

// Close completes the in-flight group and refuses later callers.
func TestGroupCommitCloseCompletesInFlight(t *testing.T) {
	release := make(chan struct{})
	var armed atomic.Bool
	s := openStore(t, t.TempDir(), Options{Sync: func(f *os.File) error {
		if armed.Load() {
			<-release
		}
		return f.Sync()
	}})
	_ = s.AppendRaw(recSessionUpsert, []byte(`{}`))
	armed.Store(true)
	done := make(chan error, 1)
	go func() { done <- s.Sync() }()
	time.Sleep(20 * time.Millisecond) // the leader is inside the injected fsync
	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	time.Sleep(20 * time.Millisecond)
	select {
	case <-closed:
		t.Fatal("Close returned while an fsync was in flight")
	default:
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("in-flight sync: %v", err)
	}
	if err := <-closed; err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := s.Sync(); err == nil {
		t.Fatal("Sync after Close succeeded")
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
