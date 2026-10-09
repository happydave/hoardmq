package wal

import (
	"os"
	"sync"
	"testing"
)

// Fsync outside the appender's lock must survive segment rolls: the file it
// targets is either synced by the caller or was synced and closed by roll,
// and every record is recoverable afterwards. Tiny segments make rolls
// frequent; run under -race.
func TestFsyncAcrossRollsAndClose(t *testing.T) {
	dir := t.TempDir()
	var appendMu sync.Mutex // the owner's lock, as the store would hold it
	l, err := Open(dir, "g", Options{SegmentBytes: 4096, Logger: quiet()})
	if err != nil {
		t.Fatal(err)
	}
	const writers, each = 8, 200
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				appendMu.Lock()
				if _, err := l.Append(1, make([]byte, 100)); err != nil {
					appendMu.Unlock()
					t.Error(err)
					return
				}
				f, gen, err := l.Flush()
				appendMu.Unlock()
				if err != nil {
					t.Error(err)
					return
				}
				// Outside the lock, as group commit does; rolls race this.
				if err := l.Fsync(f, gen); err != nil {
					t.Errorf("fsync after roll: %v", err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	appendMu.Lock()
	last := l.LastIndex()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	appendMu.Unlock()
	if last != writers*each {
		t.Fatalf("last index %d, want %d", last, writers*each)
	}
	// Fsync after Close is a no-op, not an error on a closed file.
	if err := l.Fsync(nil, 0); err != nil {
		t.Fatalf("fsync after close: %v", err)
	}

	re, err := Open(dir, "g", Options{SegmentBytes: 4096, Logger: quiet()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = re.Close() }()
	n := 0
	if err := re.Iterate(1, func(Record) error { n++; return nil }); err != nil {
		t.Fatal(err)
	}
	if n != writers*each {
		t.Fatalf("recovered %d records, want %d", n, writers*each)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatal(err)
	}
}
