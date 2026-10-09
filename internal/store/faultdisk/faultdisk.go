// Package faultdisk is a Sync hook for tests that models what a failed fsync
// does on Linux: every byte written to the file since its last fsync may be
// dropped, the pages are marked clean all the same, and a later fsync that
// succeeds writes only what was written after the failure. Crash then leaves
// each file as a power loss would: the dropped ranges zeroed, and nothing past
// the size the last successful fsync committed.
package faultdisk

import (
	"errors"
	"os"
	"sync"
	"testing"
)

// ErrInjected is the error a failed call returns.
var ErrInjected = errors.New("injected fsync failure")

// Disk is the hook. Before, when set, runs at the start of every call, and
// After once the call's result is decided and before it returns, both outside
// the lock. Match, when set, limits the model to the files it accepts: a call
// on any other file succeeds and is neither counted nor failed.
type Disk struct {
	Before func(call int, path string)
	After  func(call int, err error)
	Match  func(path string) bool

	mu    sync.Mutex
	files map[string]*file
	fail  int // the next fail calls fail
	calls int
}

type file struct {
	clean   int64 // bytes before this offset have been through an fsync, failed or not
	durable int64 // the size the last successful fsync committed
	lost    [][2]int64
}

// New returns a disk on which every call succeeds until FailNext.
func New() *Disk { return &Disk{files: map[string]*file{}} }

// FailNext makes the next n calls fail.
func (d *Disk) FailNext(n int) {
	d.mu.Lock()
	d.fail += n
	d.mu.Unlock()
}

// Calls is how many calls the disk has modelled.
func (d *Disk) Calls() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.calls
}

// Sync is the hook.
func (d *Disk) Sync(f *os.File) error {
	if d.Match != nil && !d.Match(f.Name()) {
		return nil
	}
	d.mu.Lock()
	d.calls++
	call := d.calls
	d.mu.Unlock()
	if d.Before != nil {
		d.Before(call, f.Name())
	}
	st, err := f.Stat()
	if err != nil {
		return err
	}
	err = d.decide(f.Name(), st.Size())
	if d.After != nil {
		d.After(call, err)
	}
	return err
}

func (d *Disk) decide(path string, size int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	ff := d.files[path]
	if ff == nil {
		ff = &file{}
		d.files[path] = ff
	}
	if size < ff.clean {
		// Truncated or rewritten from the start (a checkpoint's temporary file).
		*ff = file{}
	}
	if d.fail > 0 {
		d.fail--
		if size > ff.clean {
			ff.lost = append(ff.lost, [2]int64{ff.clean, size})
			ff.clean = size
		}
		return ErrInjected
	}
	ff.clean, ff.durable = size, size
	return nil
}

// Crash applies the model to every file the hook has seen that still exists.
func (d *Disk) Crash(t testing.TB) {
	t.Helper()
	d.mu.Lock()
	defer d.mu.Unlock()
	for path, ff := range d.files {
		f, err := os.OpenFile(path, os.O_RDWR, 0)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range ff.lost {
			if _, err := f.WriteAt(make([]byte, r[1]-r[0]), r[0]); err != nil {
				t.Fatal(err)
			}
		}
		st, err := f.Stat()
		if err != nil {
			t.Fatal(err)
		}
		if st.Size() > ff.durable {
			if err := f.Truncate(ff.durable); err != nil {
				t.Fatal(err)
			}
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
